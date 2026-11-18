// One peer, spoken to over any Transport, after the handshake: tracks what
// the peer has and whether it chokes us, sends interested/requests, hands
// over the blocks we asked for, and speaks the BEP 10 extension protocol
// (ut_metadata, ut_pex) and the BEP 52 hash messages.
//
// The connection may start before the torrent's metadata is known (magnet
// start): what the peer says it has is kept as is, and checked against the
// geometry once `setGeometry` is called.
//
// The engine does not upload (decision OQ-04): it never sends `unchoke`,
// `bitfield`/`have` or `piece`, rejects metadata and hash requests, and
// ignores the peer's block `request`s.

import {
	ExtensionError,
	LOCAL_EXTENSIONS,
	decodeHandshake,
	decodeMetadataMessage,
	decodePex,
	encodeMetadataReject,
	encodeOurHandshake,
	type ExtensionHandshake,
	type ExtensionName,
	type MetadataMessage,
	type PexMessage,
} from "./extension.ts";
import {
	MSG,
	RESERVED_EXTENSION,
	RESERVED_FAST,
	RESERVED_V2,
	WireParser,
	WireProtocolError,
	checkBitfield,
	encodeHashRequest,
	encodeMessage,
	encodeRequest,
	keepAlive,
	pieceSizeOf,
	type Geometry,
	type HashRequest,
	type WireMessage,
} from "./peer-wire.ts";
import type { Transport } from "./transport.ts";

export interface PeerEvents {
	unchoke?(): void;
	choke?(): void;
	/** The peer's piece set changed (bitfield, have, have-all, have-none). */
	have?(): void;
	/** A block we requested. `data` is only valid during the call: copy it. */
	block?(piece: number, begin: number, data: Uint8Array): void;
	/** BEP 6: the peer won't serve this request. */
	reject?(piece: number, begin: number, length: number): void;
	/** The peer's extended handshake (BEP 10). */
	extensions?(handshake: ExtensionHandshake): void;
	/** ut_metadata data or reject (BEP 9). */
	metadata?(message: MetadataMessage): void;
	/** ut_pex addresses (BEP 11). */
	pex?(message: PexMessage): void;
	/** BEP 52 `hashes`. */
	hashes?(request: HashRequest, hashes: Uint8Array): void;
	/** BEP 52 `hash reject`. */
	hashReject?(request: HashRequest): void;
	/** The connection ended; `error` for protocol violations and transport failures. */
	close?(error?: Error): void;
}

export interface PeerConnectionOptions {
	/** The torrent's geometry, if the metadata is known already. */
	geometry?: Geometry;
	events?: PeerEvents;
	/** Keep-alive period (default 60 s, under the gateways' 90 s idle timeout; 0 disables). */
	keepAliveMs?: number;
	/** Request queue depth we announce (default 250). */
	reqq?: number;
}

/** Most `have`s remembered before the metadata says how many pieces exist. */
const MAX_EARLY_HAVES = 100_000;
/** Request queue depth assumed when the peer does not say (libtorrent's default). */
export const DEFAULT_REQQ = 250;

const blockKey = (piece: number, begin: number) => `${piece}:${begin}`;
const bit = (r: Uint8Array, b: { byte: number; mask: number }) => (r[b.byte] & b.mask) !== 0;

export class PeerConnection {
	readonly transport: Transport;
	/** Both sides set the BEP 6 bit. */
	readonly fast: boolean;
	/** Both sides set the BEP 10 bit. */
	readonly extended: boolean;
	/** The peer set the BEP 52 bit. */
	readonly v2: boolean;
	geometry: Geometry | undefined;
	peerChoking = true;
	amInterested = false;
	closed = false;
	events: PeerEvents;
	/** The peer's extended handshake, once received. */
	remoteExtensions: ExtensionHandshake | undefined;
	/** Blocks received that we never asked for (dropped). */
	unsolicited = 0;
	/** Payload bytes of blocks received. */
	downloaded = 0;
	readonly connectedAt = Date.now();
	/** When the peer last unchoked us (0: never). */
	unchokedAt = 0;
	/** When the last requested block arrived (0: never). */
	lastBlockAt = 0;
	/** BEP 6 rejects received. */
	rejects = 0;

	private readonly parser: WireParser;
	private bits: Uint8Array | undefined;
	private earlyHaves: Set<number> | undefined = new Set();
	private haveAll = false;
	private readonly outstanding = new Map<string, number>();
	private keepAliveTimer: ReturnType<typeof setInterval> | undefined;
	/** When we last wrote to the peer (Date.now()). */
	lastSentAt = Date.now();

	constructor(transport: Transport, options: PeerConnectionOptions = {}) {
		this.transport = transport;
		this.events = options.events ?? {};
		this.parser = new WireParser(options.geometry);
		this.fast = bit(transport.local.reserved, RESERVED_FAST) && bit(transport.remote.reserved, RESERVED_FAST);
		this.extended = bit(transport.local.reserved, RESERVED_EXTENSION) && bit(transport.remote.reserved, RESERVED_EXTENSION);
		this.v2 = bit(transport.remote.reserved, RESERVED_V2);
		const keepAliveMs = options.keepAliveMs ?? 60_000;
		if (keepAliveMs > 0) this.keepAliveTimer = setInterval(() => this.write(keepAlive()), keepAliveMs);
		// BEP 6: with fast on both sides the first message must say what we
		// have; we have nothing to offer (no upload). BEP 10: the extended
		// handshake may follow right away.
		const setup: Uint8Array[] = [];
		if (this.fast) setup.push(encodeMessage(MSG.HAVE_NONE));
		if (this.extended) setup.push(extendedMessage(0, encodeOurHandshake({ reqq: options.reqq })));
		if (setup.length > 0) this.write(concat(setup));
		if (options.geometry) this.setGeometry(options.geometry);
		// Attach on the next microtask: bytes that arrived with the handshake
		// then reach the events the caller sets up right after construction.
		queueMicrotask(() => transport.attach({ data: (bytes) => this.onData(bytes), close: (error) => this.shutdown(error) }));
	}

	/**
	 * The torrent's metadata is known: check what the peer said it has
	 * against it. A bitfield of the wrong size, or a `have` past the last
	 * piece, closes the connection.
	 */
	setGeometry(geometry: Geometry): void {
		if (this.closed) return;
		const first = this.geometry === undefined;
		this.geometry = { ...geometry };
		this.parser.setGeometry(this.geometry);
		if (!first) return;
		try {
			if (this.bits) checkBitfield(this.bits, geometry.pieceCount);
			else this.bits = new Uint8Array(Math.ceil(geometry.pieceCount / 8));
			for (const piece of this.earlyHaves ?? []) {
				if (piece >= geometry.pieceCount) throw new WireProtocolError(`have for piece ${piece} of ${geometry.pieceCount}`);
				this.bits[piece >> 3] |= 0x80 >> (piece & 7);
			}
			this.earlyHaves = undefined;
		} catch (e) {
			this.fail(e as WireProtocolError);
		}
	}

	has(piece: number): boolean {
		if (piece < 0 || (this.geometry && piece >= this.geometry.pieceCount)) return false;
		if (this.haveAll) return true;
		if (this.earlyHaves?.has(piece)) return true;
		return this.bits !== undefined && (this.bits[piece >> 3] & (0x80 >> (piece & 7))) !== 0;
	}

	get isSeed(): boolean {
		if (this.haveAll) return true;
		const g = this.geometry;
		return g !== undefined && this.pieceCount === g.pieceCount;
	}

	/** Pieces the peer has (0 before the metadata is known, unless have-all). */
	get pieceCount(): number {
		const g = this.geometry;
		if (!g) return 0;
		if (this.haveAll) return g.pieceCount;
		let n = 0;
		for (let p = 0; p < g.pieceCount; p++) if (this.has(p)) n++;
		return n;
	}

	get outstandingRequests(): number {
		return this.outstanding.size;
	}

	/** Bytes requested and not answered yet. */
	get outstandingBytes(): number {
		let n = 0;
		for (const length of this.outstanding.values()) n += length;
		return n;
	}

	/** Queue depth the peer accepts. */
	get reqq(): number {
		return this.remoteExtensions?.reqq ?? DEFAULT_REQQ;
	}

	/** Whether the peer accepts this extension. */
	supports(name: ExtensionName | string): boolean {
		return (this.remoteExtensions?.m.get(name) ?? 0) > 0;
	}

	interested(): void {
		if (this.amInterested) return;
		this.amInterested = true;
		this.write(encodeMessage(MSG.INTERESTED));
	}

	notInterested(): void {
		if (!this.amInterested) return;
		this.amInterested = false;
		this.write(encodeMessage(MSG.NOT_INTERESTED));
	}

	/**
	 * Request blocks, all in one write. Returns how many were sent (blocks
	 * outside the piece, or already outstanding, are skipped).
	 */
	request(blocks: readonly (readonly [piece: number, begin: number, length: number])[]): number {
		const g = this.geometry;
		if (!g) return 0;
		const parts: Uint8Array[] = [];
		for (const [piece, begin, length] of blocks) {
			if (piece >= g.pieceCount || length <= 0 || length > 16 * 1024) continue;
			if (begin + length > pieceSizeOf(g, piece)) continue;
			const key = blockKey(piece, begin);
			if (this.outstanding.has(key)) continue;
			this.outstanding.set(key, length);
			parts.push(encodeRequest(MSG.REQUEST, piece, begin, length));
		}
		if (parts.length > 0) this.write(concat(parts));
		return parts.length;
	}

	isOutstanding(piece: number, begin: number): boolean {
		return this.outstanding.has(blockKey(piece, begin));
	}

	cancel(piece: number, begin: number): void {
		const key = blockKey(piece, begin);
		const length = this.outstanding.get(key);
		if (length === undefined) return;
		this.outstanding.delete(key);
		// A cancel after a seek must not wait for the upstream batch timer.
		this.write(encodeRequest(MSG.CANCEL, piece, begin, length), true);
	}

	/** Send an extension message the peer accepts; false if it doesn't. */
	sendExtended(name: ExtensionName | string, payload: Uint8Array): boolean {
		const id = this.remoteExtensions?.m.get(name);
		if (!id) return false;
		this.write(extendedMessage(id, payload));
		return true;
	}

	/** BEP 52 hash request. */
	requestHashes(request: HashRequest): void {
		this.write(encodeHashRequest(MSG.HASH_REQUEST, request));
	}

	close(): void {
		this.shutdown();
		this.transport.close();
	}

	/** Send a keep-alive now (a caller that schedules them itself passes keepAliveMs: 0). */
	sendKeepAlive(): void {
		this.write(keepAlive());
	}

	private write(bytes: Uint8Array, urgent = false): void {
		if (this.closed || this.transport.closed) return;
		this.lastSentAt = Date.now();
		if (urgent && this.transport.sendUrgent) this.transport.sendUrgent(bytes);
		else this.transport.send(bytes);
	}

	private fail(error: WireProtocolError): void {
		this.shutdown(error);
		this.transport.close();
	}

	private onData(bytes: Uint8Array): void {
		if (this.closed) return;
		let messages: WireMessage[];
		try {
			messages = this.parser.push(bytes);
		} catch (e) {
			this.fail(e instanceof WireProtocolError ? e : new WireProtocolError(String(e)));
			return;
		}
		for (const m of messages) {
			if (this.closed) return;
			this.onMessage(m);
		}
	}

	private onMessage(m: WireMessage): void {
		switch (m.type) {
			case "choke":
				this.peerChoking = true;
				// Without the fast extension a choke drops every pending request;
				// with it, the peer sends a reject for each one it drops.
				if (!this.fast) this.outstanding.clear();
				this.events.choke?.();
				return;
			case "unchoke":
				if (this.peerChoking) this.unchokedAt = Date.now();
				this.peerChoking = false;
				this.events.unchoke?.();
				return;
			case "have":
				if (this.bits && !this.earlyHaves) this.bits[m.piece >> 3] |= 0x80 >> (m.piece & 7);
				else if (this.earlyHaves && this.earlyHaves.size < MAX_EARLY_HAVES) this.earlyHaves.add(m.piece);
				this.events.have?.();
				return;
			case "bitfield":
				this.bits = m.bits;
				if (this.earlyHaves) this.earlyHaves.clear();
				this.events.have?.();
				return;
			case "have_all":
				this.haveAll = true;
				this.events.have?.();
				return;
			case "have_none":
				this.haveAll = false;
				this.earlyHaves?.clear();
				if (this.geometry) this.bits = new Uint8Array(Math.ceil(this.geometry.pieceCount / 8));
				else this.bits = undefined;
				this.events.have?.();
				return;
			case "piece": {
				const key = blockKey(m.piece, m.begin);
				if (this.outstanding.get(key) !== m.data.length) {
					this.unsolicited++;
					return;
				}
				this.outstanding.delete(key);
				this.downloaded += m.data.length;
				this.lastBlockAt = Date.now();
				this.events.block?.(m.piece, m.begin, m.data);
				return;
			}
			case "reject": {
				const key = blockKey(m.piece, m.begin);
				if (!this.outstanding.delete(key)) return;
				this.rejects++;
				this.events.reject?.(m.piece, m.begin, m.length);
				return;
			}
			case "extended":
				this.onExtended(m.id, m.payload);
				return;
			case "hashes":
				this.events.hashes?.(m.request, m.hashes);
				return;
			case "hash_reject":
				this.events.hashReject?.(m.request);
				return;
			case "hash_request":
				// We serve nothing (no upload).
				this.write(encodeHashRequest(MSG.HASH_REJECT, m.request));
				return;
			// request / cancel: we never upload, so there's nothing to serve or cancel.
			// interested / not interested / keep-alive / others: nothing to do.
			default:
				return;
		}
	}

	private onExtended(id: number, payload: Uint8Array): void {
		try {
			if (id === 0) {
				this.remoteExtensions = decodeHandshake(payload);
				this.events.extensions?.(this.remoteExtensions);
			} else if (id === LOCAL_EXTENSIONS.ut_metadata) {
				const msg = decodeMetadataMessage(payload);
				// We have no metadata to give (and never upload): refuse requests.
				if (msg.type === "request") this.sendExtended("ut_metadata", encodeMetadataReject(msg.piece));
				else this.events.metadata?.(msg);
			} else if (id === LOCAL_EXTENSIONS.ut_pex) {
				this.events.pex?.(decodePex(payload));
			}
		} catch (e) {
			// A malformed extension message: ignore it (the peer may still serve blocks).
			if (!(e instanceof ExtensionError)) throw e;
		}
	}

	private shutdown(error?: Error): void {
		if (this.closed) return;
		this.closed = true;
		if (this.keepAliveTimer !== undefined) clearInterval(this.keepAliveTimer);
		this.outstanding.clear();
		this.events.close?.(error);
	}
}

function extendedMessage(id: number, payload: Uint8Array): Uint8Array {
	const body = new Uint8Array(1 + payload.length);
	body[0] = id;
	body.set(payload, 1);
	return encodeMessage(MSG.EXTENDED, body);
}

function concat(parts: Uint8Array[]): Uint8Array {
	if (parts.length === 1) return parts[0];
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
}
