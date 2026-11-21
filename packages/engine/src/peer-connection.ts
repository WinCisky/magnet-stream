// One peer, spoken to over any Transport, after the handshake: tracks what
// the peer has and whether it chokes us, sends interested/requests, hands
// over the blocks we asked for, and speaks the BEP 10 extension protocol
// (ut_metadata, ut_pex) and the BEP 52 hash messages.
//
// The connection may start before the torrent's metadata is known (magnet
// start): what the peer says it has is kept as is, and checked against the
// geometry once `setGeometry` is called.
//
// Upload (decision OQ-04, M3-12, upload.ts): the connection tracks the
// peer's interest and our choke state, and hands the peer's `request`,
// `cancel` and ut_metadata requests to the uploader, which decides. Every
// upload message (bitfield/have, choke/unchoke, piece, reject, metadata)
// is lazy: it rides on the next upstream message. Without an uploader (or
// with upload off) the connection behaves as M1-06: no bitfield, `have`,
// unchoke or piece; requests ignored, metadata and hash requests rejected.

import {
	ExtensionError,
	LOCAL_EXTENSIONS,
	decodeHandshake,
	decodeMetadataMessage,
	decodePex,
	encodeMetadataData,
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
import type { UploadPeer } from "./upload.ts";

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
	/** The peer asks us for a block (upload). Unhandled: ignored. */
	request?(piece: number, begin: number, length: number): void;
	/** The peer withdrew a block request. */
	cancelRequest?(piece: number, begin: number, length: number): void;
	/** The peer became interested in us, or stopped being. */
	interest?(interested: boolean): void;
	/** A ut_metadata request (BEP 9). Unhandled: rejected. */
	metadataRequest?(piece: number): void;
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
	/**
	 * Verified pieces we offer, sent as the first message (a bitfield, or
	 * have-all with the fast extension). Absent: have-none with the fast
	 * extension, nothing without it.
	 */
	bitfield?: Uint8Array;
	/** Size of the verified info dict we serve over ut_metadata (announced in our extended handshake). */
	metadataSize?: number;
}

/** Most `have`s remembered before the metadata says how many pieces exist. */
const MAX_EARLY_HAVES = 100_000;
/** Request queue depth assumed when the peer does not say (libtorrent's default). */
export const DEFAULT_REQQ = 250;

const blockKey = (piece: number, begin: number) => `${piece}:${begin}`;
const bit = (r: Uint8Array, b: { byte: number; mask: number }) => (r[b.byte] & b.mask) !== 0;

export class PeerConnection implements UploadPeer {
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
	/** We choke the peer (we never unchoke without an uploader). */
	amChoking = true;
	/** The peer said it is interested in our pieces. */
	peerInterested = false;
	/** Payload bytes of blocks we sent. */
	uploaded = 0;

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
		// BEP 3 / BEP 6: the first message says what we have (a bitfield,
		// have-all, or have-none with fast on both sides; without fast and
		// with nothing to offer, nothing). BEP 10: the extended handshake may
		// follow right away.
		const setup: Uint8Array[] = [];
		const bits = options.bitfield && options.geometry && options.bitfield.length === Math.ceil(options.geometry.pieceCount / 8) ? options.bitfield : undefined;
		if (bits && this.fast && isFull(bits, options.geometry!.pieceCount)) setup.push(encodeMessage(MSG.HAVE_ALL));
		else if (bits && bits.some((b) => b !== 0)) setup.push(encodeMessage(MSG.BITFIELD, bits));
		else if (this.fast) setup.push(encodeMessage(MSG.HAVE_NONE));
		if (this.extended) setup.push(extendedMessage(0, encodeOurHandshake({ reqq: options.reqq, metadataSize: options.metadataSize })));
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

	/**
	 * Withdraw a request. A block that still arrives for it is dropped as
	 * unsolicited. `urgent` (a cancel after a seek) skips the upstream batch
	 * timer; a duplicate request that lost its race can wait for the next
	 * message.
	 */
	cancel(piece: number, begin: number, urgent = true): boolean {
		const key = blockKey(piece, begin);
		const length = this.outstanding.get(key);
		if (length === undefined) return false;
		this.outstanding.delete(key);
		this.write(encodeRequest(MSG.CANCEL, piece, begin, length), urgent);
		return true;
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

	// ---------------------------------------------------------- upload (lazy)

	chokePeer(): void {
		if (this.amChoking) return;
		this.amChoking = true;
		this.write(encodeMessage(MSG.CHOKE), "lazy");
	}

	unchokePeer(): void {
		if (!this.amChoking) return;
		this.amChoking = false;
		this.write(encodeMessage(MSG.UNCHOKE), "lazy");
	}

	sendHave(piece: number): void {
		const payload = new Uint8Array(4);
		new DataView(payload.buffer).setUint32(0, piece);
		this.write(encodeMessage(MSG.HAVE, payload), "lazy");
	}

	/** A block of a verified piece (the uploader checked the request). */
	sendPiece(piece: number, begin: number, data: Uint8Array): void {
		const payload = new Uint8Array(8 + data.length);
		const view = new DataView(payload.buffer);
		view.setUint32(0, piece);
		view.setUint32(4, begin);
		payload.set(data, 8);
		this.uploaded += data.length;
		this.write(encodeMessage(MSG.PIECE, payload), "lazy");
	}

	/** BEP 6 reject (only with the fast extension on both sides). */
	rejectRequest(piece: number, begin: number, length: number): void {
		if (!this.fast) return;
		this.write(encodeRequest(MSG.REJECT, piece, begin, length), "lazy");
	}

	/** A piece of the verified info dict (BEP 9); false if the peer does not accept ut_metadata. */
	sendMetadataPiece(piece: number, totalSize: number, data: Uint8Array): boolean {
		const id = this.remoteExtensions?.m.get("ut_metadata");
		if (!id) return false;
		this.write(extendedMessage(id, encodeMetadataData(piece, totalSize, data)), "lazy");
		return true;
	}

	rejectMetadata(piece: number): void {
		const id = this.remoteExtensions?.m.get("ut_metadata");
		if (id) this.write(extendedMessage(id, encodeMetadataReject(piece)), "lazy");
	}

	close(): void {
		this.shutdown();
		this.transport.close();
	}

	/** Send a keep-alive now (a caller that schedules them itself passes keepAliveMs: 0). */
	sendKeepAlive(): void {
		this.write(keepAlive());
	}

	private write(bytes: Uint8Array, mode: "normal" | "urgent" | "lazy" | boolean = "normal"): void {
		if (this.closed || this.transport.closed) return;
		this.lastSentAt = Date.now();
		const how = mode === true ? "urgent" : mode === false ? "normal" : mode;
		if (how === "urgent" && this.transport.sendUrgent) this.transport.sendUrgent(bytes);
		else if (how === "lazy" && this.transport.sendLazy) this.transport.sendLazy(bytes);
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
				// BEP 52 hash requests are not served (out of M3-12's scope).
				this.write(encodeHashRequest(MSG.HASH_REJECT, m.request), "lazy");
				return;
			case "request":
				this.events.request?.(m.piece, m.begin, m.length);
				return;
			case "cancel":
				this.events.cancelRequest?.(m.piece, m.begin, m.length);
				return;
			case "interested":
			case "not_interested": {
				const interested = m.type === "interested";
				if (interested === this.peerInterested) return;
				this.peerInterested = interested;
				this.events.interest?.(interested);
				return;
			}
			// keep-alive / others: nothing to do.
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
				// Requests go to the uploader (the verified info dict); without one, refused.
				if (msg.type === "request") {
					if (this.events.metadataRequest) this.events.metadataRequest(msg.piece);
					else this.rejectMetadata(msg.piece);
				} else this.events.metadata?.(msg);
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

/** Every one of `pieceCount` bits is set. */
function isFull(bits: Uint8Array, pieceCount: number): boolean {
	for (let p = 0; p < pieceCount; p++) if ((bits[p >> 3] & (0x80 >> (p & 7))) === 0) return false;
	return true;
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
