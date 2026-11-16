// One peer, spoken to over any Transport, after the handshake: tracks what
// the peer has and whether it chokes us, sends interested/requests, and
// hands over the blocks we asked for.
//
// The engine does not upload (decision OQ-04): it never sends `unchoke`,
// `bitfield`/`have` or `piece`, and ignores the peer's `request`s.

import { MSG, RESERVED_FAST, WireParser, WireProtocolError, encodeMessage, encodeRequest, keepAlive, pieceSizeOf, type Geometry, type WireMessage } from "./peer-wire.ts";
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
	/** The connection ended; `error` for protocol violations and transport failures. */
	close?(error?: Error): void;
}

export interface PeerConnectionOptions extends Geometry {
	events?: PeerEvents;
	/** Keep-alive period (default 60 s; 0 disables). */
	keepAliveMs?: number;
}

const blockKey = (piece: number, begin: number) => `${piece}:${begin}`;

export class PeerConnection {
	readonly transport: Transport;
	readonly geometry: Geometry;
	/** Both sides set the BEP 6 bit. */
	readonly fast: boolean;
	peerChoking = true;
	amInterested = false;
	closed = false;
	events: PeerEvents;
	/** Blocks received that we never asked for (dropped). */
	unsolicited = 0;

	private readonly parser: WireParser;
	private bits: Uint8Array;
	private haveAll = false;
	private readonly outstanding = new Map<string, number>();
	private keepAliveTimer: ReturnType<typeof setInterval> | undefined;

	constructor(transport: Transport, options: PeerConnectionOptions) {
		this.transport = transport;
		this.geometry = { pieceCount: options.pieceCount, pieceLength: options.pieceLength, totalLength: options.totalLength };
		this.events = options.events ?? {};
		this.parser = new WireParser(this.geometry);
		this.bits = new Uint8Array(Math.ceil(this.geometry.pieceCount / 8));
		const bit = (r: Uint8Array) => (r[RESERVED_FAST.byte] & RESERVED_FAST.mask) !== 0;
		this.fast = bit(transport.local.reserved) && bit(transport.remote.reserved);
		const keepAliveMs = options.keepAliveMs ?? 60_000;
		if (keepAliveMs > 0) this.keepAliveTimer = setInterval(() => this.write(keepAlive()), keepAliveMs);
		// BEP 6: with fast on both sides the first message must say what we
		// have; we have nothing to offer (no upload).
		if (this.fast) this.write(encodeMessage(MSG.HAVE_NONE));
		transport.attach({ data: (bytes) => this.onData(bytes), close: (error) => this.shutdown(error) });
	}

	has(piece: number): boolean {
		if (piece < 0 || piece >= this.geometry.pieceCount) return false;
		return this.haveAll || (this.bits[piece >> 3] & (0x80 >> (piece & 7))) !== 0;
	}

	/** Pieces the peer has. */
	get pieceCount(): number {
		let n = 0;
		for (let p = 0; p < this.geometry.pieceCount; p++) if (this.has(p)) n++;
		return n;
	}

	get outstandingRequests(): number {
		return this.outstanding.size;
	}

	interested(): void {
		if (this.amInterested) return;
		this.amInterested = true;
		this.write(encodeMessage(MSG.INTERESTED));
	}

	/**
	 * Request blocks, all in one write. Returns how many were sent (blocks
	 * outside the piece, or already outstanding, are skipped).
	 */
	request(blocks: readonly (readonly [piece: number, begin: number, length: number])[]): number {
		const parts: Uint8Array[] = [];
		for (const [piece, begin, length] of blocks) {
			if (piece >= this.geometry.pieceCount || length <= 0 || length > 16 * 1024) continue;
			if (begin + length > pieceSizeOf(this.geometry, piece)) continue;
			const key = blockKey(piece, begin);
			if (this.outstanding.has(key)) continue;
			this.outstanding.set(key, length);
			parts.push(encodeRequest(MSG.REQUEST, piece, begin, length));
		}
		if (parts.length > 0) this.write(concat(parts));
		return parts.length;
	}

	cancel(piece: number, begin: number): void {
		const key = blockKey(piece, begin);
		const length = this.outstanding.get(key);
		if (length === undefined) return;
		this.outstanding.delete(key);
		this.write(encodeRequest(MSG.CANCEL, piece, begin, length));
	}

	close(): void {
		this.shutdown();
		this.transport.close();
	}

	private write(bytes: Uint8Array): void {
		if (!this.closed && !this.transport.closed) this.transport.send(bytes);
	}

	private onData(bytes: Uint8Array): void {
		if (this.closed) return;
		let messages: WireMessage[];
		try {
			messages = this.parser.push(bytes);
		} catch (e) {
			this.shutdown(e instanceof WireProtocolError ? e : new WireProtocolError(String(e)));
			this.transport.close();
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
				this.peerChoking = false;
				this.events.unchoke?.();
				return;
			case "have":
				this.bits[m.piece >> 3] |= 0x80 >> (m.piece & 7);
				this.events.have?.();
				return;
			case "bitfield":
				this.bits = m.bits;
				this.events.have?.();
				return;
			case "have_all":
				this.haveAll = true;
				this.events.have?.();
				return;
			case "have_none":
				this.haveAll = false;
				this.bits.fill(0);
				this.events.have?.();
				return;
			case "piece": {
				const key = blockKey(m.piece, m.begin);
				if (this.outstanding.get(key) !== m.data.length) {
					this.unsolicited++;
					return;
				}
				this.outstanding.delete(key);
				this.events.block?.(m.piece, m.begin, m.data);
				return;
			}
			case "reject": {
				const key = blockKey(m.piece, m.begin);
				if (!this.outstanding.delete(key)) return;
				this.events.reject?.(m.piece, m.begin, m.length);
				return;
			}
			// request / cancel: we never upload, so there's nothing to serve or cancel.
			// interested / not interested / keep-alive / extended / others: nothing to do yet.
			default:
				return;
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
