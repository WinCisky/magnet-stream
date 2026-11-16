// BitTorrent peer-wire messages after the handshake (BEP 3, plus the BEP 6
// fast-extension messages): an encoder and an incremental, validating
// parser. Messages may be split anywhere across chunks.

export const MSG = {
	CHOKE: 0,
	UNCHOKE: 1,
	INTERESTED: 2,
	NOT_INTERESTED: 3,
	HAVE: 4,
	BITFIELD: 5,
	REQUEST: 6,
	PIECE: 7,
	CANCEL: 8,
	PORT: 9,
	SUGGEST: 0x0d,
	HAVE_ALL: 0x0e,
	HAVE_NONE: 0x0f,
	REJECT: 0x10,
	ALLOWED_FAST: 0x11,
	EXTENDED: 20,
} as const;

/** Handshake reserved bits. */
export const RESERVED_FAST = { byte: 7, mask: 0x04 } as const;
export const RESERVED_EXTENSION = { byte: 5, mask: 0x10 } as const;

/** Largest block we ever request, and so the largest `piece` payload we accept. */
export const MAX_BLOCK = 16 * 1024;
/** Upper bound for any message (guards the buffer against hostile lengths). */
export const MAX_MESSAGE = 1 << 20;

export class WireProtocolError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "WireProtocolError";
	}
}

export type WireMessage =
	| { type: "keepalive" }
	| { type: "choke" }
	| { type: "unchoke" }
	| { type: "interested" }
	| { type: "not_interested" }
	| { type: "have"; piece: number }
	| { type: "bitfield"; bits: Uint8Array }
	| { type: "request"; piece: number; begin: number; length: number }
	/** `data` is a view into the parser's buffer, valid until the next push: copy it. */
	| { type: "piece"; piece: number; begin: number; data: Uint8Array }
	| { type: "cancel"; piece: number; begin: number; length: number }
	| { type: "have_all" }
	| { type: "have_none" }
	| { type: "reject"; piece: number; begin: number; length: number }
	| { type: "extended"; id: number; payload: Uint8Array }
	| { type: "other"; id: number };

export interface Geometry {
	pieceCount: number;
	pieceLength: number;
	totalLength: number;
}

export function pieceSizeOf(g: Geometry, piece: number): number {
	return piece < g.pieceCount - 1 ? g.pieceLength : g.totalLength - g.pieceLength * (g.pieceCount - 1);
}

// ---------------------------------------------------------------- encoding

export function encodeMessage(id: number, payload: Uint8Array = new Uint8Array(0)): Uint8Array {
	const out = new Uint8Array(5 + payload.length);
	new DataView(out.buffer).setUint32(0, payload.length + 1);
	out[4] = id;
	out.set(payload, 5);
	return out;
}

export function keepAlive(): Uint8Array {
	return new Uint8Array(4);
}

export function encodeRequest(id: typeof MSG.REQUEST | typeof MSG.CANCEL, piece: number, begin: number, length: number): Uint8Array {
	const out = new Uint8Array(17);
	const view = new DataView(out.buffer);
	view.setUint32(0, 13);
	out[4] = id;
	view.setUint32(5, piece);
	view.setUint32(9, begin);
	view.setUint32(13, length);
	return out;
}

// ---------------------------------------------------------------- parsing

/**
 * Incremental parser. `push` returns the complete messages in the chunk and
 * keeps the rest; it throws WireProtocolError on anything malformed (the
 * caller then drops the peer).
 */
export class WireParser {
	private buf = new Uint8Array(64 * 1024);
	private start = 0;
	private end = 0;
	private readonly geometry: Geometry;

	constructor(geometry: Geometry) {
		this.geometry = geometry;
	}

	push(chunk: Uint8Array): WireMessage[] {
		this.append(chunk);
		const out: WireMessage[] = [];
		for (;;) {
			const available = this.end - this.start;
			if (available < 4) break;
			const len = this.u32(this.start);
			if (len > MAX_MESSAGE) throw new WireProtocolError(`message of ${len} bytes`);
			if (available < 4 + len) break;
			const at = this.start + 4;
			this.start += 4 + len;
			out.push(len === 0 ? { type: "keepalive" } : this.message(this.buf[at], at + 1, len - 1));
		}
		return out;
	}

	private message(id: number, at: number, len: number): WireMessage {
		const g = this.geometry;
		const exact = (n: number, name: string) => {
			if (len !== n) throw new WireProtocolError(`${name} with a ${len}-byte payload`);
		};
		const index = (piece: number, name: string) => {
			if (piece >= g.pieceCount) throw new WireProtocolError(`${name} for piece ${piece} of ${g.pieceCount}`);
			return piece;
		};
		switch (id) {
			case MSG.CHOKE:
				exact(0, "choke");
				return { type: "choke" };
			case MSG.UNCHOKE:
				exact(0, "unchoke");
				return { type: "unchoke" };
			case MSG.INTERESTED:
				exact(0, "interested");
				return { type: "interested" };
			case MSG.NOT_INTERESTED:
				exact(0, "not interested");
				return { type: "not_interested" };
			case MSG.HAVE_ALL:
				exact(0, "have all");
				return { type: "have_all" };
			case MSG.HAVE_NONE:
				exact(0, "have none");
				return { type: "have_none" };
			case MSG.HAVE:
				exact(4, "have");
				return { type: "have", piece: index(this.u32(at), "have") };
			case MSG.BITFIELD: {
				exact(Math.ceil(g.pieceCount / 8), "bitfield");
				const bits = this.buf.slice(at, at + len);
				const spare = bits.length * 8 - g.pieceCount;
				if (spare > 0 && (bits[bits.length - 1] & ((1 << spare) - 1)) !== 0) throw new WireProtocolError("bitfield has spare bits set");
				return { type: "bitfield", bits };
			}
			case MSG.REQUEST:
			case MSG.CANCEL:
			case MSG.REJECT: {
				exact(12, "request");
				const piece = index(this.u32(at), "request");
				const msg = { piece, begin: this.u32(at + 4), length: this.u32(at + 8) };
				return id === MSG.REQUEST ? { type: "request", ...msg } : id === MSG.CANCEL ? { type: "cancel", ...msg } : { type: "reject", ...msg };
			}
			case MSG.PIECE: {
				if (len < 8 || len - 8 > MAX_BLOCK) throw new WireProtocolError(`piece with a ${len}-byte payload`);
				const piece = index(this.u32(at), "piece");
				const begin = this.u32(at + 4);
				const size = len - 8;
				if (size === 0 || begin + size > pieceSizeOf(g, piece)) throw new WireProtocolError(`block ${piece}:${begin}+${size} outside the piece`);
				return { type: "piece", piece, begin, data: this.buf.subarray(at + 8, at + len) };
			}
			case MSG.EXTENDED:
				if (len < 1) throw new WireProtocolError("empty extended message");
				return { type: "extended", id: this.buf[at], payload: this.buf.slice(at + 1, at + len) };
			default:
				// port, suggest, allowed-fast and unknown ids: nothing we act on.
				return { type: "other", id };
		}
	}

	private append(chunk: Uint8Array): void {
		const needed = this.end - this.start + chunk.length;
		if (this.end + chunk.length > this.buf.length) {
			if (needed <= this.buf.length / 2) {
				this.buf.copyWithin(0, this.start, this.end);
			} else {
				const bigger = new Uint8Array(Math.max(this.buf.length * 2, needed * 2));
				bigger.set(this.buf.subarray(this.start, this.end));
				this.buf = bigger;
			}
			this.end -= this.start;
			this.start = 0;
		}
		this.buf.set(chunk, this.end);
		this.end += chunk.length;
	}

	private u32(at: number): number {
		const b = this.buf;
		return ((b[at] << 24) | (b[at + 1] << 16) | (b[at + 2] << 8) | b[at + 3]) >>> 0;
	}
}
