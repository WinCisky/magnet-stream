// BitTorrent peer-wire pieces the worker needs: building the handshake,
// interested/have-none and block requests, and observing what a peer sends
// before it unchokes us. Nothing after the unchoke is parsed here: those
// bytes are piped to the browser untouched.

export const HANDSHAKE_LEN = 68;
const PROTOCOL = new TextEncoder().encode("BitTorrent protocol");

export const MSG = {
	CHOKE: 0,
	UNCHOKE: 1,
	INTERESTED: 2,
	HAVE: 4,
	BITFIELD: 5,
	REQUEST: 6,
	HAVE_ALL: 0x0e,
	HAVE_NONE: 0x0f,
	EXTENDED: 20,
} as const;

// A bitfield for 1M pieces is 128 KiB; nothing a peer legitimately sends
// before unchoking is bigger than that.
export const MAX_SETUP_MESSAGE = 256 * 1024;

export class WireError extends Error {
	constructor(readonly code: "bad_handshake" | "infohash_mismatch" | "protocol") {
		super(code);
	}
}

export function peerId(): Uint8Array {
	const id = new Uint8Array(20);
	id.set(new TextEncoder().encode("-MW0100-"));
	crypto.getRandomValues(id.subarray(8));
	return id;
}

export function handshake(infoHash: Uint8Array, id: Uint8Array): Uint8Array {
	const buf = new Uint8Array(HANDSHAKE_LEN);
	buf[0] = PROTOCOL.length;
	buf.set(PROTOCOL, 1);
	buf[20 + 5] |= 0x10; // extension protocol (BEP 10), for the peer's `reqq`
	buf[20 + 7] |= 0x04; // fast extension (BEP 6), for have-all and rejects
	buf.set(infoHash, 28);
	buf.set(id, 48);
	return buf;
}

export function message(id: number, payload: Uint8Array = new Uint8Array(0)): Uint8Array {
	const buf = new Uint8Array(5 + payload.length);
	const view = new DataView(buf.buffer);
	view.setUint32(0, payload.length + 1);
	buf[4] = id;
	buf.set(payload, 5);
	return buf;
}

export type Block = readonly [piece: number, begin: number, length: number];

/** All request messages for `blocks`, in one buffer (one socket write). */
export function requests(blocks: readonly Block[]): Uint8Array {
	const buf = new Uint8Array(blocks.length * 17);
	const view = new DataView(buf.buffer);
	blocks.forEach(([piece, begin, length], i) => {
		const at = i * 17;
		view.setUint32(at, 13);
		buf[at + 4] = MSG.REQUEST;
		view.setUint32(at + 5, piece);
		view.setUint32(at + 9, begin);
		view.setUint32(at + 13, length);
	});
	return buf;
}

/**
 * Watches the bytes a peer sends until it unchokes us: its handshake, then
 * bitfield / have / have-all / have-none, and its extension handshake (for
 * `reqq`, the number of requests it will queue).
 */
export class SetupParser {
	private pending: Uint8Array = new Uint8Array(0);
	private handshakeDone = false;
	fast = false;
	unchoked = false;
	haveAll = false;
	bitfield: Uint8Array | undefined;
	readonly haves = new Set<number>();
	reqq: number | undefined;
	client: string | undefined;

	constructor(private readonly infoHash: Uint8Array) {}

	get handshaken(): boolean {
		return this.handshakeDone;
	}

	hasPiece(index: number): boolean {
		if (this.haveAll || this.haves.has(index)) return true;
		const byte = this.bitfield?.[index >> 3];
		return byte !== undefined && (byte & (0x80 >> (index & 7))) !== 0;
	}

	push(chunk: Uint8Array): void {
		const buf = concat(this.pending, chunk);
		let at = 0;
		if (!this.handshakeDone) {
			if (buf.length < HANDSHAKE_LEN) {
				this.pending = buf;
				return;
			}
			this.readHandshake(buf.subarray(0, HANDSHAKE_LEN));
			at = HANDSHAKE_LEN;
		}
		const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
		while (!this.unchoked && buf.length - at >= 4) {
			const len = view.getUint32(at);
			if (len > MAX_SETUP_MESSAGE) throw new WireError("protocol");
			if (buf.length - at - 4 < len) break;
			if (len > 0) this.readMessage(buf[at + 4], buf.subarray(at + 5, at + 4 + len));
			at += 4 + len;
		}
		this.pending = buf.slice(at);
	}

	private readHandshake(hs: Uint8Array): void {
		if (hs[0] !== PROTOCOL.length || !equal(hs.subarray(1, 20), PROTOCOL)) {
			throw new WireError("bad_handshake");
		}
		if (!equal(hs.subarray(28, 48), this.infoHash)) throw new WireError("infohash_mismatch");
		this.fast = (hs[20 + 7] & 0x04) !== 0;
		this.handshakeDone = true;
	}

	private readMessage(id: number, payload: Uint8Array): void {
		switch (id) {
			case MSG.UNCHOKE:
				this.unchoked = true;
				break;
			case MSG.CHOKE:
				this.unchoked = false;
				break;
			case MSG.BITFIELD:
				this.bitfield = payload.slice();
				break;
			case MSG.HAVE:
				if (payload.length === 4 && this.haves.size < 1 << 20) {
					this.haves.add(new DataView(payload.buffer, payload.byteOffset).getUint32(0));
				}
				break;
			case MSG.HAVE_ALL:
				this.haveAll = true;
				break;
			case MSG.HAVE_NONE:
				this.bitfield = undefined;
				break;
			case MSG.EXTENDED:
				if (payload[0] === 0) this.readExtensionHandshake(payload.subarray(1));
				break;
		}
	}

	private readExtensionHandshake(payload: Uint8Array): void {
		const dict = decodeBencode(payload);
		if (!(dict instanceof Map)) return;
		const reqq = dict.get("reqq");
		if (typeof reqq === "number" && reqq > 0) this.reqq = reqq;
		const v = dict.get("v");
		if (v instanceof Uint8Array) this.client = new TextDecoder().decode(v).slice(0, 64);
	}
}

type Bencoded = number | Uint8Array | Bencoded[] | Map<string, Bencoded>;

/** Tiny bencode decoder: returns undefined for anything malformed. */
export function decodeBencode(data: Uint8Array): Bencoded | undefined {
	let at = 0;
	const text = new TextDecoder();
	const readInt = (end: number): number | undefined => {
		const n = Number(text.decode(data.subarray(at, end)));
		return Number.isSafeInteger(n) ? n : undefined;
	};
	const value = (depth: number): Bencoded | undefined => {
		if (depth > 32 || at >= data.length) return undefined;
		const c = data[at];
		if (c === 0x69 /* i */) {
			const end = data.indexOf(0x65 /* e */, at);
			if (end < 0) return undefined;
			at++;
			const n = readInt(end);
			at = end + 1;
			return n;
		}
		if (c === 0x6c /* l */ || c === 0x64 /* d */) {
			at++;
			const list: Bencoded[] = [];
			const dict = new Map<string, Bencoded>();
			while (at < data.length && data[at] !== 0x65) {
				if (c === 0x6c) {
					const item = value(depth + 1);
					if (item === undefined) return undefined;
					list.push(item);
				} else {
					const key = value(depth + 1);
					const item = value(depth + 1);
					if (!(key instanceof Uint8Array) || item === undefined) return undefined;
					dict.set(text.decode(key), item);
				}
			}
			if (at >= data.length) return undefined;
			at++;
			return c === 0x6c ? list : dict;
		}
		const colon = data.indexOf(0x3a /* : */, at);
		if (colon < 0) return undefined;
		const len = readInt(colon);
		if (len === undefined || len < 0 || colon + 1 + len > data.length) return undefined;
		at = colon + 1 + len;
		return data.subarray(colon + 1, at);
	};
	return value(0);
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
	if (a.length === 0) return b;
	const out = new Uint8Array(a.length + b.length);
	out.set(a);
	out.set(b, a.length);
	return out;
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
	return a.length === b.length && a.every((byte, i) => byte === b[i]);
}
