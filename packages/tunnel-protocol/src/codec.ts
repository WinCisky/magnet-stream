// Encoder and decoder of tunnel frames:
//
//   [type u8][conn u16][length u32][payload: length bytes]   (big-endian)
//
// One binary WebSocket message carries one or more whole frames. The
// decoder is explicit binary parsing only: every length is checked against
// the bytes present and the configured limits, nothing is evaluated, and
// malformed input yields a typed error value instead of an exception.

import type { PeerAddress } from "./address.ts";
import {
	CLIENT_CONN_MAX,
	CLIENT_CONN_MIN,
	DEFAULT_MAX_FRAMES,
	DEFAULT_MAX_PAYLOAD,
	FrameType,
	GATEWAY_CONN_MAX,
	GATEWAY_CONN_MIN,
	HEADER_LENGTH,
	INFO_HASH_LENGTH,
	MAX_ANNOUNCE_TRACKERS,
	MAX_PING_PAYLOAD,
	MAX_TRACKER_URL_LENGTH,
	PEER_ID_LENGTH,
	RESERVED_LENGTH,
	SESSION_CONN,
	Transport,
} from "./constants.ts";

/** Client → gateway: connect to `address` and handshake for `infoHash`. */
export interface OpenFrame {
	type: "open";
	conn: number;
	infoHash: Uint8Array;
	/** Reserved bytes the gateway puts in the handshake it writes. */
	reserved: Uint8Array;
	/** Peer id the gateway puts in the handshake it writes. */
	peerId: Uint8Array;
	address: PeerAddress;
	/**
	 * Transport to dial with (see Transport). Absent means TCP, which is how
	 * it is decoded too: a TCP frame has no transport byte on the wire.
	 */
	transport?: number;
}

/** Gateway → client: the remote completed a valid handshake for the same info-hash. */
export interface OpenedFrame {
	type: "opened";
	conn: number;
	/** The remote's reserved bytes (extension bits). */
	reserved: Uint8Array;
	/** The remote's peer id. */
	peerId: Uint8Array;
}

/** Gateway → client: the `open` did not succeed (see FailReason). */
export interface FailedFrame {
	type: "failed";
	conn: number;
	reason: number;
}

/** Both ways: peer-wire bytes after the handshakes. */
export interface DataFrame {
	type: "data";
	conn: number;
	/** A view into the decoded message: copy it to keep it. */
	payload: Uint8Array;
}

/** Both ways: the connection is over (see CloseReason). */
export interface CloseFrame {
	type: "close";
	conn: number;
	reason: number;
}

/** Gateway → client (VPS only): an inbound peer handshook for `infoHash`. */
export interface AcceptFrame {
	type: "accept";
	conn: number;
	infoHash: Uint8Array;
	reserved: Uint8Array;
	peerId: Uint8Array;
	address: PeerAddress;
	/** Transport the peer connected over (see Transport); absent means TCP. */
	transport?: number;
}

/** Both ways, `conn` 0: keeps the session alive; opaque payload of 0..8 bytes. */
export interface PingFrame {
	type: "ping";
	conn: 0;
	payload: Uint8Array;
}

/**
 * Client → gateway, `conn` 0 (VPS gateway): join the swarm of `infoHash`:
 * announce the gateway's port to trackers and the DHT and hand inbound
 * peers for it to this WebSocket as `accept`. The gateway answers inbound
 * handshakes with `reserved` and `peerId`.
 */
export interface AnnounceFrame {
	type: "announce";
	conn: 0;
	infoHash: Uint8Array;
	reserved: Uint8Array;
	peerId: Uint8Array;
	/** The magnet's trackers (`http`, `https`, `udp` URLs), at most 16 of 1..512 printable ASCII bytes. */
	trackers: string[];
}

/** Gateway → client, `conn` 0: the state of the announce for `infoHash` (see AnnounceStatus). */
export interface AnnouncedFrame {
	type: "announced";
	conn: 0;
	infoHash: Uint8Array;
	status: number;
}

/** Client → gateway, `conn` 0: stop announcing `infoHash` and accepting peers for it. */
export interface WithdrawFrame {
	type: "withdraw";
	conn: 0;
	infoHash: Uint8Array;
}

export type Frame =
	| OpenFrame
	| OpenedFrame
	| FailedFrame
	| DataFrame
	| CloseFrame
	| AcceptFrame
	| PingFrame
	| AnnounceFrame
	| AnnouncedFrame
	| WithdrawFrame;

export type FrameErrorCode =
	/** The type byte is not a known frame type. */
	| "unknown_type"
	/** `length` is above the decoder's `maxPayload`. */
	| "too_long"
	/** The message ends inside a frame (header or payload). */
	| "truncated"
	/** The payload does not have the layout its frame type requires. */
	| "bad_payload"
	/** `conn` is not allowed for this frame type. */
	| "bad_conn"
	/** More frames than the decoder's `maxFrames` in one message/push. */
	| "too_many_frames"
	/** An empty message. */
	| "empty";

export class FrameError extends Error {
	readonly code: FrameErrorCode;
	/** Byte offset (within the message) of the offending frame. */
	readonly offset: number;

	constructor(code: FrameErrorCode, offset: number, detail?: string) {
		super(detail ? `${code} at byte ${offset}: ${detail}` : `${code} at byte ${offset}`);
		this.name = "FrameError";
		this.code = code;
		this.offset = offset;
	}
}

export interface DecodeLimits {
	/** Largest payload accepted (default 1 MiB). */
	maxPayload?: number;
	/** Most frames accepted in one message (default 1024). */
	maxFrames?: number;
}

export type DecodeResult = { ok: true; frames: Frame[] } | { ok: false; error: FrameError; frames: Frame[] };

const ADDRESS_FAMILY_LENGTH: Readonly<Record<number, number>> = { 4: 4, 6: 16 };
const PEER_FIELDS_LENGTH = INFO_HASH_LENGTH + RESERVED_LENGTH + PEER_ID_LENGTH; // 48

// ---------------------------------------------------------------- encoding

/** Encode frames into one WebSocket message. Throws RangeError on invalid frames (a local bug). */
export function encodeFrames(frames: readonly Frame[]): Uint8Array {
	if (frames.length === 0) throw new RangeError("no frames to encode");
	const payloads = frames.map(encodePayload);
	const out = new Uint8Array(payloads.reduce((n, p) => n + HEADER_LENGTH + p.length, 0));
	const view = new DataView(out.buffer);
	let at = 0;
	frames.forEach((frame, i) => {
		checkConn(frame);
		const payload = payloads[i];
		out[at] = typeCode(frame.type);
		view.setUint16(at + 1, frame.conn);
		view.setUint32(at + 3, payload.length);
		out.set(payload, at + HEADER_LENGTH);
		at += HEADER_LENGTH + payload.length;
	});
	return out;
}

export function encodeFrame(frame: Frame): Uint8Array {
	return encodeFrames([frame]);
}

function typeCode(type: Frame["type"]): number {
	switch (type) {
		case "open":
			return FrameType.OPEN;
		case "opened":
			return FrameType.OPENED;
		case "failed":
			return FrameType.FAILED;
		case "data":
			return FrameType.DATA;
		case "close":
			return FrameType.CLOSE;
		case "accept":
			return FrameType.ACCEPT;
		case "ping":
			return FrameType.PING;
		case "announce":
			return FrameType.ANNOUNCE;
		case "announced":
			return FrameType.ANNOUNCED;
		case "withdraw":
			return FrameType.WITHDRAW;
	}
}

function checkConn(frame: Frame): void {
	const ok = connAllowed(typeCode(frame.type), frame.conn);
	if (!ok) throw new RangeError(`conn ${frame.conn} not allowed for ${frame.type}`);
}

function encodePayload(frame: Frame): Uint8Array {
	switch (frame.type) {
		case "open":
		case "accept":
			return peerFields(frame.infoHash, frame.reserved, frame.peerId, frame.address, frame.transport);
		case "opened": {
			const out = new Uint8Array(RESERVED_LENGTH + PEER_ID_LENGTH);
			out.set(fixed(frame.reserved, RESERVED_LENGTH, "reserved"), 0);
			out.set(fixed(frame.peerId, PEER_ID_LENGTH, "peerId"), RESERVED_LENGTH);
			return out;
		}
		case "failed":
		case "close":
			if (!Number.isInteger(frame.reason) || frame.reason < 0 || frame.reason > 255) {
				throw new RangeError(`bad reason ${frame.reason}`);
			}
			return Uint8Array.of(frame.reason);
		case "data":
			if (frame.payload.length === 0) throw new RangeError("empty data frame");
			return frame.payload;
		case "ping":
			if (frame.payload.length > MAX_PING_PAYLOAD) throw new RangeError("ping payload too long");
			return frame.payload;
		case "announce":
			return announcePayload(frame);
		case "announced": {
			if (!Number.isInteger(frame.status) || frame.status < 0 || frame.status > 255) throw new RangeError(`bad status ${frame.status}`);
			const out = new Uint8Array(INFO_HASH_LENGTH + 1);
			out.set(fixed(frame.infoHash, INFO_HASH_LENGTH, "infoHash"));
			out[INFO_HASH_LENGTH] = frame.status;
			return out;
		}
		case "withdraw":
			return fixed(frame.infoHash, INFO_HASH_LENGTH, "infoHash");
	}
}

/** A tracker URL as the protocol carries it: 1..512 printable ASCII bytes (0x21..0x7e), no spaces. */
export function isTrackerUrlText(url: string): boolean {
	return url.length >= 1 && url.length <= MAX_TRACKER_URL_LENGTH && /^[\x21-\x7e]+$/.test(url);
}

/** `info_hash[20] reserved[8] peer_id[20] count u8 (count × [len u16][url])` */
function announcePayload(frame: AnnounceFrame): Uint8Array {
	if (frame.trackers.length > MAX_ANNOUNCE_TRACKERS) throw new RangeError("too many trackers");
	const urls = frame.trackers.map((t) => {
		if (!isTrackerUrlText(t)) throw new RangeError("bad tracker url");
		return Uint8Array.from(t, (c) => c.charCodeAt(0));
	});
	const out = new Uint8Array(PEER_FIELDS_LENGTH + 1 + urls.reduce((n, u) => n + 2 + u.length, 0));
	out.set(fixed(frame.infoHash, INFO_HASH_LENGTH, "infoHash"), 0);
	out.set(fixed(frame.reserved, RESERVED_LENGTH, "reserved"), INFO_HASH_LENGTH);
	out.set(fixed(frame.peerId, PEER_ID_LENGTH, "peerId"), INFO_HASH_LENGTH + RESERVED_LENGTH);
	out[PEER_FIELDS_LENGTH] = urls.length;
	let at = PEER_FIELDS_LENGTH + 1;
	for (const u of urls) {
		out[at] = u.length >> 8;
		out[at + 1] = u.length & 0xff;
		out.set(u, at + 2);
		at += 2 + u.length;
	}
	return out;
}

function peerFields(infoHash: Uint8Array, reserved: Uint8Array, peerId: Uint8Array, address: PeerAddress, transport = Transport.TCP as number): Uint8Array {
	const ipLength = ADDRESS_FAMILY_LENGTH[address.family];
	if (ipLength === undefined || address.ip.length !== ipLength) throw new RangeError("bad address");
	if (!Number.isInteger(address.port) || address.port < 1 || address.port > 65535) throw new RangeError("bad port");
	if (!Number.isInteger(transport) || transport < 0 || transport > 255) throw new RangeError(`bad transport ${transport}`);
	// TCP is the transport byte left out (the frame of protocol v1 before uTP).
	const extra = transport === Transport.TCP ? 0 : 1;
	const out = new Uint8Array(PEER_FIELDS_LENGTH + 1 + ipLength + 2 + extra);
	out.set(fixed(infoHash, INFO_HASH_LENGTH, "infoHash"), 0);
	out.set(fixed(reserved, RESERVED_LENGTH, "reserved"), INFO_HASH_LENGTH);
	out.set(fixed(peerId, PEER_ID_LENGTH, "peerId"), INFO_HASH_LENGTH + RESERVED_LENGTH);
	out[PEER_FIELDS_LENGTH] = address.family;
	out.set(address.ip, PEER_FIELDS_LENGTH + 1);
	new DataView(out.buffer).setUint16(PEER_FIELDS_LENGTH + 1 + ipLength, address.port);
	if (extra) out[PEER_FIELDS_LENGTH + 1 + ipLength + 2] = transport;
	return out;
}

function fixed(bytes: Uint8Array, length: number, what: string): Uint8Array {
	if (bytes.length !== length) throw new RangeError(`${what} must be ${length} bytes`);
	return bytes;
}

// ---------------------------------------------------------------- decoding

/** Which `conn` values each frame type may carry. */
export function connAllowed(type: number, conn: number): boolean {
	switch (type) {
		case FrameType.PING:
		case FrameType.ANNOUNCE:
		case FrameType.ANNOUNCED:
		case FrameType.WITHDRAW:
			return conn === SESSION_CONN;
		case FrameType.OPEN:
			return conn >= CLIENT_CONN_MIN && conn <= CLIENT_CONN_MAX;
		case FrameType.ACCEPT:
			return conn >= GATEWAY_CONN_MIN && conn <= GATEWAY_CONN_MAX;
		default:
			return conn !== SESSION_CONN && conn <= 0xffff;
	}
}

/**
 * Decode one WebSocket message: it must hold whole frames only. On error,
 * `frames` holds the frames decoded before the bad one (callers normally
 * drop the session anyway).
 */
export function decodeMessage(message: Uint8Array, limits: DecodeLimits = {}): DecodeResult {
	if (message.length === 0) return { ok: false, error: new FrameError("empty", 0), frames: [] };
	const { frames, consumed, error } = decodeFrom(message, 0, limits);
	if (error) return { ok: false, error, frames };
	if (consumed !== message.length) {
		return { ok: false, error: new FrameError("truncated", consumed), frames };
	}
	return { ok: true, frames };
}

/**
 * Incremental decoder for a byte stream (frames may be split across
 * chunks). After an error it stays failed: every later push returns it.
 * Decoded `data`/`ping` payloads are copies, independent of later pushes.
 */
export class FrameDecoder {
	private pending: Uint8Array = new Uint8Array(0);
	private failure: FrameError | undefined;
	private offset = 0;
	private readonly limits: DecodeLimits;

	constructor(limits: DecodeLimits = {}) {
		this.limits = limits;
	}

	/** Bytes held back waiting for the rest of a frame. */
	get buffered(): number {
		return this.pending.length;
	}

	push(chunk: Uint8Array): DecodeResult {
		if (this.failure) return { ok: false, error: this.failure, frames: [] };
		const buf = this.pending.length === 0 ? chunk : concat(this.pending, chunk);
		const { frames, consumed, error } = decodeFrom(buf, this.offset, this.limits);
		if (error) {
			this.failure = error;
			this.pending = new Uint8Array(0);
			return { ok: false, error, frames: frames.map(copyFrame) };
		}
		this.offset += consumed;
		this.pending = buf.slice(consumed);
		return { ok: true, frames: frames.map(copyFrame) };
	}
}

/**
 * Decode whole frames from the start of `buf`. Stops at the first
 * incomplete frame (`consumed` tells where); `truncated` is left to the
 * caller (a message must be complete, a stream may continue).
 */
function decodeFrom(
	buf: Uint8Array,
	baseOffset: number,
	limits: DecodeLimits,
): { frames: Frame[]; consumed: number; error?: FrameError } {
	const maxPayload = limits.maxPayload ?? DEFAULT_MAX_PAYLOAD;
	const maxFrames = limits.maxFrames ?? DEFAULT_MAX_FRAMES;
	const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
	const frames: Frame[] = [];
	let at = 0;
	while (buf.length - at >= 1) {
		const where = baseOffset + at;
		const type = buf[at];
		if (type < FrameType.OPEN || type > FrameType.WITHDRAW) {
			return { frames, consumed: at, error: new FrameError("unknown_type", where, `type 0x${type.toString(16)}`) };
		}
		if (buf.length - at < HEADER_LENGTH) break;
		const conn = view.getUint16(at + 1);
		const length = view.getUint32(at + 3);
		if (length > maxPayload) {
			return { frames, consumed: at, error: new FrameError("too_long", where, `${length} bytes`) };
		}
		if (!connAllowed(type, conn)) {
			return { frames, consumed: at, error: new FrameError("bad_conn", where, `conn ${conn}`) };
		}
		if (buf.length - at - HEADER_LENGTH < length) break;
		if (frames.length >= maxFrames) {
			return { frames, consumed: at, error: new FrameError("too_many_frames", where) };
		}
		const payload = buf.subarray(at + HEADER_LENGTH, at + HEADER_LENGTH + length);
		const frame = decodePayload(type, conn, payload);
		if (typeof frame === "string") {
			return { frames, consumed: at, error: new FrameError("bad_payload", where, frame) };
		}
		frames.push(frame);
		at += HEADER_LENGTH + length;
	}
	return { frames, consumed: at };
}

/** The frame, or why its payload is invalid. */
function decodePayload(type: number, conn: number, p: Uint8Array): Frame | string {
	switch (type) {
		case FrameType.OPEN:
		case FrameType.ACCEPT: {
			const fields = readPeerFields(p);
			if (typeof fields === "string") return fields;
			return { type: type === FrameType.OPEN ? "open" : "accept", conn, ...fields };
		}
		case FrameType.OPENED:
			if (p.length !== RESERVED_LENGTH + PEER_ID_LENGTH) return "opened payload must be 28 bytes";
			return { type: "opened", conn, reserved: p.subarray(0, RESERVED_LENGTH), peerId: p.subarray(RESERVED_LENGTH) };
		case FrameType.FAILED:
		case FrameType.CLOSE:
			if (p.length !== 1) return "reason payload must be 1 byte";
			return { type: type === FrameType.FAILED ? "failed" : "close", conn, reason: p[0] };
		case FrameType.DATA:
			if (p.length === 0) return "empty data frame";
			return { type: "data", conn, payload: p };
		case FrameType.PING:
			if (p.length > MAX_PING_PAYLOAD) return "ping payload above 8 bytes";
			return { type: "ping", conn: 0, payload: p };
		case FrameType.ANNOUNCE:
			return readAnnounce(p);
		case FrameType.ANNOUNCED:
			if (p.length !== INFO_HASH_LENGTH + 1) return "announced payload must be 21 bytes";
			return { type: "announced", conn: 0, infoHash: p.subarray(0, INFO_HASH_LENGTH), status: p[INFO_HASH_LENGTH] };
		case FrameType.WITHDRAW:
			if (p.length !== INFO_HASH_LENGTH) return "withdraw payload must be 20 bytes";
			return { type: "withdraw", conn: 0, infoHash: p };
		default:
			return "unknown type";
	}
}

function readAnnounce(p: Uint8Array): AnnounceFrame | string {
	if (p.length < PEER_FIELDS_LENGTH + 1) return "announce payload too short";
	const count = p[PEER_FIELDS_LENGTH];
	if (count > MAX_ANNOUNCE_TRACKERS) return `more than ${MAX_ANNOUNCE_TRACKERS} trackers`;
	const trackers: string[] = [];
	let at = PEER_FIELDS_LENGTH + 1;
	for (let i = 0; i < count; i++) {
		if (p.length - at < 2) return "announce ends inside a tracker length";
		const length = (p[at] << 8) | p[at + 1];
		if (length === 0 || length > MAX_TRACKER_URL_LENGTH) return "tracker url length out of range";
		if (p.length - at - 2 < length) return "announce ends inside a tracker url";
		let url = "";
		for (let j = at + 2; j < at + 2 + length; j++) {
			if (p[j] < 0x21 || p[j] > 0x7e) return "tracker url is not printable ascii";
			url += String.fromCharCode(p[j]);
		}
		trackers.push(url);
		at += 2 + length;
	}
	if (at !== p.length) return "announce payload has trailing bytes";
	return {
		type: "announce",
		conn: 0,
		infoHash: p.subarray(0, INFO_HASH_LENGTH),
		reserved: p.subarray(INFO_HASH_LENGTH, INFO_HASH_LENGTH + RESERVED_LENGTH),
		peerId: p.subarray(INFO_HASH_LENGTH + RESERVED_LENGTH, PEER_FIELDS_LENGTH),
		trackers,
	};
}

function readPeerFields(
	p: Uint8Array,
): { infoHash: Uint8Array; reserved: Uint8Array; peerId: Uint8Array; address: PeerAddress; transport?: number } | string {
	if (p.length < PEER_FIELDS_LENGTH + 1) return "peer payload too short";
	const family = p[PEER_FIELDS_LENGTH];
	const ipLength = ADDRESS_FAMILY_LENGTH[family];
	if (ipLength === undefined) return `unknown address family ${family}`;
	const base = PEER_FIELDS_LENGTH + 1 + ipLength + 2;
	if (p.length !== base && p.length !== base + 1) return "peer payload has the wrong length";
	const ipAt = PEER_FIELDS_LENGTH + 1;
	const port = (p[ipAt + ipLength] << 8) | p[ipAt + ipLength + 1];
	if (port === 0) return "port 0";
	const fields = {
		infoHash: p.subarray(0, INFO_HASH_LENGTH),
		reserved: p.subarray(INFO_HASH_LENGTH, INFO_HASH_LENGTH + RESERVED_LENGTH),
		peerId: p.subarray(INFO_HASH_LENGTH + RESERVED_LENGTH, PEER_FIELDS_LENGTH),
		address: { family: family as 4 | 6, ip: p.subarray(ipAt, ipAt + ipLength), port },
	};
	if (p.length === base) return fields;
	// One encoding per frame: TCP is the byte left out, never an explicit 0.
	if (p[base] === Transport.TCP) return "explicit tcp transport byte";
	return { ...fields, transport: p[base] };
}

/** Detach every byte field from the decoder's buffer. */
function copyFrame(frame: Frame): Frame {
	switch (frame.type) {
		case "open":
		case "accept":
			return {
				...frame,
				infoHash: frame.infoHash.slice(),
				reserved: frame.reserved.slice(),
				peerId: frame.peerId.slice(),
				address: { ...frame.address, ip: frame.address.ip.slice() },
			};
		case "opened":
			return { ...frame, reserved: frame.reserved.slice(), peerId: frame.peerId.slice() };
		case "announce":
			return { ...frame, infoHash: frame.infoHash.slice(), reserved: frame.reserved.slice(), peerId: frame.peerId.slice(), trackers: [...frame.trackers] };
		case "announced":
		case "withdraw":
			return { ...frame, infoHash: frame.infoHash.slice() };
		case "data":
		case "ping":
			return { ...frame, payload: frame.payload.slice() };
		default:
			return frame;
	}
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
	const out = new Uint8Array(a.length + b.length);
	out.set(a);
	out.set(b, a.length);
	return out;
}
