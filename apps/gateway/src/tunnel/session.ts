// One tunnel session: the frames of one WebSocket and the TCP connections
// they drive. Runtime-independent (sockets come from a Connector), so the
// same logic runs in the Durable Object and in unit tests.
//
// Rules (docs/tunnel-protocol.md, docs/architecture.md §Tunnel protocol and
// request budget › Rules that keep it cheap and safe):
//   - handshake gate: the gateway writes the 68-byte handshake itself and
//     relays nothing, either way, until the remote's handshake is valid for
//     the same info-hash;
//   - at most 6 connections per WebSocket (a slot frees once its socket closed);
//   - only public addresses on ports >= 1024 (address-policy.ts);
//   - a malformed frame closes the whole session;
//   - a connection with no traffic either way for the idle timeout (90 s)
//     is closed (`close` IDLE), and so is a WebSocket on which the client
//     sent nothing for that long (the client pings every 30 s when quiet);
//   - the host ends the session at its maximum length (`shutdown`).
// Timers are driven by the host calling `sweep()` (once a second in the
// Durable Object), against an injectable clock, so tests need no real time.
// Nothing is stored; logs carry no client IP, peer address or info-hash.

import {
	CloseReason,
	DEFAULT_IDLE_TIMEOUT_MS,
	FailReason,
	MAX_CONNECTIONS_PER_SOCKET,
	SessionCloseCode,
	decodeMessage,
	encodeFrames,
	formatSessionEnd,
	type Frame,
	type OpenFrame,
	type PeerAddress,
} from "@magnet-stream/tunnel-protocol";
import { checkAddress } from "./address-policy";

export const HANDSHAKE_LENGTH = 68;
const PROTOCOL_PREFIX = Uint8Array.of(19, ...new TextEncoder().encode("BitTorrent protocol"));
/** How long a closed socket may take to give its slot back. */
const CLOSE_WAIT_MS = 1_000;

/** The subset of a Workers `Socket` the session needs. */
export interface SocketLike {
	readonly opened: Promise<unknown>;
	readonly closed: Promise<unknown>;
	readonly readable: ReadableStream<Uint8Array>;
	readonly writable: WritableStream<Uint8Array>;
	close(): Promise<void>;
}

export type Connector = (address: PeerAddress) => SocketLike;

export interface SessionIo {
	/** Send one binary WebSocket message. */
	send(message: Uint8Array): void;
	/** Close the WebSocket. */
	close(code: number, reason: string): void;
}

export type LogFields = Record<string, string | number | boolean>;

export interface SessionOptions {
	connect: Connector;
	/** Skip the private-address check (wrangler dev tests only; ports < 1024 stay forbidden). */
	allowPrivate?: boolean;
	connectTimeoutMs?: number;
	handshakeTimeoutMs?: number;
	maxConnections?: number;
	/** Close a connection, or the WebSocket, after this long without traffic (default 90 s). */
	idleTimeoutMs?: number;
	/** A connect that timed out keeps its slot until this long after it started (default 20 s, OQ-03). */
	failedConnectHoldMs?: number;
	/** Clock for the idle checks (default Date.now). */
	now?: () => number;
	/** Structured log sink. Never receives addresses, info-hashes or client IPs. */
	log?: (event: string, fields: LogFields) => void;
}

type State = "opening" | "open" | "closed";

interface Conn {
	id: number;
	state: State;
	socket?: SocketLike;
	/** The TCP connect completed. */
	connected?: boolean;
	reader?: ReadableStreamDefaultReader<Uint8Array>;
	writer?: WritableStreamDefaultWriter<Uint8Array>;
	writes: Promise<void>;
	/** Last byte relayed either way (or the `opened`), for the idle timeout. */
	lastActivity: number;
}

/** A read of `state` that TypeScript won't narrow away across awaits. */
function isClosed(c: Conn): boolean {
	return c.state === "closed";
}

class OpenFailure extends Error {
	readonly reason: number;
	constructor(reason: number) {
		super(`open failed: ${reason}`);
		this.reason = reason;
	}
}

const FAIL_NAMES: Record<number, string> = Object.fromEntries(Object.entries(FailReason).map(([k, v]) => [v, k.toLowerCase()]));
const CLOSE_NAMES: Record<number, string> = Object.fromEntries(Object.entries(CloseReason).map(([k, v]) => [v, k.toLowerCase()]));

export class TunnelSession {
	private readonly io: SessionIo;
	private readonly opts: Required<Omit<SessionOptions, "log">> & Pick<SessionOptions, "log">;
	private lastMessageIn: number;
	/** Why the session ended, once it has. */
	endReason: string | null = null;
	private readonly conns = new Map<number, Conn>();
	/** Sockets not yet closed, including connections already ended on the wire. */
	private slots = 0;
	private ended = false;
	private readonly started: number;
	readonly stats = {
		messagesIn: 0,
		messagesOut: 0,
		framesOut: 0,
		bytesUp: 0,
		bytesDown: 0,
		opens: 0,
		opened: 0,
		failed: 0,
		idleClosed: 0,
	};

	constructor(io: SessionIo, options: SessionOptions) {
		this.io = io;
		this.opts = {
			allowPrivate: false,
			connectTimeoutMs: 5_000,
			handshakeTimeoutMs: 10_000,
			maxConnections: MAX_CONNECTIONS_PER_SOCKET,
			idleTimeoutMs: DEFAULT_IDLE_TIMEOUT_MS,
			failedConnectHoldMs: 20_000,
			now: Date.now,
			...options,
		};
		this.started = this.opts.now();
		this.lastMessageIn = this.started;
	}

	get isEnded(): boolean {
		return this.ended;
	}

	/** The WebSocket close reason for `reason`, with this session's counters. */
	closeReason(reason: string): string {
		return formatSessionEnd({
			reason,
			counters: { messagesIn: this.stats.messagesIn, bytesUp: this.stats.bytesUp, bytesDown: this.stats.bytesDown },
		});
	}

	/**
	 * Idle checks, called periodically by the host: connections with no
	 * traffic either way, and a WebSocket the client has not used, for the
	 * idle timeout.
	 */
	sweep(): void {
		if (this.ended) return;
		const now = this.opts.now();
		if (now - this.lastMessageIn >= this.opts.idleTimeoutMs) {
			this.shutdown("idle", SessionCloseCode.IDLE, CloseReason.SESSION_ENDING);
			return;
		}
		for (const c of [...this.conns.values()]) {
			if (c.state === "open" && now - c.lastActivity >= this.opts.idleTimeoutMs) {
				this.stats.idleClosed++;
				this.end(c, CloseReason.IDLE, true);
			}
		}
	}

	/**
	 * End the session from the gateway side: a `close` with `connReason` for
	 * every connection, then the WebSocket close with the counters.
	 */
	shutdown(reason: string, code: number = SessionCloseCode.SHUTDOWN, connReason: number = CloseReason.SESSION_ENDING): void {
		if (this.ended) return;
		const frames: Frame[] = [...this.conns.values()].map((c) => ({ type: "close", conn: c.id, reason: connReason }));
		if (frames.length > 0) this.emit(frames);
		this.endReason = reason;
		try {
			this.io.close(code, this.closeReason(reason));
		} catch {}
		this.onClose();
	}

	/** Connections currently holding a slot (opening, open, or closing). */
	get activeSlots(): number {
		return this.slots;
	}

	/** A WebSocket message from the client. */
	onMessage(data: ArrayBuffer | ArrayBufferView | string): void {
		if (this.ended) return;
		this.stats.messagesIn++;
		this.lastMessageIn = this.opts.now();
		if (typeof data === "string") return this.abort(SessionCloseCode.UNSUPPORTED_DATA, "unsupported_data");
		// (instanceof ArrayBuffer is unreliable across realms: test for views instead.)
		const bytes = ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data);
		const decoded = decodeMessage(bytes);
		if (!decoded.ok) {
			this.log("tunnel.protocol_error", { code: decoded.error.code });
			return this.abort(SessionCloseCode.PROTOCOL_ERROR, "protocol_error");
		}
		for (const frame of decoded.frames) {
			if (this.ended) return;
			this.onFrame(frame);
		}
	}

	/** The WebSocket closed (either side) or errored: drop every connection. */
	onClose(): void {
		if (this.ended) return;
		this.ended = true;
		this.endReason ??= "normal";
		for (const c of [...this.conns.values()]) this.end(c, CloseReason.SESSION_ENDING, false);
		this.log("tunnel.session_end", { reason: this.endReason, ms: this.opts.now() - this.started, ...this.stats });
	}

	private onFrame(frame: Frame): void {
		switch (frame.type) {
			case "open":
				return this.open(frame);
			case "data": {
				const c = this.conns.get(frame.conn);
				if (!c) return; // may race with our close
				if (c.state !== "open") {
					// Handshake gate: nothing goes to the peer before `opened`.
					this.end(c, CloseReason.PROTOCOL_ERROR, true);
					return;
				}
				this.write(c, frame.payload);
				return;
			}
			case "close": {
				const c = this.conns.get(frame.conn);
				if (c) this.end(c, CloseReason.NORMAL, false);
				return;
			}
			case "ping":
				return;
			default:
				// opened / failed / accept only flow gateway → client.
				this.log("tunnel.protocol_error", { code: `unexpected_${frame.type}` });
				return this.abort(SessionCloseCode.PROTOCOL_ERROR, "protocol_error");
		}
	}

	private open(frame: OpenFrame): void {
		this.stats.opens++;
		if (this.conns.has(frame.conn)) return this.fail(frame.conn, FailReason.CONN_IN_USE);
		if (this.slots >= this.opts.maxConnections) return this.fail(frame.conn, FailReason.TOO_MANY_CONNECTIONS);
		if (checkAddress(frame.address, this.opts.allowPrivate) !== "ok") return this.fail(frame.conn, FailReason.FORBIDDEN_ADDRESS);

		const c: Conn = { id: frame.conn, state: "opening", writes: Promise.resolve(), lastActivity: this.opts.now() };
		this.conns.set(c.id, c);
		this.slots++;
		const started = Date.now();
		void this.connect(c, frame).then(
			(rest) => {
				if (c.state === "closed") return; // the client gave up meanwhile
				c.state = "open";
				c.lastActivity = this.opts.now();
				this.stats.opened++;
				this.log("tunnel.opened", { ms: Date.now() - started });
				// The remote's peer-wire bytes that came with its handshake ride along.
				const frames: Frame[] = [{ type: "opened", conn: c.id, reserved: rest.reserved, peerId: rest.peerId }];
				if (rest.extra.length > 0) {
					frames.push({ type: "data", conn: c.id, payload: rest.extra });
					this.stats.bytesDown += rest.extra.length;
				}
				this.emit(frames);
				void this.pump(c);
			},
			(e: unknown) => {
				const reason = e instanceof OpenFailure ? e.reason : FailReason.INTERNAL_ERROR;
				if (c.state === "closed") return;
				c.state = "closed";
				this.conns.delete(c.id);
				// A connect still pending on the platform keeps blocking new
				// ones for ~20 s (OQ-03): keep its slot taken that long.
				const hold = reason === FailReason.TIMEOUT && !c.connected ? started + this.opts.failedConnectHoldMs - Date.now() : 0;
				this.release(c, hold);
				this.fail(c.id, reason, Date.now() - started);
			},
		);
	}

	/** Connect, write our handshake, and wait for a valid one from the remote. */
	private async connect(c: Conn, frame: OpenFrame): Promise<{ reserved: Uint8Array; peerId: Uint8Array; extra: Uint8Array }> {
		let socket: SocketLike;
		try {
			socket = this.opts.connect(frame.address);
		} catch {
			throw new OpenFailure(FailReason.REFUSED);
		}
		c.socket = socket;
		void socket.opened.then(
			() => (c.connected = true),
			() => {},
		);
		socket.closed.catch(() => {});
		await within(
			socket.opened.then(
				() => {},
				() => {
					throw new OpenFailure(FailReason.REFUSED);
				},
			),
			this.opts.connectTimeoutMs,
		);
		if (isClosed(c)) throw new OpenFailure(FailReason.INTERNAL_ERROR);
		c.writer = socket.writable.getWriter();
		c.reader = socket.readable.getReader();
		await c.writer.write(handshake(frame)).catch(() => {
			throw new OpenFailure(FailReason.REMOTE_CLOSED);
		});

		const deadline = Date.now() + this.opts.handshakeTimeoutMs;
		let buf: Uint8Array = new Uint8Array(0);
		while (buf.length < HANDSHAKE_LENGTH) {
			const { value, done } = await within(
				c.reader.read().catch(() => ({ value: undefined, done: true as const })),
				deadline - Date.now(),
			);
			if (done || !value) throw new OpenFailure(FailReason.REMOTE_CLOSED);
			buf = concat(buf, value);
			// Fail fast on anything that isn't a BitTorrent handshake (HTTP, SSH, ...).
			const prefix = Math.min(buf.length, PROTOCOL_PREFIX.length);
			for (let i = 0; i < prefix; i++) if (buf[i] !== PROTOCOL_PREFIX[i]) throw new OpenFailure(FailReason.NOT_BITTORRENT);
			if (isClosed(c)) throw new OpenFailure(FailReason.INTERNAL_ERROR);
		}
		for (let i = 0; i < 20; i++) if (buf[28 + i] !== frame.infoHash[i]) throw new OpenFailure(FailReason.INFOHASH_MISMATCH);
		return { reserved: buf.slice(20, 28), peerId: buf.slice(48, 68), extra: buf.slice(HANDSHAKE_LENGTH) };
	}

	/** Remote → client, one message per chunk read. */
	private async pump(c: Conn): Promise<void> {
		const reader = c.reader!;
		try {
			for (;;) {
				const { value, done } = await reader.read();
				if (c.state !== "open") return;
				if (done) break;
				if (value.length === 0) continue;
				this.stats.bytesDown += value.length;
				c.lastActivity = this.opts.now();
				this.emit([{ type: "data", conn: c.id, payload: value }]);
			}
			this.end(c, CloseReason.REMOTE_CLOSED, true);
		} catch {
			this.end(c, CloseReason.REMOTE_ERROR, true);
		}
	}

	/** Client → remote, writes kept in order. */
	private write(c: Conn, payload: Uint8Array): void {
		this.stats.bytesUp += payload.length;
		c.lastActivity = this.opts.now();
		const writer = c.writer!;
		c.writes = c.writes
			.then(() => writer.write(payload))
			.catch(() => {
				if (c.state === "open") this.end(c, CloseReason.REMOTE_ERROR, true);
			});
	}

	private end(c: Conn, reason: number, notify: boolean): void {
		if (c.state === "closed") return;
		const wasOpen = c.state === "open";
		c.state = "closed";
		if (this.conns.get(c.id) === c) this.conns.delete(c.id);
		if (notify) this.emit([{ type: "close", conn: c.id, reason }]);
		if (wasOpen) this.log("tunnel.closed", { reason: CLOSE_NAMES[reason] ?? String(reason) });
		this.release(c);
	}

	/** Close the socket; give the slot back once it has closed (and no sooner than `holdMs`). */
	private release(c: Conn, holdMs = 0): void {
		const socket = c.socket;
		if (!socket) {
			this.slots--;
			return;
		}
		const hold = holdMs > 0 ? new Promise<void>((r) => setTimeout(r, holdMs)) : Promise.resolve();
		void c.reader?.cancel().catch(() => {});
		let closed: Promise<unknown>;
		try {
			closed = socket.close().catch(() => {});
		} catch {
			closed = Promise.resolve();
		}
		void within(Promise.all([closed, socket.closed.catch(() => {})]), CLOSE_WAIT_MS)
			.catch(() => {})
			.then(() => hold)
			.then(() => {
				this.slots--;
			});
	}

	private fail(conn: number, reason: number, ms?: number): void {
		this.stats.failed++;
		this.log("tunnel.open_failed", { reason: FAIL_NAMES[reason] ?? String(reason), ...(ms === undefined ? {} : { ms }) });
		this.emit([{ type: "failed", conn, reason }]);
	}

	private emit(frames: Frame[]): void {
		if (this.ended) return;
		this.stats.messagesOut++;
		this.stats.framesOut += frames.length;
		try {
			this.io.send(encodeFrames(frames));
		} catch {
			this.onClose();
		}
	}

	private abort(code: number, reason: string): void {
		this.endReason = reason;
		try {
			this.io.close(code, this.closeReason(reason));
		} catch {}
		this.onClose();
	}

	private log(event: string, fields: LogFields): void {
		this.opts.log?.(event, fields);
	}
}

/** The 68-byte handshake the gateway writes on the client's behalf. */
export function handshake(frame: Pick<OpenFrame, "reserved" | "infoHash" | "peerId">): Uint8Array {
	const out = new Uint8Array(HANDSHAKE_LENGTH);
	out.set(PROTOCOL_PREFIX, 0);
	out.set(frame.reserved, 20);
	out.set(frame.infoHash, 28);
	out.set(frame.peerId, 48);
	return out;
}

class Timeout extends OpenFailure {
	constructor() {
		super(FailReason.TIMEOUT);
	}
}

function within<T>(promise: Promise<T>, ms: number): Promise<T> {
	if (ms <= 0) return Promise.reject(new Timeout());
	let timer: ReturnType<typeof setTimeout>;
	return Promise.race([
		promise,
		new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Timeout()), ms);
		}),
	]).finally(() => clearTimeout(timer));
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
	if (a.length === 0) return b;
	const out = new Uint8Array(a.length + b.length);
	out.set(a);
	out.set(b, a.length);
	return out;
}
