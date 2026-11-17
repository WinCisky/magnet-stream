// Client side of the tunnel protocol (docs/tunnel-protocol.md): one
// WebSocket to a gateway, multiplexing up to 6 peer connections by `conn`
// id. Each opened connection is a Transport for PeerConnection.

import {
	CLIENT_CONN_MAX,
	CLIENT_CONN_MIN,
	CloseReason,
	FailReason,
	MAX_CONNECTIONS_PER_SOCKET,
	SUBPROTOCOL,
	decodeMessage,
	encodeFrames,
	parseAddress,
	type Frame,
	type PeerAddress,
} from "@magnet-stream/tunnel-protocol";
import { BufferedTransport, type HandshakeInfo } from "./transport.ts";

/** The part of the WebSocket API the client uses (browser, worker or Node). */
export interface WebSocketLike {
	binaryType: string;
	readonly readyState: number;
	readonly protocol: string;
	send(data: Uint8Array): void;
	close(code?: number, reason?: string): void;
	addEventListener(type: "open" | "close" | "error" | "message", listener: (event: { data?: unknown; code?: number }) => void): void;
}

export type WebSocketFactory = (url: string, protocols: string) => WebSocketLike;

export interface OpenRequest {
	/** Peer address, as PeerAddress or `ip:port` / `[ip6]:port`. */
	address: PeerAddress | string;
	infoHash: Uint8Array;
	/** Our peer id, written by the gateway in the handshake. */
	peerId: Uint8Array;
	/** Our reserved bytes (default: BEP 6 fast bit only). */
	reserved?: Uint8Array;
	/** Give up waiting for `opened` after this long (default 20 s; the gateway has its own timeouts). */
	timeoutMs?: number;
}

export interface TunnelStats {
	messagesSent: number;
	framesSent: number;
	messagesReceived: number;
	framesReceived: number;
	bytesSent: number;
	bytesReceived: number;
}

/** An `open` that produced `failed` (or no answer). */
export class TunnelOpenError extends Error {
	/** FailReason code, or 0 for a local timeout / closed session. */
	readonly reason: number;
	constructor(reason: number, message?: string) {
		super(message ?? `tunnel open failed: ${failReasonName(reason)}`);
		this.name = "TunnelOpenError";
		this.reason = reason;
	}
}

export class TunnelClosedError extends Error {
	readonly reason: number;
	constructor(reason: number) {
		super(`tunnel connection closed: ${closeReasonName(reason)}`);
		this.name = "TunnelClosedError";
		this.reason = reason;
	}
}

export function failReasonName(code: number): string {
	return Object.entries(FailReason).find(([, v]) => v === code)?.[0].toLowerCase() ?? `code_${code}`;
}

export function closeReasonName(code: number): string {
	return Object.entries(CloseReason).find(([, v]) => v === code)?.[0].toLowerCase() ?? `code_${code}`;
}

/** BEP 10 extension protocol and BEP 6 fast bits. */
export const DEFAULT_RESERVED = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
/** Largest `data` frame we send; bigger writes are split. */
const MAX_DATA_FRAME = 256 * 1024;
const WS_OPEN = 1;

interface Pending {
	resolve(t: TunnelTransport): void;
	reject(e: Error): void;
	timer: ReturnType<typeof setTimeout>;
	reserved: Uint8Array;
}

export class TunnelClient {
	readonly stats: TunnelStats = { messagesSent: 0, framesSent: 0, messagesReceived: 0, framesReceived: 0, bytesSent: 0, bytesReceived: 0 };
	private readonly ws: WebSocketLike;
	private readonly pending = new Map<number, Pending>();
	private readonly transports = new Map<number, TunnelTransport>();
	private nextConn = CLIENT_CONN_MIN;
	private ended: Error | undefined;
	/** Called once when the session ends. */
	onClose: ((error?: Error) => void) | undefined;

	/** Connections allowed at once (the protocol's 6; more only to probe a gateway, see tools/spike). */
	readonly maxConnections: number;

	/** Wrap an already open WebSocket. */
	constructor(ws: WebSocketLike, options: { maxConnections?: number } = {}) {
		this.ws = ws;
		this.maxConnections = options.maxConnections ?? MAX_CONNECTIONS_PER_SOCKET;
		ws.binaryType = "arraybuffer";
		ws.addEventListener("message", (e) => this.onMessage(e.data));
		ws.addEventListener("close", (e) => this.end(new Error(`tunnel WebSocket closed (${e.code ?? "?"})`)));
		ws.addEventListener("error", () => this.end(new Error("tunnel WebSocket error")));
	}

	/** Open a WebSocket to `url` (…/v1/tunnel) and wait until it is ready. */
	static connect(url: string, options: { factory?: WebSocketFactory; timeoutMs?: number; maxConnections?: number } = {}): Promise<TunnelClient> {
		const factory: WebSocketFactory = options.factory ?? ((u, p) => new WebSocket(u, p) as unknown as WebSocketLike);
		const ws = factory(url, SUBPROTOCOL);
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				ws.close();
				reject(new Error("tunnel WebSocket did not open in time"));
			}, options.timeoutMs ?? 10_000);
			ws.addEventListener("open", () => {
				clearTimeout(timer);
				if (ws.protocol !== SUBPROTOCOL) {
					ws.close(1002, "subprotocol");
					reject(new Error(`gateway does not speak ${SUBPROTOCOL}`));
					return;
				}
				resolve(new TunnelClient(ws, { maxConnections: options.maxConnections }));
			});
			ws.addEventListener("error", () => {
				clearTimeout(timer);
				reject(new Error("tunnel WebSocket failed to open"));
			});
		});
	}

	get closed(): boolean {
		return this.ended !== undefined;
	}

	/** Connections opening or open. */
	get connections(): number {
		return this.pending.size + this.transports.size;
	}

	/** Ask the gateway to connect to a peer; resolves once its handshake was checked. */
	open(request: OpenRequest): Promise<TunnelTransport> {
		if (this.ended) return Promise.reject(this.ended);
		const address = typeof request.address === "string" ? parseAddress(request.address) : request.address;
		if (!address) return Promise.reject(new TypeError("bad peer address"));
		const conn = this.allocateConn();
		if (conn === undefined) return Promise.reject(new TunnelOpenError(FailReason.TOO_MANY_CONNECTIONS));
		const reserved = request.reserved ?? DEFAULT_RESERVED;
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				if (!this.pending.delete(conn)) return;
				this.send([{ type: "close", conn, reason: CloseReason.NORMAL }]);
				reject(new TunnelOpenError(FailReason.TIMEOUT, "no answer from the gateway"));
			}, request.timeoutMs ?? 20_000);
			this.pending.set(conn, { resolve, reject, timer, reserved });
			this.send([{ type: "open", conn, infoHash: request.infoHash, reserved, peerId: request.peerId, address }]);
		});
	}

	/** Close the session and every connection. */
	close(): void {
		try {
			this.ws.close(1000, "done");
		} catch {}
		this.end();
	}

	/** @internal */
	send(frames: Frame[]): void {
		if (this.ended || this.ws.readyState !== WS_OPEN) return;
		const message = encodeFrames(frames);
		this.stats.messagesSent++;
		this.stats.framesSent += frames.length;
		this.stats.bytesSent += message.length;
		this.ws.send(message);
	}

	/** @internal A transport closed locally. */
	release(conn: number, notifyGateway: boolean): void {
		this.transports.delete(conn);
		if (notifyGateway) this.send([{ type: "close", conn, reason: CloseReason.NORMAL }]);
	}

	private allocateConn(): number | undefined {
		if (this.connections >= this.maxConnections) return undefined;
		for (let i = 0; i <= CLIENT_CONN_MAX - CLIENT_CONN_MIN; i++) {
			const conn = this.nextConn;
			this.nextConn = conn >= CLIENT_CONN_MAX ? CLIENT_CONN_MIN : conn + 1;
			if (!this.pending.has(conn) && !this.transports.has(conn)) return conn;
		}
		return undefined;
	}

	private onMessage(data: unknown): void {
		if (this.ended) return;
		if (!(data instanceof ArrayBuffer) && !ArrayBuffer.isView(data)) {
			this.fail(new Error("gateway sent a non-binary message"));
			return;
		}
		const bytes = ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data);
		this.stats.messagesReceived++;
		this.stats.bytesReceived += bytes.length;
		const decoded = decodeMessage(bytes);
		if (!decoded.ok) {
			this.fail(new Error(`gateway sent a malformed message: ${decoded.error.code}`));
			return;
		}
		this.stats.framesReceived += decoded.frames.length;
		for (const frame of decoded.frames) this.onFrame(frame);
	}

	private onFrame(frame: Frame): void {
		switch (frame.type) {
			case "opened": {
				const p = this.pending.get(frame.conn);
				if (!p) return;
				this.pending.delete(frame.conn);
				clearTimeout(p.timer);
				const t = new TunnelTransport(this, frame.conn, { reserved: frame.reserved.slice(), peerId: frame.peerId.slice() }, p.reserved);
				this.transports.set(frame.conn, t);
				p.resolve(t);
				return;
			}
			case "failed": {
				const p = this.pending.get(frame.conn);
				if (!p) return;
				this.pending.delete(frame.conn);
				clearTimeout(p.timer);
				p.reject(new TunnelOpenError(frame.reason));
				return;
			}
			case "data":
				// Copy: the payload is a view into the message, and consumers may keep it.
				this.transports.get(frame.conn)?.receive(frame.payload.slice());
				return;
			case "close": {
				const t = this.transports.get(frame.conn);
				this.transports.delete(frame.conn);
				t?.remoteClosed(frame.reason);
				// A close for a pending open: the gateway gave up on it.
				const p = this.pending.get(frame.conn);
				if (p) {
					this.pending.delete(frame.conn);
					clearTimeout(p.timer);
					p.reject(new TunnelOpenError(FailReason.INTERNAL_ERROR, `closed while opening: ${closeReasonName(frame.reason)}`));
				}
				return;
			}
			default:
				// ping needs no answer; accept is VPS-only (M4).
				return;
		}
	}

	private fail(error: Error): void {
		try {
			this.ws.close(1002, "protocol error");
		} catch {}
		this.end(error);
	}

	private end(error?: Error): void {
		if (this.ended) return;
		this.ended = error ?? new Error("tunnel closed");
		for (const p of this.pending.values()) {
			clearTimeout(p.timer);
			p.reject(new TunnelOpenError(0, "tunnel session ended"));
		}
		this.pending.clear();
		for (const t of this.transports.values()) t.sessionEnded(this.ended);
		this.transports.clear();
		this.onClose?.(error);
	}
}

/** One tunnelled peer connection, after the gateway's `opened`. */
export class TunnelTransport extends BufferedTransport {
	readonly remote: HandshakeInfo;
	readonly local: { reserved: Uint8Array };
	readonly conn: number;
	private readonly client: TunnelClient;

	constructor(client: TunnelClient, conn: number, remote: HandshakeInfo, localReserved: Uint8Array) {
		super();
		this.client = client;
		this.conn = conn;
		this.remote = remote;
		this.local = { reserved: localReserved };
	}

	send(bytes: Uint8Array): void {
		if (this.closed) return;
		const frames: Frame[] = [];
		for (let at = 0; at < bytes.length; at += MAX_DATA_FRAME) {
			frames.push({ type: "data", conn: this.conn, payload: bytes.subarray(at, at + MAX_DATA_FRAME) });
		}
		if (frames.length > 0) this.client.send(frames);
	}

	close(): void {
		if (this.closed) return;
		this.client.release(this.conn, true);
		this.finish();
	}

	/** @internal */
	receive(bytes: Uint8Array): void {
		this.deliver(bytes);
	}

	/** @internal */
	remoteClosed(reason: number): void {
		this.finish(reason === CloseReason.NORMAL || reason === CloseReason.REMOTE_CLOSED ? undefined : new TunnelClosedError(reason));
	}

	/** @internal */
	sessionEnded(error: Error): void {
		this.finish(error);
	}
}
