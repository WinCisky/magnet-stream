// Client side of the tunnel protocol (docs/tunnel-protocol.md): one
// WebSocket to a gateway, multiplexing up to 6 peer connections by `conn`
// id. Each opened connection is a Transport for PeerConnection.
//
// On the VPS gateway the client may also `announce` an info-hash: the
// gateway joins its swarm and hands over inbound peers (`accept`). An
// accepted connection is a Transport like an opened one (the peer started
// the handshake; the gateway answered with our reserved bytes and peer id)
// and does not take one of the 6 outbound slots.

import {
	AnnounceStatus,
	CLIENT_CONN_MAX,
	CLIENT_CONN_MIN,
	CLOSE_SLOT_GRACE_MS,
	CloseReason,
	FAILED_CONNECT_HOLD_MS,
	FailReason,
	GATEWAY_CONN_MIN,
	MAX_ANNOUNCE_TRACKERS,
	MAX_CONNECTIONS_PER_SOCKET,
	SUBPROTOCOL,
	Transport,
	decodeMessage,
	encodeFrames,
	formatAddress,
	isTrackerUrlText,
	parseAddress,
	type Frame,
	type PeerAddress,
} from "@magnet-stream/tunnel-protocol";
import { BufferedTransport, type HandshakeInfo } from "./transport.ts";
import { UpstreamBatcher, type FlushMode } from "./upstream.ts";

export { AnnounceStatus, type AnnounceStatusCode } from "@magnet-stream/tunnel-protocol";

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
	/**
	 * Transport the gateway dials with (Transport.TCP by default; uTP only on
	 * the VPS gateway while this session announces the info-hash, else the
	 * open fails with UNSUPPORTED_TRANSPORT). The engine dials uTP only for
	 * a hole punch (M4-05, holepunch.ts).
	 */
	transport?: number;
	/** Send the `open` at once instead of with the next batch (a hole punch: the remote's SYN is on its way). */
	urgent?: boolean;
}

export interface AnnounceRequest {
	infoHash: Uint8Array;
	/** Our peer id: the gateway answers inbound handshakes with it. */
	peerId: Uint8Array;
	/** Our reserved bytes (default: BEP 6 fast bit and BEP 10). */
	reserved?: Uint8Array;
	/** The magnet's trackers (http, https, udp); others and the excess are left out. */
	trackers?: readonly string[];
	/** Give up waiting for `announced` after this long (default 10 s). */
	timeoutMs?: number;
}

/** An inbound peer the gateway accepted for an info-hash we announce. */
export interface AcceptedPeer {
	transport: TunnelTransport;
	infoHash: Uint8Array;
	/** `ip:port` the peer connected from (not a port it listens on). */
	address: string;
	/** How it reached the gateway: Transport.TCP or Transport.UTP. */
	peerTransport: number;
}

export interface TunnelStats {
	messagesSent: number;
	framesSent: number;
	messagesReceived: number;
	framesReceived: number;
	bytesSent: number;
	bytesReceived: number;
	/** Messages that carried only lazy frames (upload, `have`s): what the upload cost in messages. */
	lazyMessages: number;
}

export interface TunnelClientOptions {
	maxConnections?: number;
	/**
	 * Coalesce outbound frames into one message per interval (default off,
	 * so existing callers stay synchronous). The connection manager uses 400.
	 */
	batchMs?: number;
	/** With batching: lazy frames (upload) alone are flushed at most this often (default 2000). */
	lazyMs?: number;
	/** Clock for the held slots (tests). */
	now?: () => number;
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
	startedAt: number;
}

export class TunnelClient {
	readonly stats: TunnelStats = { messagesSent: 0, framesSent: 0, messagesReceived: 0, framesReceived: 0, bytesSent: 0, bytesReceived: 0, lazyMessages: 0 };
	private readonly ws: WebSocketLike;
	private readonly pending = new Map<number, Pending>();
	private readonly transports = new Map<number, TunnelTransport>();
	/**
	 * Slots the gateway still counts as taken although no connection uses
	 * them: a connect that timed out (until FAILED_CONNECT_HOLD_MS after it
	 * started) and a socket that is closing (CLOSE_SLOT_GRACE_MS). Expiry times.
	 */
	private held: number[] = [];
	private readonly now: () => number;
	private nextConn = CLIENT_CONN_MIN;
	private ended: Error | undefined;
	private readonly batcher: UpstreamBatcher | undefined;
	/** Called once when the session ends. */
	onClose: ((error?: Error) => void) | undefined;
	/** An inbound peer (VPS gateway). Without a handler it is closed. */
	onAccept: ((peer: AcceptedPeer) => void) | undefined;
	/** The gateway changed the state of an announce (e.g. stopped it). */
	onAnnounced: ((infoHash: Uint8Array, status: number) => void) | undefined;
	/** Info-hashes (hex) we announce, with the reserved bytes we announced. */
	private readonly announces = new Map<string, Uint8Array>();
	/** Info-hashes (hex) the gateway answered ACTIVE for (and still announces). */
	private readonly active = new Set<string>();
	private readonly announceWaits = new Map<string, { resolve(status: number): void; reject(e: Error): void; timer: ReturnType<typeof setTimeout> }[]>();

	/** Connections allowed at once (the protocol's 6; more only to probe a gateway, see tools/spike). */
	readonly maxConnections: number;

	/** Wrap an already open WebSocket. */
	constructor(ws: WebSocketLike, options: TunnelClientOptions = {}) {
		this.ws = ws;
		this.now = options.now ?? Date.now;
		this.maxConnections = options.maxConnections ?? MAX_CONNECTIONS_PER_SOCKET;
		if (options.batchMs !== undefined && options.batchMs > 0) {
			this.batcher = new UpstreamBatcher({ intervalMs: options.batchMs, lazyMs: options.lazyMs, send: (frames) => this.writeNow(frames) });
		}
		ws.binaryType = "arraybuffer";
		ws.addEventListener("message", (e) => this.onMessage(e.data));
		ws.addEventListener("close", (e) => this.end(new Error(`tunnel WebSocket closed (${e.code ?? "?"})`)));
		ws.addEventListener("error", () => this.end(new Error("tunnel WebSocket error")));
	}

	/** Open a WebSocket to `url` (…/v1/tunnel) and wait until it is ready. */
	static connect(url: string, options: { factory?: WebSocketFactory; timeoutMs?: number } & TunnelClientOptions = {}): Promise<TunnelClient> {
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
				resolve(new TunnelClient(ws, options));
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

	/** Outbound slots taken at the gateway: connections opening or open, and slots it still holds. */
	get connections(): number {
		return this.pending.size + this.transports.size - this.acceptedCount() + this.heldSlots();
	}

	/** Inbound connections the gateway handed over (`accept`), open now. */
	get accepted(): number {
		return this.acceptedCount();
	}

	/** Info-hashes announced on this socket. */
	get announcing(): number {
		return this.announces.size;
	}

	/**
	 * The gateway announces `infoHash` for this socket (it answered ACTIVE):
	 * the session's uTP `open`s for it are dialled (VPS gateway).
	 */
	isAnnounced(infoHash: Uint8Array): boolean {
		return !this.ended && this.active.has(hex(infoHash));
	}

	private acceptedCount(): number {
		let n = 0;
		for (const conn of this.transports.keys()) if (conn >= GATEWAY_CONN_MIN) n++;
		return n;
	}

	/**
	 * Ask the gateway to join the swarm of `infoHash` and hand inbound peers
	 * to `onAccept`. Resolves with the gateway's AnnounceStatus (ACTIVE, or
	 * UNSUPPORTED on a gateway without inbound peers, LIMIT_REACHED, ...).
	 */
	announce(request: AnnounceRequest): Promise<number> {
		if (this.ended) return Promise.reject(this.ended);
		if (request.infoHash.length !== 20 || request.peerId.length !== 20) return Promise.reject(new RangeError("bad info-hash or peer id"));
		const reserved = request.reserved ?? DEFAULT_RESERVED;
		const trackers = (request.trackers ?? [])
			.filter((t) => isTrackerUrlText(t) && /^(https?|udp):\/\//i.test(t))
			.slice(0, MAX_ANNOUNCE_TRACKERS);
		const key = hex(request.infoHash);
		this.announces.set(key, reserved.slice());
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				const list = this.announceWaits.get(key);
				const i = list?.findIndex((w) => w.timer === timer) ?? -1;
				if (list && i >= 0) list.splice(i, 1);
				reject(new Error("no answer to announce"));
			}, request.timeoutMs ?? 10_000);
			const list = this.announceWaits.get(key) ?? [];
			list.push({ resolve, reject, timer });
			this.announceWaits.set(key, list);
			this.send([{ type: "announce", conn: 0, infoHash: request.infoHash, reserved, peerId: request.peerId, trackers }], true);
		});
	}

	/** Stop the announce of `infoHash` (inbound peers already accepted stay). */
	withdraw(infoHash: Uint8Array): void {
		this.active.delete(hex(infoHash));
		if (!this.announces.delete(hex(infoHash))) return;
		this.send([{ type: "withdraw", conn: 0, infoHash }], true);
	}

	/** Slots held after a failed connect or a close (see `held`). */
	heldSlots(now = this.now()): number {
		if (this.held.length > 0) this.held = this.held.filter((until) => until > now);
		return this.held.length;
	}

	private hold(until: number): void {
		this.held.push(until);
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
				const p = this.pending.get(conn);
				if (!p || !this.pending.delete(conn)) return;
				this.hold(p.startedAt + FAILED_CONNECT_HOLD_MS + CLOSE_SLOT_GRACE_MS);
				this.send([{ type: "close", conn, reason: CloseReason.NORMAL }]);
				reject(new TunnelOpenError(FailReason.TIMEOUT, "no answer from the gateway"));
			}, request.timeoutMs ?? 20_000);
			this.pending.set(conn, { resolve, reject, timer, reserved, startedAt: this.now() });
			const transport = request.transport ?? Transport.TCP;
			this.send([{ type: "open", conn, infoHash: request.infoHash, reserved, peerId: request.peerId, address, ...(transport === Transport.TCP ? {} : { transport }) }], request.urgent === true);
		});
	}

	/** Close the session and every connection. */
	close(): void {
		try {
			this.ws.close(1000, "done");
		} catch {}
		this.end();
	}

	/** @internal Queue frames. `urgent` flushes the batch immediately; `lazy` waits for the next message (or 2 s). */
	send(frames: Frame[], mode: FlushMode | boolean = "normal"): void {
		if (this.ended || this.ws.readyState !== WS_OPEN) return;
		if (this.batcher) {
			this.batcher.enqueue(frames, mode);
			this.stats.lazyMessages = this.batcher.lazyMessages;
		} else this.writeNow(frames);
	}

	/** A ping if this socket has been quiet for `interval` (no-op while a batch is waiting). */
	pingIfIdle(now: number, interval: number): boolean {
		if (this.ended) return false;
		return this.batcher?.pingIfIdle(now, interval) ?? false;
	}

	private writeNow(frames: Frame[]): void {
		if (this.ended || this.ws.readyState !== WS_OPEN || frames.length === 0) return;
		if (this.batcher) this.stats.lazyMessages = this.batcher.lazyMessages;
		const message = encodeFrames(frames);
		this.stats.messagesSent++;
		this.stats.framesSent += frames.length;
		this.stats.bytesSent += message.length;
		this.ws.send(message);
	}

	/** @internal A transport closed locally. The close is urgent: the slot must free before the next dial. */
	release(conn: number, notifyGateway: boolean): void {
		if (this.transports.delete(conn) && conn < GATEWAY_CONN_MIN) this.hold(this.now() + CLOSE_SLOT_GRACE_MS);
		if (notifyGateway) this.send([{ type: "close", conn, reason: CloseReason.NORMAL }], true);
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
				// Mirror the gateway: a connect that timed out keeps its slot (OQ-03),
				// counted from when the gateway started it (a little after we sent
				// the open: hence the grace); any other failure frees the slot once
				// the gateway's socket has closed. A refusal for lack of slots means
				// we counted wrong, so back off briefly.
				if (frame.reason === FailReason.TIMEOUT) this.hold(p.startedAt + FAILED_CONNECT_HOLD_MS + CLOSE_SLOT_GRACE_MS);
				else if (frame.reason === FailReason.TOO_MANY_CONNECTIONS) this.hold(this.now() + 2 * CLOSE_SLOT_GRACE_MS);
				else this.hold(this.now() + CLOSE_SLOT_GRACE_MS);
				p.reject(new TunnelOpenError(frame.reason));
				return;
			}
			case "data":
				// Copy: the payload is a view into the message, and consumers may keep it.
				this.transports.get(frame.conn)?.receive(frame.payload.slice());
				return;
			case "close": {
				const t = this.transports.get(frame.conn);
				if (this.transports.delete(frame.conn) && frame.conn < GATEWAY_CONN_MIN) this.hold(this.now() + CLOSE_SLOT_GRACE_MS);
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
			case "accept":
				return this.onAcceptFrame(frame.conn, frame.infoHash, frame.reserved, frame.peerId, frame.address, frame.transport ?? Transport.TCP);
			case "announced": {
				const key = hex(frame.infoHash);
				if (frame.status !== AnnounceStatus.ACTIVE) {
					this.announces.delete(key);
					this.active.delete(key);
				} else if (this.announces.has(key)) this.active.add(key);
				const waits = this.announceWaits.get(key);
				const wait = waits?.shift();
				if (waits && waits.length === 0) this.announceWaits.delete(key);
				if (wait) {
					clearTimeout(wait.timer);
					wait.resolve(frame.status);
				} else this.onAnnounced?.(frame.infoHash.slice(), frame.status);
				return;
			}
			default:
				// ping needs no answer; open/announce/withdraw never come from a gateway.
				return;
		}
	}

	/** An inbound peer: kept only for an info-hash we announce, a free id and a transport we know. */
	private onAcceptFrame(conn: number, infoHash: Uint8Array, reserved: Uint8Array, peerId: Uint8Array, address: PeerAddress, peerTransport: number): void {
		const local = this.announces.get(hex(infoHash));
		const known = peerTransport === Transport.TCP || peerTransport === Transport.UTP;
		if (!local || !known || this.transports.has(conn) || !this.onAccept) {
			this.send([{ type: "close", conn, reason: CloseReason.NORMAL }], true);
			return;
		}
		const t = new TunnelTransport(this, conn, { reserved: reserved.slice(), peerId: peerId.slice() }, local);
		this.transports.set(conn, t);
		this.onAccept({ transport: t, infoHash: infoHash.slice(), address: formatAddress(address), peerTransport });
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
		this.batcher?.cancel();
		for (const p of this.pending.values()) {
			clearTimeout(p.timer);
			p.reject(new TunnelOpenError(0, "tunnel session ended"));
		}
		this.pending.clear();
		for (const t of this.transports.values()) t.sessionEnded(this.ended);
		this.transports.clear();
		this.announces.clear();
		this.active.clear();
		for (const waits of this.announceWaits.values()) {
			for (const w of waits) {
				clearTimeout(w.timer);
				w.reject(this.ended);
			}
		}
		this.announceWaits.clear();
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
		this.write(bytes, "normal");
	}

	sendUrgent(bytes: Uint8Array): void {
		this.write(bytes, "urgent");
	}

	sendLazy(bytes: Uint8Array): void {
		this.write(bytes, "lazy");
	}

	private write(bytes: Uint8Array, mode: FlushMode): void {
		if (this.closed) return;
		const frames: Frame[] = [];
		for (let at = 0; at < bytes.length; at += MAX_DATA_FRAME) {
			frames.push({ type: "data", conn: this.conn, payload: bytes.subarray(at, at + MAX_DATA_FRAME) });
		}
		if (frames.length > 0) this.client.send(frames, mode);
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

function hex(bytes: Uint8Array): string {
	return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}
