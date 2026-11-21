// The tunnel's Durable Object: one instance per session, holding up to
// `socketsPerSession` WebSockets (6 TCP connections each). SQLite-backed
// (the only kind the free plan offers) but it stores nothing: all state is
// in memory and ends with the WebSockets (docs/architecture.md §Storage and
// database). If every WebSocket closes and the object is evicted, the
// session is over; a later WebSocket with the same name starts a new one.

import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";
import { CloseReason, SUBPROTOCOL, SessionCloseCode, formatIp, type PeerAddress } from "@magnet-stream/tunnel-protocol";
import { allowPrivatePeers, parseTunnelConfig, type TunnelConfig, type TunnelEnv } from "./config";
import { TunnelSession as Session, type LogFields, type SocketLike } from "./session";

/** How often idle connections and the session length are checked. */
const SWEEP_MS = 1_000;

export function hostnameFor(address: PeerAddress): string {
	const ip = formatIp(address.family, address.ip);
	// connect() needs IPv6 literals bracketed.
	return address.family === 6 ? `[${ip}]` : ip;
}

export function log(event: string, fields: LogFields = {}): void {
	console.log(JSON.stringify({ event, ...fields }));
}

export class TunnelSession extends DurableObject<TunnelEnv> {
	private readonly sockets = new Set<Session>();
	/** When the first WebSocket of this session arrived. */
	private started: number | null = null;
	private timer: ReturnType<typeof setInterval> | null = null;
	private expired = false;

	async fetch(request: Request): Promise<Response> {
		const parsed = parseTunnelConfig(this.env);
		if (!parsed.ok) return json(500, { error: "gateway_misconfigured" });
		const config = parsed.config;
		const now = Date.now();
		this.started ??= now;
		if (this.expired || now - this.started >= config.sessionTimeoutMs) {
			log("tunnel.rejected", { why: "session_expired" });
			return json(410, { error: "session_expired" });
		}
		if (this.sockets.size >= config.socketsPerSession) {
			log("tunnel.rejected", { why: "session_full" });
			return json(429, { error: "session_full" });
		}

		const pair = new WebSocketPair();
		const client = pair[0];
		const server = pair[1];
		// Binary messages as ArrayBuffer (the standard default is Blob, which
		// would need async reads and could reorder frames).
		server.binaryType = "arraybuffer";
		server.accept();

		const session = new Session(
			{
				send: (message) => server.send(message),
				close: (code, reason) => server.close(code, reason),
			},
			{
				connect: (address) => connect({ hostname: hostnameFor(address), port: address.port }) as unknown as SocketLike,
				allowPrivate: allowPrivatePeers(this.env, new URL(request.url)),
				connectTimeoutMs: config.connectTimeoutMs,
				handshakeTimeoutMs: config.handshakeTimeoutMs,
				maxConnections: config.maxConnections,
				idleTimeoutMs: config.idleTimeoutMs,
				failedConnectHoldMs: config.failedConnectHoldMs,
				maxPendingWriteBytes: config.maxPendingWriteBytes,
				log,
			},
		);
		this.sockets.add(session);
		const ended = () => {
			session.onClose();
			this.sockets.delete(session);
			if (this.sockets.size === 0) this.stopTimer();
		};
		server.addEventListener("message", (event) => session.onMessage(event.data as ArrayBuffer | string));
		server.addEventListener("close", () => {
			// Answer the client's close with the session's counters.
			const reason = session.closeReason(session.endReason ?? "normal");
			ended();
			try {
				server.close(SessionCloseCode.NORMAL, reason);
			} catch {}
		});
		server.addEventListener("error", ended);
		this.startTimer(config);
		log("tunnel.session_start", { socket: this.sockets.size });

		return new Response(null, { status: 101, webSocket: client, headers: { "sec-websocket-protocol": SUBPROTOCOL } });
	}

	private startTimer(config: TunnelConfig): void {
		if (this.timer !== null) return;
		this.timer = setInterval(() => this.sweep(config), SWEEP_MS);
	}

	private stopTimer(): void {
		if (this.timer !== null) clearInterval(this.timer);
		this.timer = null;
	}

	private sweep(config: TunnelConfig): void {
		const timedOut = this.started !== null && Date.now() - this.started >= config.sessionTimeoutMs;
		if (timedOut) this.expired = true;
		for (const s of [...this.sockets]) {
			if (timedOut) s.shutdown("session_timeout", SessionCloseCode.SESSION_TIMEOUT, CloseReason.SESSION_TIMEOUT);
			else s.sweep();
			if (s.isEnded) this.sockets.delete(s);
		}
		if (this.sockets.size === 0) this.stopTimer();
	}
}

function json(status: number, body: unknown): Response {
	return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "cache-control": "no-store" } });
}
