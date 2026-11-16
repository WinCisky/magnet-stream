// The tunnel's Durable Object: one instance per WebSocket session, holding
// that session's TCP sockets. SQLite-backed (the only kind the free plan
// offers) but it stores nothing: all state is in memory and ends with the
// WebSocket (docs/architecture.md §Storage and database).

import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";
import { SUBPROTOCOL, formatIp, type PeerAddress } from "@magnet-stream/tunnel-protocol";
import { allowPrivatePeers, tunnelLimits, type TunnelEnv } from "./config";
import { TunnelSession as Session, type LogFields, type SocketLike } from "./session";

export function hostnameFor(address: PeerAddress): string {
	const ip = formatIp(address.family, address.ip);
	// connect() needs IPv6 literals bracketed.
	return address.family === 6 ? `[${ip}]` : ip;
}

export function log(event: string, fields: LogFields = {}): void {
	console.log(JSON.stringify({ event, ...fields }));
}

export class TunnelSession extends DurableObject<TunnelEnv> {
	async fetch(request: Request): Promise<Response> {
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
				...tunnelLimits(this.env),
				log,
			},
		);
		server.addEventListener("message", (event) => session.onMessage(event.data as ArrayBuffer | string));
		server.addEventListener("close", () => {
			session.onClose();
			try {
				server.close(1000, "bye");
			} catch {}
		});
		server.addEventListener("error", () => session.onClose());
		log("tunnel.session_start");

		return new Response(null, { status: 101, webSocket: client, headers: { "sec-websocket-protocol": SUBPROTOCOL } });
	}
}
