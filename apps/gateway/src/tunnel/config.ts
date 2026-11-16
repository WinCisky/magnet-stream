// Request-level policy of the tunnel route: Origin allow-list and the
// local-only ALLOW_PRIVATE_PEERS switch.

import { MAX_CONNECTIONS_PER_SOCKET } from "@magnet-stream/tunnel-protocol";

export interface TunnelEnv {
	/** Comma-separated origins allowed to open tunnels (and read CORS responses). */
	ALLOWED_ORIGINS?: string;
	/**
	 * "1" lets tunnels reach private/loopback addresses, for tests against
	 * local fake peers. Never set in wrangler.jsonc: only in .dev.vars or
	 * `--var` for `wrangler dev`, and honoured only for requests addressed
	 * to a loopback host (see allowPrivatePeers).
	 */
	ALLOW_PRIVATE_PEERS?: string;
	/** TCP connect timeout for `open`, ms (default 5000, 100..60000). */
	TUNNEL_CONNECT_TIMEOUT_MS?: string;
	/** Wait for the remote's handshake, ms (default 10000, 100..60000). */
	TUNNEL_HANDSHAKE_TIMEOUT_MS?: string;
	/**
	 * Connections per WebSocket (default 6, the protocol rule). Only for the
	 * Phase 0 spike (docs/spike-report.md), to observe what the platform
	 * does past 6 (OQ-03); 1..16.
	 */
	TUNNEL_MAX_CONNECTIONS?: string;
}

export interface TunnelLimits {
	connectTimeoutMs: number;
	handshakeTimeoutMs: number;
	maxConnections: number;
}

export function tunnelLimits(env: TunnelEnv): TunnelLimits {
	const max = env.TUNNEL_MAX_CONNECTIONS;
	return {
		connectTimeoutMs: boundedMs(env.TUNNEL_CONNECT_TIMEOUT_MS, 5_000),
		handshakeTimeoutMs: boundedMs(env.TUNNEL_HANDSHAKE_TIMEOUT_MS, 10_000),
		maxConnections: max !== undefined && /^\d{1,2}$/.test(max) ? Math.min(16, Math.max(1, Number(max))) : MAX_CONNECTIONS_PER_SOCKET,
	};
}

function boundedMs(raw: string | undefined, fallback: number): number {
	if (raw === undefined || !/^\d{1,6}$/.test(raw)) return fallback;
	return Math.min(60_000, Math.max(100, Number(raw)));
}

export function allowedOrigins(env: TunnelEnv): string[] {
	return (env.ALLOWED_ORIGINS ?? "")
		.split(",")
		.map((o) => o.trim())
		.filter(Boolean);
}

/**
 * Whether a request's Origin may use the tunnel. Requests without an Origin
 * are not from a web page (browsers always send it on WebSocket upgrades):
 * they are allowed, like the PoC's other routes, since Origin only guards
 * against other sites driving a visitor's browser.
 */
export function originAllowed(origin: string | null, env: TunnelEnv): boolean {
	if (origin === null) return true;
	const allowed = allowedOrigins(env);
	return allowed.includes("*") || allowed.includes(origin);
}

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);

/**
 * ALLOW_PRIVATE_PEERS takes effect only when it is "1" AND the request was
 * addressed to a loopback host, i.e. under `wrangler dev`. A deployed
 * worker is reached through a public hostname, so even a misconfigured
 * variable cannot open the private network.
 */
export function allowPrivatePeers(env: TunnelEnv, url: URL): boolean {
	return env.ALLOW_PRIVATE_PEERS === "1" && LOOPBACK_HOSTS.has(url.hostname);
}
