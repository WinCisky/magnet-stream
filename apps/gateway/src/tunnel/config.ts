// Request-level policy of the tunnel route: Origin allow-list, the
// local-only ALLOW_PRIVATE_PEERS switch, and the validated limits.

import {
	DEFAULT_IDLE_TIMEOUT_MS,
	DEFAULT_MAX_PENDING_WRITE_BYTES,
	DEFAULT_SESSION_TIMEOUT_S,
	DEFAULT_SOCKETS_PER_SESSION,
	FAILED_CONNECT_HOLD_MS,
	MAX_CONNECTIONS_PER_SOCKET,
} from "@magnet-stream/tunnel-protocol";

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
	 * Connections per WebSocket (default 6, the protocol rule). Raise only on
	 * a spike deployment (docs/spike-report.md, OQ-03); 1..16.
	 */
	TUNNEL_MAX_CONNECTIONS?: string;
	/** A connection, or a WebSocket, with no traffic for this long is closed, ms (default 90000, 1000..600000). */
	TUNNEL_IDLE_TIMEOUT_MS?: string;
	/** Maximum session length, seconds (default 7200, 5..86400). */
	TUNNEL_SESSION_TIMEOUT_S?: string;
	/** WebSockets per session object (default 3, 1..8). */
	TUNNEL_SOCKETS_PER_SESSION?: string;
	/**
	 * How long the slot of a connect that timed out stays taken, counted from
	 * the connect, ms (default 20000, 0..60000). The platform keeps such a
	 * connect pending for ~20 s and queues new ones behind it (OQ-03).
	 */
	TUNNEL_FAILED_CONNECT_HOLD_MS?: string;
	/** WebSocket upgrades per minute per client IP (default 30, 1..600). */
	TUNNEL_UPGRADES_PER_MINUTE?: string;
	/**
	 * Bytes held for one peer and not yet written to it; past this the peer
	 * is not reading and its connection is closed (`close` WRITE_OVERFLOW).
	 * Default 4194304 (4 MiB), 65536..268435456.
	 */
	TUNNEL_MAX_PENDING_WRITE_BYTES?: string;
}

export interface TunnelLimits {
	connectTimeoutMs: number;
	handshakeTimeoutMs: number;
	maxConnections: number;
	idleTimeoutMs: number;
	failedConnectHoldMs: number;
	maxPendingWriteBytes: number;
}

export interface TunnelConfig extends TunnelLimits {
	sessionTimeoutMs: number;
	socketsPerSession: number;
	upgradesPerMinute: number;
}

interface Rule {
	key: keyof TunnelEnv;
	min: number;
	max: number;
	fallback: number;
}

const RULES: Record<keyof Omit<TunnelConfig, "sessionTimeoutMs"> | "sessionTimeoutS", Rule> = {
	connectTimeoutMs: { key: "TUNNEL_CONNECT_TIMEOUT_MS", min: 100, max: 60_000, fallback: 5_000 },
	handshakeTimeoutMs: { key: "TUNNEL_HANDSHAKE_TIMEOUT_MS", min: 100, max: 60_000, fallback: 10_000 },
	maxConnections: { key: "TUNNEL_MAX_CONNECTIONS", min: 1, max: 16, fallback: MAX_CONNECTIONS_PER_SOCKET },
	idleTimeoutMs: { key: "TUNNEL_IDLE_TIMEOUT_MS", min: 1_000, max: 600_000, fallback: DEFAULT_IDLE_TIMEOUT_MS },
	failedConnectHoldMs: { key: "TUNNEL_FAILED_CONNECT_HOLD_MS", min: 0, max: 60_000, fallback: FAILED_CONNECT_HOLD_MS },
	sessionTimeoutS: { key: "TUNNEL_SESSION_TIMEOUT_S", min: 5, max: 86_400, fallback: DEFAULT_SESSION_TIMEOUT_S },
	socketsPerSession: { key: "TUNNEL_SOCKETS_PER_SESSION", min: 1, max: 8, fallback: DEFAULT_SOCKETS_PER_SESSION },
	upgradesPerMinute: { key: "TUNNEL_UPGRADES_PER_MINUTE", min: 1, max: 600, fallback: 30 },
	maxPendingWriteBytes: { key: "TUNNEL_MAX_PENDING_WRITE_BYTES", min: 64 * 1024, max: 256 * 1024 * 1024, fallback: DEFAULT_MAX_PENDING_WRITE_BYTES },
};

export type ConfigResult = { ok: true; config: TunnelConfig } | { ok: false; invalid: (keyof TunnelEnv)[] };

/**
 * Read and validate the tunnel limits. Unset (or empty) variables take their
 * default; a value that is not a plain integer within its range makes the
 * whole configuration invalid (the gateway then refuses every tunnel rather
 * than run with limits nobody asked for).
 */
export function parseTunnelConfig(env: TunnelEnv): ConfigResult {
	const invalid: (keyof TunnelEnv)[] = [];
	const read = (rule: Rule): number => {
		const raw = env[rule.key];
		if (raw === undefined || raw.trim() === "") return rule.fallback;
		const text = raw.trim();
		const n = /^\d{1,9}$/.test(text) ? Number(text) : NaN;
		if (!(n >= rule.min && n <= rule.max)) {
			invalid.push(rule.key);
			return rule.fallback;
		}
		return n;
	};
	const config: TunnelConfig = {
		connectTimeoutMs: read(RULES.connectTimeoutMs),
		handshakeTimeoutMs: read(RULES.handshakeTimeoutMs),
		maxConnections: read(RULES.maxConnections),
		idleTimeoutMs: read(RULES.idleTimeoutMs),
		failedConnectHoldMs: read(RULES.failedConnectHoldMs),
		sessionTimeoutMs: read(RULES.sessionTimeoutS) * 1000,
		socketsPerSession: read(RULES.socketsPerSession),
		upgradesPerMinute: read(RULES.upgradesPerMinute),
		maxPendingWriteBytes: read(RULES.maxPendingWriteBytes),
	};
	return invalid.length > 0 ? { ok: false, invalid } : { ok: true, config };
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
 * they are allowed, since Origin only guards against other sites driving a
 * visitor's browser; the per-IP upgrade limit applies to them too.
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

/** `?session=` names a session object shared by up to `socketsPerSession` WebSockets: 32 lowercase hex (128 random bits). */
export function parseSessionName(raw: string | null): string | null | undefined {
	if (raw === null) return null;
	return /^[0-9a-f]{32}$/.test(raw) ? raw : undefined;
}
