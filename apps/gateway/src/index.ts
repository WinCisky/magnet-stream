// Tunnel gateway: a WebSocket to a Durable Object that dials BitTorrent
// peers and relays bytes. The browser is the client; this worker does not
// parse peer-wire messages or store anything.

import { SUBPROTOCOL } from "@magnet-stream/tunnel-protocol";
import { parseSessionName, parseTunnelConfig, type TunnelEnv } from "./tunnel/config";
import { TunnelSession, log } from "./tunnel/durable-object";
import { allowUpgrade, clientKey, type RateLimiterBinding } from "./tunnel/rate-limit";

export { TunnelSession };

export interface Env extends TunnelEnv {
	/** Comma-separated origins allowed to open tunnels. */
	ALLOWED_ORIGINS?: string;
	/** One Durable Object per tunnel session (docs/tunnel-protocol.md). */
	TUNNEL: DurableObjectNamespace<TunnelSession>;
	/** Per-IP limit on tunnel upgrades (wrangler.jsonc `ratelimits`). */
	UPGRADE_LIMITER?: RateLimiterBinding;
}

export default {
	async fetch(request, env): Promise<Response> {
		const url = new URL(request.url);
		const cors = corsHeaders(request, env);
		if (cors === null) return json(403, { error: "origin_not_allowed" }, {});

		if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
		if (request.method !== "GET") return json(405, { error: "method_not_allowed" }, cors);

		if (url.pathname === "/health") return new Response("ok", { headers: cors });
		// Origin was checked above: a foreign one got its 403 before any upgrade.
		if (url.pathname === "/v1/tunnel") return tunnel(request, env, url);
		return json(404, { error: "not_found" }, cors);
	},
} satisfies ExportedHandler<Env>;

/**
 * Hand a WebSocket upgrade to its session's Durable Object: a fresh one, or
 * with `?session=<32 hex>` the one shared by that session's WebSockets.
 * Everything that can be refused is refused here, before any object exists.
 */
async function tunnel(request: Request, env: Env, url: URL): Promise<Response> {
	if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
		return json(426, { error: "websocket_required" }, { upgrade: "websocket" });
	}
	const offered = (request.headers.get("sec-websocket-protocol") ?? "").split(",").map((p) => p.trim());
	if (!offered.includes(SUBPROTOCOL)) {
		log("tunnel.rejected", { why: "subprotocol" });
		return json(400, { error: "unsupported_protocol", expected: SUBPROTOCOL }, {});
	}
	const name = parseSessionName(url.searchParams.get("session"));
	if (name === undefined) return json(400, { error: "bad_session" }, {});
	const parsed = parseTunnelConfig(env);
	if (!parsed.ok) {
		// Names of the offending variables only, never their values.
		log("tunnel.config_invalid", { vars: parsed.invalid.join(",") });
		return json(500, { error: "gateway_misconfigured" }, {});
	}
	if (!(await allowUpgrade(env.UPGRADE_LIMITER, clientKey(request), parsed.config.upgradesPerMinute))) {
		log("tunnel.rejected", { why: "rate_limited" });
		return json(429, { error: "rate_limited" }, { "retry-after": "60" });
	}
	const id = name === null ? env.TUNNEL.newUniqueId() : env.TUNNEL.idFromName(name);
	return env.TUNNEL.get(id).fetch(request);
}

/** CORS headers for an allowed origin; `{}` without Origin; `null` if it is not allowed. */
function corsHeaders(request: Request, env: Env): Record<string, string> | null {
	const origin = request.headers.get("origin");
	if (origin === null) return {};
	const allowed = (env.ALLOWED_ORIGINS ?? "")
		.split(",")
		.map((o) => o.trim())
		.filter(Boolean);
	if (!allowed.includes("*") && !allowed.includes(origin)) return null;
	return {
		"access-control-allow-origin": origin,
		"access-control-allow-methods": "GET, OPTIONS",
		"access-control-max-age": "86400",
		vary: "Origin",
	};
}

function json(status: number, body: unknown, headers: Record<string, string>): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { ...headers, "content-type": "application/json", "cache-control": "no-store" },
	});
}
