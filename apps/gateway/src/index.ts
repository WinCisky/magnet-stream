// magnet-worker: fetches unverified torrent blocks from peers for the
// browser, which can't open TCP connections itself.
//
// GET /v1/blocks races up to 40 candidate peers (6 connections at a time),
// waits for one to unchoke us, sends it the block requests, then pipes the
// peer's raw bytes straight into the response. That pipe runs natively (no JavaScript per chunk), so
// CPU time per request stays roughly constant however much data flows,
// which is what keeps this viable on the Workers free plan. The browser
// parses the peer-wire stream and checks every piece's SHA-1 itself.

import { BLOCK_SIZE, MAX_CANDIDATES, ParamError, formatWant, parseBlocksRequest, type BlocksRequest, type Candidate } from "./params";
import { MAX_OPEN, RaceLost, close, concat, race, type Outcome } from "./peer";
import { verifyToken } from "./token";
import { requests } from "./wire";
import { SUBPROTOCOL } from "@magnet-stream/tunnel-protocol";
import { parseSessionName, parseTunnelConfig, type TunnelEnv } from "./tunnel/config";
import { TunnelSession, log } from "./tunnel/durable-object";
import { allowUpgrade, clientKey, type RateLimiterBinding } from "./tunnel/rate-limit";

export { TunnelSession };

export interface Env extends TunnelEnv {
	/** Shared with magnet-seeders, which signs the peer tokens. */
	TOKEN_SECRET?: string;
	/** Comma-separated origins allowed to read responses (and open tunnels). */
	ALLOWED_ORIGINS?: string;
	/** One Durable Object per tunnel WebSocket (docs/tunnel-protocol.md). */
	TUNNEL: DurableObjectNamespace<TunnelSession>;
	/** Per-IP limit on tunnel upgrades (wrangler.jsonc `ratelimits`). */
	UPGRADE_LIMITER?: RateLimiterBinding;
}

/**
 * A response lasts as long as sending its blocks takes at this rate (within
 * the bounds below). The browser normally hangs up much sooner, but a
 * hang-up isn't always noticed while the peer has nothing left to send;
 * this bounds how long the peer's connection slot stays taken.
 */
const MIN_RATE_BYTES_PER_MS = 32; // 32 KB/s
const MIN_STREAM_MS = 20_000;
const MAX_STREAM_MS = 120_000;

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);
		const cors = corsHeaders(request, env);
		if (cors === null) return json(403, { error: "origin_not_allowed" }, {});

		if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
		if (request.method !== "GET") return json(405, { error: "method_not_allowed" }, cors);

		if (url.pathname === "/health") return new Response("ok", { headers: cors });
		// Origin was checked above: a foreign one got its 403 before any upgrade.
		if (url.pathname === "/v1/tunnel") return tunnel(request, env, url);
		// What this worker supports, so clients can adapt to older versions.
		if (url.pathname === "/v1/info") {
			return json(200, { v: 2, maxCandidates: MAX_CANDIDATES, maxOpen: MAX_OPEN }, cors);
		}
		if (url.pathname === "/v1/blocks") {
			try {
				return await blocks(url, request.signal, env, ctx, cors);
			} catch (e) {
				if (e instanceof ParamError) return json(400, { error: "bad_request", detail: e.message }, cors);
				throw e;
			}
		}
		return json(404, { error: "not_found" }, cors);
	},
} satisfies ExportedHandler<Env>;

async function blocks(
	url: URL,
	disconnected: AbortSignal,
	env: Env,
	ctx: ExecutionContext,
	cors: Record<string, string>,
): Promise<Response> {
	const req = parseBlocksRequest(url);
	if (!env.TOKEN_SECRET) return json(500, { error: "worker_misconfigured" }, cors);

	const [allowed, rejected] = await checkTokens(req, env.TOKEN_SECRET);
	if (allowed.length === 0) return json(403, { error: "bad_token", others: rejected }, cors);

	let result;
	try {
		result = await race(req, allowed, { deadline: Date.now() + req.waitMs, holdMs: req.holdMs, staggerMs: req.staggerMs });
	} catch (e) {
		if (e instanceof RaceLost) {
			return json(502, { error: "no_peer", others: [...e.outcomes, ...rejected] }, cors);
		}
		throw e;
	}
	const { winner, blocks, outcomes } = result;

	await winner.writer.write(requests(blocks));
	// Release, don't close: closing our write side would send a FIN, and
	// some clients hang up on that before serving the requests.
	winner.writer.releaseLock();
	winner.reader.releaseLock();

	const preamble = new TextEncoder().encode(
		JSON.stringify({
			v: 1,
			peer: winner.candidate.addr,
			reqq: winner.parser.reqq ?? null,
			client: winner.parser.client ?? null,
			sent: formatWant(blocks, req.geometry),
			blocks: blocks.length,
			ms: winner.ms,
			others: [...outcomes, ...rejected],
		}),
	);
	const length = new Uint8Array(4);
	new DataView(length.buffer).setUint32(0, preamble.byteLength);

	// Writes to an IdentityTransformStream only resolve once the response
	// is being read, so this runs after we've returned the Response.
	const { readable, writable } = new IdentityTransformStream();
	const socket = winner.socket;
	// Once the browser has its blocks it hangs up. The pipe is idle by then
	// (the peer has nothing more to send) and would only notice at the
	// peer's next keep-alive, holding the peer's connection slot meanwhile.
	if (disconnected.aborted) {
		close(socket);
		return new Response(null, { status: 499 });
	}
	disconnected.addEventListener("abort", () => close(socket), { once: true });
	const streamMs = Math.min(
		MAX_STREAM_MS,
		Math.max(MIN_STREAM_MS, (blocks.length * BLOCK_SIZE) / MIN_RATE_BYTES_PER_MS),
	);
	const pump = (async () => {
		const writer = writable.getWriter();
		await writer.write(concat(length, preamble, ...winner.setupBytes));
		writer.releaseLock();
		// Native stream to native stream: this pipe runs in the runtime, not
		// in JavaScript. (Keep the default options: `preventAbort` makes the
		// runtime handle the pipe differently, and the socket then outlives
		// the browser's abort.)
		await socket.readable.pipeTo(writable, { signal: AbortSignal.timeout(streamMs) });
	})()
		.catch(() => {})
		.finally(() => close(socket));
	ctx.waitUntil(pump);

	return new Response(readable, {
		headers: {
			...cors,
			"content-type": "application/octet-stream",
			"cache-control": "no-store",
		},
	});
}

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

async function checkTokens(req: BlocksRequest, secret: string): Promise<[Candidate[], Outcome[]]> {
	const now = Math.floor(Date.now() / 1000);
	const valid = await Promise.all(
		req.candidates.map((c) => verifyToken(secret, req.infoHashHex, c.addr, c.exp, c.token, now)),
	);
	return [
		req.candidates.filter((_, i) => valid[i]),
		req.candidates.filter((_, i) => !valid[i]).map((c) => ({ peer: c.addr, err: "bad_token" })),
	];
}

/**
 * CORS headers for an allowed origin; `{}` for requests without an Origin
 * (non-browser clients, which the tokens still gate); `null` if the origin
 * is not allowed.
 */
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
