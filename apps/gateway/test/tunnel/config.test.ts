import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { allowPrivatePeers, originAllowed, parseSessionName, parseTunnelConfig } from "../../src/tunnel/config";
import { LIMITER_UNITS } from "../../src/tunnel/rate-limit";

describe("origin allow-list", () => {
	const env = { ALLOWED_ORIGINS: "https://site.example, http://localhost:4321" };

	it("allows listed origins and requests without Origin", () => {
		expect(originAllowed("https://site.example", env)).toBe(true);
		expect(originAllowed("http://localhost:4321", env)).toBe(true);
		expect(originAllowed(null, env)).toBe(true);
	});

	it("rejects anything else", () => {
		expect(originAllowed("https://evil.example", env)).toBe(false);
		expect(originAllowed("https://site.example.evil.example", env)).toBe(false);
		expect(originAllowed("null", env)).toBe(false);
		expect(originAllowed("https://site.example", {})).toBe(false);
	});
});

describe("ALLOW_PRIVATE_PEERS", () => {
	it("is off by default", () => {
		expect(allowPrivatePeers({}, new URL("http://127.0.0.1:8787/v1/tunnel"))).toBe(false);
	});

	it("only applies to requests addressed to loopback (wrangler dev)", () => {
		const env = { ALLOW_PRIVATE_PEERS: "1" };
		expect(allowPrivatePeers(env, new URL("http://127.0.0.1:8787/v1/tunnel"))).toBe(true);
		expect(allowPrivatePeers(env, new URL("http://localhost:8787/v1/tunnel"))).toBe(true);
		expect(allowPrivatePeers(env, new URL("https://gateway.example.workers.dev/v1/tunnel"))).toBe(false);
		expect(allowPrivatePeers({ ALLOW_PRIVATE_PEERS: "true" }, new URL("http://127.0.0.1/"))).toBe(false);
	});

	it("is not set anywhere in wrangler.jsonc", () => {
		const config = readFileSync(resolve(dirname(fileURLToPath(import.meta.url)), "../../wrangler.jsonc"), "utf8");
		// Strip comments (which mention it), then look at the actual config.
		const code = config.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"])\/\/.*$/gm, "$1");
		expect(code).not.toContain("ALLOW_PRIVATE_PEERS");
		expect(code).toContain("ALLOWED_ORIGINS");
	});
});

describe("limits", () => {
	it("have defaults", () => {
		expect(parseTunnelConfig({})).toEqual({
			ok: true,
			config: {
				connectTimeoutMs: 5000,
				handshakeTimeoutMs: 10000,
				maxConnections: 6,
				idleTimeoutMs: 90_000,
				failedConnectHoldMs: 20_000,
				sessionTimeoutMs: 7_200_000,
				socketsPerSession: 3,
				upgradesPerMinute: 30,
			},
		});
	});

	it("are read from env", () => {
		const r = parseTunnelConfig({ TUNNEL_SESSION_TIMEOUT_S: "600", TUNNEL_IDLE_TIMEOUT_MS: " 5000 ", TUNNEL_UPGRADES_PER_MINUTE: "5", TUNNEL_MAX_CONNECTIONS: "" });
		expect(r.ok && r.config).toMatchObject({ sessionTimeoutMs: 600_000, idleTimeoutMs: 5000, upgradesPerMinute: 5, maxConnections: 6 });
	});

	it("reject out-of-range or malformed values instead of clamping them", () => {
		expect(parseTunnelConfig({ TUNNEL_CONNECT_TIMEOUT_MS: "1", TUNNEL_HANDSHAKE_TIMEOUT_MS: "999999", TUNNEL_MAX_CONNECTIONS: "99" })).toEqual({
			ok: false,
			invalid: ["TUNNEL_CONNECT_TIMEOUT_MS", "TUNNEL_HANDSHAKE_TIMEOUT_MS", "TUNNEL_MAX_CONNECTIONS"],
		});
		for (const bad of ["4", "86401", "-5", "1e3", "abc", "7200s", "12.5"]) {
			expect(parseTunnelConfig({ TUNNEL_SESSION_TIMEOUT_S: bad })).toEqual({ ok: false, invalid: ["TUNNEL_SESSION_TIMEOUT_S"] });
		}
		expect(parseTunnelConfig({ TUNNEL_UPGRADES_PER_MINUTE: "601" }).ok).toBe(false);
		expect(parseTunnelConfig({ TUNNEL_SOCKETS_PER_SESSION: "9" }).ok).toBe(false);
	});
});

describe("session names", () => {
	it("accept 32 lowercase hex, or nothing", () => {
		expect(parseSessionName(null)).toBeNull();
		expect(parseSessionName("0123456789abcdef0123456789abcdef")).toBe("0123456789abcdef0123456789abcdef");
		for (const bad of ["", "abc", "0123456789ABCDEF0123456789ABCDEF", "0123456789abcdef0123456789abcdef0", "../../x"]) {
			expect(parseSessionName(bad)).toBeUndefined();
		}
	});
});

describe("rate limit binding", () => {
	it("is declared with the budget the code assumes", () => {
		const config = readFileSync(resolve(dirname(fileURLToPath(import.meta.url)), "../../wrangler.jsonc"), "utf8");
		const m = /"ratelimits":\s*\[\{\s*"name":\s*"UPGRADE_LIMITER",\s*"namespace_id":\s*"\d+",\s*"simple":\s*\{\s*"limit":\s*(\d+),\s*"period":\s*60/.exec(config);
		expect(Number(m?.[1])).toBe(LIMITER_UNITS);
	});
});

describe("the Durable Object stores nothing", () => {
	it("never touches ctx.storage, SQL or KV-like APIs", () => {
		const dir = dirname(fileURLToPath(import.meta.url));
		for (const file of ["durable-object.ts", "session.ts", "config.ts", "address-policy.ts", "rate-limit.ts"]) {
			const code = readFileSync(resolve(dir, "../../src/tunnel", file), "utf8").replace(/\/\/.*$/gm, "");
			expect(code, file).not.toMatch(/\bstorage\b|\.sql\b|\bsetAlarm\b|\bput\(|transactionSync/);
		}
	});
});
