import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { allowPrivatePeers, originAllowed, tunnelLimits } from "../../src/tunnel/config";

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
	it("default, and stay within bounds", () => {
		expect(tunnelLimits({})).toEqual({ connectTimeoutMs: 5000, handshakeTimeoutMs: 10000, maxConnections: 6 });
		expect(tunnelLimits({ TUNNEL_CONNECT_TIMEOUT_MS: "1", TUNNEL_HANDSHAKE_TIMEOUT_MS: "999999", TUNNEL_MAX_CONNECTIONS: "99" })).toEqual({
			connectTimeoutMs: 100,
			handshakeTimeoutMs: 60000,
			maxConnections: 16,
		});
		expect(tunnelLimits({ TUNNEL_MAX_CONNECTIONS: "0" }).maxConnections).toBe(1);
		expect(tunnelLimits({ TUNNEL_MAX_CONNECTIONS: "x" }).maxConnections).toBe(6);
		expect(tunnelLimits({ TUNNEL_CONNECT_TIMEOUT_MS: "abc" }).connectTimeoutMs).toBe(5000);
	});
});

describe("the Durable Object stores nothing", () => {
	it("never touches ctx.storage, SQL or KV-like APIs", () => {
		const dir = dirname(fileURLToPath(import.meta.url));
		for (const file of ["durable-object.ts", "session.ts", "config.ts", "address-policy.ts"]) {
			const code = readFileSync(resolve(dir, "../../src/tunnel", file), "utf8").replace(/\/\/.*$/gm, "");
			expect(code, file).not.toMatch(/\bstorage\b|\.sql\b|\bsetAlarm\b|\bput\(|transactionSync/);
		}
	});
});
