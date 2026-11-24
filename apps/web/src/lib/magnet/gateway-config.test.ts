import { describe, expect, it } from "vitest";
import astroConfig from "../../../astro.config.mjs";
import { checkGatewayUrl, parseGatewayKind, resolveEscalation, resolveGateways, SERVICE_DEFAULTS } from "./gateway-config";

const CF = "https://gateway.example.workers.dev";
const VPS = "https://vps.example.org";

describe("PUBLIC_DEFAULT_GATEWAY", () => {
	it("unset or empty is cloudflare", () => {
		expect(parseGatewayKind(undefined)).toBe("cloudflare");
		expect(parseGatewayKind("")).toBe("cloudflare");
		expect(parseGatewayKind("  ")).toBe("cloudflare");
	});

	it("accepts the two kinds", () => {
		expect(parseGatewayKind("cloudflare")).toBe("cloudflare");
		expect(parseGatewayKind("vps")).toBe("vps");
		expect(parseGatewayKind(" vps\n")).toBe("vps");
	});

	it("rejects anything else with a clear message", () => {
		for (const bad of ["VPS", "Cloudflare", "cf", "both", "vps,cloudflare", "1"]) {
			expect(() => parseGatewayKind(bad)).toThrow(/PUBLIC_DEFAULT_GATEWAY must be one of "cloudflare", "vps"/);
		}
		expect(() => parseGatewayKind("x".repeat(1000))).toThrow(/got "x{64}"$/);
	});
});

describe("resolveGateways", () => {
	it("cloudflare: the Worker is primary, the VPS the fallback (unchanged behaviour)", () => {
		expect(resolveGateways({ PUBLIC_WORKER_URL: CF, PUBLIC_SEEDERS_URL: VPS })).toEqual({ kind: "cloudflare", primary: CF, fallback: VPS, cloudflare: CF, vps: VPS });
		expect(resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "cloudflare", PUBLIC_WORKER_URL: CF }).primary).toBe(CF);
	});

	it("vps: the VPS is primary, Cloudflare the fallback", () => {
		expect(resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_WORKER_URL: CF, PUBLIC_SEEDERS_URL: VPS })).toEqual({ kind: "vps", primary: VPS, fallback: CF, cloudflare: CF, vps: VPS });
	});

	it("the VPS gateway defaults to PUBLIC_SEEDERS_URL, PUBLIC_VPS_GATEWAY_URL overrides it", () => {
		const gw = "https://tunnel.example.org";
		expect(resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_SEEDERS_URL: VPS, PUBLIC_VPS_GATEWAY_URL: gw }).primary).toBe(gw);
		expect(resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_SEEDERS_URL: VPS, PUBLIC_VPS_GATEWAY_URL: "" }).primary).toBe(VPS);
	});

	it("without variables: the built-in Worker, no VPS gateway (the built-in seeders address is not one)", () => {
		expect(resolveGateways({})).toEqual({
			kind: "cloudflare",
			primary: SERVICE_DEFAULTS.worker,
			fallback: null,
			cloudflare: SERVICE_DEFAULTS.worker,
			vps: null,
		});
		expect(() => resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps" })).toThrow(/PUBLIC_DEFAULT_GATEWAY=vps needs the VPS gateway's address/);
		expect(resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_VPS_GATEWAY_URL: VPS }).primary).toBe(VPS);
	});

	it("an invalid kind fails", () => {
		expect(() => resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "edge" })).toThrow(/PUBLIC_DEFAULT_GATEWAY/);
	});

	it("rejects URLs that are not public gateway addresses", () => {
		expect(() => resolveGateways({ PUBLIC_WORKER_URL: "not a url" })).toThrow(/PUBLIC_WORKER_URL is not a URL/);
		expect(() => resolveGateways({ PUBLIC_WORKER_URL: "ftp://x.example" })).toThrow(/http\(s\) or ws\(s\)/);
		expect(() => resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_SEEDERS_URL: "https://user:pw@vps.example" })).toThrow(/PUBLIC_SEEDERS_URL must not carry credentials/);
		expect(() => resolveGateways({ PUBLIC_VPS_GATEWAY_URL: "https://vps.example/?token=x" })).toThrow(/PUBLIC_VPS_GATEWAY_URL must not carry a query/);
		expect(checkGatewayUrl("X", "ws://127.0.0.1:8787/v1/tunnel")).toBe("ws://127.0.0.1:8787/v1/tunnel");
	});
});

describe("escalation (M4-06)", () => {
	it("defaults: the VPS gateway, 2 peers, 90 s to escalate, 30 s to warn", () => {
		expect(resolveEscalation({ PUBLIC_WORKER_URL: CF, PUBLIC_SEEDERS_URL: VPS })).toEqual({ gatewayUrl: VPS, minDelivering: 2, escalateAfterMs: 90_000, warnAfterMs: 30_000, withdrawAfterMs: 600_000 });
		// With the VPS primary the session announces on its own tunnels.
		expect(resolveEscalation({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_WORKER_URL: CF, PUBLIC_SEEDERS_URL: VPS }).gatewayUrl).toBe(VPS);
	});

	it("takes the thresholds from the build variables", () => {
		expect(
			resolveEscalation({ PUBLIC_ESCALATION_AFTER_SECONDS: "10", PUBLIC_SLOW_NOTICE_SECONDS: "2.5", PUBLIC_ESCALATION_MIN_PEERS: "3", PUBLIC_ESCALATION_WITHDRAW_SECONDS: "120" }),
		).toMatchObject({ minDelivering: 3, escalateAfterMs: 10_000, warnAfterMs: 2_500, withdrawAfterMs: 120_000 });
		expect(() => resolveEscalation({ PUBLIC_ESCALATION_WITHDRAW_SECONDS: "0" })).toThrow(/PUBLIC_ESCALATION_WITHDRAW_SECONDS must be a number from 1 to 3600/);
		expect(() => resolveEscalation({ PUBLIC_ESCALATION_AFTER_SECONDS: "soon" })).toThrow(/PUBLIC_ESCALATION_AFTER_SECONDS must be a number from 1 to 3600/);
		expect(() => resolveEscalation({ PUBLIC_SLOW_NOTICE_SECONDS: "0" })).toThrow(/PUBLIC_SLOW_NOTICE_SECONDS/);
		expect(() => resolveEscalation({ PUBLIC_ESCALATION_MIN_PEERS: "1.5" })).toThrow(/PUBLIC_ESCALATION_MIN_PEERS must be an integer/);
	});

	it("no VPS gateway named: escalation off, without errors", () => {
		expect(resolveEscalation({ PUBLIC_WORKER_URL: CF })).toMatchObject({ gatewayUrl: null, minDelivering: 2 });
		expect(resolveEscalation({ PUBLIC_WORKER_URL: CF, PUBLIC_SEEDERS_URL: " ", PUBLIC_VPS_GATEWAY_URL: "" }).gatewayUrl).toBeNull();
	});

	it("PUBLIC_VPS_GATEWAY_URL=off: no VPS gateway, escalation off (an error with the VPS primary)", () => {
		expect(resolveEscalation({ PUBLIC_VPS_GATEWAY_URL: "off", PUBLIC_WORKER_URL: CF }).gatewayUrl).toBeNull();
		expect(resolveGateways({ PUBLIC_VPS_GATEWAY_URL: "off", PUBLIC_WORKER_URL: CF })).toEqual({ kind: "cloudflare", primary: CF, fallback: null, cloudflare: CF, vps: null });
		expect(() => resolveGateways({ PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_VPS_GATEWAY_URL: "off" })).toThrow(/leaves no gateway/);
	});
});

describe("the build", () => {
	// The Vite plugin astro.config.mjs adds: what makes `astro build` fail.
	type Hook = (config: { env: Record<string, string> }) => void;
	const plugin = (astroConfig.vite?.plugins ?? []).flat().find((p) => p && typeof p === "object" && "name" in p && p.name === "magnet-stream:default-gateway") as
		| { configResolved: Hook }
		| undefined;

	it("fails on an invalid PUBLIC_DEFAULT_GATEWAY", () => {
		expect(plugin).toBeDefined();
		expect(() => plugin?.configResolved({ env: { PUBLIC_DEFAULT_GATEWAY: "vsp" } })).toThrow(/invalid gateway configuration: PUBLIC_DEFAULT_GATEWAY must be one of/);
	});

	it("fails on an invalid escalation threshold", () => {
		expect(() => plugin?.configResolved({ env: { PUBLIC_ESCALATION_AFTER_SECONDS: "-1" } })).toThrow(/invalid gateway configuration: PUBLIC_ESCALATION_AFTER_SECONDS/);
	});

	it("passes on the valid values", () => {
		for (const kind of ["", "cloudflare", "vps"]) expect(() => plugin?.configResolved({ env: { PUBLIC_DEFAULT_GATEWAY: kind, PUBLIC_SEEDERS_URL: VPS } })).not.toThrow();
		expect(() => plugin?.configResolved({ env: {} })).not.toThrow();
	});

	it("fails on a VPS primary without its address", () => {
		expect(() => plugin?.configResolved({ env: { PUBLIC_DEFAULT_GATEWAY: "vps" } })).toThrow(/invalid gateway configuration: PUBLIC_DEFAULT_GATEWAY=vps needs/);
	});
});
