// Which tunnel gateway carries the data path (docs/architecture.md §Risks:
// "switching the default path is a configuration change"; docs/operations.md
// › Default data path). One public build variable, PUBLIC_DEFAULT_GATEWAY,
// names the primary gateway; the other one is the fallback. Resolved at
// build time by astro.config.mjs (an invalid value fails the build) and at
// run time by api.ts with the same function, from the same variables.
//
// Imported by astro.config.mjs straight from Node: relative imports only,
// erasable syntax only.

export type GatewayKind = "cloudflare" | "vps";

export const GATEWAY_KINDS: readonly GatewayKind[] = ["cloudflare", "vps"];

/** Without PUBLIC_DEFAULT_GATEWAY: the Durable Object (decision OQ-02). */
export const DEFAULT_GATEWAY_KIND: GatewayKind = "cloudflare";

/** The public build variables the resolution reads (all optional). */
export interface GatewayEnv {
	/** `cloudflare` (default) or `vps`. */
	readonly PUBLIC_DEFAULT_GATEWAY?: string;
	/** The Cloudflare gateway (Worker + Durable Object). */
	readonly PUBLIC_WORKER_URL?: string;
	/**
	 * The VPS tunnel gateway; default: PUBLIC_SEEDERS_URL when that is set
	 * (the same service serves /v1/tunnel). Neither set, or `off`: no VPS
	 * gateway (escalation off; invalid with PUBLIC_DEFAULT_GATEWAY=vps).
	 */
	readonly PUBLIC_VPS_GATEWAY_URL?: string;
	/** The VPS discovery API. */
	readonly PUBLIC_SEEDERS_URL?: string;
}

/** Where the services are when the variables are unset. */
export interface GatewayDefaults {
	worker: string;
	seeders: string;
}

/** Production addresses, for builds without the variables. */
export const SERVICE_DEFAULTS: GatewayDefaults = {
	worker: "https://magnet-worker.opentrust.it",
	seeders: "https://magnet-seeders.opentrust.it",
};

export interface Gateways {
	/** Which gateway is primary. */
	kind: GatewayKind;
	/** Every tunnel of a session opens here. */
	primary: string;
	/** The other gateway (null: none, PUBLIC_VPS_GATEWAY_URL=off). */
	fallback: string | null;
	/** Both, by kind (vps null: PUBLIC_VPS_GATEWAY_URL=off). */
	cloudflare: string;
	vps: string | null;
}

/** PUBLIC_VPS_GATEWAY_URL value that turns the VPS gateway (and escalation) off. */
export const VPS_GATEWAY_OFF = "off";

/** The kind PUBLIC_DEFAULT_GATEWAY names; unset or empty is the default. Throws on anything else. */
export function parseGatewayKind(raw: string | undefined): GatewayKind {
	const value = (raw ?? "").trim();
	if (value === "") return DEFAULT_GATEWAY_KIND;
	if ((GATEWAY_KINDS as readonly string[]).includes(value)) return value as GatewayKind;
	throw new Error(
		`PUBLIC_DEFAULT_GATEWAY must be one of ${GATEWAY_KINDS.map((k) => `"${k}"`).join(", ")} (or unset for "${DEFAULT_GATEWAY_KIND}"), got ${JSON.stringify(value.slice(0, 64))}`,
	);
}

/**
 * A gateway base URL: http(s) or ws(s), no credentials, query or fragment.
 * Only public addresses belong in PUBLIC_* variables: they end up in the
 * client bundle.
 */
export function checkGatewayUrl(name: string, raw: string): string {
	const value = raw.trim();
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		throw new Error(`${name} is not a URL: ${JSON.stringify(value.slice(0, 120))}`);
	}
	if (!["http:", "https:", "ws:", "wss:"].includes(url.protocol)) throw new Error(`${name} must be an http(s) or ws(s) URL, got ${url.protocol}`);
	if (url.username || url.password) throw new Error(`${name} must not carry credentials (it is public: it ends up in the site's bundle)`);
	if (url.search || url.hash) throw new Error(`${name} must not carry a query or fragment`);
	return value;
}

/** Primary and fallback gateway from the build variables. Throws with a clear message on an invalid value. */
export function resolveGateways(env: GatewayEnv, defaults: GatewayDefaults = SERVICE_DEFAULTS): Gateways {
	const kind = parseGatewayKind(env.PUBLIC_DEFAULT_GATEWAY);
	const set = (v: string | undefined) => (v !== undefined && v.trim() !== "" ? v : undefined);
	const cloudflare = checkGatewayUrl("PUBLIC_WORKER_URL", set(env.PUBLIC_WORKER_URL) ?? defaults.worker);
	const off = env.PUBLIC_VPS_GATEWAY_URL?.trim() === VPS_GATEWAY_OFF;
	if (off && kind === "vps") throw new Error(`PUBLIC_VPS_GATEWAY_URL=${VPS_GATEWAY_OFF} leaves no gateway for PUBLIC_DEFAULT_GATEWAY=vps`);
	// Only a VPS gateway the build names: the built-in seeders address is the
	// discovery API's, not known to serve /v1/tunnel, so it never becomes one.
	const named = off ? undefined : (set(env.PUBLIC_VPS_GATEWAY_URL) ?? set(env.PUBLIC_SEEDERS_URL));
	if (named === undefined && !off && kind === "vps") {
		throw new Error("PUBLIC_DEFAULT_GATEWAY=vps needs the VPS gateway's address: set PUBLIC_VPS_GATEWAY_URL or PUBLIC_SEEDERS_URL");
	}
	const vps = named === undefined ? null : checkGatewayUrl(set(env.PUBLIC_VPS_GATEWAY_URL) ? "PUBLIC_VPS_GATEWAY_URL" : "PUBLIC_SEEDERS_URL", named);
	return kind === "vps" && vps !== null
		? { kind, primary: vps, fallback: cloudflare, cloudflare, vps }
		: { kind, primary: cloudflare, fallback: vps, cloudflare, vps };
}

// Swarms with few seeders (M4-06, docs/architecture.md §Chunk recovery ›
// Swarms with few seeders; docs/operations.md › Escalation to the VPS
// gateway): when fewer than 2 peers deliver, or the rate stays under the
// video's bitrate, for 90 s in a row, the session asks the VPS gateway to
// join the swarm; after 30 s under the bitrate the page says so; once the
// session's own peers are enough again for 10 min it withdraws. Public
// build variables (seconds and a count; unset: the defaults, OQ-11).

/** The escalation's public build variables (all optional). */
export interface EscalationEnv extends GatewayEnv {
	/** Few peers or a rate under the bitrate this long in a row: escalate (default 90). */
	readonly PUBLIC_ESCALATION_AFTER_SECONDS?: string;
	/** A rate under the bitrate this long in a row: the page says so (default 30). */
	readonly PUBLIC_SLOW_NOTICE_SECONDS?: string;
	/** "Few peers" is fewer than this many delivering (default 2). */
	readonly PUBLIC_ESCALATION_MIN_PEERS?: string;
	/** Escalated, and the session's own peers enough this long in a row: withdraw the announce (default 600). */
	readonly PUBLIC_ESCALATION_WITHDRAW_SECONDS?: string;
}

export interface EscalationSettings {
	/** The VPS gateway a session asks to join a thin swarm (the primary itself when it is the VPS); null: escalation off. */
	gatewayUrl: string | null;
	minDelivering: number;
	escalateAfterMs: number;
	warnAfterMs: number;
	withdrawAfterMs: number;
}

export const ESCALATION_DEFAULTS = { minDelivering: 2, escalateAfterSeconds: 90, warnAfterSeconds: 30, withdrawAfterSeconds: 600 } as const;

function numberVar(name: string, raw: string | undefined, fallback: number, min: number, max: number, integer = false): number {
	const value = (raw ?? "").trim();
	if (value === "") return fallback;
	const n = Number(value);
	if (!Number.isFinite(n) || n < min || n > max || (integer && !Number.isInteger(n))) {
		throw new Error(`${name} must be ${integer ? "an integer" : "a number"} from ${min} to ${max}, got ${JSON.stringify(value.slice(0, 32))}`);
	}
	return n;
}

/** Where and when a session escalates, from the build variables. Throws with a clear message on an invalid value. */
export function resolveEscalation(env: EscalationEnv, defaults: GatewayDefaults = SERVICE_DEFAULTS): EscalationSettings {
	const gateways = resolveGateways(env, defaults);
	return {
		gatewayUrl: gateways.vps,
		minDelivering: numberVar("PUBLIC_ESCALATION_MIN_PEERS", env.PUBLIC_ESCALATION_MIN_PEERS, ESCALATION_DEFAULTS.minDelivering, 1, 64, true),
		escalateAfterMs: 1000 * numberVar("PUBLIC_ESCALATION_AFTER_SECONDS", env.PUBLIC_ESCALATION_AFTER_SECONDS, ESCALATION_DEFAULTS.escalateAfterSeconds, 1, 3600),
		warnAfterMs: 1000 * numberVar("PUBLIC_SLOW_NOTICE_SECONDS", env.PUBLIC_SLOW_NOTICE_SECONDS, ESCALATION_DEFAULTS.warnAfterSeconds, 1, 3600),
		withdrawAfterMs:
			1000 * numberVar("PUBLIC_ESCALATION_WITHDRAW_SECONDS", env.PUBLIC_ESCALATION_WITHDRAW_SECONDS, ESCALATION_DEFAULTS.withdrawAfterSeconds, 1, 3600),
	};
}
