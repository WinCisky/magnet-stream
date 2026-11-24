// Shared types and service URLs. Peer discovery and metadata now come from
// the session worker; these types are what the scheduler and the file list
// still speak.

import { resolveEscalation, resolveGateways, SERVICE_DEFAULTS, type EscalationSettings, type Gateways } from "./gateway-config";

export interface TorrentFile {
	path: string;
	size: number;
	offset: number;
}

export interface TorrentMetadata {
	info_hash: string;
	name: string;
	size: number;
	files: TorrentFile[];
}

export const SEEDERS_API_URL: string = import.meta.env.PUBLIC_SEEDERS_URL || SERVICE_DEFAULTS.seeders;

/**
 * The tunnel gateways: PUBLIC_DEFAULT_GATEWAY (`cloudflare`, the default, or
 * `vps`) picks the primary one, the other is the fallback
 * (docs/operations.md › Default data path). The build already rejected an
 * invalid value (astro.config.mjs).
 */
export const GATEWAYS: Gateways = resolveGateways(
	{
		PUBLIC_DEFAULT_GATEWAY: import.meta.env.PUBLIC_DEFAULT_GATEWAY,
		PUBLIC_WORKER_URL: import.meta.env.PUBLIC_WORKER_URL,
		PUBLIC_VPS_GATEWAY_URL: import.meta.env.PUBLIC_VPS_GATEWAY_URL,
		PUBLIC_SEEDERS_URL: import.meta.env.PUBLIC_SEEDERS_URL,
	},
	SERVICE_DEFAULTS,
);

/** Where a session's tunnels open: the primary gateway. */
export const GATEWAY_URL: string = GATEWAYS.primary;

/**
 * Swarms with few seeders (M4-06): the VPS gateway a session asks to join
 * the swarm (null: PUBLIC_VPS_GATEWAY_URL=off, or neither it nor
 * PUBLIC_SEEDERS_URL set: escalation off) and when.
 */
export const ESCALATION: EscalationSettings = resolveEscalation(
	{
		PUBLIC_DEFAULT_GATEWAY: import.meta.env.PUBLIC_DEFAULT_GATEWAY,
		PUBLIC_WORKER_URL: import.meta.env.PUBLIC_WORKER_URL,
		PUBLIC_VPS_GATEWAY_URL: import.meta.env.PUBLIC_VPS_GATEWAY_URL,
		PUBLIC_SEEDERS_URL: import.meta.env.PUBLIC_SEEDERS_URL,
		PUBLIC_ESCALATION_AFTER_SECONDS: import.meta.env.PUBLIC_ESCALATION_AFTER_SECONDS,
		PUBLIC_SLOW_NOTICE_SECONDS: import.meta.env.PUBLIC_SLOW_NOTICE_SECONDS,
		PUBLIC_ESCALATION_MIN_PEERS: import.meta.env.PUBLIC_ESCALATION_MIN_PEERS,
		PUBLIC_ESCALATION_WITHDRAW_SECONDS: import.meta.env.PUBLIC_ESCALATION_WITHDRAW_SECONDS,
	},
	SERVICE_DEFAULTS,
);

/**
 * Hard cap on one file-list lookup, in seconds (default 120; the lookup
 * gives up earlier only when nothing is left to try).
 */
export const LOOKUP_CAP_MS: number | undefined = (() => {
	const raw = Number(import.meta.env.PUBLIC_LOOKUP_CAP_SECONDS);
	return Number.isFinite(raw) && raw > 0 ? Math.round(raw * 1000) : undefined;
})();

/** A peer the scheduler can assign work to. */
export interface SwarmPeer {
	addr: string;
	/** Reachable over TCP and handshook. */
	ok: boolean;
	err?: string;
	seed: boolean;
	/** Base64 bitfield, for reachable non-seeds. */
	have?: string;
	have_count?: number;
	client?: string;
	reqq?: number;
	rtt_ms?: number;
	/** How fast it unchoked, if it did. */
	unchoke_ms?: number;
	fast: boolean;
}

export interface SwarmSnapshot {
	info_hash: string;
	announced_at: number;
	probed_at: number;
	num_pieces?: number;
	complete?: boolean;
	counts: { known: number; probed: number; pending?: number; reachable: number; seeds: number; unchoked: number };
	peers: SwarmPeer[];
}
