// Shared types and service URLs. Peer discovery and metadata now come from
// the session worker; these types are what the scheduler and the file list
// still speak.

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

export const SEEDERS_API_URL: string =
	import.meta.env.PUBLIC_SEEDERS_URL ?? "https://magnet-seeders.opentrust.it";
export const WORKER_API_URL: string =
	import.meta.env.PUBLIC_WORKER_URL ?? "https://magnet-worker.opentrust.it";

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
