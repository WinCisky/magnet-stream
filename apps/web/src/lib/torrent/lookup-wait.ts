// How long the file list may take (docs/architecture.md › Connection
// management, "The file-list wait follows progress"). The lookup gives up
// only when nothing is left to try (discovery finished, no live or unknown
// candidate left, no dial in flight, no peer still able to serve the
// metadata), or at a hard cap; never after a fixed time while candidates
// remain. A failed lookup keeps its session: a retry restarts the clock and
// carries on from the same peer book.
//
// Pure: the session worker samples the facts and the clock.

/** Hard cap on one lookup (default; the page may set another). */
export const DEFAULT_LOOKUP_CAP_MS = 120_000;
/** Caps accepted from the page. */
export const MIN_LOOKUP_CAP_MS = 5_000;
export const MAX_LOOKUP_CAP_MS = 30 * 60_000;
/**
 * A peer that just connected may still send its extended handshake (and
 * offer the metadata): it counts as hope for this long.
 */
export const CONNECT_GRACE_MS = 5_000;

/** What the lookup can still hope for, sampled by the session worker. */
export interface LookupFacts {
	/** The `/peers` stream ended (answered, failed, or was never asked for). */
	discoveryDone: boolean;
	/** Live or unknown addresses still to dial (now, or once a short back-off ends). */
	candidates: number;
	/** Dials in flight. */
	dialling: number;
	/** Peers offering the metadata still to ask, or being asked. */
	fetching: number;
	/** When a peer last connected (ms, the same clock as `now`), 0: never. */
	lastConnectAt: number;
	/** An info dict from outside (cache, the `/peers` stream) is being checked. */
	checking: boolean;
}

/** Why a lookup gave up: nothing left to try, the cap, or no tunnel to dial through. */
export type LookupFailure = "exhausted" | "cap" | "tunnel";

export type LookupVerdict = { state: "waiting" } | { state: "failed"; reason: "exhausted" | "cap" };

export class LookupWait {
	private startedAt: number;
	readonly capMs: number;

	constructor(now: number, capMs = DEFAULT_LOOKUP_CAP_MS) {
		this.startedAt = now;
		this.capMs = clampCap(capMs);
	}

	/** A retry: the cap counts from now again. */
	restart(now: number): void {
		this.startedAt = now;
	}

	elapsed(now: number): number {
		return Math.max(0, now - this.startedAt);
	}

	judge(facts: LookupFacts, now: number): LookupVerdict {
		if (this.elapsed(now) >= this.capMs) return { state: "failed", reason: "cap" };
		return hasHope(facts, now) ? { state: "waiting" } : { state: "failed", reason: "exhausted" };
	}
}

/** Something may still bring the metadata. */
export function hasHope(f: LookupFacts, now: number): boolean {
	if (!f.discoveryDone || f.checking) return true;
	if (f.candidates > 0 || f.dialling > 0 || f.fetching > 0) return true;
	return f.lastConnectAt > 0 && now - f.lastConnectAt < CONNECT_GRACE_MS;
}

/** A cap from the page, within bounds (the default when not a number). */
export function clampCap(ms: number | undefined): number {
	if (typeof ms !== "number" || !Number.isFinite(ms)) return DEFAULT_LOOKUP_CAP_MS;
	return Math.min(MAX_LOOKUP_CAP_MS, Math.max(MIN_LOOKUP_CAP_MS, Math.round(ms)));
}

/** What the page shows while it waits (no addresses). */
export interface LookupProgress {
	/** Addresses known. */
	found: number;
	/** Addresses dialled at least once. */
	tried: number;
	/** Peers connected now. */
	connected: number;
	elapsedMs: number;
	/** The `/peers` stream is still open. */
	discovering: boolean;
}

/** The message a failed lookup shows. */
export function failureMessage(reason: LookupFailure, progress: LookupProgress): string {
	if (reason === "cap") {
		const s = Math.round(progress.elapsedMs / 1000);
		return `The file list didn't arrive within ${s} s (${progress.found} peers found, ${progress.tried} tried, ${progress.connected} connected). Retry to keep looking.`;
	}
	if (reason === "tunnel") return "Couldn't reach the tunnel gateway. Retry in a moment.";
	if (progress.found === 0) return "No peers were found for this magnet. Retry to ask again.";
	return `None of the ${progress.found} peers found sent the file list (${progress.tried} tried). Retry to ask again.`;
}
