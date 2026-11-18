// Live peer score. The weights for reachability, seed, unchoke delay,
// throughput, recency and consecutive failures are the ones in
// apps/web/src/lib/torrent/swarm.ts `score` (no randomness: ties break by
// address age in the caller, so tests are deterministic). Bitfield, `reqq`
// and rejects are the extra live facts docs/architecture.md §Connection
// management names; the swarm used them to filter, the engine scores them.

/** A recent block (swarm.ts `WARM_MS`). */
export const WARM_MS = 30_000;

export interface ScoreFacts {
	/** We completed a handshake with it. */
	reachable: boolean;
	seed: boolean;
	/** Ms from connect to the first unchoke, once it has unchoked. */
	unchokeMs: number | null;
	/** Bytes/second, 0 if it has not delivered. */
	rate: number;
	/** Blocks it has delivered to us. */
	blocks: number;
	/** When the last block arrived (0: never). */
	lastBlockAt: number;
	consecutiveFailures: number;
	/** Pieces it has, and the torrent's piece count (0 before metadata). */
	pieces: number;
	pieceCount: number;
	reqq: number;
	rejects: number;
}

/** Higher is better. */
export function connectionScore(facts: ScoreFacts, now: number): number {
	let s = 0;
	if (facts.reachable) s += 2;
	if (facts.seed) s += 1.5;
	if (facts.unchokeMs !== null) s += 1.5 - Math.min(facts.unchokeMs, 6000) / 2000;
	if (facts.rate > 0) s += Math.log2(1 + facts.rate / 65_536) * 1.25;
	else if (facts.blocks === 0) s += 0.5;
	if (facts.lastBlockAt > 0 && now - facts.lastBlockAt < WARM_MS) s += 0.5;
	s -= facts.consecutiveFailures * 1.5;
	if (facts.pieceCount > 0 && facts.pieces > 0) s += (facts.pieces / facts.pieceCount) * 1.5;
	if (facts.reqq > 0) s += Math.min(facts.reqq, 500) / 500;
	s -= Math.min(facts.rejects, 20) * 0.25;
	return s;
}
