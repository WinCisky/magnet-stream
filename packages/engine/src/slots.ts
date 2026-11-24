// Which connection may be dropped to free a slot (docs/architecture.md
// §Connection management: replace the slowest or still-choked, never one
// that is delivering). A peer cannot force a delivering connection closed:
// `delivering` wins over every other fact.

export interface SlotView {
	key: string;
	delivering: boolean;
	choked: boolean;
	/** Unchoked, but slower than the peers that are actually serving. */
	slow: boolean;
	/** It has none of the pieces we still want (known once its bitfield had time to arrive). */
	useless?: boolean;
	downloaded: number;
	score: number;
	openedAt: number;
	/** Since when it has been choking us without delivering (default `openedAt`). */
	chokedSince?: number;
	/**
	 * A seed that chokes us: worth waiting for over several optimistic-unchoke
	 * rotations (docs/architecture.md §Swarms with few seeders, step 1).
	 */
	patient?: boolean;
}

/**
 * The connection to close so a better candidate can be dialled, or
 * undefined when every connection is delivering (or none is old enough to
 * replace). A connection with none of the wanted pieces goes first, at
 * once: it cannot serve us whatever it does. A choked connection is
 * replaceable only after `chokeAfterMs`: an optimistic unchoke is worth
 * waiting for when nothing better is urgent, but a waiting candidate is
 * better than a peer that has not unchoked. A choked seed (`patient`) gets
 * `patienceMs` instead (default: `chokeAfterMs`): the optimistic slot
 * rotates about every 30 s and may take several rotations to reach us.
 */
export function replacementVictim(slots: readonly SlotView[], now: number, chokeAfterMs: number, patienceMs = chokeAfterMs): SlotView | undefined {
	const chokedLongEnough = (s: SlotView) => now - (s.chokedSince ?? s.openedAt) >= (s.patient === true ? Math.max(chokeAfterMs, patienceMs) : chokeAfterMs);
	const replaceable = slots.filter((s) => !s.delivering && (s.useless === true || s.slow || (s.choked && chokedLongEnough(s))));
	if (replaceable.length === 0) return undefined;
	return replaceable.sort(
		(a, b) => Number(b.useless === true) - Number(a.useless === true) || a.downloaded - b.downloaded || a.score - b.score || a.openedAt - b.openedAt,
	)[0];
}

export interface RateSample {
	at: number;
	/** Bytes/second over the sample interval. */
	rate: number;
}

/**
 * A further WebSocket is opened only when the download rate has stayed
 * strictly under `threshold` for the whole window (default 30 s), and the
 * session is not already at `maxSockets` (3).
 */
export function shouldAddSocket(
	samples: readonly RateSample[],
	now: number,
	windowMs: number,
	threshold: number,
	sockets: number,
	maxSockets: number,
): boolean {
	if (sockets >= maxSockets || !(threshold > 0) || samples.length === 0) return false;
	if (samples[0].at > now - windowMs) return false;
	for (const s of samples) {
		if (s.at <= now - windowMs) continue;
		if (s.rate >= threshold) return false;
	}
	return true;
}

/** Bytes of block requests still allowed on this socket (never above the cap). */
export function requestAllowance(opts: {
	socketInflight: number;
	limit: number;
	peerOutstanding: number;
	peerReqq: number;
	blockLength: number;
}): number {
	const bySocket = Math.max(0, opts.limit - opts.socketInflight);
	const byQueue = Math.max(0, opts.peerReqq - opts.peerOutstanding) * opts.blockLength;
	return Math.min(bySocket, byQueue);
}
