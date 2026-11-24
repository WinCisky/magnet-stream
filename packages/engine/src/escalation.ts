// Swarms with few seeders (docs/architecture.md §Chunk recovery › Swarms
// with few seeders): the engine escalates only as far as it must.
//
//   1. Patience on the tunnels (connection-manager.ts: a choked seed keeps
//      its slot for several optimistic-unchoke rotations).
//   2. Partial peers are sources (the picker's scarce-piece rule).
//   3. Become reachable: when fewer than `minDelivering` peers deliver for
//      `escalateAfterMs` in a row (counted only once the scout's live
//      candidates are used up), or the rate stays under the video's bitrate
//      for `escalateAfterMs` in a row, the session asks the VPS gateway to
//      join the swarm (announce, inbound peers).
//   5. Degrade honestly: after `warnAfterMs` in a row under the bitrate,
//      the page says so (and the file still completes).
//
// Announce only while it is needed (docs/architecture.md §Risks): once the
// peers the session found on its own (not those the escalation brought:
// inbound peers, uTP ones from the announced port) deliver enough, and fast
// enough, for `withdrawAfterMs` in a row, the session withdraws the
// announce and may escalate again later, from scratch.
//
// This is the pure decision, driven by a clock the caller passes in (unit
// tests simulate it). Thresholds are configurable; the defaults are
// deliberately demanding (OQ-11): 2 peers, 90 s, 30 s.

export interface EscalationConfig {
	/** Fewer peers than this delivering counts as "few" (default 2). */
	minDelivering: number;
	/** Few peers, or a rate under the bitrate, this long in a row: escalate (default 90 s). */
	escalateAfterMs: number;
	/** A rate under the bitrate this long in a row: the page warns (default 30 s). */
	warnAfterMs: number;
	/** Escalated, and healthy without the escalation's peers this long in a row: withdraw (default 10 min). */
	withdrawAfterMs: number;
}

export const ESCALATION_DEFAULTS: Readonly<EscalationConfig> = {
	minDelivering: 2,
	escalateAfterMs: 90_000,
	warnAfterMs: 30_000,
	withdrawAfterMs: 600_000,
};

/** Why the session escalated: too few peers delivering, or a rate under the bitrate. */
export type EscalationReason = "few_peers" | "slow";

/** What the engine sees on each tick. */
export interface EscalationInput {
	now: number;
	/** Data is wanted (a recovery runs, not suspended): otherwise nothing is judged and the clocks restart. */
	active: boolean;
	/** Peers that delivered a block in the last few seconds. */
	delivering: number;
	/** The VPS scout's live candidates are used up (discovery over, none left to dial or in flight). */
	scoutExhausted: boolean;
	/** Download rate, bytes per second (smoothed over a few seconds; NaN: not measured yet). */
	rate: number;
	/** The video's bytes per second (0: not known: "under the bitrate" is never judged). */
	bitrate: number;
	/** Of `delivering`, the peers the escalation did not bring (default: all of them). */
	ownDelivering?: number;
	/** Of `rate`, what those peers deliver, bytes per second (default: all of it). */
	ownRate?: number;
}

export interface EscalationVerdict {
	/** The rate has been under the bitrate for `warnAfterMs`: the page says so. */
	belowBitrate: boolean;
	/** Set on the one tick that decides to escalate (again only after a withdraw). */
	escalate: EscalationReason | undefined;
	/** Set on the one tick that decides the escalation is no longer needed: withdraw the announce. */
	withdraw: boolean;
}

/** Clamp a configured value into a sane range (a bad value falls back to the default). */
function sane(value: number | undefined, fallback: number, min: number, max: number): number {
	if (value === undefined || !Number.isFinite(value)) return fallback;
	return Math.min(max, Math.max(min, value));
}

export function escalationConfig(over: Partial<EscalationConfig> = {}): EscalationConfig {
	return {
		minDelivering: Math.round(sane(over.minDelivering, ESCALATION_DEFAULTS.minDelivering, 1, 64)),
		escalateAfterMs: sane(over.escalateAfterMs, ESCALATION_DEFAULTS.escalateAfterMs, 0, 3_600_000),
		warnAfterMs: sane(over.warnAfterMs, ESCALATION_DEFAULTS.warnAfterMs, 0, 3_600_000),
		withdrawAfterMs: sane(over.withdrawAfterMs, ESCALATION_DEFAULTS.withdrawAfterMs, 0, 3_600_000),
	};
}

export class Escalation {
	readonly config: EscalationConfig;
	/** Since when fewer than minDelivering peers deliver with the scout exhausted (undefined: not now). */
	private fewSince: number | undefined;
	/** Since when the rate is under the bitrate (undefined: not now). */
	private slowSince: number | undefined;
	private decided: EscalationReason | undefined;
	private lastReason: EscalationReason | undefined;
	/** Since when, escalated, the session's own peers are enough (undefined: not now). */
	private healthySince: number | undefined;
	private below = false;
	/** Escalations withdrawn because they were no longer needed. */
	withdrawals = 0;

	constructor(config: Partial<EscalationConfig> = {}) {
		this.config = escalationConfig(config);
	}

	/** Why it escalated, the last time (undefined: never). */
	get reason(): EscalationReason | undefined {
		return this.decided ?? this.lastReason;
	}

	/** Escalated now (decided, and not withdrawn since). */
	get escalated(): boolean {
		return this.decided !== undefined;
	}

	get belowBitrate(): boolean {
		return this.below;
	}

	/** Ms the current "few peers" and "slow" streaks have lasted at `now` (0: none). */
	streaks(now: number): { fewMs: number; slowMs: number } {
		return {
			fewMs: this.fewSince === undefined ? 0 : Math.max(0, now - this.fewSince),
			slowMs: this.slowSince === undefined ? 0 : Math.max(0, now - this.slowSince),
		};
	}

	update(input: EscalationInput): EscalationVerdict {
		const { now } = input;
		if (!input.active) {
			// Nothing wanted (idle, suspended, complete): no streak survives it.
			this.fewSince = undefined;
			this.slowSince = undefined;
			this.healthySince = undefined;
			this.below = false;
			return { belowBitrate: false, escalate: undefined, withdraw: false };
		}
		const few = input.scoutExhausted && input.delivering < this.config.minDelivering;
		const slow = input.bitrate > 0 && Number.isFinite(input.rate) && input.rate < input.bitrate;
		if (few) this.fewSince ??= now;
		else this.fewSince = undefined;
		if (slow) this.slowSince ??= now;
		else this.slowSince = undefined;
		this.below = this.slowSince !== undefined && now - this.slowSince >= this.config.warnAfterMs;
		let escalate: EscalationReason | undefined;
		if (this.decided === undefined) {
			if (this.fewSince !== undefined && now - this.fewSince >= this.config.escalateAfterMs) escalate = "few_peers";
			else if (this.slowSince !== undefined && now - this.slowSince >= this.config.escalateAfterMs) escalate = "slow";
			this.decided = escalate;
			return { belowBitrate: this.below, escalate, withdraw: false };
		}
		// Escalated: is it still needed? Enough of the session's own peers
		// deliver, and fast enough when the bitrate is known.
		const ownDelivering = input.ownDelivering ?? input.delivering;
		const ownRate = input.ownRate ?? input.rate;
		const healthy =
			ownDelivering >= this.config.minDelivering && (input.bitrate <= 0 || (Number.isFinite(ownRate) && ownRate >= input.bitrate));
		if (!healthy) {
			this.healthySince = undefined;
			return { belowBitrate: this.below, escalate, withdraw: false };
		}
		this.healthySince ??= now;
		if (now - this.healthySince < this.config.withdrawAfterMs) return { belowBitrate: this.below, escalate, withdraw: false };
		// Not needed any more: back to the start, a thin swarm later escalates again.
		this.lastReason = this.decided;
		this.decided = undefined;
		this.healthySince = undefined;
		this.fewSince = undefined;
		this.slowSince = undefined;
		this.withdrawals++;
		return { belowBitrate: this.below, escalate, withdraw: true };
	}
}
