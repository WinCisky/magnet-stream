// The three numbers every delivery phase is judged on (docs/architecture.md
// §Risks, open questions and delivery; docs/measurements.md):
//
//   1. seconds from pasting the magnet to the first verified piece
//      (the lookup, then from opening the video; the time spent choosing a
//      file is not counted), for videos not saved before;
//   2. sustained MB/s: bytes received over the time spent recovering;
//   3. billed Cloudflare requests per GB: (WebSocket upgrades + messages
//      sent / 20) per 10^9 bytes received.
//
// Units are decimal (MB = 10^6 bytes, GB = 10^9 bytes). Computed in the
// browser from the diagnostics records; nothing is sent anywhere.

import type { RecoveryRecord } from "./records";

/** Durable Object WebSocket billing: 20 incoming messages are 1 request. */
export const MESSAGES_PER_REQUEST = 20;
/** Budget agreed for M2 (decision OQ-19): at most 40 billed requests per GB. */
export const REQUESTS_PER_GB_BUDGET = 40;
const MB = 1e6;
const GB = 1e9;

export interface PhaseMetrics {
	/** Recoveries that contributed to at least one number. */
	recoveries: number;
	firstPieceSeconds: { median: number | null; p90: number | null; n: number };
	sustainedMBps: number | null;
	billedRequestsPerGB: number | null;
	/** Billed requests and bytes behind the per-GB figure. */
	billed: { requests: number; upgrades: number; messages: number; bytes: number };
	budgetPerGB: number;
	/** null without data. */
	withinBudget: boolean | null;
}

/** ms from paste to the first verified piece, or null if unknown or resumed. */
export function firstPieceMs(r: RecoveryRecord): number | null {
	const first = r.milestones.firstPiece;
	if (first === null || r.traits.freshStart !== true) return null;
	return (r.lookupMs ?? 0) + first;
}

/** Billed requests charged to one recovery (null without tunnel counters). */
export function billedRequests(r: RecoveryRecord): number | null {
	if (!r.tunnel) return null;
	return r.tunnel.upgrades + r.tunnel.messages / MESSAGES_PER_REQUEST;
}

export function phaseMetrics(recoveries: readonly RecoveryRecord[]): PhaseMetrics {
	const firsts = recoveries.map(firstPieceMs).filter((ms): ms is number => ms !== null);
	const recovering = recoveries.filter((r) => r.transfer.recoveringMs > 0 && r.transfer.bytes > 0);
	const recoveringMs = recovering.reduce((n, r) => n + r.transfer.recoveringMs, 0);
	const recoveringBytes = recovering.reduce((n, r) => n + r.transfer.bytes, 0);
	const tunnelled = recoveries.filter((r) => r.tunnel && r.transfer.bytes > 0);
	const billed = { requests: 0, upgrades: 0, messages: 0, bytes: 0 };
	for (const r of tunnelled) {
		billed.upgrades += r.tunnel!.upgrades;
		billed.messages += r.tunnel!.messages;
		billed.bytes += r.transfer.bytes;
	}
	billed.requests = round(billed.upgrades + billed.messages / MESSAGES_PER_REQUEST);
	const perGB = billed.bytes > 0 ? round(billed.requests / (billed.bytes / GB)) : null;
	const ids = new Set([...recoveries.filter((r) => firstPieceMs(r) !== null), ...recovering, ...tunnelled].map((r) => r.id));
	return {
		recoveries: ids.size,
		firstPieceSeconds: { median: seconds(percentile(firsts, 0.5)), p90: seconds(percentile(firsts, 0.9)), n: firsts.length },
		sustainedMBps: recoveringMs > 0 ? round(recoveringBytes / MB / (recoveringMs / 1000)) : null,
		billedRequestsPerGB: perGB,
		billed,
		budgetPerGB: REQUESTS_PER_GB_BUDGET,
		withinBudget: perGB === null ? null : perGB <= REQUESTS_PER_GB_BUDGET,
	};
}

/**
 * One row for the real-world table of docs/measurements.md, from one
 * recovery. Anonymous: rounded sizes, no hash, name, magnet or address.
 * Columns: date | build | file size | peers (known / delivered) | first piece s | MB/s | requests/GB | outcome
 */
export function measurementRow(r: RecoveryRecord, build: string): string {
	const first = firstPieceMs(r);
	const mbps = r.transfer.recoveringMs > 0 ? r.transfer.bytes / MB / (r.transfer.recoveringMs / 1000) : null;
	const requests = billedRequests(r);
	const perGB = requests !== null && r.transfer.bytes > 0 ? requests / (r.transfer.bytes / GB) : null;
	const cells = [
		new Date(r.startedAt).toISOString().slice(0, 10),
		build,
		`${(r.traits.fileSize / MB).toFixed(0)} MB`,
		r.tunnel ? `${r.tunnel.known} / ${r.tunnel.delivered}` : "n/a",
		first === null ? "n/a" : (first / 1000).toFixed(1),
		mbps === null ? "n/a" : mbps.toFixed(1),
		perGB === null ? "n/a" : perGB.toFixed(1),
		r.outcome,
	];
	return `| ${cells.join(" | ")} |`;
}

function percentile(values: number[], q: number): number | null {
	if (values.length === 0) return null;
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1))];
}

function seconds(ms: number | null): number | null {
	return ms === null ? null : Math.round(ms / 100) / 10;
}

function round(n: number): number {
	return Math.round(n * 100) / 100;
}
