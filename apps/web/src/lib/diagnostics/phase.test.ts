import { describe, expect, it } from "vitest";
import { newRecovery, type RecoveryRecord } from "./records";
import { REQUESTS_PER_GB_BUDGET, measurementRow, phaseMetrics } from "./phase";
import { buildReport } from "./report";
import type { Environment } from "./env";

function recovery(over: {
	lookupMs?: number | null;
	firstPiece?: number | null;
	fresh?: boolean;
	bytes?: number;
	recoveringMs?: number;
	tunnel?: RecoveryRecord["tunnel"];
}): RecoveryRecord {
	const r = newRecovery({ fileSize: 1_500_000_000, videos: 1, ext: "mkv" }, Date.UTC(2026, 10, 18));
	r.lookupMs = over.lookupMs ?? null;
	r.milestones.firstPiece = over.firstPiece ?? null;
	r.traits.freshStart = over.fresh ?? true;
	r.transfer.bytes = over.bytes ?? 0;
	r.transfer.recoveringMs = over.recoveringMs ?? 0;
	r.tunnel = over.tunnel;
	r.outcome = "complete";
	return r;
}

describe("phase metrics", () => {
	it("computes the three numbers: first piece from paste, sustained MB/s, billed requests per GB", () => {
		const a = recovery({
			lookupMs: 1_200,
			firstPiece: 800,
			bytes: 2e9,
			recoveringMs: 400_000,
			tunnel: { upgrades: 2, messages: 600, known: 80, delivered: 9 },
		});
		const b = recovery({
			lookupMs: 3_000,
			firstPiece: 2_000,
			bytes: 1e9,
			recoveringMs: 200_000,
			tunnel: { upgrades: 1, messages: 300, known: 20, delivered: 3 },
		});
		const m = phaseMetrics([a, b]);
		// a: 2.0 s, b: 5.0 s.
		expect(m.firstPieceSeconds).toEqual({ median: 2, p90: 5, n: 2 });
		// 3e9 bytes in 600 s.
		expect(m.sustainedMBps).toBe(5);
		// (3 upgrades + 900 / 20) = 48 requests for 3 GB.
		expect(m.billed).toEqual({ requests: 48, upgrades: 3, messages: 900, bytes: 3e9 });
		expect(m.billedRequestsPerGB).toBe(16);
		expect(m.budgetPerGB).toBe(REQUESTS_PER_GB_BUDGET);
		expect(m.withinBudget).toBe(true);
		expect(m.recoveries).toBe(2);
	});

	it("leaves out resumed videos from the first-piece time and flags a run over budget", () => {
		const resumed = recovery({ firstPiece: 100, fresh: false, bytes: 1e8, recoveringMs: 1_000, tunnel: { upgrades: 3, messages: 100, known: 1, delivered: 1 } });
		const m = phaseMetrics([resumed]);
		expect(m.firstPieceSeconds.n).toBe(0);
		expect(m.firstPieceSeconds.median).toBeNull();
		// 3 + 5 = 8 requests for 0.1 GB.
		expect(m.billedRequestsPerGB).toBe(80);
		expect(m.withinBudget).toBe(false);
	});

	it("has no numbers without data, and older records (no tunnel counters) do not count", () => {
		expect(phaseMetrics([]).billedRequestsPerGB).toBeNull();
		expect(phaseMetrics([]).withinBudget).toBeNull();
		expect(phaseMetrics([recovery({ bytes: 1e9, recoveringMs: 1_000 })]).billedRequestsPerGB).toBeNull();
	});

	it("writes an anonymous measurement row", () => {
		const r = recovery({ lookupMs: 1_000, firstPiece: 500, bytes: 5e8, recoveringMs: 100_000, tunnel: { upgrades: 1, messages: 100, known: 40, delivered: 6 } });
		expect(measurementRow(r, "2026-11-18T09:00Z")).toBe("| 2026-11-18 | 2026-11-18T09:00Z | 1500 MB | 40 / 6 | 1.5 | 5.0 | 12.0 | complete |");
	});
});

describe("report", () => {
	it("carries the phase metrics and the measurement rows, without addresses or magnets", () => {
		const r = recovery({ lookupMs: 1_000, firstPiece: 500, bytes: 5e8, recoveringMs: 100_000, tunnel: { upgrades: 1, messages: 100, known: 40, delivered: 6 } });
		const env = { browser: "Chrome", os: "Linux" } as unknown as Environment;
		const visit = {
			v: 1 as const,
			id: "x",
			build: "b",
			startedAt: 0,
			updatedAt: 0,
			env,
			flow: {},
			metadataApi: { ok: 0, fail: 0, ms: { b: [], n: 0, sum: 0, max: 0 }, errors: {} },
			seedersCount: { ok: 0, fail: 0, ms: { b: [], n: 0, sum: 0, max: 0 }, errors: {} },
			errors: [],
			recoveries: [r],
			droppedRecoveries: 0,
		};
		const report = buildReport({ visits: [visit], installId: "i", build: "b", environment: env, storage: null, now: 0 });
		expect(report.phaseMetrics.billedRequestsPerGB).toBe(12);
		expect(report.measurementRows).toHaveLength(1);
		const text = JSON.stringify(report);
		expect(text).not.toMatch(/magnet:\?/);
		expect(text).not.toMatch(/\b\d{1,3}(\.\d{1,3}){3}:\d+\b/);
	});
});
