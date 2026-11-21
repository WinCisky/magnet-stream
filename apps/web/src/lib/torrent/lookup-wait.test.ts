import { describe, expect, it } from "vitest";
import { CONNECT_GRACE_MS, DEFAULT_LOOKUP_CAP_MS, LookupWait, MAX_LOOKUP_CAP_MS, MIN_LOOKUP_CAP_MS, clampCap, failureMessage, type LookupFacts } from "./lookup-wait";

const nothing: LookupFacts = { discoveryDone: true, candidates: 0, dialling: 0, fetching: 0, lastConnectAt: 0, checking: false };

/** A wait driven by a simulated clock, sampled every 500 ms like the worker. */
function run(wait: LookupWait, facts: (t: number) => LookupFacts, until: number): { at: number; reason: string } | null {
	for (let t = 0; t <= until; t += 500) {
		const v = wait.judge(facts(t), t);
		if (v.state === "failed") return { at: t, reason: v.reason };
	}
	return null;
}

describe("LookupWait", () => {
	it("never gives up before the cap while candidates remain, then gives up at the cap", () => {
		const wait = new LookupWait(0);
		expect(wait.capMs).toBe(DEFAULT_LOOKUP_CAP_MS);
		// Discovery over, dead addresses being dialled one after the other.
		const result = run(wait, (t) => ({ ...nothing, candidates: 20, dialling: t % 20_000 < 10_000 ? 3 : 0 }), 200_000);
		expect(result).toEqual({ at: 120_000, reason: "cap" });
	});

	it("waits while discovery runs, dials are in flight, peers are asked, or an info dict is checked", () => {
		const wait = new LookupWait(0);
		for (const hope of [
			{ discoveryDone: false },
			{ candidates: 1 },
			{ dialling: 2 },
			{ fetching: 1 },
			{ checking: true },
			{ lastConnectAt: 9_000 },
		] satisfies Partial<LookupFacts>[]) {
			expect(wait.judge({ ...nothing, ...hope }, 10_000)).toEqual({ state: "waiting" });
		}
	});

	it("gives up at once when discovery is over and nothing is left to try", () => {
		const wait = new LookupWait(0);
		// The stream ends at 3 s with every address dead; the last dial fails at 4 s.
		const facts = (t: number): LookupFacts => ({ ...nothing, discoveryDone: t >= 3_000, dialling: t < 4_000 ? 1 : 0 });
		expect(run(wait, facts, 200_000)).toEqual({ at: 4_000, reason: "exhausted" });
	});

	it("gives a peer that just connected time to offer the metadata", () => {
		const wait = new LookupWait(0);
		const facts = (): LookupFacts => ({ ...nothing, lastConnectAt: 10_000 });
		expect(run(wait, (t) => (t < 10_000 ? { ...nothing, dialling: 1 } : facts()), 200_000)).toEqual({ at: 10_000 + CONNECT_GRACE_MS, reason: "exhausted" });
	});

	it("restarts the cap on a retry", () => {
		const wait = new LookupWait(0, 10_000);
		const busy = { ...nothing, candidates: 5 };
		expect(wait.judge(busy, 10_000)).toEqual({ state: "failed", reason: "cap" });
		wait.restart(10_000);
		expect(wait.elapsed(15_000)).toBe(5_000);
		expect(wait.judge(busy, 19_500)).toEqual({ state: "waiting" });
		expect(wait.judge(busy, 20_000)).toEqual({ state: "failed", reason: "cap" });
	});

	it("keeps the cap within bounds", () => {
		expect(clampCap(undefined)).toBe(DEFAULT_LOOKUP_CAP_MS);
		expect(clampCap(Number.NaN)).toBe(DEFAULT_LOOKUP_CAP_MS);
		expect(clampCap(1)).toBe(MIN_LOOKUP_CAP_MS);
		expect(clampCap(1e12)).toBe(MAX_LOOKUP_CAP_MS);
		expect(new LookupWait(0, 30_000).capMs).toBe(30_000);
	});

	it("says clearly why a lookup gave up, without addresses", () => {
		const progress = { found: 0, tried: 0, connected: 0, elapsedMs: 2_000, discovering: false };
		expect(failureMessage("exhausted", progress)).toMatch(/No peers were found/);
		expect(failureMessage("exhausted", { ...progress, found: 7, tried: 7 })).toMatch(/None of the 7 peers found sent the file list \(7 tried\)/);
		expect(failureMessage("cap", { ...progress, found: 300, tried: 40, connected: 2, elapsedMs: 120_000 })).toMatch(/within 120 s \(300 peers found, 40 tried, 2 connected\)/);
		expect(failureMessage("tunnel", progress)).toMatch(/tunnel gateway/);
	});
});
