import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FALLBACK_BITRATE, aheadOf, averageBitrate, bufferReady, byteAt, waitForBuffer, type BufferView } from "./buffering";

/** A file of `size` bytes with these [start, end) runs verified. */
function view(size: number, bitrate: number | null, runs: [number, number][]): BufferView {
	return {
		size,
		bitrate,
		available: (offset, limit) => {
			const run = runs.find(([a, b]) => offset >= a && offset < b);
			return run ? Math.min(run[1] - offset, limit) : 0;
		},
	};
}

describe("bitrate", () => {
	it("is the size over the duration, and nothing for nonsense", () => {
		expect(averageBitrate(16_000_000, 40_000)).toBe(400_000);
		expect(averageBitrate(1_000, 0)).toBeNull();
		expect(averageBitrate(0, 1_000)).toBeNull();
		expect(averageBitrate(100, 60_000)).toBeNull(); // 1.7 B/s: a broken duration
		expect(averageBitrate(Number.NaN, 1_000)).toBeNull();
		expect(averageBitrate(5e12, 1_000)).toBeNull();
	});

	it("maps a time to a byte proportionally, within the file", () => {
		expect(byteAt(0, 40_000, 16_000)).toBe(0);
		expect(byteAt(10_000, 40_000, 16_000)).toBe(4_000);
		expect(byteAt(50_000, 40_000, 16_000)).toBe(15_999);
		expect(byteAt(-5, 40_000, 16_000)).toBe(0);
		expect(byteAt(1_000, 0, 16_000)).toBe(0);
	});
});

describe("start and resume thresholds", () => {
	// 100 kB/s of video, 60 s: 6 MB.
	const size = 6_000_000;
	const rate = 100_000;

	it("starts with 10 s verified from the position, resumes with 5 s", () => {
		const nine = view(size, rate, [[0, 900_000]]);
		expect(bufferReady("start", nine, 0)).toBe(false);
		expect(bufferReady("resume", nine, 0)).toBe(true);
		expect(bufferReady("resume", nine, 500_000)).toBe(false); // 4 s from there
		const ten = view(size, rate, [[0, 1_000_000]]);
		expect(bufferReady("start", ten, 0)).toBe(true);
		// Nothing at the position: no amount elsewhere counts.
		const elsewhere = view(size, rate, [[2_000_000, 6_000_000]]);
		expect(bufferReady("resume", elsewhere, 1_000_000)).toBe(false);
		expect(bufferReady("start", elsewhere, 2_500_000)).toBe(true);
	});

	it("needs no more than what is left of the file", () => {
		const tail = view(size, rate, [[5_800_000, size]]);
		expect(aheadOf(tail, 5_800_000, 10)).toEqual({ ms: 2_000, toEnd: true });
		expect(bufferReady("start", tail, 5_800_000)).toBe(true);
		expect(bufferReady("start", tail, 5_700_000)).toBe(false);
	});

	it("scales with the bitrate, and uses a default bitrate until it is known", () => {
		const run = view(size, 50_000, [[0, 500_000]]);
		expect(bufferReady("start", run, 0)).toBe(true); // 10 s at 50 kB/s
		expect(bufferReady("start", { ...run, bitrate: 200_000 }, 0)).toBe(false); // 2.5 s at 200 kB/s
		const unknown = view(100_000_000, null, [[0, 10 * FALLBACK_BITRATE]]);
		expect(bufferReady("start", unknown, 0)).toBe(true);
		expect(bufferReady("start", unknown, 1)).toBe(false);
	});

	it("asks the source for no more than the threshold", () => {
		const limits: number[] = [];
		const v: BufferView = { size, bitrate: rate, available: (_o, limit) => (limits.push(limit), limit) };
		expect(aheadOf(v, 0, 5)).toEqual({ ms: 5_000, toEnd: false });
		expect(limits).toEqual([500_000]);
	});
});

describe("waitForBuffer", () => {
	beforeEach(() => vi.useFakeTimers());
	afterEach(() => vi.useRealTimers());

	it("holds until the threshold is met at the current position, then reports what was ahead", async () => {
		let verified = 0;
		let position = 0;
		let clock = 0;
		const v = (): BufferView => ({ size: 6_000_000, bitrate: 100_000, available: (o, limit) => Math.min(limit, Math.max(0, verified - o)) });
		const done = vi.fn();
		const hold = waitForBuffer("resume", v, () => position, new AbortController().signal, { now: () => clock }).then(done);
		await vi.advanceTimersByTimeAsync(1_000);
		expect(done).not.toHaveBeenCalled();
		verified = 400_000; // 4 s
		await vi.advanceTimersByTimeAsync(250);
		expect(done).not.toHaveBeenCalled();
		position = 200_000; // a seek back: 2 s ahead from there
		verified = 600_000;
		await vi.advanceTimersByTimeAsync(250);
		expect(done).not.toHaveBeenCalled();
		verified = 700_000;
		clock = 1_750;
		await vi.advanceTimersByTimeAsync(250);
		await hold;
		expect(done).toHaveBeenCalledWith({ kind: "resume", aheadMs: 5_000, toEnd: false, heldMs: 1_750 });
	});

	it("resolves at once when the data is there, and rejects when aborted", async () => {
		const full = (): BufferView => ({ size: 10, bitrate: 100_000, available: (o) => 10 - o });
		await expect(waitForBuffer("start", full, () => 0, new AbortController().signal)).resolves.toMatchObject({ toEnd: true, heldMs: 0 });
		const controller = new AbortController();
		const empty = (): BufferView => ({ size: 1_000_000, bitrate: 100_000, available: () => 0 });
		const pending = waitForBuffer("start", empty, () => 0, controller.signal);
		await vi.advanceTimersByTimeAsync(500);
		controller.abort(new Error("paused"));
		await expect(pending).rejects.toThrow("paused");
		const aborted = new AbortController();
		aborted.abort(new Error("gone"));
		await expect(waitForBuffer("start", empty, () => 0, aborted.signal)).rejects.toThrow("gone");
	});
});
