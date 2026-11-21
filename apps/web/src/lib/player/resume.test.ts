import { describe, expect, it } from "vitest";
import { RESUME_END_MS, RESUME_MIN_MS, resumePoint } from "./resume";

describe("resumePoint", () => {
	const hour = 3_600_000;

	it("resumes where playback was", () => {
		expect(resumePoint({ ms: 600_000, durationMs: hour })).toBe(600_000);
		expect(resumePoint({ ms: 1234.7, durationMs: hour })).toBe(0);
		expect(resumePoint({ ms: RESUME_MIN_MS + 0.5, durationMs: hour })).toBe(RESUME_MIN_MS);
	});

	it("starts over near either end, or without a usable position", () => {
		expect(resumePoint(null)).toBe(0);
		expect(resumePoint({ ms: RESUME_MIN_MS - 1, durationMs: hour })).toBe(0);
		expect(resumePoint({ ms: hour - RESUME_END_MS + 1, durationMs: hour })).toBe(0);
		expect(resumePoint({ ms: hour, durationMs: hour })).toBe(0);
		expect(resumePoint({ ms: 96_000, durationMs: 100_000 })).toBe(0);
		expect(resumePoint({ ms: 50_000, durationMs: 0 })).toBe(0);
		expect(resumePoint({ ms: Number.NaN, durationMs: hour })).toBe(0);
	});
});
