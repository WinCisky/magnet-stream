import { describe, expect, it } from "vitest";
import { PieceState } from "./scheduler";
import { PieceTracker } from "./tracker";

const PL = 1024 * 1024;
const range = { offset: PL / 2, size: 10 * PL, firstPiece: 0, lastPiece: 10 };

describe("piece tracker", () => {
	it("tracks verified pieces, the front and seeks", () => {
		const t = new PieceTracker(range, PL);
		expect(t.total).toBe(11);
		expect(t.markVerified(0)).toBe(true);
		expect(t.markVerified(0)).toBe(false);
		expect(t.markVerified(99)).toBe(false);
		t.markVerified(1);
		expect(t.front).toBe(2);
		expect(t.contiguousFromStart()).toBe(2 * PL - PL / 2);
		t.setCursor(6);
		expect(t.seeks).toBe(1);
		expect(t.front).toBe(6);
		expect(t.inWindow(6, 3 * PL)).toBe(true);
		expect(t.inWindow(9, 3 * PL)).toBe(false);
		expect(t.inWindow(5, 3 * PL)).toBe(false);
		expect(t.forget(1)).toBe(true);
		expect(t.forget(1)).toBe(false);
		expect(t.verifiedPieces()).toEqual([0]);
		for (let p = 0; p <= 10; p++) t.markVerified(p);
		expect(t.complete).toBe(true);
	});

	it("shows pieces being assembled, verifying and failed", () => {
		const t = new PieceTracker(range, PL);
		t.markVerified(0);
		t.setAssembling([1, 32, 64, 2, 64, 64, 0, 1, 64], 0);
		const states = new Uint8Array(11);
		const progress = new Uint8Array(11);
		t.fillStates(states, progress, 0);
		expect(states[0]).toBe(PieceState.Verified);
		expect(states[1]).toBe(PieceState.Downloading);
		expect(progress[1]).toBe(127);
		expect(states[2]).toBe(PieceState.Verifying);
		expect(states[3]).toBe(PieceState.Pending);
		expect(t.active).toBe(2);
		// Piece 2 left the list unverified: it failed its hash check.
		t.setAssembling([1, 40, 64], 100);
		t.fillStates(states, progress, 200);
		expect(states[2]).toBe(PieceState.Failed);
		t.fillStates(states, progress, 5_000);
		expect(states[2]).toBe(PieceState.Pending);
	});
});
