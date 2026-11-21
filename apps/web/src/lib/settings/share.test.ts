import { describe, expect, it } from "vitest";
import { SHARE_KEY, onShareChange, parseShare, readShare, writeShare } from "./share";

function memory(initial: Record<string, string> = {}) {
	const data = new Map(Object.entries(initial));
	return { getItem: (k: string) => data.get(k) ?? null, setItem: (k: string, v: string) => void data.set(k, v), data };
}

describe("share with peers setting", () => {
	it("is on by default, and anything but the two exact values reads as the default", () => {
		expect(parseShare(null)).toBe(true);
		expect(parseShare("off")).toBe(false);
		expect(parseShare("on")).toBe(true);
		for (const bad of ["OFF", "0", "false", " off", "", undefined, 0, {}]) expect(parseShare(bad)).toBe(true);
		expect(readShare(memory())).toBe(true);
		expect(readShare(undefined)).toBe(true);
		const broken = { getItem: () => { throw new Error("denied"); }, setItem: () => { throw new Error("denied"); } };
		expect(readShare(broken)).toBe(true);
		expect(() => writeShare(false, broken)).not.toThrow();
	});

	it("is kept and announced", () => {
		const store = memory();
		const seen: boolean[] = [];
		const off = onShareChange((on) => seen.push(on));
		writeShare(false, store);
		expect(store.data.get(SHARE_KEY)).toBe("off");
		expect(readShare(store)).toBe(false);
		writeShare(true, store);
		off();
		writeShare(false, store);
		expect(seen).toEqual([false, true]);
	});
});
