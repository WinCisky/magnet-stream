import { describe, expect, it } from "vitest";
import { LIMITER_UNITS, WindowCounter, allowUpgrade, clientKey, deltaFor, type RateLimiterBinding } from "../../src/tunnel/rate-limit";

/** The binding's documented semantics: a call succeeds while the count before it is below the limit. */
function fakeBinding(limit = LIMITER_UNITS): RateLimiterBinding & { calls: { key: string; delta?: number }[] } {
	const counts = new Map<string, number>();
	const calls: { key: string; delta?: number }[] = [];
	return {
		calls,
		limit: async (o) => {
			calls.push(o);
			const before = counts.get(o.key) ?? 0;
			if (before >= limit) return { success: false };
			counts.set(o.key, before + (o.delta ?? 1));
			return { success: true };
		},
	};
}

async function allowedOutOf(n: number, perMinute: number, binding?: RateLimiterBinding, counter = new WindowCounter()): Promise<number> {
	let ok = 0;
	for (let i = 0; i < n; i++) if (await allowUpgrade(binding, "upgrade:203.0.113.9", perMinute, 0, counter)) ok++;
	return ok;
}

describe("upgrade rate limit", () => {
	it("lets exactly the configured number through the binding", async () => {
		expect(deltaFor(30)).toBe(200);
		expect(await allowedOutOf(50, 30, fakeBinding())).toBe(30);
		expect(await allowedOutOf(50, 5, fakeBinding())).toBe(5);
		expect(await allowedOutOf(700, 600, fakeBinding())).toBe(600);
	});

	it("keys per client", async () => {
		const b = fakeBinding();
		for (let i = 0; i < 30; i++) await allowUpgrade(b, "upgrade:a", 30);
		expect(await allowUpgrade(b, "upgrade:a", 30)).toBe(false);
		expect(await allowUpgrade(b, "upgrade:b", 30)).toBe(true);
	});

	it("falls back to a local counter without the binding or when it throws", async () => {
		expect(await allowedOutOf(50, 30)).toBe(30);
		const broken: RateLimiterBinding = { limit: () => Promise.reject(new Error("down")) };
		expect(await allowedOutOf(50, 7, broken)).toBe(7);
	});

	it("local windows reset after a minute and stay bounded", () => {
		const c = new WindowCounter(4);
		expect(c.hit("k", 1, 0)).toBe(true);
		expect(c.hit("k", 1, 59_999)).toBe(false);
		expect(c.hit("k", 1, 60_000)).toBe(true);
		for (let i = 0; i < 100; i++) c.hit(`flood${i}`, 1, 60_000);
		expect((c as unknown as { windows: Map<string, unknown> }).windows.size).toBeLessThanOrEqual(4);
	});

	it("keys on cf-connecting-ip", () => {
		expect(clientKey(new Request("https://x/", { headers: { "cf-connecting-ip": "198.51.100.4" } }))).toBe("upgrade:198.51.100.4");
		expect(clientKey(new Request("https://x/"))).toBe("upgrade:unknown");
	});
});
