// Per-IP limit on WebSocket upgrades (OQ-08: 30 per minute by default),
// checked in the Worker before any Durable Object is created.
//
// The Workers Rate Limiting binding (`UPGRADE_LIMITER` in wrangler.jsonc)
// is configured with a fixed budget of LIMITER_UNITS per 60 s; each upgrade
// consumes LIMITER_UNITS / TUNNEL_UPGRADES_PER_MINUTE units (the binding's
// `delta`), which makes the per-minute limit a plain variable. The binding
// is approximate and per Cloudflare location, which is all a minimal abuse
// limit needs. Without the binding (a test or a misdeployed config) an
// in-isolate fixed-window counter applies the same limit.
//
// The client IP is used only as the limiter key: it is never logged.

/** Must equal `ratelimits[UPGRADE_LIMITER].simple.limit` in wrangler.jsonc. */
export const LIMITER_UNITS = 6_000;
export const LIMITER_PERIOD_MS = 60_000;

export interface RateLimiterBinding {
	limit(options: { key: string; delta?: number }): Promise<{ success: boolean }>;
}

/** Units one upgrade consumes so that `perMinute` upgrades fit LIMITER_UNITS. */
export function deltaFor(perMinute: number): number {
	return Math.max(1, Math.round(LIMITER_UNITS / perMinute));
}

/** Fixed one-minute windows per key, bounded in memory. */
export class WindowCounter {
	private readonly windows = new Map<string, { start: number; count: number }>();
	private readonly maxKeys: number;

	constructor(maxKeys = 10_000) {
		this.maxKeys = maxKeys;
	}

	/** Count one hit for `key`; false once `limit` hits were counted in the current window. */
	hit(key: string, limit: number, now: number): boolean {
		let w = this.windows.get(key);
		if (!w || now - w.start >= LIMITER_PERIOD_MS) {
			if (!w && this.windows.size >= this.maxKeys) this.prune(now);
			w = { start: now, count: 0 };
			this.windows.set(key, w);
		}
		if (w.count >= limit) return false;
		w.count++;
		return true;
	}

	private prune(now: number): void {
		for (const [k, w] of this.windows) if (now - w.start >= LIMITER_PERIOD_MS) this.windows.delete(k);
		// Still full (a flood of distinct keys): forget the oldest half.
		if (this.windows.size >= this.maxKeys) {
			let drop = Math.ceil(this.windows.size / 2);
			for (const k of this.windows.keys()) {
				if (drop-- <= 0) break;
				this.windows.delete(k);
			}
		}
	}
}

const fallback = new WindowCounter();

/** The key the limiter counts on: the client IP as Cloudflare reports it. */
export function clientKey(request: Request): string {
	const ip = request.headers.get("cf-connecting-ip");
	return `upgrade:${ip && ip.length <= 64 ? ip : "unknown"}`;
}

export async function allowUpgrade(
	binding: RateLimiterBinding | undefined,
	key: string,
	perMinute: number,
	now = Date.now(),
	counter: WindowCounter = fallback,
): Promise<boolean> {
	if (!binding) return counter.hit(key, perMinute, now);
	try {
		return (await binding.limit({ key, delta: deltaFor(perMinute) })).success;
	} catch {
		// The binding failed: fall back to the local counter rather than fail open.
		return counter.hit(key, perMinute, now);
	}
}
