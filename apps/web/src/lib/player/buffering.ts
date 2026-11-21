// When playback may start, and when it resumes after running dry
// (docs/architecture.md §Playback): play once the container index (the
// player has read it) and about 10 s of video are verified; rebuffer only
// when the player actually ran out of data, and resume once 5 s are
// verified from where it stands. Seconds become bytes through the video's
// average bitrate (the file's size over its duration), which the picker
// also uses for its window. Pure, apart from the polling in `waitForBuffer`.

/** Video verified ahead before the first play (seconds). */
export const START_SECONDS = 10;
/** Video verified ahead before playback resumes after running dry (seconds). */
export const RESUME_SECONDS = 5;
/**
 * Bytes per second assumed while the duration is unknown: 5 Mbit/s, the
 * picker's own default (packages/engine picker.ts DEFAULT_BITRATE).
 */
export const FALLBACK_BITRATE = 625_000;
/** How often a held start or resume looks at what is verified. */
export const CHECK_MS = 250;
/** Bitrates believed, bytes per second (outside: a broken duration). */
const MIN_BITRATE = 1_000;
const MAX_BITRATE = 1_000_000_000;

export type HoldKind = "start" | "resume";

/** What the thresholds are measured against. */
export interface BufferView {
	/** File size in bytes. */
	size: number;
	/** Average bytes per second of the video (null: unknown). */
	bitrate: number | null;
	/** Bytes readable from `offset` without waiting, counting no further than `limit`. */
	available(offset: number, limit: number): number;
}

export interface Thresholds {
	startSeconds: number;
	resumeSeconds: number;
}

export const DEFAULT_THRESHOLDS: Readonly<Thresholds> = { startSeconds: START_SECONDS, resumeSeconds: RESUME_SECONDS };

/** The video's average bitrate (bytes/s) from its size and duration, or null if they make no sense. */
export function averageBitrate(sizeBytes: number, durationMs: number): number | null {
	if (!Number.isFinite(sizeBytes) || !Number.isFinite(durationMs) || sizeBytes <= 0 || durationMs <= 0) return null;
	const rate = sizeBytes / (durationMs / 1000);
	return rate >= MIN_BITRATE && rate <= MAX_BITRATE ? rate : null;
}

/**
 * The file byte playing at `ms`, assuming bytes spread evenly over time
 * (the container's index would say exactly; ±a few seconds is enough here).
 */
export function byteAt(ms: number, durationMs: number, size: number): number {
	if (!(size > 0) || !(durationMs > 0) || !Number.isFinite(ms)) return 0;
	const fraction = Math.min(1, Math.max(0, ms / durationMs));
	return Math.min(size - 1, Math.floor(fraction * size));
}

export interface Ahead {
	/** Milliseconds of video verified from the offset (capped at what was asked). */
	ms: number;
	/** Everything to the end of the file is verified. */
	toEnd: boolean;
}

/** How much video is verified from `offset`, looking no further than `seconds`. */
export function aheadOf(view: BufferView, offset: number, seconds: number): Ahead {
	const bitrate = view.bitrate ?? FALLBACK_BITRATE;
	const at = Math.min(Math.max(0, Math.floor(offset)), Math.max(0, view.size));
	const need = Math.ceil(seconds * bitrate);
	const bytes = at >= view.size ? 0 : view.available(at, need);
	return { ms: (1000 * bytes) / bitrate, toEnd: at + bytes >= view.size };
}

/** Whether playback may start (`start`) or resume (`resume`) at `offset`. */
export function bufferReady(kind: HoldKind, view: BufferView, offset: number, thresholds: Thresholds = DEFAULT_THRESHOLDS): boolean {
	const seconds = kind === "start" ? thresholds.startSeconds : thresholds.resumeSeconds;
	const ahead = aheadOf(view, offset, seconds);
	return ahead.toEnd || ahead.ms >= seconds * 1000;
}

export interface HoldResult {
	kind: HoldKind;
	/** Video verified ahead when released (ms), and whether that reached the end of the file. */
	aheadMs: number;
	toEnd: boolean;
	/** How long playback was held. */
	heldMs: number;
}

/**
 * Resolve once `bufferReady(kind)` holds at `offset()` (asked again on
 * every check: the viewer may seek meanwhile); reject if `signal` aborts.
 */
export function waitForBuffer(
	kind: HoldKind,
	view: () => BufferView,
	offset: () => number,
	signal: AbortSignal,
	{ thresholds = DEFAULT_THRESHOLDS, intervalMs = CHECK_MS, now = () => performance.now() }: { thresholds?: Thresholds; intervalMs?: number; now?: () => number } = {},
): Promise<HoldResult> {
	const startedAt = now();
	const seconds = kind === "start" ? thresholds.startSeconds : thresholds.resumeSeconds;
	return new Promise((resolve, reject) => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		const onAbort = () => {
			clearTimeout(timer);
			reject(signal.reason);
		};
		const check = () => {
			if (signal.aborted) return;
			const v = view();
			const at = offset();
			if (bufferReady(kind, v, at, thresholds)) {
				signal.removeEventListener("abort", onAbort);
				const ahead = aheadOf(v, at, seconds);
				resolve({ kind, aheadMs: Math.round(ahead.ms), toEnd: ahead.toEnd, heldMs: Math.round(now() - startedAt) });
				return;
			}
			timer = setTimeout(check, intervalMs);
		};
		if (signal.aborted) return reject(signal.reason);
		signal.addEventListener("abort", onAbort, { once: true });
		check();
	});
}
