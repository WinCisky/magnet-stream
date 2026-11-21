// Where playback resumes when a saved video is opened again (M3-04): the
// position kept in the session memory, unless it is too close to either
// end to be worth it (a few seconds in, or the credits).

/** Less than this far in: start from the beginning. */
export const RESUME_MIN_MS = 10_000;
/** Closer than this to the end (or past 95 %): start from the beginning. */
export const RESUME_END_MS = 30_000;

/** The millisecond to resume at, 0 for the start. */
export function resumePoint(position: { ms: number; durationMs: number } | null | undefined): number {
	if (!position) return 0;
	const { ms, durationMs } = position;
	if (!Number.isFinite(ms) || !Number.isFinite(durationMs) || durationMs <= 0 || ms < RESUME_MIN_MS) return 0;
	if (ms > durationMs - RESUME_END_MS || ms > durationMs * 0.95) return 0;
	return Math.floor(ms);
}
