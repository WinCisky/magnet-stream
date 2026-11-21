// The player's position in its source: libmedia's IO loader seeks and
// reads through this. Reads that have to wait (the data isn't in yet) flip
// `waiting` after a moment, for the buffering spinner; once such a read's
// data is in, `hold` may keep it back longer (playback resumes only with
// a few seconds verified, buffering.ts), the spinner still up.

import type { ByteSource } from "./source";

/** A read pending longer than this means playback is waiting for data. */
const WAITING_AFTER_MS = 400;

export class ReadCursor {
	pos = 0;
	/** When the current wait for data began (null: not waiting). */
	waitingSince: number | null = null;
	private readonly controller = new AbortController();
	private pending = 0;
	private timer: ReturnType<typeof setTimeout> | undefined;

	constructor(
		private readonly source: ByteSource,
		/** Waiting for data began (true) or ended (false, with how long it lasted). */
		private readonly onWaiting: (waiting: boolean, ms: number) => void = () => {},
		private readonly now: () => number = () => performance.now(),
		private readonly waitingAfterMs = WAITING_AFTER_MS,
		/** After a read that left playback waiting: resolve when it may go on (from `offset`). */
		private readonly hold?: (offset: number, signal: AbortSignal) => Promise<void>,
	) {}

	size(): Promise<number> {
		return this.source.size(this.controller.signal);
	}

	seek(pos: number): void {
		this.pos = Math.max(0, pos);
	}

	/** Read at the position and advance; 0 at the end. */
	async read(into: Uint8Array): Promise<number> {
		const startedAt = this.now();
		if (this.pending++ === 0) this.timer = setTimeout(() => this.startWaiting(startedAt), this.waitingAfterMs);
		try {
			const at = this.pos;
			const n = await this.source.read(at, into, this.controller.signal);
			if (n > 0 && this.waitingSince !== null && this.hold) await this.hold(at, this.controller.signal);
			this.pos = at + n;
			return n;
		} finally {
			if (--this.pending === 0) {
				clearTimeout(this.timer);
				this.stopWaiting();
			}
		}
	}

	/** Abort reads in flight and every later one. */
	stop(): void {
		this.controller.abort();
		clearTimeout(this.timer);
		this.stopWaiting();
	}

	get stopped(): boolean {
		return this.controller.signal.aborted;
	}

	private startWaiting(since: number): void {
		if (this.waitingSince !== null || this.stopped) return;
		this.waitingSince = since;
		this.onWaiting(true, 0);
	}

	private stopWaiting(): void {
		if (this.waitingSince === null) return;
		const ms = this.now() - this.waitingSince;
		this.waitingSince = null;
		this.onWaiting(false, ms);
	}
}
