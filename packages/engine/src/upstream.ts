// Upstream batching (docs/architecture.md §Tunnel protocol and request
// budget › Rules, docs/tunnel-protocol.md › Session lifetime): every frame
// for one WebSocket is held and sent as one message at most every
// 300–500 ms. Billing counts messages, not frames. An urgent frame (a
// cancel after a seek) skips the timer and flushes whatever is queued.
// A `ping` goes out when nothing else has been sent for the cadence in
// docs/spike-report.md (30 s), so a quiet session never hits the idle timeout.

import { PING_INTERVAL_MS, SESSION_CONN, type Frame } from "@magnet-stream/tunnel-protocol";

/** Midpoint of the 300–500 ms band. */
export const DEFAULT_BATCH_MS = 400;
export { PING_INTERVAL_MS };

export interface UpstreamBatcherOptions {
	/** Flush interval (default 400). Any positive value: tests use a short one. */
	intervalMs?: number;
	now?: () => number;
	setTimer?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
	clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
	/** One WebSocket message. */
	send(frames: Frame[]): void;
}

export class UpstreamBatcher {
	private readonly intervalMs: number;
	private readonly now: () => number;
	private readonly setTimer: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
	private readonly clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
	private readonly sendMessage: (frames: Frame[]) => void;
	private queue: Frame[] = [];
	private timer: ReturnType<typeof setTimeout> | undefined;
	private lastSentAt: number;
	private urgentPending = false;
	messages = 0;

	constructor(options: UpstreamBatcherOptions) {
		this.intervalMs = options.intervalMs ?? DEFAULT_BATCH_MS;
		this.now = options.now ?? Date.now;
		this.setTimer = options.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
		this.clearTimer = options.clearTimer ?? ((timer) => clearTimeout(timer));
		this.sendMessage = options.send;
		this.lastSentAt = this.now();
	}

	get pending(): number {
		return this.queue.length;
	}

	/**
	 * Queue frames. `urgent` flushes at the end of the current task, with
	 * whatever was waiting: urgent frames sent together (closes, cancels)
	 * still make one message.
	 */
	enqueue(frames: readonly Frame[], urgent = false): void {
		if (frames.length === 0) return;
		this.queue.push(...frames);
		if (urgent) {
			if (!this.urgentPending) {
				this.urgentPending = true;
				queueMicrotask(() => {
					this.urgentPending = false;
					this.flush();
				});
			}
		} else if (this.timer === undefined) this.timer = this.setTimer(() => this.flush(), this.intervalMs);
	}

	flush(): void {
		if (this.timer !== undefined) {
			this.clearTimer(this.timer);
			this.timer = undefined;
		}
		if (this.queue.length === 0) return;
		const frames = this.queue;
		this.queue = [];
		this.lastSentAt = this.now();
		this.messages++;
		this.sendMessage(frames);
	}

	/**
	 * Send a ping when this socket has not written a message for `interval`
	 * (default 30 s). A flush already pending keeps the session alive, so
	 * no extra message is added.
	 */
	pingIfIdle(now = this.now(), interval = PING_INTERVAL_MS): boolean {
		if (this.queue.length > 0 || now - this.lastSentAt < interval) return false;
		this.enqueue([{ type: "ping", conn: SESSION_CONN, payload: new Uint8Array(0) }], true);
		return true;
	}

	/** Drop a pending flush (the WebSocket is gone). */
	cancel(): void {
		if (this.timer !== undefined) this.clearTimer(this.timer);
		this.timer = undefined;
		this.queue = [];
	}
}
