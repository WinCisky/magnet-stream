// Upstream batching (docs/architecture.md §Tunnel protocol and request
// budget › Rules, docs/tunnel-protocol.md › Session lifetime): every frame
// for one WebSocket is held and sent as one message at most every
// 300–500 ms. Billing counts messages, not frames. An urgent frame (a
// cancel after a seek) skips the timer and flushes whatever is queued.
// A `ping` goes out when nothing else has been sent for the cadence in
// docs/spike-report.md (30 s), so a quiet session never hits the idle timeout.
//
// Lazy frames (upload data, `have`, choke/unchoke, rejects: OQ-04, M3-12)
// ride on the next message that is due anyway; alone, they are flushed at
// most once every 2 s per socket (`lazyMs`), so upload adds at most half a
// message per second per socket. Messages that carried only lazy frames
// are counted (`lazyMessages`): what the upload cost.

import { DEFAULT_MAX_FRAMES, PING_INTERVAL_MS, SESSION_CONN, type Frame } from "@magnet-stream/tunnel-protocol";

/** Midpoint of the 300–500 ms band. */
export const DEFAULT_BATCH_MS = 400;
/** Lazy frames alone (upload) are flushed at most this often per socket. */
export const DEFAULT_LAZY_MS = 2_000;
/** Data frames of one connection are merged up to this size (the gateway accepts 1 MiB). */
export const MAX_MERGED_DATA = 256 * 1024;
export { PING_INTERVAL_MS };

export interface UpstreamBatcherOptions {
	/** Flush interval (default 400). Any positive value: tests use a short one. */
	intervalMs?: number;
	/** Least time between two messages that carry only lazy frames (default 2000). */
	lazyMs?: number;
	/** Most frames in one message (default: the gateway decoder's 1024); more are split. */
	maxFrames?: number;
	now?: () => number;
	setTimer?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
	clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
	/** One WebSocket message. */
	send(frames: Frame[]): void;
}

/** How a frame waits: `urgent` goes at the end of the task, `normal` within the batch interval, `lazy` with the next message or after `lazyMs`. */
export type FlushMode = "urgent" | "normal" | "lazy";

export class UpstreamBatcher {
	private readonly intervalMs: number;
	private readonly lazyMs: number;
	private readonly maxFrames: number;
	private readonly now: () => number;
	private readonly setTimer: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
	private readonly clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
	private readonly sendMessage: (frames: Frame[]) => void;
	private queue: Frame[] = [];
	/** The queue holds a frame that is not lazy. */
	private eager = false;
	private timer: ReturnType<typeof setTimeout> | undefined;
	/** When the armed timer fires. */
	private dueAt = Infinity;
	private lastSentAt: number;
	private urgentPending = false;
	messages = 0;
	/** Messages that carried only lazy frames (upload flushes). */
	lazyMessages = 0;

	constructor(options: UpstreamBatcherOptions) {
		this.intervalMs = options.intervalMs ?? DEFAULT_BATCH_MS;
		this.lazyMs = Math.max(this.intervalMs, options.lazyMs ?? DEFAULT_LAZY_MS);
		this.maxFrames = Math.max(1, options.maxFrames ?? DEFAULT_MAX_FRAMES);
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
	 * still make one message. `lazy` frames wait for the next message, or
	 * at most until `lazyMs` after the last one.
	 */
	enqueue(frames: readonly Frame[], mode: FlushMode | boolean = "normal"): void {
		if (frames.length === 0) return;
		const how: FlushMode = mode === true ? "urgent" : mode === false ? "normal" : mode;
		this.queue.push(...frames);
		if (how === "urgent") {
			this.eager = true;
			if (!this.urgentPending) {
				this.urgentPending = true;
				queueMicrotask(() => {
					this.urgentPending = false;
					this.flush();
				});
			}
			return;
		}
		const now = this.now();
		if (how === "normal") {
			this.eager = true;
			this.arm(now + this.intervalMs);
		} else this.arm(Math.max(now + this.intervalMs, this.lastSentAt + this.lazyMs));
	}

	/** Arm the flush timer for `at`, unless one fires sooner already. */
	private arm(at: number): void {
		if (this.timer !== undefined && this.dueAt <= at) return;
		if (this.timer !== undefined) this.clearTimer(this.timer);
		this.dueAt = at;
		this.timer = this.setTimer(() => this.flush(), Math.max(0, at - this.now()));
	}

	flush(): void {
		if (this.timer !== undefined) {
			this.clearTimer(this.timer);
			this.timer = undefined;
		}
		this.dueAt = Infinity;
		if (this.queue.length === 0) return;
		const frames = mergeData(this.queue);
		const lazyOnly = !this.eager;
		this.queue = [];
		this.eager = false;
		this.lastSentAt = this.now();
		for (let at = 0; at < frames.length; at += this.maxFrames) {
			this.messages++;
			if (lazyOnly) this.lazyMessages++;
			this.sendMessage(frames.slice(at, at + this.maxFrames));
		}
	}

	/**
	 * Send a ping when this socket has not written a message for `interval`
	 * (default 30 s). A flush already pending keeps the session alive, so
	 * no extra message is added.
	 */
	pingIfIdle(now = this.now(), interval = PING_INTERVAL_MS): boolean {
		if (this.queue.length > 0 || now - this.lastSentAt < interval) return false;
		this.enqueue([{ type: "ping", conn: SESSION_CONN, payload: new Uint8Array(0) }], "urgent");
		return true;
	}

	/** Drop a pending flush (the WebSocket is gone). */
	cancel(): void {
		if (this.timer !== undefined) this.clearTimer(this.timer);
		this.timer = undefined;
		this.dueAt = Infinity;
		this.queue = [];
		this.eager = false;
	}
}

/**
 * Merge the `data` frames of each connection (up to MAX_MERGED_DATA each),
 * keeping every connection's bytes in order. A connection's data is not
 * merged across one of its other frames (a close, an open), and the order
 * of frames between connections does not matter (separate TCP streams).
 */
export function mergeData(frames: readonly Frame[]): Frame[] {
	const out: Frame[] = [];
	/** conn → its data frame in `out` still open for merging, and the parts merged into it. */
	const open = new Map<number, { index: number; parts: Uint8Array[]; size: number }>();
	const seal = (conn: number) => {
		const cur = open.get(conn);
		if (!cur) return;
		open.delete(conn);
		if (cur.parts.length < 2) return;
		const payload = new Uint8Array(cur.size);
		let at = 0;
		for (const p of cur.parts) {
			payload.set(p, at);
			at += p.length;
		}
		out[cur.index] = { type: "data", conn, payload };
	};
	for (const f of frames) {
		if (f.type !== "data") {
			if (f.conn !== SESSION_CONN) seal(f.conn);
			out.push(f);
			continue;
		}
		const cur = open.get(f.conn);
		if (cur && cur.size + f.payload.length <= MAX_MERGED_DATA) {
			cur.parts.push(f.payload);
			cur.size += f.payload.length;
			continue;
		}
		seal(f.conn);
		open.set(f.conn, { index: out.length, parts: [f.payload], size: f.payload.length });
		out.push(f);
	}
	for (const conn of [...open.keys()]) seal(conn);
	return out;
}
