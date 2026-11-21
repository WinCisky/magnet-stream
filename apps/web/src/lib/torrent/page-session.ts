// The page's handle on the session worker: one lookup, then recoveries
// that the engine's deadline picker runs in the worker (verified pieces come
// back, seeks and playback positions go there). Several recoveries can
// share the session (the video, a subtitle file): each one holds it with
// `acquire()`. When the last one completes, the WebSockets close at once;
// when the last one is stopped (the user switched file), they close after a
// short grace so a quick switch keeps its peers. A later want reopens them.

import type { Metainfo } from "@magnet-stream/engine/metainfo";
import { SEEDERS_API_URL, WORKER_API_URL, type TorrentFile, type TorrentMetadata } from "$lib/magnet/api";
import SessionWorker from "./session.worker.ts?worker&inline";
import type { StoreErrorCode } from "./piece-file.ts";
import type { LookupFailure, LookupProgress } from "./lookup-wait.ts";
import { onShareChange, readShare } from "$lib/settings/share";
import { parseWorkerMessage, type MetadataOrigin, type PageMessage, type SessionDiag, type SessionListing, type WorkerMessage } from "./session-protocol.ts";

export interface LookupResult {
	metadata: TorrentMetadata;
	meta: Metainfo;
	seeders: number;
	/** Where the verified info dict came from. */
	source: MetadataOrigin;
}

/** The lookup gave up; the session stays open, and `retry()` carries on where it stopped. */
export class LookupFailedError extends Error {
	readonly reason: LookupFailure;
	readonly progress: LookupProgress;
	constructor(message: string, reason: LookupFailure, progress: LookupProgress) {
		super(message);
		this.name = "LookupFailedError";
		this.reason = reason;
		this.progress = progress;
	}
}

type Files = { listing: SessionListing; info: Uint8Array; source: MetadataOrigin };

/** Sockets stay open this long after the last recovery stopped without completing. */
export const IDLE_GRACE_MS = 15_000;

export type RecoveryProgress = Omit<Extract<WorkerMessage, { type: "progress" }>, "type" | "id">;

export interface RecoveryCallbacks {
	/** A verified piece (its bytes are the page's now). `stored`: saved in OPFS by the worker. */
	onPiece(piece: number, data: Uint8Array, stored: boolean): void;
	/** Pieces couldn't be saved (the quota, say): they are kept in memory only. */
	onStoreError?(code: StoreErrorCode): void;
	onProgress?(progress: RecoveryProgress): void;
	/** Every piece is verified. */
	onDone?(): void;
}

/** A recovery running in the session worker. */
export interface TunnelRecoveryHandle {
	/** The viewer jumped to `byte` (into the range): requests outside the new window are cancelled. */
	seek(byte: number): void;
	/** Playback reached `byte`: the window follows. */
	setPosition(byte: number): void;
	/** A verified piece was lost from storage: fetch it again. */
	lost(piece: number): void;
	stop(): void;
}

/** A read of a stored piece waits this long for the worker. */
const READ_TIMEOUT_MS = 15_000;
const RELEASE_TIMEOUT_MS = 2_000;

export class PageSession {
	/** Sessions open in this page (the library asks them to let go of files before a delete). */
	private static readonly open = new Set<PageSession>();

	/** Every session closes the store files it isn't writing to. */
	static async releaseStores(): Promise<void> {
		await Promise.all([...PageSession.open].map((s) => s.release()));
	}

	private readonly requests = new Map<number, (value: ArrayBuffer | null) => void>();
	private readonly worker: Worker;
	private readonly recoveries = new Map<number, RecoveryCallbacks>();
	private nextId = 1;
	private peerCount = 0;
	/** The lookup the page waits on (lookup() or retry()). */
	private files: { resolve(result: Files): void; reject(error: Error): void } | undefined;
	/** The verified listing, once the worker sent it (a later retry resolves at once). */
	private known: Files | undefined;
	/** The session failed for good (worker error, closed): no retry. */
	private broken: Error | undefined;
	private lastDiag: SessionDiag | undefined;
	private lookupProgress: LookupProgress | undefined;
	private closed = false;
	/** While the lookup waits: peers found, tried, connected (no addresses). */
	onLookupProgress: ((progress: LookupProgress) => void) | undefined;
	/** The session's counters for the diagnostics, every few seconds. */
	onDiag: ((diag: SessionDiag) => void) | undefined;
	private holders = 0;
	private stats = { upgrades: 0, messages: 0 };
	private peers = { known: 0, delivered: 0 };
	private baselineTaken = false;
	private idleTimer: ReturnType<typeof setTimeout> | undefined;
	private readonly unshare: () => void;

	constructor() {
		this.worker = new SessionWorker();
		PageSession.open.add(this);
		this.worker.onmessage = (event: MessageEvent) => this.onMessage(event.data);
		// "Share with peers" changed: the running session follows at once.
		this.unshare = onShareChange((enabled) => this.post({ type: "upload", enabled }));
		this.worker.onerror = (event) => {
			const err = event as ErrorEvent;
			this.broken = new Error(err.message || "session worker failed");
			this.settle(this.broken);
		};
	}

	/**
	 * Parse the magnet in the worker, open the tunnel, and return the file
	 * list. One /peers request. `capMs`: the lookup's hard cap (default 120 s).
	 * A LookupFailedError leaves the session open for `retry()`.
	 */
	lookup(magnet: string, opts: { gateway?: string; seeders?: string; capMs?: number } = {}): Promise<LookupResult> {
		const done = this.waitFiles();
		this.post({
			type: "start",
			magnet,
			gatewayUrl: opts.gateway ?? WORKER_API_URL,
			seedersUrl: opts.seeders ?? SEEDERS_API_URL,
			...(opts.capMs !== undefined ? { capMs: opts.capMs } : {}),
			upload: readShare(),
		});
		return done;
	}

	/**
	 * Look again after a failed lookup, in the same session: the WebSockets
	 * already open and the peer book are reused (failed addresses keep their
	 * back-off), and the cap counts from now.
	 */
	retry(): Promise<LookupResult> {
		const done = this.waitFiles();
		if (!this.known) this.post({ type: "retry" });
		return done;
	}

	/** Whether `retry()` can still work (the worker is alive). */
	get retryable(): boolean {
		return !this.closed && !this.broken;
	}

	/** The session's counters for the diagnostics, as last reported. */
	get diag(): SessionDiag | undefined {
		return this.lastDiag;
	}

	/** The lookup's progress, as last reported. */
	get progress(): LookupProgress | undefined {
		return this.lookupProgress;
	}

	private waitFiles(): Promise<LookupResult> {
		// A lookup still waiting is replaced (its caller gave up on it).
		this.files?.reject(new Error("lookup replaced"));
		this.files = undefined;
		const files = new Promise<Files>((resolve, reject) => {
			if (this.known) return resolve(this.known);
			if (this.broken) return reject(this.broken);
			this.files = { resolve, reject };
		});
		return files.then(async ({ listing, info, source }) => {
			const { parseInfoDict } = await import("@magnet-stream/engine/metainfo");
			const meta = await parseInfoDict(info, listing.infoHash);
			return {
				meta,
				seeders: this.peerCount,
				source,
				metadata: { info_hash: listing.infoHash, name: listing.name, size: listing.size, files: listing.files },
			};
		});
	}

	private settle(result: Files | Error): void {
		const files = this.files;
		this.files = undefined;
		if (!files) return;
		if (result instanceof Error) files.reject(result);
		else files.resolve(result);
	}

	/**
	 * Recover torrent bytes [range.offset, range.offset + range.size) in the
	 * worker, from `position` (bytes into the range). `have`: pieces already
	 * stored, not fetched again.
	 */
	recover(
		range: { offset: number; size: number },
		opts: { position?: number; have?: readonly number[]; save?: boolean },
		callbacks: RecoveryCallbacks,
	): TunnelRecoveryHandle {
		const id = this.nextId++;
		this.recoveries.set(id, callbacks);
		const whole = (n: number) => Math.max(0, Math.floor(n));
		this.post({
			type: "recover",
			id,
			offset: whole(range.offset),
			size: whole(range.size),
			position: whole(opts.position ?? 0),
			have: [...(opts.have ?? [])],
			save: opts.save === true,
		});
		return {
			seek: (byte) => {
				if (this.recoveries.has(id)) this.post({ type: "seek", id, byte: whole(byte) });
			},
			setPosition: (byte) => {
				if (this.recoveries.has(id)) this.post({ type: "position", id, byte: whole(byte) });
			},
			lost: (piece) => {
				if (this.recoveries.has(id)) this.post({ type: "lost", id, piece: whole(piece) });
			},
			stop: () => {
				if (!this.recoveries.delete(id)) return;
				this.post({ type: "end", id });
			},
		};
	}

	/**
	 * The video's average bitrate (bytes per second), once the player knows
	 * it: the picker's window and the socket threshold follow it. Values
	 * out of range are dropped (see session-protocol.ts).
	 */
	setBitrate(bytesPerSecond: number): void {
		this.post({ type: "bitrate", bytesPerSecond });
	}

	/** A piece stored in OPFS, read by the worker (undefined: not stored, or the session is gone). */
	readPiece(piece: number): Promise<Uint8Array | undefined> {
		if (this.closed) return Promise.resolve(undefined);
		const req = this.nextId++;
		return new Promise((resolve) => {
			const timer = setTimeout(() => done(null), READ_TIMEOUT_MS);
			const done = (data: ArrayBuffer | null) => {
				clearTimeout(timer);
				this.requests.delete(req);
				resolve(data ? new Uint8Array(data) : undefined);
			};
			this.requests.set(req, done);
			this.post({ type: "read", req, piece: Math.max(0, Math.floor(piece)) });
		});
	}

	/** Ask the worker to close the store files if it isn't saving (a delete needs them). */
	release(): Promise<void> {
		if (this.closed) return Promise.resolve();
		const req = this.nextId++;
		return new Promise((resolve) => {
			const timer = setTimeout(() => done(), RELEASE_TIMEOUT_MS);
			const done = () => {
				clearTimeout(timer);
				this.requests.delete(req);
				resolve();
			};
			this.requests.set(req, done);
			this.post({ type: "release", req });
		});
	}

	/**
	 * Hold the session for one recovery. The returned release says whether
	 * that recovery completed (sockets close now) or stopped (after a grace).
	 */
	acquire(): (completed: boolean) => void {
		this.holders++;
		this.clearIdle();
		let released = false;
		return (completed) => {
			if (released) return;
			released = true;
			this.holders--;
			if (this.holders > 0 || this.closed) return;
			this.clearIdle();
			if (completed) this.post({ type: "idle" });
			else this.idleTimer = setTimeout(() => this.post({ type: "idle" }), IDLE_GRACE_MS);
		};
	}

	/** Upgrades attempted and messages sent by this session's tunnel so far. */
	get tunnelStats(): { upgrades: number; messages: number } {
		return { ...this.stats };
	}

	/** Peers the session knows, and how many delivered data. */
	get peerStats(): { known: number; delivered: number } {
		return { ...this.peers };
	}

	/**
	 * Where a recovery starts counting the tunnel's cost: the first one is
	 * charged from zero (the lookup's upgrade and messages are its own),
	 * later ones (a subtitle file, another file) from the current totals.
	 */
	baseline(): { upgrades: number; messages: number } {
		if (this.baselineTaken) return this.tunnelStats;
		this.baselineTaken = true;
		return { upgrades: 0, messages: 0 };
	}

	private clearIdle(): void {
		if (this.idleTimer !== undefined) clearTimeout(this.idleTimer);
		this.idleTimer = undefined;
	}

	/** Drop the session (leaving the page). */
	close(): void {
		if (this.closed) return;
		this.clearIdle();
		this.post({ type: "stop" });
		this.closed = true;
		this.worker.terminate();
		this.broken ??= new Error("session closed");
		this.settle(this.broken);
		this.recoveries.clear();
		PageSession.open.delete(this);
		this.unshare();
		for (const done of [...this.requests.values()]) done(null);
	}

	private post(message: PageMessage): void {
		if (!this.closed) this.worker.postMessage(message);
	}

	private onMessage(data: unknown): void {
		const msg = parseWorkerMessage(data);
		if (!msg) return;
		if (msg.type === "peers") this.peerCount = msg.count;
		else if (msg.type === "stats") {
			this.stats = { upgrades: msg.upgrades, messages: msg.messages };
			this.peers = { known: msg.known, delivered: msg.delivered };
		}
		else if (msg.type === "diag") {
			this.lastDiag = msg.diag;
			this.onDiag?.(msg.diag);
		}
		else if (msg.type === "lookup") {
			this.lookupProgress = msg.progress;
			this.onLookupProgress?.(msg.progress);
		} else if (msg.type === "lookup-failed") {
			this.lookupProgress = msg.progress;
			this.settle(new LookupFailedError(msg.message, msg.reason, msg.progress));
		} else if (msg.type === "error") {
			// Before the file list: the magnet couldn't be read, nothing to retry.
			if (!this.known) {
				this.broken = new Error(msg.message);
				this.settle(this.broken);
			}
		} else if (msg.type === "files") {
			this.known = { listing: msg.listing, info: new Uint8Array(msg.info), source: msg.source };
			this.settle(this.known);
		}
		else if (msg.type === "piece") this.recoveries.get(msg.id)?.onPiece(msg.piece, new Uint8Array(msg.data), msg.stored);
		else if (msg.type === "store-error") this.recoveries.get(msg.id)?.onStoreError?.(msg.code);
		else if (msg.type === "read-result") this.requests.get(msg.req)?.(msg.data);
		else if (msg.type === "released") this.requests.get(msg.req)?.(null);
		else if (msg.type === "progress") {
			const { assembling, bytes, live, hashFailures, banned, hedges, cancels, windowBytes, rateThreshold } = msg;
			this.recoveries.get(msg.id)?.onProgress?.({ assembling, bytes, live, hashFailures, banned, hedges, cancels, windowBytes, rateThreshold });
		} else if (msg.type === "recovered") {
			const recovery = this.recoveries.get(msg.id);
			this.recoveries.delete(msg.id);
			recovery?.onDone?.();
		}
	}
}

export type { TorrentFile };
