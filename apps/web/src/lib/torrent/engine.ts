// Recovers the pieces of one file through the tunnel session: the scheduler
// asks for blocks, the session fetches them, and this reports progress.

import type { SwarmSnapshot } from "$lib/magnet/api";
import { fileRange, type FileRange, type Metainfo } from "@magnet-stream/engine/metainfo";
import { rememberFile } from "./library";
import { Scheduler, PieceState, type BatchReport } from "./scheduler";
import { openPieceStore, readableWhileStoring, type PieceStore } from "./store";
import { fileBytes } from "./download";
import { FileStream } from "./stream";
import type { PageSession } from "./page-session";
import { Swarm } from "./swarm";

const SNAPSHOT_MS = 200;
const AVAILABILITY_MS = 5_000;
/** Block keys pack the block index in 12 bits (see scheduler). */
const MAX_PIECE_LENGTH = 64 * 1024 * 1024;

export type EnginePhase = "metadata" | "peers" | "recovering" | "complete" | "error";

export interface EngineSnapshot {
	phase: EnginePhase;
	message: string | null;
	firstPiece: number;
	pieceLength: number;
	fileOffset: number;
	fileSize: number;
	/** Per piece of the file: a `PieceState`. */
	states: Uint8Array;
	/** Per piece: 0–255 share of blocks received (downloading pieces). */
	progress: Uint8Array;
	/** Per piece: how many known peers hold it. */
	availability: Uint16Array;
	cursor: number;
	/** First unverified piece at or after the cursor. */
	front: number;
	/** Times the cursor was moved (seeks, reads playback waited on). */
	seeks: number;
	/** The cursor went back to the start by itself, all past it being in. */
	wrapped: boolean;
	verifiedPieces: number;
	verifiedBytes: number;
	contiguousBytes: number;
	/** Bytes/second received from peers. */
	rate: number;
	/** Bytes received from peers so far (this session). */
	bytesReceived: number;
	etaSeconds: number | null;
	activeRequests: number;
	requests: number;
	hashFailures: number;
	/** Verified pieces the browser refused to store. */
	storeErrors: number;
	peers: {
		known: number;
		reachable: number;
		seeds: number;
		/** Peers with at least one open request. */
		active: number;
		/** Open peer connections (fast peers can have several). */
		connections: number;
		usable: number;
		backedOff: number;
		banned: number;
	};
	storage: "cache" | "memory" | null;
	swarmError: string | null;
	/**
	 * This recovery's share of the tunnel's billing counters, and the
	 * session's peers (known, delivered). Null without a session.
	 */
	tunnel: { upgrades: number; messages: number; known: number; delivered: number } | null;
}

export interface EngineOptions {
	infoHash: string;
	magnet: string;
	/** `index`: its place in the magnet's file listing, to list it among saved files. */
	file: { index?: number; path: string; offset: number; size: number };
	onSnapshot: (snapshot: EngineSnapshot) => void;
	/** Where verified pieces go (default: Cache Storage, else memory). */
	store?: PieceStore;
	/** Parallel scheduler batches (default 3 on the tunnel path). */
	maxActive?: number;
	/** Candidates named per scheduler batch (the tunnel peer is the only one). */
	maxCandidates?: number;
	/** Diagnostics: one line per finished worker request. */
	debug?: (line: string) => void;
	/** Diagnostics: what the engine's requests took and how they ended. */
	observer?: EngineObserver;
	/** Tunnel session that already looked the torrent up. */
	session?: PageSession;
	knownMeta?: Metainfo;
}

/**
 * Diagnostics hooks. They run after a request is over, never per block;
 * the engine ignores anything they throw.
 */
export interface EngineObserver {
	/** One attempt at the info dict (error null: it arrived and checked out). */
	infoDict?(ms: number, error: string | null): void;
	/** One /swarm request (snapshot null: it failed). */
	swarm?(ms: number, snapshot: SwarmSnapshot | null, error: string | null): void;
	/** How many candidates the worker takes per request, and how long asking took. */
	workerInfo?(maxCandidates: number, ms: number): void;
	/** The torrent's layout is known and the pieces saved earlier are counted. */
	ready?(meta: Metainfo, range: FileRange, verifiedPieces: number): void;
	/** One finished worker request. */
	batch?(report: BatchReport): void;
}

export class RecoveryEngine {
	/** The file's bytes, for a player: reads wait for their pieces. */
	readonly stream = new FileStream();
	private phase: EnginePhase = "metadata";
	private message: string | null = null;
	private meta: Metainfo | null = null;
	private range: FileRange | null = null;
	private store: PieceStore | null = null;
	/** The store as the scheduler and the stream use it (pieces readable while being stored). */
	private readable: PieceStore | null = null;
	private swarm: Swarm | null = null;
	private scheduler: Scheduler | null = null;
	private availability: Uint16Array = new Uint16Array(0);
	private availabilityAt = 0;
	private rate = 0;
	private lastBytes = 0;
	private lastSampleAt = 0;
	private timer: ReturnType<typeof setInterval> | undefined;
	private stopped = false;
	/** The session's counters when this recovery started (see PageSession.baseline). */
	private tunnelBase: { upgrades: number; messages: number } | null = null;
	/** Our hold on the shared tunnel session, while recovering. */
	private release: ((completed: boolean) => void) | null = null;

	constructor(private readonly opts: EngineOptions) {}

	async start(): Promise<void> {
		this.tunnelBase = this.opts.session?.baseline() ?? null;
		this.timer = setInterval(() => this.emit(), SNAPSHOT_MS);
		this.emit();
		const session = this.opts.session;
		const meta = this.opts.knownMeta;
		if (!session || !meta) {
			this.phase = "error";
			this.message = "No torrent session";
			this.stream.close();
			this.emit();
			return;
		}
		await this.startFromSession(session, meta);
	}

	stop(): void {
		this.stopped = true;
		clearInterval(this.timer);
		this.scheduler?.stop();
		this.swarm?.stop();
		this.stream.close();
		this.unhold(false);
	}

	private hold(): void {
		if (!this.release && !this.stopped) this.release = this.opts.session?.acquire() ?? null;
	}

	private unhold(completed: boolean): void {
		this.release?.(completed);
		this.release = null;
	}

	/** The whole file's bytes, for a download; null until every piece is verified. */
	fileBytes(): ReadableStream<Uint8Array> | null {
		const { meta, range, readable, scheduler } = this;
		if (!meta || !range || !readable || !scheduler?.complete) return null;
		return fileBytes((piece) => readable.get(piece), range, meta.pieceLength);
	}

	/** Move the sequential recovery front to a byte offset in the file. */
	seek(fileByte: number): void {
		if (!this.meta || !this.range || !this.scheduler) return;
		this.scheduler.setCursor(Math.floor((this.range.offset + fileByte) / this.meta.pieceLength));
	}

	/**
	 * A read waits for `piece`: playback needs it next (a seek, a container
	 * index at the end of the file). The scheduler moves the front there.
	 */
	private demand(piece: number): void {
		if (!this.stopped) this.scheduler?.demand(piece);
	}

	/** A verified piece is gone from storage (refused, or evicted): fetch it again. */
	private lost(piece: number): void {
		const scheduler = this.scheduler;
		if (this.stopped || !scheduler?.isVerified(piece)) return;
		scheduler.forget(piece);
		if (this.phase !== "complete") return;
		this.phase = "recovering";
		this.hold();
		void this.swarm?.start();
		scheduler.start();
		this.emit();
	}

	private observe(report: (observer: EngineObserver) => void): void {
		if (!this.opts.observer) return;
		try {
			report(this.opts.observer);
		} catch {
			// Diagnostics must never get in the way of recovery.
		}
	}

	private finishIfComplete(): void {
		if (!this.scheduler?.complete || this.phase === "complete") return;
		this.phase = "complete";
		this.message = null;
		this.scheduler.stop();
		this.swarm?.stop();
		// The last recovery holding the session closes its WebSockets.
		this.unhold(true);
		this.emit();
	}

	/** Recovery through the tunnel session. */
	private async startFromSession(session: PageSession, meta: Metainfo): Promise<void> {
		try {
			if (meta.pieceLength > MAX_PIECE_LENGTH) throw new Error("Pieces larger than 64 MiB aren't supported");
			this.meta = meta;
			this.range = fileRange(meta, this.opts.file);
			this.store = this.opts.store ?? (await openPieceStore(meta.infoHash));
			const stored = await this.store.list().catch(() => [] as number[]);
			const { index, path } = this.opts.file;
			if (!this.opts.store && this.store.kind === "cache" && index !== undefined) {
				const { offset, size } = this.range;
				void rememberFile(meta.infoHash, { name: meta.name, magnet: this.opts.magnet, pieceLength: meta.pieceLength }, { index, path, offset, size }).catch(() => {});
			}
			this.swarm = session.swarm();
			await this.swarm.start();
			const tunnel = this.swarm.peers.get("tunnel");
			if (tunnel) tunnel.maxConns = 3;
			this.phase = "recovering";
			this.message = null;
			const store = readableWhileStoring(this.store);
			this.readable = store;
			this.scheduler = new Scheduler({
				meta,
				range: this.range,
				swarm: this.swarm,
				transport: session.transport(meta),
				store,
				maxActive: this.opts.maxActive ?? 3,
				maxCandidates: 1,
				debug: this.opts.debug,
				onBatchEnd: this.opts.observer ? (r) => this.observe((o) => o.batch?.(r)) : undefined,
				onVerified: (piece, data) => this.stream.verified(piece, data),
			});
			this.scheduler.markVerified(stored);
			const range = this.range;
			const scheduler = this.scheduler;
			this.stream.attach({
				pieceLength: meta.pieceLength,
				range,
				isVerified: (piece) => scheduler.isVerified(piece),
				load: (piece) => store.get(piece),
				demand: (piece) => this.demand(piece),
				lost: (piece) => this.lost(piece),
			});
			this.observe((o) => o.ready?.(meta, range, scheduler.verifiedCount));
			if (scheduler.complete) {
				this.finishIfComplete();
				return;
			}
			if (this.stopped) return;
			this.hold();
			scheduler.start();
		} catch (e) {
			if (this.stopped) return;
			this.phase = "error";
			this.message = e instanceof Error ? e.message : "Something went wrong";
			this.stream.close();
			this.emit();
		}
	}

	private emit(): void {
		const now = Date.now();
		const scheduler = this.scheduler;
		const meta = this.meta;
		const range = this.range;
		if (scheduler && this.phase === "recovering") this.finishIfComplete();

		if (scheduler) {
			const elapsed = (now - this.lastSampleAt) / 1000;
			if (this.lastSampleAt && elapsed > 0) {
				const sample = (scheduler.bytesReceived - this.lastBytes) / elapsed;
				this.rate = this.rate === 0 ? sample : this.rate * 0.8 + sample * 0.2;
			}
			this.lastBytes = scheduler.bytesReceived;
			this.lastSampleAt = now;
		}

		const count = range ? range.lastPiece - range.firstPiece + 1 : 0;
		const states = new Uint8Array(count);
		const progress = new Uint8Array(count);
		scheduler?.fillStates(states, progress, now);

		if (this.swarm && range && (now - this.availabilityAt > AVAILABILITY_MS || this.availability.length !== count)) {
			this.availability = this.swarm.availability(range.firstPiece, range.lastPiece);
			this.availabilityAt = now;
		}

		let verifiedBytes = 0;
		if (meta && range) {
			const fileEnd = range.offset + range.size;
			for (let i = 0; i < count; i++) {
				if (states[i] !== PieceState.Verified) continue;
				const start = (range.firstPiece + i) * meta.pieceLength;
				verifiedBytes += Math.max(0, Math.min(start + meta.pieceLength, fileEnd) - Math.max(start, range.offset));
			}
		}
		const remaining = range ? range.size - verifiedBytes : 0;
		const peerStats = this.swarm?.stats() ?? { active: 0, connections: 0, usable: 0, backedOff: 0, banned: 0 };

		this.opts.onSnapshot({
			phase: this.phase,
			message: this.message,
			firstPiece: range?.firstPiece ?? 0,
			pieceLength: meta?.pieceLength ?? 0,
			fileOffset: range?.offset ?? this.opts.file.offset,
			fileSize: range?.size ?? this.opts.file.size,
			states,
			progress,
			availability: this.availability.length === count ? this.availability : new Uint16Array(count),
			cursor: scheduler?.cursor ?? 0,
			front: scheduler?.front ?? 0,
			seeks: scheduler?.seeks ?? 0,
			wrapped: scheduler?.wrapped ?? false,
			verifiedPieces: scheduler?.verifiedCount ?? 0,
			verifiedBytes,
			contiguousBytes: scheduler?.contiguousFromStart() ?? 0,
			rate: this.phase === "recovering" ? this.rate : 0,
			bytesReceived: scheduler?.bytesReceived ?? 0,
			etaSeconds: this.phase === "recovering" && this.rate > 1024 ? remaining / this.rate : null,
			activeRequests: scheduler?.activeBatches ?? 0,
			requests: scheduler?.requests ?? 0,
			hashFailures: scheduler?.hashFailures ?? 0,
			storeErrors: scheduler?.storeErrors ?? 0,
			peers: { ...(this.swarm?.counts ?? { known: 0, reachable: 0, seeds: 0 }), ...peerStats },
			storage: this.store?.kind ?? null,
			swarmError: this.swarm?.error ?? null,
			tunnel: this.tunnelShare(),
		});
	}

	private tunnelShare(): EngineSnapshot["tunnel"] {
		const session = this.opts.session;
		const base = this.tunnelBase;
		if (!session || !base) return null;
		const now = session.tunnelStats;
		return {
			upgrades: Math.max(0, now.upgrades - base.upgrades),
			messages: Math.max(0, now.messages - base.messages),
			...session.peerStats,
		};
	}
}
