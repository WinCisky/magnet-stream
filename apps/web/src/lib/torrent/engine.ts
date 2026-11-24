// Recovers the pieces of one file through the tunnel session: the engine's
// deadline picker (in the session worker) chooses and verifies the blocks,
// this stores the verified pieces, steers the picker (seeks, playback
// position) and reports progress.

import { fileRange, type FileRange, type Metainfo } from "@magnet-stream/engine/metainfo";
import { libraryAvailable, rememberFile } from "./library";
import type { StoreErrorCode } from "./piece-file.ts";
import { PieceState } from "./scheduler";
import { MemoryPieceStore, SessionPieceStore, readableWhileStoring, type PieceStore } from "./store";
import { fileBytes } from "./download";
import { FileStream } from "./stream";
import type { PageSession, RecoveryProgress, TunnelRecoveryHandle } from "./page-session";
import { PieceTracker } from "./tracker";
import { MAX_BITRATE, MIN_BITRATE, type SwarmHealth } from "./session-protocol";

const SNAPSHOT_MS = 200;
/** The picker's window follows playback: a position goes to the worker at most this often. */
const POSITION_MS = 1_000;
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
	/** Why pieces aren't being saved (they're kept in memory meanwhile), or null. */
	storageError: string | null;
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
	storage: "opfs" | "memory" | null;
	swarmError: string | null;
	/**
	 * This recovery's share of the tunnel's billing counters, and the
	 * session's peers (known, delivered). Null without a session.
	 */
	tunnel: { upgrades: number; messages: number; known: number; delivered: number } | null;
	/** The video's average bitrate in bytes/s, once the player parsed the container (0: not yet). */
	bitrate: number;
	/** The picker's playback window in bytes (0: not reported yet); it follows the bitrate. */
	windowBytes: number;
	/** Under this download rate (bytes/s) for 30 s, another WebSocket opens (0: none). */
	rateThreshold: number;
	/**
	 * The swarm while recovering (M4-06): escalation to the VPS gateway, and
	 * whether the rate has stayed under the video's bitrate. Null otherwise.
	 */
	health: SwarmHealth | null;
}

export interface EngineOptions {
	infoHash: string;
	magnet: string;
	/** `index`: its place in the magnet's file listing, to list it among saved files. */
	file: { index?: number; path: string; offset: number; size: number };
	onSnapshot: (snapshot: EngineSnapshot) => void;
	/** Where verified pieces go (default: OPFS through the session worker, else memory). Memory: nothing is saved. */
	store?: PieceStore;
	/** Diagnostics: one line per notable event (seeks). */
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
	/** The torrent's layout is known and the pieces saved earlier are counted. */
	ready?(meta: Metainfo, range: FileRange, verifiedPieces: number): void;
}

export class RecoveryEngine {
	/** The file's bytes, for a player: reads wait for their pieces. */
	readonly stream = new FileStream();
	private phase: EnginePhase = "metadata";
	private message: string | null = null;
	private meta: Metainfo | null = null;
	private range: FileRange | null = null;
	private store: PieceStore | null = null;
	/** The store as the stream uses it (pieces readable while being stored). */
	private readable: PieceStore | null = null;
	private tracker: PieceTracker | null = null;
	private recovery: TunnelRecoveryHandle | null = null;
	private progress: RecoveryProgress | null = null;
	/** Session bytes when this recovery started. */
	private bytesBase: number | null = null;
	private storeErrors = 0;
	private storageError: string | null = null;
	private rate = 0;
	private lastBytes = 0;
	private lastSampleAt = 0;
	private lastPosition = -1;
	private lastPositionAt = 0;
	private timer: ReturnType<typeof setInterval> | undefined;
	private stopped = false;
	/** The session's counters when this recovery started (see PageSession.baseline). */
	private tunnelBase: { upgrades: number; messages: number } | null = null;
	/** Video bytes per second, as the player reported it (0: not yet). */
	private bitrate = 0;
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
		await this.startFromSession(meta);
	}

	stop(): void {
		this.stopped = true;
		clearInterval(this.timer);
		this.recovery?.stop();
		this.recovery = null;
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
		const { meta, range, readable, tracker } = this;
		if (!meta || !range || !readable || !tracker?.complete) return null;
		return fileBytes((piece) => readable.get(piece), range, meta.pieceLength);
	}

	/**
	 * The video's average bitrate (bytes/s), once the player parsed the
	 * container: the picker's window in bytes and the threshold for another
	 * socket (twice the bitrate) follow it.
	 */
	setBitrate(bytesPerSecond: number): void {
		if (this.stopped || !Number.isFinite(bytesPerSecond)) return;
		const rounded = Math.round(bytesPerSecond);
		if (rounded < MIN_BITRATE || rounded > MAX_BITRATE) return;
		if (rounded === this.bitrate) return;
		this.bitrate = rounded;
		this.opts.session?.setBitrate(rounded);
		this.opts.debug?.(`bitrate ${rounded} B/s`);
	}

	/** The viewer jumped to a byte offset in the file: the picker's window moves there. */
	seek(fileByte: number): void {
		const { meta, range, tracker } = this;
		if (!meta || !range || !tracker) return;
		const byte = Math.max(0, Math.min(range.size, Math.floor(fileByte)));
		tracker.setCursor(Math.floor((range.offset + byte) / meta.pieceLength));
		this.lastPosition = byte;
		this.lastPositionAt = Date.now();
		this.recovery?.seek(byte);
		this.opts.debug?.(`seek to piece ${tracker.cursor}`);
	}

	/**
	 * A read waits for `piece`: playback needs it next (a seek, a container
	 * index at the end of the file). Unless the picker's window already
	 * covers it, the window moves there.
	 */
	private demand(piece: number): void {
		const { meta, range, tracker } = this;
		if (this.stopped || !meta || !range || !tracker || tracker.isVerified(piece) || tracker.inWindow(piece)) return;
		this.seek(Math.max(0, piece * meta.pieceLength - range.offset));
	}

	/** A verified piece is gone from storage (refused, or evicted): fetch it again. */
	private lost(piece: number): void {
		const tracker = this.tracker;
		if (this.stopped || !tracker?.forget(piece)) return;
		if (this.recovery) {
			this.recovery.lost(piece);
			return;
		}
		// The recovery was complete: start another for what's missing.
		this.phase = "recovering";
		this.hold();
		this.startRecovery();
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
		if (!this.tracker?.complete || this.phase === "complete") return;
		this.phase = "complete";
		this.message = null;
		this.recovery?.stop();
		this.recovery = null;
		// The last recovery holding the session closes its WebSockets.
		this.unhold(true);
		this.emit();
	}

	/** Recovery through the tunnel session. */
	private async startFromSession(meta: Metainfo): Promise<void> {
		try {
			if (meta.pieceLength > MAX_PIECE_LENGTH) throw new Error("Pieces larger than 64 MiB aren't supported");
			this.meta = meta;
			this.range = fileRange(meta, this.opts.file);
			const session = this.opts.session!;
			this.store = this.opts.store ?? (libraryAvailable() ? new SessionPieceStore(session, meta.infoHash) : new MemoryPieceStore());
			if (this.store.kind === "memory" && !this.opts.store) this.storageError = describeStoreError("unavailable");
			const stored = await this.store.list().catch(() => [] as number[]);
			const { index, path } = this.opts.file;
			if (this.store.kind === "opfs" && index !== undefined) {
				// Every video opened stays saved, even partly, until the user deletes it (OQ-13).
				const { offset, size } = this.range;
				const torrent = { name: meta.name, magnet: this.opts.magnet, pieceLength: meta.pieceLength, totalLength: meta.totalLength };
				void rememberFile(meta.infoHash, torrent, { index, path, offset, size }).catch(() => {});
			}
			this.phase = "recovering";
			this.message = null;
			const store = readableWhileStoring(this.store);
			this.readable = store;
			const tracker = new PieceTracker(this.range, meta.pieceLength);
			this.tracker = tracker;
			for (const piece of stored) tracker.markVerified(piece);
			const range = this.range;
			this.stream.attach({
				pieceLength: meta.pieceLength,
				range,
				isVerified: (piece) => tracker.isVerified(piece),
				load: (piece) => store.get(piece),
				demand: (piece) => this.demand(piece),
				lost: (piece) => this.lost(piece),
			});
			this.observe((o) => o.ready?.(meta, range, tracker.verifiedCount));
			if (tracker.complete) {
				this.finishIfComplete();
				return;
			}
			if (this.stopped) return;
			this.hold();
			this.startRecovery();
		} catch (e) {
			if (this.stopped) return;
			this.phase = "error";
			this.message = e instanceof Error ? e.message : "Something went wrong";
			this.stream.close();
			this.emit();
		}
	}

	private startRecovery(): void {
		const { range, tracker, readable, store } = this;
		const session = this.opts.session;
		if (!range || !tracker || !readable || !store || !session || this.stopped) return;
		const position = Math.max(0, this.lastPosition);
		const recovery = session.recover(
			{ offset: range.offset, size: range.size },
			{ position, have: tracker.verifiedPieces(), save: store.kind === "opfs" },
			{
				onPiece: (piece, data, stored) => {
					if (this.recovery !== recovery || !tracker.markVerified(piece)) return;
					// The worker saved it already; otherwise it stays in memory.
					if (!stored) readable.put(piece, data).catch(() => this.storeErrors++);
					if (!stored && store.kind === "opfs") this.storeErrors++;
					this.stream.verified(piece, data);
				},
				onStoreError: (code) => {
					if (this.recovery === recovery) this.storageError = describeStoreError(code);
				},
				onProgress: (progress) => {
					if (this.recovery !== recovery) return;
					this.bytesBase ??= progress.bytes;
					this.progress = progress;
					tracker.setAssembling(progress.assembling);
				},
				onDone: () => {
					if (this.recovery === recovery) this.recovery = null;
					this.emit();
				},
			},
		);
		this.recovery = recovery;
	}

	/** The picker's window follows what the player reads (throttled). */
	private followPlayback(now: number): void {
		const read = this.stream.lastRead;
		if (!this.recovery || !this.meta || read < 0 || read === this.lastPosition || now - this.lastPositionAt < POSITION_MS) return;
		if (Math.abs(read - this.lastPosition) < this.meta.pieceLength) return;
		this.lastPosition = read;
		this.lastPositionAt = now;
		this.recovery.setPosition(read);
	}

	private emit(): void {
		const now = Date.now();
		const tracker = this.tracker;
		const meta = this.meta;
		const range = this.range;
		if (tracker && this.phase === "recovering") {
			this.finishIfComplete();
			this.followPlayback(now);
		}
		const bytesReceived = this.progress && this.bytesBase !== null ? Math.max(0, this.progress.bytes - this.bytesBase) : 0;

		if (tracker) {
			const elapsed = (now - this.lastSampleAt) / 1000;
			if (this.lastSampleAt && elapsed > 0) {
				const sample = (bytesReceived - this.lastBytes) / elapsed;
				this.rate = this.rate === 0 ? sample : this.rate * 0.8 + sample * 0.2;
			}
			this.lastBytes = bytesReceived;
			this.lastSampleAt = now;
		}

		const count = range ? range.lastPiece - range.firstPiece + 1 : 0;
		const states = new Uint8Array(count);
		const progress = new Uint8Array(count);
		tracker?.fillStates(states, progress, now);

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
		// While recovering, the worker's progress (every 500 ms); afterwards (the
		// sockets closed once complete) the session's own counters.
		const diag = this.opts.session?.diag;
		const live = this.recovery ? (this.progress?.live ?? 0) : (diag?.connected ?? 0);
		const sessionPeers = this.opts.session?.peerStats ?? { known: 0, delivered: 0 };

		this.opts.onSnapshot({
			phase: this.phase,
			message: this.message,
			firstPiece: range?.firstPiece ?? 0,
			pieceLength: meta?.pieceLength ?? 0,
			fileOffset: range?.offset ?? this.opts.file.offset,
			fileSize: range?.size ?? this.opts.file.size,
			states,
			progress,
			availability: new Uint16Array(count),
			cursor: tracker?.cursor ?? 0,
			front: tracker?.front ?? 0,
			seeks: tracker?.seeks ?? 0,
			wrapped: false,
			verifiedPieces: tracker?.verifiedCount ?? 0,
			verifiedBytes,
			contiguousBytes: tracker?.contiguousFromStart() ?? 0,
			rate: this.phase === "recovering" ? this.rate : 0,
			bytesReceived,
			etaSeconds: this.phase === "recovering" && this.rate > 1024 ? remaining / this.rate : null,
			activeRequests: tracker?.active ?? 0,
			requests: this.progress?.hedges ?? 0,
			hashFailures: this.progress?.hashFailures ?? 0,
			storeErrors: this.storeErrors,
			storageError: this.storageError,
			peers: {
				known: sessionPeers.known,
				reachable: live,
				seeds: Math.min(live, diag?.seeds ?? 0),
				active: Math.min(live, diag?.delivering ?? sessionPeers.delivered),
				connections: live,
				usable: Math.min(live, diag?.unchoked ?? live),
				backedOff: 0,
				banned: this.progress?.banned ?? 0,
			},
			storage: this.store?.kind ?? null,
			swarmError: null,
			tunnel: this.tunnelShare(),
			bitrate: this.bitrate,
			windowBytes: this.progress?.windowBytes ?? 0,
			rateThreshold: this.progress?.rateThreshold ?? 0,
			health: this.phase === "recovering" ? (this.opts.session?.health ?? null) : null,
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

function describeStoreError(code: StoreErrorCode): string {
	switch (code) {
		case "quota":
			return "The browser's storage is full: new pieces are kept in memory only and won't be saved. Delete saved videos to make room.";
		case "unavailable":
			return "This browser doesn't let the page save anything (a private window?): the video won't be saved.";
		case "busy":
			return "The saved copy of this torrent is in use elsewhere: new pieces won't be saved.";
		default:
			return "Pieces couldn't be saved: they're kept in memory only.";
	}
}
