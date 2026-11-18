// Connection manager (docs/architecture.md §Chunk recovery › Connection
// management, §Tunnel protocol and request budget › Rules): connections
// stay up for the session, dials race only at connect time, and a peer
// that is delivering is never dropped to make room. Upstream frames of a
// socket are one message every 300–500 ms; in-flight data stays within
// 16 MiB per socket, whatever a peer advertises. A dead gateway is
// redialled and unfinished blocks are asked for again.
//
// Piece choice here is sequential. The deadline picker is M3-01; the VPS
// gateway is M4-06. Nothing is logged: a peer address or an info-hash must
// not reach the console. Detailed debug stays opt-in in the page (`mw-debug`).

import { DEFAULT_SOCKETS_PER_SESSION, FailReason, MAX_CONNECTIONS_PER_SOCKET, PING_INTERVAL_MS } from "@magnet-stream/tunnel-protocol";
import type { MetadataMessage } from "./extension.ts";
import { HashFetcher } from "./hashes.ts";
import { bytesToHex } from "./magnet.ts";
import { BLOCK_SIZE, geometryOf, type Metainfo } from "./metainfo.ts";
import { PeerBook, PexSource, type PeerEntry, type PeerOrigin } from "./peer-book.ts";
import { PeerConnection } from "./peer-connection.ts";
import { pieceSizeOf, type Geometry } from "./peer-wire.ts";
import { connectionScore, type ScoreFacts } from "./score.ts";
import { replacementVictim, requestAllowance, shouldAddSocket, type RateSample, type SlotView } from "./slots.ts";
import type { Transport } from "./transport.ts";
import { TunnelClient, TunnelOpenError, failReasonName, type WebSocketFactory } from "./tunnel-client.ts";
import { DEFAULT_BATCH_MS } from "./upstream.ts";
import { V2Verifier, verifierFor, type PieceVerifier } from "./verify.ts";

/** Hard cap on data asked for and not yet received, per WebSocket. */
export const MAX_INFLIGHT_PER_SOCKET = 16 * 1024 * 1024;
/** Untested dials in flight per socket (OQ-03: never fill a socket with unknown connects). */
export const MAX_UNTESTED_DIALS = 3;

const DEFAULT_WINDOW_MS = 30_000;
const DEFAULT_CHOKE_REPLACE_MS = 30_000;
const DEFAULT_DELIVERING_MS = 5_000;
const DEFAULT_RECONNECT_MS = 1_000;
const DEFAULT_STALL_MS = 60_000;
const DEFAULT_SWEEP_MS = 250;
const RATE_SAMPLE_MS = 1_000;
/**
 * While some peer is delivering, new dials go out at most this often: each
 * dial burst is an upstream message (billed), and exploring matters less
 * once data flows. With nothing delivering, dials are not paced.
 */
const DEFAULT_DIAL_INTERVAL_MS = 5_000;
/**
 * Keep-alives: a peer we have not written to for this long gets one, and
 * every other quiet peer gets one in the same message. Peers drop a
 * connection after 2 min of silence.
 */
const KEEPALIVE_AFTER_MS = 90_000;
const KEEPALIVE_ALONG_MS = 45_000;
/**
 * A peer's request queue holds about this many seconds of its own rate, and
 * is refilled once half has drained: a refill every few seconds per peer,
 * whatever its speed, and a slow peer never holds much of a batch.
 */
const QUEUE_SECONDS = 8;
/** Queue depth (blocks) before a peer's rate is known, and the least it gets. */
const INITIAL_DEPTH = 16;
const MIN_DEPTH = 4;
/**
 * Blocks that arrive after the page gave up on them are kept (up to this
 * many bytes) for the page's next request: re-asking costs messages and the
 * place in the peer's queue, keeping them costs nothing.
 */
const ORPHAN_CACHE_BYTES = 32 * 1024 * 1024;
/**
 * With no peer connected at all, every slot of the open sockets busy and
 * untried addresses waiting, another WebSocket (6 more slots) opens after
 * this long, up to maxSockets. Most addresses are unreachable and a connect
 * that times out keeps its slot for 20 s (OQ-03): one socket can try only
 * ~9 addresses in the first 30 s, too few to find the peer that serves the
 * metadata. Extra sockets are the "dedicated exploration" of OQ-03.
 */
const DEFAULT_EXPLORE_AFTER_MS = 5_000;

export interface ConnectionManagerOptions {
	/** `http://host:port` or `ws://host:port/v1/tunnel`. */
	gatewayUrl: string;
	/** 20-byte wire info-hash. */
	infoHash: Uint8Array;
	peerId?: Uint8Array;
	book?: PeerBook;
	/** Dial loopback peers (local tests only; the port rule stays). */
	allowPrivate?: boolean;
	/** Upstream coalesce interval (default 400). */
	batchMs?: number;
	pingIntervalMs?: number;
	/** Open another socket only after the rate stays under this for the whole window. */
	rateThreshold?: number;
	rateWindowMs?: number;
	maxSockets?: number;
	maxUntestedDials?: number;
	/** At most 16 MiB; a lower value is for tests. */
	inflightLimit?: number;
	chokeReplaceAfterMs?: number;
	deliveringWindowMs?: number;
	reconnectDelayMs?: number;
	stallTimeoutMs?: number;
	sweepMs?: number;
	/** Pacing of dials while a peer delivers (default 5000). */
	dialIntervalMs?: number;
	/** See DEFAULT_EXPLORE_AFTER_MS (0 disables). */
	exploreAfterMs?: number;
	now?: () => number;
	factory?: WebSocketFactory;
	sessionId?: string;
	/** A peer's extended handshake arrived (metadata fetch). */
	onExtensions?(conn: PeerConnection): void;
	/** A ut_metadata message from a peer. */
	onMetadata?(conn: PeerConnection, message: MetadataMessage): void;
	/** A peer connection closed. */
	onPeerClose?(conn: PeerConnection): void;
}

export interface SessionDownloadOptions {
	pieces?: readonly number[];
	signal?: AbortSignal;
	onPiece?(piece: number, data: Uint8Array): void;
}

export interface SessionDownloadResult {
	verified: number[];
	corrupt: number[];
	missing: number[];
	bytes: number;
	messagesSent: number;
	/** Highest in-flight total observed on any socket. */
	inflightHighWater: number;
	sockets: number;
}

export interface BlockRequest {
	piece: number;
	begin: number;
	length: number;
}

type Block = BlockRequest;

interface PieceWork {
	buffer: Uint8Array;
	have: Uint8Array;
	got: number;
	attempts: number;
}

interface Sock {
	client: TunnelClient;
	untested: number;
}

interface Live {
	key: string;
	sock: Sock;
	peer: PeerConnection;
	openedAt: number;
	unchokeMs: number | null;
	downloaded: number;
	lastBlockAt: number;
	/** Bytes/s, smoothed over 1 s samples (0 until it delivered). */
	rate: number;
	/** `downloaded` at the last rate sample. */
	sampledBytes: number;
	failures: number;
}

const blockKey = (b: Block) => `${b.piece}:${b.begin}`;

export function newSessionId(): string {
	const bytes = new Uint8Array(16);
	crypto.getRandomValues(bytes);
	return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** WebSocket URL of a session on `gatewayUrl`. */
export function tunnelUrl(gatewayUrl: string, sessionId: string): string {
	const raw = /^https?:/.test(gatewayUrl) ? gatewayUrl.replace(/^http/, "ws") : gatewayUrl;
	const url = new URL(raw);
	if (!url.pathname.endsWith("/v1/tunnel")) url.pathname = `${url.pathname.replace(/\/$/, "")}/v1/tunnel`;
	url.searchParams.set("session", sessionId);
	return url.toString();
}

export class ConnectionManager {
	readonly book: PeerBook;
	readonly sessionId: string;
	private readonly opts: Required<
		Pick<
			ConnectionManagerOptions,
			| "batchMs"
			| "pingIntervalMs"
			| "rateWindowMs"
			| "maxSockets"
			| "maxUntestedDials"
			| "inflightLimit"
			| "chokeReplaceAfterMs"
			| "deliveringWindowMs"
			| "reconnectDelayMs"
			| "stallTimeoutMs"
			| "sweepMs"
			| "dialIntervalMs"
			| "exploreAfterMs"
		>
	> &
		ConnectionManagerOptions;
	private readonly peerId: Uint8Array;
	private readonly pex = new PexSource();
	private readonly sockets: Sock[] = [];
	private readonly lives = new Map<string, Live>();
	private readonly samples: RateSample[] = [];
	private wantedSockets = 1;
	private opening: Promise<void> | undefined;
	private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	private reconnects = 0;
	private disconnectedAt = 0;
	/** connect() was called: keep the tunnel up before a download starts. */
	private sessionOpen = false;
	/** suspend() closed the sockets; the next requestBlocks reopens them. */
	private suspended = false;
	/** Blocks the page asked for (the scheduler), served ahead of the sequential queue. */
	private readonly externalQueue: Block[] = [];
	private readonly externalHandlers = new Map<string, Set<(data: Uint8Array) => void>>();
	/** Finishers of the requestBlocks calls still waiting. */
	private readonly externalRequests = new Set<() => void>();
	/** Blocks that arrived with nobody waiting (see ORPHAN_CACHE_BYTES), oldest first. */
	private readonly orphans = new Map<string, Uint8Array>();
	private orphanBytes = 0;
	private sweepTimer: ReturnType<typeof setInterval> | undefined;
	private stopped = false;
	private settled = false;
	private meta: Metainfo | undefined;
	private geometry: Geometry | undefined;
	private verifier: PieceVerifier | undefined;
	private hashes: HashFetcher | undefined;
	private wanted: number[] = [];
	private readonly queuedPieces = new Set<number>();
	private queue: Block[] = [];
	private readonly assigned = new Map<string, { block: Block; live: Live; external: boolean }>();
	private readonly work = new Map<number, PieceWork>();
	private readonly verified = new Set<number>();
	private readonly corrupt = new Set<number>();
	private verifying = 0;
	private downloaded = 0;
	private sampleBytes = 0;
	private sampleAt = 0;
	private lastProgress = 0;
	private sentBefore = 0;
	private highWater = 0;
	private untestedHigh = 0;
	private deliveringClosed = 0;
	private peakSockets = 0;
	private upgradeAttempts = 0;
	private lastDialAt = 0;
	/** When the last WebSocket opened, or the search for peers last made progress. */
	private exploreFrom = 0;
	private resolveDownload: ((r: SessionDownloadResult) => void) | undefined;
	private rejectDownload: ((e: Error) => void) | undefined;
	private onPiece: SessionDownloadOptions["onPiece"];

	constructor(options: ConnectionManagerOptions) {
		if (options.infoHash.length !== 20) throw new RangeError("info-hash must be 20 bytes");
		this.opts = {
			batchMs: DEFAULT_BATCH_MS,
			pingIntervalMs: PING_INTERVAL_MS,
			rateWindowMs: DEFAULT_WINDOW_MS,
			maxSockets: DEFAULT_SOCKETS_PER_SESSION,
			maxUntestedDials: MAX_UNTESTED_DIALS,
			inflightLimit: MAX_INFLIGHT_PER_SOCKET,
			chokeReplaceAfterMs: DEFAULT_CHOKE_REPLACE_MS,
			deliveringWindowMs: DEFAULT_DELIVERING_MS,
			reconnectDelayMs: DEFAULT_RECONNECT_MS,
			stallTimeoutMs: DEFAULT_STALL_MS,
			sweepMs: DEFAULT_SWEEP_MS,
			dialIntervalMs: DEFAULT_DIAL_INTERVAL_MS,
			exploreAfterMs: DEFAULT_EXPLORE_AFTER_MS,
			...options,
		};
		this.opts.inflightLimit = Math.min(Math.max(1, this.opts.inflightLimit), MAX_INFLIGHT_PER_SOCKET);
		this.opts.maxSockets = Math.min(Math.max(1, this.opts.maxSockets), DEFAULT_SOCKETS_PER_SESSION);
		this.opts.maxUntestedDials = Math.min(Math.max(1, this.opts.maxUntestedDials), MAX_UNTESTED_DIALS);
		this.book = options.book ?? new PeerBook({ allowPrivate: options.allowPrivate === true, ...(options.now ? { now: options.now } : {}) });
		this.pex.start(this.book.sink);
		this.sessionId = options.sessionId ?? newSessionId();
		this.peerId = options.peerId ?? ourPeerId();
	}

	get liveCount(): number {
		return this.lives.size;
	}

	get socketsOpen(): number {
		return this.sockets.length;
	}

	get downloadedBytes(): number {
		return this.downloaded;
	}

	/** WebSocket upgrades attempted (each one is a billed Worker request). */
	get upgrades(): number {
		return this.upgradeAttempts;
	}

	get messagesSent(): number {
		return this.sentBefore + this.sockets.reduce((n, s) => n + s.client.stats.messagesSent, 0);
	}

	get inflightHighWater(): number {
		return this.highWater;
	}

	get inflight(): number {
		return Math.max(0, ...this.sockets.map((s) => this.socketInflight(s)), 0);
	}

	/** Untested dials observed at once (never above the cap). */
	get untestedHighWater(): number {
		return this.untestedHigh;
	}

	/** Delivering connections this manager closed to free a slot (must stay 0). */
	get deliveringDropped(): number {
		return this.deliveringClosed;
	}

	addPeers(addresses: readonly string[], origin: PeerOrigin = "manual"): number {
		const n = this.book.addMany(addresses, origin);
		if (!this.stopped && (this.meta || this.sockets.length > 0)) this.fill();
		return n;
	}

	/** Open the tunnel and start dialling, before metadata or a download. */
	connect(): Promise<void> {
		if (this.stopped) return Promise.reject(new Error("stopped"));
		this.sessionOpen = true;
		this.armSweep();
		return this.openSocket().then(() => this.fill());
	}

	/** The torrent layout, once metadata has been checked. Peers can then be asked for blocks. */
	setInfo(meta: Metainfo): void {
		if (meta.infoHash !== bytesToHex(this.opts.infoHash)) throw new Error("metadata does not match the session info-hash");
		this.geometry = geometryOf(meta);
		this.verifier = verifierFor(meta);
		for (const live of this.lives.values()) live.peer.setGeometry(this.geometry);
	}

	/**
	 * Ask connected peers for these blocks (the page's scheduler). Resolves
	 * when every block arrived, the signal aborted, or the timeout elapsed.
	 * In-flight data stays within the socket cap.
	 */
	requestBlocks(
		blocks: readonly BlockRequest[],
		opts: { signal?: AbortSignal; timeoutMs?: number; onBlock(piece: number, begin: number, data: Uint8Array): void },
	): Promise<{ received: number; missing: number }> {
		if (!this.geometry) return Promise.reject(new Error("metadata is not known yet"));
		if (this.stopped) return Promise.resolve({ received: 0, missing: blocks.length });
		if (this.suspended) this.resume();
		const pending = new Map<string, (data: Uint8Array) => void>();
		let received = 0;
		return new Promise((resolve) => {
			let settled = false;
			const finish = () => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				this.externalRequests.delete(finish);
				opts.signal?.removeEventListener("abort", finish);
				for (const [key, handler] of pending) this.cancelExternal(key, handler);
				resolve({ received, missing: pending.size });
			};
			const timer = setTimeout(finish, opts.timeoutMs ?? 20_000);
			this.externalRequests.add(finish);
			opts.signal?.addEventListener("abort", finish, { once: true });
			for (const block of blocks) {
				const key = blockKey(block);
				if (pending.has(key)) continue;
				const handler = (data: Uint8Array) => {
					if (!pending.delete(key)) return;
					received++;
					opts.onBlock(block.piece, block.begin, data);
					if (pending.size === 0) finish();
				};
				pending.set(key, handler);
				const kept = this.orphans.get(key);
				if (kept) {
					this.orphans.delete(key);
					this.orphanBytes -= kept.length;
					queueMicrotask(() => handler(kept));
					continue;
				}
				let handlers = this.externalHandlers.get(key);
				if (!handlers) this.externalHandlers.set(key, (handlers = new Set()));
				handlers.add(handler);
				if (!this.externalQueue.some((b) => blockKey(b) === key) && !this.assigned.has(key)) this.externalQueue.push(block);
			}
			this.lastProgress = this.now();
			this.pipeline();
			if (pending.size === 0 || opts.signal?.aborted) finish();
		});
	}

	/** One requester gave up on `key`; the block is cancelled only when nobody else waits for it. */
	private cancelExternal(key: string, handler: (data: Uint8Array) => void): void {
		const handlers = this.externalHandlers.get(key);
		handlers?.delete(handler);
		if (handlers && handlers.size > 0) return;
		this.externalHandlers.delete(key);
		const at = this.externalQueue.findIndex((b) => blockKey(b) === key);
		if (at >= 0) this.externalQueue.splice(at, 1);
		// Already asked of a peer: no cancel (an upstream message, and the
		// page often asks again). It arrives as an orphan and is kept.
	}

	private keepOrphan(key: string, data: Uint8Array): void {
		if (this.orphans.has(key) || data.length > ORPHAN_CACHE_BYTES) return;
		this.orphans.set(key, data);
		this.orphanBytes += data.length;
		for (const [k, d] of this.orphans) {
			if (this.orphanBytes <= ORPHAN_CACHE_BYTES) break;
			this.orphans.delete(k);
			this.orphanBytes -= d.length;
		}
	}

	private armSweep(): void {
		if (this.sweepTimer !== undefined) return;
		this.lastProgress = this.now();
		this.sampleAt = this.lastProgress;
		this.sweepTimer = setInterval(() => this.sweep(), this.opts.sweepMs);
	}

	/**
	 * Download `meta` (sequential pieces) through the gateway. Resolves when
	 * every wanted piece is verified or given up; rejects if the session
	 * stalls or is aborted.
	 */
	download(meta: Metainfo, options: SessionDownloadOptions = {}): Promise<SessionDownloadResult> {
		if (this.meta) return Promise.reject(new Error("a download is already running"));
		if (meta.infoHash !== bytesToHex(this.opts.infoHash)) return Promise.reject(new Error("metadata does not match the session info-hash"));
		this.meta = meta;
		this.geometry = geometryOf(meta);
		this.verifier = verifierFor(meta);
		this.onPiece = options.onPiece;
		this.wanted = [...new Set(options.pieces ?? Array.from({ length: meta.pieceCount }, (_, i) => i))].filter(
			(p) => Number.isInteger(p) && p >= 0 && p < meta.pieceCount,
		);
		if (this.verifier instanceof V2Verifier) {
			this.hashes = new HashFetcher(this.verifier, {
				onLayer: () => {
					this.enqueueVerifiable();
					this.pipeline();
				},
			});
			this.hashes.want(this.verifier.missingLayers(this.wanted));
		}
		this.enqueueVerifiable();
		if (options.signal?.aborted) return Promise.reject(new Error("aborted"));
		options.signal?.addEventListener("abort", () => this.fail(new Error("aborted")), { once: true });
		return new Promise((resolve, reject) => {
			this.resolveDownload = resolve;
			this.rejectDownload = reject;
			this.armSweep();
			void this.openSocket().then(
				() => this.fill(),
				(e: unknown) => this.fail(e instanceof Error ? e : new Error("tunnel failed to open")),
			);
		});
	}

	stop(): void {
		this.stopped = true;
		this.cleanup();
		this.finish();
		this.endExternalRequests();
	}

	/**
	 * Close the WebSockets (the page has every block it wants) but keep the
	 * metadata and the peers learned so far. The next requestBlocks reopens
	 * the tunnel and redials the best peers.
	 */
	suspend(): void {
		if (this.stopped || this.suspended || this.meta) return;
		this.suspended = true;
		this.sessionOpen = false;
		this.endExternalRequests();
		this.closeConnections();
	}

	private resume(): void {
		this.suspended = false;
		this.sessionOpen = true;
		this.armSweep();
		void this.openSocket().catch(() => this.scheduleReconnect());
	}

	private endExternalRequests(): void {
		for (const finish of [...this.externalRequests]) finish();
	}

	private now(): number {
		return this.opts.now?.() ?? Date.now();
	}

	private enqueueVerifiable(): void {
		const verifier = this.verifier;
		if (!verifier) return;
		for (const piece of this.wanted) {
			if (this.verified.has(piece) || this.corrupt.has(piece) || this.queuedPieces.has(piece)) continue;
			if (!verifier.canVerify(piece)) continue;
			this.queuedPieces.add(piece);
			this.queue.push(...this.blocksOf(piece));
		}
	}

	private blocksOf(piece: number): Block[] {
		const size = pieceSizeOf(this.geometry!, piece);
		const out: Block[] = [];
		for (let begin = 0; begin < size; begin += BLOCK_SIZE) out.push({ piece, begin, length: Math.min(BLOCK_SIZE, size - begin) });
		return out;
	}

	private async openSocket(): Promise<void> {
		if (this.stopped || this.sockets.length >= this.wantedSockets) return;
		if (this.opening) {
			await this.opening;
			return this.openSocket();
		}
		let release!: () => void;
		this.opening = new Promise((r) => {
			release = r;
		});
		try {
			this.upgradeAttempts++;
			const client = await TunnelClient.connect(tunnelUrl(this.opts.gatewayUrl, this.sessionId), {
				factory: this.opts.factory,
				batchMs: this.opts.batchMs,
				timeoutMs: 10_000,
			});
			if (this.stopped || this.suspended) {
				client.close();
				return;
			}
			const sock: Sock = { client, untested: 0 };
			client.onClose = () => this.onSocketClosed(sock);
			this.sockets.push(sock);
			this.exploreFrom = this.now();
			if (this.sockets.length > this.peakSockets) this.peakSockets = this.sockets.length;
			this.reconnects = 0;
			this.disconnectedAt = 0;
			this.lastProgress = this.now();
			this.fill();
		} finally {
			this.opening = undefined;
			release();
		}
	}

	private onSocketClosed(sock: Sock): void {
		this.sentBefore += sock.client.stats.messagesSent;
		const i = this.sockets.indexOf(sock);
		if (i >= 0) this.sockets.splice(i, 1);
		if (this.sockets.length === 0) this.disconnectedAt = this.now();
		for (const [key, live] of [...this.lives]) {
			if (live.sock !== sock) continue;
			this.lives.delete(key);
			this.requeue(live);
			this.book.markClosed(key);
		}
		if (!this.stopped && !this.settled && !this.suspended && (this.meta || this.sessionOpen)) this.scheduleReconnect();
	}

	private scheduleReconnect(): void {
		if (this.reconnectTimer !== undefined || this.stopped) return;
		const delay = Math.min(30_000, this.opts.reconnectDelayMs * 2 ** Math.min(this.reconnects, 5));
		this.reconnects++;
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = undefined;
			if (this.stopped || this.settled || this.suspended) return;
			void this.openSocket().then(
				() => this.fill(),
				() => this.scheduleReconnect(),
			);
		}, delay);
	}

	private sweep(): void {
		if (this.stopped || this.settled) return;
		const now = this.now();
		this.keepAlives();
		this.explore(now);
		for (const sock of this.sockets) sock.client.pingIfIdle(now, this.opts.pingIntervalMs);
		this.noteRate(now);
		const quiet = this.sockets.length > 0 ? now - this.lastProgress : this.disconnectedAt > 0 ? now - this.disconnectedAt : 0;
		// A download can stall. Waiting on the file list must not close the tunnel.
		if (this.resolveDownload && quiet >= this.opts.stallTimeoutMs) {
			this.fail(new Error(`no progress for ${this.opts.stallTimeoutMs} ms`));
			return;
		}
		this.replace();
		this.fill();
		this.pipeline();
		this.checkDone();
	}

	private noteRate(now: number): void {
		if (now - this.sampleAt < RATE_SAMPLE_MS) return;
		const seconds = Math.max(0.001, (now - this.sampleAt) / 1000);
		for (const live of this.lives.values()) {
			const sample = (live.downloaded - live.sampledBytes) / seconds;
			live.sampledBytes = live.downloaded;
			if (live.downloaded === 0) continue;
			live.rate = live.rate === 0 ? sample : live.rate * 0.7 + sample * 0.3;
		}
		const rate = ((this.downloaded - this.sampleBytes) * 1000) / Math.max(1, now - this.sampleAt);
		this.samples.push({ at: now, rate });
		this.sampleAt = now;
		this.sampleBytes = this.downloaded;
		const cutoff = now - this.opts.rateWindowMs * 2;
		while (this.samples.length > 0 && this.samples[0].at < cutoff) this.samples.shift();
		const threshold = this.opts.rateThreshold;
		if (threshold === undefined) return;
		if (shouldAddSocket(this.samples, now, this.opts.rateWindowMs, threshold, this.wantedSockets, this.opts.maxSockets)) {
			this.wantedSockets++;
			void this.openSocket().catch(() => this.scheduleReconnect());
		}
	}

	private fill(): void {
		if (this.stopped || this.settled || this.suspended) return;
		const now = this.now();
		if (this.anyDelivering(now) && now - this.lastDialAt < this.opts.dialIntervalMs) return;
		let dialled = false;
		for (const sock of this.sockets) {
			if (sock.client.closed) continue;
			while (this.room(sock) > 0) {
				const allowUntested = sock.untested < this.opts.maxUntestedDials;
				const next = this.book.candidates(1, (p) => !allowUntested && !p.connectedBefore)[0];
				if (!next) break;
				this.dial(sock, next);
				dialled = true;
			}
		}
		if (dialled) this.lastDialAt = now;
	}

	/** Open another socket while nothing is connected (see DEFAULT_EXPLORE_AFTER_MS). */
	private explore(now: number): void {
		if (this.opts.exploreAfterMs <= 0 || this.opening || this.sockets.length === 0) return;
		if (this.lives.size > 0 || this.book.untried === 0) {
			this.exploreFrom = now;
			return;
		}
		// A socket can still try an untried address: no need for another.
		const canDial = (s: Sock) => !s.client.closed && this.room(s) > 0 && s.untested < this.opts.maxUntestedDials;
		if (this.sockets.length >= this.opts.maxSockets || this.sockets.some(canDial)) return;
		if (now - this.exploreFrom < this.opts.exploreAfterMs) return;
		this.exploreFrom = now;
		this.wantedSockets = Math.max(this.wantedSockets, this.sockets.length + 1);
		void this.openSocket().catch(() => this.scheduleReconnect());
	}

	private anyDelivering(now: number): boolean {
		for (const live of this.lives.values()) if (this.isDelivering(live, now)) return true;
		return false;
	}

	/** One message carries every keep-alive due (see KEEPALIVE_AFTER_MS). */
	private keepAlives(): void {
		const now = Date.now();
		let due = false;
		for (const live of this.lives.values()) if (now - live.peer.lastSentAt >= KEEPALIVE_AFTER_MS) due = true;
		if (!due) return;
		for (const live of this.lives.values()) if (now - live.peer.lastSentAt >= KEEPALIVE_ALONG_MS) live.peer.sendKeepAlive();
	}

	private room(sock: Sock): number {
		return Math.max(0, Math.min(sock.client.maxConnections, MAX_CONNECTIONS_PER_SOCKET) - sock.client.connections);
	}

	private dial(sock: Sock, entry: PeerEntry): void {
		const untested = !entry.connectedBefore;
		this.book.markConnecting(entry.key);
		if (untested) {
			sock.untested++;
			if (sock.untested > this.untestedHigh) this.untestedHigh = sock.untested;
		}
		const address = this.book.addressForOpen(entry.key);
		if (!address || sock.client.closed) {
			if (untested) sock.untested--;
			this.book.markFailed(entry.key, "forbidden_address");
			return;
		}
		void sock.client
			.open({ address, infoHash: this.opts.infoHash, peerId: this.peerId, timeoutMs: 20_000 })
			.then(
				(transport) => {
					if (this.stopped || sock.client.closed || this.lives.has(entry.key)) {
						transport.close();
						if (!this.lives.has(entry.key)) this.book.markClosed(entry.key);
						return;
					}
					this.attach(sock, entry.key, transport);
				},
				(e: unknown) => {
					if (sock.client.closed) this.book.markClosed(entry.key);
					// No slot at the gateway: the peer was never dialled, so it keeps its standing.
					else if (e instanceof TunnelOpenError && e.reason === FailReason.TOO_MANY_CONNECTIONS) this.book.markNotDialled(entry.key);
					else this.book.markFailed(entry.key, e instanceof TunnelOpenError ? failReasonName(e.reason) : "timeout");
				},
			)
			.finally(() => {
				if (untested) sock.untested = Math.max(0, sock.untested - 1);
			});
	}

	private attach(sock: Sock, key: string, transport: Transport): void {
		const live: Live = {
			key,
			sock,
			peer: undefined as unknown as PeerConnection,
			openedAt: this.now(),
			unchokeMs: null,
			downloaded: 0,
			lastBlockAt: 0,
			rate: 0,
			sampledBytes: 0,
			failures: 0,
		};
		const peer = new PeerConnection(transport, {
			geometry: this.geometry,
			// Keep-alives are sent together from the sweep (one message, not one per peer).
			keepAliveMs: 0,
			events: {
				have: () => this.pipeline(),
				extensions: () => this.opts.onExtensions?.(peer),
				metadata: (message) => this.opts.onMetadata?.(peer, message),
				unchoke: () => {
					if (live.unchokeMs === null) live.unchokeMs = this.now() - live.openedAt;
					this.lastProgress = this.now();
					this.pipeline();
				},
				block: (piece, begin, data) => {
					if (this.lives.get(key) !== live) return;
					this.onBlock(live, piece, begin, data.slice());
				},
				reject: (piece, begin, length) => {
					if (this.lives.get(key) !== live) return;
					live.failures++;
					this.requeueBlock({ piece, begin, length }, live);
					this.pipeline();
				},
				pex: (message) => {
					this.pex.ingest(message);
					this.fill();
				},
				hashes: (request, hashes) => void this.hashes?.onHashes(peer, request, hashes),
				hashReject: (request) => this.hashes?.onReject(peer, request),
				close: () => {
					if (this.lives.get(key) !== live) return;
					this.lives.delete(key);
					this.requeue(live);
					if (sock.client.closed) this.book.markClosed(key);
					else this.book.markFailed(key, "remote_closed");
					this.hashes?.onClose(peer);
					this.opts.onPeerClose?.(peer);
					if (!sock.client.closed) this.fill();
				},
			},
		});
		live.peer = peer;
		this.lives.set(key, live);
		this.book.markConnected(key);
		this.hashes?.offer(peer);
		peer.interested();
		this.pipeline();
	}

	private replace(): void {
		if (this.book.candidates(1).length === 0) return;
		const now = this.now();
		for (const sock of this.sockets) {
			if (sock.client.closed || this.room(sock) > 0) continue;
			const views = [...this.lives.values()].filter((l) => l.sock === sock).map((l) => this.view(l, now));
			const victim = replacementVictim(views, now, this.opts.chokeReplaceAfterMs);
			if (!victim) continue;
			const live = this.lives.get(victim.key);
			if (!live || this.isDelivering(live, now)) continue;
			// Back off, or the peer we just dropped is the next candidate (it connected before).
			this.drop(live, live.peer.peerChoking ? "choked" : "slow");
			return;
		}
	}

	private view(live: Live, now: number): SlotView {
		const delivering = this.isDelivering(live, now);
		const facts = this.facts(live);
		return {
			key: live.key,
			delivering,
			choked: live.peer.peerChoking,
			slow: !delivering && !live.peer.peerChoking && live.downloaded > 0 && live.rate > 0 && live.rate < this.bestRate() / 4,
			downloaded: live.downloaded,
			score: connectionScore(facts, now),
			openedAt: live.openedAt,
		};
	}

	private facts(live: Live): ScoreFacts {
		const entry = this.book.get(live.key);
		return {
			reachable: true,
			seed: live.peer.isSeed || entry?.seedHint === true,
			unchokeMs: live.unchokeMs,
			rate: live.rate,
			blocks: Math.floor(live.downloaded / BLOCK_SIZE),
			lastBlockAt: live.lastBlockAt,
			consecutiveFailures: live.failures,
			pieces: live.peer.pieceCount,
			pieceCount: this.geometry?.pieceCount ?? 0,
			reqq: live.peer.reqq,
			rejects: live.peer.rejects,
		};
	}

	private bestRate(): number {
		let best = 0;
		for (const live of this.lives.values()) if (live.rate > best) best = live.rate;
		return best;
	}

	private isDelivering(live: Live, now = this.now()): boolean {
		return live.downloaded > 0 && now - live.lastBlockAt < this.opts.deliveringWindowMs;
	}

	private drop(live: Live, reason?: string): void {
		if (this.isDelivering(live)) this.deliveringClosed++;
		this.lives.delete(live.key);
		this.requeue(live);
		if (reason) this.book.markFailed(live.key, reason);
		else this.book.markClosed(live.key);
		live.peer.close();
	}

	private pipeline(): void {
		if (!this.geometry) return;
		for (const live of this.lives.values()) {
			if (live.peer.closed || live.peer.peerChoking) continue;
			// Refill a peer's queue only once half of it has drained, so each
			// upstream message asks for seconds of data instead of a block or two.
			const depth = queueDepth(live.rate, live.peer.reqq, this.opts.inflightLimit);
			const outstanding = live.peer.outstandingRequests;
			if (!needsRefill(outstanding, depth)) continue;
			const allowance = Math.min(
				requestAllowance({
					socketInflight: this.socketInflight(live.sock),
					limit: this.opts.inflightLimit,
					peerOutstanding: outstanding,
					peerReqq: live.peer.reqq,
					blockLength: BLOCK_SIZE,
				}),
				(depth - outstanding) * BLOCK_SIZE,
			);
			if (allowance < 1) continue;
			const servingExternal = this.externalQueue.length > 0;
			const queue = servingExternal ? this.externalQueue : this.queue;
			const batch: Block[] = [];
			let bytes = 0;
			for (let i = 0; i < queue.length; i++) {
				const block = queue[i];
				if (!live.peer.has(block.piece)) continue;
				if (bytes + block.length > allowance) break;
				batch.push(block);
				bytes += block.length;
				queue.splice(i, 1);
				i--;
			}
			if (batch.length === 0) continue;
			for (const block of batch) this.assigned.set(blockKey(block), { block, live, external: servingExternal });
			live.peer.request(batch.map((b) => [b.piece, b.begin, b.length]));
			this.noteInflight(live.sock);
		}
	}

	private socketInflight(sock: Sock): number {
		let n = 0;
		for (const live of this.lives.values()) if (live.sock === sock) n += live.peer.outstandingBytes;
		return n;
	}

	private noteInflight(sock: Sock): void {
		const n = this.socketInflight(sock);
		if (n > this.highWater) this.highWater = n;
	}

	private onBlock(live: Live, piece: number, begin: number, data: Uint8Array): void {
		const assigned = this.assigned.get(`${piece}:${begin}`);
		if (!assigned || assigned.live !== live) return;
		this.assigned.delete(`${piece}:${begin}`);
		const now = this.now();
		this.lastProgress = now;
		live.downloaded += data.length;
		live.lastBlockAt = now;
		live.failures = 0;
		this.downloaded += data.length;
		this.book.markDelivered(live.key, data.length);
		if (assigned.external) {
			const key = `${piece}:${begin}`;
			const handlers = this.externalHandlers.get(key);
			this.externalHandlers.delete(key);
			if (!handlers || handlers.size === 0) this.keepOrphan(key, data);
			for (const handler of handlers ?? []) handler(data);
			this.pipeline();
			return;
		}
		this.store(piece, begin, data);
		this.pipeline();
		this.checkDone();
	}

	private store(piece: number, begin: number, data: Uint8Array): void {
		const size = pieceSizeOf(this.geometry!, piece);
		let work = this.work.get(piece);
		if (!work) {
			work = { buffer: new Uint8Array(size), have: new Uint8Array(Math.ceil(size / BLOCK_SIZE)), got: 0, attempts: 0 };
			this.work.set(piece, work);
		}
		const index = Math.floor(begin / BLOCK_SIZE);
		if (work.have[index]) return;
		work.buffer.set(data.subarray(0, Math.min(data.length, size - begin)), begin);
		work.have[index] = 1;
		work.got += data.length;
		if (work.got >= size) void this.verify(piece, work);
	}

	private async verify(piece: number, work: PieceWork): Promise<void> {
		this.verifying++;
		let ok: boolean;
		try {
			ok = (await this.verifier?.verify(piece, work.buffer)) === true;
		} catch {
			ok = false;
		}
		this.verifying--;
		if (this.settled) return;
		this.work.delete(piece);
		if (ok) {
			this.verified.add(piece);
			this.onPiece?.(piece, work.buffer);
		} else if (work.attempts >= 1) {
			this.corrupt.add(piece);
		} else {
			this.queue.push(...this.blocksOf(piece));
			const again: PieceWork = { buffer: new Uint8Array(work.buffer.length), have: new Uint8Array(work.have.length), got: 0, attempts: work.attempts + 1 };
			this.work.set(piece, again);
			this.pipeline();
		}
		this.checkDone();
	}

	private requeue(live: Live): void {
		const back: Block[] = [];
		const external: Block[] = [];
		for (const [k, a] of this.assigned) {
			if (a.live !== live) continue;
			this.assigned.delete(k);
			// An orphan nobody waits for any more is not asked again.
			if (a.external && !this.externalHandlers.get(k)?.size) continue;
			(a.external ? external : back).push(a.block);
		}
		if (external.length > 0) this.externalQueue.unshift(...external);
		if (back.length > 0) this.queue.unshift(...back);
	}

	private requeueBlock(block: Block, live: Live): void {
		const k = blockKey(block);
		const a = this.assigned.get(k);
		if (!a || a.live !== live) return;
		this.assigned.delete(k);
		if (a.external && !this.externalHandlers.get(k)?.size) return;
		(a.external ? this.externalQueue : this.queue).unshift(block);
	}

	private checkDone(): void {
		if (this.verifying > 0 || this.wanted.length === 0) return;
		if (this.verified.size + this.corrupt.size < this.wanted.length) return;
		this.finish();
	}

	private fail(error: Error): void {
		if (this.settled) return;
		this.settled = true;
		this.cleanup();
		this.rejectDownload?.(error);
	}

	private finish(): void {
		if (this.settled) return;
		this.settled = true;
		const missing = this.wanted.filter((p) => !this.verified.has(p) && !this.corrupt.has(p));
		const result: SessionDownloadResult = {
			verified: [...this.verified].sort((a, b) => a - b),
			corrupt: [...this.corrupt].sort((a, b) => a - b),
			missing,
			bytes: this.downloaded,
			messagesSent: this.messagesSent,
			inflightHighWater: this.highWater,
			sockets: this.peakSockets,
		};
		this.cleanup();
		this.resolveDownload?.(result);
	}

	private cleanup(): void {
		this.hashes?.stop();
		this.pex.stop();
		this.closeConnections();
	}

	/** Stop the timers, close every peer and every WebSocket. */
	private closeConnections(): void {
		if (this.sweepTimer !== undefined) clearInterval(this.sweepTimer);
		this.sweepTimer = undefined;
		if (this.reconnectTimer !== undefined) clearTimeout(this.reconnectTimer);
		this.reconnectTimer = undefined;
		// Detach first: a close event fired from here must not requeue or redial.
		const lives = [...this.lives.values()];
		this.lives.clear();
		for (const live of lives) {
			this.requeue(live);
			this.book.markClosed(live.key);
			live.peer.close();
		}
		// A socket's onClose runs synchronously and splices this.sockets: iterate a copy.
		for (const sock of this.sockets.splice(0)) {
			this.sentBefore += sock.client.stats.messagesSent;
			sock.client.onClose = undefined;
			sock.client.close();
		}
	}
}

/**
 * How many requests a peer should have queued: QUEUE_SECONDS of its rate
 * (INITIAL_DEPTH before it is known), never more than its `reqq` or the
 * socket's in-flight cap.
 */
export function queueDepth(rate: number, reqq: number, inflightLimit = MAX_INFLIGHT_PER_SOCKET): number {
	const cap = Math.max(1, Math.min(reqq, Math.floor(inflightLimit / BLOCK_SIZE)));
	const wanted = rate > 0 ? Math.round((rate * QUEUE_SECONDS) / BLOCK_SIZE) : INITIAL_DEPTH;
	return Math.min(cap, Math.max(MIN_DEPTH, wanted));
}

/**
 * A peer's queue gets more requests only once at most half of `depth` is
 * left. Upstream messages are billed, data is not: refilling half a queue
 * at a time keeps a message worth seconds of data, whatever the peer's speed.
 */
export function needsRefill(outstanding: number, depth: number): boolean {
	return outstanding <= Math.floor(Math.max(1, depth) / 2);
}

function ourPeerId(): Uint8Array {
	const id = new Uint8Array(20);
	const prefix = new TextEncoder().encode("-MS0001-");
	id.set(prefix);
	crypto.getRandomValues(id.subarray(prefix.length));
	return id;
}
