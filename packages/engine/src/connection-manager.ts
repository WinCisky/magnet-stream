// Connection manager (docs/architecture.md §Chunk recovery › Connection
// management, §Tunnel protocol and request budget › Rules): connections
// stay up for the session, dials race only at connect time, and a peer
// that is delivering is never dropped to make room. Upstream frames of a
// socket are one message every 300–500 ms; in-flight data stays within
// 16 MiB per socket, whatever a peer advertises. A dead gateway is
// redialled and unfinished blocks are asked for again.
//
// Which block goes to which peer is the deadline picker's choice
// (picker.ts): container index, playback window from the fastest peers,
// read-ahead, scarce pieces, back-fill. Each recovery (`recover`, or
// `download` for a whole torrent) is one picker target; a seek withdraws
// requests with `cancel` and keeps the connections. The VPS gateway is M4-06. Nothing is logged: a peer address or an info-hash must
// not reach the console. Detailed debug stays opt-in in the page (`mw-debug`).
//
// Dialling follows the VPS scout (revision of 2026-11-19, OQ-21, OQ-23):
// addresses the scout saw alive may take all 6 slots of a socket, untried
// ones at most 3 (OQ-03: a dead connect holds its slot ~20 s, and that
// hold counts as in flight). Until a peer delivers (metadata or the first
// piece still missing), more candidates than free slots open the second
// and third WebSocket at once. A connection with none of the wanted pieces
// is the first replaced when a candidate waits.
//
// Keeping dead connects off the sockets that carry data: untried dials go
// only to sockets none of whose peers has unchoked us
// (or, when every socket carries data, to one of them); an exploring
// socket jammed by held dead connects is closed and replaced (at most
// `maxRecycles` times, one per `recycleIntervalMs`: each is a billed
// upgrade); and once enough peers deliver fast enough, untried addresses
// are not dialled at all.
//
// Width costs while nothing uses it: a WebSocket with no connection still
// pings every 30 s. The rate rule adds a socket only when candidates
// outnumber the free slots, and a surplus socket left without any
// connection for IDLE_SOCKET_MS is closed (never the last one).
//
// Verification (§Chunk recovery › Verification, M3-02): a piece reaches
// `onPiece` (store, player) only after its hash check. Every kept block is
// attributed to the peer that sent it (attribution.ts); a failed piece is
// dropped and fetched again, a mixed one from a single peer to isolate the
// culprit (v2: its leaf hashes name the bad blocks directly). A peer that
// causes two failed pieces is closed and banned in the PeerBook for the
// session, and the unverified blocks it sent are thrown away. Hashing is
// WebCrypto, in the engine's dedicated worker: never on the page's thread.
//
// Reciprocation (§Chunk recovery › Reciprocation (upload), OQ-04, M3-12,
// upload.ts): with a PieceSource and the setting on, verified pieces are
// announced (bitfield or have_none + have) and served to the peers the
// tit-for-tat choker unchokes, at a capped total rate, only while a
// recovery runs; every upload message rides on the batched upstream
// (alone, at most one flush per 2 s per socket). Without a source, or with
// the setting off, nothing is uploaded (as M1-06).

import {
	CLOSE_SLOT_GRACE_MS,
	DEFAULT_SOCKETS_PER_SESSION,
	FAILED_CONNECT_HOLD_MS,
	FailReason,
	MAX_CONNECTIONS_PER_SOCKET,
	PING_INTERVAL_MS,
	formatAddress,
} from "@magnet-stream/tunnel-protocol";
import { Attribution, blockDigests, type AttributionOptions, type Verdict } from "./attribution.ts";
import type { MetadataMessage } from "./extension.ts";
import { HashFetcher } from "./hashes.ts";
import { bytesToHex } from "./magnet.ts";
import { BLOCK_SIZE, geometryOf, type Metainfo } from "./metainfo.ts";
import { PeerBook, PexSource, type DialClass, type PeerEntry, type PeerOrigin } from "./peer-book.ts";
import type { ScoutFacts } from "./peers-stream.ts";
import { PeerConnection } from "./peer-connection.ts";
import { pieceSizeOf, type Geometry } from "./peer-wire.ts";
import { Picker, type BlockCancel, type PickedBlock, type PickerConfig, type PickerPeer } from "./picker.ts";
import { connectionScore, type ScoreFacts } from "./score.ts";
import { replacementVictim, requestAllowance, shouldAddSocket, type RateSample, type SlotView } from "./slots.ts";
import type { Transport } from "./transport.ts";
import { TunnelClient, TunnelOpenError, failReasonName, type WebSocketFactory } from "./tunnel-client.ts";
import { UPLOAD_DEFAULTS, Uploader, type UploadOptions, type UploadStats } from "./upload.ts";
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
 * A connection's bitfield (or have_all / have_none) comes right after the
 * handshake: before this long, "it has none of the wanted pieces" is not
 * known yet.
 */
const USELESS_GRACE_MS = 3_000;
/** Recycle an exploring socket only if its held slots would stay taken at least this long. */
const RECYCLE_MIN_GAIN_MS = 5_000;
/**
 * A surplus WebSocket without any connection (nor slot held) for this long
 * is closed: shorter than the 30 s ping, so it costs no message. On a thin
 * swarm two idle sockets otherwise sent a ping every 30 s for the whole
 * session (M3-07 measurements).
 */
export const IDLE_SOCKET_MS = 25_000;

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
	/**
	 * Once the video's bitrate is known (`setBitrate`), `rateThreshold`
	 * becomes this many times it (default 2: another socket after
	 * `rateWindowMs` under twice the bitrate, OQ-11).
	 */
	socketRateFactor?: number;
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
	/**
	 * Open the second and third WebSocket at once while nothing has been
	 * delivered and candidates outnumber the free slots (default true).
	 */
	earlyWidth?: boolean;
	/** With this many peers delivering (and the rate above `rateThreshold`), untried addresses are not dialled (default 3). */
	enoughDelivering?: number;
	/** Exploring sockets closed and replaced when jammed by dead connects, at most (default 4; 0 disables). */
	maxRecycles?: number;
	/** Least time between two recycles (default 20 s). */
	recycleIntervalMs?: number;
	now?: () => number;
	factory?: WebSocketFactory;
	sessionId?: string;
	/** A peer's extended handshake arrived (metadata fetch). */
	onExtensions?(conn: PeerConnection): void;
	/** A ut_metadata message from a peer. */
	onMetadata?(conn: PeerConnection, message: MetadataMessage): void;
	/** A peer connection closed. */
	onPeerClose?(conn: PeerConnection): void;
	/** Windows and thresholds of the block picker (see picker.ts for the defaults). */
	picker?: Partial<PickerConfig>;
	/** Strikes before a corrupting peer is banned, and memory bounds (see attribution.ts). */
	attribution?: AttributionOptions;
	/** A peer was banned for corrupt data (diagnostics: no address is passed). */
	onBan?(): void;
	/** Reciprocation: where verified pieces are read, the setting, the caps (upload.ts). No source: no upload. */
	upload?: UploadOptions;
}

export interface RecoverOptions {
	/** Playback position, bytes from the start of the range (default 0). */
	position?: number;
	/** Playback window in bytes (default: the picker's, 30 s at its bitrate). */
	windowBytes?: number;
	/** Only these pieces of the range (default: all). */
	pieces?: readonly number[];
	/** Pieces already verified and stored: not fetched. */
	have?: Iterable<number>;
	/**
	 * Re-request a piece that failed verification this many times (default:
	 * always). Failures proven to be a banned peer's fault do not count.
	 */
	retries?: number;
	/** A verified piece of the range. */
	onPiece?(piece: number, data: Uint8Array): void;
}

/** One file (or byte range) being recovered by the picker. */
export interface Recovery {
	readonly id: number;
	/** Pieces covering the range. */
	readonly firstPiece: number;
	readonly lastPiece: number;
	/** Resolves true once every wanted piece is verified, false if stopped first. */
	readonly done: Promise<boolean>;
	/** The viewer jumped: requests outside the new window are cancelled (connections stay). */
	seek(byte: number): void;
	/** Playback moved on (no cancels). */
	setPosition(byte: number): void;
	setWindowBytes(bytes: number | undefined): void;
	/** The playback window in bytes now (it follows the bitrate unless set in bytes). */
	windowBytes(): number;
	/** Pieces being assembled: [piece, blocks received, blocks]. */
	assembling(): Array<[number, number, number]>;
	/** A verified piece was lost from storage: fetch it again. */
	forget(piece: number): void;
	stop(): void;
}

interface RecoveryState {
	id: number;
	first: number;
	last: number;
	only: Set<number> | undefined;
	retries: number;
	onPiece: RecoverOptions["onPiece"];
	settle(complete: boolean): void;
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
	/** Duplicate requests for late blocks, and duplicate copies discarded. */
	hedges: number;
	duplicates: number;
	/** Pieces that failed their check, and peers banned for it. */
	hashFailures: number;
	banned: number;
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

/** download(): failed checks of one piece before it is given up (failures pinned on a banned peer excluded). */
const DOWNLOAD_RETRIES = 3;

interface Sock {
	client: TunnelClient;
	/** Untried dials in flight. */
	untested: number;
	/** Until when the slots of untried dials that timed out stay taken (OQ-03). */
	untestedHeld: number[];
	/** Since when it has had no connection, no open in flight and no held slot. */
	emptySince: number;
}

export interface DialCounts {
	/** `open`s sent. */
	attempted: number;
	succeeded: number;
	/** By reason: the gateway's FailReason names, `timeout` (no answer), `busy`, `closed`. */
	failed: Record<string, number>;
}

/** Counters for the page's diagnostics (no addresses, no info-hash). */
export interface ConnectionDiagnostics {
	/** Dials by class: scout-alive, untried, known (connected before). */
	dials: Record<DialClass, DialCounts>;
	connected: number;
	unchoked: number;
	/** Delivered a block in the last few seconds. */
	delivering: number;
	seeds: number;
	/** Ms from the session start to the first unchoke by any peer (null: none yet). */
	firstUnchokeMs: number | null;
	/** Where the verified metadata came from, and when (ms from the session start). */
	metadata: { source: string; ms: number } | null;
	sockets: number;
	upgrades: number;
	messagesSent: number;
	/** Most untried dials in flight (or holding a slot) on one socket. */
	untestedHighWater: number;
	/** Exploring sockets closed and replaced because dead connects jammed them. */
	recycles: number;
	/** Reciprocation (M3-12). */
	upload: UploadDiagnostics;
}

/** What the upload did (no addresses). */
export interface UploadDiagnostics {
	/** The setting is on and there is a store to read from. */
	enabled: boolean;
	/** A download is running, so peers may be unchoked. */
	active: boolean;
	/** Piece bytes sent to peers. */
	bytes: number;
	/** Peers we unchoke now. */
	unchoked: number;
	/** Upstream messages that carried only upload traffic (what the upload cost). */
	messages: number;
	/** Pieces we announce. */
	pieces: number;
	/** Requests refused (rejected or ignored). */
	refused: number;
	metadataPieces: number;
}

const newDialCounts = (): DialCounts => ({ attempted: 0, succeeded: 0, failed: {} });

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
			| "earlyWidth"
			| "enoughDelivering"
			| "maxRecycles"
			| "recycleIntervalMs"
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
	/** download() is running (the session closes when it ends). */
	private downloading = false;
	private info: Metainfo | undefined;
	private geometry: Geometry | undefined;
	private verifier: PieceVerifier | undefined;
	private hashes: HashFetcher | undefined;
	private picker: Picker | undefined;
	private readonly recoveries = new Map<number, RecoveryState>();
	private wanted: number[] = [];
	/** Blocks of requestBlocks calls asked of a peer. */
	private readonly assigned = new Map<string, { block: Block; live: Live }>();
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
	/** When connect(), download() or recover() first started the session (0: not yet). */
	private startedAt = 0;
	private firstUnchokeAt = 0;
	private metadataInfo: { source: string; ms: number } | null = null;
	private readonly dialCounts: Record<DialClass, DialCounts> = { known: newDialCounts(), scout: newDialCounts(), untried: newDialCounts() };
	private recycles = 0;
	private lastRecycleAt = -Infinity;
	private resolveDownload: ((r: SessionDownloadResult) => void) | undefined;
	private rejectDownload: ((e: Error) => void) | undefined;
	/** Cancels sent after seeks (urgent) and for duplicates that lost their race. */
	private cancelsSent = 0;
	private hashFailureCount = 0;
	private readonly attribution: Attribution;
	/** Video bytes per second given by `setBitrate` (0: not known). */
	private videoBitrate = 0;
	private readonly uploader: Uploader;
	/** Upload-only messages of sockets already closed. */
	private lazyBefore = 0;

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
			earlyWidth: true,
			enoughDelivering: 3,
			maxRecycles: 4,
			recycleIntervalMs: 20_000,
			...options,
		};
		this.opts.inflightLimit = Math.min(Math.max(1, this.opts.inflightLimit), MAX_INFLIGHT_PER_SOCKET);
		this.opts.maxSockets = Math.min(Math.max(1, this.opts.maxSockets), DEFAULT_SOCKETS_PER_SESSION);
		this.opts.maxUntestedDials = Math.min(Math.max(1, this.opts.maxUntestedDials), MAX_UNTESTED_DIALS);
		this.book = options.book ?? new PeerBook({ allowPrivate: options.allowPrivate === true, ...(options.now ? { now: options.now } : {}) });
		this.pex.start(this.book.sink);
		this.sessionId = options.sessionId ?? newSessionId();
		this.peerId = options.peerId ?? ourPeerId();
		this.attribution = new Attribution(options.attribution);
		this.uploader = new Uploader(options.upload, () => this.now());
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

	/** Messages that carried only upload traffic (lazy frames flushed alone). */
	get uploadMessages(): number {
		return this.lazyBefore + this.sockets.reduce((n, s) => n + s.client.stats.lazyMessages, 0);
	}

	/** Upload counters (bytes, blocks, refusals...). */
	get uploadStats(): UploadStats {
		const s = this.uploader.stats;
		return { ...s, refused: { ...s.refused } };
	}

	/** Peers we unchoke now. */
	get uploadUnchoked(): number {
		return this.uploader.unchoked;
	}

	/** Bytes uploaded to one peer (`ip:port`), for tests. */
	uploadedTo(key: string): number {
		return this.uploader.uploadedTo(key);
	}

	/** The user's "share with peers" setting: off chokes everyone and stops announcing pieces. */
	setUpload(enabled: boolean): void {
		this.uploader.setEnabled(enabled);
	}

	/**
	 * Pieces the source can now serve (e.g. the store opened): announced to
	 * the peers that lack them. Verified pieces of this session are offered
	 * on their own.
	 */
	offerPieces(pieces: Iterable<number>): void {
		for (const piece of pieces) this.uploader.offer(piece);
	}

	get inflightHighWater(): number {
		return this.highWater;
	}

	/** Cancel messages sent (seeks, stopped recoveries, duplicates). */
	get cancels(): number {
		return this.cancelsSent;
	}

	/** Pieces that failed verification so far. */
	get hashFailures(): number {
		return this.hashFailureCount;
	}

	/** Peers banned this session for corrupt data. */
	get bannedPeers(): number {
		return this.attribution.banned;
	}

	/** Failed pieces by how their culprit was found (sole sender, mixed; then by diff or by v2 leaf hashes). */
	get attributionCounts(): Attribution["counts"] {
		return { ...this.attribution.counts };
	}

	/** Strikes of a peer (`ip:port`), for tests. */
	strikesOf(key: string): number {
		return this.attribution.strikesOf(key);
	}

	/** Duplicate requests for late blocks so far. */
	get hedges(): number {
		return this.picker?.hedgesSent ?? 0;
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

	/** Counters for the diagnostics (M3-11): dials by class and outcome, peers, first unchoke, metadata source. */
	diagnostics(): ConnectionDiagnostics {
		const now = this.now();
		let unchoked = 0;
		let delivering = 0;
		let seeds = 0;
		for (const live of this.lives.values()) {
			if (!live.peer.peerChoking) unchoked++;
			if (this.isDelivering(live, now)) delivering++;
			if (live.peer.isSeed) seeds++;
		}
		const copy = (c: DialCounts): DialCounts => ({ attempted: c.attempted, succeeded: c.succeeded, failed: { ...c.failed } });
		return {
			dials: { known: copy(this.dialCounts.known), scout: copy(this.dialCounts.scout), untried: copy(this.dialCounts.untried) },
			connected: this.lives.size,
			unchoked,
			delivering,
			seeds,
			firstUnchokeMs: this.firstUnchokeAt > 0 ? this.firstUnchokeAt - this.startedAt : null,
			metadata: this.metadataInfo ? { ...this.metadataInfo } : null,
			sockets: this.sockets.length,
			upgrades: this.upgradeAttempts,
			messagesSent: this.messagesSent,
			untestedHighWater: this.untestedHigh,
			recycles: this.recycles,
			upload: {
				enabled: this.uploader.on,
				active: this.uploader.isActive,
				bytes: this.uploader.stats.bytes,
				unchoked: this.uploader.unchoked,
				messages: this.uploadMessages,
				pieces: this.uploader.availableCount,
				refused: this.uploader.stats.rejected + this.uploader.stats.ignored,
				metadataPieces: this.uploader.stats.metadataPieces,
			},
		};
	}

	private markStarted(): void {
		if (this.startedAt === 0) this.startedAt = this.now();
	}

	/**
	 * Addresses to dial. `scouts` (discovery only, same order) are the VPS
	 * scout's facts: a dial-order prior, never an authorisation.
	 */
	addPeers(addresses: readonly string[], origin: PeerOrigin = "manual", scouts?: readonly (ScoutFacts | undefined)[]): number {
		const n = this.book.addMany(addresses, origin, undefined, scouts);
		if (!this.stopped && (this.downloading || this.sockets.length > 0)) this.fill();
		return n;
	}

	/** Open the tunnel and start dialling, before metadata or a download. */
	connect(): Promise<void> {
		if (this.stopped) return Promise.reject(new Error("stopped"));
		this.sessionOpen = true;
		this.markStarted();
		this.armSweep();
		return this.openSocket().then(() => this.fill());
	}

	/**
	 * The torrent layout, once metadata has been checked. Peers can then be
	 * asked for blocks. `source` (diagnostics only): where it came from.
	 * `infoDict`: the verified info dict, served to peers over ut_metadata.
	 */
	setInfo(meta: Metainfo, source = "unknown", infoDict?: Uint8Array): void {
		if (meta.infoHash !== bytesToHex(this.opts.infoHash)) throw new Error("metadata does not match the session info-hash");
		if (this.info) return;
		this.info = meta;
		this.markStarted();
		this.metadataInfo = { source: source.slice(0, 32), ms: this.now() - this.startedAt };
		this.geometry = geometryOf(meta);
		const verifier = verifierFor(meta);
		this.verifier = verifier;
		this.picker = new Picker(this.geometry, this.opts.picker, (piece) => verifier.canVerify(piece));
		if (this.videoBitrate > 0) this.picker.setBitrate(this.videoBitrate);
		if (verifier instanceof V2Verifier) {
			this.hashes = new HashFetcher(verifier, { onLayer: () => this.pipeline() });
			for (const live of this.lives.values()) this.hashes.offer(live.peer);
		}
		for (const live of this.lives.values()) live.peer.setGeometry(this.geometry);
		this.uploader.setInfo(this.geometry, infoDict);
	}

	/**
	 * Video bytes per second, known once the player parsed the container
	 * (M3-06): it sizes the picker's playback window and scarce-piece
	 * threshold, and another socket opens after `rateWindowMs` under
	 * `socketRateFactor` times it. Kept for a picker made later.
	 */
	setBitrate(bytesPerSecond: number): void {
		if (!(bytesPerSecond > 0) || !Number.isFinite(bytesPerSecond)) return;
		this.videoBitrate = bytesPerSecond;
		this.picker?.setBitrate(bytesPerSecond);
		const factor = this.opts.socketRateFactor ?? 2;
		if (factor > 0 && Number.isFinite(factor)) this.opts.rateThreshold = bytesPerSecond * factor;
	}

	/** Video bytes per second (0: not known yet). */
	get bitrate(): number {
		return this.videoBitrate;
	}

	/** Under this rate for `rateWindowMs`, another socket opens (undefined: never). */
	get rateThreshold(): number | undefined {
		return this.opts.rateThreshold;
	}

	/**
	 * Recover the torrent bytes [range.offset, range.offset + range.size)
	 * with the deadline picker, alongside any other recovery. Verified pieces
	 * go to `onPiece`. The session stays open when it completes.
	 */
	recover(range: { offset: number; size: number }, options: RecoverOptions = {}): Recovery {
		const picker = this.picker;
		if (!picker || !this.verifier) throw new Error("metadata is not known yet");
		if (this.stopped) throw new Error("stopped");
		if (this.suspended) this.resume();
		this.markStarted();
		for (const piece of options.have ?? []) {
			if (!Number.isInteger(piece) || piece < 0 || piece >= this.geometry!.pieceCount) continue;
			picker.markVerified(piece);
			this.verified.add(piece);
			this.uploader.offer(piece);
		}
		const id = picker.addTarget(range, { position: options.position, windowBytes: options.windowBytes, pieces: options.pieces });
		const { first, last } = picker.pieceRange(id)!;
		let settle!: (complete: boolean) => void;
		const done = new Promise<boolean>((resolve) => {
			settle = resolve;
		});
		const state: RecoveryState = {
			id,
			first,
			last,
			only: options.pieces ? new Set(options.pieces) : undefined,
			retries: options.retries ?? Infinity,
			onPiece: options.onPiece,
			settle: (complete) => {
				if (!this.recoveries.delete(id)) return;
				const withdrawn = picker.removeTarget(id);
				this.withdrawn(withdrawn.cancels, withdrawn.dropped, false);
				settle(complete);
			},
		};
		this.recoveries.set(id, state);
		if (this.hashes && this.verifier instanceof V2Verifier) {
			const pieces: number[] = [];
			for (let p = first; p <= last; p++) if (!state.only || state.only.has(p)) pieces.push(p);
			this.hashes.want(this.verifier.missingLayers(pieces));
		}
		this.armSweep();
		this.lastProgress = this.now();
		const handle: Recovery = {
			id,
			firstPiece: first,
			lastPiece: last,
			done,
			seek: (byte) => {
				if (!this.recoveries.has(id)) return;
				const r = picker.seek(id, byte);
				this.withdrawn(r.cancels, r.dropped, true);
				this.lastProgress = this.now();
				this.pipeline();
			},
			setPosition: (byte) => {
				if (!this.recoveries.has(id)) return;
				picker.setPosition(id, byte);
				this.pipeline();
			},
			setWindowBytes: (bytes) => {
				if (this.recoveries.has(id)) picker.setWindowBytes(bytes, id);
			},
			windowBytes: () => picker.windowBytesOf(id),
			assembling: () => {
				const out: Array<[number, number, number]> = [];
				for (const piece of this.work.keys()) if (piece >= first && piece <= last) out.push([piece, picker.receivedBlocks(piece), picker.blocks(piece)]);
				return out;
			},
			forget: (piece) => {
				if (!this.recoveries.has(id) || !covers(state, piece) || !this.verified.delete(piece)) return;
				picker.reset(piece);
				this.pipeline();
			},
			stop: () => state.settle(false),
		};
		// Already complete (every piece stored earlier)?
		queueMicrotask(() => this.checkRecoveries());
		if (this.sockets.length === 0 && !this.opening) void this.openSocket().catch(() => this.scheduleReconnect());
		this.pipeline();
		return handle;
	}

	/** Apply cancels from the picker and drop the bytes of pieces it gave up. */
	private withdrawn(cancels: readonly BlockCancel[], dropped: readonly number[], urgent: boolean): void {
		for (const c of cancels) if (this.lives.get(c.peer)?.peer.cancel(c.piece, c.begin, urgent)) this.cancelsSent++;
		for (const piece of dropped) {
			this.work.delete(piece);
			this.attribution.clear(piece);
		}
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
	 * Download `meta` (all pieces, or `pieces`) through the gateway with the
	 * deadline picker. Resolves when every wanted piece is verified or given
	 * up, then closes the session; rejects if the session stalls or is aborted.
	 */
	download(meta: Metainfo, options: SessionDownloadOptions = {}): Promise<SessionDownloadResult> {
		if (this.downloading) return Promise.reject(new Error("a download is already running"));
		if (meta.infoHash !== bytesToHex(this.opts.infoHash)) return Promise.reject(new Error("metadata does not match the session info-hash"));
		if (options.signal?.aborted) return Promise.reject(new Error("aborted"));
		this.setInfo(meta);
		this.markStarted();
		this.downloading = true;
		this.wanted = [...new Set(options.pieces ?? Array.from({ length: meta.pieceCount }, (_, i) => i))].filter(
			(p) => Number.isInteger(p) && p >= 0 && p < meta.pieceCount,
		);
		options.signal?.addEventListener("abort", () => this.fail(new Error("aborted")), { once: true });
		return new Promise((resolve, reject) => {
			this.resolveDownload = resolve;
			this.rejectDownload = reject;
			this.armSweep();
			if (this.wanted.length === 0) {
				queueMicrotask(() => this.finish());
				return;
			}
			const recovery = this.recover({ offset: 0, size: meta.totalLength }, { pieces: this.wanted, retries: DOWNLOAD_RETRIES, onPiece: options.onPiece });
			void recovery.done.then((complete) => {
				if (complete) this.finish();
			});
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
		if (this.stopped || this.suspended || this.downloading || this.recoveries.size > 0) return;
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
				lazyMs: this.opts.upload?.flushMs ?? UPLOAD_DEFAULTS.flushMs,
			});
			if (this.stopped || this.suspended) {
				client.close();
				return;
			}
			const sock: Sock = { client, untested: 0, untestedHeld: [], emptySince: this.now() };
			client.onClose = () => this.onSocketClosed(sock);
			this.sockets.push(sock);
			if (this.sockets.length > this.peakSockets) this.peakSockets = this.sockets.length;
			this.reconnects = 0;
			this.disconnectedAt = 0;
			this.lastProgress = this.now();
			this.fill();
		} finally {
			this.opening = undefined;
			release();
		}
		// The next socket, if candidates still outnumber the slots (early width).
		this.widen();
	}

	private onSocketClosed(sock: Sock): void {
		this.sentBefore += sock.client.stats.messagesSent;
		this.lazyBefore += sock.client.stats.lazyMessages;
		const i = this.sockets.indexOf(sock);
		if (i >= 0) this.sockets.splice(i, 1);
		if (this.sockets.length === 0) this.disconnectedAt = this.now();
		for (const [key, live] of [...this.lives]) {
			if (live.sock !== sock) continue;
			this.lives.delete(key);
			this.uploader.remove(key);
			this.requeue(live);
			this.book.markClosed(key);
		}
		if (!this.stopped && !this.settled && !this.suspended && (this.downloading || this.sessionOpen || this.recoveries.size > 0)) this.scheduleReconnect();
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
		for (const sock of this.sockets) sock.client.pingIfIdle(now, this.opts.pingIntervalMs);
		this.noteRate(now);
		this.recycle(now);
		this.closeIdleSockets(now);
		this.uploadTick(now);
		const quiet = this.sockets.length > 0 ? now - this.lastProgress : this.disconnectedAt > 0 ? now - this.disconnectedAt : 0;
		// A download can stall. Waiting on the file list must not close the tunnel.
		if (this.resolveDownload && quiet >= this.opts.stallTimeoutMs) {
			this.fail(new Error(`no progress for ${this.opts.stallTimeoutMs} ms`));
			return;
		}
		this.replace();
		this.fill();
		this.pipeline();
		this.checkRecoveries();
	}

	/** Choke/unchoke and serve: only while a recovery runs (idle, suspended or done: everyone choked). */
	private uploadTick(now: number): void {
		const active = !this.stopped && !this.settled && !this.suspended && this.recoveries.size > 0;
		this.uploader.tick(active, (key) => this.lives.get(key)?.downloaded ?? 0, now);
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
		// Slow, and someone to dial that the open sockets have no room for.
		if (
			shouldAddSocket(this.samples, now, this.opts.rateWindowMs, threshold, this.wantedSockets, this.opts.maxSockets) &&
			this.sockets.length >= this.wantedSockets &&
			this.candidatesExceedSlots()
		) {
			this.wantedSockets++;
			void this.openSocket().catch(() => this.scheduleReconnect());
		}
	}

	private fill(): void {
		if (this.stopped || this.settled || this.suspended) return;
		const now = this.now();
		if (this.anyDelivering(now) && now - this.lastDialAt < this.opts.dialIntervalMs) return;
		let dialled = false;
		const exploring = this.exploringSockets(now);
		const untried = (p: PeerEntry) => this.book.dialClass(p) === "untried";
		for (;;) {
			const open = this.sockets.filter((s) => !s.client.closed && this.room(s) > 0);
			if (open.length === 0) break;
			const explorers = open.filter((s) => exploring.has(s) && this.untestedLoad(s, now) < this.opts.maxUntestedDials);
			const next = this.book.candidates(1, explorers.length > 0 ? undefined : untried)[0];
			if (!next) break;
			// Untried: the exploring socket with most room. Live and known: a socket
			// with the fewest dead connects pending, then the most room.
			const pool = untried(next) ? explorers : open;
			const sock = pool.reduce((best, s) => {
				const by = this.untestedLoad(s, now) - this.untestedLoad(best, now);
				if (!untried(next) && by !== 0) return by < 0 ? s : best;
				return this.room(s) > this.room(best) ? s : best;
			});
			this.dial(sock, next);
			dialled = true;
		}
		if (dialled) this.lastDialAt = now;
		this.widen();
	}

	/** Whether more peers are worth looking for (untried addresses are dialled only then). */
	private needMorePeers(now: number): boolean {
		if (!this.info || this.downloaded === 0) return true;
		let delivering = 0;
		for (const live of this.lives.values()) if (this.isDelivering(live, now)) delivering++;
		if (delivering < this.opts.enoughDelivering) return true;
		const threshold = this.opts.rateThreshold;
		if (threshold === undefined) return false;
		// The last full sample, or the bytes since it if that is more (data may have just started).
		const last = this.samples[this.samples.length - 1]?.rate ?? 0;
		const since = ((this.downloaded - this.sampleBytes) * 1000) / Math.max(1, now - this.sampleAt);
		return Math.max(last, since) < threshold;
	}

	/** A socket carries data once one of its peers has unchoked us. */
	private carrying(sock: Sock): boolean {
		for (const live of this.lives.values()) if (live.sock === sock && !live.peer.peerChoking) return true;
		return false;
	}

	/**
	 * Sockets untried dials may use: those not carrying data, so a dead
	 * connect never delays a socket that serves us; when every socket
	 * carries data, the one with the fewest unchoked peers. None once enough
	 * peers deliver.
	 */
	private exploringSockets(now: number): Set<Sock> {
		if (!this.needMorePeers(now)) return new Set();
		const open = this.sockets.filter((s) => !s.client.closed);
		const idle = open.filter((s) => !this.carrying(s));
		if (idle.length > 0) return new Set(idle);
		let best: Sock | undefined;
		let fewest = Infinity;
		for (const s of open) {
			let n = 0;
			for (const live of this.lives.values()) if (live.sock === s && !live.peer.peerChoking) n++;
			if (n < fewest) {
				fewest = n;
				best = s;
			}
		}
		return new Set(best ? [best] : []);
	}

	/**
	 * Close and replace an exploring socket that only holds slots of dead
	 * connects (OQ-03: the platform keeps them ~20 s, even after close();
	 * a new WebSocket is a new invocation with its own 6). Only while more
	 * peers are needed, untried candidates wait and no other socket can try
	 * them; at most maxRecycles per session, one per recycleIntervalMs.
	 */
	private recycle(now: number): void {
		if (this.recycles >= this.opts.maxRecycles || this.opening || now - this.lastRecycleAt < this.opts.recycleIntervalMs) return;
		if (!this.needMorePeers(now)) return;
		const exploring = this.exploringSockets(now);
		for (const s of exploring) if (this.untestedLoad(s, now) < this.opts.maxUntestedDials && this.room(s) > 0) return;
		for (const sock of exploring) {
			if (sock.client.closed || sock.untested > 0) continue;
			if ([...this.lives.values()].some((l) => l.sock === sock)) continue;
			// Nothing but held slots on it: no open in flight, no connection.
			if (sock.client.connections - sock.client.heldSlots(now) > 0) continue;
			if (this.untestedLoad(sock, now) < this.opts.maxUntestedDials) continue;
			if (Math.min(...sock.untestedHeld) - now < RECYCLE_MIN_GAIN_MS) continue;
			if (!this.book.candidates(Infinity).some((p) => this.book.dialClass(p) === "untried")) return;
			this.recycles++;
			this.lastRecycleAt = now;
			this.retire(sock);
			void this.openSocket().catch(() => this.scheduleReconnect());
			return;
		}
	}

	/**
	 * Close the surplus sockets that have had no connection for
	 * IDLE_SOCKET_MS: fill() dials into any socket with room every sweep, so
	 * one left empty that long had nobody to dial. The session keeps one.
	 */
	private closeIdleSockets(now: number): void {
		for (const sock of [...this.sockets]) {
			if (sock.client.closed) continue;
			if (sock.client.connections > 0) {
				sock.emptySince = now;
				continue;
			}
			if (this.sockets.length <= 1 || this.opening || now - sock.emptySince < IDLE_SOCKET_MS) continue;
			this.retire(sock);
			this.wantedSockets = Math.max(1, Math.min(this.wantedSockets, this.sockets.length));
		}
	}

	/** Take a socket out of the session and close it, without the reconnect of an unexpected close. */
	private retire(sock: Sock): void {
		const i = this.sockets.indexOf(sock);
		if (i >= 0) this.sockets.splice(i, 1);
		this.sentBefore += sock.client.stats.messagesSent;
		this.lazyBefore += sock.client.stats.lazyMessages;
		sock.client.onClose = undefined;
		sock.client.close();
	}

	/** Untried dials in flight on `sock`, counting the slots their timed-out connects still hold. */
	private untestedLoad(sock: Sock, now = this.now()): number {
		if (sock.untestedHeld.length > 0) sock.untestedHeld = sock.untestedHeld.filter((until) => until > now);
		return sock.untested + sock.untestedHeld.length;
	}

	/** Candidates `sock` may not dial now: untried ones unless it explores and has its share free (OQ-03). */
	private notDialableOn(sock: Sock, now = this.now()): ((p: PeerEntry) => boolean) | undefined {
		if (this.exploringSockets(now).has(sock) && this.untestedLoad(sock, now) < this.opts.maxUntestedDials) return undefined;
		return (p) => this.book.dialClass(p) === "untried";
	}

	/**
	 * Open width early while nothing flows (OQ-23): until a peer delivers
	 * (metadata or the first block still missing), if the candidates
	 * outnumber what the open sockets can dial now, open the next WebSocket
	 * at once, up to maxSockets. Each costs one billed request.
	 */
	private widen(): void {
		if (!this.opts.earlyWidth || this.opening || this.stopped || this.settled || this.suspended) return;
		if (this.sockets.length === 0 || this.sockets.length >= this.opts.maxSockets) return;
		const wantsData = !this.info || this.downloading || this.recoveries.size > 0 || this.externalQueue.length > 0;
		if (!wantsData || this.downloaded > 0) return;
		if (!this.candidatesExceedSlots()) return;
		this.wantedSockets = Math.max(this.wantedSockets, this.sockets.length + 1);
		void this.openSocket().catch(() => this.scheduleReconnect());
	}

	/** More dialable candidates than the open sockets can take right now (alive and known anywhere, untried within their share). */
	private candidatesExceedSlots(): boolean {
		const now = this.now();
		const exploring = this.exploringSockets(now);
		let free = 0;
		let untriedFree = 0;
		for (const sock of this.sockets) {
			if (sock.client.closed) continue;
			const room = this.room(sock);
			free += room;
			if (exploring.has(sock)) untriedFree += Math.min(room, Math.max(0, this.opts.maxUntestedDials - this.untestedLoad(sock, now)));
		}
		let others = 0;
		let untried = 0;
		for (const p of this.book.candidates(Infinity)) {
			if (this.book.dialClass(p) === "untried") untried++;
			else others++;
		}
		if (others > free) return true;
		// Alive and known candidates take untried slots first only where they must: count what is left for untried ones.
		return untried > Math.min(untriedFree, free - others);
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
		const kind = this.book.dialClass(entry);
		const untested = kind === "untried";
		const counts = this.dialCounts[kind];
		const failed = (why: string) => (counts.failed[why] = (counts.failed[why] ?? 0) + 1);
		this.book.markConnecting(entry.key);
		if (untested) {
			sock.untested++;
			const load = this.untestedLoad(sock);
			if (load > this.untestedHigh) this.untestedHigh = load;
		}
		const address = this.book.addressForOpen(entry.key);
		if (!address || sock.client.closed) {
			if (untested) sock.untested--;
			this.book.markFailed(entry.key, "forbidden_address");
			return;
		}
		counts.attempted++;
		const startedAt = this.now();
		void sock.client
			.open({ address, infoHash: this.opts.infoHash, peerId: this.peerId, timeoutMs: 20_000 })
			.then(
				(transport) => {
					counts.succeeded++;
					if (this.stopped || sock.client.closed || this.lives.has(entry.key)) {
						transport.close();
						if (!this.lives.has(entry.key)) this.book.markClosed(entry.key);
						return;
					}
					this.attach(sock, entry.key, transport);
				},
				(e: unknown) => {
					if (sock.client.closed) {
						failed("closed");
						this.book.markClosed(entry.key);
					} else if (e instanceof TunnelOpenError && e.reason === FailReason.TOO_MANY_CONNECTIONS) {
						// No slot at the gateway: the peer was never dialled, so it keeps its standing.
						failed("busy");
						this.book.markNotDialled(entry.key);
					} else {
						const reason = e instanceof TunnelOpenError ? failReasonName(e.reason) : "timeout";
						failed(reason);
						// A connect that timed out keeps holding its slot on the platform (OQ-03): still "in flight".
						if (untested && reason === "timeout") sock.untestedHeld.push(startedAt + FAILED_CONNECT_HOLD_MS + CLOSE_SLOT_GRACE_MS);
						this.book.markFailed(entry.key, reason);
					}
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
			reqq: this.opts.upload?.maxQueuedRequests ?? UPLOAD_DEFAULTS.maxQueuedRequests,
			bitfield: this.uploader.bitfield(),
			metadataSize: this.uploader.metadataSize,
			events: {
				request: (piece, begin, length) => {
					if (this.lives.get(key) === live) this.uploader.onRequest(key, piece, begin, length);
				},
				cancelRequest: (piece, begin, length) => this.uploader.onCancel(key, piece, begin, length),
				metadataRequest: (piece) => this.uploader.onMetadataRequest(key, piece),
				have: () => this.pipeline(),
				extensions: () => this.opts.onExtensions?.(peer),
				metadata: (message) => this.opts.onMetadata?.(peer, message),
				unchoke: () => {
					if (live.unchokeMs === null) live.unchokeMs = this.now() - live.openedAt;
					if (this.firstUnchokeAt === 0) this.firstUnchokeAt = this.now();
					this.lastProgress = this.now();
					this.pipeline();
				},
				choke: () => {
					if (this.lives.get(key) !== live) return;
					// Without the fast extension the peer dropped every request
					// silently; with it, a reject comes for each one.
					if (!peer.fast) this.requeue(live);
				},
				block: (piece, begin, data) => {
					if (this.lives.get(key) !== live) return;
					this.onBlock(live, piece, begin, data.slice());
				},
				reject: (piece, begin, length) => {
					if (this.lives.get(key) !== live) return;
					live.failures++;
					if (!this.requeueBlock({ piece, begin, length }, live)) this.withdrawn([], this.picker?.onReject(key, piece, begin, this.now()) ?? [], false);
					this.pipeline();
				},
				pex: (message) => {
					this.pex.ingest(message);
					// Named by a peer that delivered: likely a live swarm member.
					if (live.downloaded > 0) for (const a of message.added.slice(0, 100)) this.book.vouch(formatAddress({ family: a.family, ip: a.ip, port: a.port }));
					this.fill();
				},
				hashes: (request, hashes) => void this.hashes?.onHashes(peer, request, hashes),
				hashReject: (request) => this.hashes?.onReject(peer, request),
				close: () => {
					if (this.lives.get(key) !== live) return;
					this.lives.delete(key);
					this.uploader.remove(key);
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
		this.uploader.add(key, peer, live.openedAt);
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
			// A candidate this socket could dial once a slot frees.
			if (this.book.candidates(1, this.notDialableOn(sock, now)).length === 0) continue;
			const views = [...this.lives.values()].filter((l) => l.sock === sock).map((l) => this.view(l, now));
			const victim = replacementVictim(views, now, this.opts.chokeReplaceAfterMs);
			if (!victim) continue;
			const live = this.lives.get(victim.key);
			if (!live || this.isDelivering(live, now)) continue;
			// Back off, or the peer we just dropped is the next candidate (it connected before).
			this.drop(live, victim.useless ? "useless" : live.peer.peerChoking ? "choked" : "slow");
			return;
		}
	}

	/** It has none of the pieces still wanted (false while that is not known, or nothing is wanted). */
	private isUseless(live: Live, now: number): boolean {
		if (!this.picker || now - live.openedAt < USELESS_GRACE_MS) return false;
		return this.picker.servesWanted((p) => live.peer.has(p)) === false;
	}

	private view(live: Live, now: number): SlotView {
		const delivering = this.isDelivering(live, now);
		const facts = this.facts(live);
		return {
			key: live.key,
			delivering,
			choked: live.peer.peerChoking,
			useless: !delivering && this.isUseless(live, now),
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
		if (this.isDelivering(live) && reason !== "corrupt") this.deliveringClosed++;
		this.lives.delete(live.key);
		this.uploader.remove(live.key);
		this.requeue(live);
		if (reason) this.book.markFailed(live.key, reason);
		else this.book.markClosed(live.key);
		live.peer.close();
	}

	private pipeline(): void {
		if (!this.geometry) return;
		if (this.externalQueue.length > 0) this.pipelineExternal();
		this.pipelinePicker();
	}

	/** Blocks of requestBlocks calls, first come first served. */
	private pipelineExternal(): void {
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
			const queue = this.externalQueue;
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
			for (const block of batch) this.assigned.set(blockKey(block), { block, live });
			live.peer.request(batch.map((b) => [b.piece, b.begin, b.length]));
			this.noteInflight(live.sock);
		}
	}

	/**
	 * The picker's blocks. A socket whose peers get requests sends a message
	 * anyway, so its other peers are topped up in the same message (a second
	 * pass with `eager`): fewer, fuller upstream messages.
	 */
	private pipelinePicker(): void {
		const picker = this.picker;
		if (!picker || picker.targetCount === 0 || this.lives.size === 0) return;
		const views: PickerPeer[] = [];
		const byKey = new Map<string, Live>();
		const remaining = new Map<Sock, number>();
		for (const live of this.lives.values()) {
			if (live.peer.closed) continue;
			byKey.set(live.key, live);
			views.push({ id: live.key, has: (p) => live.peer.has(p), rate: live.rate, reqq: live.peer.reqq, unchoked: !live.peer.peerChoking });
			if (!remaining.has(live.sock)) remaining.set(live.sock, Math.max(0, this.opts.inflightLimit - this.socketInflight(live.sock)));
		}
		const budget = (id: string) => {
			const live = byKey.get(id)!;
			const byQueue = Math.max(0, live.peer.reqq - live.peer.outstandingRequests) * BLOCK_SIZE;
			return Math.min(remaining.get(live.sock) ?? 0, byQueue);
		};
		const spent = (id: string, bytes: number) => {
			const sock = byKey.get(id)!.sock;
			remaining.set(sock, (remaining.get(sock) ?? 0) - bytes);
		};
		const now = this.now();
		const first = picker.plan(views, now, { budget, spent });
		const all = new Map<string, PickedBlock[]>(first);
		if (first.size > 0) {
			const touched = new Set([...first.keys()].map((id) => byKey.get(id)!.sock));
			const second = picker.plan(views, now, { budget, spent, eager: (id) => touched.has(byKey.get(id)!.sock) });
			for (const [id, blocks] of second) all.set(id, [...(all.get(id) ?? []), ...blocks]);
		}
		for (const [id, blocks] of all) {
			const live = byKey.get(id)!;
			live.peer.request(blocks.map((b) => [b.piece, b.begin, b.length]));
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
		const key = `${piece}:${begin}`;
		const external = this.assigned.get(key);
		const now = this.now();
		const ours = external?.live === live;
		const fromPicker = !ours && this.picker?.isRequested(live.key, piece, begin) === true;
		if (!ours && !fromPicker) return;
		this.lastProgress = now;
		live.downloaded += data.length;
		live.lastBlockAt = now;
		live.failures = 0;
		this.downloaded += data.length;
		this.book.markDelivered(live.key, data.length);
		if (ours) {
			this.assigned.delete(key);
			const handlers = this.externalHandlers.get(key);
			this.externalHandlers.delete(key);
			if (!handlers || handlers.size === 0) this.keepOrphan(key, data);
			for (const handler of handlers ?? []) handler(data);
			this.pipeline();
			return;
		}
		const r = this.picker!.onBlock(live.key, piece, begin, data.length);
		// The other copy of a duplicated block is no longer needed: its cancel rides the next message.
		this.withdrawn(r.cancels, [], false);
		if (r.fresh) {
			this.attribution.record(piece, begin / BLOCK_SIZE, this.picker!.blocks(piece), live.key);
			this.store(piece, begin, data);
		}
		this.pipeline();
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
		// Who sent each block of this attempt (the piece's next attempt starts afresh).
		const sources = this.attribution.take(piece);
		let ok: boolean;
		try {
			ok = (await this.verifier?.verify(piece, work.buffer)) === true;
		} catch {
			ok = false;
		}
		this.verifying--;
		if (this.settled || this.work.get(piece) !== work) return;
		this.work.delete(piece);
		const picker = this.picker!;
		if (ok) {
			this.verified.add(piece);
			this.withdrawn(picker.markVerified(piece), [], false);
			if (this.attribution.suspected(piece)) void this.settleSuspects(piece, work.buffer);
			for (const rec of [...this.recoveries.values()]) if (covers(rec, piece)) rec.onPiece?.(piece, work.buffer);
			// Stored by the host (onPiece)? Then peers that lack it hear about it.
			this.uploader.offer(piece);
		} else {
			this.hashFailureCount++;
			const verdict = this.attribution.failed(piece, sources);
			// A failure that got its only sender banned is not the piece's fault.
			const attempts = verdict.banned.length > 0 ? work.attempts : work.attempts + 1;
			const retries = Math.max(0, ...[...this.recoveries.values()].filter((r) => covers(r, piece)).map((r) => r.retries));
			if (attempts > retries) {
				this.corrupt.add(piece);
				picker.abandon(piece);
			} else {
				picker.reset(piece);
				if (verdict.isolate || picker.isolation(piece)) picker.isolate(piece, verdict.culprits);
				const again: PieceWork = { buffer: new Uint8Array(work.buffer.length), have: new Uint8Array(work.have.length), got: 0, attempts };
				this.work.set(piece, again);
			}
			if (verdict.attempt !== undefined) void this.investigate(piece, work.buffer, sources, verdict.attempt);
			this.applyVerdict(verdict);
		}
		this.checkRecoveries();
		this.pipeline();
	}

	/**
	 * A mixed piece failed: keep its blocks' digests for the comparison with
	 * the verified data, and (v2) ask a peer for its leaf hashes, which name
	 * the bad blocks at once.
	 */
	private async investigate(piece: number, data: Uint8Array, sources: (string | undefined)[], attempt: number): Promise<void> {
		try {
			this.attribution.setDigests(piece, attempt, await blockDigests(data));
		} catch {
			// No digests: the attempt is skipped when the piece verifies.
		}
		const verifier = this.verifier;
		const request = verifier instanceof V2Verifier ? verifier.leafRequest(piece) : undefined;
		if (!request || !this.hashes || !(verifier instanceof V2Verifier)) return;
		let bad: number[] | undefined;
		await this.hashes.fetch(request, async (hashes) => {
			bad = await verifier.badBlocks(piece, data, hashes);
			return bad !== undefined;
		});
		if (this.settled || !bad) return;
		const verdict = this.attribution.blame(piece, sources, bad, attempt);
		// Culprits known: the piece no longer needs a single source.
		if (!this.verified.has(piece) && this.picker?.isolation(piece)?.owner === undefined) this.picker?.unisolate(piece);
		this.applyVerdict(verdict);
		this.pipeline();
	}

	/** A piece that failed mixed verified: the blocks that differed name their senders. */
	private async settleSuspects(piece: number, data: Uint8Array): Promise<void> {
		let digests: Uint8Array[];
		try {
			digests = await blockDigests(data);
		} catch {
			return;
		}
		if (this.settled) return;
		this.applyVerdict(this.attribution.passed(piece, digests));
	}

	/**
	 * Banned peers: connection closed, address banned in the PeerBook (never
	 * dialled again this session), and the unverified blocks they sent to
	 * other pieces thrown away.
	 */
	private applyVerdict(verdict: Verdict): void {
		if (verdict.banned.length === 0 || this.settled) return;
		const picker = this.picker;
		for (const key of verdict.banned) {
			this.book.ban(key, "corrupt");
			const live = this.lives.get(key);
			if (live) this.drop(live, "corrupt");
			this.withdrawn([], picker?.removePeer(key) ?? [], false);
			for (const piece of this.attribution.piecesFrom(key)) {
				const work = this.work.get(piece);
				// Complete pieces are being verified: their outcome decides.
				if (!work || work.got >= work.buffer.length) continue;
				for (const block of this.attribution.forget(piece, key)) {
					if (!work.have[block] || !picker?.unreceive(piece, block)) continue;
					work.have[block] = 0;
					work.got -= Math.min(BLOCK_SIZE, work.buffer.length - block * BLOCK_SIZE);
				}
			}
			this.opts.onBan?.();
		}
		this.pipeline();
	}

	/** A peer's requests are void (it left, or choked without the fast extension). */
	private requeue(live: Live): void {
		this.withdrawn([], this.picker?.releasePeer(live.key) ?? [], false);
		const external: Block[] = [];
		for (const [k, a] of this.assigned) {
			if (a.live !== live) continue;
			this.assigned.delete(k);
			// An orphan nobody waits for any more is not asked again.
			if (!this.externalHandlers.get(k)?.size) continue;
			external.push(a.block);
		}
		if (external.length > 0) this.externalQueue.unshift(...external);
	}

	/** A requestBlocks block was rejected: queue it again. False if it was not one. */
	private requeueBlock(block: Block, live: Live): boolean {
		const k = blockKey(block);
		const a = this.assigned.get(k);
		if (!a || a.live !== live) return false;
		this.assigned.delete(k);
		if (this.externalHandlers.get(k)?.size) this.externalQueue.unshift(block);
		return true;
	}

	/** Settle the recoveries whose every piece is verified (or given up). */
	private checkRecoveries(): void {
		const picker = this.picker;
		if (!picker) return;
		for (const rec of [...this.recoveries.values()]) if (picker.targetDone(rec.id)) rec.settle(true);
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
		const wanted = new Set(this.wanted);
		const missing = this.wanted.filter((p) => !this.verified.has(p) && !this.corrupt.has(p));
		const result: SessionDownloadResult = {
			verified: [...this.verified].filter((p) => wanted.has(p)).sort((a, b) => a - b),
			corrupt: [...this.corrupt].filter((p) => wanted.has(p)).sort((a, b) => a - b),
			missing,
			bytes: this.downloaded,
			messagesSent: this.messagesSent,
			inflightHighWater: this.highWater,
			sockets: this.peakSockets,
			hedges: this.picker?.hedgesSent ?? 0,
			duplicates: this.picker?.duplicatesDiscarded ?? 0,
			hashFailures: this.hashFailureCount,
			banned: this.attribution.banned,
		};
		this.cleanup();
		this.resolveDownload?.(result);
	}

	private cleanup(): void {
		for (const rec of [...this.recoveries.values()]) rec.settle(false);
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
		// Nobody is unchoked any more, nothing queued is served.
		this.uploader.reset();
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
			this.lazyBefore += sock.client.stats.lazyMessages;
			sock.client.onClose = undefined;
			sock.client.close();
		}
	}
}

function covers(rec: RecoveryState, piece: number): boolean {
	return piece >= rec.first && piece <= rec.last && (!rec.only || rec.only.has(piece));
}

/**
 * How many requests a peer should have queued (requestBlocks path): QUEUE_SECONDS of its rate
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
