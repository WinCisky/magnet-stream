// Deadline block picker (docs/architecture.md §Chunk recovery › Piece
// picking). Requests are assigned per 16 KiB block, in five levels:
//
//   1. Container index: the head and the tail of each recovered file
//      (MP4 `moov`, Matroska cues).
//   2. Playback window: the next `windowBytes` from the playback position
//      (30 s of video by default), strictly in order, from the fastest
//      peers only. A block that is late is asked of a second peer, and the
//      copy that arrives second is discarded (its request is cancelled).
//   3. Read-ahead: sequential past the window, on every unchoked peer, each
//      kept `readAheadSeconds` (4 s) of its own rate deep, within its `reqq`;
//      the fastest peers at least `minQueueBytes` deep (see below).
//      It runs up to `scarceAheadBytes` (120 s of video) past the position.
//   4. Scarce pieces: once everything up to there is verified (the buffer is
//      comfortably ahead), pieces that only one or two connected peers hold,
//      rarest first. They may not be there later.
//   5. Back-fill: the rest of the read-ahead, then the gaps before the
//      playback position, last.
//
// A seek (`seek`) cancels the requests outside the new window and index;
// connections are never closed here. Ported from the page scheduler of the
// proof of concept (apps/web/src/lib/torrent/scheduler.ts): its urgent
// region is levels 1–2, its hedges are the late-block duplicates, and its
// steal is the same rule applied when a slow peer's read-ahead block enters
// the window.
//
// Isolation (M3-02, attribution.ts): a piece that failed its check with
// blocks from several peers is fetched again from a single peer (`isolate`):
// the first peer given one of its blocks owns all of them, no other peer
// is asked (no duplicates, no endgame), and peers proven to have corrupted
// it are passed over while another holder is connected. If the owner
// chokes, rejects or leaves, the blocks it sent are dropped (the caller gets
// the pieces back) and another peer starts over.
//
// Hard limits, whatever peers do: at most two requests per block (one
// duplicate), at most `maxHedgedBlocks` duplicated blocks at once, a peer's
// queue never above min(`reqq`, its time depth), and the caller's byte
// budget (the 16 MiB per socket cap) per call. Pure and deterministic: no
// timers, no I/O, the clock is passed in.

import { BLOCK_SIZE } from "./metainfo.ts";
import { pieceSizeOf, type Geometry } from "./peer-wire.ts";

export const PickLevel = { Index: 1, Window: 2, ReadAhead: 3, Scarce: 4, BackFill: 5 } as const;
export type PickLevel = (typeof PickLevel)[keyof typeof PickLevel];

/**
 * Bytes per second assumed for the video until the caller knows its bitrate
 * (M3-06): 5 Mbit/s, a typical 1080p stream.
 */
export const DEFAULT_BITRATE = 625_000;

export interface PickerConfig {
	/** Video bytes per second: turns the time thresholds into bytes. */
	bitrate: number;
	/** Playback window, in seconds of video (default 30). */
	windowSeconds: number;
	/** Playback window in bytes; overrides windowSeconds × bitrate. */
	windowBytes?: number;
	/** Requests queued per peer, in seconds of that peer's rate (default 4). */
	readAheadSeconds: number;
	/** Verified video ahead of the position before scarce pieces are fetched (default 120 s). */
	scarceAheadSeconds: number;
	/** In bytes; overrides scarceAheadSeconds × bitrate. */
	scarceAheadBytes?: number;
	/** A piece is scarce when at most this many connected peers hold it (default 2). */
	scarceHolders: number;
	/** Container index: bytes at the head and at the tail of a file (default 2 MiB each). */
	headBytes: number;
	tailBytes: number;
	/** Fast peers: rate at least this share of the best rate (default 0.5). */
	fastRatio: number;
	/** A request is late this long after the time its peer's rate says it is due (default 1.5 s). */
	hedgeGraceMs: number;
	/** When a request is due if its peer's rate is unknown (default 4 s). */
	unknownRateDueMs: number;
	/** Blocks requested twice at once, at most (default 64). */
	maxHedgedBlocks: number;
	/** Queue depth in blocks before a peer's rate is known (default 16). */
	initialDepth: number;
	/** Least queue depth of a peer that has a rate (default 2). */
	minDepth: number;
	/** A peer's queue is refilled once at most this share of it is left (default 0.5). */
	refillAt: number;
	/**
	 * Least queue of a fast peer (rate at least `fastRatio` of the best),
	 * in bytes, within its `reqq` and `maxQueueSeconds` of its rate
	 * (default 4 MiB). Each refill is an upstream message (billed): on a
	 * slow swarm, `readAheadSeconds` of a 70 KB/s peer is a message every
	 * 2 s, about 440 billed requests per GB. With 4 MiB a refill asks for
	 * 2 MiB, so refills stay under ~25 per GB (budget 40,
	 * docs/measurements.md). Slower peers keep the time depth. 0 disables.
	 */
	minQueueBytes: number;
	/** The `minQueueBytes` floor never exceeds this many seconds of the peer's rate (default 60). */
	maxQueueSeconds: number;
	/** A piece a peer rejected is not asked of it again for this long (default 30 s). */
	refusedMs: number;
}

export const DEFAULT_PICKER_CONFIG: Readonly<PickerConfig> = {
	bitrate: DEFAULT_BITRATE,
	windowSeconds: 30,
	readAheadSeconds: 4,
	scarceAheadSeconds: 120,
	scarceHolders: 2,
	headBytes: 2 * 1024 * 1024,
	tailBytes: 2 * 1024 * 1024,
	fastRatio: 0.5,
	hedgeGraceMs: 1_500,
	unknownRateDueMs: 4_000,
	maxHedgedBlocks: 64,
	initialDepth: 16,
	minDepth: 2,
	refillAt: 0.5,
	refusedMs: 30_000,
	minQueueBytes: 4 * 1024 * 1024,
	maxQueueSeconds: 60,
};

/** Hard cap on requests per block: the original and one duplicate. */
export const MAX_REQUESTS_PER_BLOCK = 2;
/** Pieces a peer refused that are remembered for it. */
const MAX_REFUSED = 4096;
/** Availability counts are reused for this long. */
const AVAILABILITY_MS = 1_000;

/** What the picker needs to know about a connected peer. */
export interface PickerPeer {
	id: string;
	has(piece: number): boolean;
	/** Bytes/second it delivers to us (0: not known yet). */
	rate: number;
	/** Queue depth it accepts. */
	reqq: number;
	/** It unchoked us. */
	unchoked: boolean;
}

export interface PickedBlock {
	piece: number;
	begin: number;
	length: number;
	level: PickLevel;
	/** A second request for a late block. */
	hedge: boolean;
}

/** A request to withdraw with a `cancel` message. */
export interface BlockCancel {
	peer: string;
	piece: number;
	begin: number;
	length: number;
}

export interface TargetOptions {
	/** Playback position, in bytes from the target's start (default 0). */
	position?: number;
	/** This target's window in bytes (default: the picker's). */
	windowBytes?: number;
	/** Only these pieces of the range are wanted (default: all). */
	pieces?: Iterable<number>;
}

export interface PlanOptions {
	/** Bytes this peer may still be asked for (the socket's in-flight cap). */
	budget?(peer: string): number;
	/** Bytes just assigned to this peer. */
	spent?(peer: string, bytes: number): void;
	/**
	 * Top this peer's queue up even if it is not half drained: its socket
	 * sends a message anyway, so more requests ride along for free.
	 */
	eager?(peer: string): boolean;
}

export interface BlockResult {
	/** First copy of a block still wanted: keep it. Otherwise discard it. */
	fresh: boolean;
	/** Every block of the piece is in: verify it. */
	complete: boolean;
	/** Duplicate requests of this block to withdraw. */
	cancels: BlockCancel[];
}

export interface SeekResult {
	cancels: BlockCancel[];
	/** Partly received pieces left behind: their bytes can be dropped. */
	dropped: number[];
}

interface Target {
	id: number;
	/** Torrent byte range [start, end). */
	start: number;
	end: number;
	first: number;
	last: number;
	/** Torrent byte of the playback position. */
	position: number;
	windowBytes: number | undefined;
	headEnd: number;
	tailStart: number;
	only: Set<number> | undefined;
}

interface Zones {
	posPiece: number;
	winLast: number;
	horizonLast: number;
	/** Everything from the position to the horizon is verified. */
	ahead: boolean;
	scarce: number[];
}

interface Req {
	peer: string;
	due: number;
}

interface Context {
	now: number;
	byId: Map<string, PickerPeer>;
	fast: PickerPeer[];
	fastIds: Set<string>;
	zones: Map<number, Zones>;
	endgame: boolean;
}

const OPEN = 0;
const COMPLETE = 1;
const VERIFIED = 2;
/** Kept failing verification: no longer requested. */
const ABANDONED = 3;

export class Picker {
	readonly config: PickerConfig;
	private readonly g: Geometry;
	private readonly bpp: number;
	private readonly ready: (piece: number) => boolean;
	private readonly state: Uint8Array;
	/** Blocks neither received nor requested, per piece. */
	private readonly free: Int32Array;
	private readonly got: Int32Array;
	/** Blocks per piece. */
	private readonly counts: Int32Array;
	private readonly received = new Map<number, Uint8Array>();
	private readonly reqs = new Map<number, Req[]>();
	private readonly byPeer = new Map<string, Map<number, number>>();
	private readonly refused = new Map<string, Map<number, number>>();
	private readonly targets = new Map<number, Target>();
	private nextTarget = 1;
	private hedged = 0;
	private availability: { at: number; peers: number; counts: Uint16Array } | undefined;
	/** Pieces fetched from a single peer (isolating who corrupted a mixed piece). */
	private readonly exclusive = new Map<number, { owner: string | undefined; avoid: Set<string> }>();
	/** Duplicate requests sent, and duplicate copies discarded. */
	hedgesSent = 0;
	duplicatesDiscarded = 0;

	constructor(geometry: Geometry, config: Partial<PickerConfig> = {}, ready: (piece: number) => boolean = () => true) {
		this.g = geometry;
		this.config = { ...DEFAULT_PICKER_CONFIG, ...config };
		this.bpp = Math.max(1, Math.ceil(geometry.pieceLength / BLOCK_SIZE));
		this.ready = ready;
		this.state = new Uint8Array(geometry.pieceCount);
		this.free = new Int32Array(geometry.pieceCount);
		this.got = new Int32Array(geometry.pieceCount);
		this.counts = new Int32Array(geometry.pieceCount);
		for (let p = 0; p < geometry.pieceCount; p++) {
			this.counts[p] = Math.ceil(pieceSizeOf(geometry, p) / BLOCK_SIZE);
			this.free[p] = this.counts[p];
			// An empty piece (v2: none past a file's end) has nothing to fetch.
			if (this.counts[p] === 0) this.state[p] = VERIFIED;
		}
	}

	// --- targets -----------------------------------------------------------

	/** Recover the torrent bytes [offset, offset + size). Returns the target id. */
	addTarget(range: { offset: number; size: number }, opts: TargetOptions = {}): number {
		const total = this.g.totalLength;
		const start = clamp(Math.floor(range.offset), 0, total);
		const end = clamp(Math.floor(range.offset + Math.max(range.size, 0)), start, total);
		const pl = this.g.pieceLength;
		const first = Math.min(this.g.pieceCount - 1, Math.floor(start / pl));
		const last = Math.max(first, Math.min(this.g.pieceCount - 1, Math.floor((Math.max(end, start + 1) - 1) / pl)));
		const headEnd = Math.min(last, Math.floor((start + Math.max(1, this.config.headBytes) - 1) / pl));
		const tailStart = Math.max(first, Math.floor(Math.max(start, end - Math.max(1, this.config.tailBytes)) / pl));
		const only = opts.pieces ? new Set([...opts.pieces].filter((p) => Number.isInteger(p) && p >= first && p <= last)) : undefined;
		const id = this.nextTarget++;
		this.targets.set(id, {
			id,
			start,
			end,
			first,
			last,
			position: clamp(start + Math.floor(opts.position ?? 0), start, Math.max(start, end - 1)),
			windowBytes: positive(opts.windowBytes),
			headEnd,
			tailStart,
			only,
		});
		return id;
	}

	/** Stop recovering a target: requests nobody else wants are withdrawn. */
	removeTarget(id: number): SeekResult {
		if (!this.targets.delete(id)) return { cancels: [], dropped: [] };
		return this.withdraw((p) => this.wanted(p));
	}

	get targetCount(): number {
		return this.targets.size;
	}

	/** Pieces of a target, first and last. */
	pieceRange(id: number): { first: number; last: number } | undefined {
		const t = this.targets.get(id);
		return t ? { first: t.first, last: t.last } : undefined;
	}

	/** Whether every wanted piece of a target is verified. */
	targetDone(id: number): boolean {
		const t = this.targets.get(id);
		if (!t) return true;
		for (let p = t.first; p <= t.last; p++) if (this.inTarget(t, p) && (this.state[p] === OPEN || this.state[p] === COMPLETE)) return false;
		return true;
	}

	/** Playback moved on (no cancels): the window follows. */
	setPosition(id: number, byte: number): void {
		const t = this.targets.get(id);
		if (t) t.position = clamp(t.start + Math.floor(byte), t.start, Math.max(t.start, t.end - 1));
	}

	/**
	 * The viewer jumped to `byte` (from the target's start): requests outside
	 * the new window and the container index are withdrawn, and pieces they
	 * leave partly received are dropped.
	 */
	seek(id: number, byte: number): SeekResult {
		const t = this.targets.get(id);
		if (!t) return { cancels: [], dropped: [] };
		this.setPosition(id, byte);
		const keep = new Set<number>();
		for (const target of this.targets.values()) {
			const z = this.zonesOf(target, false);
			for (let p = target.first; p <= target.headEnd; p++) keep.add(p);
			for (let p = target.tailStart; p <= target.last; p++) keep.add(p);
			for (let p = z.posPiece; p <= z.winLast; p++) keep.add(p);
		}
		return this.withdraw((p) => keep.has(p));
	}

	position(id: number): number | undefined {
		const t = this.targets.get(id);
		return t ? t.position - t.start : undefined;
	}

	/** Window of one target (undefined: every target), in bytes. */
	setWindowBytes(bytes: number | undefined, id?: number): void {
		if (id === undefined) this.config.windowBytes = positive(bytes);
		else {
			const t = this.targets.get(id);
			if (t) t.windowBytes = positive(bytes);
		}
	}

	/** Video bytes per second (M3-06): window and scarce threshold follow, unless set in bytes. */
	setBitrate(bytesPerSecond: number): void {
		if (bytesPerSecond > 0 && Number.isFinite(bytesPerSecond)) this.config.bitrate = bytesPerSecond;
	}

	windowBytesOf(id: number): number {
		const t = this.targets.get(id);
		return t?.windowBytes ?? this.config.windowBytes ?? Math.round(this.config.windowSeconds * this.config.bitrate);
	}

	get scarceAheadBytes(): number {
		return this.config.scarceAheadBytes ?? Math.round(this.config.scarceAheadSeconds * this.config.bitrate);
	}

	// --- piece state -------------------------------------------------------

	isVerified(piece: number): boolean {
		return this.state[piece] === VERIFIED;
	}

	/**
	 * Whether a peer holding `has` could serve a piece some target still
	 * needs (not yet complete, verified or given up). Undefined when no
	 * target needs anything: then no peer is useless.
	 */
	servesWanted(has: (piece: number) => boolean): boolean | undefined {
		let needed = false;
		for (const t of this.targets.values()) {
			for (let p = t.first; p <= t.last; p++) {
				if (this.state[p] !== OPEN || !this.inTarget(t, p)) continue;
				needed = true;
				if (has(p)) return true;
			}
		}
		return needed ? false : undefined;
	}

	/** A piece passed verification (or was already stored). Its requests are withdrawn. */
	markVerified(piece: number): BlockCancel[] {
		if (!this.valid(piece) || this.state[piece] === VERIFIED) return [];
		const cancels = this.dropRequests(piece);
		this.exclusive.delete(piece);
		this.state[piece] = VERIFIED;
		this.received.delete(piece);
		this.got[piece] = 0;
		this.free[piece] = 0;
		return cancels;
	}

	/** Stop asking for a piece (it kept failing verification). */
	abandon(piece: number): void {
		if (!this.valid(piece) || this.state[piece] === VERIFIED) return;
		this.dropRequests(piece);
		this.exclusive.delete(piece);
		this.state[piece] = ABANDONED;
		this.received.delete(piece);
		this.got[piece] = 0;
		this.free[piece] = 0;
	}

	/** A piece failed verification, or a stored piece was lost: fetch it again. */
	reset(piece: number): void {
		if (!this.valid(piece)) return;
		if (this.counts[piece] === 0) return;
		this.dropRequests(piece);
		const ex = this.exclusive.get(piece);
		if (ex) ex.owner = undefined;
		this.state[piece] = OPEN;
		this.received.delete(piece);
		this.got[piece] = 0;
		this.free[piece] = this.blocks(piece);
	}

	/**
	 * Fetch `piece` from one peer only, and not from `avoid` (peers proven to
	 * have corrupted it) while another holder is connected. Call it after
	 * `reset`, before new requests; again to add peers to avoid.
	 */
	isolate(piece: number, avoid: Iterable<string> = []): void {
		if (!this.valid(piece) || this.state[piece] === VERIFIED || this.state[piece] === ABANDONED) return;
		let ex = this.exclusive.get(piece);
		if (!ex) this.exclusive.set(piece, (ex = { owner: undefined, avoid: new Set() }));
		for (const peer of avoid) ex.avoid.add(peer);
	}

	/** Any peer may serve `piece` again (its culprit was found another way). */
	unisolate(piece: number): void {
		this.exclusive.delete(piece);
	}

	/** Whether `piece` is fetched from a single peer, and which one (undefined: not chosen yet). */
	isolation(piece: number): { owner: string | undefined } | undefined {
		const ex = this.exclusive.get(piece);
		return ex ? { owner: ex.owner } : undefined;
	}

	/** Throw away a received block of a piece being assembled (its sender was banned): it is wanted again. */
	unreceive(piece: number, block: number): boolean {
		if (!this.valid(piece) || this.state[piece] !== OPEN) return false;
		const rec = this.received.get(piece);
		if (!rec?.[block]) return false;
		rec[block] = 0;
		this.got[piece]--;
		if (!this.reqs.has(this.key(piece, block))) this.free[piece]++;
		return true;
	}

	/** Blocks received of a piece being assembled (0 for verified pieces). */
	receivedBlocks(piece: number): number {
		return this.valid(piece) ? this.got[piece] : 0;
	}

	blocks(piece: number): number {
		return this.counts[piece] ?? 0;
	}

	// --- requests ----------------------------------------------------------

	/** Requests a peer has outstanding from this picker. */
	outstanding(peer: string): number {
		return this.byPeer.get(peer)?.size ?? 0;
	}

	/** Blocks requested from more than one peer right now. */
	get hedgedBlocks(): number {
		return this.hedged;
	}

	/** Whether this block is asked of this peer. */
	isRequested(peer: string, piece: number, begin: number): boolean {
		return this.byPeer.get(peer)?.has(this.key(piece, begin / BLOCK_SIZE)) === true;
	}

	/**
	 * Queue depth of a peer, in blocks: `readAheadSeconds` of its rate, at
	 * least `minQueueBytes` if it is among the fast peers, within its `reqq`.
	 */
	depth(peer: Pick<PickerPeer, "rate" | "reqq">, fast = false): number {
		const cap = Math.max(1, Math.floor(peer.reqq));
		if (!(peer.rate > 0)) return Math.min(this.config.initialDepth, cap);
		const byTime = Math.floor((peer.rate * this.config.readAheadSeconds) / BLOCK_SIZE);
		const floorBytes = Math.min(Math.max(0, this.config.minQueueBytes), peer.rate * Math.max(0, this.config.maxQueueSeconds));
		const floor = fast ? Math.floor(floorBytes / BLOCK_SIZE) : 0;
		return Math.min(cap, Math.max(Math.min(this.config.minDepth, cap), byTime, floor));
	}

	/**
	 * Assign blocks to the unchoked peers, fastest first. Returns, per peer,
	 * the blocks to request now (in order). A peer is topped up to its depth
	 * once half its queue has drained (or when `eager`): upstream messages
	 * are billed, so each one asks for seconds of data. Late index and
	 * window blocks are duplicated at any time.
	 */
	plan(peers: readonly PickerPeer[], now: number, opts: PlanOptions = {}): Map<string, PickedBlock[]> {
		const out = new Map<string, PickedBlock[]>();
		if (this.targets.size === 0) return out;
		const ctx = this.context(peers, now);
		const order = peers.filter((p) => p.unchoked).sort((a, b) => b.rate - a.rate || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
		for (const peer of order) {
			const depth = this.depth(peer, ctx.fastIds.has(peer.id));
			const queued = this.outstanding(peer.id);
			let room = depth - queued;
			const budget = opts.budget?.(peer.id);
			if (budget !== undefined) room = Math.min(room, Math.floor(Math.max(0, budget) / BLOCK_SIZE));
			if (room <= 0) continue;
			const refill = queued <= Math.floor(depth * this.config.refillAt) || opts.eager?.(peer.id) === true;
			const picked = this.pickFor(peer, ctx, room, !refill);
			if (picked.length === 0) continue;
			out.set(peer.id, picked);
			opts.spent?.(
				peer.id,
				picked.reduce((n, b) => n + b.length, 0),
			);
		}
		return out;
	}

	/** A block arrived from `peer`. */
	onBlock(peer: string, piece: number, begin: number, length: number): BlockResult {
		const none: BlockResult = { fresh: false, complete: false, cancels: [] };
		if (!this.valid(piece) || begin % BLOCK_SIZE !== 0) return none;
		const block = begin / BLOCK_SIZE;
		if (block >= this.blocks(piece) || length !== this.blockLength(piece, block)) return none;
		const key = this.key(piece, block);
		const rec = this.received.get(piece);
		if (this.state[piece] !== OPEN || rec?.[block]) {
			this.unlink(key, peer);
			this.duplicatesDiscarded++;
			return none;
		}
		const reqs = this.reqs.get(key) ?? [];
		const cancels: BlockCancel[] = [];
		for (const r of reqs) if (r.peer !== peer) cancels.push({ peer: r.peer, piece, begin, length });
		if (reqs.length === 0) this.free[piece]--;
		this.unlinkAll(key);
		let bits = rec;
		if (!bits) this.received.set(piece, (bits = new Uint8Array(this.blocks(piece))));
		bits[block] = 1;
		this.got[piece]++;
		const complete = this.got[piece] >= this.blocks(piece);
		if (complete) this.state[piece] = COMPLETE;
		return { fresh: true, complete, cancels };
	}

	/**
	 * The peer rejected a request (BEP 6): ask someone else, and not it for a
	 * while. Returns the pieces dropped (an isolated piece it owned).
	 */
	onReject(peer: string, piece: number, begin: number, now: number): number[] {
		if (!this.valid(piece) || begin % BLOCK_SIZE !== 0) return [];
		this.unlink(this.key(piece, begin / BLOCK_SIZE), peer);
		const dropped = this.exclusive.get(piece)?.owner === peer ? this.disown(peer, piece) : [];
		let refused = this.refused.get(peer);
		if (!refused) this.refused.set(peer, (refused = new Map()));
		refused.delete(piece);
		refused.set(piece, now + this.config.refusedMs);
		if (refused.size > MAX_REFUSED) refused.delete(refused.keys().next().value!);
		return dropped;
	}

	/**
	 * Every request of a peer is void (it choked without the fast extension,
	 * or left). Returns the pieces dropped: isolated pieces it owned, whose
	 * blocks are thrown away so another single peer starts over.
	 */
	releasePeer(peer: string): number[] {
		const mine = this.byPeer.get(peer);
		if (mine) for (const key of [...mine.keys()]) this.unlink(key, peer);
		return this.disown(peer);
	}

	/** A peer left for good. Returns the pieces dropped (see releasePeer). */
	removePeer(peer: string): number[] {
		const dropped = this.releasePeer(peer);
		this.byPeer.delete(peer);
		this.refused.delete(peer);
		return dropped;
	}

	/** `peer` no longer owns its isolated pieces (or just `only`); those partly received are reset. */
	private disown(peer: string, only?: number): number[] {
		const dropped: number[] = [];
		for (const [p, ex] of this.exclusive) {
			if (ex.owner !== peer || (only !== undefined && p !== only)) continue;
			ex.owner = undefined;
			if (this.state[p] !== OPEN) continue;
			if (this.got[p] > 0) dropped.push(p);
			this.reset(p);
		}
		return dropped;
	}

	/** A request withdrawn by the caller (e.g. a cancel of its own). */
	release(peer: string, piece: number, begin: number): void {
		if (this.valid(piece) && begin % BLOCK_SIZE === 0) this.unlink(this.key(piece, begin / BLOCK_SIZE), peer);
	}

	// --- internals ---------------------------------------------------------

	private context(peers: readonly PickerPeer[], now: number): Context {
		const byId = new Map(peers.map((p) => [p.id, p]));
		const unchoked = peers.filter((p) => p.unchoked);
		const best = unchoked.reduce((m, p) => Math.max(m, p.rate > 0 ? p.rate : 0), 0);
		const fast = unchoked.filter((p) => (best > 0 ? p.rate >= this.config.fastRatio * best : true));
		const zones = new Map<number, Zones>();
		let scarce = false;
		for (const t of this.targets.values()) {
			const z = this.zonesOf(t, true);
			zones.set(t.id, z);
			if (z.ahead) scarce = true;
		}
		if (scarce) {
			const counts = this.availabilityOf(peers, now);
			for (const t of this.targets.values()) {
				const z = zones.get(t.id)!;
				if (z.ahead) z.scarce = this.scarceOf(t, z, counts);
			}
		}
		return { now, byId, fast, fastIds: new Set(fast.map((p) => p.id)), zones, endgame: !this.anyFree() };
	}

	private zonesOf(t: Target, withAhead: boolean): Zones {
		const pl = this.g.pieceLength;
		const posPiece = clamp(Math.floor(t.position / pl), t.first, t.last);
		const lastAt = (end: number) => clamp(Math.floor((Math.max(end, t.position + 1) - 1) / pl), posPiece, t.last);
		const winLast = lastAt(Math.min(t.end, t.position + this.windowBytesOf(t.id)));
		const horizonLast = Math.max(winLast, lastAt(Math.min(t.end, t.position + this.scarceAheadBytes)));
		let ahead = false;
		if (withAhead) {
			ahead = true;
			for (let p = posPiece; p <= horizonLast; p++) {
				if (this.inTarget(t, p) && this.state[p] !== VERIFIED) {
					ahead = false;
					break;
				}
			}
		}
		return { posPiece, winLast, horizonLast, ahead, scarce: [] };
	}

	private availabilityOf(peers: readonly PickerPeer[], now: number): Uint16Array {
		const cached = this.availability;
		if (cached && now - cached.at < AVAILABILITY_MS && cached.peers === peers.length) return cached.counts;
		const counts = new Uint16Array(this.g.pieceCount);
		for (const t of this.targets.values()) {
			for (let p = t.first; p <= t.last; p++) {
				if (counts[p] > 0 || this.state[p] === VERIFIED) continue;
				let n = 0;
				for (const peer of peers) if (peer.has(p)) n++;
				counts[p] = Math.min(n, 0xffff);
			}
		}
		this.availability = { at: now, peers: peers.length, counts };
		return counts;
	}

	/** Scarce pieces of a target, rarest first, then in order from the horizon on. */
	private scarceOf(t: Target, z: Zones, counts: Uint16Array): number[] {
		const list: number[] = [];
		for (let p = t.first; p <= t.last; p++) {
			if (this.state[p] !== OPEN || !this.inTarget(t, p)) continue;
			const n = counts[p];
			if (n >= 1 && n <= this.config.scarceHolders) list.push(p);
		}
		const span = t.last - t.first + 1;
		const from = (p: number) => (p - z.horizonLast + span) % span;
		return list.sort((a, b) => counts[a] - counts[b] || from(a) - from(b));
	}

	private anyFree(): boolean {
		for (const t of this.targets.values()) {
			for (let p = t.first; p <= t.last; p++) if (this.state[p] === OPEN && this.free[p] > 0 && this.inTarget(t, p) && this.ready(p)) return true;
		}
		return false;
	}

	private *levelPieces(t: Target, z: Zones, level: PickLevel): Generator<number> {
		switch (level) {
			case PickLevel.Index:
				for (let p = t.first; p <= t.headEnd; p++) yield p;
				for (let p = Math.max(t.tailStart, t.headEnd + 1); p <= t.last; p++) yield p;
				return;
			case PickLevel.Window:
				for (let p = z.posPiece; p <= z.winLast; p++) yield p;
				return;
			case PickLevel.ReadAhead:
				for (let p = z.winLast + 1; p <= z.horizonLast; p++) yield p;
				return;
			case PickLevel.Scarce:
				yield* z.scarce;
				return;
			case PickLevel.BackFill:
				for (let p = z.horizonLast + 1; p <= t.last; p++) yield p;
				for (let p = t.first; p < z.posPiece; p++) yield p;
				return;
		}
	}

	private pickFor(peer: PickerPeer, ctx: Context, room: number, urgentOnly: boolean): PickedBlock[] {
		const out: PickedBlock[] = [];
		const fast = ctx.fastIds.has(peer.id);
		const refused = this.refused.get(peer.id);
		const levels: PickLevel[] = urgentOnly ? [PickLevel.Index, PickLevel.Window] : [1, 2, 3, 4, 5];
		for (const level of levels) {
			for (const t of this.targets.values()) {
				const z = ctx.zones.get(t.id)!;
				for (const p of this.levelPieces(t, z, level)) {
					if (out.length >= room) return out;
					if (this.state[p] !== OPEN || !this.inTarget(t, p) || !peer.has(p) || !this.ready(p)) continue;
					const ex = this.exclusive.get(p);
					if (ex && !this.mayServeIsolated(ex, peer, p, ctx)) continue;
					const until = refused?.get(p);
					if (until !== undefined) {
						if (until > ctx.now) continue;
						refused!.delete(p);
					}
					const urgent = level <= PickLevel.Window;
					// The window is served by the fastest peers, unless none of them has the piece.
					// (An isolated piece: unless none of them may serve it.)
					const allowed =
						!urgent || level === PickLevel.Index || fast || !ctx.fast.some((f) => f.id !== peer.id && f.has(p) && (!ex || this.mayServeIsolated(ex, f, p, ctx)));
					if (!allowed) continue;
					if (this.free[p] > 0 && !urgentOnly) this.takeFree(peer, p, level, room, out, ctx.now);
					if ((urgent || ctx.endgame) && out.length < room) this.takeLate(peer, p, level, room, out, ctx);
				}
			}
		}
		return out;
	}

	/** An isolated piece goes to its owner only; a peer to avoid gets it only if no other connected peer has it. */
	private mayServeIsolated(ex: { owner: string | undefined; avoid: Set<string> }, peer: PickerPeer, p: number, ctx: Context): boolean {
		if (ex.owner !== undefined) return ex.owner === peer.id;
		if (!ex.avoid.has(peer.id)) return true;
		for (const other of ctx.byId.values()) if (!ex.avoid.has(other.id) && other.has(p)) return false;
		return true;
	}

	private takeFree(peer: PickerPeer, p: number, level: PickLevel, room: number, out: PickedBlock[], now: number): void {
		const ex = this.exclusive.get(p);
		if (ex && ex.owner === undefined) ex.owner = peer.id;
		const rec = this.received.get(p);
		const n = this.blocks(p);
		for (let b = 0; b < n && out.length < room && this.free[p] > 0; b++) {
			if (rec?.[b]) continue;
			const key = this.key(p, b);
			if (this.reqs.has(key)) continue;
			this.link(key, peer, now, false);
			out.push({ piece: p, begin: b * BLOCK_SIZE, length: this.blockLength(p, b), level, hedge: false });
		}
	}

	/** Late requests of others for this piece get a second request, from `peer`. */
	private takeLate(peer: PickerPeer, p: number, level: PickLevel, room: number, out: PickedBlock[], ctx: Context): void {
		// An isolated piece is never asked of a second peer.
		if (this.hedged >= this.config.maxHedgedBlocks || this.exclusive.has(p)) return;
		const rec = this.received.get(p);
		const n = this.blocks(p);
		for (let b = 0; b < n && out.length < room && this.hedged < this.config.maxHedgedBlocks; b++) {
			if (rec?.[b]) continue;
			const key = this.key(p, b);
			const reqs = this.reqs.get(key);
			if (!reqs || reqs.length >= MAX_REQUESTS_PER_BLOCK) continue;
			const first = reqs[0];
			if (first.peer === peer.id) continue;
			const holder = ctx.byId.get(first.peer);
			// A window block held by a peer that is no longer among the fast
			// ones (a fast peer's deep queue, minQueueBytes) is late for a fast peer.
			const demoted = level <= PickLevel.Window && !ctx.fastIds.has(first.peer) && ctx.fastIds.has(peer.id);
			const late = !holder || !holder.unchoked || demoted || ctx.now >= first.due + this.config.hedgeGraceMs;
			if (!late) continue;
			this.link(key, peer, ctx.now, true);
			this.hedgesSent++;
			out.push({ piece: p, begin: b * BLOCK_SIZE, length: this.blockLength(p, b), level, hedge: true });
		}
	}

	private link(key: number, peer: PickerPeer, now: number, hedge: boolean): void {
		let mine = this.byPeer.get(peer.id);
		if (!mine) this.byPeer.set(peer.id, (mine = new Map()));
		const p = Math.floor(key / this.bpp);
		const length = this.blockLength(p, key % this.bpp);
		mine.set(key, length);
		const queuedBytes = mine.size * BLOCK_SIZE;
		const due = peer.rate > 0 ? now + (queuedBytes * 1000) / peer.rate : now + this.config.unknownRateDueMs;
		const reqs = this.reqs.get(key);
		if (reqs) {
			reqs.push({ peer: peer.id, due });
			if (hedge && reqs.length === 2) this.hedged++;
		} else {
			this.reqs.set(key, [{ peer: peer.id, due }]);
			this.free[p]--;
		}
	}

	/** Withdraw one peer's request of a block. */
	private unlink(key: number, peer: string): void {
		this.byPeer.get(peer)?.delete(key);
		const reqs = this.reqs.get(key);
		if (!reqs) return;
		const i = reqs.findIndex((r) => r.peer === peer);
		if (i < 0) return;
		if (reqs.length === 2) this.hedged--;
		reqs.splice(i, 1);
		if (reqs.length > 0) return;
		this.reqs.delete(key);
		const p = Math.floor(key / this.bpp);
		if (this.state[p] === OPEN && !this.received.get(p)?.[key % this.bpp]) this.free[p]++;
	}

	private unlinkAll(key: number): void {
		const reqs = this.reqs.get(key);
		if (!reqs) return;
		if (reqs.length >= 2) this.hedged--;
		for (const r of reqs) this.byPeer.get(r.peer)?.delete(key);
		this.reqs.delete(key);
	}

	/** Withdraw every request of a piece; returns the cancels. */
	private dropRequests(piece: number): BlockCancel[] {
		const cancels: BlockCancel[] = [];
		const n = this.blocks(piece);
		for (let b = 0; b < n; b++) {
			const key = this.key(piece, b);
			const reqs = this.reqs.get(key);
			if (!reqs) continue;
			for (const r of reqs) cancels.push({ peer: r.peer, piece, begin: b * BLOCK_SIZE, length: this.blockLength(piece, b) });
			this.unlinkAll(key);
		}
		return cancels;
	}

	/** Cancel every request of a piece that `keep` rejects, and drop such pieces partly received. */
	private withdraw(keep: (piece: number) => boolean): SeekResult {
		const cancels: BlockCancel[] = [];
		for (const [peer, mine] of this.byPeer) {
			for (const [key, length] of [...mine]) {
				const p = Math.floor(key / this.bpp);
				if (keep(p)) continue;
				cancels.push({ peer, piece: p, begin: (key % this.bpp) * BLOCK_SIZE, length });
				this.unlink(key, peer);
			}
		}
		const dropped: number[] = [];
		for (const p of [...this.received.keys()]) {
			if (this.state[p] !== OPEN || keep(p)) continue;
			this.reset(p);
			dropped.push(p);
		}
		return { cancels, dropped };
	}

	private wanted(piece: number): boolean {
		for (const t of this.targets.values()) if (piece >= t.first && piece <= t.last && this.inTarget(t, piece)) return true;
		return false;
	}

	private inTarget(t: Target, piece: number): boolean {
		return t.only === undefined || t.only.has(piece);
	}

	private key(piece: number, block: number): number {
		return piece * this.bpp + block;
	}

	private blockLength(piece: number, block: number): number {
		return Math.min(BLOCK_SIZE, pieceSizeOf(this.g, piece) - block * BLOCK_SIZE);
	}

	private valid(piece: number): boolean {
		return Number.isInteger(piece) && piece >= 0 && piece < this.g.pieceCount;
	}
}

function clamp(n: number, lo: number, hi: number): number {
	return Math.min(hi, Math.max(lo, n));
}

function positive(n: number | undefined): number | undefined {
	return n !== undefined && Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined;
}
