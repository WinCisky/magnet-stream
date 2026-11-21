// Reciprocation (docs/architecture.md §Chunk recovery › Reciprocation
// (upload), decision OQ-04, M3-12): the browser uploads verified pieces so
// that leechers unchoke it as a trading partner, not only on their
// optimistic slot.
//
// - What: only pieces the host says are verified and readable (a
//   PieceSource: the OPFS store in the page's worker), announced with a
//   bitfield (or have_none + have) and a `have` per new piece; the verified
//   info dict to `ut_metadata` requests.
// - Whom (Choker): every 10 s the 4 interested peers that delivered most
//   to us over the last 20 s, plus one optimistic slot rotated every 30 s
//   that favours new connections. Seeds are never unchoked.
// - Which requests (checkRequest): from a peer we unchoked, for a piece we
//   have, inside the piece, blocks of at most 16 KiB, at most 250 requests
//   (the `reqq` we announce) and 2 MiB queued per peer. Anything else is
//   rejected (BEP 6) or ignored, and never read from storage.
// - Cost: a token bucket caps the total rate (2 MiB/s); everything rides on
//   the batched upstream messages as lazy frames (upstream.ts), flushed alone
//   at most every 2 s per socket. Upload runs only while a download is
//   active: otherwise every peer is choked.
//
// Nothing here logs: no address, no info-hash.

import { METADATA_PIECE_SIZE } from "./extension.ts";
import { MAX_BLOCK, pieceSizeOf, type Geometry } from "./peer-wire.ts";

/** Verified pieces the engine may upload (the host's store). */
export interface PieceSource {
	/** The piece is verified and can be read now. */
	has(piece: number): boolean;
	/** Verified pieces known now (e.g. kept from an earlier session). */
	pieces?(): Iterable<number>;
	/** Bytes [begin, begin + length) of a verified piece. Throws (or rejects) if it can't. */
	read(piece: number, begin: number, length: number): Uint8Array | Promise<Uint8Array>;
}

export interface UploadOptions {
	/** Where verified pieces are read from. Without one the engine never uploads (as M1-06). */
	source?: PieceSource;
	/** The user's setting (default true). Off: no bitfield, `have`, unchoke or piece. */
	enabled?: boolean;
	/** Total upload rate, bytes per second (default 2 MiB/s). */
	rate?: number;
	/** Regular unchoke slots (default 4). */
	regularSlots?: number;
	/** Regular slots are chosen this often (default 10 s). */
	rechokeMs?: number;
	/** The optimistic slot rotates this often (default 30 s). */
	optimisticMs?: number;
	/** "Delivered most" counts over this window (default 20 s). */
	deliveredWindowMs?: number;
	/** Connections younger than this weigh `newPeerWeight` in the optimistic draw (default 60 s, 3). */
	newPeerMs?: number;
	newPeerWeight?: number;
	/** Requests queued per peer (default 250: the `reqq` we announce). */
	maxQueuedRequests?: number;
	/** Bytes queued per peer (default 2 MiB). */
	maxQueuedBytes?: number;
	/** Upload frames alone are flushed at most this often per socket (default 2 s). */
	flushMs?: number;
	/** Reads from the source in flight at once (default 16). */
	maxReads?: number;
	/** Rejects sent to one peer before its further bad requests are just ignored (default 1000). */
	maxRejects?: number;
	/** For the optimistic draw (tests). */
	random?: () => number;
}

export const UPLOAD_DEFAULTS = {
	enabled: true,
	rate: 2 * 1024 * 1024,
	regularSlots: 4,
	rechokeMs: 10_000,
	optimisticMs: 30_000,
	deliveredWindowMs: 20_000,
	newPeerMs: 60_000,
	newPeerWeight: 3,
	maxQueuedRequests: 250,
	maxQueuedBytes: 2 * 1024 * 1024,
	flushMs: 2_000,
	maxReads: 16,
	maxRejects: 1_000,
} as const;

// ------------------------------------------------------------------ choker

export interface ChokerPeer {
	id: string;
	interested: boolean;
	seed: boolean;
	connectedAt: number;
	/** Bytes it delivered to us over the window. */
	delivered: number;
}

export interface ChokerOptions {
	regularSlots?: number;
	rechokeMs?: number;
	optimisticMs?: number;
	newPeerMs?: number;
	newPeerWeight?: number;
	random?: () => number;
}

/**
 * Tit-for-tat choker on an injected clock. `tick` returns the peers to keep
 * unchoked: the regular slots, recomputed every `rechokeMs` among the
 * interested non-seeds that delivered something, most first; and one
 * optimistic slot among the other interested non-seeds, redrawn every
 * `optimisticMs` (or at once when its peer leaves, loses interest or earns
 * a regular slot), new connections weighing more.
 */
export class Choker {
	regular = new Set<string>();
	optimistic: string | undefined;
	private readonly opts: Required<ChokerOptions>;
	private nextRechoke = -Infinity;
	private nextOptimistic = -Infinity;

	constructor(options: ChokerOptions = {}) {
		this.opts = {
			regularSlots: UPLOAD_DEFAULTS.regularSlots,
			rechokeMs: UPLOAD_DEFAULTS.rechokeMs,
			optimisticMs: UPLOAD_DEFAULTS.optimisticMs,
			newPeerMs: UPLOAD_DEFAULTS.newPeerMs,
			newPeerWeight: UPLOAD_DEFAULTS.newPeerWeight,
			random: Math.random,
			...options,
		};
	}

	tick(peers: readonly ChokerPeer[], now: number): Set<string> {
		const valid = new Map<string, ChokerPeer>();
		for (const p of peers) if (p.interested && !p.seed) valid.set(p.id, p);
		for (const id of [...this.regular]) if (!valid.has(id)) this.regular.delete(id);
		if (now >= this.nextRechoke) {
			const ranked = [...valid.values()].filter((p) => p.delivered > 0).sort((a, b) => b.delivered - a.delivered || a.connectedAt - b.connectedAt);
			this.regular = new Set(ranked.slice(0, Math.max(0, this.opts.regularSlots)).map((p) => p.id));
			this.nextRechoke = now + this.opts.rechokeMs;
		}
		if (this.optimistic !== undefined && (!valid.has(this.optimistic) || this.regular.has(this.optimistic))) this.optimistic = undefined;
		if (this.optimistic === undefined || now >= this.nextOptimistic) {
			const pool = [...valid.values()].filter((p) => !this.regular.has(p.id) && p.id !== this.optimistic);
			const next = this.draw(pool, now);
			if (next !== undefined) {
				this.optimistic = next;
				this.nextOptimistic = now + this.opts.optimisticMs;
			} else if (this.optimistic !== undefined) this.nextOptimistic = now + this.opts.optimisticMs;
		}
		const out = new Set(this.regular);
		if (this.optimistic !== undefined) out.add(this.optimistic);
		return out;
	}

	reset(): void {
		this.regular.clear();
		this.optimistic = undefined;
		this.nextRechoke = -Infinity;
		this.nextOptimistic = -Infinity;
	}

	private draw(pool: readonly ChokerPeer[], now: number): string | undefined {
		if (pool.length === 0) return undefined;
		const weight = (p: ChokerPeer) => (now - p.connectedAt < this.opts.newPeerMs ? Math.max(1, this.opts.newPeerWeight) : 1);
		const total = pool.reduce((n, p) => n + weight(p), 0);
		let r = Math.min(Math.max(this.opts.random(), 0), 0.999999) * total;
		for (const p of pool) {
			r -= weight(p);
			if (r < 0) return p.id;
		}
		return pool[pool.length - 1].id;
	}
}

// ------------------------------------------------------------ token bucket

/** Bytes allowed per second, with a burst of `burst` bytes. */
export class TokenBucket {
	private tokens: number;
	private at: number;
	rate: number;
	readonly burst: number;

	constructor(rate: number, burst: number, now: number) {
		this.rate = Math.max(0, rate);
		this.burst = Math.max(1, burst);
		this.tokens = this.burst;
		this.at = now;
	}

	available(now: number): number {
		this.refill(now);
		return this.tokens;
	}

	/** Spend `n` bytes if the bucket holds them. */
	take(n: number, now: number): boolean {
		this.refill(now);
		if (n > this.tokens) return false;
		this.tokens -= n;
		return true;
	}

	private refill(now: number): void {
		if (now > this.at) this.tokens = Math.min(this.burst, this.tokens + ((now - this.at) * this.rate) / 1000);
		this.at = Math.max(this.at, now);
	}
}

// ----------------------------------------------------- request validation

export type RequestVerdict = "serve" | "choked" | "no_metadata" | "bad_piece" | "too_large" | "bad_range" | "missing" | "queue_full";

export interface RequestContext {
	/** We choke the peer. */
	choking: boolean;
	geometry: Geometry | undefined;
	/** A verified piece we may serve. */
	available(piece: number): boolean;
	/** Requests and bytes the peer has queued with us already. */
	queued: number;
	queuedBytes: number;
	maxQueued: number;
	maxQueuedBytes: number;
}

/**
 * Whether a peer's `request` may be served. Checked before any read: a
 * choked peer, a piece we lack, a block outside its piece or over 16 KiB,
 * or a full queue never reach storage.
 */
export function checkRequest(r: { piece: number; begin: number; length: number }, ctx: RequestContext): RequestVerdict {
	if (ctx.choking) return "choked";
	const g = ctx.geometry;
	if (!g) return "no_metadata";
	if (!Number.isInteger(r.piece) || r.piece < 0 || r.piece >= g.pieceCount) return "bad_piece";
	if (!Number.isInteger(r.length) || r.length < 1 || r.length > MAX_BLOCK) return "too_large";
	if (!Number.isInteger(r.begin) || r.begin < 0 || r.begin + r.length > pieceSizeOf(g, r.piece)) return "bad_range";
	if (!ctx.available(r.piece)) return "missing";
	if (ctx.queued + 1 > ctx.maxQueued || ctx.queuedBytes + r.length > ctx.maxQueuedBytes) return "queue_full";
	return "serve";
}

// --------------------------------------------------------------- uploader

/** What the uploader needs of a connection (PeerConnection implements it). */
export interface UploadPeer {
	readonly fast: boolean;
	readonly closed: boolean;
	readonly isSeed: boolean;
	/** The peer told us it is interested. */
	readonly peerInterested: boolean;
	/** We choke the peer (true until `unchokePeer`). */
	readonly amChoking: boolean;
	has(piece: number): boolean;
	chokePeer(): void;
	unchokePeer(): void;
	sendHave(piece: number): void;
	sendPiece(piece: number, begin: number, data: Uint8Array): void;
	rejectRequest(piece: number, begin: number, length: number): void;
	sendMetadataPiece(piece: number, totalSize: number, data: Uint8Array): boolean;
	rejectMetadata(piece: number): void;
}

interface Request {
	piece: number;
	begin: number;
	length: number;
}

interface PeerState {
	id: string;
	peer: UploadPeer;
	connectedAt: number;
	queue: Request[];
	/** Requests read from the source right now, by `piece:begin`. */
	reading: Map<string, Request>;
	/** Bytes of `queue` and `reading`. */
	queuedBytes: number;
	rejects: number;
	/** Bytes it delivered to us, sampled about once a second: [at, total]. */
	history: [number, number][];
	uploaded: number;
}

export interface UploadStats {
	/** Piece bytes sent to peers. */
	bytes: number;
	blocks: number;
	/** Requests refused with a reject, or ignored. */
	rejected: number;
	ignored: number;
	/** Requests refused, by reason (checkRequest's verdicts, `read_failed`, `cancelled`). */
	refused: Record<string, number>;
	/** ut_metadata pieces sent, and refused. */
	metadataPieces: number;
	metadataRejected: number;
	/** Unchoke messages sent. */
	unchokes: number;
	/** Reads from the source (only for requests that passed every check). */
	reads: number;
}

const key = (piece: number, begin: number) => `${piece}:${begin}`;

export class Uploader {
	readonly stats: UploadStats = { bytes: 0, blocks: 0, rejected: 0, ignored: 0, refused: {}, metadataPieces: 0, metadataRejected: 0, unchokes: 0, reads: 0 };
	private readonly opts: Required<Omit<UploadOptions, "source" | "random">> & Pick<UploadOptions, "source" | "random">;
	private readonly now: () => number;
	private readonly peers = new Map<string, PeerState>();
	private readonly choker: Choker;
	private readonly bucket: TokenBucket;
	private readonly availableSet = new Set<number>();
	private geometry: Geometry | undefined;
	private infoDict: Uint8Array | undefined;
	private setting: boolean;
	/** A download is running (set by `tick`). */
	private active = false;
	private reads = 0;
	private turn = 0;

	constructor(options: UploadOptions = {}, now: () => number = Date.now) {
		this.opts = { ...UPLOAD_DEFAULTS, ...options };
		this.now = now;
		this.setting = this.opts.enabled;
		this.choker = new Choker({
			regularSlots: this.opts.regularSlots,
			rechokeMs: this.opts.rechokeMs,
			optimisticMs: this.opts.optimisticMs,
			newPeerMs: this.opts.newPeerMs,
			newPeerWeight: this.opts.newPeerWeight,
			...(this.opts.random ? { random: this.opts.random } : {}),
		});
		const rate = Math.max(MAX_BLOCK, this.opts.rate);
		// Half a second of rate: the 250 ms sweep never leaves the bucket full.
		this.bucket = new TokenBucket(rate, Math.max(METADATA_PIECE_SIZE, MAX_BLOCK, rate / 2), now());
	}

	/** Uploading is possible: the setting is on and there is a source. */
	get on(): boolean {
		return this.setting && this.opts.source !== undefined;
	}

	/** The user's setting. */
	get enabled(): boolean {
		return this.setting;
	}

	get isActive(): boolean {
		return this.active;
	}

	/** Peers we unchoke now. */
	get unchoked(): number {
		let n = 0;
		for (const s of this.peers.values()) if (!s.peer.amChoking && !s.peer.closed) n++;
		return n;
	}

	/** Pieces we may serve. */
	get availableCount(): number {
		return this.availableSet.size;
	}

	isAvailable(piece: number): boolean {
		return this.availableSet.has(piece);
	}

	/** The upload setting changed: on announces what we have, off chokes everyone. */
	setEnabled(on: boolean): void {
		if (on === this.setting) return;
		this.setting = on;
		if (!this.on) {
			this.chokeAll();
			return;
		}
		for (const s of this.peers.values()) for (const piece of this.availableSet) if (!s.peer.has(piece)) s.peer.sendHave(piece);
	}

	/** The torrent's layout and verified info dict (served over ut_metadata). */
	setInfo(geometry: Geometry, infoDict?: Uint8Array): void {
		this.geometry = geometry;
		if (infoDict && infoDict.length > 0) this.infoDict = infoDict;
		for (const piece of this.opts.source?.pieces?.() ?? []) this.offer(piece);
	}

	/** Size of the info dict we can serve (announced in our extended handshake). */
	get metadataSize(): number | undefined {
		return this.on ? this.infoDict?.length : undefined;
	}

	/**
	 * A verified piece may be uploaded if the source can read it: announce
	 * it (`have`) to the peers that lack it. Returns whether it was new.
	 */
	offer(piece: number): boolean {
		const g = this.geometry;
		const source = this.opts.source;
		if (!g || !source || !Number.isInteger(piece) || piece < 0 || piece >= g.pieceCount) return false;
		if (this.availableSet.has(piece) || !source.has(piece)) return false;
		this.availableSet.add(piece);
		if (this.on) for (const s of this.peers.values()) if (!s.peer.closed && !s.peer.has(piece)) s.peer.sendHave(piece);
		return true;
	}

	/** Our bitfield for a new connection (undefined: say nothing, or have_none with the fast extension). */
	bitfield(): Uint8Array | undefined {
		const g = this.geometry;
		if (!this.on || !g || this.availableSet.size === 0) return undefined;
		const bits = new Uint8Array(Math.ceil(g.pieceCount / 8));
		for (const p of this.availableSet) bits[p >> 3] |= 0x80 >> (p & 7);
		return bits;
	}

	add(id: string, peer: UploadPeer, connectedAt = this.now()): void {
		this.peers.set(id, { id, peer, connectedAt, queue: [], reading: new Map(), queuedBytes: 0, rejects: 0, history: [], uploaded: 0 });
	}

	remove(id: string): void {
		this.peers.delete(id);
	}

	/** A peer's `request`: validated, then queued or refused, never read before. */
	onRequest(id: string, piece: number, begin: number, length: number): void {
		const s = this.peers.get(id);
		if (!s) return;
		// Upload off: as before M3-12, requests are ignored.
		if (!this.on) {
			this.refuse(s, { piece, begin, length }, "off", false);
			return;
		}
		const verdict = checkRequest(
			{ piece, begin, length },
			{
				choking: s.peer.amChoking,
				geometry: this.geometry,
				available: (p) => this.availableSet.has(p),
				queued: s.queue.length + s.reading.size,
				queuedBytes: s.queuedBytes,
				maxQueued: this.opts.maxQueuedRequests,
				maxQueuedBytes: this.opts.maxQueuedBytes,
			},
		);
		if (verdict !== "serve") {
			this.refuse(s, { piece, begin, length }, verdict, true);
			return;
		}
		const k = key(piece, begin);
		if (s.reading.has(k) || s.queue.some((r) => r.piece === piece && r.begin === begin)) return;
		s.queue.push({ piece, begin, length });
		s.queuedBytes += length;
		this.pump();
	}

	/** A peer's `cancel`: the request leaves the queue (BEP 6: answered with a reject). */
	onCancel(id: string, piece: number, begin: number, length: number): void {
		const s = this.peers.get(id);
		if (!s) return;
		const k = key(piece, begin);
		const at = s.queue.findIndex((r) => r.piece === piece && r.begin === begin && r.length === length);
		let found: Request | undefined;
		if (at >= 0) found = s.queue.splice(at, 1)[0];
		else if (s.reading.get(k)?.length === length) {
			found = s.reading.get(k);
			s.reading.delete(k);
		}
		if (!found) return;
		s.queuedBytes -= found.length;
		if (s.peer.fast) s.peer.rejectRequest(piece, begin, length);
	}

	/** A peer's ut_metadata request: the verified info dict while upload is on, else a reject. */
	onMetadataRequest(id: string, piece: number): void {
		const s = this.peers.get(id);
		if (!s) return;
		const info = this.infoDict;
		const pieces = info ? Math.ceil(info.length / METADATA_PIECE_SIZE) : 0;
		if (!this.on || !info || !Number.isInteger(piece) || piece < 0 || piece >= pieces) {
			this.stats.metadataRejected++;
			s.peer.rejectMetadata(piece);
			return;
		}
		const data = info.subarray(piece * METADATA_PIECE_SIZE, (piece + 1) * METADATA_PIECE_SIZE);
		if (!this.bucket.take(data.length, this.now())) {
			this.stats.metadataRejected++;
			s.peer.rejectMetadata(piece);
			return;
		}
		if (s.peer.sendMetadataPiece(piece, info.length, data)) this.stats.metadataPieces++;
	}

	/**
	 * Called from the sweep. `active`: a download is running (otherwise, and
	 * with upload off, every peer is choked). `delivered`: bytes a peer has
	 * delivered to us so far (its running total).
	 */
	tick(active: boolean, delivered: (id: string) => number, now = this.now()): void {
		for (const s of this.peers.values()) this.sample(s, delivered(s.id), now);
		this.active = active;
		if (!this.on || !active) {
			this.chokeAll();
			return;
		}
		const window = this.opts.deliveredWindowMs;
		const views: ChokerPeer[] = [];
		for (const s of this.peers.values()) {
			if (s.peer.closed || !s.peer.peerInterested) continue;
			views.push({ id: s.id, interested: true, seed: s.peer.isSeed, connectedAt: s.connectedAt, delivered: this.deliveredSince(s, now - window, delivered(s.id)) });
		}
		const keep = this.choker.tick(views, now);
		for (const s of this.peers.values()) {
			if (s.peer.closed) continue;
			if (keep.has(s.id)) {
				if (s.peer.amChoking) {
					s.peer.unchokePeer();
					this.stats.unchokes++;
				}
			} else if (!s.peer.amChoking) this.choke(s);
		}
		this.pump();
	}

	/** Choke everyone and drop every queue (upload off, session idle, suspended or done). */
	chokeAll(): void {
		this.choker.reset();
		for (const s of this.peers.values()) if (!s.peer.amChoking || s.queue.length > 0 || s.reading.size > 0) this.choke(s);
	}

	/** Forget every peer (the connections are gone). */
	reset(): void {
		this.chokeAll();
		this.peers.clear();
		this.active = false;
	}

	/** Bytes uploaded to one peer (tests, diagnostics). */
	uploadedTo(id: string): number {
		return this.peers.get(id)?.uploaded ?? 0;
	}

	/** Requests queued for one peer, reads included (tests). */
	queuedFor(id: string): number {
		const s = this.peers.get(id);
		return s ? s.queue.length + s.reading.size : 0;
	}

	private choke(s: PeerState): void {
		if (!s.peer.closed && !s.peer.amChoking) s.peer.chokePeer();
		// BEP 6: with the fast extension a choke does not void requests: reject each one.
		for (const r of [...s.queue, ...s.reading.values()]) if (s.peer.fast && !s.peer.closed) s.peer.rejectRequest(r.piece, r.begin, r.length);
		s.queue = [];
		s.reading.clear();
		s.queuedBytes = 0;
	}

	private refuse(s: PeerState, r: Request, why: string, answer: boolean): void {
		this.stats.refused[why] = (this.stats.refused[why] ?? 0) + 1;
		if (answer && s.peer.fast && s.rejects < this.opts.maxRejects) {
			s.rejects++;
			this.stats.rejected++;
			s.peer.rejectRequest(r.piece, r.begin, r.length);
		} else this.stats.ignored++;
	}

	private sample(s: PeerState, total: number, now: number): void {
		const last = s.history[s.history.length - 1];
		if (!last || now - last[0] >= 1_000) s.history.push([now, total]);
		const cutoff = now - this.opts.deliveredWindowMs - 2_000;
		while (s.history.length > 1 && s.history[1][0] <= cutoff) s.history.shift();
	}

	/** Bytes delivered since `from`: the total now minus the last sample at or before it. */
	private deliveredSince(s: PeerState, from: number, total: number): number {
		let base = s.history[0]?.[1] ?? total;
		for (const [at, value] of s.history) {
			if (at > from) break;
			base = value;
		}
		return Math.max(0, total - base);
	}

	/** Serve queued requests, round robin across peers, while the bucket and the read budget allow. */
	private pump(): void {
		const source = this.opts.source;
		if (!this.on || !this.active || !source) return;
		const states = [...this.peers.values()];
		if (states.length === 0) return;
		const now = this.now();
		let idle = 0;
		while (this.reads < this.opts.maxReads && idle < states.length) {
			const s = states[this.turn++ % states.length];
			const r = s.queue[0];
			if (!r || s.peer.closed || s.peer.amChoking) {
				idle++;
				continue;
			}
			idle = 0;
			if (!source.has(r.piece)) {
				s.queue.shift();
				s.queuedBytes -= r.length;
				this.refuse(s, r, "missing", true);
				continue;
			}
			if (!this.bucket.take(r.length, now)) return;
			s.queue.shift();
			this.read(s, r, source);
		}
	}

	private read(s: PeerState, r: Request, source: PieceSource): void {
		const k = key(r.piece, r.begin);
		s.reading.set(k, r);
		this.reads++;
		this.stats.reads++;
		let pending: Uint8Array | Promise<Uint8Array>;
		try {
			pending = source.read(r.piece, r.begin, r.length);
		} catch (e) {
			pending = Promise.reject(e);
		}
		void Promise.resolve(pending).then(
			(data) => this.served(s, r, k, data),
			() => this.served(s, r, k, undefined),
		);
	}

	private served(s: PeerState, r: Request, k: string, data: Uint8Array | undefined): void {
		this.reads--;
		// Cancelled, choked or gone meanwhile: the reject (if any) was sent then.
		if (s.reading.get(k) !== r || this.peers.get(s.id) !== s) {
			this.pump();
			return;
		}
		s.reading.delete(k);
		s.queuedBytes -= r.length;
		if (s.peer.closed || s.peer.amChoking || !this.on || !this.active) return;
		if (!(data instanceof Uint8Array) || data.length !== r.length) {
			this.refuse(s, r, "read_failed", true);
		} else {
			s.peer.sendPiece(r.piece, r.begin, data);
			s.uploaded += data.length;
			this.stats.bytes += data.length;
			this.stats.blocks++;
		}
		this.pump();
	}
}
