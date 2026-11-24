// The session's address book (docs/architecture.md §Chunk recovery › Peer
// sources, › Connection management "Remember within the session"): every
// peer address learned, deduplicated, with where it came from, what
// happened when we tried it, and a dial priority. In memory only.
//
// Addresses come from untrusted places (discovery, peers' ut_pex), so
// every one is validated before it is kept: literal IPv4/IPv6, public
// unicast, port >= 1024 (the gateways' rule: dialling anything else would
// only be refused). The book is bounded in total and per batch, so a peer
// flooding pex messages cannot grow it without limit.
//
// Discovery lines carry the VPS scout's facts (OQ-21): they set the dial
// order of addresses we have not contacted yet (live seeds, then live
// partial peers, then unknown, dead last) and let a live address take any
// tunnel slot (OQ-23). Once the engine dialled an address, what it
// measured replaces them: a "live" address that fails through the tunnel
// is untried from then on and backs off like any other. An address the
// scout saw dead is not dialled at all (each dead dial holds a tunnel slot
// ~20 s, OQ-03) unless a peer names it again by peer exchange.
//
// What the book learned can be kept as notes (`notes()`, persisted by the
// page in IndexedDB, M3-04) and given to the next session for the same
// torrent (`restore()`): addresses that delivered are redialled first,
// addresses that failed go last, and the scout's facts come back as the
// prior. Notes are untrusted input like any other address source: every
// one is validated again, and a malformed one is dropped.

import { checkAddress, formatAddress, parseAddress, type PeerAddress } from "@magnet-stream/tunnel-protocol";
import type { PexMessage } from "./extension.ts";
import type { ScoutFacts } from "./peers-stream.ts";

export type PeerOrigin = "discovery" | "pex" | "magnet" | "manual" | "memory";

export type PeerState = "new" | "connecting" | "connected" | "failed";

/** Why a dial or a connection ended badly (FailReason / CloseReason names, or the engine's own). */
export type PeerFailure = string;

/**
 * How a dial is counted against a socket (OQ-03, OQ-23): `known` connected
 * before, `scout` the VPS saw it alive and we have not tried it yet,
 * `untried` everything else (at most 3 in flight per socket).
 */
export type DialClass = "known" | "scout" | "untried";

export interface PeerEntry {
	/** Canonical `ip:port` / `[ip6]:port`. */
	key: string;
	address: PeerAddress;
	origins: Set<PeerOrigin>;
	state: PeerState;
	/** Pex flags: it said it is a seed (0x02) or reachable (0x10). */
	seedHint: boolean;
	reachableHint: boolean;
	failures: number;
	lastFailure: PeerFailure | null;
	/** Not to be dialled again before this time (ms). */
	retryAt: number;
	/** It handshook with us at least once. */
	connectedBefore: boolean;
	/** Payload bytes it delivered (verified or not), over all connections. */
	delivered: number;
	/** Bytes it delivered in earlier sessions (restored notes): a dial prior, not this session's count. */
	remembered: number;
	addedAt: number;
	/** Banned for the session (corrupt data, wrong torrent). */
	banned: boolean;
	/** The VPS scout's facts (discovery), a prior until the engine measured the peer itself. */
	scout: ScoutFacts | undefined;
	/** When `scout` was received (ms, the book's clock). */
	scoutAt: number;
	/** The engine dialled it and saw the outcome (connected, or failed): `scout` no longer counts. */
	measured: boolean;
	/** The scout saw it dead, then a peer named it again (pex): it may have come back. */
	revived: boolean;
	/** Learned from a peer that delivered to us: likely a live swarm member. */
	vouched: boolean;
}

export interface PeerSink {
	/**
	 * New addresses from a source; returns how many were new.
	 * `flags`, when present, are the BEP 11 `added.f` bytes (same order).
	 */
	(addresses: readonly (string | PeerAddress)[], origin: PeerOrigin, flags?: readonly number[], scouts?: readonly (ScoutFacts | undefined)[]): number;
}

/** A provider of addresses (discovery stream, magnet hints...). */
export interface PeerSource {
	start(sink: PeerSink): void;
	stop(): void;
}

export interface PeerBookOptions {
	/** Addresses kept at most (default 3000). */
	maxPeers?: number;
	/** Addresses taken from one batch at most (default 100: a pex message carries up to 50). */
	maxPerBatch?: number;
	/** Accept private/loopback addresses (local tests only; the port rule stays). */
	allowPrivate?: boolean;
	/** Dial addresses the scout saw dead (last of all). Default false: only once pex names them again. */
	dialScoutDead?: boolean;
	now?: () => number;
}

/** Back-off after a failure: base × 2^(failures-1), capped. */
const BACKOFF: Record<string, number> = {
	refused: 10 * 60_000,
	timeout: 5 * 60_000,
	not_bittorrent: Infinity,
	infohash_mismatch: Infinity,
	forbidden_address: Infinity,
	remote_closed: 60_000,
	choked: 30_000,
	slow: 60_000,
	idle: 30_000,
	/** Our socket was full: the peer is fine, try again shortly. */
	busy: 500,
	/** It had none of the pieces we want (it may get some later). */
	useless: 2 * 60_000,
	/** It caused failed pieces (attribution.ts): never again this session. */
	corrupt: Infinity,
	/** A hole-punch relay says the target is ourselves (BEP 55 NoSelf). */
	hp_no_self: Infinity,
};
/** Notes kept per torrent, at most (each about 100 to 200 bytes once stored). */
export const MAX_PEER_NOTES = 200;
/** Scout facts older than this are not restored from notes (the VPS forgets them after about 2 h). */
export const SCOUT_NOTE_TTL_MS = 2 * 60 * 60_000;
const MAX_NOTE_FAILURES = 1_000;
const FAILURE_NAME = /^[a-z_]{1,32}$/;

/**
 * What a session learned about one address, kept for the next session of
 * the same torrent (M3-04). Stored in the browser only.
 */
export interface PeerNote {
	/** Canonical `ip:port` / `[ip6]:port`. */
	addr: string;
	/** Payload bytes it delivered, this session and earlier ones. */
	delivered: number;
	/** It completed a handshake with us at least once. */
	connected: boolean;
	/** Failures since it last delivered. */
	failures: number;
	/** The last failure's reason. */
	failure?: string;
	/** Banned (corrupt data, wrong torrent): never dialled. */
	banned?: boolean;
	/** The VPS scout's facts, and when they were received (ms since the epoch). */
	scout?: ScoutFacts;
	scoutAt?: number;
}

/** A note read back from storage, validated; null if anything is off. */
export function parsePeerNote(value: unknown): PeerNote | null {
	if (!value || typeof value !== "object" || Array.isArray(value)) return null;
	const v = value as Record<string, unknown>;
	if (typeof v.addr !== "string" || v.addr.length > 64) return null;
	const address = parseAddress(v.addr);
	if (!address) return null;
	if (!Number.isSafeInteger(v.delivered) || (v.delivered as number) < 0) return null;
	if (typeof v.connected !== "boolean") return null;
	if (!Number.isInteger(v.failures) || (v.failures as number) < 0 || (v.failures as number) > MAX_NOTE_FAILURES) return null;
	if (v.failure !== undefined && (typeof v.failure !== "string" || !FAILURE_NAME.test(v.failure))) return null;
	if (v.banned !== undefined && typeof v.banned !== "boolean") return null;
	const note: PeerNote = { addr: formatAddress(address), delivered: v.delivered as number, connected: v.connected, failures: v.failures as number };
	if (v.failure !== undefined) note.failure = v.failure as string;
	if (v.banned === true) note.banned = true;
	if (v.scout !== undefined) {
		const scout = validScout(v.scout as ScoutFacts);
		if (!scout || typeof v.scoutAt !== "number" || !Number.isFinite(v.scoutAt) || v.scoutAt < 0) return null;
		note.scout = scout;
		note.scoutAt = v.scoutAt;
	}
	return note;
}

/** Banned addresses remembered even once their entry is evicted, at most. */
const MAX_BANS = 10_000;
const DEFAULT_BACKOFF = 60_000;
const MAX_BACKOFF = 30 * 60_000;

export class PeerBook {
	private readonly peers = new Map<string, PeerEntry>();
	private readonly opts: Required<PeerBookOptions>;
	/** Addresses banned this session: an evicted entry must not come back as new. */
	private readonly bans = new Set<string>();
	/** Addresses refused: invalid, forbidden, banned, over a limit (counters only). */
	readonly rejected = { invalid: 0, forbidden: 0, banned: 0, full: 0, batch: 0 };

	constructor(options: PeerBookOptions = {}) {
		this.opts = { maxPeers: 3000, maxPerBatch: 100, allowPrivate: false, dialScoutDead: false, now: Date.now, ...options };
	}

	get size(): number {
		return this.peers.size;
	}

	get(key: string): PeerEntry | undefined {
		return this.peers.get(key);
	}

	entries(): IterableIterator<PeerEntry> {
		return this.peers.values();
	}

	/** The sink sources feed. */
	readonly sink: PeerSink = (addresses, origin, flags, scouts) => this.addMany(addresses, origin, flags, scouts);

	addMany(addresses: readonly (string | PeerAddress)[], origin: PeerOrigin, flags?: readonly number[], scouts?: readonly (ScoutFacts | undefined)[]): number {
		let added = 0;
		const n = Math.min(addresses.length, this.opts.maxPerBatch);
		this.rejected.batch += addresses.length - n;
		for (let i = 0; i < n; i++) if (this.add(addresses[i], origin, flags?.[i] ?? 0, scouts?.[i])) added++;
		return added;
	}

	/**
	 * Validate and keep one address; true if it is new. `scout` (discovery
	 * only) is re-checked here: facts out of range are dropped, the address
	 * is kept without them.
	 */
	add(address: string | PeerAddress, origin: PeerOrigin, flags = 0, scout?: ScoutFacts): boolean {
		const parsed = typeof address === "string" ? parseAddress(address) : address;
		if (!parsed || !(parsed.family === 4 ? parsed.ip.length === 4 : parsed.ip.length === 16)) {
			this.rejected.invalid++;
			return false;
		}
		if (checkAddress(parsed, this.opts.allowPrivate) !== "ok") {
			this.rejected.forbidden++;
			return false;
		}
		const key = formatAddress(parsed);
		if (this.bans.has(key)) {
			this.rejected.banned++;
			return false;
		}
		const facts = origin === "discovery" || origin === "memory" ? validScout(scout) : undefined;
		const known = this.peers.get(key);
		if (known) {
			known.origins.add(origin);
			if (flags & 0x02) known.seedHint = true;
			if (flags & 0x10) known.reachableHint = true;
			// Remembered facts never replace what this session was told.
			if (facts && !known.measured && !(origin === "memory" && known.scout)) {
				known.scout = facts;
				known.scoutAt = this.opts.now();
			}
			if (origin === "pex" && known.scout?.alive === false) known.revived = true;
			return false;
		}
		if (this.peers.size >= this.opts.maxPeers && !this.evict()) {
			this.rejected.full++;
			return false;
		}
		this.peers.set(key, {
			key,
			address: { family: parsed.family, ip: parsed.ip.slice(), port: parsed.port },
			origins: new Set([origin]),
			state: "new",
			seedHint: (flags & 0x02) !== 0,
			reachableHint: (flags & 0x10) !== 0,
			failures: 0,
			lastFailure: null,
			retryAt: 0,
			connectedBefore: false,
			delivered: 0,
			remembered: 0,
			addedAt: this.opts.now(),
			banned: false,
			scout: facts,
			scoutAt: facts ? this.opts.now() : 0,
			measured: false,
			revived: false,
			vouched: false,
		});
		return true;
	}

	/**
	 * Make room by forgetting a peer that failed for good or repeatedly
	 * (never one that worked, unless it was banned: its ban outlives the entry).
	 */
	private evict(): boolean {
		let worst: PeerEntry | undefined;
		for (const p of this.peers.values()) {
			if (p.state === "connecting" || p.state === "connected" || (p.connectedBefore && !p.banned)) continue;
			if (p.failures === 0 && !p.banned) continue;
			if (!worst || p.banned || p.failures > worst.failures) worst = p;
		}
		if (!worst) return false;
		this.peers.delete(worst.key);
		return true;
	}

	/**
	 * The address to put in an `open`, re-checked against the gateway's
	 * rules. Undefined when it would be refused (unknown, banned, private,
	 * reserved, or port < 1024): a caller must not dial that.
	 */
	addressForOpen(key: string): PeerAddress | undefined {
		const p = this.peers.get(key);
		if (!p || p.banned) return undefined;
		if (checkAddress(p.address, this.opts.allowPrivate) !== "ok") return undefined;
		return { family: p.address.family, ip: p.address.ip.slice(), port: p.address.port };
	}

	/**
	 * Up to `n` peers to dial now, best first: those that delivered, then
	 * those that connected before, then pex seeds and reachable hints, then
	 * untried ones (oldest first), then failed ones whose back-off is over.
	 */
	candidates(n: number, exclude?: (p: PeerEntry) => boolean): PeerEntry[] {
		const now = this.opts.now();
		const ready: PeerEntry[] = [];
		for (const p of this.peers.values()) {
			if (p.banned || p.state === "connecting" || p.state === "connected" || p.retryAt > now) continue;
			if (!this.opts.dialScoutDead && scoutDead(p)) continue;
			if (checkAddress(p.address, this.opts.allowPrivate) !== "ok") continue;
			if (exclude?.(p)) continue;
			ready.push(p);
		}
		return ready.sort((a, b) => priority(b) - priority(a) || a.addedAt - b.addedAt).slice(0, Math.max(0, n));
	}

	/** Untried (never dialled) addresses. */
	get untried(): number {
		let n = 0;
		for (const p of this.peers.values()) if (p.state === "new" && p.failures === 0 && !p.banned) n++;
		return n;
	}

	/** A peer that delivered to us named `key` (pex). */
	vouch(key: string): void {
		const p = this.peers.get(key);
		if (p) p.vouched = true;
	}

	/** How a dial to `p` counts against a socket's limits. */
	dialClass(p: PeerEntry): DialClass {
		if (p.connectedBefore) return "known";
		return scoutAlive(p) ? "scout" : "untried";
	}

	/**
	 * The dial never happened (the tunnel had no free slot): no fault of the
	 * peer. It goes back to the candidates as it was.
	 */
	markNotDialled(key: string): void {
		const p = this.peers.get(key);
		if (!p || p.state !== "connecting") return;
		p.state = p.connectedBefore || p.failures > 0 ? "failed" : "new";
		p.retryAt = this.opts.now();
	}

	markConnecting(key: string): void {
		const p = this.peers.get(key);
		if (p) p.state = "connecting";
	}

	markConnected(key: string): void {
		const p = this.peers.get(key);
		if (!p) return;
		p.state = "connected";
		p.connectedBefore = true;
		p.measured = true;
	}

	/** The dial failed, or the connection ended for `reason`. */
	markFailed(key: string, reason: PeerFailure): void {
		const p = this.peers.get(key);
		if (!p) return;
		p.state = "failed";
		p.failures++;
		p.lastFailure = reason;
		// What the tunnel saw replaces the scout's word (a "live" address is untried from now on).
		p.measured = true;
		const base = BACKOFF[reason] ?? DEFAULT_BACKOFF;
		if (base === Infinity) {
			this.markBanned(p);
			return;
		}
		// A peer that served us is worth coming back to soon.
		const scaled = p.delivered + p.remembered > 0 ? Math.min(base, 15_000) : base;
		p.retryAt = this.opts.now() + Math.min(scaled * 2 ** Math.min(p.failures - 1, 10), MAX_BACKOFF);
	}

	/** A connection closed without fault (we dropped it, or the session ended): dial again whenever. */
	markClosed(key: string): void {
		const p = this.peers.get(key);
		if (!p) return;
		p.state = "failed";
		p.retryAt = this.opts.now();
	}

	markDelivered(key: string, bytes: number): void {
		const p = this.peers.get(key);
		if (!p) return;
		p.delivered += bytes;
		p.failures = 0;
		p.lastFailure = null;
	}

	/** Never again this session (corrupt data, protocol abuse); `reason` is kept as its last failure. */
	ban(key: string, reason?: PeerFailure): void {
		const p = this.peers.get(key);
		if (!p) return;
		if (reason !== undefined) p.lastFailure = reason;
		this.markBanned(p);
	}

	isBanned(key: string): boolean {
		return this.bans.has(key) || this.peers.get(key)?.banned === true;
	}

	private markBanned(p: PeerEntry): void {
		p.banned = true;
		this.bans.delete(p.key);
		this.bans.add(p.key);
		if (this.bans.size > MAX_BANS) this.bans.delete(this.bans.keys().next().value!);
	}

	/** The best peers we know worked, to redial after the gateway dropped (most delivered first). */
	best(n: number): PeerEntry[] {
		return [...this.peers.values()]
			.filter((p) => !p.banned && p.connectedBefore)
			.sort((a, b) => b.delivered + b.remembered - (a.delivered + a.remembered))
			.slice(0, n);
	}

	/**
	 * What this book learned that is worth keeping for the next session of
	 * the same torrent, at most `max` addresses, most useful first: peers
	 * that delivered, then those that connected, then those the scout saw
	 * alive, then those that failed or were banned. Untried addresses with
	 * no scout facts are left out (discovery names them again).
	 */
	notes(max = MAX_PEER_NOTES): PeerNote[] {
		const worth: PeerEntry[] = [];
		for (const p of this.peers.values()) {
			if (p.delivered + p.remembered > 0 || p.connectedBefore || p.banned || (p.measured && p.failures > 0) || p.scout) worth.push(p);
		}
		const rank = (p: PeerEntry) => (p.banned ? -1_000 : priority(p));
		return worth
			.sort((a, b) => rank(b) - rank(a) || a.addedAt - b.addedAt)
			.slice(0, Math.max(0, Math.min(max, MAX_PEER_NOTES)))
			.map((p) => {
				const note: PeerNote = { addr: p.key, delivered: Math.min(p.delivered + p.remembered, Number.MAX_SAFE_INTEGER), connected: p.connectedBefore, failures: Math.min(p.failures, MAX_NOTE_FAILURES) };
				if (p.lastFailure !== null && FAILURE_NAME.test(p.lastFailure)) note.failure = p.lastFailure;
				if (p.banned) note.banned = true;
				if (p.scout) {
					note.scout = { ...p.scout };
					note.scoutAt = p.scoutAt;
				}
				return note;
			});
	}

	/**
	 * Notes kept from an earlier session (untrusted: each one is validated,
	 * a bad one is dropped). Addresses that delivered become the first to
	 * dial and may take any tunnel slot; addresses that failed go last;
	 * banned ones stay out. The scout's facts are a prior again, unless older
	 * than `SCOUT_NOTE_TTL_MS` (scores fade on the VPS within hours).
	 * Returns how many notes were applied.
	 */
	restore(notes: readonly unknown[]): number {
		const now = this.opts.now();
		let applied = 0;
		for (const raw of notes.slice(0, MAX_PEER_NOTES)) {
			const note = parsePeerNote(raw);
			if (!note) {
				this.rejected.invalid++;
				continue;
			}
			const fresh = note.scout !== undefined && note.scoutAt !== undefined && note.scoutAt <= now + 60_000 && now - note.scoutAt <= SCOUT_NOTE_TTL_MS;
			const address = parseAddress(note.addr)!;
			const added = this.add(address, "memory", 0, fresh ? note.scout : undefined);
			const p = this.peers.get(formatAddress(address));
			if (!p || p.state === "connecting" || p.state === "connected") continue;
			if (added && fresh) p.scoutAt = note.scoutAt!;
			p.remembered = Math.max(p.remembered, note.delivered);
			if (note.connected) p.connectedBefore = true;
			if (note.connected || note.failures > 0) {
				// What an earlier session measured replaces the scout's word, as it would have then.
				p.measured = true;
				p.state = "failed";
				p.failures = Math.max(p.failures, note.failures);
				if (note.failure !== undefined) p.lastFailure = note.failure;
				p.retryAt = 0;
			}
			if (note.banned) this.markBanned(p);
			applied++;
		}
		return applied;
	}

	counts(): BookCounts {
		const c: BookCounts = { known: this.peers.size, untried: 0, connected: 0, failed: 0, banned: 0, delivered: 0, scoutAlive: 0, scoutSeeds: 0, scoutDead: 0, scoutUnknown: 0 };
		for (const p of this.peers.values()) {
			if (p.banned) c.banned++;
			else if (p.state === "connected") c.connected++;
			else if (p.state === "failed") c.failed++;
			else if (p.state === "new") c.untried++;
			if (p.delivered > 0) c.delivered++;
			if (!p.scout) continue;
			if (p.scout.alive === true) {
				c.scoutAlive++;
				if (p.scout.seed === true) c.scoutSeeds++;
			} else if (p.scout.alive === false) c.scoutDead++;
			else c.scoutUnknown++;
		}
		return c;
	}
}

export interface BookCounts {
	known: number;
	untried: number;
	connected: number;
	failed: number;
	banned: number;
	delivered: number;
	/** Addresses by what the scout said (whatever happened since). */
	scoutAlive: number;
	scoutSeeds: number;
	scoutDead: number;
	scoutUnknown: number;
}

/** The scout saw it dead, the engine has not tried it, and no peer named it since. */
function scoutDead(p: PeerEntry): boolean {
	return !p.measured && p.scout?.alive === false && !p.revived;
}

/** The scout saw it alive and the engine has not contacted it yet. */
export function scoutAlive(p: PeerEntry): boolean {
	return !p.measured && p.scout?.alive === true;
}

/** A copy of `scout` if every fact is in range, else undefined (the address stays, without facts). */
function validScout(scout: ScoutFacts | undefined): ScoutFacts | undefined {
	if (!scout || typeof scout !== "object") return undefined;
	const unit = (x: unknown) => typeof x === "number" && Number.isFinite(x) && x >= 0 && x <= 1;
	const flag = (x: unknown) => x === undefined || typeof x === "boolean";
	if (!unit(scout.score) || (scout.have !== undefined && !unit(scout.have))) return undefined;
	if (!flag(scout.alive) || !flag(scout.seed) || !flag(scout.md) || !flag(scout.unchoked)) return undefined;
	if (scout.rtt !== undefined && !(typeof scout.rtt === "number" && Number.isFinite(scout.rtt) && scout.rtt >= 0)) return undefined;
	const out: ScoutFacts = { score: scout.score };
	if (scout.alive !== undefined) out.alive = scout.alive;
	if (scout.seed !== undefined) out.seed = scout.seed;
	if (scout.have !== undefined) out.have = scout.have;
	if (scout.md !== undefined) out.md = scout.md;
	if (scout.unchoked !== undefined) out.unchoked = scout.unchoked;
	if (scout.rtt !== undefined) out.rtt = scout.rtt;
	return out;
}

/**
 * The scout's prior for a peer not contacted yet (docs/architecture.md
 * §Session start step 4): live seeds (30..35) > live partial peers
 * (20..29) > unknown (0) > dead (-30). Below a peer that connected before
 * (40) and one that delivered (100+).
 */
function scoutPriority(p: PeerEntry): number {
	const f = p.scout;
	if (!f || p.measured) return 0;
	if (f.alive === false) return -30;
	if (f.alive !== true) return 0;
	const md = f.md === true ? 1 : 0;
	if (f.seed === true) return 30 + f.score * 4 + md;
	return 20 + (f.have ?? 0) * 4 + f.score * 4 + md;
}

function priority(p: PeerEntry): number {
	let s = scoutPriority(p);
	const delivered = p.delivered + p.remembered;
	if (delivered > 0) s += 100 + Math.min(50, Math.log2(1 + delivered / 65_536));
	if (p.connectedBefore) s += 40;
	if (p.seedHint) s += 8;
	if (p.reachableHint) s += 6;
	if (p.vouched) s += 5;
	if (p.origins.has("pex")) s += 3; // learned from a live peer: likely alive
	if (p.origins.has("magnet")) s += 2;
	s -= p.failures * 10;
	return s;
}

/** Pex addresses to the book, with their flags. */
export function addPex(book: PeerBook, added: readonly { family: 4 | 6; ip: Uint8Array; port: number; flags: number }[]): number {
	return book.addMany(
		added.map((a) => ({ family: a.family, ip: a.ip, port: a.port })),
		"pex",
		added.map((a) => a.flags),
	);
}

/**
 * `ut_pex` as a PeerSource (docs/architecture.md › Peer sources). The
 * session forwards each connection's `pex` event to `ingest`; the source
 * does not own the connection. Stopped, it drops further messages.
 */
export class PexSource implements PeerSource {
	private sink: PeerSink | undefined;

	start(sink: PeerSink): void {
		this.sink = sink;
	}

	stop(): void {
		this.sink = undefined;
	}

	/** How many of `message.added` were new. Dropped addresses are ignored. */
	ingest(message: PexMessage): number {
		if (!this.sink || message.added.length === 0) return 0;
		return this.sink(
			message.added.map((a) => ({ family: a.family, ip: a.ip, port: a.port })),
			"pex",
			message.added.map((a) => a.flags),
		);
	}
}
