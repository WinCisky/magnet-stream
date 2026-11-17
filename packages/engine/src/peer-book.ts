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

import { checkAddress, formatAddress, parseAddress, type PeerAddress } from "@magnet-stream/tunnel-protocol";

export type PeerOrigin = "discovery" | "pex" | "magnet" | "manual";

export type PeerState = "new" | "connecting" | "connected" | "failed";

/** Why a dial or a connection ended badly (FailReason / CloseReason names, or the engine's own). */
export type PeerFailure = string;

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
	addedAt: number;
	/** Banned for the session (corrupt data, wrong torrent). */
	banned: boolean;
}

export interface PeerSink {
	/** New addresses from a source; returns how many were accepted. */
	(addresses: readonly (string | PeerAddress)[], origin: PeerOrigin): number;
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
};
const DEFAULT_BACKOFF = 60_000;
const MAX_BACKOFF = 30 * 60_000;

export class PeerBook {
	private readonly peers = new Map<string, PeerEntry>();
	private readonly opts: Required<PeerBookOptions>;
	/** Addresses refused: invalid, forbidden, over a limit (counters only). */
	readonly rejected = { invalid: 0, forbidden: 0, full: 0, batch: 0 };

	constructor(options: PeerBookOptions = {}) {
		this.opts = { maxPeers: 3000, maxPerBatch: 100, allowPrivate: false, now: Date.now, ...options };
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
	readonly sink: PeerSink = (addresses, origin) => this.addMany(addresses, origin);

	addMany(addresses: readonly (string | PeerAddress)[], origin: PeerOrigin, flags?: readonly number[]): number {
		let added = 0;
		const n = Math.min(addresses.length, this.opts.maxPerBatch);
		this.rejected.batch += addresses.length - n;
		for (let i = 0; i < n; i++) if (this.add(addresses[i], origin, flags?.[i] ?? 0)) added++;
		return added;
	}

	/** Validate and keep one address; true if it is new. */
	add(address: string | PeerAddress, origin: PeerOrigin, flags = 0): boolean {
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
		const known = this.peers.get(key);
		if (known) {
			known.origins.add(origin);
			if (flags & 0x02) known.seedHint = true;
			if (flags & 0x10) known.reachableHint = true;
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
			addedAt: this.opts.now(),
			banned: false,
		});
		return true;
	}

	/** Make room by forgetting a peer that failed for good or repeatedly (never one that worked). */
	private evict(): boolean {
		let worst: PeerEntry | undefined;
		for (const p of this.peers.values()) {
			if (p.state === "connecting" || p.state === "connected" || p.connectedBefore) continue;
			if (p.failures === 0 && !p.banned) continue;
			if (!worst || p.banned || p.failures > worst.failures) worst = p;
		}
		if (!worst) return false;
		this.peers.delete(worst.key);
		return true;
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

	markConnecting(key: string): void {
		const p = this.peers.get(key);
		if (p) p.state = "connecting";
	}

	markConnected(key: string): void {
		const p = this.peers.get(key);
		if (!p) return;
		p.state = "connected";
		p.connectedBefore = true;
	}

	/** The dial failed, or the connection ended for `reason`. */
	markFailed(key: string, reason: PeerFailure): void {
		const p = this.peers.get(key);
		if (!p) return;
		p.state = "failed";
		p.failures++;
		p.lastFailure = reason;
		const base = BACKOFF[reason] ?? DEFAULT_BACKOFF;
		if (base === Infinity) {
			p.banned = true;
			return;
		}
		// A peer that served us is worth coming back to soon.
		const scaled = p.delivered > 0 ? Math.min(base, 15_000) : base;
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

	/** Never again this session (corrupt data, protocol abuse). */
	ban(key: string): void {
		const p = this.peers.get(key);
		if (p) p.banned = true;
	}

	/** The best peers we know worked, to redial after the gateway dropped (most delivered first). */
	best(n: number): PeerEntry[] {
		return [...this.peers.values()]
			.filter((p) => !p.banned && p.connectedBefore)
			.sort((a, b) => b.delivered - a.delivered)
			.slice(0, n);
	}

	counts(): { known: number; untried: number; connected: number; failed: number; banned: number; delivered: number } {
		const c = { known: this.peers.size, untried: 0, connected: 0, failed: 0, banned: 0, delivered: 0 };
		for (const p of this.peers.values()) {
			if (p.banned) c.banned++;
			else if (p.state === "connected") c.connected++;
			else if (p.state === "failed") c.failed++;
			else if (p.state === "new") c.untried++;
			if (p.delivered > 0) c.delivered++;
		}
		return c;
	}
}

function priority(p: PeerEntry): number {
	let s = 0;
	if (p.delivered > 0) s += 100 + Math.min(50, Math.log2(1 + p.delivered / 65_536));
	if (p.connectedBefore) s += 40;
	if (p.seedHint) s += 8;
	if (p.reachableHint) s += 6;
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
