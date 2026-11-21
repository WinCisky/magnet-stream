// Block attribution and exclusion of peers that corrupt (docs/architecture.md
// §Chunk recovery › Verification). Every 16 KiB block kept for a piece is
// attributed to the peer that sent it. When a piece fails its check:
//
//   - all its blocks came from one peer: that peer is to blame (a strike);
//   - they came from several peers (a mixed piece): nobody is struck yet.
//     The attempt is remembered (who sent each block, and a digest of each
//     block), the piece is fetched again from a single peer, and once it
//     verifies, the blocks of the failed attempt that differ from the good
//     data name exactly who sent wrong bytes. If the single-peer attempt
//     fails instead, that peer was the sole source: a strike.
//   - v2 torrents (BEP 52) need no isolation: the piece's leaf hashes,
//     checked against the verified piece hash, tell which blocks were wrong
//     (`blame`), and so who sent them.
//
// A strike needs proof: the peer was the only source of a failed piece, or
// a block it sent is shown to differ from verified data. An honest peer
// that shared a piece with a corrupting one is therefore never struck,
// whatever the corrupting peer does. A peer with `strikesToBan` strikes
// (two failed pieces, by default) is excluded for the session: the caller
// closes its connection and bans its address in the PeerBook.
//
// Pure bookkeeping, bounded in memory; the digests are computed by the
// caller (`blockDigests`, WebCrypto, off the page's main thread: the engine
// runs in a dedicated worker).

import { BLOCK_SIZE } from "./metainfo.ts";

export interface AttributionOptions {
	/** Strikes (failed pieces proven to be its fault) before a peer is excluded (default 2). */
	strikesToBan?: number;
	/** Pieces with failed mixed attempts remembered at once (default 256; oldest dropped). */
	maxSuspectPieces?: number;
	/** Failed attempts remembered per piece (default 4; oldest dropped). */
	maxAttemptsPerPiece?: number;
	/** Peers whose strike counts are kept (default 4096; strikes of the oldest not banned are dropped). */
	maxPeers?: number;
}

/** What a failed or verified piece taught. */
export interface Verdict {
	/** Peers given a strike now. */
	struck: string[];
	/** Peers that reached the ban threshold now. */
	banned: string[];
	/** The failed piece had blocks from several peers: fetch it again from a single one. */
	isolate: boolean;
	/** Peers proven to have sent bad data for this piece (do not ask them for it again). */
	culprits: string[];
	/** Failed attempt id, for `setDigests` (mixed failures only). */
	attempt?: number;
}

interface Attempt {
	id: number;
	/** Sender of each block. */
	sources: (string | undefined)[];
	/** Digest of each block as received; undefined until computed. */
	digests: Uint8Array[] | undefined;
}

const none = (): Verdict => ({ struck: [], banned: [], isolate: false, culprits: [] });

export class Attribution {
	private readonly opts: Required<AttributionOptions>;
	/** Sender of each block of the pieces being assembled. */
	private readonly sources = new Map<number, (string | undefined)[]>();
	private readonly strikes = new Map<string, number>();
	private readonly bannedPeers = new Set<string>();
	/** Failed mixed attempts awaiting the verified data, per piece (insertion order: oldest first). */
	private readonly suspects = new Map<number, Attempt[]>();
	private nextAttempt = 1;
	/** Failed pieces, by how the fault was found. */
	readonly counts = { sole: 0, mixed: 0, resolvedByDiff: 0, resolvedByLeaves: 0 };

	constructor(options: AttributionOptions = {}) {
		this.opts = { strikesToBan: 2, maxSuspectPieces: 256, maxAttemptsPerPiece: 4, maxPeers: 4096, ...options };
		this.opts.strikesToBan = Math.max(1, Math.floor(this.opts.strikesToBan));
	}

	/** `peer` sent block `block` (of `blocks`) of `piece`, and it was kept. */
	record(piece: number, block: number, blocks: number, peer: string): void {
		if (!Number.isInteger(block) || block < 0 || block >= blocks) return;
		let s = this.sources.get(piece);
		if (!s || s.length !== blocks) this.sources.set(piece, (s = new Array<string | undefined>(blocks)));
		s[block] = peer;
	}

	/** Senders of a piece's blocks, removed from the record (the piece is complete, or dropped). */
	take(piece: number): (string | undefined)[] {
		const s = this.sources.get(piece) ?? [];
		this.sources.delete(piece);
		return s;
	}

	/** The blocks of a piece were dropped (seek, lost owner): forget who sent them. */
	clear(piece: number): void {
		this.sources.delete(piece);
	}

	/** Forget the blocks `peer` sent for `piece` (they are thrown away); returns their indexes. */
	forget(piece: number, peer: string): number[] {
		const s = this.sources.get(piece);
		if (!s) return [];
		const out: number[] = [];
		s.forEach((who, i) => {
			if (who !== peer) return;
			out.push(i);
			s[i] = undefined;
		});
		return out;
	}

	/** Pieces being assembled with at least one block from `peer`. */
	piecesFrom(peer: string): number[] {
		const out: number[] = [];
		for (const [piece, s] of this.sources) if (s.includes(peer)) out.push(piece);
		return out;
	}

	/**
	 * A piece assembled from `sources` failed its check. One sender: it is
	 * struck. Several: the attempt is kept (its digests come with
	 * `setDigests`) and the piece must be fetched from a single peer.
	 */
	failed(piece: number, sources: readonly (string | undefined)[]): Verdict {
		const senders = distinct(sources);
		if (senders.length === 0) return none();
		if (senders.length === 1) {
			this.counts.sole++;
			const v = none();
			v.culprits.push(senders[0]);
			this.strike(senders[0], v);
			return v;
		}
		this.counts.mixed++;
		const attempt: Attempt = { id: this.nextAttempt++, sources: sources.slice(), digests: undefined };
		let list = this.suspects.get(piece);
		if (list) this.suspects.delete(piece);
		else list = [];
		list.push(attempt);
		while (list.length > this.opts.maxAttemptsPerPiece) list.shift();
		this.suspects.set(piece, list);
		while (this.suspects.size > this.opts.maxSuspectPieces) this.suspects.delete(this.suspects.keys().next().value!);
		return { ...none(), isolate: true, attempt: attempt.id };
	}

	/** The digests of a failed mixed attempt's blocks (as received). */
	setDigests(piece: number, attempt: number, digests: Uint8Array[]): void {
		const a = this.suspects.get(piece)?.find((x) => x.id === attempt);
		if (a && digests.length === a.sources.length) a.digests = digests;
	}

	/** Whether a piece has failed mixed attempts waiting for its verified data. */
	suspected(piece: number): boolean {
		return this.suspects.has(piece);
	}

	/**
	 * The piece verified; `digests` are its blocks' digests. Every failed
	 * attempt is compared with them: whoever sent a block that differs is
	 * struck (once per attempt).
	 */
	passed(piece: number, digests: readonly Uint8Array[]): Verdict {
		const v = none();
		const list = this.suspects.get(piece);
		this.suspects.delete(piece);
		this.sources.delete(piece);
		if (!list) return v;
		for (const a of list) {
			if (!a.digests || a.digests.length !== digests.length) continue;
			const bad = new Set<string>();
			a.digests.forEach((d, i) => {
				const who = a.sources[i];
				if (who !== undefined && !equal(d, digests[i])) bad.add(who);
			});
			if (bad.size > 0) this.counts.resolvedByDiff++;
			for (const peer of bad) {
				if (!v.culprits.includes(peer)) v.culprits.push(peer);
				this.strike(peer, v);
			}
		}
		return v;
	}

	/**
	 * v2: the blocks `bad` of a failed piece assembled from `sources` were
	 * shown wrong by their leaf hashes. Their senders are struck (once each);
	 * the attempt `attempt` (if any) is settled.
	 */
	blame(piece: number, sources: readonly (string | undefined)[], bad: readonly number[], attempt?: number): Verdict {
		const v = none();
		if (attempt !== undefined) {
			const list = this.suspects.get(piece);
			const rest = list?.filter((a) => a.id !== attempt);
			if (rest && rest.length > 0) this.suspects.set(piece, rest);
			else this.suspects.delete(piece);
		}
		const who = new Set<string>();
		for (const b of bad) {
			const peer = Number.isInteger(b) ? sources[b] : undefined;
			if (peer !== undefined) who.add(peer);
		}
		if (who.size > 0) this.counts.resolvedByLeaves++;
		for (const peer of who) {
			v.culprits.push(peer);
			this.strike(peer, v);
		}
		return v;
	}

	strikesOf(peer: string): number {
		return this.strikes.get(peer) ?? 0;
	}

	isBanned(peer: string): boolean {
		return this.bannedPeers.has(peer);
	}

	get banned(): number {
		return this.bannedPeers.size;
	}

	private strike(peer: string, v: Verdict): void {
		if (this.bannedPeers.has(peer)) return;
		const n = (this.strikes.get(peer) ?? 0) + 1;
		this.strikes.delete(peer);
		this.strikes.set(peer, n);
		v.struck.push(peer);
		if (n >= this.opts.strikesToBan) {
			this.bannedPeers.add(peer);
			v.banned.push(peer);
		}
		while (this.strikes.size > this.opts.maxPeers) this.strikes.delete(this.strikes.keys().next().value!);
	}
}

function distinct(sources: readonly (string | undefined)[]): string[] {
	return [...new Set(sources.filter((s): s is string => s !== undefined))];
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}

/** SHA-1 of each 16 KiB block of `data` (WebCrypto: off the calling thread). */
export async function blockDigests(data: Uint8Array): Promise<Uint8Array[]> {
	const out: Promise<ArrayBuffer>[] = [];
	for (let at = 0; at < data.length; at += BLOCK_SIZE) out.push(crypto.subtle.digest("SHA-1", data.subarray(at, at + BLOCK_SIZE) as Uint8Array<ArrayBuffer>));
	return (await Promise.all(out)).map((d) => new Uint8Array(d));
}
