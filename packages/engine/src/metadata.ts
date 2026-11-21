// Fetch the info dict from peers with ut_metadata (BEP 9): ask the first
// peers that announce it for its 16 KiB pieces, reassemble, and accept it
// only if its hash matches the magnet's info-hash (SHA-1 for v1, SHA-256
// for v2, both for a hybrid when both are known). A peer that rejects, goes
// silent, hangs up or serves a dict that doesn't hash right is dropped from
// the candidates and the next one is asked.
//
// The fetcher does not own connections: the session forwards each
// connection's `extensions`, `metadata` and `close` events to it. An info
// dict that arrives from elsewhere (the VPS's `/peers` stream, a cache) is
// handed to `offerBytes` and goes through the same hash check: the first
// verified one, from wherever, ends the search.

import { METADATA_PIECE_SIZE, encodeMetadataRequest, type MetadataMessage } from "./extension.ts";
import { MetainfoError, parseInfoDict, type ExpectedHashes, type Metainfo, type MetainfoLimits } from "./metainfo.ts";
import type { PeerConnection } from "./peer-connection.ts";

export const DEFAULT_MAX_METADATA_SIZE = 16 * 1024 * 1024;

export interface MetadataOptions {
	expected: ExpectedHashes;
	/** Largest `metadata_size` accepted (default 16 MiB). */
	maxSize?: number;
	/** Give up on a peer after this long without a piece (default 5 s). */
	pieceTimeoutMs?: number;
	/** Peers asked at once (default 4). */
	parallel?: number;
	limits?: MetainfoLimits;
}

export type MetadataFailure = "too_large" | "reject" | "timeout" | "closed" | "bad_data" | "hash_mismatch" | "external_mismatch";

/** Where the verified info dict came from: a peer (BEP 9) or `offerBytes`. */
export type MetadataSource = "peer" | "external";

export interface MetadataResult {
	meta: Metainfo;
	/** The verified info dict. */
	bytes: Uint8Array;
	source: MetadataSource;
}

interface Attempt {
	conn: PeerConnection;
	size: number;
	buffer: Uint8Array;
	pending: Set<number>;
	timer: ReturnType<typeof setTimeout> | undefined;
}

export class MetadataFetcher {
	readonly done: Promise<MetadataResult>;
	readonly failures: Record<MetadataFailure, number> = { too_large: 0, reject: 0, timeout: 0, closed: 0, bad_data: 0, hash_mismatch: 0, external_mismatch: 0 };
	private readonly opts: Required<Omit<MetadataOptions, "limits">> & Pick<MetadataOptions, "limits">;
	private readonly candidates: PeerConnection[] = [];
	private readonly attempts = new Map<PeerConnection, Attempt>();
	private readonly tried = new WeakSet<PeerConnection>();
	private resolve!: (r: MetadataResult) => void;
	private reject!: (e: Error) => void;
	private finished = false;

	constructor(options: MetadataOptions) {
		this.opts = { maxSize: DEFAULT_MAX_METADATA_SIZE, pieceTimeoutMs: 5_000, parallel: 4, ...options };
		this.done = new Promise((resolve, reject) => {
			this.resolve = resolve;
			this.reject = reject;
		});
		// Callers may never await it (session stopped first).
		this.done.catch(() => {});
	}

	get isDone(): boolean {
		return this.finished;
	}

	/** Peers offering the metadata still to ask, or being asked now (0 once done). */
	get pending(): number {
		return this.finished ? 0 : this.candidates.length + this.attempts.size;
	}

	/** A connection's extended handshake arrived: a candidate if it offers the metadata. */
	offer(conn: PeerConnection): void {
		if (this.finished || this.tried.has(conn) || conn.closed) return;
		const size = conn.remoteExtensions?.metadataSize;
		if (!conn.supports("ut_metadata") || size === undefined) return;
		if (size > this.opts.maxSize) {
			this.tried.add(conn);
			this.failures.too_large++;
			return;
		}
		this.candidates.push(conn);
		this.next();
	}

	/**
	 * An info dict from outside the swarm (the VPS's `info` line, a cache).
	 * Checked like one from a peer; true if it was accepted and ended the
	 * search. A wrong one is counted and dropped, and the peers carry on.
	 */
	async offerBytes(bytes: Uint8Array): Promise<boolean> {
		if (this.finished) return false;
		if (!(bytes instanceof Uint8Array) || bytes.length === 0 || bytes.length > this.opts.maxSize) {
			this.failures.external_mismatch++;
			return false;
		}
		const copy = bytes.slice();
		let meta: Metainfo;
		try {
			meta = await parseInfoDict(copy, this.opts.expected, this.opts.limits);
		} catch {
			this.failures.external_mismatch++;
			return false;
		}
		return this.succeed(meta, copy, "external");
	}

	/** A ut_metadata message from `conn`. */
	onMessage(conn: PeerConnection, msg: MetadataMessage): void {
		const a = this.attempts.get(conn);
		if (!a || this.finished) return;
		if (msg.type === "reject") return this.fail(a, "reject");
		if (msg.type !== "data") return;
		const expected = Math.min(METADATA_PIECE_SIZE, a.size - msg.piece * METADATA_PIECE_SIZE);
		if (msg.totalSize !== a.size || !a.pending.has(msg.piece) || msg.data.length !== expected) return this.fail(a, "bad_data");
		a.buffer.set(msg.data, msg.piece * METADATA_PIECE_SIZE);
		a.pending.delete(msg.piece);
		this.arm(a);
		if (a.pending.size === 0) void this.check(a);
	}

	/** `conn` closed. */
	onClose(conn: PeerConnection): void {
		const a = this.attempts.get(conn);
		if (a) this.fail(a, "closed");
		const i = this.candidates.indexOf(conn);
		if (i >= 0) this.candidates.splice(i, 1);
	}

	stop(): void {
		if (this.finished) return;
		this.finished = true;
		for (const a of this.attempts.values()) clearTimeout(a.timer);
		this.attempts.clear();
		this.reject(new Error("stopped"));
	}

	private next(): void {
		while (!this.finished && this.attempts.size < this.opts.parallel && this.candidates.length > 0) {
			const conn = this.candidates.shift()!;
			if (conn.closed) continue;
			this.tried.add(conn);
			const size = conn.remoteExtensions!.metadataSize!;
			const pieces = Math.ceil(size / METADATA_PIECE_SIZE);
			const a: Attempt = { conn, size, buffer: new Uint8Array(size), pending: new Set(Array.from({ length: pieces }, (_, i) => i)), timer: undefined };
			this.attempts.set(conn, a);
			this.arm(a);
			for (let p = 0; p < pieces; p++) conn.sendExtended("ut_metadata", encodeMetadataRequest(p));
		}
	}

	private arm(a: Attempt): void {
		clearTimeout(a.timer);
		a.timer = setTimeout(() => this.fail(a, "timeout"), this.opts.pieceTimeoutMs);
	}

	private fail(a: Attempt, why: MetadataFailure): void {
		if (this.attempts.get(a.conn) !== a) return;
		clearTimeout(a.timer);
		this.attempts.delete(a.conn);
		this.failures[why]++;
		this.next();
	}

	private async check(a: Attempt): Promise<void> {
		clearTimeout(a.timer);
		let meta: Metainfo;
		try {
			meta = await parseInfoDict(a.buffer, this.opts.expected, this.opts.limits);
		} catch (e) {
			if (!(e instanceof MetainfoError)) throw e;
			return this.fail(a, "hash_mismatch");
		}
		this.succeed(meta, a.buffer, "peer");
	}

	private succeed(meta: Metainfo, bytes: Uint8Array, source: MetadataSource): boolean {
		if (this.finished) return false;
		this.finished = true;
		for (const other of this.attempts.values()) clearTimeout(other.timer);
		this.attempts.clear();
		this.candidates.length = 0;
		this.resolve({ meta, bytes, source });
		return true;
	}
}
