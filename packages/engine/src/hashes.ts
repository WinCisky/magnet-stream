// Fetch v2 piece layers (BEP 52 `hash request` → `hashes`) from peers that
// set the v2 bit, for the files whose pieces we want. Each answer is
// checked up to the file's pieces root (V2Verifier.acceptHashes); a peer
// that rejects, goes silent or sends hashes that don't chain up is not
// asked for that file again, and another peer is.
//
// Like the metadata fetcher it owns no connection: the session forwards
// `hashes`, `hashReject` and `close` events.

import type { MetainfoFile } from "./metainfo.ts";
import type { PeerConnection } from "./peer-connection.ts";
import type { HashRequest } from "./peer-wire.ts";
import { hexOf, type V2Verifier } from "./verify.ts";

interface Job {
	request: HashRequest;
	key: string;
	root: string;
	conn: PeerConnection | undefined;
	timer: ReturnType<typeof setTimeout> | undefined;
}

export interface HashFetcherOptions {
	/** Give up on a peer's answer after this long (default 10 s). */
	timeoutMs?: number;
	/** A file's layer was validated (its pieces can now be verified). */
	onLayer?(file: MetainfoFile): void;
}

const keyOf = (r: HashRequest) => `${hexOf(r.piecesRoot)}:${r.baseLayer}:${r.index}:${r.length}`;

export class HashFetcher {
	readonly failures = { reject: 0, bad_hashes: 0, timeout: 0, closed: 0 };
	private readonly verifier: V2Verifier;
	private readonly opts: HashFetcherOptions;
	private readonly conns = new Set<PeerConnection>();
	private readonly jobs = new Map<string, Job>();
	/** (conn, pieces root) pairs that failed: never retried. */
	private readonly refused = new WeakMap<PeerConnection, Set<string>>();
	private stopped = false;

	constructor(verifier: V2Verifier, options: HashFetcherOptions = {}) {
		this.verifier = verifier;
		this.opts = options;
	}

	/** Ask for the layers of these files (no-op for those known or pending). */
	want(files: Iterable<MetainfoFile>): void {
		for (const file of files) {
			for (const request of this.verifier.layerRequests(file)) {
				const key = keyOf(request);
				if (!this.jobs.has(key)) this.jobs.set(key, { request, key, root: hexOf(request.piecesRoot), conn: undefined, timer: undefined });
			}
		}
		this.assign();
	}

	/** A connection that may answer (it must have set the v2 bit). */
	offer(conn: PeerConnection): void {
		if (!conn.v2 || conn.closed || this.conns.has(conn)) return;
		this.conns.add(conn);
		this.assign();
	}

	get pending(): number {
		return this.jobs.size;
	}

	async onHashes(conn: PeerConnection, request: HashRequest, hashes: Uint8Array): Promise<void> {
		const job = this.jobs.get(keyOf(request));
		if (!job || job.conn !== conn) return;
		clearTimeout(job.timer);
		const ok = await this.verifier.acceptHashes(job.request, hashes);
		if (this.stopped || this.jobs.get(job.key) !== job) return;
		if (!ok) return this.fail(job, "bad_hashes");
		this.jobs.delete(job.key);
		if (this.verifier.missingLayers().every((f) => hexOf(f.piecesRoot!) !== job.root)) {
			const file = this.verifier.meta.files.find((f) => f.piecesRoot && hexOf(f.piecesRoot) === job.root);
			if (file) this.opts.onLayer?.(file);
		}
		this.assign();
	}

	onReject(conn: PeerConnection, request: HashRequest): void {
		const job = this.jobs.get(keyOf(request));
		if (job && job.conn === conn) this.fail(job, "reject");
	}

	onClose(conn: PeerConnection): void {
		this.conns.delete(conn);
		for (const job of this.jobs.values()) if (job.conn === conn) this.fail(job, "closed");
	}

	stop(): void {
		this.stopped = true;
		for (const job of this.jobs.values()) clearTimeout(job.timer);
		this.jobs.clear();
	}

	private fail(job: Job, why: keyof HashFetcher["failures"]): void {
		clearTimeout(job.timer);
		this.failures[why]++;
		if (job.conn) {
			let set = this.refused.get(job.conn);
			if (!set) this.refused.set(job.conn, (set = new Set()));
			set.add(job.root);
		}
		job.conn = undefined;
		this.assign();
	}

	private assign(): void {
		if (this.stopped) return;
		for (const job of this.jobs.values()) {
			if (job.conn) continue;
			const conn = [...this.conns].find((c) => !c.closed && !this.refused.get(c)?.has(job.root));
			if (!conn) continue;
			job.conn = conn;
			job.timer = setTimeout(() => this.fail(job, "timeout"), this.opts.timeoutMs ?? 10_000);
			conn.requestHashes(job.request);
		}
	}
}
