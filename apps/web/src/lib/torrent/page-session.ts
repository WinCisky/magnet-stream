// The page's handle on the session worker: one lookup, then the block
// transport the existing scheduler already speaks. Several recoveries can
// share the session (the video, a subtitle file): each one holds it with
// `acquire()`. When the last one completes, the WebSockets close at once;
// when the last one is stopped (the user switched file), they close after a
// short grace so a quick switch keeps its peers. A later want reopens them.

import { blockLength, type Metainfo } from "@magnet-stream/engine/metainfo";
import { SEEDERS_API_URL, WORKER_API_URL, type TorrentFile, type TorrentMetadata } from "$lib/magnet/api";
import type { BatchPlan, BatchSink, Transport } from "./batch";
import SessionWorker from "./session.worker.ts?worker&inline";
import { parseWorkerMessage, type PageMessage, type SessionListing } from "./session-protocol.ts";
import { Swarm } from "./swarm";

export interface LookupResult {
	metadata: TorrentMetadata;
	meta: Metainfo;
	seeders: number;
}

/** Sockets stay open this long after the last recovery stopped without completing. */
export const IDLE_GRACE_MS = 15_000;

export class PageSession {
	private readonly worker: Worker;
	private readonly wants = new Map<number, { onBlock(piece: number, begin: number, data: Uint8Array): void; done(received: number, missing: number): void }>();
	private nextId = 1;
	private peerCount = 0;
	private files: { resolve(result: { listing: SessionListing; info: Uint8Array }): void; reject(error: Error): void } | undefined;
	private readonly filesReady: Promise<{ listing: SessionListing; info: Uint8Array }>;
	private closed = false;
	private holders = 0;
	private stats = { upgrades: 0, messages: 0 };
	private peers = { known: 0, delivered: 0 };
	private baselineTaken = false;
	private idleTimer: ReturnType<typeof setTimeout> | undefined;

	constructor() {
		this.worker = new SessionWorker();
		this.filesReady = new Promise((resolve, reject) => {
			this.files = { resolve, reject };
		});
		this.worker.onmessage = (event: MessageEvent) => this.onMessage(event.data);
		this.worker.onerror = (event) => {
			const err = event as ErrorEvent;
			this.files?.reject(new Error(err.message || "session worker failed"));
		};
	}

	/** Parse the magnet in the worker, open the tunnel, and return the file list. One /peers request. */
	lookup(magnet: string, urls: { gateway?: string; seeders?: string } = {}): Promise<LookupResult> {
		this.post({
			type: "start",
			magnet,
			gatewayUrl: urls.gateway ?? WORKER_API_URL,
			seedersUrl: urls.seeders ?? SEEDERS_API_URL,
		});
		return this.filesReady.then(async ({ listing, info }) => {
			const { parseInfoDict } = await import("@magnet-stream/engine/metainfo");
			const meta = await parseInfoDict(info, listing.infoHash);
			return {
				meta,
				seeders: this.peerCount,
				metadata: { info_hash: listing.infoHash, name: listing.name, size: listing.size, files: listing.files },
			};
		});
	}

	/** Scheduler transport: the blocks it asks for, fetched over the tunnel. */
	transport(meta: Metainfo): Transport {
		return (plan, sink, signal) => servePlan(this, meta, plan, sink, signal);
	}

	/** Blocks from the tunnel. Used by the scheduler transport. */
	request(
		blocks: { piece: number; begin: number; length: number }[],
		onBlock: (piece: number, begin: number, data: Uint8Array) => void,
		signal: AbortSignal,
	): Promise<{ received: number; missing: number }> {
		return this.want(blocks, onBlock, signal);
	}

	/** A swarm of one always-available peer, so the scheduler plans head and tail without /swarm. */
	swarm(): Swarm {
		const now = Date.now();
		return new Swarm(
			async () => ({
				info_hash: "tunnel",
				announced_at: now,
				probed_at: now,
				complete: true,
				counts: { known: 1, probed: 1, reachable: 1, seeds: 1, unchoked: 1 },
				peers: [{ addr: "tunnel", ok: true, seed: true, fast: true, reqq: 250, unchoke_ms: 50, rtt_ms: 1 }],
			}),
			() => {},
		);
	}

	/**
	 * Hold the session for one recovery. The returned release says whether
	 * that recovery completed (sockets close now) or stopped (after a grace).
	 */
	acquire(): (completed: boolean) => void {
		this.holders++;
		this.clearIdle();
		let released = false;
		return (completed) => {
			if (released) return;
			released = true;
			this.holders--;
			if (this.holders > 0 || this.closed) return;
			this.clearIdle();
			if (completed) this.post({ type: "idle" });
			else this.idleTimer = setTimeout(() => this.post({ type: "idle" }), IDLE_GRACE_MS);
		};
	}

	/** Upgrades attempted and messages sent by this session's tunnel so far. */
	get tunnelStats(): { upgrades: number; messages: number } {
		return { ...this.stats };
	}

	/** Peers the session knows, and how many delivered data. */
	get peerStats(): { known: number; delivered: number } {
		return { ...this.peers };
	}

	/**
	 * Where a recovery starts counting the tunnel's cost: the first one is
	 * charged from zero (the lookup's upgrade and messages are its own),
	 * later ones (a subtitle file, another file) from the current totals.
	 */
	baseline(): { upgrades: number; messages: number } {
		if (this.baselineTaken) return this.tunnelStats;
		this.baselineTaken = true;
		return { upgrades: 0, messages: 0 };
	}

	private clearIdle(): void {
		if (this.idleTimer !== undefined) clearTimeout(this.idleTimer);
		this.idleTimer = undefined;
	}

	/** Drop the session (leaving the page). */
	close(): void {
		if (this.closed) return;
		this.clearIdle();
		this.post({ type: "stop" });
		this.closed = true;
		this.worker.terminate();
		this.files?.reject(new Error("session closed"));
		for (const want of this.wants.values()) want.done(0, 1);
		this.wants.clear();
	}

	private post(message: PageMessage): void {
		if (!this.closed) this.worker.postMessage(message);
	}

	private onMessage(data: unknown): void {
		const msg = parseWorkerMessage(data);
		if (!msg) return;
		if (msg.type === "peers") this.peerCount = msg.count;
		else if (msg.type === "stats") {
			this.stats = { upgrades: msg.upgrades, messages: msg.messages };
			this.peers = { known: msg.known, delivered: msg.delivered };
		}
		else if (msg.type === "error") this.files?.reject(new Error(msg.message));
		else if (msg.type === "files") this.files?.resolve({ listing: msg.listing, info: new Uint8Array(msg.info) });
		else if (msg.type === "block") this.wants.get(msg.id)?.onBlock(msg.piece, msg.begin, new Uint8Array(msg.data));
		else if (msg.type === "want-done") {
			const want = this.wants.get(msg.id);
			this.wants.delete(msg.id);
			want?.done(msg.received, msg.missing);
		}
	}

	private want(
		blocks: { piece: number; begin: number; length: number }[],
		onBlock: (piece: number, begin: number, data: Uint8Array) => void,
		signal: AbortSignal,
	): Promise<{ received: number; missing: number }> {
		const id = this.nextId++;
		return new Promise((resolve) => {
			let done = false;
			const finish = (received: number, missing: number) => {
				if (done) return;
				done = true;
				this.wants.delete(id);
				signal.removeEventListener("abort", abort);
				resolve({ received, missing });
			};
			// The worker cancels the blocks still outstanding (cancel messages to the peers).
			const abort = () => {
				this.post({ type: "cancel", id });
				finish(0, blocks.length);
			};
			if (signal.aborted) {
				resolve({ received: 0, missing: blocks.length });
				return;
			}
			this.wants.set(id, { onBlock, done: finish });
			signal.addEventListener("abort", abort, { once: true });
			this.post({ type: "want", id, blocks });
		});
	}
}

async function servePlan(session: PageSession, meta: Metainfo, plan: BatchPlan, sink: BatchSink, signal: AbortSignal): Promise<import("./batch").BatchEnd> {
	const blocks = plan.blocks.map(([piece, block]) => ({ piece, begin: block * 16 * 1024, length: blockLength(meta, piece, block) }));
	let bytes = 0;
	let finished = false;
	const controller = new AbortController();
	const abort = () => controller.abort();
	signal.addEventListener("abort", abort, { once: true });
	const end = await session.request(blocks, (piece, begin, data) => {
		bytes += data.length;
		if (sink.block(piece, begin, data)) {
			finished = true;
			controller.abort();
		}
	}, controller.signal);
	signal.removeEventListener("abort", abort);
	if (signal.aborted) return { kind: "aborted", bytes };
	if (finished || end.missing === 0) return { kind: "done", bytes };
	if (end.received === 0) return { kind: "no_peer", others: [], bytes };
	return { kind: "ended", bytes };
}

export type { TorrentFile };
