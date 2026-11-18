// A batch of blocks the scheduler asks the tunnel session for. The session
// fetches them from peers it already holds open.

import type { Preamble } from "@magnet-stream/engine/blocks-stream";
import type { Peer } from "./swarm";

export interface BatchPlan {
	id: number;
	/** [piece, block] pairs in request order. */
	blocks: Array<[number, number]>;
	/** Candidates whose connection slot the request holds (lead, race). */
	peers: Peer[];
	/** Extra candidates. The tunnel session ignores these and uses its own peers. */
	fallbacks: Peer[];
	urgent: boolean;
	/** How long to wait for an unchoke. */
	waitMs: number;
	/** How long a handshaken but choked candidate may keep its slot while others wait. */
	holdMs: number;
	/** How long the lead is tried alone before the fallbacks join in. */
	staggerMs: number;
}

export interface BatchSink {
	started(preamble: Preamble, sent: Array<[number, number]>): void;
	/**
	 * Returns true when the batch has nothing left to wait for. `data` is
	 * only valid during the call.
	 */
	block(piece: number, begin: number, data: Uint8Array): boolean;
	/** Returns true when the batch has nothing left to wait for. */
	rejected(piece: number, begin: number): boolean;
	have(piece: number): void;
}

export type BatchEnd = { bytes: number } & (
	| { kind: "done" }
	| { kind: "choked" }
	| { kind: "stalled" }
	| { kind: "ended" }
	| { kind: "aborted" }
	| { kind: "no_peer"; others: { peer: string; err?: string; hs?: number }[] }
	| { kind: "refused"; status: number; message: string }
	| { kind: "error"; message: string }
);

export type Transport = (plan: BatchPlan, sink: BatchSink, signal: AbortSignal) => Promise<BatchEnd>;
