// Download every piece of a torrent from one peer, with the metadata
// already known, verifying each piece (SHA-1, or SHA-256 merkle trees for
// v2: the piece layers are fetched from the same peer first) before handing
// it over. A piece that fails verification is discarded and reported, never
// delivered (docs/architecture.md §Chunk recovery › Verification).

import { HashFetcher } from "./hashes.ts";
import { BLOCK_SIZE, geometryOf, type Metainfo } from "./metainfo.ts";
import type { PeerConnection } from "./peer-connection.ts";
import { pieceSizeOf } from "./peer-wire.ts";
import { V2Verifier, verifierFor, type PieceVerifier } from "./verify.ts";

export interface DownloadOptions {
	/** Block requests kept in flight (default 64); refilled in batches when half are answered. */
	pipeline?: number;
	/** Give up after this long without any progress (default 30 s). */
	stallTimeoutMs?: number;
	/** Re-request a piece that failed verification this many times (default 1). */
	retries?: number;
	/** Only these pieces (default: all). */
	pieces?: readonly number[];
	signal?: AbortSignal;
	/** How pieces are verified (default: for the torrent's version). */
	verifier?: PieceVerifier;
	/** A verified piece. */
	onPiece?(piece: number, data: Uint8Array): void;
	/** A piece that failed SHA-1 verification (its data is dropped). */
	onCorrupt?(piece: number): void;
}

export interface DownloadResult {
	/** Pieces verified and delivered. */
	verified: number[];
	/** Pieces that kept failing verification. */
	corrupt: number[];
	/** Pieces the peer doesn't have (or kept rejecting). */
	missing: number[];
	/** Payload bytes received (including discarded ones). */
	bytes: number;
	ms: number;
}

export class DownloadError extends Error {
	readonly result: DownloadResult;
	constructor(message: string, result: DownloadResult, options?: { cause?: unknown }) {
		super(message, options);
		this.name = "DownloadError";
		this.result = result;
	}
}

type Block = [piece: number, begin: number, length: number];

interface PieceWork {
	buffer: Uint8Array;
	received: number;
	attempts: number;
}

const key = (piece: number, begin: number) => `${piece}:${begin}`;

export function downloadAll(conn: PeerConnection, meta: Metainfo, opts: DownloadOptions = {}): Promise<DownloadResult> {
	const pipeline = opts.pipeline ?? 64;
	const stallMs = opts.stallTimeoutMs ?? 30_000;
	const retries = opts.retries ?? 1;
	const started = Date.now();
	const geometry = geometryOf(meta);
	if (!conn.geometry) conn.setGeometry(geometry);
	const verifier = opts.verifier ?? verifierFor(meta);
	const wanted = [...new Set(opts.pieces ?? Array.from({ length: meta.pieceCount }, (_, i) => i))].filter(
		(p) => Number.isInteger(p) && p >= 0 && p < meta.pieceCount,
	);
	const result: DownloadResult = { verified: [], corrupt: [], missing: [], bytes: 0, ms: 0 };

	const blocksOf = (piece: number): Block[] => {
		const size = pieceSizeOf(geometry, piece);
		const out: Block[] = [];
		for (let begin = 0; begin < size; begin += BLOCK_SIZE) out.push([piece, begin, Math.min(BLOCK_SIZE, size - begin)]);
		return out;
	};

	return new Promise<DownloadResult>((resolve, reject) => {
		/** Blocks still to request. */
		let queue: Block[] = [];
		/** Blocks requested and not yet answered. */
		const inflight = new Map<string, Block>();
		const work = new Map<number, PieceWork>();
		const rejects = new Map<number, number>();
		let verifying = 0;
		let settled = false;
		let planned = false;
		let stallTimer: ReturnType<typeof setTimeout> | undefined;
		/** The connection ended; finish once pending verifications are done. */
		let gone: Error | undefined;

		const finish = (error?: Error) => {
			if (settled) return;
			settled = true;
			clearTimeout(stallTimer);
			hashes?.stop();
			opts.signal?.removeEventListener("abort", onAbort);
			conn.events = {};
			result.ms = Date.now() - started;
			result.verified.sort((a, b) => a - b);
			if (error) reject(new DownloadError(error.message, result, { cause: error }));
			else resolve(result);
		};
		const progress = () => {
			clearTimeout(stallTimer);
			stallTimer = setTimeout(() => {
				finish(new Error(`no progress for ${stallMs} ms`));
				conn.close();
			}, stallMs);
		};
		const complete = () => planned && verifying === 0 && result.verified.length + result.corrupt.length + result.missing.length === wanted.length;
		const maybeFinish = () => {
			if (complete()) finish();
			else if (gone && verifying === 0) finish(gone);
		};
		// v2: the piece layers of the wanted files come first.
		let hashes: HashFetcher | undefined;
		if (verifier instanceof V2Verifier && verifier.missingLayers(wanted).length > 0) {
			hashes = new HashFetcher(verifier, {
				onLayer: () => {
					if (verifier.missingLayers(wanted).length > 0) return;
					hashes = undefined;
					progress();
					if (!conn.peerChoking) {
						plan();
						fill();
					}
				},
			});
			hashes.want(verifier.missingLayers(wanted));
			hashes.offer(conn);
		}
		const plan = () => {
			if (planned || hashes) return;
			planned = true;
			for (const piece of wanted) {
				if (conn.has(piece) && verifier.canVerify(piece)) queue.push(...blocksOf(piece));
				else result.missing.push(piece);
			}
			maybeFinish();
		};
		const fill = () => {
			if (settled || conn.peerChoking || conn.closed) return;
			const room = pipeline - inflight.size;
			if (room <= 0 || queue.length === 0) return;
			const batch = queue.splice(0, room);
			for (const b of batch) inflight.set(key(b[0], b[1]), b);
			conn.request(batch);
		};
		const giveUp = (piece: number) => {
			if (result.missing.includes(piece)) return;
			result.missing.push(piece);
			queue = queue.filter(([p]) => p !== piece);
			for (const [k, [p]] of inflight) if (p === piece) inflight.delete(k);
			work.delete(piece);
		};
		const onAbort = () => {
			finish(new Error("aborted"));
			conn.close();
		};

		const verify = async (piece: number, w: PieceWork) => {
			verifying++;
			let ok: boolean;
			try {
				ok = await verifier.verify(piece, w.buffer);
			} catch {
				ok = false;
			}
			verifying--;
			if (settled) return;
			work.delete(piece);
			if (ok) {
				result.verified.push(piece);
				opts.onPiece?.(piece, w.buffer);
			} else {
				opts.onCorrupt?.(piece);
				if (w.attempts >= retries) {
					result.corrupt.push(piece);
				} else {
					// Drop the bytes and ask again.
					work.set(piece, { buffer: new Uint8Array(w.buffer.length), received: 0, attempts: w.attempts + 1 });
					queue.push(...blocksOf(piece));
					fill();
				}
			}
			maybeFinish();
		};

		conn.events = {
			unchoke: () => {
				progress();
				plan();
				fill();
			},
			choke: () => {
				// Without the fast extension the peer silently dropped every
				// request in flight: ask again after the next unchoke. (With it,
				// a reject arrives for each.)
				if (!conn.fast) {
					queue = [...inflight.values(), ...queue];
					inflight.clear();
				}
			},
			block: (piece, begin, data) => {
				if (!inflight.delete(key(piece, begin))) return;
				progress();
				result.bytes += data.length;
				let w = work.get(piece);
				if (!w) {
					w = { buffer: new Uint8Array(pieceSizeOf(geometry, piece)), received: 0, attempts: 0 };
					work.set(piece, w);
				}
				w.buffer.set(data, begin);
				w.received += data.length;
				if (w.received === w.buffer.length) void verify(piece, w);
				// Refill in batches (one write, so one tunnel message, per half
				// pipeline) rather than one request per block received.
				if (inflight.size <= pipeline / 2) fill();
			},
			reject: (piece, begin, length) => {
				if (!inflight.delete(key(piece, begin))) return;
				const n = (rejects.get(piece) ?? 0) + 1;
				rejects.set(piece, n);
				// Rejected while unchoked, again and again: the peer won't serve it.
				if (!conn.peerChoking && n > 3 * blocksOf(piece).length) {
					giveUp(piece);
					maybeFinish();
					return;
				}
				queue.unshift([piece, begin, length]);
				fill();
			},
			hashes: (request, data) => void hashes?.onHashes(conn, request, data),
			hashReject: (request) => {
				hashes?.onReject(conn, request);
				// The only peer won't give the layers: nothing can be verified.
				if (hashes && hashes.pending > 0) {
					finish(new Error("the peer refused the v2 piece layers"));
					conn.close();
				}
			},
			close: (error) => {
				gone = error ?? new Error("the peer closed the connection");
				clearTimeout(stallTimer);
				maybeFinish();
			},
		};

		if (opts.signal?.aborted) return onAbort();
		opts.signal?.addEventListener("abort", onAbort, { once: true });
		if (conn.closed) return finish(new Error("the peer connection is closed"));
		progress();
		conn.interested();
		if (!conn.peerChoking) {
			plan();
			fill();
		}
	});
}
