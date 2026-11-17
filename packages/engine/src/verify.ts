// Piece verification behind one interface, for every torrent version:
//   - v1 and hybrid: SHA-1 of the whole piece against the info dict's
//     `pieces` (a hybrid's v2 hashes describe the same bytes);
//   - v2-only (BEP 52): SHA-256 merkle trees. Each file has a tree over its
//     16 KiB blocks (leaves), padded with zero leaves to a power of two; its
//     root is the file's `pieces root`, from the verified info dict. A
//     piece's hash is the root of its subtree, read from the file's "piece
//     layer", which peers send in `hashes` messages and which is accepted
//     only if it hashes up to the pieces root. A file no larger than one
//     piece needs no layer: its piece hash is the pieces root itself.
// Leaf hashes (for one piece) can also be checked against the piece hash,
// which tells which 16 KiB blocks of a failed piece were wrong.
//
// Nothing v2 is trusted without a chain of hashes up to a verified pieces
// root.

import { BLOCK_SIZE, pieceSize, verifyPiece, type Metainfo, type MetainfoFile } from "./metainfo.ts";
import type { HashRequest } from "./peer-wire.ts";

export interface PieceVerifier {
	/** Whether `piece` can be verified now (v2: its file's piece layer is known). */
	canVerify(piece: number): boolean;
	verify(piece: number, data: Uint8Array): Promise<boolean>;
}

const HASH = 32;
/** Most hashes asked for in one request (BEP 52 / libtorrent limit). */
export const MAX_HASHES_PER_REQUEST = 512;

export async function sha256(data: Uint8Array): Promise<Uint8Array> {
	return new Uint8Array(await crypto.subtle.digest("SHA-256", data as Uint8Array<ArrayBuffer>));
}

async function pair(a: Uint8Array, b: Uint8Array): Promise<Uint8Array> {
	const both = new Uint8Array(64);
	both.set(a, 0);
	both.set(b, 32);
	return sha256(both);
}

export function nextPow2(n: number): number {
	let p = 1;
	while (p < n) p *= 2;
	return p;
}

const zeroRoots: Promise<Uint8Array>[] = [Promise.resolve(new Uint8Array(HASH))];
/** Root of a perfect subtree of 2^level zero leaves. */
export function zeroRoot(level: number): Promise<Uint8Array> {
	for (let l = zeroRoots.length; l <= level; l++) {
		const below = zeroRoots[l - 1];
		zeroRoots.push(below.then((h) => pair(h, h)));
	}
	return zeroRoots[level];
}

/**
 * Root of a tree whose bottom layer is `nodes` (at `level` above the
 * leaves), padded to `width` nodes with zero subtrees.
 */
export async function merkleRoot(nodes: Uint8Array[], width: number, level = 0): Promise<Uint8Array> {
	let layer = nodes.slice();
	const pad = await zeroRoot(level);
	while (layer.length < width) layer.push(pad);
	while (layer.length > 1) {
		const up: Promise<Uint8Array>[] = [];
		for (let i = 0; i < layer.length; i += 2) up.push(pair(layer[i], layer[i + 1]));
		layer = await Promise.all(up);
	}
	return layer[0];
}

/** SHA-256 of each 16 KiB block of `data`. */
export async function leafHashes(data: Uint8Array): Promise<Uint8Array[]> {
	const out: Promise<Uint8Array>[] = [];
	for (let at = 0; at < Math.max(data.length, 1); at += BLOCK_SIZE) out.push(sha256(data.subarray(at, at + BLOCK_SIZE)));
	return Promise.all(out);
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}

export function hexOf(b: Uint8Array): string {
	return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}

export class V1Verifier implements PieceVerifier {
	private readonly meta: Metainfo;
	constructor(meta: Metainfo) {
		this.meta = meta;
	}
	canVerify(piece: number): boolean {
		return piece >= 0 && piece < this.meta.pieceCount;
	}
	verify(piece: number, data: Uint8Array): Promise<boolean> {
		return verifyPiece(this.meta, piece, data);
	}
}

interface FileTree {
	file: MetainfoFile;
	firstPiece: number;
	pieces: number;
	/** Height of the tree above the piece layer (layer width = 2^height). */
	height: number;
	/** Validated piece-layer hashes (one per piece of the file), once known. */
	layer: Uint8Array[] | undefined;
	/** Chunks of the layer validated so far (files over 512 pieces come in chunks). */
	chunks: Map<number, Uint8Array[]>;
}

export class V2Verifier implements PieceVerifier {
	readonly meta: Metainfo;
	/** log2 of blocks per piece: the piece layer's level above the leaves. */
	readonly pieceLevel: number;
	private readonly trees = new Map<string, FileTree>();
	private readonly byPiece: (FileTree | undefined)[];

	constructor(meta: Metainfo) {
		if (meta.version === "v1") throw new Error("not a v2 torrent");
		this.meta = meta;
		this.pieceLevel = Math.log2(meta.pieceLength / BLOCK_SIZE);
		this.byPiece = new Array(meta.pieceCount);
		for (const file of meta.files) {
			if (file.padding || file.length === 0 || !file.piecesRoot) continue;
			const firstPiece = file.offset / meta.pieceLength;
			const pieces = Math.ceil(file.length / meta.pieceLength);
			const t: FileTree = { file, firstPiece, pieces, height: Math.log2(nextPow2(pieces)), layer: pieces === 1 ? [file.piecesRoot] : undefined, chunks: new Map() };
			this.trees.set(hexOf(file.piecesRoot), t);
			for (let p = firstPiece; p < firstPiece + pieces; p++) this.byPiece[p] = t;
		}
	}

	/** Files whose piece layer is still needed (to verify `pieces`, or all). */
	missingLayers(pieces?: Iterable<number>): MetainfoFile[] {
		const out = new Set<FileTree>();
		if (pieces === undefined) {
			for (const t of this.trees.values()) if (!t.layer) out.add(t);
		} else {
			for (const p of pieces) {
				const t = this.byPiece[p];
				if (t && !t.layer) out.add(t);
			}
		}
		return [...out].map((t) => t.file);
	}

	/** The hash requests that fetch a file's whole piece layer (one per 512 hashes, with proofs up to the root). */
	layerRequests(file: MetainfoFile): HashRequest[] {
		const t = this.trees.get(hexOf(file.piecesRoot!));
		if (!t || t.layer) return [];
		const width = 2 ** t.height;
		const length = Math.min(width, MAX_HASHES_PER_REQUEST);
		const out: HashRequest[] = [];
		for (let index = 0; index < t.pieces; index += length) {
			if (t.chunks.has(index)) continue;
			out.push({ piecesRoot: file.piecesRoot!, baseLayer: this.pieceLevel, index, length, proofLayers: t.height - Math.log2(length) });
		}
		return out;
	}

	/**
	 * A peer's `hashes` answer. Returns true if the hashes were accepted
	 * (they chain up to the pieces root); false otherwise (ask someone else).
	 */
	async acceptHashes(request: HashRequest, hashes: Uint8Array): Promise<boolean> {
		const t = this.trees.get(hexOf(request.piecesRoot));
		if (!t || t.layer) return false;
		const { baseLayer, index, length } = request;
		if (baseLayer !== this.pieceLevel || length < 2 || (length & (length - 1)) !== 0 || index % length !== 0) return false;
		const levels = Math.log2(length);
		const uncles = t.height - levels;
		if (hashes.length !== (length + uncles) * HASH || index >= 2 ** t.height) return false;
		const nodes: Uint8Array[] = [];
		for (let i = 0; i < length; i++) nodes.push(hashes.slice(i * HASH, (i + 1) * HASH));
		let node = await merkleRoot(nodes, length, this.pieceLevel);
		let at = index / length;
		for (let u = 0; u < uncles; u++, at >>= 1) {
			const uncle = hashes.subarray((length + u) * HASH, (length + u + 1) * HASH);
			node = at & 1 ? await pair(uncle, node) : await pair(node, uncle);
		}
		if (!equal(node, t.file.piecesRoot!)) return false;
		// Past the file's last piece the layer must hold zero subtrees.
		const pad = await zeroRoot(this.pieceLevel);
		for (let i = 0; i < length; i++) if (index + i >= t.pieces && !equal(nodes[i], pad)) return false;
		t.chunks.set(index, nodes.slice(0, Math.max(0, Math.min(length, t.pieces - index))));
		let have = 0;
		for (const c of t.chunks.values()) have += c.length;
		if (have >= t.pieces) {
			t.layer = [...t.chunks.entries()].sort(([a], [b]) => a - b).flatMap(([, c]) => c);
			t.chunks.clear();
		}
		return true;
	}

	/** A whole layer at once (e.g. from a .torrent's `piece layers`), checked against the root. */
	async setLayer(piecesRoot: Uint8Array, layer: Uint8Array): Promise<boolean> {
		const t = this.trees.get(hexOf(piecesRoot));
		if (!t || layer.length !== t.pieces * HASH) return false;
		const nodes = Array.from({ length: t.pieces }, (_, i) => layer.slice(i * HASH, (i + 1) * HASH));
		if (!equal(await merkleRoot(nodes, 2 ** t.height, this.pieceLevel), piecesRoot)) return false;
		t.layer = nodes;
		return true;
	}

	canVerify(piece: number): boolean {
		return this.byPiece[piece]?.layer !== undefined;
	}

	/** The expected hash of `piece` (subtree root), once known. */
	pieceHash(piece: number): Uint8Array | undefined {
		const t = this.byPiece[piece];
		return t?.layer?.[piece - t.firstPiece];
	}

	async verify(piece: number, data: Uint8Array): Promise<boolean> {
		const t = this.byPiece[piece];
		const expected = this.pieceHash(piece);
		if (!t || !expected || data.length !== pieceSize(this.meta, piece)) return false;
		return equal(await this.computePieceHash(t, data), expected);
	}

	/** A piece's subtree root: over its blocks, padded to a full piece (or, for a one-piece file, to a power of two). */
	private async computePieceHash(t: FileTree, data: Uint8Array): Promise<Uint8Array> {
		const leaves = await leafHashes(data);
		const width = t.pieces === 1 ? nextPow2(leaves.length) : this.meta.pieceLength / BLOCK_SIZE;
		return merkleRoot(leaves, width, 0);
	}

	/** The hash request for one piece's leaf hashes (to find bad blocks). */
	leafRequest(piece: number): HashRequest | undefined {
		const t = this.byPiece[piece];
		if (!t?.layer) return undefined;
		const perPiece = t.pieces === 1 ? nextPow2(Math.ceil(t.file.length / BLOCK_SIZE)) : this.meta.pieceLength / BLOCK_SIZE;
		if (perPiece < 2 || perPiece > MAX_HASHES_PER_REQUEST) return undefined;
		return { piecesRoot: t.file.piecesRoot!, baseLayer: 0, index: (piece - t.firstPiece) * perPiece, length: perPiece, proofLayers: 0 };
	}

	/**
	 * Which 16 KiB blocks of a failed piece are wrong, given leaf hashes a
	 * peer sent for it (`leafRequest`). The leaves are trusted only if they
	 * hash up to the verified piece hash. Undefined if they don't.
	 */
	async badBlocks(piece: number, data: Uint8Array, leaves: Uint8Array): Promise<number[] | undefined> {
		const t = this.byPiece[piece];
		const expected = this.pieceHash(piece);
		const req = this.leafRequest(piece);
		if (!t || !expected || !req || leaves.length !== req.length * HASH) return undefined;
		const given = Array.from({ length: req.length }, (_, i) => leaves.slice(i * HASH, (i + 1) * HASH));
		if (!equal(await merkleRoot(given, req.length, 0), expected)) return undefined;
		const actual = await leafHashes(data);
		const bad: number[] = [];
		actual.forEach((h, i) => {
			if (!equal(h, given[i])) bad.push(i);
		});
		return bad;
	}
}

/** The verifier for a torrent: SHA-1 for v1 and hybrid, merkle trees for v2-only. */
export function verifierFor(meta: Metainfo): PieceVerifier {
	return meta.version === "v2" ? new V2Verifier(meta) : new V1Verifier(meta);
}
