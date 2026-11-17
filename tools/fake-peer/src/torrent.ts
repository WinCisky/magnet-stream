// Deterministic test torrents: the same options always give the same bytes,
// info dict and info-hash. Content is pseudo-random noise derived from the
// seed: never real data.
//
// Three kinds (BEP 3 / BEP 52):
//   - v1 (default): SHA-1 piece hashes; one file, or several (`files`)
//     back to back;
//   - v2: a `file tree` with a SHA-256 merkle root per file ("pieces
//     root"); every file starts at a piece boundary, and the last piece of
//     each file is shorter on the wire (no padding bytes are transferred);
//   - hybrid: both, with BEP 47 padding files in the v1 `files` list so the
//     two layouts agree (pad bytes are zeros and are part of v1 pieces).
//
// `data` is the "piece space": every file at its offset, zeros between
// files (v2 and hybrid). Requests address it by piece and offset.

import { createHash } from "node:crypto";
import { bencode, concat, type BValue } from "./bencode.ts";

export const BLOCK = 16 * 1024;

export interface FileSpec {
	/** Path components below the torrent's root directory. */
	path: readonly string[];
	length: number;
}

export interface TorrentOptions {
	/** PRNG seed (default 1). */
	seed?: number;
	/** Total length in bytes of a single-file torrent (default 8 MiB). Ignored with `files`. */
	size?: number;
	/** Piece length in bytes (default 256 KiB; v2 and hybrid need a power of two >= 16 KiB). */
	pieceLength?: number;
	/** Torrent name (default `fake-<seed>.bin`, or `fake-<seed>` with `files`). */
	name?: string;
	/** Several files (a directory torrent) instead of one. */
	files?: readonly FileSpec[];
	/** 1 (default), 2 (v2 only) or "hybrid". */
	version?: 1 | 2 | "hybrid";
}

export interface TorrentFileInfo {
	/** `a/b.mkv` (relative to the root directory; the name for a single file). */
	path: string;
	length: number;
	/** Offset in the piece space. */
	offset: number;
	/** BEP 47 padding file (hybrid v1 list only). */
	pad: boolean;
	/** v2: SHA-256 merkle root of the file (absent for empty files). */
	piecesRoot?: Uint8Array;
}

/** A v2 file's merkle tree, leaves (16 KiB blocks) first, padded to a power of two. */
export interface MerkleTree {
	/** layers[0] = leaf hashes, layers[last] = [root]. */
	layers: Uint8Array[][];
	/** The layer whose nodes each cover one piece. */
	pieceLayer: number;
	/** Pieces of the file (unpadded count). */
	pieces: number;
}

export interface FakeTorrent {
	seed: number;
	name: string;
	version: 1 | 2 | "hybrid";
	/** Piece space (see above). */
	data: Uint8Array;
	totalLength: number;
	pieceLength: number;
	pieceCount: number;
	/** Bytes of piece `p` on the wire (shorter at the end of each v2 file). */
	pieceSize(p: number): number;
	/** SHA-1 of each piece (v1 and hybrid; empty for v2). */
	hashes: Uint8Array[];
	/** Concatenated piece hashes, as in the info dict (v1 and hybrid). */
	pieceHashes: Uint8Array;
	files: TorrentFileInfo[];
	/** v2 and hybrid: merkle tree per pieces root (hex). */
	trees: Map<string, MerkleTree>;
	/** v2 and hybrid: the `piece layers` a .torrent would carry (pieces root hex → concatenated hashes), for files larger than one piece. */
	pieceLayers: Map<string, Uint8Array>;
	/** The bencoded info dict (what ut_metadata serves). */
	info: Uint8Array;
	/** The 20 bytes used in handshakes, DHT and trackers: SHA-1 of `info` (v1, hybrid) or its SHA-256 truncated (v2). */
	infoHash: Uint8Array;
	infoHashHex: string;
	/** SHA-1 of `info` (v1 and hybrid). */
	infoHashV1?: Uint8Array;
	/** SHA-256 of `info` (v2 and hybrid). */
	infoHashV2?: Uint8Array;
	/** `magnet:?xt=urn:btih:...` (v1, hybrid also btmh) or `xt=urn:btmh:1220...` (v2). */
	magnet: string;
}

export const MiB = 1024 * 1024;

function noise(seed: number, size: number): Uint8Array {
	const data = new Uint8Array(size);
	// xorshift32 over 4 bytes at a time: fast enough for hundreds of MiB.
	let x = (seed >>> 0) ^ 0x9e3779b9 || 1;
	const words = Math.floor(size / 4);
	const view = new DataView(data.buffer);
	for (let i = 0; i < words; i++) {
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		view.setUint32(i * 4, x >>> 0);
	}
	for (let i = words * 4; i < size; i++) {
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		data[i] = x & 0xff;
	}
	return data;
}

const sha1 = (b: Uint8Array) => new Uint8Array(createHash("sha1").update(b).digest());
const sha256 = (b: Uint8Array) => new Uint8Array(createHash("sha256").update(b).digest());
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

/** Root of a perfect tree of `leaves` zero leaves (`leaves` a power of two). */
export function zeroSubtree(leaves: number): Uint8Array {
	let h: Uint8Array = new Uint8Array(32);
	for (let n = leaves; n > 1; n /= 2) h = sha256(concat([h, h]));
	return h;
}

function nextPow2(n: number): number {
	let p = 1;
	while (p < n) p *= 2;
	return p;
}

/** BEP 52 merkle tree of one file's bytes. */
export function merkleTree(file: Uint8Array, pieceLength: number): MerkleTree {
	const blocks = Math.max(1, Math.ceil(file.length / BLOCK));
	const leaves: Uint8Array[] = [];
	for (let b = 0; b < blocks; b++) leaves.push(sha256(file.subarray(b * BLOCK, (b + 1) * BLOCK)));
	const width = nextPow2(blocks);
	while (leaves.length < width) leaves.push(new Uint8Array(32));
	const layers: Uint8Array[][] = [leaves];
	while (layers[layers.length - 1].length > 1) {
		const below = layers[layers.length - 1];
		const up: Uint8Array[] = [];
		for (let i = 0; i < below.length; i += 2) up.push(sha256(concat([below[i], below[i + 1]])));
		layers.push(up);
	}
	const blocksPerPiece = pieceLength / BLOCK;
	return { layers, pieceLayer: Math.log2(blocksPerPiece), pieces: Math.ceil(file.length / pieceLength) };
}

export function makeTorrent(options: TorrentOptions = {}): FakeTorrent {
	const { seed = 1, size = 8 * MiB, pieceLength = 256 * 1024, version = 1 } = options;
	if (!Number.isSafeInteger(pieceLength) || pieceLength <= 0) throw new RangeError("pieceLength must be a positive integer");
	if (!Number.isInteger(seed)) throw new RangeError("seed must be an integer");
	if (version !== 1 && (pieceLength < BLOCK || (pieceLength & (pieceLength - 1)) !== 0)) {
		throw new RangeError("v2 piece length must be a power of two >= 16 KiB");
	}
	const multi = options.files !== undefined;
	let specs: readonly FileSpec[] = options.files ?? [{ path: [], length: size }];
	// A v2 file tree is a bencoded dict: its files come in sorted path order,
	// and a hybrid's v1 list must follow the same order.
	if (version !== 1) specs = [...specs].sort((a, b) => comparePaths(a.path, b.path));
	for (const f of specs) if (!Number.isSafeInteger(f.length) || f.length < 0) throw new RangeError("file length must be >= 0");
	const realSize = specs.reduce((n, f) => n + f.length, 0);
	if (realSize <= 0) throw new RangeError("size must be a positive integer");
	const torrentName = options.name ?? (multi ? `fake-${seed}` : `fake-${seed}.bin`);
	const content = noise(seed, realSize);

	// Layout in the piece space.
	const aligned = version !== 1;
	const files: TorrentFileInfo[] = [];
	const contentAt: number[] = [];
	let offset = 0;
	let read = 0;
	specs.forEach((f, i) => {
		const path = multi ? f.path.join("/") : torrentName;
		files.push({ path, length: f.length, offset, pad: false });
		contentAt.push(read);
		read += f.length;
		offset += f.length;
		const last = specs.slice(i + 1).every((g) => g.length === 0);
		const gap = aligned && !last && f.length > 0 ? (pieceLength - (offset % pieceLength)) % pieceLength : 0;
		if (gap > 0) {
			if (version === "hybrid") files.push({ path: `.pad/${gap}`, length: gap, offset, pad: true });
			offset += gap;
		}
	});
	const totalLength = offset;
	const data = new Uint8Array(totalLength);
	files.forEach((f) => {
		if (f.pad) return;
		const i = files.filter((g) => !g.pad).indexOf(f);
		data.set(content.subarray(contentAt[i], contentAt[i] + f.length), f.offset);
	});
	const pieceCount = Math.ceil(totalLength / pieceLength);
	const realFiles = files.filter((f) => !f.pad);
	const pieceSize = (p: number): number => {
		const start = p * pieceLength;
		if (version !== 2) return Math.min(pieceLength, totalLength - start);
		const file = realFiles.find((f) => f.length > 0 && start >= f.offset && start < f.offset + f.length);
		return file ? Math.min(pieceLength, file.offset + file.length - start) : 0;
	};

	// v1 hashes.
	const hashes: Uint8Array[] = [];
	const pieceHashes = new Uint8Array(version === 2 ? 0 : pieceCount * 20);
	if (version !== 2) {
		for (let p = 0; p < pieceCount; p++) {
			const digest = sha1(data.subarray(p * pieceLength, p * pieceLength + pieceSize(p)));
			hashes.push(digest);
			pieceHashes.set(digest, p * 20);
		}
	}

	// v2 trees.
	const trees = new Map<string, MerkleTree>();
	const pieceLayers = new Map<string, Uint8Array>();
	if (version !== 1) {
		for (const f of realFiles) {
			if (f.length === 0) continue;
			const tree = merkleTree(data.subarray(f.offset, f.offset + f.length), pieceLength);
			f.piecesRoot = tree.layers[tree.layers.length - 1][0];
			trees.set(hex(f.piecesRoot), tree);
			if (f.length > pieceLength) pieceLayers.set(hex(f.piecesRoot), concat(tree.layers[tree.pieceLayer].slice(0, tree.pieces)));
		}
	}

	const info: Record<string, BValue> = { name: torrentName, "piece length": pieceLength };
	if (version !== 2) {
		info.pieces = pieceHashes;
		if (multi) {
			info.files = files.map(
				(f): BValue =>
					f.pad ? { attr: "p", length: f.length, path: [".pad", String(f.length)] } : { length: f.length, path: f.path.split("/") },
			);
		} else info.length = realSize;
	}
	if (version !== 1) {
		info["meta version"] = 2;
		const tree: Record<string, BValue> = {};
		for (const f of realFiles) {
			const parts = multi ? f.path.split("/") : [torrentName];
			let node = tree;
			for (const part of parts.slice(0, -1)) node = (node[part] ??= {}) as Record<string, BValue>;
			node[parts[parts.length - 1]] = { "": f.length > 0 ? { length: f.length, "pieces root": f.piecesRoot! } : { length: 0 } };
		}
		info["file tree"] = tree;
	}
	const infoBytes = bencode(info);
	const infoHashV1 = version !== 2 ? sha1(infoBytes) : undefined;
	const infoHashV2 = version !== 1 ? sha256(infoBytes) : undefined;
	const infoHash = infoHashV1 ?? infoHashV2!.slice(0, 20);
	const infoHashHex = hex(infoHash);
	let magnet: string;
	if (version === 1) magnet = magnetFor(infoHashHex, torrentName);
	else if (version === 2) magnet = `magnet:?xt=urn:btmh:1220${hex(infoHashV2!)}&dn=${encodeURIComponent(torrentName)}`;
	else magnet = `magnet:?xt=urn:btih:${infoHashHex}&xt=urn:btmh:1220${hex(infoHashV2!)}&dn=${encodeURIComponent(torrentName)}`;
	return {
		seed,
		name: torrentName,
		version,
		data,
		totalLength,
		pieceLength,
		pieceCount,
		pieceSize,
		hashes,
		pieceHashes,
		files,
		trees,
		pieceLayers,
		info: infoBytes,
		infoHash,
		infoHashHex,
		infoHashV1,
		infoHashV2,
		magnet,
	};
}

function comparePaths(a: readonly string[], b: readonly string[]): number {
	for (let i = 0; i < Math.min(a.length, b.length); i++) {
		const x = Buffer.from(a[i]);
		const y = Buffer.from(b[i]);
		const c = Buffer.compare(x, y);
		if (c !== 0) return c;
	}
	return a.length - b.length;
}

/** A magnet link; `peer` adds an `x.pe` hint (BEP 9). */
export function magnetFor(infoHashHex: string, name: string, peer?: string): string {
	let magnet = `magnet:?xt=urn:btih:${infoHashHex}&dn=${encodeURIComponent(name)}`;
	if (peer) magnet += `&x.pe=${encodeURIComponent(peer)}`;
	return magnet;
}

/** `8MiB`, `512KiB`, `1GiB`, `1000` → bytes. */
export function parseSize(text: string): number {
	const m = /^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|KB|MB|GB)?$/i.exec(text.trim());
	if (!m) throw new RangeError(`bad size "${text}"`);
	const unit = (m[2] ?? "B").toLowerCase();
	const scale: Record<string, number> = { b: 1, kib: 1024, mib: MiB, gib: 1024 * MiB, kb: 1e3, mb: 1e6, gb: 1e9 };
	const n = Math.round(Number(m[1]) * scale[unit]);
	if (!Number.isSafeInteger(n) || n <= 0) throw new RangeError(`bad size "${text}"`);
	return n;
}
