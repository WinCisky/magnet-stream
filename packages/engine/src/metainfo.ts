// A torrent's info dict: piece length, piece hashes and file layout, for
// v1, v2-only (BEP 52) and hybrid torrents. The bytes come from peers
// (BEP 9) and are trusted only once their hash equals the info-hash of the
// magnet: SHA-1 for v1, SHA-256 for v2, both for a hybrid when both are
// known.
//
// Layout ("piece space"): files at their byte offsets, pieces of
// `pieceLength` bytes. In a v1 torrent files are back to back. In v2 and
// hybrid torrents every file starts at a piece boundary (hybrids say so
// with BEP 47 padding files, whose bytes are zeros); in a v2-only torrent
// the last piece of each file is shorter on the wire (`pieceSize`).

import { decodeBencode, getBytes, getDict, getInt, type BencodeDict, type Bencoded } from "./bencode.ts";

export const BLOCK_SIZE = 16 * 1024;

export interface MetainfoFile {
	/** Sanitised path relative to the torrent's root (`a/b.mkv`); the torrent name for a single-file torrent. */
	path: string;
	length: number;
	/** Byte offset in the piece space. */
	offset: number;
	/** BEP 47 padding file (hybrid v1 list): not a real file. */
	padding?: boolean;
	/** v2: SHA-256 merkle root of the file's 16 KiB blocks (absent for empty files). */
	piecesRoot?: Uint8Array;
}

export type TorrentVersion = "v1" | "v2" | "hybrid";

export interface Metainfo {
	/** The 20-byte wire info-hash, hex (v1 hash, or truncated v2 hash for v2-only). */
	infoHash: string;
	/** SHA-1 of the info dict (v1 and hybrid). */
	infoHashV1?: string;
	/** SHA-256 of the info dict (v2 and hybrid). */
	infoHashV2?: string;
	version: TorrentVersion;
	/** Sanitised torrent name. */
	name: string;
	pieceLength: number;
	/** Size of the piece space. */
	totalLength: number;
	pieceCount: number;
	/** v1 and hybrid: 20 bytes per piece. Empty for v2-only. */
	pieceHashes: Uint8Array;
	files: MetainfoFile[];
}

export interface ExpectedHashes {
	/** v1 SHA-1, 40 hex. */
	v1?: string;
	/** v2 SHA-256, 64 hex. */
	v2?: string;
	/** 40 hex: the wire hash (v1, or v2 truncated), when that is all we know. */
	wire?: string;
}

export interface MetainfoLimits {
	maxFiles?: number;
	maxPieceLength?: number;
	maxPieces?: number;
	maxPathDepth?: number;
}

export class MetainfoError extends Error {}

const DEFAULTS: Required<MetainfoLimits> = { maxFiles: 100_000, maxPieceLength: 64 * 1024 * 1024, maxPieces: 4_000_000, maxPathDepth: 32 };

/**
 * Parse an info dict, after checking its hash. `expected` is a wire hash
 * (40 hex) or the hashes the magnet named.
 */
export async function parseInfoDict(bytes: Uint8Array, expected: string | ExpectedHashes, limits: MetainfoLimits = {}): Promise<Metainfo> {
	const lim = { ...DEFAULTS, ...limits };
	const want: ExpectedHashes = typeof expected === "string" ? { wire: expected.toLowerCase() } : expected;
	const sha1 = await digestHex("SHA-1", bytes);
	let sha256: string | undefined;
	const v2Hash = async () => (sha256 ??= await digestHex("SHA-256", bytes));

	let matchedV1 = false;
	let matchedV2 = false;
	if (want.v1 !== undefined) {
		if (sha1 !== want.v1.toLowerCase()) throw new MetainfoError("Metadata doesn't match the torrent's info-hash");
		matchedV1 = true;
	}
	if (want.v2 !== undefined) {
		if ((await v2Hash()) !== want.v2.toLowerCase()) throw new MetainfoError("Metadata doesn't match the torrent's v2 info-hash");
		matchedV2 = true;
	}
	if (want.wire !== undefined && !matchedV1 && !matchedV2) {
		if (sha1 === want.wire) matchedV1 = true;
		else if ((await v2Hash()).slice(0, 40) === want.wire) matchedV2 = true;
		else throw new MetainfoError("Metadata doesn't match the torrent's info-hash");
	}
	if (!matchedV1 && !matchedV2) throw new MetainfoError("No info-hash to check the metadata against");

	let info: Bencoded;
	try {
		info = decodeBencode(bytes);
	} catch (e) {
		throw new MetainfoError(`Info dict is not valid bencode: ${(e as Error).message}`);
	}
	if (!(info instanceof Map)) throw new MetainfoError("Info dict is not a dictionary");
	const name = sanitizeComponent(text(info, "name"));
	const pieceLength = getInt(info, "piece length");
	if (pieceLength === undefined || pieceLength <= 0 || pieceLength > lim.maxPieceLength) throw new MetainfoError("Bad piece length");

	const hasV2 = getInt(info, "meta version") === 2 && info.get("file tree") instanceof Map;
	const hasV1 = info.has("pieces");
	if (!hasV1 && !hasV2) throw new MetainfoError("Info dict has neither piece hashes nor a v2 file tree");
	if (matchedV2 && !hasV2) throw new MetainfoError("A v2 info-hash names a v1-only info dict");
	if (matchedV1 && !hasV1) throw new MetainfoError("A v1 info-hash names a v2-only info dict");
	const version: TorrentVersion = hasV1 && hasV2 ? "hybrid" : hasV2 ? "v2" : "v1";
	if (version !== "v1" && (pieceLength < BLOCK_SIZE || (pieceLength & (pieceLength - 1)) !== 0)) {
		throw new MetainfoError("v2 piece length must be a power of two of at least 16 KiB");
	}

	let files: MetainfoFile[];
	let pieceHashes: Uint8Array = new Uint8Array(0);
	if (hasV1) {
		const pieces = getBytes(info, "pieces");
		if (!pieces || pieces.length === 0 || pieces.length % 20 !== 0) throw new MetainfoError("Info dict has no valid piece hashes");
		pieceHashes = pieces;
		files = v1Files(info, name, lim);
	} else {
		files = v2Files(getDict(info, "file tree")!, name, pieceLength, lim);
	}
	if (hasV2) {
		const tree = v2Files(getDict(info, "file tree")!, name, pieceLength, lim);
		if (hasV1) attachRoots(files, tree);
		else files = tree;
	}

	const real = files.filter((f) => !f.padding);
	const last = real.reduce((end, f) => Math.max(end, f.offset + f.length), 0);
	const totalLength = version === "v1" ? files.reduce((n, f) => n + f.length, 0) : Math.max(last, files.reduce((n, f) => Math.max(n, f.offset + f.length), 0));
	if (totalLength <= 0) throw new MetainfoError("The torrent is empty");
	const pieceCount = Math.ceil(totalLength / pieceLength);
	if (pieceCount > lim.maxPieces) throw new MetainfoError("Too many pieces");
	if (hasV1 && pieceHashes.length / 20 !== pieceCount) throw new MetainfoError("Piece count doesn't match the torrent size");

	const v1Hex = hasV1 ? sha1 : undefined;
	const v2Hex = hasV2 ? await v2Hash() : undefined;
	return {
		infoHash: v1Hex ?? v2Hex!.slice(0, 40),
		...(v1Hex ? { infoHashV1: v1Hex } : {}),
		...(v2Hex ? { infoHashV2: v2Hex } : {}),
		version,
		name,
		pieceLength,
		totalLength,
		pieceCount,
		pieceHashes,
		files,
	};
}

function v1Files(info: BencodeDict, name: string, lim: Required<MetainfoLimits>): MetainfoFile[] {
	const list = info.get("files");
	if (!Array.isArray(list)) {
		const length = getInt(info, "length");
		if (length === undefined || length < 0) throw new MetainfoError("Bad length");
		return [{ path: name, length, offset: 0 }];
	}
	if (list.length === 0 || list.length > lim.maxFiles) throw new MetainfoError("Bad file list");
	const files: MetainfoFile[] = [];
	let offset = 0;
	for (const entry of list) {
		if (!(entry instanceof Map)) throw new MetainfoError("Bad file entry");
		const length = getInt(entry, "length");
		if (length === undefined || length < 0) throw new MetainfoError("Bad file length");
		const parts = entry.get("path.utf-8") ?? entry.get("path");
		if (!Array.isArray(parts) || parts.length === 0 || parts.length > lim.maxPathDepth) throw new MetainfoError("Bad file path");
		const path = parts.map((p) => sanitizeComponent(p instanceof Uint8Array ? utf8(p) : "")).join("/");
		const attr = getBytes(entry, "attr");
		const padding = (attr !== undefined && utf8(attr).includes("p")) || path.startsWith(".pad/") || path.includes("_____padding_file_");
		files.push({ path, length, offset, ...(padding ? { padding: true } : {}) });
		offset += length;
		if (!Number.isSafeInteger(offset)) throw new MetainfoError("Torrent too large");
	}
	return files;
}

/** Walk a v2 `file tree` in order; every file starts at a piece boundary. */
function v2Files(tree: BencodeDict, name: string, pieceLength: number, lim: Required<MetainfoLimits>): MetainfoFile[] {
	const files: MetainfoFile[] = [];
	const leaves: { parts: string[]; length: number; root?: Uint8Array }[] = [];
	const walk = (node: BencodeDict, parts: string[]) => {
		if (parts.length > lim.maxPathDepth) throw new MetainfoError("File tree too deep");
		for (const [key, child] of node) {
			if (!(child instanceof Map)) throw new MetainfoError("Bad file tree");
			if (key === "") throw new MetainfoError("Bad file tree");
			const leaf = getDict(child, "");
			if (leaf) {
				if (child.size !== 1) throw new MetainfoError("Bad file tree entry");
				const length = getInt(leaf, "length");
				if (length === undefined || length < 0) throw new MetainfoError("Bad file length");
				const root = getBytes(leaf, "pieces root");
				if (length > 0 && (!root || root.length !== 32)) throw new MetainfoError("File without a pieces root");
				leaves.push({ parts: [...parts, key], length, ...(length > 0 ? { root: root! } : {}) });
				if (leaves.length > lim.maxFiles) throw new MetainfoError("Too many files");
			} else walk(child, [...parts, key]);
		}
	};
	walk(tree, []);
	if (leaves.length === 0) throw new MetainfoError("Empty file tree");
	// A single file named like the torrent sits at the tree's root.
	const single = leaves.length === 1 && leaves[0].parts.length === 1;
	let offset = 0;
	leaves.forEach((leaf, i) => {
		const path = single ? name : leaf.parts.map(sanitizeComponent).join("/");
		files.push({ path, length: leaf.length, offset, ...(leaf.root ? { piecesRoot: leaf.root } : {}) });
		offset += leaf.length;
		const more = leaves.slice(i + 1).some((l) => l.length > 0);
		if (more && leaf.length > 0) offset = Math.ceil(offset / pieceLength) * pieceLength;
		if (!Number.isSafeInteger(offset)) throw new MetainfoError("Torrent too large");
	});
	return files;
}

/** Hybrid: the v1 list (with padding) and the v2 tree must describe the same files at the same offsets. */
function attachRoots(v1: MetainfoFile[], v2: MetainfoFile[]): void {
	const real = v1.filter((f) => !f.padding);
	if (real.length !== v2.length) throw new MetainfoError("Hybrid torrent: v1 and v2 file lists differ");
	real.forEach((f, i) => {
		const g = v2[i];
		if (f.length !== g.length || (f.length > 0 && f.offset !== g.offset) || (v2.length > 1 && f.path !== g.path)) {
			throw new MetainfoError("Hybrid torrent: v1 and v2 file lists differ");
		}
		if (g.piecesRoot) f.piecesRoot = g.piecesRoot;
	});
}

/**
 * One path component, safe for display and storage keys: no separators,
 * control characters, `.` or `..`; at most 255 characters.
 */
export function sanitizeComponent(raw: string): string {
	// eslint-disable-next-line no-control-regex
	let s = raw.replace(/[\u0000-\u001f\u007f/\\]/g, "_").trim();
	if (s === "" || s === "." || s === "..") s = "_";
	return s.length > 255 ? s.slice(0, 255) : s;
}

/** Real (non-padding) files containing piece `p`'s first byte, for v2 piece sizes. */
function fileAt(meta: Metainfo, offset: number): MetainfoFile | undefined {
	const files = meta.files;
	let lo = 0;
	let hi = files.length - 1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		const f = files[mid];
		if (f.offset + f.length <= offset) lo = mid + 1;
		else if (f.offset > offset) hi = mid - 1;
		else if (f.length === 0) lo = mid + 1;
		else return f;
	}
	return undefined;
}

/** Bytes of piece `piece` on the wire. */
export function pieceSize(meta: Pick<Metainfo, "pieceCount" | "pieceLength" | "totalLength"> & Partial<Pick<Metainfo, "version" | "files">>, piece: number): number {
	const start = piece * meta.pieceLength;
	if (meta.version === "v2" && meta.files) {
		const file = fileAt(meta as Metainfo, start);
		return file ? Math.min(meta.pieceLength, file.offset + file.length - start) : 0;
	}
	return piece < meta.pieceCount - 1 ? meta.pieceLength : meta.totalLength - meta.pieceLength * (meta.pieceCount - 1);
}

export function blockCount(meta: Metainfo, piece: number): number {
	return Math.ceil(pieceSize(meta, piece) / BLOCK_SIZE);
}

export function blockLength(meta: Metainfo, piece: number, block: number): number {
	return Math.min(BLOCK_SIZE, pieceSize(meta, piece) - block * BLOCK_SIZE);
}

export interface FileRange {
	/** Byte range of the file within the torrent. */
	offset: number;
	size: number;
	firstPiece: number;
	lastPiece: number;
}

/**
 * The pieces covering a file. The first and last may include bytes of the
 * neighbouring files; they're fetched whole, since only whole pieces can
 * be verified. The verified info dict wins over the listing's numbers if
 * they disagree.
 */
export function fileRange(meta: Metainfo, file: { path: string; offset: number; size: number }): FileRange {
	const real = meta.files.filter((f) => !f.padding);
	const match =
		real.find((f) => f.offset === file.offset && f.length === file.size) ??
		real.find((f) => f.path === file.path || f.path.endsWith(`/${file.path}`));
	const offset = match?.offset ?? file.offset;
	const size = match?.length ?? file.size;
	const firstPiece = Math.floor(offset / meta.pieceLength);
	const lastPiece = Math.max(firstPiece, Math.floor((offset + Math.max(size, 1) - 1) / meta.pieceLength));
	return { offset, size, firstPiece, lastPiece: Math.min(lastPiece, meta.pieceCount - 1) };
}

export interface ListedFile {
	path: string;
	size: number;
	offset: number;
}

export interface Listing {
	infoHash: string;
	name: string;
	/** Total size of the listed files. */
	size: number;
	pieceLength: number;
	files: ListedFile[];
}

/**
 * The file list the page shows, in the order magnet-seeders' `/files` used
 * (file indices in links and saved videos are positions in it): largest
 * first, same-size files in torrent order, padding files left out.
 */
export function listing(meta: Metainfo): Listing {
	const files = meta.files
		.filter((f) => !f.padding)
		.map((f, i) => ({ f: { path: f.path, size: f.length, offset: f.offset }, i }))
		.sort((a, b) => b.f.size - a.f.size || a.i - b.i)
		.map(({ f }) => f);
	return { infoHash: meta.infoHash, name: meta.name, size: files.reduce((n, f) => n + f.size, 0), pieceLength: meta.pieceLength, files };
}

export async function sha1Hex(data: Uint8Array): Promise<string> {
	return digestHex("SHA-1", data);
}

async function digestHex(algorithm: "SHA-1" | "SHA-256", data: Uint8Array): Promise<string> {
	const digest = new Uint8Array(await crypto.subtle.digest(algorithm, data as Uint8Array<ArrayBuffer>));
	return Array.from(digest, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** SHA-1 check of a v1 (or hybrid) piece. v2-only torrents verify through a PieceVerifier (verify.ts). */
export async function verifyPiece(meta: Metainfo, piece: number, data: Uint8Array): Promise<boolean> {
	if (meta.pieceHashes.length < (piece + 1) * 20) return false;
	const digest = new Uint8Array(await crypto.subtle.digest("SHA-1", data as Uint8Array<ArrayBuffer>));
	const expected = meta.pieceHashes.subarray(piece * 20, piece * 20 + 20);
	return digest.every((b, i) => b === expected[i]);
}

const decoder = new TextDecoder();
function utf8(b: Uint8Array): string {
	return decoder.decode(b);
}

function text(dict: BencodeDict, key: string): string {
	const value = dict.get(`${key}.utf-8`) ?? dict.get(key);
	if (!(value instanceof Uint8Array)) throw new MetainfoError(`Info dict has no ${key}`);
	return utf8(value);
}

/** The piece geometry a PeerConnection checks messages against. */
export function geometryOf(meta: Metainfo): { pieceCount: number; pieceLength: number; totalLength: number; pieceSize: (piece: number) => number } {
	return { pieceCount: meta.pieceCount, pieceLength: meta.pieceLength, totalLength: meta.totalLength, pieceSize: (p) => pieceSize(meta, p) };
}
