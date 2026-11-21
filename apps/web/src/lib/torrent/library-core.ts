// The videos saved in this browser (decision OQ-13: every video opened is
// saved as it is recovered, and deleted only when the user asks). Each
// torrent has three OPFS files in `pieces/`, named after its hex info-hash:
// the sparse data file and the verified-piece map (piece-file.ts), and a
// record of the files opened from it (name, magnet, piece length, files),
// so they can be listed, reopened and deleted file by file. Pieces at a
// file's edges may be shared with the neighbouring file: deleting a file
// keeps the ones another saved file still needs.
//
// Runs in the library worker (library.worker.ts): deletes need
// FileSystemSyncAccessHandle, which a page cannot open. Reads use getFile(),
// which works while the engine worker holds the files open.

import { PieceFile, STORE_DIR, StoreError, bitsToPieces, hasBit, storeNames, toStoreError, type PieceLayout, type SyncFile } from "./piece-file.ts";

export interface SavedFileRecord {
	/** Index in the magnet's file listing (the page's `file` URL parameter). */
	index: number;
	path: string;
	/** Byte range within the torrent, from the verified info dict. */
	offset: number;
	size: number;
	openedAt: number;
}

export interface TorrentRecord {
	v: 1;
	name: string;
	magnet: string;
	pieceLength: number;
	/** Torrent bytes (absent in records migrated from Cache Storage). */
	totalLength?: number;
	files: SavedFileRecord[];
}

export interface SavedProgress {
	savedBytes: number;
	size: number;
	complete: boolean;
}

export interface SavedFile extends SavedFileRecord, SavedProgress {
	infoHash: string;
	torrentName: string;
	magnet: string;
}

/** Pieces saved without a record of their files: no name, can only be deleted. */
export interface UnnamedTorrent {
	infoHash: string;
	pieces: number;
}

export interface SavedLibrary {
	/** Most recently opened first; files with nothing saved are left out. */
	files: SavedFile[];
	unnamed: UnnamedTorrent[];
}

// --- the part of the File System API used here (OPFS, or a fake in tests) ---

export interface FileHandleLike {
	readonly kind: "file";
	getFile(): Promise<{ size: number; arrayBuffer(): Promise<ArrayBuffer>; slice(start: number, end: number): { arrayBuffer(): Promise<ArrayBuffer> } }>;
	createSyncAccessHandle(): Promise<SyncFile>;
}

export interface DirHandleLike {
	readonly kind: "directory";
	getFileHandle(name: string, options?: { create?: boolean }): Promise<FileHandleLike>;
	getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<DirHandleLike>;
	removeEntry(name: string, options?: { recursive?: boolean }): Promise<void>;
	keys(): AsyncIterable<string>;
}

/** The pieces directory, or null when it doesn't exist and `create` is false. */
export async function piecesDir(root: DirHandleLike, create: boolean): Promise<DirHandleLike | null> {
	try {
		return await root.getDirectoryHandle(STORE_DIR, { create });
	} catch (e) {
		if ((e as { name?: string }).name === "NotFoundError") return null;
		throw toStoreError(e);
	}
}

async function fileOrNull(dir: DirHandleLike, name: string): Promise<FileHandleLike | null> {
	try {
		return await dir.getFileHandle(name);
	} catch (e) {
		if ((e as { name?: string }).name === "NotFoundError" || (e as { name?: string }).name === "TypeMismatchError") return null;
		throw toStoreError(e);
	}
}

async function readBytes(handle: FileHandleLike | null): Promise<Uint8Array | null> {
	if (!handle) return null;
	try {
		return new Uint8Array(await (await handle.getFile()).arrayBuffer());
	} catch (e) {
		throw toStoreError(e);
	}
}

async function writeWhole(dir: DirHandleLike, name: string, bytes: Uint8Array): Promise<void> {
	const handle = await dir.getFileHandle(name, { create: true });
	let file: SyncFile;
	try {
		file = await handle.createSyncAccessHandle();
	} catch (e) {
		throw toStoreError(e);
	}
	try {
		file.truncate(0);
		if (file.write(bytes, { at: 0 }) !== bytes.length) throw new StoreError("quota", "short write");
		file.flush();
	} catch (e) {
		throw toStoreError(e);
	} finally {
		file.close();
	}
}

// --- records -----------------------------------------------------------------

export function parseRecord(bytes: Uint8Array | null): TorrentRecord | null {
	if (!bytes) return null;
	try {
		const r = JSON.parse(new TextDecoder().decode(bytes)) as Partial<TorrentRecord> | null;
		if (!r || r.v !== 1 || typeof r.name !== "string" || typeof r.magnet !== "string") return null;
		if (!Number.isInteger(r.pieceLength) || r.pieceLength! <= 0 || !Array.isArray(r.files)) return null;
		if (r.totalLength !== undefined && (!Number.isInteger(r.totalLength) || r.totalLength < 0)) return null;
		const files = r.files.filter(
			(f): f is SavedFileRecord =>
				!!f &&
				Number.isInteger(f.index) &&
				f.index >= 0 &&
				typeof f.path === "string" &&
				Number.isInteger(f.offset) &&
				f.offset >= 0 &&
				Number.isInteger(f.size) &&
				f.size >= 0 &&
				typeof f.openedAt === "number",
		);
		return { v: 1, name: r.name, magnet: r.magnet, pieceLength: r.pieceLength!, totalLength: r.totalLength, files };
	} catch {
		return null;
	}
}

async function readRecord(dir: DirHandleLike, infoHash: string): Promise<TorrentRecord | null> {
	return parseRecord(await readBytes(await fileOrNull(dir, storeNames(infoHash).record)));
}

async function writeRecord(dir: DirHandleLike, infoHash: string, record: TorrentRecord): Promise<void> {
	await writeWhole(dir, storeNames(infoHash).record, new TextEncoder().encode(JSON.stringify(record)));
}

/** Verified pieces of a torrent, from its map (empty when nothing is saved). */
export async function storedPieces(root: DirHandleLike, infoHash: string): Promise<Set<number>> {
	const dir = await piecesDir(root, false);
	if (!dir) return new Set();
	const bits = await readBytes(await fileOrNull(dir, storeNames(infoHash).map));
	return new Set(bits ? bitsToPieces(bits) : []);
}

/** Record that a file of this torrent is being recovered (and when). */
export async function rememberFile(
	root: DirHandleLike,
	infoHash: string,
	torrent: { name: string; magnet: string; pieceLength: number; totalLength: number },
	file: Omit<SavedFileRecord, "openedAt">,
	now = Date.now(),
): Promise<void> {
	const dir = (await piecesDir(root, true))!;
	const previous = await readRecord(dir, infoHash);
	await writeRecord(dir, infoHash, {
		v: 1,
		...torrent,
		files: [...(previous?.files.filter((f) => f.index !== file.index) ?? []), { ...file, openedAt: now }],
	});
}

/** Info-hashes with any file in the pieces directory. */
async function torrents(dir: DirHandleLike): Promise<Set<string>> {
	const out = new Set<string>();
	for await (const name of dir.keys()) {
		const m = /^([0-9a-f]{40})\.(data|map|json)$/.exec(name);
		if (m) out.add(m[1]);
	}
	return out;
}

/** Info-hashes with anything saved (pieces, map or record). */
export async function savedTorrents(root: DirHandleLike): Promise<Set<string>> {
	const dir = await piecesDir(root, false);
	return dir ? torrents(dir) : new Set();
}

export async function listSaved(root: DirHandleLike): Promise<SavedLibrary> {
	const library: SavedLibrary = { files: [], unnamed: [] };
	const dir = await piecesDir(root, false);
	if (!dir) return library;
	for (const infoHash of await torrents(dir)) {
		const [record, stored] = await Promise.all([readRecord(dir, infoHash), storedPieces(root, infoHash)]);
		if (!record) {
			if (stored.size > 0) library.unnamed.push({ infoHash, pieces: stored.size });
			continue;
		}
		for (const file of record.files) {
			const progress = fileProgress(file, record.pieceLength, stored);
			if (progress.savedBytes === 0) continue;
			library.files.push({ ...file, ...progress, infoHash, torrentName: record.name, magnet: record.magnet });
		}
	}
	library.files.sort((a, b) => b.openedAt - a.openedAt);
	return library;
}

/** How much of each recorded file of one torrent is saved, by file index. */
export async function savedProgress(root: DirHandleLike, infoHash: string): Promise<Map<number, SavedProgress>> {
	const progress = new Map<number, SavedProgress>();
	const dir = await piecesDir(root, false);
	if (!dir) return progress;
	const [record, stored] = await Promise.all([readRecord(dir, infoHash), storedPieces(root, infoHash)]);
	for (const file of record?.files ?? []) {
		const p = fileProgress(file, record!.pieceLength, stored);
		if (p.savedBytes > 0) progress.set(file.index, p);
	}
	return progress;
}

/** Where a saved file's bytes are: the data file, the range, and whether every piece is there. */
export async function savedFile(root: DirHandleLike, infoHash: string, index: number): Promise<{ file: SavedFileRecord; pieceLength: number; complete: boolean }> {
	const dir = await piecesDir(root, false);
	const record = dir ? await readRecord(dir, infoHash) : null;
	const file = record?.files.find((f) => f.index === index);
	if (!record || !file) throw new StoreError("range", "This video isn't saved anymore");
	const stored = await storedPieces(root, infoHash);
	return { file, pieceLength: record.pieceLength, complete: fileProgress(file, record.pieceLength, stored).complete };
}

/**
 * Bytes [start, end) of a torrent's data file, if every piece they touch
 * is verified (a download of a saved file). Read with getFile(): works
 * while the engine worker has the file open.
 */
export async function readSaved(root: DirHandleLike, infoHash: string, start: number, end: number, pieceLength: number): Promise<Uint8Array> {
	const dir = await piecesDir(root, false);
	const names = storeNames(infoHash);
	const [data, bits] = dir ? [await fileOrNull(dir, names.data), await readBytes(await fileOrNull(dir, names.map))] : [null, null];
	if (!data || !bits) throw new StoreError("unverified", "This video isn't saved anymore");
	for (let p = Math.floor(start / pieceLength); p * pieceLength < end; p++) {
		if (!hasBit(bits, p)) throw new StoreError("unverified", `Piece ${p} is missing: open the video to recover it again`);
	}
	try {
		return new Uint8Array(await (await data.getFile()).slice(start, end).arrayBuffer());
	} catch (e) {
		throw toStoreError(e);
	}
}

// --- deletion ------------------------------------------------------------------

async function removeIfPresent(dir: DirHandleLike, name: string): Promise<void> {
	try {
		await dir.removeEntry(name);
	} catch (e) {
		if ((e as { name?: string }).name === "NotFoundError") return;
		throw toStoreError(e);
	}
}

/** Remove a torrent's files. The data file goes first: if it's in use, nothing is removed. */
export async function deleteSavedTorrent(root: DirHandleLike, infoHash: string): Promise<void> {
	const dir = await piecesDir(root, false);
	if (!dir) return;
	const names = storeNames(infoHash);
	await removeIfPresent(dir, names.data);
	await removeIfPresent(dir, names.map);
	await removeIfPresent(dir, names.record);
}

/**
 * Delete one file's pieces, keeping those another saved file still covers.
 * Returns true when that was the torrent's last saved file and the whole
 * torrent went (its session memory goes with it, M3-04).
 */
export async function deleteSavedFile(root: DirHandleLike, infoHash: string, index: number): Promise<boolean> {
	const dir = await piecesDir(root, false);
	if (!dir) return true;
	const record = await readRecord(dir, infoHash);
	const target = record?.files.find((f) => f.index === index);
	if (!record) return !(await fileOrNull(dir, storeNames(infoHash).map));
	if (!target) return false;
	const others = record.files.filter((f) => f !== target);
	const stored = await storedPieces(root, infoHash);
	if (others.every((f) => fileProgress(f, record.pieceLength, stored).savedBytes === 0)) {
		await deleteSavedTorrent(root, infoHash);
		return true;
	}
	const doomed = exclusivePieces(target, others, record.pieceLength).filter((p) => stored.has(p));
	const names = storeNames(infoHash);
	const [dataHandle, mapHandle] = [await dir.getFileHandle(names.data, { create: true }), await dir.getFileHandle(names.map, { create: true })];
	let data: SyncFile | undefined;
	let map: SyncFile | undefined;
	try {
		data = await dataHandle.createSyncAccessHandle();
		map = await mapHandle.createSyncAccessHandle();
	} catch (e) {
		data?.close();
		throw toStoreError(e);
	}
	const pieceCount = Math.max(Math.ceil((record.totalLength ?? 0) / record.pieceLength), map.getSize() * 8, 1);
	const layout: PieceLayout = {
		pieceLength: record.pieceLength,
		pieceCount,
		pieceSize: (p) => (record.totalLength ? Math.min(record.pieceLength, record.totalLength - p * record.pieceLength) : record.pieceLength),
	};
	const file = new PieceFile(data, map, layout);
	try {
		// Forget the file first: an interrupted delete leaves orphan pieces, not
		// a listed file with holes in it.
		await writeRecord(dir, infoHash, { ...record, files: others });
		file.clear(doomed);
	} finally {
		file.close();
	}
	return false;
}

/** Everything saved: the pieces directory (and Cache Storage left by older versions). */
export async function deleteAllSaved(root: DirHandleLike, legacy?: CacheStorage): Promise<void> {
	const dir = await piecesDir(root, false);
	let busy: StoreError | undefined;
	if (dir) {
		for (const infoHash of await torrents(dir)) {
			try {
				await deleteSavedTorrent(root, infoHash);
			} catch (e) {
				busy = toStoreError(e);
			}
		}
	}
	if (legacy) for (const name of (await legacy.keys()).filter((n) => n.startsWith(LEGACY_PREFIX))) await legacy.delete(name);
	if (busy) throw busy;
}

// --- helpers on piece ranges ---------------------------------------------------

/** First and last piece holding bytes of a file. */
export function pieceSpan(file: { offset: number; size: number }, pieceLength: number): [number, number] {
	const first = Math.floor(file.offset / pieceLength);
	return [first, Math.max(first, Math.floor((file.offset + Math.max(file.size, 1) - 1) / pieceLength))];
}

export function fileProgress(file: { offset: number; size: number }, pieceLength: number, stored: Set<number>): SavedProgress {
	const [first, last] = pieceSpan(file, pieceLength);
	const end = file.offset + file.size;
	let savedBytes = 0;
	let complete = true;
	for (let p = first; p <= last; p++) {
		if (!stored.has(p)) {
			complete = false;
			continue;
		}
		savedBytes += Math.min((p + 1) * pieceLength, end) - Math.max(p * pieceLength, file.offset);
	}
	return { savedBytes: Math.max(0, savedBytes), size: file.size, complete };
}

/** A file's pieces that none of the other files overlaps. */
export function exclusivePieces(target: { offset: number; size: number }, others: { offset: number; size: number }[], pieceLength: number): number[] {
	const spans = others.map((f) => pieceSpan(f, pieceLength));
	const [first, last] = pieceSpan(target, pieceLength);
	const pieces: number[] = [];
	for (let p = first; p <= last; p++) if (!spans.some(([a, b]) => p >= a && p <= b)) pieces.push(p);
	return pieces;
}

// --- migration from Cache Storage (versions up to M3-01) -----------------------

/** Caches of the previous store: one per torrent, one entry per piece, plus a `files` record. */
export const LEGACY_PREFIX = "mw-pieces-";

/**
 * Move the pieces of the previous Cache Storage store into OPFS, once.
 * A torrent with a record of its files is copied (piece p at p × piece
 * length) and its cache deleted; a cache without a record cannot be placed
 * (its piece length is unknown) and is deleted. A torrent that fails (the
 * quota, say) keeps its cache and is tried again at the next start; its
 * partial copy is removed. Returns the torrents moved.
 */
export async function migrateLegacy(root: DirHandleLike, legacy: CacheStorage): Promise<number> {
	let moved = 0;
	for (const name of await legacy.keys()) {
		if (!name.startsWith(LEGACY_PREFIX)) continue;
		const infoHash = name.slice(LEGACY_PREFIX.length);
		if (!/^[0-9a-f]{40}$/.test(infoHash)) {
			await legacy.delete(name);
			continue;
		}
		const cache = await legacy.open(name);
		const recordRes = await cache.match(`https://magnet-watcher.pieces/${infoHash}/files`);
		const record = recordRes ? parseRecord(new Uint8Array(await recordRes.arrayBuffer())) : null;
		const dir = (await piecesDir(root, true))!;
		const names = storeNames(infoHash);
		if (!record || (await fileOrNull(dir, names.data))) {
			// Unplaceable, or already in OPFS (that copy wins).
			await legacy.delete(name);
			continue;
		}
		try {
			await copyPieces(dir, names, cache, record.pieceLength);
			await writeRecord(dir, infoHash, record);
		} catch {
			await deleteSavedTorrent(root, infoHash).catch(() => {});
			continue;
		}
		await legacy.delete(name);
		moved++;
	}
	return moved;
}

async function copyPieces(dir: DirHandleLike, names: { data: string; map: string }, cache: Cache, pieceLength: number): Promise<void> {
	const data = await (await dir.getFileHandle(names.data, { create: true })).createSyncAccessHandle();
	let map: SyncFile | undefined;
	try {
		map = await (await dir.getFileHandle(names.map, { create: true })).createSyncAccessHandle();
		let bits: Uint8Array = new Uint8Array(0);
		for (const req of await cache.keys()) {
			const tail = req.url.slice(req.url.lastIndexOf("/") + 1);
			if (!/^\d+$/.test(tail)) continue;
			const piece = Number(tail);
			const res = await cache.match(req.url);
			if (!res) continue;
			const bytes = new Uint8Array(await res.arrayBuffer());
			if (bytes.length === 0 || bytes.length > pieceLength) continue;
			if (data.write(bytes, { at: piece * pieceLength }) !== bytes.length) throw new StoreError("quota", "short write");
			if (bits.length <= piece >> 3) bits = grow(bits, (piece >> 3) + 1);
			bits[piece >> 3] |= 0x80 >> (piece & 7);
		}
		data.flush();
		// The map last: a copy cut short leaves no piece marked verified.
		if (map.write(bits, { at: 0 }) !== bits.length) throw new StoreError("quota", "short write");
		map.flush();
	} catch (e) {
		throw toStoreError(e);
	} finally {
		data.close();
		map?.close();
	}
}

function grow(bits: Uint8Array, size: number): Uint8Array {
	const out = new Uint8Array(Math.max(size, bits.length * 2));
	out.set(bits);
	return out;
}
