import { describe, expect, it } from "vitest";
import { FakeFs } from "./fake-opfs";
import {
	LEGACY_PREFIX,
	deleteAllSaved,
	deleteSavedFile,
	deleteSavedTorrent,
	exclusivePieces,
	fileProgress,
	listSaved,
	migrateLegacy,
	readSaved,
	rememberFile,
	savedProgress,
	storedPieces,
} from "./library-core";
import { PieceFile } from "./piece-file";

/** Just enough of Cache Storage for the library. */
class FakeCache {
	readonly entries = new Map<string, Response>();

	async match(url: string): Promise<Response | undefined> {
		return this.entries.get(url)?.clone();
	}

	async put(url: string, res: Response): Promise<void> {
		this.entries.set(url, res);
	}

	async delete(url: string): Promise<boolean> {
		return this.entries.delete(url);
	}

	async keys(): Promise<Request[]> {
		return [...this.entries.keys()].map((url) => new Request(url));
	}
}

class FakeCacheStorage {
	readonly caches = new Map<string, FakeCache>();

	async open(name: string): Promise<FakeCache> {
		let cache = this.caches.get(name);
		if (!cache) this.caches.set(name, (cache = new FakeCache()));
		return cache;
	}

	async has(name: string): Promise<boolean> {
		return this.caches.has(name);
	}

	async delete(name: string): Promise<boolean> {
		return this.caches.delete(name);
	}

	async keys(): Promise<string[]> {
		return [...this.caches.keys()];
	}
}

const HASH = "a".repeat(40);
const OTHER = "b".repeat(40);
const PL = 100;
const TORRENT = { name: "Show", magnet: `magnet:?xt=urn:btih:${HASH}`, pieceLength: PL, totalLength: 1000 };
// Two episodes sharing piece 2 (bytes 200–299).
const E1 = { index: 4, path: "Show/E1.mkv", offset: 50, size: 200 }; // pieces 0–2
const E2 = { index: 7, path: "Show/E2.mkv", offset: 250, size: 300 }; // pieces 2–5

const bytesOf = (p: number) => new Uint8Array(PL).fill(p + 1);

/** Store pieces the way the engine worker does. */
async function store(fs: FakeFs, infoHash: string, pieces: number[], pieceCount = 10) {
	const dir = await fs.root.getDirectoryHandle("pieces", { create: true });
	const data = await (await dir.getFileHandle(`${infoHash}.data`, { create: true })).createSyncAccessHandle();
	const map = await (await dir.getFileHandle(`${infoHash}.map`, { create: true })).createSyncAccessHandle();
	const file = new PieceFile(data, map, { pieceLength: PL, pieceCount, pieceSize: () => PL });
	for (const p of pieces) file.write(p, bytesOf(p));
	return file;
}

async function pieces(fs: FakeFs, infoHash: string) {
	return [...(await storedPieces(fs.root, infoHash))].sort((a, b) => a - b);
}

async function files(fs: FakeFs): Promise<string[]> {
	const dir = await fs.root.getDirectoryHandle("pieces", { create: true });
	const out: string[] = [];
	for await (const name of dir.keys()) out.push(name);
	return out.sort();
}

describe("fileProgress", () => {
	it("counts only the file's bytes of each stored piece", () => {
		expect(fileProgress(E1, 100, new Set([0, 2]))).toEqual({ savedBytes: 50 + 50, size: 200, complete: false });
		expect(fileProgress(E1, 100, new Set([0, 1, 2]))).toEqual({ savedBytes: 200, size: 200, complete: true });
	});
});

describe("exclusivePieces", () => {
	it("leaves out pieces a neighbouring file shares", () => {
		expect(exclusivePieces(E1, [E2], 100)).toEqual([0, 1]);
		expect(exclusivePieces(E2, [E1], 100)).toEqual([3, 4, 5]);
		expect(exclusivePieces(E2, [], 100)).toEqual([2, 3, 4, 5]);
	});
});

describe("saved library", () => {
	it("lists opened files with what's saved, newest first, tagging partial ones", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1, 1);
		await rememberFile(fs.root, HASH, TORRENT, E2, 2);
		(await store(fs, HASH, [0, 1, 2, 3])).close();

		const { files: list, unnamed } = await listSaved(fs.root);
		expect(unnamed).toEqual([]);
		expect(list.map((f) => [f.path, f.complete, f.savedBytes])).toEqual([
			["Show/E2.mkv", false, 50 + 100],
			["Show/E1.mkv", true, 200],
		]);
		expect(list[0]).toMatchObject({ infoHash: HASH, torrentName: "Show", magnet: TORRENT.magnet, index: 7 });

		const progress = await savedProgress(fs.root, HASH);
		expect(progress.get(4)?.complete).toBe(true);
		expect(progress.get(7)?.complete).toBe(false);
	});

	it("reads the map while the engine worker holds the files open (a partly recovered video stays listed)", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		const open = await store(fs, HASH, [1]);
		expect((await listSaved(fs.root)).files.map((f) => [f.index, f.complete, f.savedBytes])).toEqual([[4, false, 100]]);
		open.write(0, bytesOf(0));
		expect(await pieces(fs, HASH)).toEqual([0, 1]);
		expect(await readSaved(fs.root, HASH, 0, 200, PL)).toEqual(new Uint8Array([...bytesOf(0), ...bytesOf(1)]));
		await expect(readSaved(fs.root, HASH, 100, 300, PL)).rejects.toThrow(/Piece 2 is missing/);
		open.close();
	});

	it("reopening a file moves it to the top and doesn't duplicate it", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1, 1);
		await rememberFile(fs.root, HASH, TORRENT, E2, 2);
		await rememberFile(fs.root, HASH, TORRENT, E1, 3);
		(await store(fs, HASH, [1, 3])).close();
		expect((await listSaved(fs.root)).files.map((f) => f.index)).toEqual([4, 7]);
	});

	it("leaves out files with nothing saved yet", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		expect((await listSaved(fs.root)).files).toEqual([]);
	});

	it("lists pieces without a record as unnamed, and deletes them", async () => {
		const fs = new FakeFs();
		(await store(fs, OTHER, [5, 6, 7])).close();
		expect((await listSaved(fs.root)).unnamed).toEqual([{ infoHash: OTHER, pieces: 3 }]);
		await deleteSavedTorrent(fs.root, OTHER);
		expect(await files(fs)).toEqual([]);
	});

	it("deleting a file keeps the pieces a saved neighbour needs, then removes the files with the last one", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		await rememberFile(fs.root, HASH, TORRENT, E2);
		(await store(fs, HASH, [0, 1, 2, 3, 4, 5])).close();

		// Not the last one: the torrent stays (and so does its session memory).
		expect(await deleteSavedFile(fs.root, HASH, 4)).toBe(false);
		expect(await pieces(fs, HASH)).toEqual([2, 3, 4, 5]);
		expect((await listSaved(fs.root)).files.map((f) => [f.index, f.complete])).toEqual([[7, true]]);
		expect(await readSaved(fs.root, HASH, 200, 600, PL)).toHaveLength(400);

		// The last one: the whole torrent goes (its session memory with it).
		expect(await deleteSavedFile(fs.root, HASH, 7)).toBe(true);
		expect(await files(fs)).toEqual([]);
	});

	it("gives back the space of trailing pieces when a later episode is deleted", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		await rememberFile(fs.root, HASH, TORRENT, E2);
		(await store(fs, HASH, [0, 1, 2, 3, 4, 5])).close();
		const data = async () => (await (await (await fs.root.getDirectoryHandle("pieces")).getFileHandle(`${HASH}.data`)).getFile()).size;
		expect(await data()).toBe(6 * PL);
		await deleteSavedFile(fs.root, HASH, 7);
		expect(await pieces(fs, HASH)).toEqual([0, 1, 2]);
		expect(await data()).toBe(3 * PL);
	});

	it("deleting the only file with anything saved removes the torrent's files", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		await rememberFile(fs.root, HASH, TORRENT, E2);
		(await store(fs, HASH, [3, 4])).close();
		await deleteSavedFile(fs.root, HASH, 7);
		expect(await files(fs)).toEqual([]);
	});

	it("refuses to delete files in use, and changes nothing", async () => {
		const fs = new FakeFs();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		await rememberFile(fs.root, HASH, TORRENT, E2);
		const open = await store(fs, HASH, [0, 1, 2, 3, 4, 5]);
		await expect(deleteSavedFile(fs.root, HASH, 4)).rejects.toMatchObject({ code: "busy" });
		await expect(deleteSavedTorrent(fs.root, HASH)).rejects.toMatchObject({ code: "busy" });
		expect((await listSaved(fs.root)).files).toHaveLength(2);
		open.close();
		await deleteSavedFile(fs.root, HASH, 4);
		expect((await listSaved(fs.root)).files).toHaveLength(1);
	});

	it("deletes everything it saved (and the old caches), and nothing else on the origin", async () => {
		const fs = new FakeFs();
		const legacy = new FakeCacheStorage();
		await rememberFile(fs.root, HASH, TORRENT, E1);
		(await store(fs, HASH, [0])).close();
		(await store(fs, OTHER, [1])).close();
		await fs.root.getFileHandle("some-other-app", { create: true });
		await legacy.open(LEGACY_PREFIX + HASH);
		await legacy.open("some-other-app");
		await deleteAllSaved(fs.root, legacy as unknown as CacheStorage);
		expect(await files(fs)).toEqual([]);
		expect([...fs.root.entries.keys()].sort()).toEqual(["pieces", "some-other-app"]);
		expect(await legacy.keys()).toEqual(["some-other-app"]);
	});
});

describe("migration from Cache Storage", () => {
	const url = (piece: number | string) => `https://magnet-watcher.pieces/${HASH}/${piece}`;
	const record = { v: 1, name: "Show", magnet: TORRENT.magnet, pieceLength: PL, files: [{ ...E1, openedAt: 5 }] };

	async function legacyWith(pieceList: number[]) {
		const legacy = new FakeCacheStorage();
		const cache = await legacy.open(LEGACY_PREFIX + HASH);
		await cache.put(url("files"), new Response(JSON.stringify(record)));
		for (const p of pieceList) await cache.put(url(p), new Response(bytesOf(p)));
		return legacy;
	}

	it("moves named torrents into OPFS and deletes the caches, unnamed ones are dropped", async () => {
		const fs = new FakeFs();
		const legacy = await legacyWith([0, 2]);
		const unnamed = await legacy.open(LEGACY_PREFIX + OTHER);
		await unnamed.put(`https://magnet-watcher.pieces/${OTHER}/3`, new Response(bytesOf(3)));
		expect(await migrateLegacy(fs.root, legacy as unknown as CacheStorage)).toBe(1);
		expect(await legacy.keys()).toEqual([]);
		expect(await pieces(fs, HASH)).toEqual([0, 2]);
		expect(await readSaved(fs.root, HASH, 200, 300, PL)).toEqual(bytesOf(2));
		expect((await listSaved(fs.root)).files.map((f) => [f.index, f.savedBytes])).toEqual([[4, 100]]);
		// Nothing left: a second start does nothing.
		expect(await migrateLegacy(fs.root, legacy as unknown as CacheStorage)).toBe(0);
	});

	it("keeps the cache when the copy doesn't fit, and leaves no partial copy", async () => {
		const fs = new FakeFs();
		const legacy = await legacyWith([0, 9]);
		fs.quota = 500;
		expect(await migrateLegacy(fs.root, legacy as unknown as CacheStorage)).toBe(0);
		expect(await legacy.keys()).toEqual([LEGACY_PREFIX + HASH]);
		expect(await files(fs)).toEqual([]);
	});
});
