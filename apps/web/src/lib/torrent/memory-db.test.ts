// Session memory (M3-04): schema and its migration, the bounded cache of
// magnets only looked up, cascade deletion with the video, and rows read
// back as untrusted.

import "fake-indexeddb/auto";
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { DB_NAME, DB_VERSION, MIGRATIONS, SEARCH_CACHE_BYTES, SEARCH_CACHE_ENTRIES, SessionMemory, openMemory } from "./memory-db";

const hash = (n: number) => n.toString(16).padStart(40, "0");
const A = hash(0xa);
const B = hash(0xb);
const info = (n: number, size = 64) => new Uint8Array(size).fill(n & 0xff);

async function fresh(): Promise<{ factory: IDBFactory; memory: SessionMemory }> {
	const factory = new IDBFactory();
	const memory = await openMemory(factory);
	if (!memory) throw new Error("no memory");
	return { factory, memory };
}

function storeNames(factory: IDBFactory, version?: number): Promise<{ names: string[]; version: number; indexes: string[] }> {
	return new Promise((resolve, reject) => {
		const req = factory.open(DB_NAME, version);
		req.onsuccess = () => {
			const db = req.result;
			const names = [...db.objectStoreNames].sort();
			const indexes = [...db.transaction("torrents").objectStore("torrents").indexNames];
			db.close();
			resolve({ names, version: db.version, indexes });
		};
		req.onerror = () => reject(req.error);
	});
}

describe("session memory schema", () => {
	it("creates every store at the current version", async () => {
		const { factory, memory } = await fresh();
		memory.close();
		expect(MIGRATIONS).toHaveLength(DB_VERSION);
		expect(await storeNames(factory)).toEqual({ names: ["peers", "positions", "torrents"], version: DB_VERSION, indexes: ["usedAt"] });
	});

	it("starts over on a database from a newer version (a rolled-back site)", async () => {
		const factory = new IDBFactory();
		await new Promise<void>((resolve, reject) => {
			const req = factory.open(DB_NAME, DB_VERSION + 5);
			req.onupgradeneeded = () => req.result.createObjectStore("future");
			req.onsuccess = () => {
				req.result.close();
				resolve();
			};
			req.onerror = () => reject(req.error);
		});
		const memory = await openMemory(factory);
		expect(memory).not.toBeNull();
		await memory!.rememberMetadata(A, info(1));
		expect(await memory!.metadata(A)).toEqual(info(1));
		memory!.close();
		expect((await storeNames(factory)).names).toEqual(["peers", "positions", "torrents"]);
	});

	it("starts over on a database at this version without its stores", async () => {
		const factory = new IDBFactory();
		await new Promise<void>((resolve, reject) => {
			const req = factory.open(DB_NAME, DB_VERSION);
			req.onsuccess = () => {
				req.result.close();
				resolve();
			};
			req.onerror = () => reject(req.error);
		});
		const memory = await openMemory(factory);
		await memory!.rememberMetadata(A, info(1));
		expect(await memory!.metadata(A)).toEqual(info(1));
	});

	it("reads rows of another record version or shape as missing", async () => {
		const { factory, memory } = await fresh();
		await memory.markSaved(A);
		await memory.rememberMetadata(B, info(2));
		memory.close();
		await new Promise<void>((resolve, reject) => {
			const req = factory.open(DB_NAME, DB_VERSION);
			req.onsuccess = () => {
				const db = req.result;
				const tx = db.transaction(["torrents", "peers", "positions"], "readwrite");
				tx.objectStore("torrents").put({ infoHash: B, v: 99, info: info(2), saved: false, usedAt: 1 });
				tx.objectStore("torrents").put({ infoHash: A, v: 1, info: "not bytes", saved: true, usedAt: 1 });
				tx.objectStore("peers").put({ infoHash: A, v: 1, notes: "nope", at: 1 });
				tx.objectStore("positions").put({ infoHash: A, index: 0, v: 1, ms: -5, durationMs: 10, at: 1 });
				tx.oncomplete = () => {
					db.close();
					resolve();
				};
				tx.onerror = () => reject(tx.error);
			};
		});
		const again = (await openMemory(factory))!;
		expect(await again.metadata(B)).toBeNull();
		expect(await again.metadata(A)).toBeNull();
		expect(await again.peerNotes(A)).toEqual([]);
		expect(await again.position(A, 0)).toBeNull();
	});

	it("refuses keys that aren't a hex info-hash or a file index", async () => {
		const { memory } = await fresh();
		await expect(memory.metadata("../x")).rejects.toThrow(/info-hash/);
		await expect(memory.rememberPosition(A, -1, 0, 0)).rejects.toThrow(/index/);
	});
});

describe("session memory records", () => {
	it("keeps metadata, peer notes and positions of a saved torrent", async () => {
		const { memory } = await fresh();
		await memory.rememberMetadata(A, info(7));
		// Positions only for saved torrents; notes once the torrent is known.
		expect(await memory.rememberPosition(A, 1, 1000, 5000)).toBe(false);
		expect(await memory.rememberPeers(B, [{ addr: "1.1.1.1:6881" }])).toBe(false);
		expect(await memory.peerNotes(B)).toEqual([]);
		await memory.markSaved(A);
		expect(await memory.isSaved(A)).toBe(true);
		expect(await memory.rememberPeers(A, [{ addr: "1.1.1.1:6881" }])).toBe(true);
		expect(await memory.rememberPosition(A, 1, 1234.4, 5000)).toBe(true);
		expect(await memory.metadata(A)).toEqual(info(7));
		expect(await memory.peerNotes(A)).toEqual([{ addr: "1.1.1.1:6881" }]);
		expect(await memory.position(A, 1)).toEqual({ ms: 1234, durationMs: 5000 });
		// A later lookup keeps the saved flag.
		await memory.rememberMetadata(A, info(7));
		expect(await memory.isSaved(A)).toBe(true);
	});

	it("marking saved before the metadata is known keeps the torrent", async () => {
		const { memory } = await fresh();
		await memory.markSaved(A);
		expect(await memory.metadata(A)).toBeNull();
		await memory.rememberMetadata(A, info(3));
		expect(await memory.metadata(A)).toEqual(info(3));
		expect(await memory.isSaved(A)).toBe(true);
	});

	it("deletes in cascade: one file's position, or the whole torrent, or everything", async () => {
		const { memory } = await fresh();
		for (const h of [A, B]) {
			await memory.rememberMetadata(h, info(1));
			await memory.markSaved(h);
			await memory.rememberPeers(h, [{ addr: "1.1.1.1:6881" }]);
			await memory.rememberPosition(h, 0, 10, 100);
			await memory.rememberPosition(h, 3, 20, 100);
		}
		await memory.forgetFile(A, 0);
		expect(await memory.position(A, 0)).toBeNull();
		expect(await memory.position(A, 3)).toEqual({ ms: 20, durationMs: 100 });
		expect(await memory.metadata(A)).not.toBeNull();

		await memory.forgetTorrent(A);
		expect(await memory.metadata(A)).toBeNull();
		expect(await memory.peerNotes(A)).toEqual([]);
		expect(await memory.position(A, 3)).toBeNull();
		expect(await memory.isSaved(A)).toBe(false);
		// The other torrent is untouched.
		expect(await memory.position(B, 3)).toEqual({ ms: 20, durationMs: 100 });
		expect(await memory.peerNotes(B)).toHaveLength(1);

		// "Delete all" with B's delete refused (in use): B keeps its records.
		await memory.rememberMetadata(A, info(4));
		await memory.forgetAll(new Set([B]));
		expect(await memory.metadata(A)).toBeNull();
		expect(await memory.position(B, 3)).toEqual({ ms: 20, durationMs: 100 });
		expect(await memory.peerNotes(B)).toHaveLength(1);

		await memory.forgetAll();
		expect(await memory.metadata(B)).toBeNull();
		expect(await memory.peerNotes(B)).toEqual([]);
		expect(await memory.counts()).toEqual({ saved: 0, cached: 0, cachedBytes: 0 });
	});
});

describe("cache of magnets only looked up", () => {
	it("keeps at most SEARCH_CACHE_ENTRIES, least recently used out first; saved torrents don't count", async () => {
		const factory = new IDBFactory();
		let t = 1_000;
		const raw = await new Promise<IDBDatabase>((resolve) => {
			void openMemory(factory).then((m) => {
				m!.close();
				const req = factory.open(DB_NAME, DB_VERSION);
				req.onsuccess = () => resolve(req.result);
			});
		});
		const memory = new SessionMemory(raw, () => t++);
		const saved = [hash(1000), hash(1001)];
		for (const h of saved) {
			await memory.rememberMetadata(h, info(9));
			await memory.markSaved(h);
		}
		for (let i = 0; i < SEARCH_CACHE_ENTRIES; i++) await memory.rememberMetadata(hash(i + 1), info(i));
		await memory.rememberPeers(hash(1), [{ addr: "1.1.1.1:6881" }]);
		// Torrent 1 is used again: torrent 2 is now the least recently used.
		expect(await memory.metadata(hash(1))).not.toBeNull();
		await memory.rememberMetadata(hash(999), info(1));
		expect(await memory.counts()).toEqual({ saved: 2, cached: SEARCH_CACHE_ENTRIES, cachedBytes: SEARCH_CACHE_ENTRIES * 64 });
		expect(await memory.metadata(hash(2))).toBeNull();
		expect(await memory.metadata(hash(1))).not.toBeNull();
		expect(await memory.peerNotes(hash(1))).toHaveLength(1);
		for (const h of saved) expect(await memory.metadata(h)).not.toBeNull();
	});

	it("keeps at most SEARCH_CACHE_BYTES of info dicts", async () => {
		const { memory } = await fresh();
		const big = 12 * 1024 * 1024;
		await memory.rememberMetadata(hash(1), info(1, big));
		await memory.rememberMetadata(hash(2), info(2, big));
		await memory.rememberPeers(hash(1), [{ addr: "1.1.1.1:6881" }]);
		await memory.rememberMetadata(hash(3), info(3, big));
		const counts = await memory.counts();
		expect(counts.cachedBytes).toBeLessThanOrEqual(SEARCH_CACHE_BYTES);
		expect(counts.cached).toBe(2);
		expect(await memory.metadata(hash(1))).toBeNull();
		// Its notes went with it.
		expect(await memory.peerNotes(hash(1))).toEqual([]);
		expect(await memory.metadata(hash(3))).not.toBeNull();
	});
});
