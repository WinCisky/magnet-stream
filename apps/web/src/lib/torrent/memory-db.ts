// Session memory in IndexedDB (M3-04, docs/architecture.md §Storage and
// database, §Connection management "Remember within the session"): per
// torrent, the verified info dict, notes on its peers (who delivered, who
// failed, the scout's facts) and the playback position of each file
// opened. A reload, or a second paste of the same magnet, starts from
// here: no discovery or BEP 9 when the data is present.
//
// Lifecycle (OQ-13, as the pieces in OPFS): the records of a torrent with a
// saved video (`saved`) stay until the user deletes the video; the library
// removes them with it (`forgetFile`, `forgetTorrent`, `forgetAll`).
// Torrents only looked up (no video opened) form a small cache: at most
// SEARCH_CACHE_ENTRIES torrents or SEARCH_CACHE_BYTES of info dicts, the
// least recently used leaving first, with their peer notes.
//
// Everything read back is untrusted: rows of the wrong shape read as
// missing, the info dict is checked against the info-hash by whoever uses
// it (the session worker), peer notes are validated by the peer book.
// Nothing here leaves the browser. Usable from the page and from workers.

export const DB_NAME = "magnet-stream-memory";
/** Schema version: one migration step per version (see MIGRATIONS). */
export const DB_VERSION = 1;
/** Torrents only looked up, kept at most (saved ones don't count). */
export const SEARCH_CACHE_ENTRIES = 50;
/** Info dict bytes of torrents only looked up, kept at most. */
export const SEARCH_CACHE_BYTES = 32 * 1024 * 1024;
/** Largest info dict kept (the engine's own limit). */
export const MAX_INFO_BYTES = 16 * 1024 * 1024;
/** Peer notes kept per torrent (the engine's MAX_PEER_NOTES). */
export const MAX_NOTES = 200;

const TORRENTS = "torrents";
const PEERS = "peers";
const POSITIONS = "positions";
const STORES = [TORRENTS, PEERS, POSITIONS];
const RECORD_V = 1;
const HEX = /^[0-9a-f]{40}$/;

interface TorrentRow {
	infoHash: string;
	v: number;
	/** The verified info dict (absent: a saved torrent whose dict wasn't kept yet). */
	info: Uint8Array | null;
	/** A video of it is saved: never evicted, removed only with the video. */
	saved: boolean;
	/** Last looked up or opened (ms since the epoch). */
	usedAt: number;
}

interface PeersRow {
	infoHash: string;
	v: number;
	notes: unknown[];
	at: number;
}

interface PositionRow {
	infoHash: string;
	index: number;
	v: number;
	ms: number;
	durationMs: number;
	at: number;
}

export interface PlaybackPosition {
	ms: number;
	durationMs: number;
}

/**
 * Schema steps: MIGRATIONS[n] takes a database from version n to n + 1.
 * A database at an older version runs the steps it misses, in order.
 */
export const MIGRATIONS: ReadonlyArray<(db: IDBDatabase, tx: IDBTransaction) => void> = [
	(db) => {
		const torrents = db.createObjectStore(TORRENTS, { keyPath: "infoHash" });
		torrents.createIndex("usedAt", "usedAt");
		db.createObjectStore(PEERS, { keyPath: "infoHash" });
		db.createObjectStore(POSITIONS, { keyPath: ["infoHash", "index"] });
	},
];

function request<T>(req: IDBRequest<T>): Promise<T> {
	return new Promise((resolve, reject) => {
		req.onsuccess = () => resolve(req.result);
		req.onerror = () => reject(req.error);
	});
}

function done(tx: IDBTransaction): Promise<void> {
	return new Promise((resolve, reject) => {
		tx.oncomplete = () => resolve();
		tx.onerror = () => reject(tx.error);
		tx.onabort = () => reject(tx.error ?? new Error("transaction aborted"));
	});
}

function checkHash(infoHash: string): void {
	if (typeof infoHash !== "string" || !HEX.test(infoHash)) throw new Error("bad info-hash");
}

function checkIndex(index: number): void {
	if (!Number.isSafeInteger(index) || index < 0) throw new Error("bad file index");
}

function finite(x: unknown): x is number {
	return typeof x === "number" && Number.isFinite(x) && x >= 0;
}

function torrentRow(value: unknown, infoHash: string): TorrentRow | null {
	if (!value || typeof value !== "object") return null;
	const r = value as Partial<TorrentRow>;
	if (r.infoHash !== infoHash || r.v !== RECORD_V || typeof r.saved !== "boolean" || !finite(r.usedAt)) return null;
	if (r.info !== null && !(r.info instanceof Uint8Array && r.info.length > 0 && r.info.length <= MAX_INFO_BYTES)) return null;
	return r as TorrentRow;
}

/** Info bytes a row weighs in the cache. */
function weight(value: unknown): number {
	const info = (value as Partial<TorrentRow> | null)?.info;
	return info instanceof Uint8Array ? info.length : 0;
}

/** Open (and create or migrate) the database; null where IndexedDB is unavailable or refuses. */
export async function openMemory(factory: IDBFactory | undefined = globalThis.indexedDB, name = DB_NAME): Promise<SessionMemory | null> {
	if (!factory) return null;
	try {
		const db = await openDb(factory, name);
		if (STORES.every((store) => db.objectStoreNames.contains(store))) return new SessionMemory(db);
		// At this version but without its stores (not made by this code): start over.
		db.close();
		await request(factory.deleteDatabase(name) as IDBRequest<unknown>);
		return new SessionMemory(await openDb(factory, name));
	} catch (e) {
		// A database from a newer version of the site (the site was rolled
		// back): this version can't read it. It holds a cache and notes only
		// (the pieces are in OPFS), so start over rather than run without.
		if ((e as { name?: string } | null)?.name !== "VersionError") return null;
		try {
			await request(factory.deleteDatabase(name) as IDBRequest<unknown>);
			return new SessionMemory(await openDb(factory, name));
		} catch {
			return null;
		}
	}
}

function openDb(factory: IDBFactory, name: string): Promise<IDBDatabase> {
	return new Promise((resolve, reject) => {
		const req = factory.open(name, DB_VERSION);
		req.onupgradeneeded = (event) => {
			const db = req.result;
			const tx = req.transaction!;
			for (let v = event.oldVersion; v < DB_VERSION; v++) MIGRATIONS[v](db, tx);
		};
		req.onsuccess = () => {
			const db = req.result;
			// Another tab upgrades: let it.
			db.onversionchange = () => db.close();
			resolve(db);
		};
		req.onerror = () => reject(req.error);
		req.onblocked = () => reject(new Error("blocked"));
	});
}

let shared: Promise<SessionMemory | null> | undefined;

/** This context's handle on the session memory (opened once). */
export function sessionMemory(): Promise<SessionMemory | null> {
	shared ??= openMemory().catch(() => null);
	return shared;
}

export class SessionMemory {
	private readonly db: IDBDatabase;
	private readonly now: () => number;

	constructor(db: IDBDatabase, now: () => number = Date.now) {
		this.db = db;
		this.now = now;
	}

	close(): void {
		this.db.close();
	}

	/**
	 * The info dict kept for this torrent (unverified: check it against the
	 * info-hash before use), or null. Counts as a use for the cache.
	 */
	async metadata(infoHash: string): Promise<Uint8Array | null> {
		checkHash(infoHash);
		const tx = this.db.transaction(TORRENTS, "readwrite");
		const store = tx.objectStore(TORRENTS);
		const row = torrentRow(await request(store.get(infoHash)), infoHash);
		if (row?.info) store.put({ ...row, usedAt: this.now() });
		await done(tx);
		return row?.info ? row.info.slice() : null;
	}

	/**
	 * Keep a verified info dict, as soon as a lookup has it (before any video
	 * is opened). Torrents only looked up are evicted beyond the cache's
	 * limits, least recently used first.
	 */
	async rememberMetadata(infoHash: string, info: Uint8Array): Promise<void> {
		checkHash(infoHash);
		if (!(info instanceof Uint8Array) || info.length === 0 || info.length > MAX_INFO_BYTES) return;
		const tx = this.db.transaction([TORRENTS, PEERS, POSITIONS], "readwrite");
		const store = tx.objectStore(TORRENTS);
		const previous = torrentRow(await request(store.get(infoHash)), infoHash);
		const row: TorrentRow = { infoHash, v: RECORD_V, info: info.slice(), saved: previous?.saved ?? false, usedAt: this.now() };
		store.put(row);
		await this.evict(tx, infoHash);
		await done(tx);
	}

	/** A video of this torrent is saved: its records stay until the user deletes it. */
	async markSaved(infoHash: string): Promise<void> {
		checkHash(infoHash);
		const tx = this.db.transaction(TORRENTS, "readwrite");
		const store = tx.objectStore(TORRENTS);
		const previous = torrentRow(await request(store.get(infoHash)), infoHash);
		store.put({ infoHash, v: RECORD_V, info: previous?.info ?? null, saved: true, usedAt: this.now() } satisfies TorrentRow);
		await done(tx);
	}

	/** Whether this torrent has a saved video (diagnostics, tests). */
	async isSaved(infoHash: string): Promise<boolean> {
		checkHash(infoHash);
		const tx = this.db.transaction(TORRENTS, "readonly");
		const row = torrentRow(await request(tx.objectStore(TORRENTS).get(infoHash)), infoHash);
		await done(tx);
		return row?.saved === true;
	}

	/** Peer notes kept for this torrent, unvalidated (the peer book checks each one). */
	async peerNotes(infoHash: string): Promise<unknown[]> {
		checkHash(infoHash);
		const tx = this.db.transaction(PEERS, "readonly");
		const row = (await request(tx.objectStore(PEERS).get(infoHash))) as Partial<PeersRow> | undefined;
		await done(tx);
		if (!row || row.infoHash !== infoHash || row.v !== RECORD_V || !Array.isArray(row.notes)) return [];
		return row.notes.slice(0, MAX_NOTES);
	}

	/**
	 * Replace this torrent's peer notes. Kept only while the torrent itself
	 * is (its info dict was found, or a video is saved): notes never outlive
	 * the torrent they belong to.
	 */
	async rememberPeers(infoHash: string, notes: readonly unknown[]): Promise<boolean> {
		checkHash(infoHash);
		const tx = this.db.transaction([TORRENTS, PEERS], "readwrite");
		const known = torrentRow(await request(tx.objectStore(TORRENTS).get(infoHash)), infoHash);
		if (known) tx.objectStore(PEERS).put({ infoHash, v: RECORD_V, notes: notes.slice(0, MAX_NOTES), at: this.now() } satisfies PeersRow);
		await done(tx);
		return known !== null;
	}

	/** Where playback of a file was, or null. */
	async position(infoHash: string, index: number): Promise<PlaybackPosition | null> {
		checkHash(infoHash);
		checkIndex(index);
		const tx = this.db.transaction(POSITIONS, "readonly");
		const row = (await request(tx.objectStore(POSITIONS).get([infoHash, index]))) as Partial<PositionRow> | undefined;
		await done(tx);
		if (!row || row.infoHash !== infoHash || row.index !== index || row.v !== RECORD_V || !finite(row.ms) || !finite(row.durationMs)) return null;
		return { ms: row.ms, durationMs: row.durationMs };
	}

	/** Keep where playback of a file is (saved torrents only). */
	async rememberPosition(infoHash: string, index: number, ms: number, durationMs: number): Promise<boolean> {
		checkHash(infoHash);
		checkIndex(index);
		if (!finite(ms) || !finite(durationMs)) return false;
		const tx = this.db.transaction([TORRENTS, POSITIONS], "readwrite");
		const known = torrentRow(await request(tx.objectStore(TORRENTS).get(infoHash)), infoHash);
		const keep = known?.saved === true;
		if (keep) tx.objectStore(POSITIONS).put({ infoHash, index, v: RECORD_V, ms: Math.round(ms), durationMs: Math.round(durationMs), at: this.now() } satisfies PositionRow);
		await done(tx);
		return keep;
	}

	/** One video of a torrent was deleted (others of it stay saved). */
	async forgetFile(infoHash: string, index: number): Promise<void> {
		checkHash(infoHash);
		checkIndex(index);
		const tx = this.db.transaction(POSITIONS, "readwrite");
		tx.objectStore(POSITIONS).delete([infoHash, index]);
		await done(tx);
	}

	/** A torrent's last saved video was deleted: everything kept about it goes. */
	async forgetTorrent(infoHash: string): Promise<void> {
		checkHash(infoHash);
		const tx = this.db.transaction([TORRENTS, PEERS, POSITIONS], "readwrite");
		this.drop(tx, infoHash);
		await done(tx);
	}

	/**
	 * Everything, cache included ("Delete all"), except the torrents in
	 * `keep` (saved videos whose delete was refused: they're in use).
	 */
	async forgetAll(keep?: ReadonlySet<string>): Promise<void> {
		const tx = this.db.transaction([TORRENTS, PEERS, POSITIONS], "readwrite");
		if (!keep || keep.size === 0) {
			for (const name of [TORRENTS, PEERS, POSITIONS]) tx.objectStore(name).clear();
		} else {
			for (const name of [TORRENTS, PEERS, POSITIONS]) {
				const store = tx.objectStore(name);
				for (const key of await request(store.getAllKeys())) {
					const infoHash = Array.isArray(key) ? key[0] : key;
					if (typeof infoHash === "string" && keep.has(infoHash)) continue;
					store.delete(key);
				}
			}
		}
		await done(tx);
	}

	/** Torrents kept: saved ones, and those only looked up (with their info bytes). */
	async counts(): Promise<{ saved: number; cached: number; cachedBytes: number }> {
		const tx = this.db.transaction(TORRENTS, "readonly");
		const rows = (await request(tx.objectStore(TORRENTS).getAll())) as unknown[];
		await done(tx);
		const out = { saved: 0, cached: 0, cachedBytes: 0 };
		for (const row of rows) {
			if ((row as Partial<TorrentRow>).saved === true) out.saved++;
			else {
				out.cached++;
				out.cachedBytes += weight(row);
			}
		}
		return out;
	}

	private drop(tx: IDBTransaction, infoHash: string): void {
		tx.objectStore(TORRENTS).delete(infoHash);
		tx.objectStore(PEERS).delete(infoHash);
		tx.objectStore(POSITIONS).delete(IDBKeyRange.bound([infoHash, 0], [infoHash, Number.MAX_SAFE_INTEGER]));
	}

	/** Torrents only looked up, beyond the cache's limits, least recently used first (never `keep`). */
	private async evict(tx: IDBTransaction, keep: string): Promise<void> {
		const rows = (await request(tx.objectStore(TORRENTS).index("usedAt").getAll())) as unknown[];
		const cached = rows.filter((r) => (r as Partial<TorrentRow>).saved !== true);
		let count = cached.length;
		let bytes = cached.reduce<number>((n, r) => n + weight(r), 0);
		for (const row of cached) {
			if (count <= SEARCH_CACHE_ENTRIES && bytes <= SEARCH_CACHE_BYTES) break;
			const infoHash = (row as Partial<TorrentRow>).infoHash;
			if (typeof infoHash !== "string" || infoHash === keep) continue;
			this.drop(tx, infoHash);
			count--;
			bytes -= weight(row);
		}
	}
}
