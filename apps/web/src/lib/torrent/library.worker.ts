// The saved-videos library, in a worker of its own: deleting part of a
// torrent needs FileSystemSyncAccessHandle, which only workers can open.
// The page asks (library.ts), this answers. Requests are validated; info-
// hashes become file names only through storeNames (hex only).
//
// The session memory in IndexedDB (memory-db.ts, M3-04) follows the pieces:
// a video opened marks its torrent saved there, and deleting a video
// deletes what was kept about it (positions; with the torrent's last
// video, its metadata and peer notes too).

import * as core from "./library-core.ts";
import { sessionMemory } from "./memory-db.ts";
import { toStoreError } from "./piece-file.ts";

const scope = self as unknown as {
	onmessage: ((event: MessageEvent) => void) | null;
	postMessage(message: unknown, transfer?: Transferable[]): void;
};

let migrated: Promise<unknown> | undefined;

async function root(): Promise<core.DirHandleLike> {
	const dir = (await navigator.storage.getDirectory()) as unknown as core.DirHandleLike;
	// Pieces saved by earlier versions in Cache Storage move here first (once).
	migrated ??= typeof caches === "undefined" ? Promise.resolve(0) : core.migrateLegacy(dir, caches).catch(() => 0);
	await migrated;
	return dir;
}

const hex = (v: unknown): string => {
	if (typeof v !== "string" || !/^[0-9a-f]{40}$/.test(v)) throw new Error("bad info-hash");
	return v;
};
const whole = (v: unknown): number => {
	if (!Number.isSafeInteger(v) || (v as number) < 0) throw new Error("bad number");
	return v as number;
};
const text = (v: unknown, max: number): string => {
	if (typeof v !== "string" || v.length > max) throw new Error("bad text");
	return v;
};

async function run(op: string, a: Record<string, unknown>): Promise<unknown> {
	const dir = await root();
	switch (op) {
		case "list":
			return core.listSaved(dir);
		case "progress":
			return core.savedProgress(dir, hex(a.infoHash));
		case "pieces":
			return [...(await core.storedPieces(dir, hex(a.infoHash)))];
		case "remember": {
			const t = a.torrent as Record<string, unknown>;
			const f = a.file as Record<string, unknown>;
			const infoHash = hex(a.infoHash);
			await (await sessionMemory())?.markSaved(infoHash).catch(() => {});
			return core.rememberFile(
				dir,
				infoHash,
				{ name: text(t.name, 4096), magnet: text(t.magnet, 8192), pieceLength: whole(t.pieceLength), totalLength: whole(t.totalLength) },
				{ index: whole(f.index), path: text(f.path, 4096), offset: whole(f.offset), size: whole(f.size) },
			);
		}
		case "file":
			return core.savedFile(dir, hex(a.infoHash), whole(a.index));
		case "read":
			return core.readSaved(dir, hex(a.infoHash), whole(a.start), whole(a.end), whole(a.pieceLength));
		case "delete-file": {
			const infoHash = hex(a.infoHash);
			const index = whole(a.index);
			const removed = await core.deleteSavedFile(dir, infoHash, index);
			const memory = await sessionMemory();
			await (removed ? memory?.forgetTorrent(infoHash) : memory?.forgetFile(infoHash, index));
			return undefined;
		}
		case "delete-torrent": {
			const infoHash = hex(a.infoHash);
			await core.deleteSavedTorrent(dir, infoHash);
			await (await sessionMemory())?.forgetTorrent(infoHash);
			return undefined;
		}
		case "delete-all": {
			let error: unknown;
			try {
				await core.deleteAllSaved(dir, typeof caches === "undefined" ? undefined : caches);
			} catch (e) {
				error = e;
			}
			// What a refused delete kept (in use) keeps its memory too.
			const kept = error ? await core.savedTorrents(dir).catch(() => new Set<string>()) : undefined;
			await (await sessionMemory())?.forgetAll(kept);
			if (error) throw error;
			return undefined;
		}
		default:
			throw new Error("unknown request");
	}
}

scope.onmessage = (event: MessageEvent) => {
	const msg = event.data as { req?: unknown; op?: unknown; args?: unknown } | null;
	if (!msg || !Number.isInteger(msg.req) || typeof msg.op !== "string") return;
	const req = msg.req as number;
	const args = msg.args && typeof msg.args === "object" ? (msg.args as Record<string, unknown>) : {};
	run(msg.op, args).then(
		(value) => scope.postMessage({ req, ok: true, value }, value instanceof Uint8Array ? [value.buffer] : []),
		(e: unknown) => {
			const err = toStoreError(e);
			scope.postMessage({ req, ok: false, code: err.code, message: err.message.slice(0, 200) });
		},
	);
};
