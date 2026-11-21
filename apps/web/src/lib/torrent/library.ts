// The page's handle on the saved-videos library (library-core.ts, run in
// library.worker.ts). Pieces live in OPFS, one torrent per sparse file; a
// video stays saved, even partly, until the user deletes it (OQ-13).

import { fileBytes } from "./download";
import type { SavedFile, SavedFileRecord, SavedLibrary, SavedProgress, UnnamedTorrent } from "./library-core.ts";
import { PageSession } from "./page-session";
import { StoreError, type StoreErrorCode } from "./piece-file.ts";
import LibraryWorker from "./library.worker.ts?worker&inline";

export { exclusivePieces, fileProgress, pieceSpan } from "./library-core.ts";
export type { SavedFile, SavedFileRecord, SavedLibrary, SavedProgress, UnnamedTorrent };

let worker: Worker | undefined;
let nextReq = 1;
const pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();

export function libraryAvailable(): boolean {
	return typeof navigator !== "undefined" && typeof navigator.storage?.getDirectory === "function" && typeof Worker !== "undefined";
}

function call<T>(op: string, args: Record<string, unknown> = {}): Promise<T> {
	if (!libraryAvailable()) return Promise.reject(new StoreError("unavailable", "This browser doesn't let the page save anything"));
	if (!worker) {
		worker = new LibraryWorker();
		worker.onmessage = (event: MessageEvent) => {
			const msg = event.data as { req: number; ok: boolean; value?: unknown; code?: StoreErrorCode; message?: string };
			const p = pending.get(msg.req);
			if (!p) return;
			pending.delete(msg.req);
			if (msg.ok) p.resolve(msg.value);
			else p.reject(new StoreError(msg.code ?? "io", msg.message ?? "storage error"));
		};
	}
	const req = nextReq++;
	return new Promise<T>((resolve, reject) => {
		pending.set(req, { resolve: resolve as (value: unknown) => void, reject });
		worker!.postMessage({ req, op, args });
	});
}

/**
 * Deletes fail while a session holds the torrent's files open: ask the
 * sessions to let go of the files they aren't writing, then try again once.
 */
async function deleting(run: () => Promise<void>): Promise<void> {
	try {
		await run();
	} catch (e) {
		if (!(e instanceof StoreError) || e.code !== "busy") throw e;
		await PageSession.releaseStores();
		try {
			await run();
		} catch (again) {
			if (again instanceof StoreError && again.code === "busy") throw new Error("This video is being recovered right now: close it, then delete it.", { cause: again });
			throw again;
		}
	}
}

/** Record that a file of this torrent is being recovered (and when). */
export function rememberFile(
	infoHash: string,
	torrent: { name: string; magnet: string; pieceLength: number; totalLength: number },
	file: Omit<SavedFileRecord, "openedAt">,
): Promise<void> {
	return call("remember", { infoHash, torrent, file });
}

export async function listSaved(): Promise<SavedLibrary> {
	if (!libraryAvailable()) return { files: [], unnamed: [] };
	return call("list");
}

/** How much of each recorded file of one torrent is saved, by file index. */
export async function savedProgress(infoHash: string): Promise<Map<number, SavedProgress>> {
	if (!libraryAvailable()) return new Map();
	return call("progress", { infoHash });
}

/** Verified pieces saved for this torrent (from any session). */
export async function storedPieces(infoHash: string): Promise<number[]> {
	if (!libraryAvailable()) return [];
	return call("pieces", { infoHash });
}

/** A saved file's bytes, for a download (every piece must be saved), read a piece at a time. */
export async function savedFileBytes(infoHash: string, index: number): Promise<ReadableStream<Uint8Array>> {
	const { file, pieceLength } = await call<{ file: SavedFileRecord; pieceLength: number }>("file", { infoHash, index });
	const load = (piece: number) => call<Uint8Array>("read", { infoHash, start: piece * pieceLength, end: (piece + 1) * pieceLength, pieceLength }).then((b) => (b.length > 0 ? b : undefined));
	return fileBytes(load, file, pieceLength);
}

/** Delete one file's pieces, keeping those another saved file still covers. */
export function deleteSavedFile(infoHash: string, index: number): Promise<void> {
	return deleting(() => call("delete-file", { infoHash, index }));
}

export function deleteSavedTorrent(infoHash: string): Promise<void> {
	return deleting(() => call("delete-torrent", { infoHash }));
}

/** Everything saved in this browser, in one action. */
export function deleteAllSaved(): Promise<void> {
	if (!libraryAvailable()) return Promise.resolve();
	return deleting(() => call("delete-all"));
}
