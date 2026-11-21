// The engine worker's piece store: a torrent's data and map files in OPFS
// (piece-file.ts) opened with FileSystemSyncAccessHandle, which only a
// dedicated worker can do. Only one handle per file may be open at a time,
// so the worker closes them when it has nothing to write (see
// session.worker.ts); the library reads with getFile() meanwhile.

import { PieceFile, STORE_DIR, storeNames, toStoreError, type PieceLayout, type SyncFile } from "./piece-file.ts";

interface Syncable {
	createSyncAccessHandle(): Promise<SyncFile>;
}

export async function openOpfsStore(infoHash: string, layout: PieceLayout): Promise<PieceFile> {
	const names = storeNames(infoHash);
	let data: SyncFile | undefined;
	try {
		const root = await navigator.storage.getDirectory();
		const dir = await root.getDirectoryHandle(STORE_DIR, { create: true });
		// Worker-only API, absent from the DOM typings.
		const dataHandle = (await dir.getFileHandle(names.data, { create: true })) as unknown as Syncable;
		const mapHandle = (await dir.getFileHandle(names.map, { create: true })) as unknown as Syncable;
		const opened = await dataHandle.createSyncAccessHandle();
		data = opened;
		const map = await mapHandle.createSyncAccessHandle();
		return new PieceFile(opened, map, layout);
	} catch (e) {
		data?.close();
		throw toStoreError(e);
	}
}
