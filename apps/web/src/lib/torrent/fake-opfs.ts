// An in-memory stand-in for the origin private file system, for tests:
// files with exclusive sync access handles, getFile() snapshots that work
// while a handle is open (as in Chromium), removeEntry refused while one is,
// and an optional quota on the total size (sparse files count their size).

import type { DirHandleLike, FileHandleLike } from "./library-core.ts";
import type { SyncFile } from "./piece-file.ts";

const fail = (name: string, message = name) => new DOMException(message, name);

export class FakeFs {
	/** Total bytes allowed (Infinity: no limit). */
	quota = Infinity;
	readonly root: FakeDir = new FakeDir(this);

	usage(dir: FakeDir = this.root): number {
		let n = 0;
		for (const entry of dir.entries.values()) n += entry instanceof FakeDir ? this.usage(entry) : entry.bytes.length;
		return n;
	}
}

export class FakeFile implements FileHandleLike {
	readonly kind = "file";
	bytes = new Uint8Array(0);
	locked = false;
	constructor(private readonly fs: FakeFs) {}

	async getFile() {
		const snapshot = this.bytes.slice();
		const blob = (data: Uint8Array) => ({ size: data.length, arrayBuffer: async () => data.slice().buffer as ArrayBuffer });
		return { ...blob(snapshot), slice: (start: number, end: number) => blob(snapshot.subarray(start, end)) };
	}

	async createSyncAccessHandle(): Promise<SyncFile> {
		if (this.locked) throw fail("NoModificationAllowedError");
		this.locked = true;
		let open = true;
		const check = () => {
			if (!open) throw fail("InvalidStateError");
		};
		const resize = (size: number) => {
			const grown = this.fs.usage() - this.bytes.length + size;
			if (size > this.bytes.length && grown > this.fs.quota) throw fail("QuotaExceededError");
			const next = new Uint8Array(size);
			next.set(this.bytes.subarray(0, Math.min(size, this.bytes.length)));
			this.bytes = next;
		};
		return {
			read: (buffer, { at }) => {
				check();
				const part = this.bytes.subarray(at, at + buffer.length);
				buffer.set(part);
				return part.length;
			},
			write: (buffer, { at }) => {
				check();
				if (at + buffer.length > this.bytes.length) resize(at + buffer.length);
				this.bytes.set(buffer, at);
				return buffer.length;
			},
			getSize: () => {
				check();
				return this.bytes.length;
			},
			truncate: (size) => {
				check();
				resize(size);
			},
			flush: () => check(),
			close: () => {
				if (open) this.locked = false;
				open = false;
			},
		};
	}
}

export class FakeDir implements DirHandleLike {
	readonly kind = "directory";
	readonly entries = new Map<string, FakeFile | FakeDir>();
	constructor(private readonly fs: FakeFs) {}

	async getFileHandle(name: string, options: { create?: boolean } = {}): Promise<FakeFile> {
		const entry = this.entries.get(name);
		if (entry instanceof FakeFile) return entry;
		if (entry) throw fail("TypeMismatchError");
		if (!options.create) throw fail("NotFoundError");
		const file = new FakeFile(this.fs);
		this.entries.set(name, file);
		return file;
	}

	async getDirectoryHandle(name: string, options: { create?: boolean } = {}): Promise<FakeDir> {
		const entry = this.entries.get(name);
		if (entry instanceof FakeDir) return entry;
		if (entry) throw fail("TypeMismatchError");
		if (!options.create) throw fail("NotFoundError");
		const dir = new FakeDir(this.fs);
		this.entries.set(name, dir);
		return dir;
	}

	async removeEntry(name: string): Promise<void> {
		const entry = this.entries.get(name);
		if (!entry) throw fail("NotFoundError");
		if (entry instanceof FakeFile && entry.locked) throw fail("NoModificationAllowedError");
		this.entries.delete(name);
	}

	async *keys(): AsyncIterable<string> {
		yield* [...this.entries.keys()];
	}
}
