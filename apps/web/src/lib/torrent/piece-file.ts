// One torrent's verified pieces in two OPFS files (docs/architecture.md
// §Storage and database, decision OQ-13): a sparse data file where piece p
// sits at byte p × pieceLength (the torrent's own layout, so a file of the
// torrent is one byte range of it), and a bitmap of the verified pieces
// (bit p of byte p >> 3, most significant first, as a BitTorrent bitfield).
//
// A piece's bit is set only after its bytes are written and flushed, so an
// interrupted write or a quota error leaves the piece missing, never a
// verified piece with wrong bytes. Reads return verified bytes only.
//
// Synchronous: it runs in the engine worker over FileSystemSyncAccessHandle
// (anything with the same four methods will do, e.g. in tests).

/** The part of FileSystemSyncAccessHandle used here. */
export interface SyncFile {
	read(buffer: Uint8Array, options: { at: number }): number;
	write(buffer: Uint8Array, options: { at: number }): number;
	getSize(): number;
	truncate(size: number): void;
	flush(): void;
	close(): void;
}

export interface PieceLayout {
	pieceLength: number;
	pieceCount: number;
	/** Bytes of a piece (the last one, or v2 pieces at a file's end, are shorter). */
	pieceSize(piece: number): number;
}

export type StoreErrorCode = "quota" | "unverified" | "range" | "io" | "unavailable" | "busy";

export class StoreError extends Error {
	readonly code: StoreErrorCode;
	constructor(code: StoreErrorCode, message: string, options?: { cause?: unknown }) {
		super(message, options);
		this.name = "StoreError";
		this.code = code;
	}
}

/** Largest block the upload path may read (BEP 3 requests). */
export const MAX_READ_BLOCK = 16 * 1024;

/** OPFS directory and file names of a torrent: from the validated hex info-hash only, never from torrent names. */
export const STORE_DIR = "pieces";
export function storeNames(infoHash: string): { data: string; map: string; record: string } {
	if (!/^[0-9a-f]{40}$/.test(infoHash)) throw new StoreError("range", "not an info-hash");
	return { data: `${infoHash}.data`, map: `${infoHash}.map`, record: `${infoHash}.json` };
}

export function hasBit(bits: Uint8Array, piece: number): boolean {
	return piece >= 0 && (piece >> 3) < bits.length && (bits[piece >> 3] & (0x80 >> (piece & 7))) !== 0;
}

/** Pieces whose bit is set, ascending. */
export function bitsToPieces(bits: Uint8Array, limit = bits.length * 8): number[] {
	const out: number[] = [];
	for (let i = 0; i < bits.length; i++) {
		if (bits[i] === 0) continue;
		for (let b = 0; b < 8; b++) {
			const p = i * 8 + b;
			if (p < limit && bits[i] & (0x80 >> b)) out.push(p);
		}
	}
	return out;
}

/** The pieces covering torrent bytes [offset, offset + length). */
export function piecesOf(offset: number, length: number, pieceLength: number): [number, number] {
	const first = Math.floor(offset / pieceLength);
	return [first, Math.max(first, Math.floor((offset + Math.max(length, 1) - 1) / pieceLength))];
}

export class PieceFile {
	readonly layout: PieceLayout;
	private readonly data: SyncFile;
	private readonly map: SyncFile;
	private bits: Uint8Array;
	private count = 0;
	private closed = false;

	constructor(data: SyncFile, map: SyncFile, layout: PieceLayout) {
		if (!(layout.pieceLength > 0) || !(layout.pieceCount > 0)) throw new StoreError("range", "bad layout");
		this.data = data;
		this.map = map;
		this.layout = layout;
		this.bits = new Uint8Array(Math.ceil(layout.pieceCount / 8));
		const size = Math.min(map.getSize(), this.bits.length);
		if (size > 0) map.read(this.bits.subarray(0, size), { at: 0 });
		// Bits past the last piece (a map from a bad write) are ignored.
		const spare = this.bits.length * 8 - layout.pieceCount;
		if (spare > 0) this.bits[this.bits.length - 1] &= 0xff << spare;
		this.count = bitsToPieces(this.bits, layout.pieceCount).length;
	}

	/** Verified pieces stored. */
	get size(): number {
		return this.count;
	}

	has(piece: number): boolean {
		return Number.isInteger(piece) && piece < this.layout.pieceCount && hasBit(this.bits, piece);
	}

	pieces(): number[] {
		return bitsToPieces(this.bits, this.layout.pieceCount);
	}

	/** A copy of the verified-piece bitfield (what M3-12 sends to peers). */
	bitfield(): Uint8Array {
		return this.bits.slice();
	}

	/**
	 * Store a verified piece. Throws StoreError("quota") when the browser
	 * refuses the bytes; the piece then stays missing.
	 */
	write(piece: number, bytes: Uint8Array): void {
		this.open();
		const size = this.checkPiece(piece);
		if (bytes.length !== size) throw new StoreError("range", `piece ${piece} is ${size} bytes, got ${bytes.length}`);
		if (this.has(piece)) return;
		guard(() => {
			const written = this.data.write(bytes, { at: piece * this.layout.pieceLength });
			if (written !== bytes.length) throw new StoreError("quota", "short write");
			this.data.flush();
		});
		const at = piece >> 3;
		const byte = this.bits[at] | (0x80 >> (piece & 7));
		guard(() => {
			if (this.map.write(Uint8Array.of(byte), { at }) !== 1) throw new StoreError("quota", "short write");
			this.map.flush();
		});
		this.bits[at] = byte;
		this.count++;
	}

	/** Bytes of a verified piece. */
	readPiece(piece: number): Uint8Array {
		return this.readBlock(piece, 0, this.checkPiece(piece), Infinity);
	}

	/**
	 * A block of a verified piece (the upload path, M3-12): at most 16 KiB,
	 * within the piece. Anything else throws, and nothing unverified is read.
	 */
	readBlock(piece: number, begin: number, length: number, max = MAX_READ_BLOCK): Uint8Array {
		this.open();
		const size = this.checkPiece(piece);
		if (!Number.isInteger(begin) || !Number.isInteger(length) || begin < 0 || length < 1 || length > max || begin + length > size) {
			throw new StoreError("range", "block outside the piece");
		}
		if (!this.has(piece)) throw new StoreError("unverified", `piece ${piece} is not verified`);
		const out = new Uint8Array(length);
		const n = guard(() => this.data.read(out, { at: piece * this.layout.pieceLength + begin }));
		if (n !== length) throw new StoreError("io", "short read");
		return out;
	}

	/**
	 * Torrent bytes [offset, offset + length): the bytes if every piece they
	 * touch is verified, otherwise the missing pieces and no bytes.
	 */
	readRange(offset: number, length: number): { bytes: Uint8Array; missing: [] } | { bytes: null; missing: number[] } {
		this.open();
		const total = (this.layout.pieceCount - 1) * this.layout.pieceLength + this.layout.pieceSize(this.layout.pieceCount - 1);
		if (!Number.isInteger(offset) || !Number.isInteger(length) || offset < 0 || length < 0 || offset + length > total) {
			throw new StoreError("range", "range outside the torrent");
		}
		if (length === 0) return { bytes: new Uint8Array(0), missing: [] };
		const [first, last] = piecesOf(offset, length, this.layout.pieceLength);
		const missing: number[] = [];
		for (let p = first; p <= last; p++) if (!this.has(p)) missing.push(p);
		if (missing.length > 0) return { bytes: null, missing };
		const out = new Uint8Array(length);
		const n = guard(() => this.data.read(out, { at: offset }));
		if (n !== length) throw new StoreError("io", "short read");
		return { bytes: out, missing: [] };
	}

	/** Forget pieces (their bytes stay until the file is removed: OPFS can't punch holes). */
	clear(pieces: Iterable<number>): void {
		this.open();
		const touched = new Set<number>();
		for (const p of pieces) {
			if (!this.has(p)) continue;
			this.bits[p >> 3] &= ~(0x80 >> (p & 7));
			this.count--;
			touched.add(p >> 3);
		}
		for (const at of touched) guard(() => this.map.write(this.bits.subarray(at, at + 1), { at }));
		if (touched.size > 0) guard(() => this.map.flush());
		// Trailing pieces gone: give their space back.
		const last = this.pieces().at(-1);
		const end = last === undefined ? 0 : last * this.layout.pieceLength + this.layout.pieceSize(last);
		if (this.data.getSize() > end) guard(() => this.data.truncate(end));
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		try {
			this.data.close();
		} finally {
			this.map.close();
		}
	}

	get isClosed(): boolean {
		return this.closed;
	}

	private open(): void {
		if (this.closed) throw new StoreError("io", "the piece store is closed");
	}

	private checkPiece(piece: number): number {
		if (!Number.isInteger(piece) || piece < 0 || piece >= this.layout.pieceCount) throw new StoreError("range", "no such piece");
		const size = this.layout.pieceSize(piece);
		if (!(size > 0) || size > this.layout.pieceLength) throw new StoreError("range", "no such piece");
		return size;
	}
}

/** Turn platform errors into StoreErrors (QuotaExceededError → "quota"). */
export function guard<T>(fn: () => T): T {
	try {
		return fn();
	} catch (e) {
		throw toStoreError(e);
	}
}

export function toStoreError(e: unknown): StoreError {
	if (e instanceof StoreError) return e;
	const name = (e as { name?: unknown } | null)?.name;
	if (name === "QuotaExceededError") return new StoreError("quota", "the browser's storage is full", { cause: e });
	if (name === "NoModificationAllowedError" || name === "InvalidStateError") return new StoreError("busy", "the saved video is in use", { cause: e });
	if (name === "SecurityError" || name === "NotAllowedError") return new StoreError("unavailable", "this browser doesn't let the page save anything", { cause: e });
	return new StoreError("io", e instanceof Error ? e.message : "storage error", { cause: e });
}
