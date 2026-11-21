// What the page knows about one file's pieces while the engine's deadline
// picker (in the session worker) recovers them: which are verified, which
// are being assembled, where the viewer is. It decides nothing about
// requests; it feeds the piece strip, the stream and the snapshot.

import { DEFAULT_BITRATE, DEFAULT_PICKER_CONFIG } from "@magnet-stream/engine/picker";
import type { FileRange } from "@magnet-stream/engine/metainfo";
import { PieceState } from "./scheduler";

const FAILED_FLASH_MS = 2_000;
/** The picker's default playback window (30 s at the provisional bitrate), until M3-06 sets the bitrate. */
export const DEFAULT_WINDOW_BYTES = DEFAULT_PICKER_CONFIG.windowSeconds * DEFAULT_BITRATE;

export class PieceTracker {
	readonly first: number;
	readonly last: number;
	/** Piece the viewer last jumped to. */
	cursor: number;
	/** First unverified piece at or after the cursor: where playback would stall. */
	front: number;
	seeks = 0;
	verifiedCount = 0;
	private readonly verified: Uint8Array;
	/** Pieces being assembled: [blocks received, blocks]. */
	private assembling = new Map<number, [number, number]>();
	private readonly failedAt = new Map<number, number>();
	private readonly pieceLength: number;
	private readonly range: FileRange;

	constructor(range: FileRange, pieceLength: number) {
		this.range = range;
		this.pieceLength = pieceLength;
		this.first = range.firstPiece;
		this.last = range.lastPiece;
		this.cursor = this.first;
		this.front = this.first;
		this.verified = new Uint8Array(this.last - this.first + 1);
	}

	get total(): number {
		return this.verified.length;
	}

	get complete(): boolean {
		return this.verifiedCount === this.total;
	}

	/** Pieces being assembled right now. */
	get active(): number {
		return this.assembling.size;
	}

	isVerified(piece: number): boolean {
		return piece >= this.first && piece <= this.last && this.verified[piece - this.first] === 1;
	}

	/** Returns whether the piece is new. */
	markVerified(piece: number): boolean {
		if (piece < this.first || piece > this.last || this.verified[piece - this.first]) return false;
		this.verified[piece - this.first] = 1;
		this.verifiedCount++;
		this.assembling.delete(piece);
		this.advanceFront();
		return true;
	}

	/** A verified piece is gone from storage. Returns whether it was verified. */
	forget(piece: number): boolean {
		if (!this.isVerified(piece)) return false;
		this.verified[piece - this.first] = 0;
		this.verifiedCount--;
		if (piece >= this.cursor && piece < this.front) this.front = piece;
		return true;
	}

	/** Verified pieces of the file. */
	verifiedPieces(): number[] {
		const out: number[] = [];
		for (let i = 0; i < this.verified.length; i++) if (this.verified[i]) out.push(this.first + i);
		return out;
	}

	/** The viewer jumped to `piece`. */
	setCursor(piece: number): void {
		this.cursor = Math.min(this.last, Math.max(this.first, piece));
		this.seeks++;
		this.front = this.cursor;
		this.advanceFront();
	}

	/** Whether `piece` lies in the picker's window from the cursor (it's being fetched first anyway). */
	inWindow(piece: number, windowBytes = DEFAULT_WINDOW_BYTES): boolean {
		const pieces = Math.max(1, Math.ceil(windowBytes / this.pieceLength));
		return piece >= this.cursor && piece < this.cursor + pieces;
	}

	/** Flat [piece, received, blocks] triples from the worker. Pieces that left the list unverified failed or were dropped. */
	setAssembling(flat: readonly number[], now = Date.now()): void {
		const next = new Map<number, [number, number]>();
		for (let i = 0; i + 2 < flat.length; i += 3) {
			const piece = flat[i];
			if (piece < this.first || piece > this.last || this.isVerified(piece)) continue;
			next.set(piece, [Math.min(flat[i + 1], flat[i + 2]), Math.max(1, flat[i + 2])]);
		}
		for (const [piece, [got, blocks]] of this.assembling) {
			// It was complete and is gone without being verified: it failed its hash check.
			if (got >= blocks && !next.has(piece) && !this.isVerified(piece)) this.failedAt.set(piece, now);
		}
		this.assembling = next;
	}

	/** Display state (and 0–255 progress) of every piece in the range. */
	fillStates(states: Uint8Array, progress: Uint8Array, now = Date.now()): void {
		for (let i = 0; i < this.verified.length; i++) {
			const piece = this.first + i;
			const work = this.assembling.get(piece);
			progress[i] = work ? Math.floor((255 * work[0]) / work[1]) : 0;
			if (this.verified[i]) states[i] = PieceState.Verified;
			else if (work && work[0] >= work[1]) states[i] = PieceState.Verifying;
			else if ((this.failedAt.get(piece) ?? -Infinity) > now - FAILED_FLASH_MS) states[i] = PieceState.Failed;
			else if (work) states[i] = PieceState.Downloading;
			else states[i] = PieceState.Pending;
		}
	}

	/** Verified bytes of the file available contiguously from its start. */
	contiguousFromStart(): number {
		let piece = this.first;
		while (piece <= this.last && this.isVerified(piece)) piece++;
		const end = piece * this.pieceLength;
		const { offset, size } = this.range;
		return Math.max(0, Math.min(end, offset + size) - offset);
	}

	private advanceFront(): void {
		while (this.front <= this.last && this.isVerified(this.front)) this.front++;
	}
}
