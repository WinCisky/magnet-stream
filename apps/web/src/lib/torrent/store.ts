// Where the page reads verified pieces from. Saved videos (decision OQ-13)
// live in OPFS, written by the session worker as soon as a piece is
// verified (opfs-store.ts); the page reads them back through the session.
// Pieces the worker couldn't save (the quota, say) and files that are never
// saved (a subtitle fetched whole) stay in memory.

import { storedPieces } from "./library";
import type { PageSession } from "./page-session";

export interface PieceStore {
	readonly kind: "opfs" | "memory";
	/** Pieces already stored (all verified before being stored). */
	list(): Promise<number[]>;
	/** Keep a piece (in memory: the worker saves to OPFS itself). */
	put(piece: number, data: Uint8Array): Promise<void>;
	get(piece: number): Promise<Uint8Array | undefined>;
}

export class MemoryPieceStore implements PieceStore {
	readonly kind: "opfs" | "memory" = "memory";
	private readonly pieces = new Map<number, Uint8Array>();

	async list(): Promise<number[]> {
		return [...this.pieces.keys()];
	}

	async put(piece: number, data: Uint8Array): Promise<void> {
		this.pieces.set(piece, data);
	}

	async get(piece: number): Promise<Uint8Array | undefined> {
		return this.pieces.get(piece);
	}
}

/** A torrent's OPFS pieces, read through the session worker; pieces it couldn't save, in memory. */
export class SessionPieceStore implements PieceStore {
	readonly kind = "opfs";
	private readonly memory = new MemoryPieceStore();

	constructor(
		private readonly session: Pick<PageSession, "readPiece">,
		private readonly infoHash: string,
	) {}

	async list(): Promise<number[]> {
		const [saved, kept] = await Promise.all([storedPieces(this.infoHash), this.memory.list()]);
		return [...new Set([...saved, ...kept])];
	}

	put(piece: number, data: Uint8Array): Promise<void> {
		return this.memory.put(piece, data);
	}

	async get(piece: number): Promise<Uint8Array | undefined> {
		return (await this.memory.get(piece)) ?? this.session.readPiece(piece);
	}
}

/**
 * The store, with each piece readable from memory until its put settles:
 * a player may ask for a piece the moment it's verified.
 */
export function readableWhileStoring(store: PieceStore): PieceStore {
	const storing = new Map<number, Uint8Array>();
	return {
		kind: store.kind,
		list: () => store.list(),
		get: async (piece) => storing.get(piece) ?? store.get(piece),
		put: async (piece, data) => {
			storing.set(piece, data);
			try {
				await store.put(piece, data);
			} finally {
				if (storing.get(piece) === data) storing.delete(piece);
			}
		},
	};
}
