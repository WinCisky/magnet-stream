// Parsing and validating `/v1/blocks` queries, and the block arithmetic.

import type { Block } from "./wire";

export const BLOCK_SIZE = 16 * 1024;
export const MAX_BLOCKS = 2000;
/** BEP 10: assume this queue depth when a peer doesn't advertise `reqq`. */
export const DEFAULT_REQQ = 250;
/**
 * Each candidate costs one connect, and a free-plan invocation may make at
 * most 50 subrequests: stay well below.
 */
export const MAX_CANDIDATES = 40;
export const DEFAULT_WAIT_MS = 8_000;
const MIN_WAIT_MS = 1_000;
const MAX_WAIT_MS = 30_000;
const MIN_HOLD_MS = 500;
const MAX_PIECE_LENGTH = 64 * 1024 * 1024;

export interface Candidate {
	/** Exactly as signed by magnet-seeders: `1.2.3.4:6881` or `[2001:db8::1]:6881`. */
	addr: string;
	hostname: string;
	port: number;
	exp: number;
	token: string;
}

/** Blocks `first..=last` of `piece`. */
export interface BlockRange {
	piece: number;
	first: number;
	last: number;
}

export interface BlocksRequest {
	infoHash: Uint8Array;
	infoHashHex: string;
	geometry: Geometry;
	want: BlockRange[];
	candidates: Candidate[];
	waitMs: number;
	/** How long a handshaken but choked peer keeps its slot while others wait. */
	holdMs: number;
	/** How long the first candidate is tried alone (0: all at once). */
	staggerMs: number;
}

export class ParamError extends Error {}

export class Geometry {
	readonly pieceCount: number;

	constructor(
		readonly pieceLength: number,
		readonly totalLength: number,
	) {
		this.pieceCount = Math.ceil(totalLength / pieceLength);
	}

	pieceSize(piece: number): number {
		return piece < this.pieceCount - 1
			? this.pieceLength
			: this.totalLength - this.pieceLength * (this.pieceCount - 1);
	}

	blockCount(piece: number): number {
		return Math.ceil(this.pieceSize(piece) / BLOCK_SIZE);
	}

	block(piece: number, index: number): Block {
		const begin = index * BLOCK_SIZE;
		return [piece, begin, Math.min(BLOCK_SIZE, this.pieceSize(piece) - begin)];
	}
}

export function parseBlocksRequest(url: URL): BlocksRequest {
	const q = url.searchParams;
	const infoHashHex = (q.get("ih") ?? "").toLowerCase();
	if (!/^[0-9a-f]{40}$/.test(infoHashHex)) throw new ParamError("ih must be a 40-char hex info-hash");

	const pieceLength = integer(q.get("pl"), "pl", 1, MAX_PIECE_LENGTH);
	const totalLength = integer(q.get("len"), "len", 1, Number.MAX_SAFE_INTEGER);
	const geometry = new Geometry(pieceLength, totalLength);

	const want = parseWant(q.get("want") ?? "", geometry);

	const rawCandidates = q.getAll("c");
	if (rawCandidates.length === 0 || rawCandidates.length > MAX_CANDIDATES) {
		throw new ParamError(`give 1 to ${MAX_CANDIDATES} candidates (c=addr|exp|token)`);
	}
	const candidates = rawCandidates.map(parseCandidate);

	const wait = q.get("wait");
	const waitMs = wait === null ? DEFAULT_WAIT_MS : integer(wait, "wait", MIN_WAIT_MS, MAX_WAIT_MS);
	const hold = q.get("hold");
	const holdMs = hold === null ? waitMs : integer(hold, "hold", MIN_HOLD_MS, MAX_WAIT_MS);
	const stagger = q.get("stagger");
	const staggerMs = stagger === null ? 0 : integer(stagger, "stagger", 0, MAX_WAIT_MS);

	return { infoHash: hexToBytes(infoHashHex), infoHashHex, geometry, want, candidates, waitMs, holdMs, staggerMs };
}

/** `12,13,14:0-63,15:7` → whole pieces, block ranges, single blocks. */
export function parseWant(spec: string, geometry: Geometry): BlockRange[] {
	if (!spec) throw new ParamError("want is required");
	let total = 0;
	const ranges = spec.split(",").map((item) => {
		const m = /^(\d+)(?::(\d+)(?:-(\d+))?)?$/.exec(item);
		if (!m) throw new ParamError(`bad want item "${item}"`);
		const piece = Number(m[1]);
		if (piece >= geometry.pieceCount) throw new ParamError(`piece ${piece} out of range`);
		const blocks = geometry.blockCount(piece);
		const first = m[2] === undefined ? 0 : Number(m[2]);
		const last = m[2] === undefined ? blocks - 1 : m[3] === undefined ? first : Number(m[3]);
		if (first > last || last >= blocks) throw new ParamError(`bad block range in "${item}"`);
		total += last - first + 1;
		return { piece, first, last };
	});
	if (total > MAX_BLOCKS) throw new ParamError(`at most ${MAX_BLOCKS} blocks per request`);
	return ranges;
}

/** Wanted blocks the peer has, in the order asked, capped at its `reqq`. */
export function planBlocks(
	req: Pick<BlocksRequest, "want" | "geometry">,
	peer: { reqq: number | undefined; hasPiece(piece: number): boolean },
): Block[] {
	const cap = Math.min(MAX_BLOCKS, peer.reqq ?? DEFAULT_REQQ);
	const blocks: Block[] = [];
	for (const { piece, first, last } of req.want) {
		if (!peer.hasPiece(piece)) continue;
		for (let b = first; b <= last && blocks.length < cap; b++) {
			blocks.push(req.geometry.block(piece, b));
		}
	}
	return blocks;
}

/** Inverse of `parseWant` for the blocks actually requested. */
export function formatWant(blocks: readonly Block[], geometry: Geometry): string {
	const parts: string[] = [];
	let i = 0;
	while (i < blocks.length) {
		const piece = blocks[i][0];
		const first = blocks[i][1] / BLOCK_SIZE;
		let last = first;
		while (
			i + 1 < blocks.length &&
			blocks[i + 1][0] === piece &&
			blocks[i + 1][1] / BLOCK_SIZE === last + 1
		) {
			i++;
			last++;
		}
		i++;
		if (first === 0 && last === geometry.blockCount(piece) - 1) parts.push(`${piece}`);
		else if (first === last) parts.push(`${piece}:${first}`);
		else parts.push(`${piece}:${first}-${last}`);
	}
	return parts.join(",");
}

function parseCandidate(raw: string): Candidate {
	const [addr, exp, token, ...rest] = raw.split("|");
	if (rest.length > 0 || !addr || !exp || !token) throw new ParamError(`bad candidate "${raw}"`);
	const m = /^(?:\[([0-9a-fA-F:.]+)\]|(\d{1,3}(?:\.\d{1,3}){3})):(\d{1,5})$/.exec(addr);
	if (!m) throw new ParamError(`bad peer address "${addr}"`);
	const port = Number(m[3]);
	if (port < 1 || port > 65535) throw new ParamError(`bad port in "${addr}"`);
	if (!/^[A-Za-z0-9_-]{43}$/.test(token)) throw new ParamError("bad token");
	return {
		addr,
		// connect() needs IPv6 literals bracketed, or `::1:6881` is ambiguous.
		hostname: m[1] ? `[${m[1]}]` : m[2],
		port,
		exp: integer(exp, "exp", 0, Number.MAX_SAFE_INTEGER),
		token,
	};
}

function integer(raw: string | null, name: string, min: number, max: number): number {
	if (raw === null || !/^\d+$/.test(raw)) throw new ParamError(`${name} must be an integer`);
	const n = Number(raw);
	if (!Number.isSafeInteger(n) || n < min || n > max) {
		throw new ParamError(`${name} must be between ${min} and ${max}`);
	}
	return n;
}

function hexToBytes(hex: string): Uint8Array {
	const out = new Uint8Array(hex.length / 2);
	for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	return out;
}
