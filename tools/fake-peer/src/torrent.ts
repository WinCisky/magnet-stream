// Deterministic test torrents: the same (seed, size, piece length) always
// gives the same bytes, info dict and info-hash. Content is pseudo-random
// noise derived from the seed: never real data.

import { createHash } from "node:crypto";
import { bencode } from "./bencode.ts";

export interface TorrentOptions {
	/** PRNG seed (default 1). */
	seed?: number;
	/** Total length in bytes (default 8 MiB). */
	size?: number;
	/** Piece length in bytes (default 256 KiB). */
	pieceLength?: number;
	/** Torrent name (default `fake-<seed>.bin`). */
	name?: string;
}

export interface FakeTorrent {
	seed: number;
	name: string;
	data: Uint8Array;
	totalLength: number;
	pieceLength: number;
	pieceCount: number;
	/** SHA-1 of each piece. */
	hashes: Uint8Array[];
	/** Concatenated piece hashes, as in the info dict. */
	pieceHashes: Uint8Array;
	/** The bencoded info dict (what ut_metadata serves). */
	info: Uint8Array;
	/** SHA-1 of `info`. */
	infoHash: Uint8Array;
	infoHashHex: string;
	/** `magnet:?xt=urn:btih:...&dn=...` */
	magnet: string;
}

export const MiB = 1024 * 1024;

export function makeTorrent({ seed = 1, size = 8 * MiB, pieceLength = 256 * 1024, name }: TorrentOptions = {}): FakeTorrent {
	if (!Number.isSafeInteger(size) || size <= 0) throw new RangeError("size must be a positive integer");
	if (!Number.isSafeInteger(pieceLength) || pieceLength <= 0) throw new RangeError("pieceLength must be a positive integer");
	if (!Number.isInteger(seed)) throw new RangeError("seed must be an integer");
	const data = new Uint8Array(size);
	// xorshift32 over 4 bytes at a time: fast enough for hundreds of MiB.
	let x = (seed >>> 0) ^ 0x9e3779b9 || 1;
	const words = Math.floor(size / 4);
	const view = new DataView(data.buffer);
	for (let i = 0; i < words; i++) {
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		view.setUint32(i * 4, x >>> 0);
	}
	for (let i = words * 4; i < size; i++) {
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		data[i] = x & 0xff;
	}

	const pieceCount = Math.ceil(size / pieceLength);
	const hashes: Uint8Array[] = [];
	const pieceHashes = new Uint8Array(pieceCount * 20);
	for (let p = 0; p < pieceCount; p++) {
		const digest = new Uint8Array(createHash("sha1").update(data.subarray(p * pieceLength, Math.min(size, (p + 1) * pieceLength))).digest());
		hashes.push(digest);
		pieceHashes.set(digest, p * 20);
	}
	const torrentName = name ?? `fake-${seed}.bin`;
	const info = bencode({ length: size, name: torrentName, "piece length": pieceLength, pieces: pieceHashes });
	const infoHash = new Uint8Array(createHash("sha1").update(info).digest());
	const infoHashHex = Buffer.from(infoHash).toString("hex");
	return {
		seed,
		name: torrentName,
		data,
		totalLength: size,
		pieceLength,
		pieceCount,
		hashes,
		pieceHashes,
		info,
		infoHash,
		infoHashHex,
		magnet: magnetFor(infoHashHex, torrentName),
	};
}

/** A magnet link; `peer` adds an `x.pe` hint (BEP 9). */
export function magnetFor(infoHashHex: string, name: string, peer?: string): string {
	let magnet = `magnet:?xt=urn:btih:${infoHashHex}&dn=${encodeURIComponent(name)}`;
	if (peer) magnet += `&x.pe=${encodeURIComponent(peer)}`;
	return magnet;
}

/** `8MiB`, `512KiB`, `1GiB`, `1000` → bytes. */
export function parseSize(text: string): number {
	const m = /^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|KB|MB|GB)?$/i.exec(text.trim());
	if (!m) throw new RangeError(`bad size "${text}"`);
	const unit = (m[2] ?? "B").toLowerCase();
	const scale: Record<string, number> = { b: 1, kib: 1024, mib: MiB, gib: 1024 * MiB, kb: 1e3, mb: 1e6, gb: 1e9 };
	const n = Math.round(Number(m[1]) * scale[unit]);
	if (!Number.isSafeInteger(n) || n <= 0) throw new RangeError(`bad size "${text}"`);
	return n;
}
