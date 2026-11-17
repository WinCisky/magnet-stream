// v1 multi-file, v2-only and hybrid info dicts (from the fake peer's
// deterministic torrents), listings, hash checks and path sanitising.

import { describe, expect, it } from "vitest";
import { bencode, makeTorrent } from "@magnet-stream/fake-peer";
import { MetainfoError, fileRange, listing, parseInfoDict, pieceSize, sanitizeComponent } from "../src/metainfo.ts";
import { sha1, toHex } from "./helpers.ts";

const files = [
	{ path: ["Season 1", "ep1.mkv"], length: 300_000 },
	{ path: ["a.srt"], length: 1_000 },
	{ path: ["empty.txt"], length: 0 },
	{ path: ["Season 1", "ep2.mkv"], length: 300_000 },
	{ path: ["poster.jpg"], length: 70_000 },
];
const hex = (b?: Uint8Array) => (b ? Buffer.from(b).toString("hex") : undefined);

describe("info dicts of every version", () => {
	it("v1 multi-file: listing as magnet-seeders' /files (largest first, stable, relative paths)", async () => {
		const t = makeTorrent({ seed: 9, files, pieceLength: 32 * 1024 });
		const meta = await parseInfoDict(t.info, t.infoHashHex);
		expect(meta.version).toBe("v1");
		expect(listing(meta)).toEqual({
			infoHash: t.infoHashHex,
			name: "fake-9",
			size: 671_000,
			pieceLength: 32 * 1024,
			files: [
				{ path: "Season 1/ep1.mkv", size: 300_000, offset: 0 },
				{ path: "Season 1/ep2.mkv", size: 300_000, offset: 301_000 },
				{ path: "poster.jpg", size: 70_000, offset: 601_000 },
				{ path: "a.srt", size: 1_000, offset: 300_000 },
				{ path: "empty.txt", size: 0, offset: 301_000 },
			],
		});
	});

	it("v1 single file: the name is the path", async () => {
		const t = makeTorrent({ seed: 9, size: 100_000, pieceLength: 32 * 1024 });
		const meta = await parseInfoDict(t.info, t.infoHashHex);
		expect(listing(meta).files).toEqual([{ path: "fake-9.bin", size: 100_000, offset: 0 }]);
	});

	it("v2-only: accepted only if SHA-256 matches; files from the file tree, aligned to pieces", async () => {
		const t = makeTorrent({ seed: 9, files, pieceLength: 32 * 1024, version: 2 });
		const meta = await parseInfoDict(t.info, { v2: hex(t.infoHashV2) });
		expect(meta.version).toBe("v2");
		expect(meta.infoHash).toBe(t.infoHashHex);
		expect(meta.infoHashV2).toBe(hex(t.infoHashV2));
		expect(meta.infoHashV1).toBeUndefined();
		expect(meta.pieceHashes).toHaveLength(0);
		expect(meta.files.map((f) => [f.path, f.offset, f.length, hex(f.piecesRoot)])).toEqual(t.files.map((f) => [f.path, f.offset, f.length, hex(f.piecesRoot)]));
		expect(meta.pieceCount).toBe(t.pieceCount);
		for (let p = 0; p < t.pieceCount; p++) expect(pieceSize(meta, p), `piece ${p}`).toBe(t.pieceSize(p));
		// The truncated hash on its own works too (a v2 magnet's wire hash).
		expect((await parseInfoDict(t.info, t.infoHashHex)).version).toBe("v2");
		// SHA-1 of a v2 dict is not its info-hash.
		await expect(parseInfoDict(t.info, { v1: toHex(await sha1(t.info)) })).rejects.toThrow(MetainfoError);
		await expect(parseInfoDict(t.info, { v2: "0".repeat(64) })).rejects.toThrow(MetainfoError);
		const ep2 = meta.files.find((f) => f.path === "Season 1/ep2.mkv")!;
		expect(fileRange(meta, { path: ep2.path, offset: ep2.offset, size: ep2.length }).firstPiece).toBe(ep2.offset / meta.pieceLength);
	});

	it("hybrid: both hashes are checked, padding files are left out of the listing", async () => {
		const t = makeTorrent({ seed: 9, files, pieceLength: 32 * 1024, version: "hybrid" });
		const meta = await parseInfoDict(t.info, { v1: t.infoHashHex, v2: hex(t.infoHashV2) });
		expect(meta.version).toBe("hybrid");
		expect([meta.infoHashV1, meta.infoHashV2]).toEqual([t.infoHashHex, hex(t.infoHashV2)]);
		expect(listing(meta).files.every((f) => !f.path.startsWith(".pad"))).toBe(true);
		expect(meta.files.filter((f) => f.padding)).toHaveLength(t.files.filter((f) => f.pad).length);
		expect(meta.files.filter((f) => !f.padding && f.length > 0).every((f) => f.piecesRoot?.length === 32)).toBe(true);
		await expect(parseInfoDict(t.info, { v1: t.infoHashHex, v2: "1".repeat(64) })).rejects.toThrow(MetainfoError);
		await expect(parseInfoDict(t.info, { v1: "2".repeat(40), v2: hex(t.infoHashV2) })).rejects.toThrow(MetainfoError);
	});

	it("sanitises paths", async () => {
		expect(sanitizeComponent("..")).toBe("_");
		expect(sanitizeComponent("a/b\\c\u0000d")).toBe("a_b_c_d");
		expect(sanitizeComponent("x".repeat(300))).toHaveLength(255);
		const info = bencode({
			name: "..",
			"piece length": 16384,
			pieces: new Uint8Array(20),
			files: [{ length: 10, path: ["..", "..", "etc\u0007", "passwd"] }],
		});
		const meta = await parseInfoDict(info, toHex(await sha1(info)));
		expect(meta.name).toBe("_");
		expect(meta.files[0].path).toBe("_/_/etc_/passwd");
	});

	it("enforces limits", async () => {
		const t = makeTorrent({ seed: 9, files, pieceLength: 32 * 1024 });
		await expect(parseInfoDict(t.info, t.infoHashHex, { maxFiles: 3 })).rejects.toThrow(/file list/);
		await expect(parseInfoDict(t.info, t.infoHashHex, { maxPieceLength: 16 * 1024 })).rejects.toThrow(/piece length/);
	});
});
