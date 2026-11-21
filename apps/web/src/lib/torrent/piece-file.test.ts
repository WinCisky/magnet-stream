import { describe, expect, it } from "vitest";
import { FakeFs } from "./fake-opfs";
import { PieceFile, StoreError, bitsToPieces, piecesOf, storeNames } from "./piece-file";

const PL = 64 * 1024;

async function open(fs: FakeFs, pieceCount = 10, last = PL) {
	const dir = await fs.root.getDirectoryHandle("pieces", { create: true });
	const data = await (await dir.getFileHandle("x.data", { create: true })).createSyncAccessHandle();
	const map = await (await dir.getFileHandle("x.map", { create: true })).createSyncAccessHandle();
	const layout = { pieceLength: PL, pieceCount, pieceSize: (p: number) => (p === pieceCount - 1 ? last : PL) };
	return { file: new PieceFile(data, map, layout), dir };
}

const piece = (p: number, size = PL) => Uint8Array.from({ length: size }, (_, i) => (p * 7 + i) & 0xff);

describe("piece file", () => {
	it("writes pieces where the torrent has them and reads verified bytes only", async () => {
		const fs = new FakeFs();
		const { file } = await open(fs, 10, 1000);
		file.write(7, piece(7));
		file.write(9, piece(9, 1000));
		expect(file.pieces()).toEqual([7, 9]);
		expect(file.has(7)).toBe(true);
		expect(file.has(8)).toBe(false);
		expect(file.readPiece(7)).toEqual(piece(7));
		expect(file.readBlock(9, 512, 488)).toEqual(piece(9, 1000).subarray(512));
		// A 16 KiB block of a verified piece, for the upload path.
		expect(file.readBlock(7, 16384, 16384)).toEqual(piece(7).subarray(16384, 32768));
		expect(() => file.readBlock(8, 0, 16384)).toThrow(StoreError);
		expect(() => file.readBlock(7, 0, 16385)).toThrow(/block outside/);
		expect(() => file.readBlock(7, PL - 10, 20)).toThrow(/block outside/);
		expect(() => file.write(3, piece(3, 10))).toThrow(/bytes/);
	});

	it("reports the missing pieces of a range instead of returning unverified bytes", async () => {
		const fs = new FakeFs();
		const { file } = await open(fs);
		for (const p of [2, 3, 5]) file.write(p, piece(p));
		const ok = file.readRange(2 * PL + 100, PL);
		expect(ok.missing).toEqual([]);
		expect(ok.bytes).toEqual(new Uint8Array([...piece(2).subarray(100), ...piece(3).subarray(0, 100)]));
		expect(file.readRange(3 * PL, 3 * PL)).toEqual({ bytes: null, missing: [4] });
		expect(file.readRange(0, PL * 10)).toEqual({ bytes: null, missing: [0, 1, 4, 6, 7, 8, 9] });
		expect(() => file.readRange(0, PL * 10 + 1)).toThrow(/outside/);
	});

	it("keeps its map across reopen, and a failed write leaves the piece missing", async () => {
		const fs = new FakeFs();
		const first = await open(fs);
		first.file.write(1, piece(1));
		first.file.close();
		const again = await open(fs);
		expect(again.file.pieces()).toEqual([1]);
		// Room for piece 2 but not piece 9 (the file would grow to 10 pieces).
		fs.quota = fs.usage() + 2 * PL;
		again.file.write(2, piece(2));
		expect(() => again.file.write(9, piece(9))).toThrow(expect.objectContaining({ code: "quota" }));
		expect(again.file.pieces()).toEqual([1, 2]);
		expect(again.file.has(9)).toBe(false);
		expect(() => again.file.readPiece(9)).toThrow(expect.objectContaining({ code: "unverified" }));
	});

	it("forgets pieces and gives trailing space back", async () => {
		const fs = new FakeFs();
		const { file } = await open(fs);
		for (const p of [0, 4, 8]) file.write(p, piece(p));
		file.clear([4, 8]);
		expect(file.pieces()).toEqual([0]);
		expect(fs.usage()).toBe(PL + 2);
	});

	it("has exclusive handles: a second store on the same files fails", async () => {
		const fs = new FakeFs();
		await open(fs);
		await expect(open(fs)).rejects.toThrow();
	});
});

describe("piece maths", () => {
	it("maps bitfields and byte ranges to pieces", () => {
		expect(bitsToPieces(Uint8Array.of(0b1000_0001, 0b0100_0000))).toEqual([0, 7, 9]);
		expect(bitsToPieces(Uint8Array.of(0xff), 3)).toEqual([0, 1, 2]);
		expect(piecesOf(0, 1, 100)).toEqual([0, 0]);
		expect(piecesOf(99, 2, 100)).toEqual([0, 1]);
		expect(piecesOf(250, 0, 100)).toEqual([2, 2]);
	});

	it("names files after the hex info-hash only", () => {
		expect(storeNames("a".repeat(40)).data).toBe(`${"a".repeat(40)}.data`);
		expect(() => storeNames("../../etc")).toThrow();
		expect(() => storeNames("A".repeat(40))).toThrow();
	});
});
