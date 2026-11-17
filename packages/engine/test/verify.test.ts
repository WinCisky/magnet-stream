// BEP 52 merkle verification: trees, padding, one-block files, piece
// layers in chunks with proofs, bad layers, bad blocks.

import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { hashAnswer, makeTorrent, merkleTree, zeroSubtree, type FakeTorrent } from "@magnet-stream/fake-peer";
import { V1Verifier, V2Verifier, leafHashes, merkleRoot, parseInfoDict, verifierFor, zeroRoot, type HashRequest, type Metainfo } from "../src/index.ts";

const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

async function load(t: FakeTorrent): Promise<Metainfo> {
	return parseInfoDict(t.info, t.infoHashHex);
}

/** Answer `req` the way the fake peer would. */
function answer(t: FakeTorrent, req: HashRequest, mode: "serve" | "corrupt" = "serve"): Uint8Array {
	const raw = Buffer.alloc(48);
	Buffer.from(req.piecesRoot).copy(raw);
	raw.writeUInt32BE(req.baseLayer, 32);
	raw.writeUInt32BE(req.index, 36);
	raw.writeUInt32BE(req.length, 40);
	raw.writeUInt32BE(req.proofLayers, 44);
	return hashAnswer(t, raw, mode)!.subarray(48);
}

describe("merkle trees", () => {
	it("match the reference implementation, padding included", async () => {
		const data = Uint8Array.from({ length: 5 * 16384 + 100 }, (_, i) => i * 7);
		const reference = merkleTree(data, 64 * 1024);
		const leaves = await leafHashes(data);
		expect(leaves).toHaveLength(6);
		expect(hex(await merkleRoot(leaves, 8))).toBe(hex(reference.layers.at(-1)![0]));
		expect(hex(await zeroRoot(3))).toBe(hex(zeroSubtree(8)));
		// A file of one short block: its root is the hash of the block itself.
		const tiny = Uint8Array.of(1, 2, 3);
		expect(hex(await merkleRoot(await leafHashes(tiny), 1))).toBe(createHash("sha256").update(tiny).digest("hex"));
	});
});

describe("V2Verifier", () => {
	const t = makeTorrent({
		seed: 31,
		files: [
			{ path: ["big.mkv"], length: 40 * 32 * 1024 + 5_000 },
			{ path: ["small.srt"], length: 20_000 },
		],
		pieceLength: 32 * 1024,
		version: 2,
	});

	it("needs the piece layer of multi-piece files only, and accepts it only if it hashes to the root", async () => {
		const meta = await load(t);
		const v = new V2Verifier(meta);
		const big = meta.files.find((f) => f.path === "big.mkv")!;
		const small = meta.files.find((f) => f.path === "small.srt")!;
		expect(v.missingLayers().map((f) => f.path)).toEqual(["big.mkv"]);
		expect(v.canVerify(small.offset / meta.pieceLength)).toBe(true);
		expect(v.canVerify(0)).toBe(false);
		const [req] = v.layerRequests(big);
		expect(req).toMatchObject({ baseLayer: 1, index: 0, length: 64, proofLayers: 0 });
		expect(await v.acceptHashes(req, answer(t, req, "corrupt"))).toBe(false);
		expect(v.canVerify(0)).toBe(false);
		expect(await v.acceptHashes(req, answer(t, req))).toBe(true);
		expect(v.missingLayers()).toEqual([]);
		for (let p = 0; p < meta.pieceCount; p++) {
			const data = t.data.subarray(p * meta.pieceLength, p * meta.pieceLength + t.pieceSize(p));
			expect(await v.verify(p, data), `piece ${p}`).toBe(true);
		}
		const bad = Uint8Array.from(t.data.subarray(0, meta.pieceLength));
		bad[20_000] ^= 1;
		expect(await v.verify(0, bad)).toBe(false);
		// Wrong length (a v2 piece is short at the end of a file).
		expect(await v.verify(40, t.data.subarray(40 * meta.pieceLength, 41 * meta.pieceLength))).toBe(false);
	});

	it("takes layers of more than 512 pieces in chunks, each with its proof", async () => {
		const huge = makeTorrent({ seed: 32, files: [{ path: ["x.bin"], length: 700 * 16384 }], pieceLength: 16384, version: 2 });
		const meta = await load(huge);
		const v = new V2Verifier(meta);
		const reqs = v.layerRequests(meta.files[0]);
		expect(reqs.map((r) => [r.index, r.length, r.proofLayers])).toEqual([
			[0, 512, 1],
			[512, 512, 1],
		]);
		expect(await v.acceptHashes(reqs[1], answer(huge, reqs[1]))).toBe(true);
		expect(v.canVerify(600)).toBe(false);
		expect(await v.acceptHashes(reqs[0], answer(huge, reqs[0]))).toBe(true);
		expect(await v.verify(600, huge.data.subarray(600 * 16384, 601 * 16384))).toBe(true);
	});

	it("finds the corrupt 16 KiB blocks of a failed piece from its leaf hashes", async () => {
		const meta = await load(t);
		const v = new V2Verifier(meta);
		const [req] = v.layerRequests(meta.files[0]);
		await v.acceptHashes(req, answer(t, req));
		const piece = Uint8Array.from(t.data.subarray(3 * meta.pieceLength, 4 * meta.pieceLength));
		piece[16384 + 9] ^= 0xff;
		expect(await v.verify(3, piece)).toBe(false);
		const leafReq = v.leafRequest(3)!;
		expect(await v.badBlocks(3, piece, answer(t, leafReq))).toEqual([1]);
		// Leaves that don't hash to the piece hash are not trusted.
		expect(await v.badBlocks(3, piece, answer(t, leafReq, "corrupt"))).toBeUndefined();
	});

	it("v1 and hybrids verify with SHA-1", async () => {
		const hybrid = makeTorrent({ seed: 33, files: [{ path: ["a"], length: 50_000 }, { path: ["b"], length: 10_000 }], pieceLength: 32 * 1024, version: "hybrid" });
		const meta = await load(hybrid);
		const v = verifierFor(meta);
		expect(v).toBeInstanceOf(V1Verifier);
		for (let p = 0; p < meta.pieceCount; p++) expect(await v.verify(p, hybrid.data.subarray(p * 32768, (p + 1) * 32768))).toBe(true);
	});
});
