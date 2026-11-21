// Block attribution (docs/architecture.md §Chunk recovery › Verification,
// M3-02): per-peer strikes, isolation of the culprit of a mixed piece by
// comparison with the verified data, v2 attribution from leaf hashes, and
// the picker's single-peer isolation.

import { describe, expect, it } from "vitest";
import { hashAnswer, makeTorrent, type FakeTorrent } from "@magnet-stream/fake-peer";
import { Attribution, blockDigests } from "../src/attribution.ts";
import { BLOCK_SIZE } from "../src/metainfo.ts";
import { Picker, type PickerPeer } from "../src/picker.ts";
import { V2Verifier, parseInfoDict, type HashRequest } from "../src/index.ts";

const PL = 4 * BLOCK_SIZE;

function data(seed: number): Uint8Array {
	return Uint8Array.from({ length: PL }, (_, i) => (i * 31 + seed * 7) & 0xff);
}

function corrupted(good: Uint8Array, blocks: number[]): Uint8Array {
	const out = good.slice();
	for (const b of blocks) out[b * BLOCK_SIZE + 5] ^= 0xff;
	return out;
}

describe("Attribution: per-peer accounting", () => {
	it("strikes the sole sender of a failed piece and bans it at the second", () => {
		const a = new Attribution();
		const first = a.failed(1, ["bad", "bad", "bad", "bad"]);
		expect(first).toMatchObject({ struck: ["bad"], banned: [], isolate: false, culprits: ["bad"] });
		expect(a.strikesOf("bad")).toBe(1);
		expect(a.isBanned("bad")).toBe(false);
		const second = a.failed(7, ["bad", "bad", "bad", "bad"]);
		expect(second.banned).toEqual(["bad"]);
		expect(a.isBanned("bad")).toBe(true);
		expect(a.banned).toBe(1);
		// Already banned: no further strike.
		expect(a.failed(9, ["bad"]).struck).toEqual([]);
	});

	it("records senders per block and hands them over once", () => {
		const a = new Attribution();
		a.record(3, 0, 4, "x");
		a.record(3, 1, 4, "y");
		a.record(3, 9, 4, "z"); // out of range: ignored
		expect(a.piecesFrom("y")).toEqual([3]);
		expect(a.forget(3, "y")).toEqual([1]);
		expect(a.take(3)).toEqual(["x", undefined, undefined, undefined]);
		expect(a.take(3)).toEqual([]);
	});

	it("strikes nobody for a mixed piece; the verified data names the culprit only", async () => {
		const a = new Attribution();
		const good = data(1);
		const sources = ["honest", "bad", "honest", "bad"];
		const v = a.failed(2, sources);
		expect(v).toMatchObject({ struck: [], banned: [], isolate: true });
		a.setDigests(2, v.attempt!, await blockDigests(corrupted(good, [1, 3])));
		expect(a.suspected(2)).toBe(true);
		const resolved = a.passed(2, await blockDigests(good));
		expect(resolved.struck).toEqual(["bad"]);
		expect(resolved.culprits).toEqual(["bad"]);
		expect(a.strikesOf("honest")).toBe(0);
		expect(a.suspected(2)).toBe(false);
	});

	it("never bans an honest peer that shared mixed pieces with a corrupting one", async () => {
		const a = new Attribution();
		for (let piece = 0; piece < 5; piece++) {
			const good = data(piece);
			const v = a.failed(piece, ["honest", "bad", "honest", "honest"]);
			expect(v.struck).toEqual([]);
			a.setDigests(piece, v.attempt!, await blockDigests(corrupted(good, [1])));
			const r = a.passed(piece, await blockDigests(good));
			if (piece === 1) expect(r.banned).toEqual(["bad"]);
		}
		expect(a.strikesOf("honest")).toBe(0);
		expect(a.isBanned("honest")).toBe(false);
		expect(a.isBanned("bad")).toBe(true);
	});

	it("isolation: a single-peer retry that fails convicts that peer", async () => {
		const a = new Attribution();
		const mixed = a.failed(4, ["h", "b", "h", "b"]);
		a.setDigests(4, mixed.attempt!, await blockDigests(corrupted(data(4), [1, 3])));
		// The retry from one peer only: "b" alone, and it fails again.
		expect(a.failed(4, ["b", "b", "b", "b"]).struck).toEqual(["b"]);
		// Then "h" alone: it verifies, and the first attempt's diff convicts "b" again.
		const r = a.passed(4, await blockDigests(data(4)));
		expect(r.banned).toEqual(["b"]);
		expect(a.strikesOf("h")).toBe(0);
	});

	it("skips attempts whose digests never came, and stays bounded", () => {
		const a = new Attribution({ maxSuspectPieces: 3, maxAttemptsPerPiece: 2 });
		for (let p = 0; p < 10; p++) a.failed(p, ["x", "y"]);
		expect([0, 6, 7, 8, 9].map((p) => a.suspected(p))).toEqual([false, false, true, true, true]);
		expect(a.passed(9, [new Uint8Array(20), new Uint8Array(20)]).struck).toEqual([]);
	});
});

describe("Attribution: v2 torrents", () => {
	const t: FakeTorrent = makeTorrent({ seed: 91, files: [{ path: ["v.mkv"], length: 12 * 32 * 1024 }], pieceLength: 32 * 1024, version: 2 });

	function answer(req: HashRequest): Uint8Array {
		const raw = Buffer.alloc(48);
		Buffer.from(req.piecesRoot).copy(raw);
		raw.writeUInt32BE(req.baseLayer, 32);
		raw.writeUInt32BE(req.index, 36);
		raw.writeUInt32BE(req.length, 40);
		raw.writeUInt32BE(req.proofLayers, 44);
		return hashAnswer(t, raw, "serve")!.subarray(48);
	}

	it("finds the responsible peer from the single failed block", async () => {
		const meta = await parseInfoDict(t.info, t.infoHashHex);
		const v = new V2Verifier(meta);
		const [layer] = v.layerRequests(meta.files[0]);
		expect(await v.acceptHashes(layer, answer(layer))).toBe(true);
		const piece = Uint8Array.from(t.data.subarray(5 * meta.pieceLength, 6 * meta.pieceLength));
		piece[BLOCK_SIZE + 100] ^= 0x01; // block 1, from "bad"
		expect(await v.verify(5, piece)).toBe(false);
		const a = new Attribution();
		const sources = ["honest", "bad"];
		const failed = a.failed(5, sources);
		expect(failed.struck).toEqual([]);
		const bad = await v.badBlocks(5, piece, answer(v.leafRequest(5)!));
		expect(bad).toEqual([1]);
		const verdict = a.blame(5, sources, bad!, failed.attempt);
		expect(verdict.struck).toEqual(["bad"]);
		expect(a.strikesOf("honest")).toBe(0);
		// The attempt is settled: the verified piece convicts nobody twice.
		expect(a.suspected(5)).toBe(false);
	});
});

describe("Picker isolation", () => {
	const geometry = { pieceCount: 4, pieceLength: PL, totalLength: 4 * PL };
	const peer = (id: string, rate = 1e6, has: (p: number) => boolean = () => true): PickerPeer => ({ id, rate, reqq: 250, unchoked: true, has });

	it("fetches an isolated piece from one peer only, with no duplicates", () => {
		const picker = new Picker(geometry, { headBytes: PL, tailBytes: PL, windowBytes: 4 * PL, hedgeGraceMs: 0, unknownRateDueMs: 0 });
		picker.addTarget({ offset: 0, size: 4 * PL });
		picker.isolate(1);
		const peers = [peer("a"), peer("b"), peer("c")];
		const plan = picker.plan(peers, 0);
		const holders = [...plan].filter(([, blocks]) => blocks.some((b) => b.piece === 1)).map(([id]) => id);
		expect(holders).toHaveLength(1);
		expect(picker.isolation(1)?.owner).toBe(holders[0]);
		expect([...plan.values()].flat().filter((b) => b.piece === 1)).toHaveLength(4);
		// Late, but never asked of another peer.
		const later = picker.plan(peers, 60_000);
		expect([...later.values()].flat().some((b) => b.piece === 1)).toBe(false);
	});

	it("passes over a peer to avoid while another holder is connected, but not when it is the only one", () => {
		const picker = new Picker(geometry, { headBytes: PL, tailBytes: PL, windowBytes: 4 * PL });
		picker.addTarget({ offset: 0, size: 4 * PL }, { pieces: [2] });
		picker.isolate(2, ["fast"]);
		const plan = picker.plan([peer("fast", 1e7), peer("slow", 1e3)], 0);
		expect(plan.get("fast")).toBeUndefined();
		expect(plan.get("slow")!.every((b) => b.piece === 2)).toBe(true);
		picker.releasePeer("slow");
		const alone = picker.plan([peer("fast", 1e7)], 1);
		expect(alone.get("fast")!.length).toBe(4);
	});

	it("drops an isolated piece's blocks when its owner leaves, so another peer starts over", () => {
		const picker = new Picker(geometry, { headBytes: PL, tailBytes: PL, windowBytes: 4 * PL });
		picker.addTarget({ offset: 0, size: 4 * PL }, { pieces: [3] });
		picker.isolate(3);
		const first = picker.plan([peer("a")], 0).get("a")!;
		expect(picker.onBlock("a", 3, first[0].begin, first[0].length).fresh).toBe(true);
		expect(picker.removePeer("a")).toEqual([3]);
		expect(picker.receivedBlocks(3)).toBe(0);
		expect(picker.plan([peer("b")], 1).get("b")!).toHaveLength(4);
		expect(picker.isolation(3)?.owner).toBe("b");
		// Verified: no longer isolated.
		for (const b of [0, 1, 2, 3]) picker.onBlock("b", 3, b * BLOCK_SIZE, BLOCK_SIZE);
		picker.markVerified(3);
		expect(picker.isolation(3)).toBeUndefined();
	});

	it("unreceive makes a block from a banned peer wanted again", () => {
		const picker = new Picker(geometry, { headBytes: PL, tailBytes: PL, windowBytes: 4 * PL });
		picker.addTarget({ offset: 0, size: 4 * PL }, { pieces: [0] });
		const blocks = picker.plan([peer("bad")], 0).get("bad")!;
		picker.onBlock("bad", 0, blocks[0].begin, blocks[0].length);
		picker.removePeer("bad");
		expect(picker.unreceive(0, 0)).toBe(true);
		expect(picker.unreceive(0, 0)).toBe(false);
		expect(picker.plan([peer("good")], 1).get("good")!.map((b) => b.begin)).toEqual([0, BLOCK_SIZE, 2 * BLOCK_SIZE, 3 * BLOCK_SIZE]);
	});
});
