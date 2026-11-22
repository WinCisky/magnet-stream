// Deterministic simulations of the deadline picker: priority levels,
// fast-peer window, hedging of late blocks, seeks, queue depth and scarce
// pieces (docs/architecture.md §Chunk recovery › Piece picking).

import { describe, expect, it } from "vitest";
import { BLOCK_SIZE } from "../src/metainfo.ts";
import { MAX_REQUESTS_PER_BLOCK, PickLevel, Picker, type PickedBlock, type PickerConfig, type PickerPeer } from "../src/picker.ts";

const PL = 64 * 1024; // 4 blocks per piece
const KiB = 1024;

function geometry(pieces: number, last = PL) {
	return { pieceCount: pieces, pieceLength: PL, totalLength: PL * (pieces - 1) + last };
}

/** 64 KiB/s of video: 1 piece per second. */
const config: Partial<PickerConfig> = {
	bitrate: PL,
	windowSeconds: 2,
	scarceAheadSeconds: 6,
	headBytes: PL,
	tailBytes: PL,
	// Time depth only (the byte floor of fast peers has its own tests).
	minQueueBytes: 0,
};

function peer(id: string, opts: { rate?: number; reqq?: number; has?: (p: number) => boolean; unchoked?: boolean } = {}): PickerPeer {
	return { id, rate: opts.rate ?? 0, reqq: opts.reqq ?? 250, unchoked: opts.unchoked ?? true, has: opts.has ?? (() => true) };
}

/** Deliver a block: verify the piece when complete. */
function deliver(picker: Picker, from: string, b: { piece: number; begin: number; length: number }) {
	const r = picker.onBlock(from, b.piece, b.begin, b.length);
	if (r.complete) picker.markVerified(b.piece);
	return r;
}

describe("picker priority", () => {
	it("follows index, window, read-ahead, scarce, back-fill in that order", () => {
		const picker = new Picker(geometry(64), config);
		const scarce = new Set([10, 50]);
		const a = peer("a", { rate: 1e6, reqq: 1 });
		// b and c are connected but choked: they count for availability only.
		const b = peer("b", { unchoked: false, has: (p) => !scarce.has(p) });
		const c = peer("c", { unchoked: false, has: (p) => !scarce.has(p) });
		picker.addTarget({ offset: 0, size: 64 * PL }, { position: 32 * PL });
		const order: PickedBlock[] = [];
		let now = 0;
		for (let i = 0; i < 64 * 4 + 5; i++) {
			const picked = picker.plan([a, b, c], (now += 10)).get("a") ?? [];
			expect(picked.length).toBeLessThanOrEqual(1);
			for (const blk of picked) {
				order.push(blk);
				deliver(picker, "a", blk);
			}
		}
		expect(order).toHaveLength(64 * 4);
		const levels = order.map((o) => o.level);
		for (let i = 1; i < levels.length; i++) expect(levels[i]).toBeGreaterThanOrEqual(levels[i - 1]);
		const piecesAt = (level: number) => [...new Set(order.filter((o) => o.level === level).map((o) => o.piece))];
		expect(piecesAt(PickLevel.Index)).toEqual([0, 63]);
		expect(piecesAt(PickLevel.Window)).toEqual([32, 33]);
		expect(piecesAt(PickLevel.ReadAhead)).toEqual([34, 35, 36, 37]);
		// Rarest first, then from the horizon on: 50 before 10.
		expect(piecesAt(PickLevel.Scarce)).toEqual([50, 10]);
		const back = piecesAt(PickLevel.BackFill);
		expect(back[0]).toBe(38);
		expect(back.at(-1)).toBe(31);
		expect(back.includes(10) || back.includes(50)).toBe(false);
		// Within a piece, blocks go in order.
		expect(order.slice(0, 4).map((o) => o.begin)).toEqual([0, 16 * KiB, 32 * KiB, 48 * KiB]);
	});

	it("takes scarce pieces before back-fill only once the buffer is ahead by the threshold", () => {
		const scarce = new Set([5]);
		const peers = [
			peer("a", { rate: 1e6, reqq: 1 }),
			peer("b", { unchoked: false, has: (p) => !scarce.has(p) }),
			peer("c", { unchoked: false, has: (p) => !scarce.has(p) }),
		];
		const run = (verifiedAhead: boolean) => {
			const picker = new Picker(geometry(40), config);
			picker.addTarget({ offset: 0, size: 40 * PL }, { position: 20 * PL });
			for (const p of [0, 39, 20, 21, 22, 23]) picker.markVerified(p);
			if (verifiedAhead) for (const p of [24, 25]) picker.markVerified(p);
			const first = picker.plan(peers, 1).get("a")![0];
			return first;
		};
		// Not ahead: read-ahead first (24), and piece 5 waits for back-fill.
		expect(run(false)).toMatchObject({ piece: 24, level: PickLevel.ReadAhead });
		// 6 s (6 pieces) verified past the position: the scarce piece comes before 26… (back-fill).
		expect(run(true)).toMatchObject({ piece: 5, level: PickLevel.Scarce });
	});

	it("serves the window from the fastest peers only, read-ahead from everyone", () => {
		const picker = new Picker(geometry(64), config);
		picker.addTarget({ offset: 0, size: 64 * PL });
		for (const p of [0, 63]) picker.markVerified(p);
		const fast = peer("fast", { rate: 1_000_000, reqq: 2 });
		const quick = peer("quick", { rate: 800_000, reqq: 4 });
		const slow = peer("slow", { rate: 100_000, reqq: 4 });
		const plan = picker.plan([slow, fast, quick], 0);
		const levels = (id: string) => new Set((plan.get(id) ?? []).map((b) => b.level));
		expect([...levels("slow")]).toEqual([PickLevel.ReadAhead]);
		const window = [...(plan.get("fast") ?? []), ...(plan.get("quick") ?? [])].filter((b) => b.level === PickLevel.Window);
		// Window pieces 0..1 (position 0, 2 pieces; piece 0 is index, verified): piece 1's blocks.
		expect(window.map((b) => `${b.piece}:${b.begin}`).sort()).toEqual(["1:0", "1:16384", "1:32768", "1:49152"]);
		// The fastest asks first and in order.
		expect(plan.get("fast")![0]).toMatchObject({ piece: 1, begin: 0, level: PickLevel.Window });
	});

	it("lets a slow peer take a window piece no fast peer has", () => {
		const picker = new Picker(geometry(16), config);
		picker.addTarget({ offset: 0, size: 16 * PL });
		for (const p of [0, 15]) picker.markVerified(p);
		const fast = peer("fast", { rate: 1_000_000, has: (p) => p !== 1 });
		const slow = peer("slow", { rate: 100_000 });
		const plan = picker.plan([fast, slow], 0);
		expect(plan.get("slow")!.filter((b) => b.level === PickLevel.Window).map((b) => b.piece)).toEqual([1, 1, 1, 1]);
		expect(plan.get("fast")!.some((b) => b.piece === 1)).toBe(false);
	});
});

describe("picker hedging", () => {
	it("asks a second peer for a late window block and discards the copy that arrives second", () => {
		const picker = new Picker(geometry(16), { ...config, hedgeGraceMs: 1000 });
		picker.addTarget({ offset: 0, size: 16 * PL });
		for (const p of [0, 15]) picker.markVerified(p);
		// One block each: a holds 1:0, b is busy with 1:16384.
		const a = peer("a", { rate: 1_000_000, reqq: 1 });
		const b = peer("b", { rate: 900_000, reqq: 1 });
		const c = peer("c", { rate: 950_000, reqq: 4 });
		const first = picker.plan([a, b], 0);
		expect(first.get("a")).toEqual([{ piece: 1, begin: 0, length: BLOCK_SIZE, level: PickLevel.Window, hedge: false }]);
		expect(first.get("b")![0]).toMatchObject({ piece: 1, begin: 16384 });
		// Soon after: nothing is late, c gets the rest of the window, no duplicates.
		const early = picker.plan([a, b, c], 100).get("c")!;
		expect(early.every((x) => !x.hedge)).toBe(true);
		expect(picker.hedgedBlocks).toBe(0);
		// Much later (a and b silent): c, now drained, duplicates the late window blocks, earliest first.
		for (const x of early) deliver(picker, "c", x);
		const late = picker.plan([a, b, c], 5_000).get("c")!;
		expect(late.slice(0, 2)).toEqual([
			{ piece: 1, begin: 0, length: BLOCK_SIZE, level: PickLevel.Window, hedge: true },
			{ piece: 1, begin: 16384, length: BLOCK_SIZE, level: PickLevel.Window, hedge: true },
		]);
		expect(picker.hedgedBlocks).toBe(2);
		// A third peer cannot add a third request for the same block.
		const d = peer("d", { rate: 990_000 });
		const more = picker.plan([a, b, c, d], 6_000).get("d") ?? [];
		expect(more.some((x) => x.piece === 1 && (x.begin === 0 || x.begin === 16384))).toBe(false);
		// c's copy arrives first: a's request is cancelled; a's late copy is discarded.
		const won = picker.onBlock("c", 1, 0, BLOCK_SIZE);
		expect(won.fresh).toBe(true);
		expect(won.cancels).toEqual([{ peer: "a", piece: 1, begin: 0, length: BLOCK_SIZE }]);
		const dup = picker.onBlock("a", 1, 0, BLOCK_SIZE);
		expect(dup.fresh).toBe(false);
		expect(picker.duplicatesDiscarded).toBe(1);
		expect(picker.hedgedBlocks).toBe(1);
		expect(picker.outstanding("a")).toBe(0);
	});

	it("never keeps more than maxHedgedBlocks duplicates nor two requests per block", () => {
		const picker = new Picker(geometry(64), { ...config, windowSeconds: 40, maxHedgedBlocks: 5 });
		picker.addTarget({ offset: 0, size: 64 * PL });
		const peers = ["a", "b", "c", "d", "e"].map((id, i) => peer(id, { rate: 1_000_000 - i }));
		let now = 0;
		for (let round = 0; round < 20; round++) {
			picker.plan(peers, (now += 3_000));
			expect(picker.hedgedBlocks).toBeLessThanOrEqual(5);
		}
		const perBlock = new Map<string, number>();
		for (const p of peers) {
			for (let piece = 0; piece < 64; piece++) {
				for (let begin = 0; begin < PL; begin += BLOCK_SIZE) {
					if (!picker.isRequested(p.id, piece, begin)) continue;
					const k = `${piece}:${begin}`;
					perBlock.set(k, (perBlock.get(k) ?? 0) + 1);
				}
			}
		}
		expect(Math.max(...perBlock.values())).toBeLessThanOrEqual(MAX_REQUESTS_PER_BLOCK);
		expect([...perBlock.values()].filter((n) => n === 2)).toHaveLength(5);
	});

	it("treats a choked holder's requests as late", () => {
		const picker = new Picker(geometry(16), config);
		picker.addTarget({ offset: 0, size: 16 * PL });
		for (const p of [0, 15]) picker.markVerified(p);
		const a = peer("a", { rate: 1_000_000, reqq: 4 });
		picker.plan([a], 0);
		const choked = { ...a, unchoked: false };
		const b = peer("b", { rate: 1_000_000, reqq: 4 });
		const got = picker.plan([choked, b], 10).get("b")!;
		expect(got.filter((x) => x.piece === 1 && x.hedge)).toHaveLength(4);
	});
});

describe("picker seeks", () => {
	it("cancels the requests outside the new window and index, and keeps the rest", () => {
		const picker = new Picker(geometry(64), config);
		const id = picker.addTarget({ offset: 0, size: 64 * PL });
		const a = peer("a", { rate: 400_000, reqq: 500 });
		const b = peer("b", { rate: 300_000, reqq: 500 });
		picker.plan([a, b], 0);
		const before = picker.outstanding("a") + picker.outstanding("b");
		expect(before).toBeGreaterThan(8);
		// Half a piece arrived of piece 4 (read-ahead): it is dropped with the seek.
		const partial = [16 * KiB, 0].map((begin) => ({ piece: 4, begin, length: BLOCK_SIZE }));
		for (const blk of partial) deliver(picker, picker.isRequested("a", 4, blk.begin) ? "a" : "b", blk);
		const { cancels, dropped } = picker.seek(id, 40 * PL);
		expect(cancels.length).toBeGreaterThan(0);
		// Kept: index (0, 63) and the new window (40, 41).
		for (const c of cancels) expect([0, 63, 40, 41]).not.toContain(c.piece);
		expect(dropped).toContain(4);
		expect(picker.receivedBlocks(4)).toBe(0);
		const kept = ["a", "b"].flatMap((p) => [0, 63].filter((piece) => picker.isRequested(p, piece, 0)));
		expect(kept.length).toBeGreaterThan(0);
		// The next round covers the new window first.
		const next = picker.plan([a, b], 10);
		expect(next.get("a")![0].level).toBeLessThanOrEqual(PickLevel.Window);
		for (const piece of [40, 41]) {
			for (let begin = 0; begin < PL; begin += BLOCK_SIZE) expect(picker.isRequested("a", piece, begin) || picker.isRequested("b", piece, begin)).toBe(true);
		}
		expect(picker.position(id)).toBe(40 * PL);
	});

	it("setPosition moves the window without cancelling anything", () => {
		const picker = new Picker(geometry(64), config);
		const id = picker.addTarget({ offset: 0, size: 64 * PL });
		const a = peer("a", { rate: 400_000 });
		picker.plan([a], 0);
		const before = picker.outstanding("a");
		picker.setPosition(id, 30 * PL);
		expect(picker.outstanding("a")).toBe(before);
		expect(picker.plan([a, peer("b", { rate: 400_000 })], 1).get("b")?.[0]).toMatchObject({ piece: 30, level: PickLevel.Window });
	});
});

describe("picker queue depth", () => {
	it("never queues more than reqq or readAheadSeconds of a peer's rate", () => {
		const picker = new Picker(geometry(256), { ...config, readAheadSeconds: 4 });
		picker.addTarget({ offset: 0, size: 256 * PL });
		const peers = [
			peer("reqq-bound", { rate: 10_000_000, reqq: 5 }),
			peer("time-bound", { rate: 64 * KiB, reqq: 500 }), // 4 s = 16 blocks
			peer("unknown", { rate: 0, reqq: 500 }),
			peer("partial", { rate: 200 * KiB, reqq: 500, has: (p) => p % 3 === 0 }),
		];
		const caps: Record<string, number> = { "reqq-bound": 5, "time-bound": 16, unknown: 16, partial: 50 };
		let seed = 7;
		const rand = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
		let now = 0;
		for (let round = 0; round < 200; round++) {
			now += 250;
			const plan = picker.plan(peers, now);
			for (const p of peers) {
				expect(picker.outstanding(p.id)).toBeLessThanOrEqual(caps[p.id]);
				for (const blk of plan.get(p.id) ?? []) if (rand() < 0.6) deliver(picker, p.id, blk);
			}
		}
		expect(picker.depth({ rate: 64 * KiB, reqq: 500 })).toBe(16);
		expect(picker.depth({ rate: 10_000_000, reqq: 5 })).toBe(5);
	});

	it("refills a queue only once half has drained, unless a window block is late", () => {
		const picker = new Picker(geometry(256), config);
		picker.addTarget({ offset: 0, size: 256 * PL });
		const a = peer("a", { rate: 64 * KiB, reqq: 500 }); // depth 16
		const first = picker.plan([a], 0).get("a")!;
		expect(first).toHaveLength(16);
		// 3 delivered (13 left > 8): no refill.
		for (const blk of first.slice(0, 3)) deliver(picker, "a", blk);
		expect(picker.plan([a], 1).get("a")).toBeUndefined();
		// ...unless its socket sends a message anyway (eager).
		expect(picker.plan([a], 2, { eager: () => true }).get("a")).toHaveLength(3);
		// b, full too, duplicates a's window blocks once they are late.
		const b = peer("b", { rate: 64 * KiB, reqq: 500 });
		const bFirst = picker.plan([a, b], 3).get("b")!;
		for (const blk of bFirst.slice(0, 3)) deliver(picker, "b", blk);
		expect(picker.plan([a, b], 4).get("b")).toBeUndefined();
		const late = picker.plan([a, b], 60_000).get("b")!;
		expect(late.length).toBe(3);
		expect(late.every((x) => x.hedge && x.level <= PickLevel.Window)).toBe(true);
	});

	it("stays within the caller's byte budget", () => {
		const picker = new Picker(geometry(64), config);
		picker.addTarget({ offset: 0, size: 64 * PL });
		let left = 5 * BLOCK_SIZE;
		const plan = picker.plan([peer("a", { rate: 1e7 }), peer("b", { rate: 1e7 })], 0, {
			budget: () => left,
			spent: (_, bytes) => (left -= bytes),
		});
		expect([...plan.values()].flat()).toHaveLength(5);
	});
});

describe("picker queue floor of the fast peers (billed messages on slow swarms)", () => {
	const floored: Partial<PickerConfig> = { ...config, minQueueBytes: 4 * 1024 * KiB, maxQueueSeconds: 60 };

	it("keeps a fast peer at least minQueueBytes deep, within reqq and maxQueueSeconds of its rate", () => {
		const picker = new Picker(geometry(4), floored);
		const floor = (4 * 1024 * KiB) / BLOCK_SIZE; // 256 blocks
		expect(picker.depth({ rate: 70_000, reqq: 500 }, true)).toBe(floor);
		expect(picker.depth({ rate: 70_000, reqq: 250 }, true)).toBe(250);
		// 2 KB/s: 60 s of it is 7 blocks.
		expect(picker.depth({ rate: 2_000, reqq: 500 }, true)).toBe(7);
		// Not among the fast peers: the time depth (4 s).
		expect(picker.depth({ rate: 70_000, reqq: 500 }, false)).toBe(17);
		// Fast peers already deeper than the floor keep their time depth.
		expect(picker.depth({ rate: 2_000_000, reqq: 1000 }, true)).toBe(488);
		// Off.
		expect(new Picker(geometry(4), { ...floored, minQueueBytes: 0 }).depth({ rate: 70_000, reqq: 500 }, true)).toBe(17);
	});

	it("asks a lone 70 KB/s peer for 64 MiB in a few dozen messages, not one every 2 s", () => {
		const count = (cfg: Partial<PickerConfig>) => {
			const pieces = 1024; // 64 MiB
			const picker = new Picker(geometry(pieces), cfg);
			picker.addTarget({ offset: 0, size: pieces * PL });
			const a = peer("a", { rate: 70_000, reqq: 500 });
			const queue: PickedBlock[] = [];
			let messages = 0;
			let credit = 0;
			let verified = 0;
			for (let now = 0; verified < pieces && now < 2_000_000; now += 250) {
				const picked = picker.plan([a], now).get("a") ?? [];
				if (picked.length > 0) messages++;
				queue.push(...picked);
				credit += 70_000 / 4;
				while (queue.length > 0 && credit >= queue[0].length) {
					const b = queue.shift()!;
					credit -= b.length;
					if (deliver(picker, "a", b).complete) verified++;
				}
				if (queue.length === 0) credit = 0;
			}
			expect(verified).toBe(pieces);
			return messages;
		};
		const timeOnly = count({ ...floored, minQueueBytes: 0 });
		const withFloor = count(floored);
		// 64 MiB in 2 MiB refills: ~32 messages (~25 billed requests per GB with
		// the 20-messages rule); the time depth alone sends one every ~2 s.
		expect(withFloor).toBeLessThanOrEqual(40);
		expect(timeOnly).toBeGreaterThan(300);
	});

	it("lets a faster peer take at once the window blocks a demoted peer's deep queue holds", () => {
		const picker = new Picker(geometry(64), { ...floored, windowSeconds: 8 });
		picker.addTarget({ offset: 0, size: 64 * PL });
		const slow = peer("slow", { rate: 20_000, reqq: 500 });
		const first = picker.plan([slow], 0).get("slow")!;
		// Alone, it is the fastest: 60 s of 20 KB/s.
		expect(first.length).toBe(Math.floor((20_000 * 60) / BLOCK_SIZE));
		const fast = peer("fast", { rate: 2_000_000, reqq: 500 });
		// Long before the slow peer's requests are due.
		const plan = picker.plan([slow, fast], 1_000).get("fast") ?? [];
		const hedges = plan.filter((b) => b.hedge && b.level <= PickLevel.Window);
		expect(hedges.length).toBeGreaterThan(0);
		for (const h of hedges) expect(picker.isRequested("slow", h.piece, h.begin)).toBe(true);
	});
});

describe("picker state", () => {
	it("re-requests a piece after a failed verification and after a reject (not from the rejecting peer)", () => {
		const picker = new Picker(geometry(8), config);
		picker.addTarget({ offset: 0, size: 8 * PL });
		const a = peer("a", { rate: 1e6, reqq: 4 });
		const blocks = picker.plan([a], 0).get("a")!;
		let last;
		for (const blk of blocks) last = picker.onBlock("a", blk.piece, blk.begin, blk.length);
		expect(last!.complete).toBe(true);
		picker.reset(0);
		expect(picker.plan([a], 1).get("a")!.map((b) => b.piece)).toEqual([0, 0, 0, 0]);
		picker.onReject("a", 0, 0, 2);
		const b = peer("b", { rate: 1e5, reqq: 4 });
		const again = picker.plan([a, b], 3);
		expect(again.get("a")?.some((x) => x.piece === 0) ?? false).toBe(false);
		expect(again.get("b")!.some((x) => x.piece === 0 && x.begin === 0)).toBe(true);
	});

	it("rejects blocks of the wrong size or offset and handles a short last piece", () => {
		const picker = new Picker(geometry(3, 20 * KiB), config);
		const id = picker.addTarget({ offset: 0, size: 2 * PL + 20 * KiB });
		const got = picker.plan([peer("a", { rate: 1e6 })], 0).get("a")!;
		const tail = got.filter((b) => b.piece === 2);
		expect(tail.map((b) => b.length)).toEqual([16 * KiB, 4 * KiB]);
		expect(picker.onBlock("a", 2, 16 * KiB, 16 * KiB).fresh).toBe(false);
		expect(picker.onBlock("a", 2, 100, 4 * KiB).fresh).toBe(false);
		expect(picker.onBlock("a", 99, 0, 16 * KiB).fresh).toBe(false);
		for (const b of got) deliver(picker, "a", b);
		expect(picker.targetDone(id)).toBe(true);
	});

	it("serves several targets and withdraws a removed one's requests", () => {
		const picker = new Picker(geometry(32), config);
		const video = picker.addTarget({ offset: 0, size: 24 * PL });
		const subs = picker.addTarget({ offset: 24 * PL, size: 8 * PL });
		const a = peer("a", { rate: 1e6, reqq: 500 });
		const plan = picker.plan([a], 0).get("a")!;
		expect(plan.some((b) => b.piece >= 24)).toBe(true);
		// Index of both targets first.
		expect(plan.slice(0, 8).map((b) => b.piece)).toEqual([0, 0, 0, 0, 23, 23, 23, 23]);
		const { cancels } = picker.removeTarget(subs);
		expect(cancels.length).toBeGreaterThan(0);
		expect(cancels.every((c) => c.piece >= 24)).toBe(true);
		expect(picker.targetCount).toBe(1);
		expect(picker.pieceRange(video)).toEqual({ first: 0, last: 23 });
	});
});
