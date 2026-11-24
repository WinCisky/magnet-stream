// Scoring, replacement, upstream coalescing (simulated clock), the urgent
// path, and the in-flight cap. The gateway integration lives next to this.

import { describe, expect, it } from "vitest";
import type { Frame } from "@magnet-stream/tunnel-protocol";
import { PeerConnection } from "../src/index.ts";
import { connectionScore } from "../src/score.ts";
import { replacementVictim, requestAllowance, shouldAddSocket, type SlotView } from "../src/slots.ts";
import { UpstreamBatcher } from "../src/upstream.ts";
import { BufferedTransport } from "../src/transport.ts";
import { MAX_INFLIGHT_PER_SOCKET, needsRefill, queueDepth } from "../src/connection-manager.ts";

const geometry = { pieceCount: 4, pieceLength: 32 * 1024, totalLength: 128 * 1024 };

class RecordingTransport extends BufferedTransport {
	readonly remote = { reserved: Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04), peerId: new Uint8Array(20) };
	readonly local = { reserved: this.remote.reserved };
	sent: Uint8Array[] = [];
	urgent: Uint8Array[] = [];
	send(bytes: Uint8Array): void {
		this.sent.push(bytes);
	}
	sendUrgent(bytes: Uint8Array): void {
		this.urgent.push(bytes);
	}
	close(): void {
		this.finish();
	}
}

function clock() {
	let now = 0;
	const timers: ({ fn: () => void; at: number } | undefined)[] = [];
	return {
		now: () => now,
		advance(ms: number) {
			now += ms;
			for (const t of timers) if (t && t.at <= now) t.fn();
		},
		setTimer(fn: () => void, ms: number) {
			const id = timers.length;
			timers.push({ fn, at: now + ms });
			return id as unknown as ReturnType<typeof setTimeout>;
		},
		clearTimer(id: ReturnType<typeof setTimeout>) {
			timers[id as unknown as number] = undefined;
		},
	};
}

describe("upstream batching", () => {
	it("coalesces frames on a simulated clock and flushes urgent frames at the end of the task, together", async () => {
		const c = clock();
		const sent: Frame[][] = [];
		const batcher = new UpstreamBatcher({
			intervalMs: 400,
			now: c.now,
			setTimer: c.setTimer,
			clearTimer: c.clearTimer,
			send: (frames) => sent.push(frames),
		});
		const ping = (n: number): Frame => ({ type: "ping", conn: 0, payload: Uint8Array.of(n) });
		batcher.enqueue([ping(1)]);
		batcher.enqueue([ping(2)]);
		c.advance(399);
		expect(sent).toEqual([]);
		c.advance(1);
		expect(sent).toEqual([[ping(1), ping(2)]]);
		expect(batcher.messages).toBe(1);
		batcher.enqueue([{ type: "data", conn: 1, payload: Uint8Array.of(9) }], true);
		batcher.enqueue([{ type: "close", conn: 2, reason: 0 }], true);
		expect(sent).toHaveLength(1);
		await Promise.resolve();
		expect(sent).toHaveLength(2);
		expect(sent[1]).toEqual([
			{ type: "data", conn: 1, payload: Uint8Array.of(9) },
			{ type: "close", conn: 2, reason: 0 },
		]);
	});

	it("sends ping only after a quiet interval, and not while a flush is already waiting", async () => {
		const c = clock();
		const sent: Frame[][] = [];
		const batcher = new UpstreamBatcher({
			intervalMs: 400,
			now: c.now,
			setTimer: c.setTimer,
			clearTimer: c.clearTimer,
			send: (frames) => sent.push(frames),
		});
		expect(batcher.pingIfIdle(c.now(), 30_000)).toBe(false);
		c.advance(30_000);
		expect(batcher.pingIfIdle(c.now(), 30_000)).toBe(true);
		await Promise.resolve();
		expect(sent[0]?.[0]).toMatchObject({ type: "ping", conn: 0 });
		batcher.enqueue([{ type: "ping", conn: 0, payload: Uint8Array.of(1) }]);
		expect(batcher.pingIfIdle(c.now() + 30_000, 30_000)).toBe(false);
	});

	it("a cancel skips the batch (urgent path)", () => {
		const transport = new RecordingTransport();
		const conn = new PeerConnection(transport, { geometry, keepAliveMs: 0 });
		conn.request([[0, 0, 16 * 1024]]);
		expect(transport.urgent).toEqual([]);
		conn.cancel(0, 0);
		expect(transport.urgent).toHaveLength(1);
		expect(transport.urgent[0][4]).toBe(8);
		conn.close();
	});
});

describe("connection score", () => {
	const base = { reachable: true, seed: false, unchokeMs: null as number | null, rate: 0, blocks: 0, lastBlockAt: 0, consecutiveFailures: 0, pieces: 0, pieceCount: 10, reqq: 0, rejects: 0 };

	it("uses the swarm weights: throughput dominates, unchoke delay and failures cost", () => {
		const now = 10_000;
		const fast = connectionScore({ ...base, rate: 1_048_576, blocks: 40, lastBlockAt: now, unchokeMs: 200 }, now);
		const seed = connectionScore({ ...base, seed: true }, now);
		const slowUnchoke = connectionScore({ ...base, unchokeMs: 6000, blocks: 1, rate: 1 }, now);
		const quick = connectionScore({ ...base, unchokeMs: 200, blocks: 1, rate: 1 }, now);
		expect(fast).toBeGreaterThan(seed);
		expect(quick).toBeGreaterThan(slowUnchoke);
		const failed = connectionScore({ ...base, consecutiveFailures: 2 }, now);
		expect(failed).toBeLessThan(connectionScore(base, now));
		// Shared terms, copied from apps/web swarm.ts score (no random tie-break).
		const shared =
			2 +
			1.5 +
			(1.5 - 200 / 2000) +
			Math.log2(1 + 65_536 / 65_536) * 1.25 +
			0.5;
		expect(connectionScore({ ...base, seed: true, unchokeMs: 200, rate: 65_536, blocks: 4, lastBlockAt: now }, now)).toBeCloseTo(shared, 5);
	});

	it("rewards a full bitfield and a deep reqq, and penalises rejects", () => {
		const now = 0;
		const full = connectionScore({ ...base, pieces: 10, reqq: 500 }, now);
		const thin = connectionScore({ ...base, pieces: 1, reqq: 10, rejects: 8 }, now);
		expect(full).toBeGreaterThan(thin);
	});
});

describe("replacement policy", () => {
	const slot = (over: Partial<SlotView>): SlotView => ({
		key: "k",
		delivering: false,
		choked: false,
		slow: false,
		downloaded: 0,
		score: 0,
		openedAt: 0,
		...over,
	});

	it("never closes a connection that is delivering, even if it is the slowest", () => {
		const delivering = slot({ key: "fast", delivering: true, score: -100, downloaded: 1 });
		const choked = slot({ key: "choked", choked: true, openedAt: 0 });
		const victim = replacementVictim([delivering, choked], 30_000, 30_000);
		expect(victim?.key).toBe("choked");
		expect(replacementVictim([delivering, slot({ key: "also", delivering: true, slow: true })], 60_000, 30_000)).toBeUndefined();
	});

	it("replaces a choked connection only after the grace period, slowest first", () => {
		const young = slot({ key: "young", choked: true, openedAt: 29_000 });
		const old = slot({ key: "old", choked: true, openedAt: 0, downloaded: 10, score: 5 });
		const slower = slot({ key: "slow", slow: true, downloaded: 1, score: 1, openedAt: 0 });
		expect(replacementVictim([young], 30_000, 30_000)).toBeUndefined();
		expect(replacementVictim([young, old], 30_000, 30_000)?.key).toBe("old");
		expect(replacementVictim([old, slower], 30_000, 30_000)?.key).toBe("slow");
	});

	it("replaces a connection with none of the wanted pieces first, before a choked one that has some", () => {
		const chokedUseful = slot({ key: "choked", choked: true, openedAt: 0, score: -5 });
		const useless = slot({ key: "useless", openedAt: 29_000, downloaded: 1_000_000, score: 10 });
		const slower = slot({ key: "slow", slow: true, downloaded: 1, openedAt: 0 });
		expect(replacementVictim([chokedUseful, slower, useless], 30_000, 30_000)?.key).toBe("choked");
		useless.useless = true;
		expect(replacementVictim([chokedUseful, slower, useless], 30_000, 30_000)?.key).toBe("useless");
		// Even before the choked one's grace period, and never one that delivers.
		expect(replacementVictim([slot({ key: "young", choked: true, openedAt: 29_000 }), useless], 30_000, 30_000)?.key).toBe("useless");
		expect(replacementVictim([slot({ key: "d", delivering: true, useless: true })], 30_000, 30_000)).toBeUndefined();
	});

	it("waits longer for a choked seed (patience), counting from its last delivery", () => {
		const seed = slot({ key: "seed", choked: true, patient: true, openedAt: 0 });
		const partial = slot({ key: "partial", choked: true, openedAt: 0, score: 9 });
		expect(replacementVictim([seed], 35_000, 30_000, 120_000)).toBeUndefined();
		expect(replacementVictim([seed, partial], 35_000, 30_000, 120_000)?.key).toBe("partial");
		expect(replacementVictim([seed], 120_000, 30_000, 120_000)?.key).toBe("seed");
		// It delivered until 100 s: choked only since then.
		expect(replacementVictim([{ ...partial, chokedSince: 100_000 }], 120_000, 30_000, 120_000)).toBeUndefined();
		// Without a patience the grace is the same for everyone.
		expect(replacementVictim([seed], 30_000, 30_000)?.key).toBe("seed");
	});
});

describe("socket width and in-flight cap", () => {
	it("adds a socket only when the rate stayed under the threshold for the whole window", () => {
		const samples = Array.from({ length: 31 }, (_, i) => ({ at: i * 1000, rate: 100 }));
		expect(shouldAddSocket(samples, 30_000, 30_000, 1_000, 1, 3)).toBe(true);
		expect(shouldAddSocket(samples, 29_000, 30_000, 1_000, 1, 3)).toBe(false);
		expect(shouldAddSocket([...samples, { at: 30_500, rate: 5_000 }], 31_000, 30_000, 1_000, 1, 3)).toBe(false);
		expect(shouldAddSocket(samples, 30_000, 30_000, 1_000, 3, 3)).toBe(false);
	});

	it("never allows more in flight than 16 MiB, whatever reqq a peer advertises", () => {
		expect(
			requestAllowance({ socketInflight: 0, limit: MAX_INFLIGHT_PER_SOCKET, peerOutstanding: 0, peerReqq: 100_000, blockLength: 16 * 1024 }),
		).toBe(MAX_INFLIGHT_PER_SOCKET);
		expect(
			requestAllowance({ socketInflight: MAX_INFLIGHT_PER_SOCKET - 1000, limit: MAX_INFLIGHT_PER_SOCKET, peerOutstanding: 0, peerReqq: 100_000, blockLength: 16 * 1024 }),
		).toBe(1000);
		expect(
			requestAllowance({ socketInflight: MAX_INFLIGHT_PER_SOCKET, limit: MAX_INFLIGHT_PER_SOCKET, peerOutstanding: 0, peerReqq: 10, blockLength: 16 * 1024 }),
		).toBe(0);
		expect(requestAllowance({ socketInflight: 0, limit: MAX_INFLIGHT_PER_SOCKET, peerOutstanding: 2, peerReqq: 2, blockLength: 16 * 1024 })).toBe(0);
	});
});

describe("request refill", () => {
	it("refills a peer only once half of its queue has drained, within the socket cap", () => {
		expect(needsRefill(0, 250)).toBe(true);
		expect(needsRefill(125, 250)).toBe(true);
		expect(needsRefill(126, 250)).toBe(false);
		expect(needsRefill(0, 1)).toBe(true);
		expect(needsRefill(1, 1)).toBe(false);
	});

	it("sizes a peer's queue to 8 s of its rate, within its reqq and the socket cap", () => {
		// Unknown rate: a small start.
		expect(queueDepth(0, 250)).toBe(16);
		// 50 KB/s → 8 s ≈ 24 blocks; 200 KB/s → ~98.
		expect(queueDepth(50_000, 250)).toBe(24);
		expect(queueDepth(200_000, 250)).toBe(98);
		// Fast peers are bounded by reqq, then by the 16 MiB socket cap (1024 blocks).
		expect(queueDepth(10_000_000, 250)).toBe(250);
		expect(queueDepth(10_000_000, 5000)).toBe(1024);
		// Very slow: never under 4.
		expect(queueDepth(100, 250)).toBe(4);
	});
});

