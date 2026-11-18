import { describe, expect, it } from "vitest";
import { blockLength, type Metainfo } from "@magnet-stream/engine/metainfo";
import type { BatchPlan, BatchSink } from "./batch";
import { parsePageMessage, parseWorkerMessage } from "./session-protocol";

describe("session messages", () => {
	it("accepts a start and a want, and rejects anything else", () => {
		expect(parsePageMessage({ type: "start", magnet: "magnet:?xt=urn:btih:aa", gatewayUrl: "http://127.0.0.1:8787", seedersUrl: "http://127.0.0.1:8080" })?.type).toBe("start");
		expect(parsePageMessage({ type: "want", id: 1, blocks: [{ piece: 0, begin: 0, length: 16 }] })?.type).toBe("want");
		expect(parsePageMessage({ type: "stop" })).toEqual({ type: "stop" });
		expect(parsePageMessage({ type: "idle" })).toEqual({ type: "idle" });
		expect(parsePageMessage({ type: "cancel", id: 3 })).toEqual({ type: "cancel", id: 3 });
		expect(parsePageMessage({ type: "cancel", id: -1 })).toBeNull();
		expect(parsePageMessage({ type: "cancel", id: "3" })).toBeNull();
		expect(parsePageMessage({ type: "start", magnet: "magnet:?xt=urn:btih:aa", gatewayUrl: "ftp://x", seedersUrl: "http://127.0.0.1" })).toBeNull();
		expect(parsePageMessage({ type: "want", id: 1, blocks: [{ piece: 0, begin: 0, length: 0 }] })).toBeNull();
		expect(parsePageMessage({ type: "files" })).toBeNull();
		expect(parseWorkerMessage({ type: "error", message: "metadata timed out" })?.type).toBe("error");
		expect(parseWorkerMessage({ type: "peers", count: 2 })).toEqual({ type: "peers", count: 2 });
		expect(parseWorkerMessage({ type: "stats", upgrades: 1, messages: 40, known: 9, delivered: 2 })).toEqual({ type: "stats", upgrades: 1, messages: 40, known: 9, delivered: 2 });
		expect(parseWorkerMessage({ type: "stats", upgrades: 1, messages: -1, known: 0, delivered: 0 })).toBeNull();
		expect(parseWorkerMessage({ type: "stats", upgrades: 1, messages: 2 })).toBeNull();
		expect(parseWorkerMessage({ type: "error", message: "1.2.3.4:6881" })).toEqual({ type: "error", message: "1.2.3.4:6881" });
	});
});

describe("scheduler transport", () => {
	it("asks the session for the planned blocks and reports them back", async () => {
		const meta = { pieceLength: 32 * 1024, totalLength: 64 * 1024, pieceCount: 2 } as Metainfo;
		const asked: { piece: number; begin: number; length: number }[][] = [];
		const session = {
			request(blocks: { piece: number; begin: number; length: number }[], onBlock: (piece: number, begin: number, data: Uint8Array) => void) {
				asked.push(blocks);
				for (const block of blocks) onBlock(block.piece, block.begin, new Uint8Array(block.length).fill(block.piece + 1));
				return Promise.resolve({ received: blocks.length, missing: 0 });
			},
		};
		const plan: BatchPlan = { id: 1, blocks: [[0, 0], [0, 1], [1, 0]], peers: [], fallbacks: [], urgent: true, waitMs: 1000, holdMs: 0, staggerMs: 0 };
		const got: number[] = [];
		const sink: BatchSink = {
			started() {},
			block(piece, begin, data) {
				got.push(piece * 1000 + begin);
				expect(data[0]).toBe(piece + 1);
				expect(data.length).toBe(blockLength(meta, piece, begin / 16384));
				return false;
			},
			rejected() {
				return false;
			},
			have() {},
		};
		const end = await serve(session, meta, plan, sink);
		expect(end).toEqual({ kind: "done", bytes: 16 * 1024 * 3 });
		expect(asked[0].map((b) => [b.piece, b.begin])).toEqual([[0, 0], [0, 16384], [1, 0]]);
		expect(got).toEqual([0, 16384, 1000]);
	});
});

async function serve(
	session: { request(blocks: { piece: number; begin: number; length: number }[], onBlock: (piece: number, begin: number, data: Uint8Array) => void, signal: AbortSignal): Promise<{ received: number; missing: number }> },
	meta: Metainfo,
	plan: BatchPlan,
	sink: BatchSink,
) {
	const controller = new AbortController();
	const blocks = plan.blocks.map(([piece, block]) => ({ piece, begin: block * 16384, length: blockLength(meta, piece, block) }));
	let bytes = 0;
	const end = await session.request(blocks, (piece, begin, data) => {
		bytes += data.length;
		sink.block(piece, begin, data);
	}, controller.signal);
	return end.missing === 0 ? { kind: "done" as const, bytes } : { kind: "ended" as const, bytes };
}
