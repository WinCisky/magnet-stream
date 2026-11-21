// The deadline picker through wrangler dev, on fake peers that are fast,
// slow, partial or stalling: the playback window goes to the fast peers, a
// late block is asked of another peer and its duplicate dropped, and a seek
// sends cancels without closing a connection.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { freePort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { ConnectionManager, parseInfoDict } from "../src/index.ts";

const MiB = 1024 * 1024;
const PL = 64 * 1024;
const peers: FakePeer[] = [];
let dev: WranglerDev;

async function until(pred: () => boolean, ms = 20_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!pred()) {
		if (Date.now() > deadline) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 20));
	}
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

beforeAll(async () => {
	dev = await startWranglerDev({ port: await freePort(), vars: { ALLOW_PRIVATE_PEERS: "1" } });
}, 120_000);

afterAll(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
	await dev?.stop();
});

describe("deadline picker through the gateway", () => {
	it("serves the playback window from the fast peers; slow and partial peers do read-ahead", async () => {
		const torrent = makeTorrent({ seed: 81, size: 20 * MiB, pieceLength: PL });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const half = Array.from({ length: meta.pieceCount / 2 }, (_, i) => meta.pieceCount / 2 + i);
		const [fastA, fastB, slow, partial] = await Promise.all([
			startFakePeer({ torrent, blockGapMs: 4 }),
			startFakePeer({ torrent, blockGapMs: 4 }),
			startFakePeer({ torrent, blockGapMs: 40 }),
			startFakePeer({ torrent, blockGapMs: 4, have: half }),
		]);
		peers.push(fastA, fastB, slow, partial);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			picker: { windowBytes: 2 * MiB, scarceAheadBytes: 4 * MiB, headBytes: PL, tailBytes: PL },
		});
		mgr.addPeers([slow.addr, partial.addr, fastA.addr, fastB.addr]);
		await mgr.connect();
		mgr.setInfo(meta);
		// Warm-up: every peer delivers for a while, so the session knows their rates.
		const started = Date.now();
		const warm = mgr.recover({ offset: 0, size: 4 * MiB });
		await warm.done;
		await sleep(Math.max(0, 1_500 - (Date.now() - started)));
		expect(slow.stats.served).toBeGreaterThan(0);
		const mark = new Map([fastA, fastB, slow, partial].map((p) => [p, p.stats.servedPieces.length]));

		// The file: 12 MiB at 8 MiB; window 2 MiB (32 pieces) after the index piece.
		const first = (8 * MiB) / PL;
		const windowPieces = new Set(Array.from({ length: 31 }, (_, i) => first + 1 + i));
		const verified: number[] = [];
		const rec = mgr.recover({ offset: 8 * MiB, size: 12 * MiB }, { onPiece: (p) => verified.push(p) });
		expect(await rec.done).toBe(true);
		const after = (p: FakePeer) => p.stats.servedPieces.slice(mark.get(p));
		const inWindow = (p: FakePeer) => after(p).filter((piece) => windowPieces.has(piece)).length;
		expect(inWindow(slow)).toBe(0);
		expect(inWindow(fastA) + inWindow(fastB) + inWindow(partial)).toBeGreaterThanOrEqual(31 * 4);
		// Read-ahead went to everyone unchoked, the slow peer too.
		expect(after(slow).length).toBeGreaterThan(0);
		expect(after(partial).every((piece) => piece >= meta.pieceCount / 2)).toBe(true);
		expect(new Set(verified).size).toBe((12 * MiB) / PL);
		expect([fastA, fastB, slow, partial].every((p) => p.stats.closed === 0)).toBe(true);
		mgr.stop();
	}, 90_000);

	it("asks a second peer for blocks a stalling peer holds, and discards the late duplicate", async () => {
		const torrent = makeTorrent({ seed: 82, size: 2 * MiB, pieceLength: PL });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const stalling = await startFakePeer({ torrent, blockGapMs: 8_000 });
		const fast = await startFakePeer({ torrent, blockGapMs: 2 });
		peers.push(stalling, fast);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			picker: { windowBytes: 2 * MiB, unknownRateDueMs: 1_000, hedgeGraceMs: 500 },
		});
		mgr.addPeers([stalling.addr]);
		const started = Date.now();
		const pending = mgr.download(meta);
		// The stalling peer is alone at first: it takes the head of the window.
		await until(() => stalling.stats.requests > 0);
		mgr.addPeers([fast.addr]);
		const result = await pending;
		const elapsed = Date.now() - started;
		expect(result.verified).toHaveLength(meta.pieceCount);
		expect(result.hedges).toBeGreaterThan(0);
		// Far less than the stalling peer would need for its queue (8 s per block).
		expect(elapsed).toBeLessThan(20_000);
		// The losing requests were withdrawn.
		await until(() => stalling.stats.cancels > 0, 5_000);
	}, 60_000);

	it("on a seek cancels requests outside the new window, keeps every connection, and recovers from the new position", async () => {
		const torrent = makeTorrent({ seed: 83, size: 16 * MiB, pieceLength: PL });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const swarm = await Promise.all([0, 1, 2].map(() => startFakePeer({ torrent, blockGapMs: 15 })));
		peers.push(...swarm);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			picker: { windowBytes: 1 * MiB, headBytes: PL, tailBytes: PL },
		});
		mgr.addPeers(swarm.map((p) => p.addr));
		await mgr.connect();
		mgr.setInfo(meta);
		const verifiedAt = new Map<number, number>();
		const rec = mgr.recover({ offset: 0, size: meta.totalLength }, { onPiece: (p) => verifiedAt.set(p, Date.now()) });
		await until(() => verifiedAt.size >= 8 && mgr.liveCount === 3);
		const live = mgr.liveCount;
		const seekAt = Date.now();
		const target = 192; // 12 MiB in
		rec.seek(target * PL);
		expect(mgr.cancels).toBeGreaterThan(0);
		// The new window (16 pieces from 192) arrives within a few seconds.
		await until(() => [...Array(16).keys()].every((i) => verifiedAt.has(target + i)), 15_000);
		expect(Math.max(...[...Array(16).keys()].map((i) => verifiedAt.get(target + i)! - seekAt))).toBeLessThan(10_000);
		await until(() => swarm.reduce((n, p) => n + p.stats.cancels, 0) > 0, 5_000);
		expect(swarm.every((p) => p.stats.closed === 0 && p.stats.connections === 1)).toBe(true);
		expect(mgr.liveCount).toBe(live);
		expect(mgr.deliveringDropped).toBe(0);
		rec.stop();
		expect(await rec.done).toBe(false);
		mgr.stop();
	}, 90_000);
});
