// Reciprocation through wrangler dev (M3-12): a tit-for-tat leecher that
// holds pieces we lack unchokes us only once we upload to it, and we then
// download from it; with upload off it never does and sees no bitfield,
// unchoke or piece. Upload stays within the upstream message budget.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent } from "@magnet-stream/fake-peer";
import { freePort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { ConnectionManager, parseInfoDict, type PieceSource } from "../src/index.ts";

const peers: FakePeer[] = [];
let dev: WranglerDev;

beforeAll(async () => {
	dev = await startWranglerDev({ port: await freePort(), vars: { ALLOW_PRIVATE_PEERS: "1" } });
}, 120_000);

afterAll(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
	await dev?.stop();
});

/** Pieces held in memory (what the page's OPFS store does), with a read counter. */
function memorySource(torrent: FakeTorrent, pieces: Iterable<number>): PieceSource & { held: Set<number>; reads: number } {
	const held = new Set(pieces);
	const src = {
		held,
		reads: 0,
		has: (p: number) => held.has(p),
		pieces: () => [...held],
		read(p: number, begin: number, length: number) {
			if (!held.has(p)) throw new Error("not verified");
			src.reads++;
			const at = p * torrent.pieceLength + begin;
			return torrent.data.slice(at, at + length);
		},
	};
	return src;
}

const range = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => from + i);

async function session(torrent: FakeTorrent, upload: boolean, addresses: string[], have: number[]) {
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const source = memorySource(torrent, have);
	const mgr = new ConnectionManager({ gatewayUrl: dev.url, infoHash: torrent.infoHash, allowPrivate: true, upload: { source, enabled: upload } });
	mgr.setInfo(meta, "test", torrent.info);
	mgr.addPeers(addresses);
	let bad = 0;
	const recovery = mgr.recover(
		{ offset: 0, size: meta.totalLength },
		{
			have,
			onPiece: (piece, data) => {
				const at = piece * meta.pieceLength;
				if (!Buffer.from(data).equals(Buffer.from(torrent.data.subarray(at, at + data.length)))) bad++;
				source.held.add(piece);
			},
		},
	);
	return { mgr, recovery, source, bad: () => bad };
}

describe("upload through the gateway", () => {
	it("a tit-for-tat leecher unchokes us once we upload, and we download from it", async () => {
		const torrent = makeTorrent({ seed: 121, size: 2 * 1024 * 1024, pieceLength: 64 * 1024 });
		const mine = range(0, torrent.pieceCount / 2);
		const leecher = await startFakePeer({ torrent, titForTat: true, have: range(torrent.pieceCount / 2, torrent.pieceCount) });
		peers.push(leecher);
		const started = Date.now();
		const s = await session(torrent, true, [leecher.addr], mine);
		const complete = await Promise.race([s.recovery.done, new Promise<false>((r) => setTimeout(() => r(false), 30_000))]);
		const elapsed = Date.now() - started;
		const d = s.mgr.diagnostics();
		const stats = s.mgr.uploadStats;
		s.mgr.stop();
		expect(complete).toBe(true);
		expect(s.bad()).toBe(0);
		expect(leecher.stats.titForTatUnchokes).toBe(1);
		expect(leecher.stats.received.bitfield).toBe(1);
		expect(leecher.stats.received.unchoke).toBeGreaterThanOrEqual(1);
		expect(leecher.stats.received.bytes).toBeGreaterThanOrEqual(16 * 1024);
		expect(leecher.stats.received.badBlocks).toBe(0);
		expect(leecher.stats.served).toBeGreaterThan(0);
		// Counted when queued: blocks still waiting for the lazy flush die with stop().
		expect(stats.bytes).toBeGreaterThanOrEqual(leecher.stats.received.bytes);
		expect(d.upload.bytes).toBe(stats.bytes);
		// One gateway (a Cloudflare one): every byte, both ways, went through it.
		expect(d.paths.cloudflare).toEqual({ downloaded: s.mgr.downloadedBytes, uploaded: stats.bytes, connections: 1 });
		expect(d.paths.vps).toEqual({ downloaded: 0, uploaded: 0, connections: 0 });
		// Every read was for a verified piece, through a request that passed the checks.
		expect(s.source.reads).toBe(stats.reads);
		// Upstream: one batch per 400 ms per socket at most, plus the upload flushes (one per 2 s per socket).
		const sockets = Math.max(1, d.sockets);
		expect(d.messagesSent).toBeLessThanOrEqual(sockets * (Math.ceil(elapsed / 400) + 4));
		expect(d.upload.messages).toBeLessThanOrEqual(sockets * (Math.ceil(elapsed / 2_000) + 1));
		console.log(`upload: ${stats.bytes} B up, ${s.mgr.downloadedBytes} B down, ${d.messagesSent} messages (${d.upload.messages} upload-only) in ${elapsed} ms`);
	}, 60_000);

	it("with upload off the leecher never unchokes us and sees no bitfield, have, unchoke or piece", async () => {
		const torrent = makeTorrent({ seed: 122, size: 2 * 1024 * 1024, pieceLength: 64 * 1024 });
		const mine = range(0, torrent.pieceCount / 2);
		const leecher = await startFakePeer({ torrent, titForTat: true, have: range(torrent.pieceCount / 2, torrent.pieceCount) });
		peers.push(leecher);
		const s = await session(torrent, false, [leecher.addr], mine);
		await new Promise((r) => setTimeout(r, 6_000));
		const d = s.mgr.diagnostics();
		s.mgr.stop();
		expect(d.connected).toBe(1);
		expect(s.mgr.downloadedBytes).toBe(0);
		const r = leecher.stats.received;
		expect([r.bitfield, r.have, r.haveAll, r.unchoke, r.piece]).toEqual([0, 0, 0, 0, 0]);
		expect(r.haveNone).toBe(1);
		expect(leecher.stats.titForTatUnchokes).toBe(0);
		expect(s.source.reads).toBe(0);
		expect(d.upload).toMatchObject({ enabled: false, bytes: 0, unchoked: 0 });
	}, 30_000);

	it("in a swarm with a seed: the seed is never unchoked, the leecher is, and upload stops when the download completes", async () => {
		const torrent = makeTorrent({ seed: 123, size: 4 * 1024 * 1024, pieceLength: 64 * 1024 });
		const half = torrent.pieceCount / 2;
		const seed = await startFakePeer({ torrent, blockGapMs: 5 });
		const leecher = await startFakePeer({ torrent, titForTat: true, have: range(half, torrent.pieceCount) });
		peers.push(seed, leecher);
		const s = await session(torrent, true, [seed.addr, leecher.addr], range(0, half / 2));
		const complete = await Promise.race([s.recovery.done, new Promise<false>((r) => setTimeout(() => r(false), 40_000))]);
		expect(complete).toBe(true);
		// The next sweep (250 ms) sees no recovery: everyone is choked.
		await new Promise((r) => setTimeout(r, 600));
		const d = s.mgr.diagnostics();
		const up = s.mgr.uploadStats.bytes;
		// The choke is lazy: it leaves with the next flush (at most 2 s).
		await new Promise((r) => setTimeout(r, 2_500));
		const later = s.mgr.uploadStats.bytes;
		s.mgr.stop();
		expect(seed.stats.received.unchoke).toBe(0);
		expect(leecher.stats.received.unchoke).toBeGreaterThanOrEqual(1);
		expect(leecher.stats.received.choke).toBeGreaterThanOrEqual(1);
		expect(d.upload.unchoked).toBe(0);
		expect(d.upload.active).toBe(false);
		expect(later).toBe(up);
		expect(s.bad()).toBe(0);
	}, 60_000);
});
