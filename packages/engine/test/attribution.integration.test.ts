// Peers that corrupt, through wrangler dev (M3-02): in a swarm with honest
// peers and one that serves corrupted blocks, every piece handed over is
// verified, the corrupting peer is excluded after two failed pieces and
// never dialled again, the honest peers that shared mixed pieces with it
// keep their standing, and the download completes. v1 isolates the
// culprit of a mixed piece by fetching it from one peer; v2 names it from
// the piece's leaf hashes.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent } from "@magnet-stream/fake-peer";
import { freePort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { ConnectionManager, parseInfoDict } from "../src/index.ts";

const MiB = 1024 * 1024;
const peers: FakePeer[] = [];
let dev: WranglerDev;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

beforeAll(async () => {
	dev = await startWranglerDev({ port: await freePort(), vars: { ALLOW_PRIVATE_PEERS: "1" } });
}, 120_000);

afterAll(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
	await dev?.stop();
});

async function swarm(torrent: FakeTorrent) {
	// A shallow queue spreads each piece's 16 blocks over several peers: mixed pieces.
	const [honestA, honestB, corrupt] = await Promise.all([
		startFakePeer({ torrent, reqq: 8 }),
		startFakePeer({ torrent, reqq: 8 }),
		startFakePeer({ torrent, reqq: 8, corrupt: true }),
	]);
	peers.push(honestA, honestB, corrupt);
	return { honestA, honestB, corrupt };
}

async function run(torrent: FakeTorrent) {
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const { honestA, honestB, corrupt } = await swarm(torrent);
	let bans = 0;
	const mgr = new ConnectionManager({ gatewayUrl: dev.url, infoHash: torrent.infoHash, allowPrivate: true, onBan: () => bans++ });
	mgr.addPeers([corrupt.addr, honestA.addr, honestB.addr]);
	const delivered = new Map<number, Uint8Array>();
	const result = await mgr.download(meta, { onPiece: (p, data) => delivered.set(p, data.slice()) });
	return { meta, mgr, result, delivered, bans, honestA, honestB, corrupt };
}

function expectSource(torrent: FakeTorrent, pieceLength: number, delivered: Map<number, Uint8Array>): void {
	for (const [p, data] of delivered) {
		const expected = torrent.data.subarray(p * pieceLength, p * pieceLength + data.length);
		expect(Buffer.from(data).equals(Buffer.from(expected)), `piece ${p}`).toBe(true);
	}
}

describe("a peer that corrupts", () => {
	it("v1: is banned after two failed pieces, never redialled; honest peers of mixed pieces stay; the download completes", async () => {
		const torrent = makeTorrent({ seed: 201, size: 3 * MiB, pieceLength: 256 * 1024 });
		const { meta, mgr, result, delivered, bans, honestA, honestB, corrupt } = await run(torrent);
		expect(result.verified).toHaveLength(meta.pieceCount);
		expect(result.corrupt).toEqual([]);
		expect(result.missing).toEqual([]);
		// Nothing unverified reached onPiece: every piece is the source's bytes.
		expect(delivered.size).toBe(meta.pieceCount);
		expectSource(torrent, meta.pieceLength, delivered);
		expect(result.hashFailures).toBeGreaterThanOrEqual(1);
		expect(result.banned).toBe(1);
		expect(bans).toBe(1);
		expect(mgr.strikesOf(corrupt.addr)).toBe(2);
		expect(mgr.book.isBanned(corrupt.addr)).toBe(true);
		expect(mgr.strikesOf(honestA.addr) + mgr.strikesOf(honestB.addr)).toBe(0);
		expect(mgr.book.isBanned(honestA.addr) || mgr.book.isBanned(honestB.addr)).toBe(false);
		// The pieces it spoiled were shared with honest peers (mixed), and resolved without striking them.
		expect(mgr.attributionCounts.mixed).toBeGreaterThan(0);
		expect(corrupt.stats.connections).toBe(1);
	}, 90_000);

	it("is not contacted again in the session, even when named again", async () => {
		const torrent = makeTorrent({ seed: 202, size: 3 * MiB, pieceLength: 256 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const { honestA, corrupt } = await swarm(torrent);
		const mgr = new ConnectionManager({ gatewayUrl: dev.url, infoHash: torrent.infoHash, allowPrivate: true });
		mgr.addPeers([corrupt.addr, honestA.addr]);
		await mgr.connect();
		mgr.setInfo(meta);
		const first = mgr.recover({ offset: 0, size: 2 * MiB });
		expect(await first.done).toBe(true);
		expect(mgr.book.isBanned(corrupt.addr)).toBe(true);
		mgr.addPeers([corrupt.addr]);
		const second = mgr.recover({ offset: 2 * MiB, size: 1 * MiB });
		expect(await second.done).toBe(true);
		await sleep(500);
		expect(corrupt.stats.connections).toBe(1);
		expect(mgr.strikesOf(honestA.addr)).toBe(0);
		mgr.stop();
	}, 90_000);

	it("v2: the leaf hashes of a failed piece name the peer that sent the bad block", async () => {
		const torrent = makeTorrent({ seed: 203, files: [{ path: ["v2.mkv"], length: 3 * MiB }], pieceLength: 256 * 1024, version: 2 });
		const { meta, mgr, result, delivered, honestA, honestB, corrupt } = await run(torrent);
		expect(result.verified).toHaveLength(meta.pieceCount);
		expect(result.corrupt).toEqual([]);
		expectSource(torrent, meta.pieceLength, delivered);
		expect(result.banned).toBe(1);
		expect(mgr.book.isBanned(corrupt.addr)).toBe(true);
		expect(mgr.attributionCounts.resolvedByLeaves).toBeGreaterThan(0);
		expect(mgr.strikesOf(honestA.addr) + mgr.strikesOf(honestB.addr)).toBe(0);
		expect(corrupt.stats.connections).toBe(1);
	}, 90_000);
});
