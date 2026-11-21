// Connection manager through wrangler dev: dead and choked peers are
// replaced, a delivering peer is never dropped, upstream stays within a
// message budget, and a gateway restart resumes a verified download.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { closedPort, freePort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { ConnectionManager, MAX_INFLIGHT_PER_SOCKET, parseInfoDict, type Metainfo } from "../src/index.ts";

const peers: FakePeer[] = [];
let dev: WranglerDev;
let port: number;

async function until(pred: () => boolean, ms = 15_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!pred()) {
		if (Date.now() > deadline) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 20));
	}
}

/** ~1 upstream message per batch interval per socket, plus a burst for open and one reconnect. */
function messageBudget(elapsedMs: number, sockets: number, batchMs = 400): number {
	return sockets * (Math.ceil(elapsedMs / batchMs) + 4);
}

beforeAll(async () => {
	port = await freePort();
	dev = await startWranglerDev({ port, vars: { ALLOW_PRIVATE_PEERS: "1" } });
}, 120_000);

afterAll(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
	await dev?.stop();
});

describe("connection manager through the gateway", () => {
	it("replaces dead candidates and keeps in-flight data and upstream messages inside their caps", async () => {
		const torrent = makeTorrent({ seed: 71, size: 512 * 1024, pieceLength: 16 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const good = await Promise.all([0, 1, 2, 3].map(() => startFakePeer({ torrent, reqq: 2000 })));
		peers.push(...good);
		const dead = await Promise.all([0, 1, 2, 3, 4, 5].map(() => closedPort()));
		const addresses: string[] = [];
		for (let i = 0; i < dead.length; i++) {
			addresses.push(`127.0.0.1:${dead[i]}`);
			if (good[i]) addresses.push(good[i].addr);
		}
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			chokeReplaceAfterMs: 300,
			stallTimeoutMs: 30_000,
		});
		mgr.addPeers(addresses);
		const started = Date.now();
		const result = await mgr.download(meta, {
			onPiece: (piece, data) => {
				const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
				expect(Buffer.from(data).equals(Buffer.from(expected))).toBe(true);
			},
		});
		const elapsed = Date.now() - started;
		expect(result.verified).toEqual([...Array(meta.pieceCount).keys()]);
		expect(result.corrupt).toEqual([]);
		expect(result.missing).toEqual([]);
		expect(mgr.untestedHighWater).toBeLessThanOrEqual(3);
		expect(mgr.deliveringDropped).toBe(0);
		expect(result.inflightHighWater).toBeLessThanOrEqual(MAX_INFLIGHT_PER_SOCKET);
		expect(result.inflightHighWater).toBeGreaterThan(0);
		expect(result.messagesSent).toBeLessThanOrEqual(messageBudget(elapsed, Math.max(1, result.sockets)));
		expect(result.messagesSent).toBeLessThan(Math.ceil(torrent.totalLength / 16384));
		expect(good.some((p) => p.stats.served > 0)).toBe(true);
		expect(dead.filter((p) => (mgr.book.get(`127.0.0.1:${p}`)?.failures ?? 0) > 0).length).toBeGreaterThan(0);
		const logs = dev.output();
		expect(logs.includes(torrent.infoHashHex)).toBe(false);
		expect(logs.includes(good[0].addr)).toBe(false);
		console.log(`upstream messages: ${result.messagesSent} in ${elapsed} ms, inflight high water ${result.inflightHighWater}, sockets ${result.sockets}`);
	}, 60_000);

	it("replaces peers that stay choked so a candidate can deliver", async () => {
		const torrent = makeTorrent({ seed: 74, size: 256 * 1024, pieceLength: 16 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const choked = await Promise.all([0, 1, 2, 3, 4, 5].map(() => startFakePeer({ torrent, unchoke: false })));
		const good = await startFakePeer({ torrent });
		peers.push(...choked, good);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			chokeReplaceAfterMs: 400,
			stallTimeoutMs: 20_000,
			// One socket: early width would otherwise give the good peer a slot of its own.
			maxSockets: 1,
		});
		mgr.addPeers([...choked.map((p) => p.addr), good.addr]);
		const result = await mgr.download(meta);
		expect(result.verified).toEqual([...Array(meta.pieceCount).keys()]);
		expect(good.stats.served).toBeGreaterThan(0);
		expect(choked.some((p) => p.stats.closed > 0)).toBe(true);
		expect(mgr.deliveringDropped).toBe(0);
	}, 40_000);

	it("does not close a delivering connection to make room for a candidate", async () => {
		const torrent = makeTorrent({ seed: 72, size: 2 * 1024 * 1024, pieceLength: 64 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const six = await Promise.all([0, 1, 2, 3, 4, 5].map(() => startFakePeer({ torrent, blockGapMs: 200 })));
		const extra = await startFakePeer({ torrent });
		peers.push(...six, extra);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			chokeReplaceAfterMs: 200,
			deliveringWindowMs: 5_000,
			stallTimeoutMs: 30_000,
		});
		mgr.addPeers(six.map((p) => p.addr));
		const pending = mgr.download(meta);
		await until(() => mgr.liveCount === 6 && mgr.downloadedBytes > 0);
		mgr.addPeers([extra.addr]);
		await new Promise((r) => setTimeout(r, 800));
		expect(mgr.downloadedBytes).toBeLessThan(torrent.totalLength);
		expect(extra.stats.connections).toBe(0);
		expect(six.every((p) => p.stats.connections === 1 && p.stats.closed === 0)).toBe(true);
		expect(mgr.liveCount).toBe(6);
		expect(mgr.deliveringDropped).toBe(0);
		mgr.stop();
		await pending;
	}, 60_000);

	it("resumes a download after the gateway restarts, with every piece verified", async () => {
		const torrent = makeTorrent({ seed: 73, size: 2 * 1024 * 1024, pieceLength: 16 * 1024 });
		const meta: Metainfo = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const peer = await startFakePeer({ torrent, blockGapMs: 250, reqq: 64 });
		peers.push(peer);
		const mgr = new ConnectionManager({
			gatewayUrl: `http://127.0.0.1:${port}`,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			reconnectDelayMs: 500,
			stallTimeoutMs: 90_000,
		});
		mgr.addPeers([peer.addr]);
		const started = Date.now();
		const pending = mgr.download(meta, {
			onPiece: (piece, data) => {
				const expected = torrent.data.subarray(piece * torrent.pieceLength, piece * torrent.pieceLength + data.length);
				if (!Buffer.from(data).equals(Buffer.from(expected))) throw new Error(`piece ${piece} mismatch`);
			},
		});
		await until(() => mgr.downloadedBytes >= 64 * 1024, 20_000);
		expect(mgr.downloadedBytes).toBeLessThan(torrent.totalLength);
		const connectionsBeforeRestart = peer.stats.connections;
		expect(connectionsBeforeRestart).toBeGreaterThan(0);
		await dev.stop();
		dev = await startWranglerDev({ port, vars: { ALLOW_PRIVATE_PEERS: "1" } });
		const result = await pending;
		expect(peer.stats.connections).toBeGreaterThan(connectionsBeforeRestart);
		const elapsed = Date.now() - started;
		expect(result.verified).toEqual([...Array(meta.pieceCount).keys()]);
		expect(result.corrupt).toEqual([]);
		expect(result.messagesSent).toBeLessThanOrEqual(messageBudget(elapsed, Math.max(1, result.sockets)));
		expect(mgr.deliveringDropped).toBe(0);
		expect(result.inflightHighWater).toBeLessThanOrEqual(MAX_INFLIGHT_PER_SOCKET);
		console.log(`resumed download: ${result.messagesSent} upstream messages in ${elapsed} ms`);
	}, 180_000);

	it("serves the page's block requests: overlapping wants share a block, a cancel leaves the other want alone, suspend closes and a want reopens", async () => {
		const torrent = makeTorrent({ seed: 75, size: 256 * 1024, pieceLength: 32 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const peer = await startFakePeer({ torrent, blockGapMs: 20 });
		peers.push(peer);
		const sockets: WebSocket[] = [];
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			batchMs: 50,
			factory: (url, protocol) => {
				const ws = new WebSocket(url, protocol);
				sockets.push(ws);
				return ws as never;
			},
		});
		mgr.addPeers([peer.addr]);
		await mgr.connect();
		mgr.setInfo(meta);
		const block = (piece: number, begin: number) => ({ piece, begin, length: 16 * 1024 });
		const expectBlock = (piece: number, begin: number, data: Uint8Array) => {
			const at = piece * meta.pieceLength + begin;
			expect(Buffer.from(data).equals(Buffer.from(torrent.data.subarray(at, at + data.length)))).toBe(true);
		};

		const cancel = new AbortController();
		const first = mgr.requestBlocks([block(0, 0), block(0, 16384), block(1, 0)], { signal: cancel.signal, onBlock: expectBlock });
		const second = mgr.requestBlocks([block(1, 0), block(1, 16384)], { onBlock: expectBlock });
		cancel.abort();
		expect((await first).received).toBeLessThan(3);
		expect(await second).toEqual({ received: 2, missing: 0 });

		mgr.suspend();
		await until(() => sockets.length === 1 && sockets[0].readyState === WebSocket.CLOSED);
		expect(mgr.socketsOpen).toBe(0);
		expect(mgr.liveCount).toBe(0);
		const later = await mgr.requestBlocks([block(7, 0), block(7, 16384)], { onBlock: expectBlock, timeoutMs: 15_000 });
		expect(later).toEqual({ received: 2, missing: 0 });
		expect(sockets.length).toBe(2);
		mgr.stop();
		expect(await mgr.requestBlocks([block(2, 0)], { onBlock: expectBlock })).toEqual({ received: 0, missing: 1 });
	}, 60_000);

	it("closes every WebSocket of a wide session on stop", async () => {
		const torrent = makeTorrent({ seed: 76, size: 4 * 1024 * 1024, pieceLength: 64 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const peer = await startFakePeer({ torrent, blockGapMs: 100, reqq: 8 });
		peers.push(peer);
		const sockets: WebSocket[] = [];
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			// Always "too slow": a socket is added after every 1 s window, up to 3.
			rateThreshold: 1e12,
			rateWindowMs: 1_000,
			factory: (url, protocol) => {
				const ws = new WebSocket(url, protocol);
				sockets.push(ws);
				return ws as never;
			},
		});
		mgr.addPeers([peer.addr]);
		const pending = mgr.download(meta);
		await until(() => mgr.socketsOpen === 3, 20_000);
		mgr.stop();
		await pending;
		expect(mgr.socketsOpen).toBe(0);
		await until(() => sockets.length === 3 && sockets.every((ws) => ws.readyState === WebSocket.CLOSED));
	}, 60_000);

	it("on a slow peer, upstream messages follow the data (half-queue refills), not the clock", async () => {
		// ~400 KB/s: 4 MiB takes ~10 s. Refilling per block would send a message every batch (~25).
		const torrent = makeTorrent({ seed: 77, size: 4 * 1024 * 1024, pieceLength: 256 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const peer = await startFakePeer({ torrent, blockGapMs: 40, reqq: 128 });
		peers.push(peer);
		const mgr = new ConnectionManager({ gatewayUrl: dev.url, infoHash: torrent.infoHash, allowPrivate: true, stallTimeoutMs: 30_000 });
		mgr.addPeers([peer.addr]);
		const started = Date.now();
		const result = await mgr.download(meta);
		const elapsed = Date.now() - started;
		expect(result.verified).toHaveLength(meta.pieceCount);
		expect(elapsed).toBeGreaterThan(6_000);
		// open + interested + first fill, then one refill per 64 blocks (1 MiB): ~4, plus slack.
		expect(result.messagesSent).toBeLessThanOrEqual(10);
		console.log(`slow peer: ${result.messagesSent} upstream messages for 4 MiB in ${elapsed} ms`);
	}, 60_000);
});

