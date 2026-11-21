// Scout-guided dialling through wrangler dev (docs/architecture.md
// §Session start steps 3–4, §Connection management; OQ-21, OQ-23): a
// simulated `/peers` stream with 30 dead addresses (silent peers: the
// gateway waits for a handshake that never comes) and 2 live ones; the
// info dict from the stream; early width; one socket while a peer
// delivers; a connection with none of the wanted pieces replaced first.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { freePort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { ConnectionManager, MetadataFetcher, parseInfoDict, parseMagnet, readPeerBody, type ScoutFacts } from "../src/index.ts";

const peers: FakePeer[] = [];
let dev: WranglerDev;

async function until(pred: () => boolean, ms = 15_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!pred()) {
		if (Date.now() > deadline) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 20));
	}
}

/** An NDJSON body whose parts are written with pauses between them. */
function stream(parts: (string | number)[]): ReadableStream<Uint8Array> {
	return new ReadableStream({
		async start(controller) {
			for (const part of parts) {
				if (typeof part === "number") await new Promise((r) => setTimeout(r, part));
				else controller.enqueue(new TextEncoder().encode(part));
			}
			controller.close();
		},
	});
}

const line = (peer: string, scout?: ScoutFacts) => `${JSON.stringify({ peer, ...scout })}\n`;
const info = (bytes: Uint8Array) => `${JSON.stringify({ info: Buffer.from(bytes).toString("base64"), size: bytes.length })}\n`;
const SEED: ScoutFacts = { score: 0.92, alive: true, seed: true, have: 1, md: true, unchoked: true, rtt: 12 };

beforeAll(async () => {
	dev = await startWranglerDev({ port: await freePort(), vars: { ALLOW_PRIVATE_PEERS: "1" } });
}, 120_000);

afterAll(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
	await dev?.stop();
});

describe("scout-guided dialling through the gateway", () => {
	it("connects the 2 live peers within 3 s of their lines behind 30 dead ones, and takes the info dict from the stream", async () => {
		const torrent = makeTorrent({ seed: 101, size: 2 * 1024 * 1024, pieceLength: 64 * 1024 });
		const dead = await Promise.all(Array.from({ length: 30 }, () => startFakePeer({ torrent, silent: true })));
		const live = await Promise.all([startFakePeer({ torrent }), startFakePeer({ torrent })]);
		peers.push(...dead, ...live);
		const magnet = parseMagnet(live[0].magnet);
		const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, v2: magnet.v2, wire: magnet.infoHashHex } });
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			onExtensions: (conn) => fetcher.offer(conn),
			onMetadata: (conn, m) => fetcher.onMessage(conn, m),
			onPeerClose: (conn) => fetcher.onClose(conn),
		});
		const started = Date.now();
		await mgr.connect();
		const arrived = new Map<string, number>();
		const connectedAt = new Map<string, number>();
		const liveAddrs = new Set(live.map((p) => p.addr));
		const body = stream([
			...dead.map((p, i) => line(p.addr, i % 2 === 0 ? { score: 0.2 } : undefined)),
			1_500,
			...live.map((p) => line(p.addr, SEED)),
			info(torrent.info),
			'{"done":true,"count":32,"cached":false,"complete":true,"alive":2,"dead":0,"unknown":30}\n',
		]);
		const read = readPeerBody(body, {
			onPeer: (address, scout) => {
				if (liveAddrs.has(address)) arrived.set(address, Date.now());
				mgr.addPeers([address], "discovery", [scout]);
			},
			onInfo: (bytes) => void fetcher.offerBytes(bytes),
		});
		const watch = setInterval(() => {
			for (const a of liveAddrs) if (!connectedAt.has(a) && mgr.book.get(a)?.state === "connected") connectedAt.set(a, Date.now());
		}, 10);
		let meta;
		try {
			const result = await read;
			expect(result).toMatchObject({ peers: 32, info: true, rejected: 0, done: true });
			const fetched = await fetcher.done;
			expect(fetched.source).toBe("external");
			meta = fetched.meta;
			mgr.setInfo(meta, fetched.source);
			await until(() => connectedAt.size === 2, 5_000);
		} finally {
			clearInterval(watch);
		}
		const delays = [...liveAddrs].map((a) => connectedAt.get(a)! - arrived.get(a)!);
		for (const ms of delays) expect(ms).toBeLessThan(3_000);
		// Early width: nothing delivered and 30 candidates for 6 slots.
		expect(mgr.socketsOpen).toBe(3);
		expect(mgr.untestedHighWater).toBeLessThanOrEqual(3);
		const d = mgr.diagnostics();
		expect(d.dials.scout).toMatchObject({ attempted: 2, succeeded: 2 });
		expect(d.dials.untried.attempted).toBeLessThanOrEqual(9);
		expect(d.metadata?.source).toBe("external");
		console.log(
			`live peers connected ${delays.join(" / ")} ms after their lines; dials ${JSON.stringify(d.dials)}; ` +
				`upstream messages ${mgr.messagesSent} over ${mgr.upgrades} sockets in ${Date.now() - started} ms`,
		);

		// The session goes on: the file through the two live peers.
		const recovery = mgr.recover({ offset: 0, size: meta.totalLength });
		expect(await recovery.done).toBe(true);
		expect(live.some((p) => p.stats.served > 0)).toBe(true);
		mgr.stop();
		fetcher.stop();
		const logs = dev.output();
		expect(logs.toLowerCase().includes(torrent.infoHashHex)).toBe(false);
		for (const p of [...live, ...dead.slice(0, 3)]) expect(logs.includes(p.addr)).toBe(false);
	}, 60_000);

	it("refuses a wrong info dict from the stream and gets the metadata from a peer", async () => {
		const torrent = makeTorrent({ seed: 102, size: 512 * 1024, pieceLength: 32 * 1024 });
		const other = makeTorrent({ seed: 103, size: 512 * 1024, pieceLength: 32 * 1024 });
		const live = await startFakePeer({ torrent });
		peers.push(live);
		const magnet = parseMagnet(live.magnet);
		const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, wire: magnet.infoHashHex } });
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			onExtensions: (conn) => fetcher.offer(conn),
			onMetadata: (conn, m) => fetcher.onMessage(conn, m),
			onPeerClose: (conn) => fetcher.onClose(conn),
		});
		await mgr.connect();
		const offered: Promise<boolean>[] = [];
		await readPeerBody(stream([info(other.info), line(live.addr, SEED)]), {
			onPeer: (address, scout) => mgr.addPeers([address], "discovery", [scout]),
			onInfo: (bytes) => offered.push(fetcher.offerBytes(bytes)),
		});
		expect(await Promise.all(offered)).toEqual([false]);
		const { meta, source } = await fetcher.done;
		expect(source).toBe("peer");
		expect(meta.infoHash).toBe(torrent.infoHashHex);
		expect(fetcher.failures.external_mismatch).toBe(1);
		mgr.stop();
	}, 30_000);

	it("stays at one WebSocket while a peer delivers above the rate threshold", async () => {
		const torrent = makeTorrent({ seed: 104, size: 4 * 1024 * 1024, pieceLength: 64 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const good = await startFakePeer({ torrent, blockGapMs: 8 });
		const dead = await Promise.all(Array.from({ length: 20 }, () => startFakePeer({ torrent, silent: true })));
		peers.push(good, ...dead);
		const mgr = new ConnectionManager({ gatewayUrl: dev.url, infoHash: torrent.infoHash, allowPrivate: true, rateThreshold: 1, rateWindowMs: 1_000 });
		mgr.addPeers([good.addr], "discovery", [SEED]);
		const pending = mgr.download(meta);
		await until(() => mgr.downloadedBytes > 0);
		mgr.addPeers(
			dead.map((p) => p.addr),
			"discovery",
		);
		const result = await pending;
		expect(result.verified).toHaveLength(meta.pieceCount);
		expect(result.sockets).toBe(1);
		expect(mgr.upgrades).toBe(1);
	}, 60_000);

	it("replaces a connection with none of the wanted pieces before a choked one that has them", async () => {
		const torrent = makeTorrent({ seed: 105, size: 512 * 1024, pieceLength: 32 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const choked = await Promise.all(Array.from({ length: 5 }, () => startFakePeer({ torrent, unchoke: false })));
		const useless = await startFakePeer({ torrent, have: "none" });
		const good = await startFakePeer({ torrent });
		peers.push(...choked, useless, good);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			maxSockets: 1,
			chokeReplaceAfterMs: 60_000,
			stallTimeoutMs: 30_000,
		});
		mgr.addPeers([...choked.map((p) => p.addr), useless.addr], "discovery");
		const pending = mgr.download(meta);
		await until(() => mgr.liveCount === 6);
		mgr.addPeers([good.addr], "discovery");
		const result = await pending;
		expect(result.verified).toHaveLength(meta.pieceCount);
		expect(good.stats.served).toBeGreaterThan(0);
		expect(useless.stats.closed).toBeGreaterThan(0);
		expect(mgr.book.get(useless.addr)?.lastFailure).toBe("useless");
		// The choked peers with wanted pieces stayed until the download ended.
		for (const p of choked) expect(mgr.book.get(p.addr)?.lastFailure ?? null).toBeNull();
		expect(mgr.deliveringDropped).toBe(0);
	}, 60_000);

	it("stops dialling untried addresses once enough peers deliver above the rate threshold", async () => {
		const torrent = makeTorrent({ seed: 106, size: 8 * 1024 * 1024, pieceLength: 64 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		const good = await Promise.all([0, 1, 2].map(() => startFakePeer({ torrent, blockGapMs: 10 })));
		const dead = await Promise.all(Array.from({ length: 12 }, () => startFakePeer({ torrent, silent: true })));
		peers.push(...good, ...dead);
		const mgr = new ConnectionManager({
			gatewayUrl: dev.url,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			rateThreshold: 1,
			dialIntervalMs: 0,
			deliveringWindowMs: 3_000,
		});
		mgr.addPeers(
			good.map((p) => p.addr),
			"discovery",
			good.map(() => SEED),
		);
		const pending = mgr.download(meta);
		await until(() => mgr.diagnostics().delivering >= 3 && mgr.downloadedBytes < meta.totalLength / 2, 20_000);
		mgr.addPeers(
			dead.map((p) => p.addr),
			"discovery",
		);
		await new Promise((r) => setTimeout(r, 1_500));
		expect(mgr.diagnostics().dials.untried.attempted).toBe(0);
		expect(dead.every((p) => p.stats.connections === 0)).toBe(true);
		const result = await pending;
		expect(result.verified).toHaveLength(meta.pieceCount);
	}, 60_000);
});
