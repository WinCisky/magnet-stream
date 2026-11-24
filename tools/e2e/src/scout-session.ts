// Hermetic end-to-end test of scout-guided dialling (docs/architecture.md
// §Session start steps 2–4, §Connection management; OQ-21, OQ-23): a
// `/peers` stub streams 30 dead addresses (silent peers: the gateway waits
// for a handshake that never comes, holding the slot), then 2 live seeds
// with the scout's facts, then the info dict. The engine must connect the
// live peers within 3 s of their lines, take the file list from the stream
// (checked against the info-hash), and download the torrent through
// `wrangler dev`. One live seed also names, by peer exchange, a third live
// seed and 10 dead addresses that discovery never listed: they are dialled
// as untried, only on sockets that carry no data. The report prints the dials by class and outcome and the upstream
// messages.
//
//   npm run e2e:scout -w tools/e2e
//   E2E_GATEWAY_URL=http://127.0.0.1:8787 npm run e2e:scout -w tools/e2e   (a running wrangler dev with ALLOW_PRIVATE_PEERS=1)
//   E2E_GATEWAY=vps npm run e2e:scout -w tools/e2e   (through the VPS gateway, services/seeders, instead of wrangler dev)

import { ConnectionManager, MetadataFetcher, parseMagnet, readPeerStream } from "@magnet-stream/engine";
import { makeTorrent, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { gatewayKind, startGateway, startPeersStub, type PeersStub, type StubLine, type WranglerDev } from "@magnet-stream/harness";

const MiB = 1024 * 1024;
const DEAD = 30;
const LIVE_WITHIN_MS = 3_000;
const MESSAGES_PER_REQUEST = 20;

let dev: WranglerDev | undefined;
let stub: PeersStub | undefined;
const peers: FakePeer[] = [];
let failed = false;

async function main(): Promise<void> {
	const torrent = makeTorrent({ seed: 43, name: "scout.mkv", size: 16 * MiB, pieceLength: 256 * 1024 });
	const dead = await Promise.all(Array.from({ length: DEAD }, () => startFakePeer({ torrent, silent: true })));
	// Only reachable by peer exchange, never in the /peers stream.
	const pexLive = await startFakePeer({ torrent, blockGapMs: 20 });
	const pexDead = await Promise.all(Array.from({ length: 10 }, () => startFakePeer({ torrent, silent: true })));
	// Slow enough (~10 s) for the peers learned by pex to be tried during the download.
	const live = await Promise.all([
		startFakePeer({ torrent, blockGapMs: 20, pex: [pexLive.addr, ...pexDead.map((p) => p.addr)] }),
		startFakePeer({ torrent, blockGapMs: 20 }),
	]);
	peers.push(...dead, ...live, pexLive, ...pexDead);
	const lines: StubLine[] = [
		// Unknown to the scout (half without facts, as a plain list would be).
		...dead.map((p, i): StubLine => (i % 2 === 0 ? { peer: p.addr, score: 0.2 } : p.addr)),
		{ pauseMs: 1_500 },
		...live.map((p): StubLine => ({ peer: p.addr, score: 0.93, alive: true, seed: true, have: 1, md: true, unchoked: true, rtt: 3 })),
		{ info: torrent.info },
	];
	stub = await startPeersStub(lines);
	const base = process.env.E2E_GATEWAY_URL ?? (dev = await startGateway({ vars: { ALLOW_PRIVATE_PEERS: "1" } })).url;

	const magnet = parseMagnet(live[0].magnet);
	const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, v2: magnet.v2, wire: magnet.infoHashHex } });
	const mgr = new ConnectionManager({
		gatewayUrl: base,
		infoHash: magnet.infoHash,
		allowPrivate: true,
		onExtensions: (conn) => fetcher.offer(conn),
		onMetadata: (conn, m) => fetcher.onMessage(conn, m),
		onPeerClose: (conn) => fetcher.onClose(conn),
	});
	const started = Date.now();
	const liveAddrs = new Set(live.map((p) => p.addr));
	const arrived = new Map<string, number>();
	const connectedAt = new Map<string, number>();
	const watch = setInterval(() => {
		for (const a of liveAddrs) if (!connectedAt.has(a) && mgr.book.get(a)?.state === "connected") connectedAt.set(a, Date.now());
	}, 10);
	const url = new URL("/peers", stub.url);
	url.searchParams.set("ih", magnet.infoHashHex);
	const discovery = readPeerStream(
		url.toString(),
		(address, scout) => {
			if (liveAddrs.has(address)) arrived.set(address, Date.now());
			mgr.addPeers([address], "discovery", [scout]);
		},
		{ onInfo: (bytes) => void fetcher.offerBytes(bytes) },
	);
	await mgr.connect();
	const { meta, source } = await fetcher.done;
	const metadataMs = Date.now() - started;
	const result = await mgr.download(meta, {
		onPiece: (piece, data) => {
			const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
			if (!Buffer.from(data).equals(Buffer.from(expected))) throw new Error(`piece ${piece} differs from the source`);
		},
	});
	clearInterval(watch);
	const seconds = (Date.now() - started) / 1000;
	const addresses = await discovery;
	const d = mgr.diagnostics();
	const delays = [...liveAddrs].map((a) => (connectedAt.has(a) && arrived.has(a) ? connectedAt.get(a)! - arrived.get(a)! : Infinity));

	console.log(`discovery: ${addresses} addresses (${DEAD} dead, ${live.length} live), metadata from ${source} after ${metadataMs} ms`);
	const pexDialled = pexDead.filter((p) => p.stats.connections > 0).length;
	console.log(
		`peer exchange: 11 addresses, the live one ${pexLive.stats.served > 0 ? "delivered" : pexLive.stats.connections > 0 ? "connected" : "not reached"}, ${pexDialled} dead ones dialled; ` +
			`exploring sockets recycled ${d.recycles}`,
	);
	console.log(`live peers connected ${delays.map((ms) => `${ms} ms`).join(" / ")} after their lines`);
	for (const [kind, c] of Object.entries(d.dials)) {
		const outcomes = Object.entries(c.failed)
			.map(([why, n]) => `${why} ${n}`)
			.join(", ");
		console.log(`dials ${kind}: ${c.attempted} attempted, ${c.succeeded} succeeded${outcomes ? `, failed: ${outcomes}` : ""}`);
	}
	console.log(
		`verified ${result.verified.length}/${meta.pieceCount} pieces (${(meta.totalLength / MiB).toFixed(1)} MiB in ${seconds.toFixed(2)} s); ` +
			`websocket: ${mgr.upgrades} upgrades, ${result.messagesSent} upstream messages (billed as ~${(mgr.upgrades + result.messagesSent / MESSAGES_PER_REQUEST).toFixed(1)} requests); ` +
			`first unchoke after ${d.firstUnchokeMs ?? "-"} ms; untried in flight per socket at most ${mgr.untestedHighWater}`,
	);

	check(source === "external", "the file list comes from the stream's info line");
	check(
		delays.every((ms) => ms < LIVE_WITHIN_MS),
		`both live peers connected within ${LIVE_WITHIN_MS} ms of their lines`,
	);
	check(mgr.untestedHighWater <= 3, "at most 3 untried dials in flight per socket");
	check(d.dials.scout.attempted === live.length && d.dials.scout.succeeded === live.length, "each live address dialled once, successfully");
	check(result.verified.length === meta.pieceCount && result.corrupt.length === 0 && result.missing.length === 0, "every piece verified");
	check(stub.requests() === 1, "one discovery request");
	if (dev) {
		const logs = dev.output();
		check(!logs.toLowerCase().includes(torrent.infoHashHex), `info-hash absent from ${gatewayKind()} gateway logs`);
		check(![...live, ...dead].some((p) => logs.includes(p.addr)), `peer addresses absent from ${gatewayKind()} gateway logs`);
	}
	fetcher.stop();
}

function check(ok: boolean, what: string): void {
	if (!ok) {
		failed = true;
		console.error(`FAIL ${what}`);
	}
}

main()
	.catch((e: unknown) => {
		failed = true;
		console.error(e);
		if (dev) console.error(dev.output().slice(-4000));
	})
	.finally(async () => {
		await Promise.all(peers.map((p) => p.close()));
		await stub?.close();
		await dev?.stop();
		process.exit(failed ? 1 : 0);
	});
