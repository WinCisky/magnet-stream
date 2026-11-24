// Hermetic end-to-end test of M4-02 (docs/tunnel-protocol.md › Announce
// and inbound connections): a seed behind "NAT" (a fake peer that only
// dials out, finding peers through a fake tracker), the VPS gateway
// (services/seeders) with an inbound address, and the engine. Nothing is
// announced and no port listens until the session asks; then the gateway
// announces to the tracker, the seed connects in, the engine gets `accept`
// and downloads the whole torrent from it, every piece verified. When the
// session ends the gateway announces `stopped` and closes the port.
//
//   npm run e2e:inbound -w tools/e2e

import net from "node:net";
import { AnnounceStatus, ConnectionManager, parseInfoDict } from "@magnet-stream/engine";
import { makeTorrent, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { freePort, startFakeTracker, startVpsGateway, type FakeTracker, type WranglerDev } from "@magnet-stream/harness";

const MiB = 1024 * 1024;
let failed = false;
let gateway: WranglerDev | undefined;
let tracker: FakeTracker | undefined;
let seed: FakePeer | undefined;

async function main(): Promise<void> {
	const torrent = makeTorrent({ seed: 202, size: 8 * MiB, pieceLength: 256 * 1024 });
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	tracker = await startFakeTracker();
	const inboundPort = await freePort();
	gateway = await startVpsGateway({
		vars: {
			ALLOW_PRIVATE_PEERS: "1",
			TUNNEL_INBOUND_ADDR: `127.0.0.1:${inboundPort}`,
			// Hermetic: no public DHT, no public trackers.
			TUNNEL_ANNOUNCE_DHT: "0",
			DEFAULT_TRACKERS: "",
		},
	});
	seed = await startFakePeer({ torrent, listen: false, tracker: tracker.url, announceIntervalMs: 300 });
	check(seed.addr === "", "the seed does not listen");

	const mgr = new ConnectionManager({ gatewayUrl: gateway.url, infoHash: torrent.infoHash, allowPrivate: true, stallTimeoutMs: 30_000 });
	await mgr.connect();
	await sleep(1_000);
	const fromGateway = () => tracker!.announces.filter((a) => a.port === inboundPort);
	check(fromGateway().length === 0, "nothing announced before the session asks");
	check(!(await listening(inboundPort)), "no inbound port before the session asks");

	const started = Date.now();
	const status = await mgr.announce([tracker.url]);
	check(status === AnnounceStatus.ACTIVE, `announce is active (got ${status})`);
	check(await listening(inboundPort), "the inbound port listens once announced");
	const result = await mgr.download(meta, {
		onPiece: (piece, data) => {
			const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
			if (!Buffer.from(data).equals(Buffer.from(expected))) throw new Error(`piece ${piece} differs from the source`);
		},
	});
	const seconds = (Date.now() - started) / 1000;
	const d = mgr.diagnostics();
	console.log(`verified ${result.verified.length}/${meta.pieceCount} pieces from an inbound peer in ${seconds.toFixed(2)} s (accepted ${d.accepted})`);
	check(result.verified.length === meta.pieceCount && result.corrupt.length === 0, "every piece verified");
	check(d.accepted >= 1, "the piece came through an accepted connection");
	const started_ = fromGateway().find((a) => a.event === "started");
	check(started_ !== undefined && started_.left > 0, "the gateway announced itself as a leecher");
	check(seed.stats.handshakes >= 1, "the seed connected in");

	mgr.stop();
	const until = Date.now() + 10_000;
	while (Date.now() < until && !fromGateway().some((a) => a.event === "stopped")) await sleep(100);
	check(
		fromGateway().some((a) => a.event === "stopped"),
		"the end of the session announces stopped",
	);
	let closed = false;
	for (let i = 0; i < 50 && !closed; i++) {
		closed = !(await listening(inboundPort));
		if (!closed) await sleep(100);
	}
	check(closed, "the inbound port closes with the session");
	const logs = gateway.output();
	check(!logs.toLowerCase().includes(torrent.infoHashHex), "info-hash absent from the gateway's logs");
	const tunnelLines = logs.split("\n").filter((l) => l.includes("tunnel."));
	check(tunnelLines.length > 0 && !tunnelLines.some((l) => l.includes("127.0.0.1")), "no peer address in the gateway's tunnel logs");
}

function listening(port: number): Promise<boolean> {
	return new Promise((resolve) => {
		const s = net.connect({ port, host: "127.0.0.1" });
		s.once("connect", () => {
			s.destroy();
			resolve(true);
		});
		s.once("error", () => resolve(false));
	});
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

function check(ok: boolean, what: string): void {
	if (!ok) {
		failed = true;
		console.error(`FAIL ${what}`);
	} else console.log(`ok   ${what}`);
}

main()
	.catch((e: unknown) => {
		failed = true;
		console.error(e);
		if (gateway) console.error(gateway.output().slice(-4000));
	})
	.finally(async () => {
		await seed?.close();
		await tracker?.close();
		await gateway?.stop();
		process.exit(failed ? 1 : 0);
	});
