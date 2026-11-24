// Hermetic end-to-end test of a swarm with a peer that corrupts (M3-02,
// docs/architecture.md §Chunk recovery › Verification): two honest fake
// peers and one that serves corrupted blocks, the gateway under `wrangler
// dev`, and the engine's connection manager downloading the whole torrent.
// Every piece handed over must be the source's bytes; the corrupting peer
// is banned after two failed pieces and never dialled again (even when
// named again); the honest peers, which shared mixed pieces with it, keep
// their standing. Run once for a v1 torrent and once for a v2 one.
//
//   npm run e2e:corrupt -w tools/e2e
//   E2E_GATEWAY_URL=http://127.0.0.1:8787 npm run e2e:corrupt -w tools/e2e   (a running wrangler dev with ALLOW_PRIVATE_PEERS=1)
//   E2E_GATEWAY=vps npm run e2e:corrupt -w tools/e2e   (through the VPS gateway, services/seeders, instead of wrangler dev)

import { ConnectionManager, parseInfoDict } from "@magnet-stream/engine";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent } from "@magnet-stream/fake-peer";
import { gatewayKind, startGateway, type WranglerDev } from "@magnet-stream/harness";

const MiB = 1024 * 1024;

let dev: WranglerDev | undefined;
const peers: FakePeer[] = [];
let failed = false;

async function scenario(base: string, label: string, torrent: FakeTorrent): Promise<void> {
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	// A shallow queue spreads each piece over several peers: mixed pieces.
	const [honestA, honestB, corrupt] = await Promise.all([
		startFakePeer({ torrent, reqq: 8 }),
		startFakePeer({ torrent, reqq: 8 }),
		startFakePeer({ torrent, reqq: 8, corrupt: true }),
	]);
	peers.push(honestA, honestB, corrupt);
	const mgr = new ConnectionManager({ gatewayUrl: base, infoHash: torrent.infoHash, allowPrivate: true });
	mgr.addPeers([corrupt.addr, honestA.addr, honestB.addr]);
	let mismatched = 0;
	let delivered = 0;
	const started = Date.now();
	let renamed = false;
	const result = await mgr.download(meta, {
		onPiece: (piece, data) => {
			const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
			if (!Buffer.from(data).equals(Buffer.from(expected))) mismatched++;
			delivered++;
			// Once banned, a peer naming it again (as pex would) must not bring it back.
			if (!renamed && mgr.book.isBanned(corrupt.addr)) {
				renamed = true;
				mgr.addPeers([corrupt.addr], "pex");
			}
		},
	});
	const seconds = (Date.now() - started) / 1000;
	const counts = mgr.attributionCounts;
	console.log(
		`${label}: verified ${result.verified.length}/${meta.pieceCount} pieces in ${seconds.toFixed(2)} s; ` +
			`${result.hashFailures} failed checks (sole ${counts.sole}, mixed ${counts.mixed}; resolved by diff ${counts.resolvedByDiff}, by leaf hashes ${counts.resolvedByLeaves}); ${result.banned} banned`,
	);
	check(result.verified.length === meta.pieceCount && result.corrupt.length === 0 && result.missing.length === 0, `${label}: every piece verified`);
	check(delivered === meta.pieceCount && mismatched === 0, `${label}: only verified pieces handed over`);
	check(result.hashFailures >= 1, `${label}: the corrupting peer caused failed checks`);
	check(mgr.book.isBanned(corrupt.addr) && mgr.strikesOf(corrupt.addr) === 2 && result.banned === 1, `${label}: the corrupting peer is banned after two failed pieces`);
	check(corrupt.stats.connections === 1, `${label}: the banned peer is not contacted again (${corrupt.stats.connections} connections)`);
	check(!mgr.book.isBanned(honestA.addr) && !mgr.book.isBanned(honestB.addr), `${label}: honest peers are not banned`);
	check(mgr.strikesOf(honestA.addr) + mgr.strikesOf(honestB.addr) === 0, `${label}: honest peers of mixed pieces are not struck`);
	if (label === "v2") check(counts.resolvedByLeaves > 0, "v2: a failed piece's culprit named by its leaf hashes");
}

async function main(): Promise<void> {
	const base = process.env.E2E_GATEWAY_URL ?? (dev = await startGateway({ vars: { ALLOW_PRIVATE_PEERS: "1" } })).url;
	const v1 = makeTorrent({ seed: 301, size: 4 * MiB, pieceLength: 256 * 1024 });
	await scenario(base, "v1", v1);
	const v2 = makeTorrent({ seed: 302, files: [{ path: ["corrupt.mkv"], length: 4 * MiB }], pieceLength: 256 * 1024, version: 2 });
	await scenario(base, "v2", v2);
	if (dev) {
		const logs = dev.output();
		check(![v1, v2].some((t) => logs.toLowerCase().includes(t.infoHashHex)), `info-hash absent from ${gatewayKind()} gateway logs`);
		check(!peers.some((p) => logs.includes(p.addr)), `peer addresses absent from ${gatewayKind()} gateway logs`);
	}
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
		await dev?.stop();
		process.exit(failed ? 1 : 0);
	});
