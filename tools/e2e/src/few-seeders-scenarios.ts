// The few-seeders path end to end (M4-08, docs/architecture.md §Chunk
// recovery › Swarms with few seeders), on the full local stack: the
// Cloudflare gateway (wrangler dev, primary), the VPS gateway (escalation,
// announce, inbound peers), a fake tracker and fake peers, all on loopback
// (nothing reaches public trackers or the DHT: tools/harness
// HERMETIC_VPS_VARS). The engine is the one the page runs, in Node.
//
//   1. nat-seed: the only seed sits behind NAT (it only dials out, to the
//      addresses the tracker names). Nobody delivers, the session escalates,
//      the VPS gateway announces itself, the seed connects in: every byte
//      comes through the VPS.
//   2. late-unchoke: one reachable seed unchokes only after 20 s. The
//      session stays connected and interested (one connection, never
//      dropped) and every byte comes through Cloudflare.
//   3. partial-mix: no seed; two reachable leechers hold the first three
//      quarters between them, one leecher behind NAT the second half.
//      Together they cover the file: the session takes what Cloudflare
//      reaches, escalates when that runs dry, and the rest comes in through
//      the VPS.
//   4. vps-crash: a reachable leecher with the first quarter and a NATed
//      seed. Midway through the VPS gateway is killed (SIGKILL) and comes
//      back a second later: the session reopens its WebSocket, announces
//      again, the seed connects in again and the file completes.
//
// Every scenario checks that each piece verified and matches the source, and
// that the diagnostics report the bytes of each path (Cloudflare / VPS).
// Scenarios 1–3 run together, then 4 (it kills the shared VPS gateway).
// Declared time: the four scenarios (once the stack is up) must end within
// FEW_SEEDERS_BUDGET_S (default 120 s), or the run fails.
//
//   npm run e2e:few-seeders -w tools/e2e

import { ConnectionManager, parseInfoDict, type ConnectionDiagnostics, type Metainfo } from "@magnet-stream/engine";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent, type PeerBehaviour } from "@magnet-stream/fake-peer";
import { startFewSeedersStack, type FewSeedersStack } from "@magnet-stream/harness";

const MiB = 1024 * 1024;
const PIECE = 128 * 1024;
/** Few peers this long in a row: escalate (the page's default is 90 s). */
const ESCALATE_AFTER_MS = 2_000;
const UNCHOKE_DELAY_MS = 20_000;
const BUDGET_S = Number(process.env.FEW_SEEDERS_BUDGET_S ?? 120);

let failed = false;
let stack: FewSeedersStack | undefined;
const peers: FakePeer[] = [];
const sessions: ConnectionManager[] = [];
const infoHashes: string[] = [];

interface Outcome {
	name: string;
	ms: number;
	diag: ConnectionDiagnostics;
	downloaded: number;
	total: number;
}

const range = (from: number, to: number) => Array.from({ length: Math.max(0, to - from) }, (_, i) => from + i);

async function peer(torrent: FakeTorrent, b: PeerBehaviour, nat = false): Promise<FakePeer> {
	const p = await startFakePeer({ torrent, ...b, ...(nat ? { listen: false, tracker: stack!.tracker.url, announceIntervalMs: 300 } : {}) });
	peers.push(p);
	return p;
}

/** One session: dial `addresses` through Cloudflare, escalate to the VPS gateway when few peers deliver. */
async function run(name: string, torrent: FakeTorrent, addresses: string[], during?: (mgr: ConnectionManager, meta: Metainfo) => Promise<void>): Promise<Outcome> {
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const mgr = new ConnectionManager({
		gatewayUrl: stack!.primary.url,
		infoHash: torrent.infoHash,
		allowPrivate: true,
		stallTimeoutMs: 60_000,
		escalation: { gatewayUrl: stack!.vps.url, trackers: [stack!.tracker.url], escalateAfterMs: ESCALATE_AFTER_MS, warnAfterMs: 1_000 },
	});
	sessions.push(mgr);
	infoHashes.push(torrent.infoHashHex);
	mgr.addPeers(addresses);
	const started = Date.now();
	let mismatched = 0;
	const download = mgr.download(meta, {
		onPiece: (piece, data) => {
			const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
			if (!Buffer.from(data).equals(Buffer.from(expected))) mismatched++;
		},
	});
	const side = during?.(mgr, meta);
	const result = await download;
	await side;
	const ms = Date.now() - started;
	const diag = mgr.diagnostics();
	mgr.stop();
	check(result.verified.length === meta.pieceCount && result.corrupt.length === 0 && result.missing.length === 0, `${name}: every piece verified (${result.verified.length}/${meta.pieceCount})`);
	check(mismatched === 0, `${name}: every verified piece matches the source`);
	const { cloudflare, vps } = diag.paths;
	check(cloudflare.downloaded + vps.downloaded === mgr.downloadedBytes, `${name}: the paths add up to the bytes received`);
	console.log(
		`${name}: complete in ${(ms / 1000).toFixed(1)} s; received ${mb(cloudflare.downloaded)} MiB through Cloudflare (${cloudflare.connections} connection(s)), ` +
			`${mb(vps.downloaded)} MiB through the VPS (${vps.connections}); escalation ${diag.escalation.state}` +
			(diag.escalation.reason ? ` (${diag.escalation.reason} after ${((diag.escalation.afterMs ?? 0) / 1000).toFixed(1)} s, ${diag.escalation.upgrades} WebSocket(s))` : ""),
	);
	return { name, ms, diag, downloaded: mgr.downloadedBytes, total: meta.totalLength };
}

/** 1. The only seed is behind NAT: everything through the VPS. */
async function natSeed(): Promise<void> {
	const torrent = makeTorrent({ seed: 801, size: 4 * MiB, pieceLength: PIECE });
	const seed = await peer(torrent, {}, true);
	const o = await run("nat-seed", torrent, []);
	const { cloudflare, vps } = o.diag.paths;
	check(o.diag.escalation.reason === "few_peers" && o.diag.escalation.state === "joined", `nat-seed: escalated for few peers and joined (${o.diag.escalation.state})`);
	check(o.diag.accepted >= 1 && seed.stats.handshakes >= 1, `nat-seed: the seed connected in (${o.diag.accepted} accepted)`);
	check(cloudflare.downloaded === 0 && vps.downloaded >= o.total, `nat-seed: every byte through the VPS (${cloudflare.downloaded} / ${vps.downloaded})`);
	const announces = stack!.vpsAnnounces(torrent.infoHashHex);
	check(announces.some((a) => a.event === "started"), "nat-seed: the VPS gateway announced the torrent to the magnet's tracker");
}

/** 2. A reachable seed late to unchoke: waited for, everything through Cloudflare. */
async function lateUnchoke(): Promise<void> {
	const torrent = makeTorrent({ seed: 802, size: 2 * MiB, pieceLength: PIECE });
	const seed = await peer(torrent, { unchokeDelayMs: UNCHOKE_DELAY_MS });
	const o = await run("late-unchoke", torrent, [seed.addr]);
	const { cloudflare, vps } = o.diag.paths;
	check(o.ms >= UNCHOKE_DELAY_MS - 1_000, `late-unchoke: waited for the unchoke (complete in ${o.ms} ms)`);
	check(seed.stats.connections === 1, `late-unchoke: one connection to the seed, never dropped nor redialled (${seed.stats.connections})`);
	check(cloudflare.downloaded >= o.total && cloudflare.connections === 1 && vps.downloaded === 0, `late-unchoke: every byte through Cloudflare (${cloudflare.downloaded} / ${vps.downloaded})`);
}

/** 3. Partial leechers, two reachable and one behind NAT, that together cover the file. */
async function partialMix(): Promise<void> {
	const torrent = makeTorrent({ seed: 803, size: 4 * MiB, pieceLength: PIECE });
	const n = torrent.pieceCount;
	const a = await peer(torrent, { have: range(0, n / 2) });
	const b = await peer(torrent, { have: range(n / 4, (3 * n) / 4) });
	const c = await peer(torrent, { have: range(n / 2, n) }, true);
	const o = await run("partial-mix", torrent, [a.addr, b.addr]);
	const { cloudflare, vps } = o.diag.paths;
	const onlyNat = (n / 4) * PIECE;
	check(a.stats.served > 0 && b.stats.served > 0 && c.stats.served > 0, `partial-mix: all three leechers served (${a.stats.served}, ${b.stats.served}, ${c.stats.served} blocks)`);
	check(cloudflare.downloaded >= (3 * o.total) / 4 - PIECE * 2 && vps.downloaded >= onlyNat, `partial-mix: both paths carried their share (${cloudflare.downloaded} / ${vps.downloaded})`);
	check(o.diag.escalation.state === "joined" && o.diag.accepted >= 1, `partial-mix: escalated and joined (${o.diag.escalation.state}, ${o.diag.accepted} accepted)`);
}

/** 4. The VPS gateway dies mid-download and comes back. */
async function vpsCrash(): Promise<void> {
	const torrent = makeTorrent({ seed: 804, size: 4 * MiB, pieceLength: PIECE });
	const n = torrent.pieceCount;
	const a = await peer(torrent, { have: range(0, n / 4), blockGapMs: 20 });
	const seed = await peer(torrent, { blockGapMs: 40 }, true);
	let crashedAt = 0;
	const o = await run("vps-crash", torrent, [a.addr], async (mgr, meta) => {
		const until = Date.now() + 60_000;
		while (Date.now() < until && mgr.diagnostics().paths.vps.downloaded < meta.totalLength / 4) await sleep(50);
		const before = mgr.diagnostics();
		check(before.paths.vps.downloaded >= meta.totalLength / 4 && before.paths.vps.downloaded < meta.totalLength / 2, `vps-crash: crashed midway (${before.paths.vps.downloaded} of ${meta.totalLength} through the VPS)`);
		crashedAt = Date.now();
		await stack!.vps.crash();
		await sleep(1_000);
		await stack!.vps.restart();
	});
	const { cloudflare, vps } = o.diag.paths;
	check(crashedAt > 0, "vps-crash: the VPS gateway was killed");
	check(o.diag.escalation.upgrades >= 2 && o.diag.escalation.state === "joined", `vps-crash: the session reopened the VPS WebSocket (${o.diag.escalation.upgrades}, ${o.diag.escalation.state})`);
	check(o.diag.accepted >= 2 && seed.stats.handshakes >= 2, `vps-crash: the seed came in again (${o.diag.accepted} accepted, ${seed.stats.handshakes} handshakes)`);
	check(stack!.vpsAnnounces(torrent.infoHashHex).filter((x) => x.event === "started" && x.at >= crashedAt).length >= 1, "vps-crash: announced again after the restart");
	check(cloudflare.downloaded > 0 && vps.downloaded >= o.total / 2, `vps-crash: both paths carried bytes (${cloudflare.downloaded} / ${vps.downloaded})`);
	console.log(`vps-crash: complete ${((Date.now() - crashedAt) / 1000).toFixed(1)} s after the crash`);
}

async function main(): Promise<void> {
	const started = Date.now();
	stack = await startFewSeedersStack({ trackerInterval: 1 });
	const upAt = Date.now();
	await Promise.all([natSeed(), lateUnchoke(), partialMix()]);
	await vpsCrash();
	const logs = stack.vps.output().toLowerCase();
	check(infoHashes.length === 4 && !infoHashes.some((ih) => logs.includes(ih)), "no info-hash in the VPS gateway's logs");
	const tunnelLines = stack.vps.output().split("\n").filter((l) => l.includes("tunnel."));
	check(!tunnelLines.some((l) => /127\.0\.0\.1:\d+/.test(l)), "no peer address in the VPS gateway's tunnel logs");
	const seconds = (Date.now() - upAt) / 1000;
	console.log(`few-seeders: stack up in ${((upAt - started) / 1000).toFixed(1)} s, 4 scenarios in ${((Date.now() - upAt) / 1000).toFixed(1)} s (budget ${BUDGET_S} s)`);
	check(seconds <= BUDGET_S, `few-seeders: ${seconds.toFixed(1)} s, within the declared ${BUDGET_S} s`);
}

function mb(bytes: number): string {
	return (bytes / MiB).toFixed(2);
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
		if (stack) console.error(stack.vps.output().slice(-4000));
	})
	.finally(async () => {
		for (const s of sessions) s.stop();
		await Promise.all(peers.map((p) => p.close()));
		await stack?.close();
		process.exit(failed ? 1 : 0);
	});
