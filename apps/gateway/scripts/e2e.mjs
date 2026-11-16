// End-to-end test of the legacy /v1/blocks route: runs the real worker
// under `wrangler dev` against fake seeds (@magnet-stream/fake-peer) on
// localhost and checks every scenario the browser relies on.
//
//   npm run e2e -w apps/gateway         (starts wrangler dev itself, with a random TOKEN_SECRET)
//   E2E_URL=http://127.0.0.1:8787 E2E_TOKEN_SECRET=... npm run e2e -w apps/gateway
//                                      (use a running dev server; secret defaults to .dev.vars)

import { createHash, randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { startWranglerDev } from "@magnet-stream/harness";
import { signToken } from "../src/token.ts";

let base = process.env.E2E_URL;
const SECRET = base
	? (process.env.E2E_TOKEN_SECRET ??
		/^TOKEN_SECRET=(.*)$/m.exec(readFileSync(new URL("../.dev.vars", import.meta.url), "utf8"))?.[1]?.trim())
	: randomBytes(32).toString("base64url");
if (!SECRET) throw new Error("set E2E_TOKEN_SECRET or put TOKEN_SECRET in .dev.vars (see .dev.vars.example)");

const torrent = makeTorrent({ seed: 1, size: 250_123, pieceLength: 40_000 });
const IH = torrent.infoHashHex;
const EXP = Math.floor(Date.now() / 1000) + 3600;

/** The PoC's startPeer(torrent, behaviour), on top of the shared fake peer. */
function startPeer(t, behaviour = {}) {
	return startFakePeer({ torrent: t, ...behaviour });
}

let wrangler;
let failures = 0;

async function main() {
	if (!base) {
		wrangler = await startWranglerDev({ vars: { TOKEN_SECRET: SECRET } });
		base = wrangler.url;
	}

	const seed = await startPeer(torrent);
	const slow = await startPeer(torrent, { unchokeDelayMs: 1500 });
	const choker = await startPeer(torrent, { chokeAfterBlocks: 5 });
	const wrongHash = await startPeer(torrent, { wrongInfoHash: true });
	const stubborn = await startPeer(torrent, { unchoke: false });
	const partial = await startPeer(torrent, { have: [0, 2], fast: false });
	const hangUp = await startPeer(torrent, { closeAfterHandshake: true });
	const closedPort = await startPeer(torrent);
	await closedPort.close();

	await test("downloads every piece intact from a seed", async () => {
		const res = await blocks([seed], allPieces());
		assertEq(res.status, 200, "status");
		const stream = await readStream(res, blockCount(allPieces()));
		assertEq(stream.preamble.peer, seed.addr, "winner");
		assertEq(stream.preamble.reqq, 500, "reqq from the extension handshake");
		assertEq(stream.preamble.sent, allPieces(), "sent");
		for (let p = 0; p < torrent.pieceCount; p++) {
			const hash = createHash("sha1").update(assemble(stream.blocks, p)).digest();
			assert(hash.equals(torrent.hashes[p]), `piece ${p} hash`);
		}
		// BEP 6: with fast on both sides, have-none must come first.
		assertEq(seed.stats.firstMessages.join(","), "15,2", "have-none then interested");
	});

	await test("races candidates and reports the losers", async () => {
		const res = await blocks([wrongHash, stubborn, seed], "0");
		assertEq(res.status, 200, "status");
		const stream = await readStream(res, blockCount("0"));
		assertEq(stream.preamble.peer, seed.addr, "winner");
		const others = Object.fromEntries(stream.preamble.others.map((o) => [o.peer, o.err]));
		assertEq(others[wrongHash.addr], "infohash_mismatch", "wrong hash");
		assertEq(others[stubborn.addr], "lost", "still choked when the race ended");
	});

	await test("waits for a slow unchoke within `wait`", async () => {
		const res = await blocks([slow], "1", { wait: "5000" });
		assertEq(res.status, 200, "status");
		const stream = await readStream(res, 3);
		assert(stream.preamble.ms.unchoke >= 1400, `unchoke took ${stream.preamble.ms.unchoke} ms`);
	});

	await test("gives up on a peer that doesn't unchoke in time", async () => {
		const res = await blocks([slow], "1", { wait: "1000" });
		assertEq(res.status, 502, "status");
		const body = await res.json();
		assertEq(body.others[0].err, "choked", "reason");
	});

	await test("streams what arrived before a mid-stream choke", async () => {
		const res = await blocks([choker], "0,1,2");
		assertEq(res.status, 200, "status");
		const stream = await readStream(res, 5, { stopOnChoke: true });
		assert(stream.choked, "saw the choke");
		assertEq(stream.blocks.size, 5, "blocks before the choke");
		for (const [key, data] of stream.blocks) {
			const [piece, begin] = key.split(":").map(Number);
			const start = piece * torrent.pieceLength + begin;
			assert(Buffer.from(data).equals(Buffer.from(torrent.data.subarray(start, start + data.length))), `block ${key}`);
		}
	});

	await test("only requests pieces the peer has", async () => {
		const res = await blocks([partial], "0,1,2");
		assertEq(res.status, 200, "status");
		const stream = await readStream(res, blockCount("0,2"));
		assertEq(stream.preamble.sent, "0,2", "sent");
		const none = await blocks([partial], "1");
		assertEq(none.status, 502, "status when it has none");
		assertEq((await none.json()).others[0].err, "no_pieces", "reason");
	});

	await test("reports peers that hang up or refuse", async () => {
		const res = await blocks([hangUp, closedPort], "0");
		assertEq(res.status, 502, "status");
		const others = Object.fromEntries((await res.json()).others.map((o) => [o.peer, o.err]));
		assertEq(others[hangUp.addr], "closed", "hang-up");
		assertEq(others[closedPort.addr], "connect_failed", "refused");
	});

	await test("works through a long list of mostly dead candidates", async () => {
		const dead = [];
		for (let i = 0; i < 12; i++) {
			const p = await startPeer(torrent);
			await p.close();
			dead.push(p);
		}
		const chokers = [];
		for (let i = 0; i < 6; i++) chokers.push(await startPeer(torrent, { unchoke: false }));
		// Six chokers first: they take every slot until `hold` evicts them.
		const started = Date.now();
		const res = await blocks([...chokers, ...dead, seed], "0", { wait: "8000", hold: "1000" });
		assertEq(res.status, 200, "status");
		const stream = await readStream(res, blockCount("0"));
		assertEq(stream.preamble.peer, seed.addr, "winner");
		assert(Date.now() - started < 5_000, `took ${Date.now() - started} ms`);
		const others = Object.fromEntries(stream.preamble.others.map((o) => [o.peer, o]));
		for (const p of dead) assertEq(others[p.addr].err, "connect_failed", "dead peer");
		assert(chokers.some((p) => others[p.addr].err === "evicted"), "a choker was evicted");
		for (const p of chokers) assertEq(others[p.addr].hs, 1, "choker handshook");
		for (const p of chokers) await p.close();
	});

	await test("describes itself", async () => {
		const info = await (await fetch(`${base}/v1/info`)).json();
		assertEq(info.maxCandidates, 40, "maxCandidates");
		assertEq(info.maxOpen, 6, "maxOpen");
	});

	await test("frees the peer connection after the browser aborts", async () => {
		const before = seed.stats.closed;
		const controller = new AbortController();
		const res = await blocks([seed], allPieces(), {}, { signal: controller.signal });
		const reader = res.body.getReader();
		await reader.read();
		controller.abort();
		// Promptly when the runtime notices the hang-up; at the latest when
		// the stream's size-based cap (20 s for this small request) expires.
		await waitFor(() => seed.stats.closed > before, 25_000, "peer socket closed");
	});

	await test("rejects bad, expired and foreign tokens", async () => {
		const bad = await fetch(url({ ih: IH, pl: `${torrent.pieceLength}`, len: `${torrent.totalLength}`, want: "0", c: `${seed.addr}|${EXP}|${"A".repeat(43)}` }));
		assertEq(bad.status, 403, "bad token");
		const past = Math.floor(Date.now() / 1000) - 10;
		const expired = await fetch(url({ ih: IH, pl: `${torrent.pieceLength}`, len: `${torrent.totalLength}`, want: "0", c: `${seed.addr}|${past}|${await signToken(SECRET, IH, seed.addr, past)}` }));
		assertEq(expired.status, 403, "expired token");
		const otherPeer = await fetch(url({ ih: IH, pl: `${torrent.pieceLength}`, len: `${torrent.totalLength}`, want: "0", c: `${seed.addr}|${EXP}|${await signToken(SECRET, IH, stubborn.addr, EXP)}` }));
		assertEq(otherPeer.status, 403, "token for another peer");
	});

	await test("validates input and origins", async () => {
		assertEq((await fetch(url({ ih: "nope" }))).status, 400, "bad params");
		const foreign = await fetch(`${base}/health`, { headers: { origin: "https://evil.example" } });
		assertEq(foreign.status, 403, "foreign origin");
		const allowed = await fetch(`${base}/health`, { headers: { origin: "http://localhost:4321" } });
		assertEq(allowed.headers.get("access-control-allow-origin"), "http://localhost:4321", "CORS");
	});

	await test("connects to IPv6 peers", async () => {
		let v6;
		try {
			v6 = await startPeer(torrent, { host: "::1" });
		} catch {
			console.log("    (skipped: no IPv6 loopback)");
			return;
		}
		try {
			const res = await blocks([v6], "0");
			assertEq(res.status, 200, "status");
			await readStream(res, blockCount("0"));
		} finally {
			await v6.close();
		}
	});

	for (const peer of [seed, slow, choker, wrongHash, stubborn, partial, hangUp]) await peer.close();
}

function blockCount(want) {
	return want
		.split(",")
		.map(Number)
		.reduce((n, p) => n + Math.ceil(Math.min(torrent.pieceLength, torrent.totalLength - p * torrent.pieceLength) / 16384), 0);
}

function allPieces() {
	return Array.from({ length: torrent.pieceCount }, (_, i) => i).join(",");
}

function url(params) {
	const u = new URL("/v1/blocks", base);
	for (const [k, v] of Object.entries(params)) for (const x of [v].flat()) u.searchParams.append(k, x);
	return u;
}

async function blocks(peers, want, extra = {}, init = {}) {
	const c = await Promise.all(peers.map(async (p) => `${p.addr}|${EXP}|${await signToken(SECRET, IH, p.addr, EXP)}`));
	const controller = new AbortController();
	const res = await fetch(url({ ih: IH, pl: `${torrent.pieceLength}`, len: `${torrent.totalLength}`, want, c, ...extra }), {
		signal: controller.signal,
		...init,
	});
	// Like the browser: once it has its blocks it aborts the request.
	res.abort = () => controller.abort();
	return res;
}

/** Minimal browser-side parser: preamble, peer handshake, then messages. */
async function readStream(res, expectedBlocks, { stopOnChoke = false } = {}) {
	const reader = res.body.getReader();
	let buf = Buffer.alloc(0);
	let preamble;
	let handshaken = false;
	let choked = false;
	const blocks = new Map();
	const deadline = Date.now() + 10_000;
	while (blocks.size < expectedBlocks && !(stopOnChoke && choked)) {
		const left = deadline - Date.now();
		const timeout = new Promise((_, reject) =>
			setTimeout(() => reject(new Error(`timed out with ${blocks.size}/${expectedBlocks} blocks`)), left),
		);
		const { value, done } = await Promise.race([reader.read(), timeout]);
		if (done) break;
		buf = Buffer.concat([buf, value]);
		for (;;) {
			if (!preamble) {
				if (buf.length < 4 || buf.length < 4 + buf.readUInt32BE(0)) break;
				const n = buf.readUInt32BE(0);
				preamble = JSON.parse(buf.subarray(4, 4 + n).toString());
				buf = buf.subarray(4 + n);
			} else if (!handshaken) {
				if (buf.length < 68) break;
				handshaken = true;
				buf = buf.subarray(68);
			} else {
				if (buf.length < 4 || buf.length < 4 + buf.readUInt32BE(0)) break;
				const len = buf.readUInt32BE(0);
				const id = buf[4];
				if (len > 0 && id === 7) {
					const piece = buf.readUInt32BE(5);
					const begin = buf.readUInt32BE(9);
					blocks.set(`${piece}:${begin}`, new Uint8Array(buf.subarray(13, 4 + len)));
				} else if (len > 0 && id === 0) {
					choked = true;
				}
				buf = buf.subarray(4 + len);
			}
		}
	}
	res.abort?.();
	return { preamble, blocks, choked };
}

function assemble(blocks, piece) {
	const size = Math.min(torrent.pieceLength, torrent.totalLength - piece * torrent.pieceLength);
	const out = new Uint8Array(size);
	for (let begin = 0; begin < size; begin += 16384) {
		const block = blocks.get(`${piece}:${begin}`);
		if (!block) throw new Error(`missing block ${piece}:${begin}`);
		out.set(block, begin);
	}
	return out;
}

async function test(name, fn) {
	try {
		await fn();
		console.log(`  ok   ${name}`);
	} catch (e) {
		failures++;
		console.log(`  FAIL ${name}\n       ${e.message}`);
	}
}

function assert(cond, what) {
	if (!cond) throw new Error(what);
}

function assertEq(actual, expected, what) {
	if (actual !== expected) throw new Error(`${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

async function waitFor(cond, ms, what) {
	const until = Date.now() + ms;
	while (!cond()) {
		if (Date.now() > until) throw new Error(`timed out waiting for: ${what}`);
		await new Promise((r) => setTimeout(r, 50));
	}
}

main()
	.catch((e) => {
		failures++;
		console.error(e);
	})
	.finally(async () => {
		await wrangler?.stop();
		console.log(failures ? `\n${failures} failed` : "\nall passed");
		process.exit(failures ? 1 : 0);
	});
