// wrangler dev + fake peers: open/data/close, failed (refused, timeout,
// not BitTorrent, info-hash mismatch, forbidden address), the 6-connection
// limit, Origin and subprotocol checks.

import http from "node:http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type PeerBehaviour } from "@magnet-stream/fake-peer";
import { closedPort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { CloseReason, FailReason, SUBPROTOCOL, type Frame, type OpenFrame } from "@magnet-stream/tunnel-protocol";
import { openTunnel } from "./tunnel-client";

const torrent = makeTorrent({ seed: 11, size: 2 * 1024 * 1024, pieceLength: 256 * 1024 });
const PEER_ID = new TextEncoder().encode("-MS0001-integration0");
const RESERVED = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);

let dev: WranglerDev; // ALLOW_PRIVATE_PEERS=1
let strict: WranglerDev; // ALLOW_PRIVATE_PEERS=0
const peers: FakePeer[] = [];

async function peer(b: PeerBehaviour = {}): Promise<FakePeer> {
	const p = await startFakePeer({ torrent, ...b });
	peers.push(p);
	return p;
}

function open(conn: number, port: number, infoHash = torrent.infoHash): OpenFrame {
	return { type: "open", conn, infoHash, reserved: RESERVED, peerId: PEER_ID, address: { family: 4, ip: Uint8Array.of(127, 0, 0, 1), port } };
}

function message(id: number, payload: number[] = []): Uint8Array {
	const len = payload.length + 1;
	return Uint8Array.of(len >>> 24, (len >> 16) & 0xff, (len >> 8) & 0xff, len & 0xff, id, ...payload);
}

const u32 = (n: number) => [n >>> 24, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff];

/** Peer-wire messages (id, payload) in the concatenated data frames of a connection. */
function wireMessages(frames: Frame[], conn: number): { id: number; payload: Uint8Array }[] {
	const parts = frames.filter((f) => f.type === "data" && f.conn === conn).map((f) => (f as { payload: Uint8Array }).payload);
	const buf = Buffer.concat(parts);
	const out = [];
	let at = 0;
	while (buf.length - at >= 4 && buf.length - at >= 4 + buf.readUInt32BE(at)) {
		const len = buf.readUInt32BE(at);
		if (len > 0) out.push({ id: buf[at + 4], payload: new Uint8Array(buf.subarray(at + 5, at + 4 + len)) });
		at += 4 + len;
	}
	return out;
}

const vars = { ALLOWED_ORIGINS: "http://localhost:4321", TUNNEL_HANDSHAKE_TIMEOUT_MS: "1500", TUNNEL_CONNECT_TIMEOUT_MS: "1500" };

beforeAll(async () => {
	const started = await Promise.allSettled([
		startWranglerDev({ vars: { ...vars, ALLOW_PRIVATE_PEERS: "1" } }),
		startWranglerDev({ vars: { ...vars, ALLOW_PRIVATE_PEERS: "0" } }),
	]);
	// Keep whichever came up, so afterAll stops it even if the other failed.
	if (started[0].status === "fulfilled") dev = started[0].value;
	if (started[1].status === "fulfilled") strict = started[1].value;
	for (const s of started) if (s.status === "rejected") throw s.reason;
});

afterAll(async () => {
	await Promise.all(peers.map((p) => p.close()));
	await Promise.all([dev?.stop(), strict?.stop()]);
});

describe("tunnel through wrangler dev", () => {
	it("opens a tunnel and exchanges peer-wire bytes both ways", async () => {
		const seed = await peer();
		const t = await openTunnel(dev.url);
		t.send(open(1, seed.port));
		const opened = await t.waitFor((f) => f.type === "opened");
		expect(Buffer.from((opened as { peerId: Uint8Array }).peerId).subarray(0, 8).toString()).toBe("-FP0001-");
		// The fake peer's extension handshake and have-all arrive as data.
		await t.waitFor(() => wireMessages(t.frames, 1).some((m) => m.id === 0x0e));
		// have-none + interested, then a request once unchoked.
		t.send({ type: "data", conn: 1, payload: Buffer.concat([message(0x0f), message(2)]) });
		await t.waitFor(() => wireMessages(t.frames, 1).some((m) => m.id === 1));
		t.send({ type: "data", conn: 1, payload: message(6, [...u32(1), ...u32(16384), ...u32(16384)]) });
		await t.waitFor(() => wireMessages(t.frames, 1).some((m) => m.id === 7));
		const piece = wireMessages(t.frames, 1).find((m) => m.id === 7)!.payload;
		const start = 1 * torrent.pieceLength + 16384;
		expect(Buffer.from(piece.subarray(8)).equals(Buffer.from(torrent.data.subarray(start, start + 16384)))).toBe(true);
		expect(seed.stats.firstMessages.slice(-2)).toEqual([0x0f, 2]);
		// Our close reaches the peer.
		const closedBefore = seed.stats.closed;
		t.send({ type: "close", conn: 1, reason: CloseReason.NORMAL });
		await waitUntil(() => seed.stats.closed > closedBefore);
		t.close();
	});

	it("reports a peer that hangs up", async () => {
		const p = await peer({ closeAfterBlocks: 1 });
		const t = await openTunnel(dev.url);
		t.send(open(3, p.port));
		await t.waitFor((f) => f.type === "opened");
		t.send({ type: "data", conn: 3, payload: Buffer.concat([message(0x0f), message(2)]) });
		await t.waitFor(() => wireMessages(t.frames, 3).some((m) => m.id === 1));
		t.send({ type: "data", conn: 3, payload: message(6, [...u32(0), ...u32(0), ...u32(16384)]) });
		const close = await t.waitFor((f) => f.type === "close");
		expect(close).toEqual({ type: "close", conn: 3, reason: CloseReason.REMOTE_CLOSED });
		t.close();
	});

	it("fails refused, timeout, not BitTorrent and info-hash mismatch without relaying anything", async () => {
		const refused = await closedPort();
		const silent = await peer({ silent: true });
		const http11 = await peer({ respondWith: Buffer.from("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".padEnd(80, " ")) });
		const wrong = await peer({ wrongInfoHash: true });
		const t = await openTunnel(dev.url);
		t.send(open(1, refused), open(2, silent.port), open(3, http11.port), open(4, wrong.port));
		for (const conn of [1, 2, 3, 4]) await t.waitFor((f) => f.type === "failed" && f.conn === conn);
		const reasons = Object.fromEntries(t.frames.map((f) => [f.conn, (f as { reason?: number }).reason]));
		expect(reasons).toEqual({
			1: FailReason.REFUSED,
			2: FailReason.TIMEOUT,
			3: FailReason.NOT_BITTORRENT,
			4: FailReason.INFOHASH_MISMATCH,
		});
		expect(t.frames.every((f) => f.type === "failed")).toBe(true);
		t.close();
	});

	it("refuses the 7th connection on a socket", async () => {
		const seed = await peer();
		const t = await openTunnel(dev.url);
		t.send(...[1, 2, 3, 4, 5, 6].map((c) => open(c, seed.port)));
		for (const conn of [1, 2, 3, 4, 5, 6]) await t.waitFor((f) => f.type === "opened" && f.conn === conn);
		t.send(open(7, seed.port));
		expect(await t.waitFor((f) => f.conn === 7)).toEqual({ type: "failed", conn: 7, reason: FailReason.TOO_MANY_CONNECTIONS });
		t.close();
	});

	it("closes the session on a malformed frame", async () => {
		const t = await openTunnel(dev.url);
		t.send({ type: "opened", conn: 1, reserved: RESERVED, peerId: PEER_ID });
		expect((await t.closed).code).toBe(1002);
	});

	it("without ALLOW_PRIVATE_PEERS, loopback peers are forbidden", async () => {
		const seed = await peer();
		const t = await openTunnel(strict.url);
		t.send(open(1, seed.port));
		expect(await t.waitFor((f) => f.conn === 1)).toEqual({ type: "failed", conn: 1, reason: FailReason.FORBIDDEN_ADDRESS });
		expect(seed.stats.connections).toBe(0);
		t.close();
	});

	it("rejects foreign origins with 403 before the upgrade", async () => {
		expect(await upgrade(dev.url, { origin: "https://evil.example", "sec-websocket-protocol": SUBPROTOCOL })).toBe(403);
		expect(await upgrade(dev.url, { origin: "http://localhost:4321", "sec-websocket-protocol": SUBPROTOCOL })).toBe(101);
	});

	it("requires the subprotocol and an upgrade", async () => {
		expect(await upgrade(dev.url, {})).toBe(400);
		expect((await fetch(`${dev.url}/v1/tunnel`)).status).toBe(426);
	});
});

async function waitUntil(cond: () => boolean, ms = 10_000): Promise<void> {
	const end = Date.now() + ms;
	while (!cond()) {
		if (Date.now() > end) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 20));
	}
}

/** Status of a raw WebSocket upgrade request (like curl -H 'Upgrade: websocket'). */
function upgrade(base: string, headers: Record<string, string>): Promise<number> {
	return new Promise((resolve, reject) => {
		const req = http.request(`${base}/v1/tunnel`, {
			headers: {
				connection: "Upgrade",
				upgrade: "websocket",
				"sec-websocket-version": "13",
				"sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==", // gitleaks:allow (RFC 6455 sample nonce)
				...headers,
			},
		});
		req.on("upgrade", (res, socket) => {
			socket.destroy();
			resolve(res.statusCode ?? 0);
		});
		req.on("response", (res) => {
			res.resume();
			resolve(res.statusCode ?? 0);
		});
		req.on("error", reject);
		req.end();
	});
}
