// wrangler dev + fake peers: open/data/close, failed (refused, timeout,
// not BitTorrent, info-hash mismatch, forbidden address), the 6-connection
// limit, Origin and subprotocol checks, several WebSockets per session,
// idle and session timeouts, socket cleanup, the per-IP upgrade limit.

import { randomBytes } from "node:crypto";
import http from "node:http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type PeerBehaviour } from "@magnet-stream/fake-peer";
import { closedPort, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { CloseReason, FailReason, SUBPROTOCOL, SessionCloseCode, parseSessionEnd, type Frame, type OpenFrame } from "@magnet-stream/tunnel-protocol";
import { openTunnel } from "./tunnel-client";

const torrent = makeTorrent({ seed: 11, size: 2 * 1024 * 1024, pieceLength: 256 * 1024 });
const PEER_ID = new TextEncoder().encode("-MS0001-integration0");
const RESERVED = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);

let dev: WranglerDev; // ALLOW_PRIVATE_PEERS=1
let strict: WranglerDev; // ALLOW_PRIVATE_PEERS=0, 3 upgrades per minute
let short: WranglerDev; // 2 s idle timeout, 6 s sessions
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
		startWranglerDev({ vars: { ...vars, ALLOW_PRIVATE_PEERS: "1", TUNNEL_UPGRADES_PER_MINUTE: "600" } }),
		startWranglerDev({ vars: { ...vars, ALLOW_PRIVATE_PEERS: "0", TUNNEL_UPGRADES_PER_MINUTE: "3" } }),
		startWranglerDev({
			vars: { ...vars, ALLOW_PRIVATE_PEERS: "1", TUNNEL_UPGRADES_PER_MINUTE: "600", TUNNEL_IDLE_TIMEOUT_MS: "2000", TUNNEL_SESSION_TIMEOUT_S: "6" },
		}),
	]);
	// Keep whichever came up, so afterAll stops it even if another failed.
	if (started[0].status === "fulfilled") dev = started[0].value;
	if (started[1].status === "fulfilled") strict = started[1].value;
	if (started[2].status === "fulfilled") short = started[2].value;
	for (const s of started) if (s.status === "rejected") throw s.reason;
});

afterAll(async () => {
	await Promise.all(peers.map((p) => p.close()));
	await Promise.all([dev?.stop(), strict?.stop(), short?.stop()]);
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
		expect(await upgrade(dev.url, { "sec-websocket-protocol": SUBPROTOCOL }, "?session=NOT-HEX")).toBe(400);
	});

	it("closing the WebSocket closes every TCP socket", async () => {
		const seed = await peer();
		const t = await openTunnel(dev.url);
		t.send(open(1, seed.port), open(2, seed.port));
		for (const conn of [1, 2]) await t.waitFor((f) => f.type === "opened" && f.conn === conn);
		t.send({ type: "data", conn: 1, payload: message(2) });
		const before = seed.stats.closed;
		t.close();
		await waitUntil(() => seed.stats.closed >= before + 2, 5_000);
		expect((await t.closed).code).toBe(1000);
		// The gateway's counters reach the client with the closes it initiates (below);
		// whatever it does end, the session is logged with its counters.
		await waitUntil(() => dev.output().includes('"reason":"normal"'), 5_000);
		expect(dev.output()).toMatch(/"event":"tunnel.session_end","reason":"normal","ms":\d+,"messagesIn":2,/);
	});

	it("a second WebSocket to the same session gets 6 more connections; a 4th is refused", async () => {
		const seed = await peer();
		const session = randomBytes(16).toString("hex");
		const a = await openTunnel(dev.url, session);
		const b = await openTunnel(dev.url, session);
		const c = await openTunnel(dev.url, session);
		a.send(...[1, 2, 3, 4, 5, 6].map((conn) => open(conn, seed.port)));
		b.send(...[1, 2, 3, 4, 5, 6].map((conn) => open(conn, seed.port)));
		for (const t of [a, b]) for (const conn of [1, 2, 3, 4, 5, 6]) await t.waitFor((f) => f.type === "opened" && f.conn === conn);
		b.send(open(7, seed.port));
		expect(await b.waitFor((f) => f.conn === 7)).toEqual({ type: "failed", conn: 7, reason: FailReason.TOO_MANY_CONNECTIONS });
		expect(await upgrade(dev.url, { "sec-websocket-protocol": SUBPROTOCOL }, `?session=${session}`)).toBe(429);
		// Another session is unaffected.
		expect(await upgrade(dev.url, { "sec-websocket-protocol": SUBPROTOCOL }, `?session=${randomBytes(16).toString("hex")}`)).toBe(101);
		for (const t of [a, b, c]) t.close();
	});

	it("over the per-IP upgrade limit answers 429 without creating a session", async () => {
		const sessionsBefore = count(strict.output(), "tunnel.session_start");
		const statuses: number[] = [];
		for (let i = 0; i < 5; i++) statuses.push(await upgrade(strict.url, { "sec-websocket-protocol": SUBPROTOCOL }));
		// The forbidden-address test above already used one of the 3.
		expect(statuses.filter((s) => s === 101).length).toBeLessThanOrEqual(3);
		expect(statuses.slice(-2)).toEqual([429, 429]);
		await new Promise((r) => setTimeout(r, 500));
		expect(count(strict.output(), "tunnel.session_start") - sessionsBefore).toBe(statuses.filter((s) => s === 101).length);
		expect(strict.output()).toContain("rate_limited");
	});
});

describe("timeouts (2 s idle, 6 s sessions)", () => {
	it("closes a connection without traffic with reason idle, and keeps a busy one", async () => {
		const seed = await peer();
		const t = await openTunnel(short.url);
		t.send(open(1, seed.port), open(2, seed.port));
		for (const conn of [1, 2]) await t.waitFor((f) => f.type === "opened" && f.conn === conn);
		const keepAlive = setInterval(() => t.send({ type: "data", conn: 2, payload: Uint8Array.of(0, 0, 0, 0) }), 400);
		try {
			expect(await t.waitFor((f) => f.type === "close", 6_000)).toEqual({ type: "close", conn: 1, reason: CloseReason.IDLE });
			await new Promise((r) => setTimeout(r, 500));
			expect(t.frames.some((f) => f.type === "close" && f.conn === 2)).toBe(false);
		} finally {
			clearInterval(keepAlive);
		}
		t.close();
	});

	it("closes a WebSocket on which the client sends nothing", async () => {
		const t = await openTunnel(short.url);
		const closed = await t.closed;
		expect(closed.code).toBe(SessionCloseCode.IDLE);
		expect(parseSessionEnd(closed.reason)?.reason).toBe("idle");
	});

	it("ends the session at its maximum length: close frames, WebSocket close, TCP sockets closed", async () => {
		const seed = await peer();
		const session = randomBytes(16).toString("hex");
		const t = await openTunnel(short.url, session);
		t.send(open(1, seed.port));
		await t.waitFor((f) => f.type === "opened");
		const before = seed.stats.closed;
		const ping = setInterval(() => t.send({ type: "ping", conn: 0, payload: new Uint8Array(0) }, { type: "data", conn: 1, payload: Uint8Array.of(0, 0, 0, 0) }), 400);
		try {
			const closed = await t.closed;
			expect(closed.code).toBe(SessionCloseCode.SESSION_TIMEOUT);
			expect(parseSessionEnd(closed.reason)).toMatchObject({ reason: "session_timeout" });
			expect(t.frames.find((f) => f.type === "close")).toEqual({ type: "close", conn: 1, reason: CloseReason.SESSION_TIMEOUT });
			await waitUntil(() => seed.stats.closed > before, 5_000);
		} finally {
			clearInterval(ping);
		}
	});
});

function count(text: string, needle: string): number {
	return text.split(needle).length - 1;
}

async function waitUntil(cond: () => boolean, ms = 10_000): Promise<void> {
	const end = Date.now() + ms;
	while (!cond()) {
		if (Date.now() > end) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 20));
	}
}

/** Status of a raw WebSocket upgrade request (like curl -H 'Upgrade: websocket'). */
function upgrade(base: string, headers: Record<string, string>, query = ""): Promise<number> {
	return new Promise((resolve, reject) => {
		const req = http.request(`${base}/v1/tunnel${query}`, {
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
