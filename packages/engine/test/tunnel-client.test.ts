import { describe, expect, it } from "vitest";
import { AnnounceStatus, CloseReason, FailReason, SUBPROTOCOL, Transport, decodeMessage, encodeFrames, type Frame } from "@magnet-stream/tunnel-protocol";
import { TunnelClient, TunnelOpenError, type WebSocketLike } from "../src/index.ts";

class FakeWebSocket implements WebSocketLike {
	binaryType = "blob";
	readyState = 0;
	protocol = "";
	sent: Frame[][] = [];
	closedWith: number | undefined;
	private listeners: Record<string, ((e: { data?: unknown; code?: number }) => void)[]> = {};
	addEventListener(type: string, l: (e: { data?: unknown; code?: number }) => void) {
		(this.listeners[type] ??= []).push(l);
	}
	emit(type: string, e: { data?: unknown; code?: number } = {}) {
		for (const l of this.listeners[type] ?? []) l(e);
	}
	open(protocol = SUBPROTOCOL) {
		this.readyState = 1;
		this.protocol = protocol;
		this.emit("open");
	}
	send(data: Uint8Array) {
		const r = decodeMessage(data);
		if (!r.ok) throw new Error("client sent garbage");
		this.sent.push(r.frames);
	}
	close(code = 1000) {
		this.closedWith = code;
		this.readyState = 3;
		this.emit("close", { code });
	}
	/** Deliver frames from the "gateway", as one message. */
	deliver(...frames: Frame[]) {
		const bytes = encodeFrames(frames);
		this.emit("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
	}
}

async function client() {
	const ws = new FakeWebSocket();
	const pending = TunnelClient.connect("ws://gateway/v1/tunnel", { factory: (url, protocol) => (expect(protocol).toBe(SUBPROTOCOL), ws) });
	ws.open();
	return { ws, tunnel: await pending };
}

const infoHash = new Uint8Array(20).fill(7);
const peerId = new Uint8Array(20).fill(8);
const reserved = new Uint8Array(8);
const opened = (conn: number): Frame => ({ type: "opened", conn, reserved, peerId: new Uint8Array(20).fill(conn) });

describe("TunnelClient", () => {
	it("sends open frames with distinct conn ids and resolves on opened", async () => {
		const { ws, tunnel } = await client();
		const a = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		const b = tunnel.open({ address: "[2001:db8::1]:51413", infoHash, peerId });
		const [fa, fb] = ws.sent.flat();
		expect(fa).toMatchObject({ type: "open", conn: 1, address: { family: 4, port: 6881 } });
		expect(fb).toMatchObject({ type: "open", conn: 2, address: { family: 6, port: 51413 } });
		ws.deliver(opened(2), opened(1));
		const [ta, tb] = await Promise.all([a, b]);
		expect(ta.conn).toBe(1);
		expect(tb.remote.peerId[0]).toBe(2);
	});

	it("demultiplexes interleaved data frames to the right transports", async () => {
		const { ws, tunnel } = await client();
		const a = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		const b = tunnel.open({ address: "1.2.3.5:6881", infoHash, peerId });
		// opened + data for both in one message, interleaved.
		ws.deliver(
			opened(1),
			{ type: "data", conn: 1, payload: Uint8Array.of(1, 1) },
			opened(2),
			{ type: "data", conn: 2, payload: Uint8Array.of(2) },
			{ type: "data", conn: 1, payload: Uint8Array.of(1) },
			{ type: "data", conn: 2, payload: Uint8Array.of(2, 2) },
		);
		const [ta, tb] = await Promise.all([a, b]);
		const got: Record<number, number[]> = { 1: [], 2: [] };
		ta.attach({ data: (d) => got[1].push(...d), close: () => {} });
		tb.attach({ data: (d) => got[2].push(...d), close: () => {} });
		ws.deliver({ type: "data", conn: 2, payload: Uint8Array.of(2) }, { type: "data", conn: 1, payload: Uint8Array.of(1) });
		expect(got).toEqual({ 1: [1, 1, 1, 1], 2: [2, 2, 2, 2] });

		ta.send(Uint8Array.of(9));
		expect(ws.sent.at(-1)).toEqual([{ type: "data", conn: 1, payload: Uint8Array.of(9) }]);
	});

	it("rejects with the gateway's failure reason", async () => {
		const { ws, tunnel } = await client();
		const p = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		ws.deliver({ type: "failed", conn: 1, reason: FailReason.NOT_BITTORRENT });
		const err = await p.catch((e) => e);
		expect(err).toBeInstanceOf(TunnelOpenError);
		expect(err.reason).toBe(FailReason.NOT_BITTORRENT);
		expect(err.message).toContain("not_bittorrent");
		// The id is free again; the gateway's slot frees once its socket closed (counted as held for a moment).
		void tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId }).catch(() => {});
		expect(tunnel.connections).toBe(2);
		expect(tunnel.heldSlots()).toBe(1);
	});

	it("never opens more than 6 connections", async () => {
		const { ws, tunnel } = await client();
		for (let i = 0; i < 6; i++) void tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId }).catch(() => {});
		const err = await tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId }).catch((e) => e);
		expect(err.reason).toBe(FailReason.TOO_MANY_CONNECTIONS);
		expect(ws.sent.flat()).toHaveLength(6);
	});

	it("reports remote close and sends close on local close", async () => {
		const { ws, tunnel } = await client();
		const p = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		ws.deliver(opened(1));
		const t = await p;
		let closed: Error | undefined | null = null;
		t.attach({ data: () => {}, close: (e) => (closed = e) });
		ws.deliver({ type: "close", conn: 1, reason: CloseReason.REMOTE_ERROR });
		expect(closed).toBeInstanceOf(Error);

		const q = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		ws.deliver(opened(2));
		(await q).close();
		expect(ws.sent.at(-1)).toEqual([{ type: "close", conn: 2, reason: CloseReason.NORMAL }]);
	});

	it("drops the session on a malformed message from the gateway", async () => {
		const { ws, tunnel } = await client();
		const p = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		ws.emit("message", { data: Uint8Array.of(0x55, 0, 0, 0, 0, 0, 0).buffer });
		await expect(p).rejects.toBeInstanceOf(TunnelOpenError);
		expect(ws.closedWith).toBe(1002);
		expect(tunnel.closed).toBe(true);
	});

	it("refuses a gateway that doesn't speak the subprotocol", async () => {
		const ws = new FakeWebSocket();
		const p = TunnelClient.connect("ws://x", { factory: () => ws });
		ws.open("");
		await expect(p).rejects.toThrow(/does not speak/);
	});

	it("counts messages and frames", async () => {
		const { ws, tunnel } = await client();
		void tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		ws.deliver(opened(1), { type: "data", conn: 1, payload: Uint8Array.of(1) });
		expect(tunnel.stats).toMatchObject({ messagesSent: 1, framesSent: 1, messagesReceived: 1, framesReceived: 2 });
	});

	it("counts slots the gateway still holds: a timed-out connect for 20 s from its start, a closed one briefly", async () => {
		let now = 1_000;
		const ws = new FakeWebSocket();
		const pending = TunnelClient.connect("ws://gateway/v1/tunnel", { factory: () => ws, now: () => now });
		ws.open();
		const tunnel = await pending;
		const a = tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId });
		const b = tunnel.open({ address: "1.2.3.5:6881", infoHash, peerId });
		now = 6_000;
		ws.deliver({ type: "failed", conn: 1, reason: FailReason.TIMEOUT }, opened(2));
		await expect(a).rejects.toBeInstanceOf(TunnelOpenError);
		const tb = await b;
		// conn 1 is held until 1_000 + 20_000 + 1_000 (grace); conn 2 is open.
		expect(tunnel.connections).toBe(2);
		tb.close();
		expect(tunnel.connections).toBe(2); // closing: held for a moment
		now = 7_500;
		expect(tunnel.connections).toBe(1);
		now = 21_999;
		expect(tunnel.connections).toBe(1);
		now = 22_000;
		expect(tunnel.connections).toBe(0);
	});

	it("never offers more slots than the gateway has while timed-out connects are held", async () => {
		let now = 0;
		const ws = new FakeWebSocket();
		const pending = TunnelClient.connect("ws://gateway/v1/tunnel", { factory: () => ws, now: () => now });
		ws.open();
		const tunnel = await pending;
		const opens = [1, 2, 3, 4, 5, 6].map((i) => tunnel.open({ address: `1.2.3.${i}:6881`, infoHash, peerId }).catch((e: unknown) => e));
		now = 5_000;
		ws.deliver(...[1, 2, 3, 4, 5, 6].map((conn): Frame => ({ type: "failed", conn, reason: FailReason.TIMEOUT })));
		await Promise.all(opens);
		await expect(tunnel.open({ address: "1.2.3.7:6881", infoHash, peerId })).rejects.toMatchObject({ reason: FailReason.TOO_MANY_CONNECTIONS });
		now = 21_000;
		void tunnel.open({ address: "1.2.3.7:6881", infoHash, peerId }).catch(() => {});
		expect(tunnel.connections).toBe(1);
	});

	it("dials TCP unless asked otherwise, and passes a transport through", async () => {
		const { ws, tunnel } = await client();
		void tunnel.open({ address: "1.2.3.4:6881", infoHash, peerId }).catch(() => {});
		void tunnel.open({ address: "1.2.3.5:6881", infoHash, peerId, transport: Transport.UTP }).catch(() => {});
		const [tcp, utp] = ws.sent.flat();
		expect(tcp.type === "open" && "transport" in tcp).toBe(false);
		expect(utp).toMatchObject({ type: "open", conn: 2, transport: Transport.UTP });
		ws.deliver({ type: "failed", conn: 2, reason: FailReason.UNSUPPORTED_TRANSPORT });
	});

	it("announces, resolves with the gateway's status and hands accepted peers over as transports", async () => {
		const { ws, tunnel } = await client();
		const accepted: { address: string; conn: number; peerTransport: number }[] = [];
		tunnel.onAccept = (p) => {
			accepted.push({ address: p.address, conn: p.transport.conn, peerTransport: p.peerTransport });
			p.transport.attach({ data: (b) => received.push(...b), close: () => {} });
		};
		const received: number[] = [];
		const status = tunnel.announce({ infoHash, peerId, trackers: ["udp://t.example:1337/announce", "wss://x.example", "has space"] });
		const sent = ws.sent.flat();
		expect(sent).toEqual([
			{ type: "announce", conn: 0, infoHash, reserved: Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04), peerId, trackers: ["udp://t.example:1337/announce"] },
		]);
		ws.deliver({ type: "announced", conn: 0, infoHash, status: AnnounceStatus.ACTIVE });
		expect(await status).toBe(AnnounceStatus.ACTIVE);
		expect(tunnel.announcing).toBe(1);
		ws.deliver(
			{ type: "accept", conn: 0x8000, infoHash, reserved, peerId: new Uint8Array(20).fill(9), address: { family: 4, ip: Uint8Array.of(203, 0, 113, 9), port: 40000 } },
			{ type: "data", conn: 0x8000, payload: Uint8Array.of(0, 0, 0, 1, 1) },
		);
		expect(accepted).toEqual([{ address: "203.0.113.9:40000", conn: 0x8000, peerTransport: Transport.TCP }]);
		expect(received).toEqual([0, 0, 0, 1, 1]);
		// An inbound uTP peer is a transport like any other; an unknown transport is closed.
		ws.sent = [];
		ws.deliver(
			{ type: "accept", conn: 0x8003, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(203, 0, 113, 9), port: 40003 }, transport: Transport.UTP },
			{ type: "accept", conn: 0x8004, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(203, 0, 113, 9), port: 40004 }, transport: 7 },
		);
		expect(accepted[1]).toEqual({ address: "203.0.113.9:40003", conn: 0x8003, peerTransport: Transport.UTP });
		expect(accepted).toHaveLength(2);
		expect(ws.sent.flat()).toEqual([{ type: "close", conn: 0x8004, reason: CloseReason.NORMAL }]);
		// Accepted peers do not take outbound slots.
		expect(tunnel.accepted).toBe(2);
		expect(tunnel.connections).toBe(0);
		// An accept for an info-hash we did not announce is closed at once.
		ws.sent = [];
		ws.deliver({ type: "accept", conn: 0x8001, infoHash: new Uint8Array(20).fill(1), reserved, peerId, address: { family: 4, ip: Uint8Array.of(203, 0, 113, 9), port: 40001 } });
		expect(ws.sent.flat()).toEqual([{ type: "close", conn: 0x8001, reason: CloseReason.NORMAL }]);
		// Withdraw: later accepts are refused too.
		ws.sent = [];
		tunnel.withdraw(infoHash);
		expect(ws.sent.flat()).toEqual([{ type: "withdraw", conn: 0, infoHash }]);
		ws.sent = [];
		ws.deliver({ type: "accept", conn: 0x8002, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(203, 0, 113, 9), port: 40002 } });
		expect(ws.sent.flat()).toEqual([{ type: "close", conn: 0x8002, reason: CloseReason.NORMAL }]);
		expect(tunnel.accepted).toBe(2);
	});

	it("a gateway without inbound peers answers unsupported; a closed session rejects pending announces", async () => {
		const { ws, tunnel } = await client();
		const status = tunnel.announce({ infoHash, peerId });
		ws.deliver({ type: "announced", conn: 0, infoHash, status: AnnounceStatus.UNSUPPORTED });
		expect(await status).toBe(AnnounceStatus.UNSUPPORTED);
		expect(tunnel.announcing).toBe(0);
		const pending = tunnel.announce({ infoHash, peerId });
		ws.close(1006);
		await expect(pending).rejects.toThrow();
	});
});
