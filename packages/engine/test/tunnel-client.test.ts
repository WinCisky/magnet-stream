import { describe, expect, it } from "vitest";
import { CloseReason, FailReason, SUBPROTOCOL, decodeMessage, encodeFrames, type Frame } from "@magnet-stream/tunnel-protocol";
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
});
