// Exploration sockets: with nothing connected and every slot held by
// connects that timed out (20 s each, OQ-03), the session opens more
// WebSockets, up to its maximum, instead of trying ~9 addresses in 30 s.
// A fake gateway answers every `open` with `failed` TIMEOUT after 5 s.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FailReason, SUBPROTOCOL, decodeMessage, encodeFrames, type Frame } from "@magnet-stream/tunnel-protocol";
import { ConnectionManager, type WebSocketLike } from "../src/index.ts";

class DeadSwarmGateway implements WebSocketLike {
	binaryType = "blob";
	readyState = 0;
	protocol = "";
	opens = 0;
	private listeners: Record<string, ((e: { data?: unknown; code?: number }) => void)[]> = {};
	constructor() {
		setTimeout(() => {
			this.readyState = 1;
			this.protocol = SUBPROTOCOL;
			this.emit("open");
		}, 0);
	}
	addEventListener(type: string, l: (e: { data?: unknown; code?: number }) => void) {
		(this.listeners[type] ??= []).push(l);
	}
	private emit(type: string, e: { data?: unknown; code?: number } = {}) {
		for (const l of this.listeners[type] ?? []) l(e);
	}
	send(data: Uint8Array) {
		const r = decodeMessage(data);
		if (!r.ok) throw new Error("garbage");
		for (const f of r.frames) {
			if (f.type !== "open") continue;
			this.opens++;
			const conn = f.conn;
			setTimeout(() => this.deliver({ type: "failed", conn, reason: FailReason.TIMEOUT }), 5_000);
		}
	}
	close(code = 1000) {
		if (this.readyState === 3) return;
		this.readyState = 3;
		this.emit("close", { code });
	}
	private deliver(...frames: Frame[]) {
		if (this.readyState !== 1) return;
		const bytes = encodeFrames(frames);
		this.emit("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
	}
}

beforeEach(() => {
	vi.useFakeTimers();
});
afterEach(() => {
	vi.useRealTimers();
});

function addresses(n: number): string[] {
	return Array.from({ length: n }, (_, i) => `8.8.${Math.floor(i / 200)}.${(i % 200) + 1}:6881`);
}

describe("exploration sockets", () => {
	it("opens more WebSockets while nothing connects and every slot is held, up to the maximum", async () => {
		const sockets: DeadSwarmGateway[] = [];
		const mgr = new ConnectionManager({
			gatewayUrl: "http://gateway.test",
			infoHash: new Uint8Array(20).fill(1),
			factory: () => {
				const ws = new DeadSwarmGateway();
				sockets.push(ws);
				return ws;
			},
		});
		mgr.addPeers(addresses(60), "discovery");
		const connected = mgr.connect();
		await vi.advanceTimersByTimeAsync(10);
		await connected;
		expect(mgr.socketsOpen).toBe(1);

		await vi.advanceTimersByTimeAsync(4_000);
		// Under 5 s: one socket, at most 3 untried addresses in flight.
		expect(mgr.socketsOpen).toBe(1);
		expect(mgr.untestedHighWater).toBeLessThanOrEqual(3);

		await vi.advanceTimersByTimeAsync(30_000);
		expect(mgr.socketsOpen).toBe(3);
		expect(mgr.upgrades).toBe(3);
		const tried = sockets.reduce((n, s) => n + s.opens, 0);
		// Three sockets try far more than the ~9 addresses one socket reaches in 30 s.
		expect(tried).toBeGreaterThan(20);

		await vi.advanceTimersByTimeAsync(60_000);
		expect(mgr.socketsOpen).toBe(3);
		mgr.stop();
	});

	it("does not explore without untried addresses", async () => {
		const mgr = new ConnectionManager({
			gatewayUrl: "http://gateway.test",
			infoHash: new Uint8Array(20).fill(1),
			exploreAfterMs: 5_000,
			factory: () => new DeadSwarmGateway(),
		});
		const connected = mgr.connect();
		await vi.advanceTimersByTimeAsync(10);
		await connected;
		// No candidates at all: nothing to explore for.
		await vi.advanceTimersByTimeAsync(30_000);
		expect(mgr.socketsOpen).toBe(1);
		mgr.stop();
	});
});
