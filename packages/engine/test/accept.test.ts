// Inbound peers through a (fake) VPS gateway: the connection manager
// announces on request and runs an accepted connection as a peer.

import { describe, expect, it } from "vitest";
import { AnnounceStatus, CloseReason, SUBPROTOCOL, decodeMessage, encodeFrames, type Frame } from "@magnet-stream/tunnel-protocol";
import { ConnectionManager, type WebSocketLike } from "../src/index.ts";

class FakeWebSocket implements WebSocketLike {
	binaryType = "blob";
	readyState = 0;
	protocol = "";
	sent: Frame[] = [];
	private listeners: Record<string, ((e: { data?: unknown; code?: number }) => void)[]> = {};
	addEventListener(type: string, l: (e: { data?: unknown; code?: number }) => void) {
		(this.listeners[type] ??= []).push(l);
	}
	emit(type: string, e: { data?: unknown; code?: number } = {}) {
		for (const l of this.listeners[type] ?? []) l(e);
	}
	send(data: Uint8Array) {
		const r = decodeMessage(data);
		if (!r.ok) throw new Error("garbage");
		this.sent.push(...r.frames);
	}
	close(code = 1000) {
		this.readyState = 3;
		this.emit("close", { code });
	}
	deliver(...frames: Frame[]) {
		const bytes = encodeFrames(frames);
		this.emit("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
	}
}

const infoHash = new Uint8Array(20).fill(5);
const reserved = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
const addr = (port: number) => ({ family: 4 as const, ip: Uint8Array.of(203, 0, 113, 7), port });
const tick = () => new Promise((r) => setTimeout(r, 0));

describe("accepted peers", () => {
	it("announces with the session's trackers and runs an accepted connection as a peer", async () => {
		const sockets: FakeWebSocket[] = [];
		const mgr = new ConnectionManager({
			gatewayUrl: "ws://vps.example/v1/tunnel",
			infoHash,
			factory: (_url, protocol) => {
				const ws = new FakeWebSocket();
				sockets.push(ws);
				queueMicrotask(() => {
					ws.readyState = 1;
					ws.protocol = protocol === SUBPROTOCOL ? SUBPROTOCOL : "";
					ws.emit("open");
				});
				return ws;
			},
		});
		const status = mgr.announce(["udp://t.example:1337/announce"]);
		await tick();
		await tick();
		const ws = sockets[0];
		const announce = ws.sent.find((f) => f.type === "announce");
		expect(announce).toMatchObject({ type: "announce", conn: 0, infoHash, trackers: ["udp://t.example:1337/announce"] });
		ws.deliver({ type: "announced", conn: 0, infoHash, status: AnnounceStatus.ACTIVE });
		expect(await status).toBe(AnnounceStatus.ACTIVE);

		ws.deliver(
			{ type: "accept", conn: 0x8000, infoHash, reserved, peerId: new Uint8Array(20).fill(9), address: addr(40000) },
			{ type: "data", conn: 0x8000, payload: Uint8Array.of(0, 0, 0, 1, 0x0e) }, // have-all
		);
		expect(mgr.liveCount).toBe(1);
		const d = mgr.diagnostics();
		expect(d.accepted).toBe(1);
		// We said interested to it, as to any peer.
		await new Promise((r) => setTimeout(r, 450));
		expect(ws.sent.some((f) => f.type === "data" && f.conn === 0x8000)).toBe(true);

		// For another info-hash: closed, not a peer.
		ws.sent = [];
		ws.deliver({ type: "accept", conn: 0x8001, infoHash: new Uint8Array(20).fill(6), reserved, peerId: new Uint8Array(20), address: addr(40001) });
		await tick();
		expect(ws.sent).toContainEqual({ type: "close", conn: 0x8001, reason: CloseReason.NORMAL });
		expect(mgr.liveCount).toBe(1);

		// The gateway's close ends the peer.
		ws.deliver({ type: "close", conn: 0x8000, reason: CloseReason.REMOTE_CLOSED });
		expect(mgr.liveCount).toBe(0);

		ws.sent = [];
		mgr.withdraw();
		await tick();
		expect(ws.sent).toContainEqual({ type: "withdraw", conn: 0, infoHash });
		mgr.stop();
	});
});
