import { describe, expect, it } from "vitest";
import {
	CloseReason,
	FailReason,
	FrameType,
	decodeMessage,
	encodeFrames,
	type Frame,
	type OpenFrame,
	type PeerAddress,
} from "@magnet-stream/tunnel-protocol";
import { parseSessionEnd, SessionCloseCode } from "@magnet-stream/tunnel-protocol";
import { TunnelSession, handshake, type LogFields, type SocketLike } from "../../src/tunnel/session";

const INFO_HASH = Uint8Array.from({ length: 20 }, (_, i) => i + 1);
const OTHER_HASH = Uint8Array.from({ length: 20 }, (_, i) => 200 - i);
const PEER_ID = new TextEncoder().encode("-MS0001-000000000000");
const RESERVED = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
const REMOTE_ID = new TextEncoder().encode("-FP0001-remoteremote");

type Behaviour =
	| { kind: "refuse" }
	| { kind: "hang" } // never connects
	| { kind: "silent" } // connects, never answers
	| { kind: "raw"; bytes: Uint8Array } // answers with these bytes
	| { kind: "peer"; infoHash?: Uint8Array; extra?: Uint8Array; chunked?: boolean; stall?: boolean };

/** Fake remote side of a socket: records writes, lets the test push bytes. */
class Remote {
	written: Uint8Array[] = [];
	closed = false;
	private controller!: ReadableStreamDefaultController<Uint8Array>;
	private resolveClosed!: () => void;
	readonly socket: SocketLike;

	constructor(behaviour: Behaviour) {
		const closed = new Promise<void>((r) => (this.resolveClosed = r));
		const readable = new ReadableStream<Uint8Array>({ start: (c) => void (this.controller = c) });
		const writable = new WritableStream<Uint8Array>(
			{
				write: (chunk) => {
					this.written.push(chunk.slice());
					if (this.written.length === 1) this.answer(behaviour);
					// A peer that stops reading after the handshake: later writes never complete.
					else if (behaviour.kind === "peer" && behaviour.stall) return new Promise<void>(() => {});
				},
			},
			{ highWaterMark: 1 },
		);
		const opened =
			behaviour.kind === "refuse"
				? Promise.reject(new Error("refused"))
				: behaviour.kind === "hang"
					? new Promise<void>(() => {})
					: Promise.resolve();
		opened.catch(() => {});
		this.socket = {
			opened,
			closed,
			readable,
			writable,
			close: async () => this.close(),
		};
	}

	private answer(b: Behaviour): void {
		if (b.kind === "raw") this.push(b.bytes);
		if (b.kind !== "peer") return;
		const hs = handshake({ reserved: RESERVED, infoHash: b.infoHash ?? INFO_HASH, peerId: REMOTE_ID });
		const all = b.extra ? Uint8Array.of(...hs, ...b.extra) : hs;
		if (b.chunked) for (const byte of all) this.push(Uint8Array.of(byte));
		else this.push(all);
	}

	push(bytes: Uint8Array): void {
		if (!this.closed) this.controller.enqueue(bytes);
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		try {
			this.controller.close();
		} catch {}
		this.resolveClosed();
	}
}

interface SetupOptions {
	allowPrivate?: boolean;
	handshakeTimeoutMs?: number;
	connectTimeoutMs?: number;
	idleTimeoutMs?: number;
	failedConnectHoldMs?: number;
	maxPendingWriteBytes?: number;
	now?: () => number;
}

function setup(behaviours: Behaviour[] | ((i: number) => Behaviour), options: SetupOptions = {}) {
	const sent: Frame[][] = [];
	const logs: { event: string; fields: LogFields }[] = [];
	const remotes: Remote[] = [];
	const wsClosed: { code: number; reason: string }[] = [];
	const connected: PeerAddress[] = [];
	const session = new TunnelSession(
		{
			send: (m) => {
				const r = decodeMessage(m);
				if (!r.ok) throw new Error("gateway sent an invalid message");
				sent.push(r.frames);
			},
			close: (code, reason) => wsClosed.push({ code, reason }),
		},
		{
			allowPrivate: options.allowPrivate ?? true,
			handshakeTimeoutMs: options.handshakeTimeoutMs ?? 300,
			connectTimeoutMs: options.connectTimeoutMs ?? 300,
			failedConnectHoldMs: options.failedConnectHoldMs ?? 0,
			...(options.idleTimeoutMs === undefined ? {} : { idleTimeoutMs: options.idleTimeoutMs }),
			...(options.maxPendingWriteBytes === undefined ? {} : { maxPendingWriteBytes: options.maxPendingWriteBytes }),
			...(options.now === undefined ? {} : { now: options.now }),
			connect: (address) => {
				connected.push(address);
				const i = remotes.length;
				const b = typeof behaviours === "function" ? behaviours(i) : behaviours[i];
				const remote = new Remote(b);
				remotes.push(remote);
				return remote.socket;
			},
			log: (event, fields) => logs.push({ event, fields }),
		},
	);
	const frames = () => sent.flat();
	const send = (...fs: Frame[]) => session.onMessage(encodeFrames(fs));
	return { session, sent, frames, logs, remotes, wsClosed, connected, send };
}

function open(conn: number, ip = [127, 0, 0, 1], port = 6881, infoHash = INFO_HASH): OpenFrame {
	const address: PeerAddress = ip.length === 4 ? { family: 4, ip: Uint8Array.from(ip), port } : { family: 6, ip: Uint8Array.from(ip), port };
	return { type: "open", conn, infoHash, reserved: RESERVED, peerId: PEER_ID, address };
}

const tick = (ms = 10) => new Promise((r) => setTimeout(r, ms));
async function until(cond: () => boolean, ms = 1000): Promise<void> {
	const end = Date.now() + ms;
	while (!cond()) {
		if (Date.now() > end) throw new Error("timed out");
		await tick(5);
	}
}

describe("tunnel session", () => {
	it("writes the handshake, waits for the remote's, then relays both ways", async () => {
		const t = setup([{ kind: "peer", extra: Uint8Array.of(0, 0, 0, 1, 1) }]);
		t.send(open(1));
		await until(() => t.frames().some((f) => f.type === "opened"));
		// Our handshake was exactly the 68 bytes built from the open frame.
		expect(t.remotes[0].written[0]).toEqual(handshake({ reserved: RESERVED, infoHash: INFO_HASH, peerId: PEER_ID }));
		const opened = t.sent.find((m) => m[0].type === "opened")!;
		expect(opened[0]).toMatchObject({ type: "opened", conn: 1 });
		expect((opened[0] as { peerId: Uint8Array }).peerId).toEqual(REMOTE_ID);
		// The unchoke that came with the handshake rides in the same message.
		expect(opened[1]).toMatchObject({ type: "data", conn: 1, payload: Uint8Array.of(0, 0, 0, 1, 1) });

		t.send({ type: "data", conn: 1, payload: Uint8Array.of(0, 0, 0, 1, 2) });
		await until(() => t.remotes[0].written.length === 2);
		expect(t.remotes[0].written[1]).toEqual(Uint8Array.of(0, 0, 0, 1, 2));

		t.remotes[0].push(Uint8Array.of(9, 9, 9));
		await until(() => t.frames().filter((f) => f.type === "data").length === 2);
		expect(t.frames().at(-1)).toMatchObject({ type: "data", conn: 1, payload: Uint8Array.of(9, 9, 9) });

		t.remotes[0].close();
		await until(() => t.frames().some((f) => f.type === "close"));
		expect(t.frames().at(-1)).toEqual({ type: "close", conn: 1, reason: CloseReason.REMOTE_CLOSED });
	});

	it("closes a peer that does not read past the pending-write bound, and only that one", async () => {
		const t = setup([{ kind: "peer", stall: true }, { kind: "peer" }], { maxPendingWriteBytes: 64 * 1024 });
		t.send(open(1), open(2));
		await until(() => t.frames().filter((f) => f.type === "opened").length === 2);
		const chunk = new Uint8Array(16 * 1024);
		// The first write is in progress (and never ends); the next ones wait behind it.
		for (let i = 0; i < 4; i++) t.send({ type: "data", conn: 1, payload: chunk });
		await tick(10);
		expect(t.frames().some((f) => f.type === "close")).toBe(false);
		t.send({ type: "data", conn: 1, payload: Uint8Array.of(1) });
		await until(() => t.frames().some((f) => f.type === "close"));
		expect(t.frames().filter((f) => f.type === "close")).toEqual([{ type: "close", conn: 1, reason: CloseReason.WRITE_OVERFLOW }]);
		expect(t.session.stats.overflowClosed).toBe(1);
		expect(t.logs.some((l) => l.event === "tunnel.closed" && l.fields.reason === "write_overflow")).toBe(true);
		// The other connection of the socket goes on.
		t.send({ type: "data", conn: 2, payload: chunk }, { type: "data", conn: 2, payload: chunk });
		await until(() => t.remotes[1].written.length === 3);
		expect(t.session.isEnded).toBe(false);
	});

	it("frees the bound as the peer reads", async () => {
		const t = setup([{ kind: "peer" }], { maxPendingWriteBytes: 64 * 1024 });
		t.send(open(1));
		await until(() => t.frames().some((f) => f.type === "opened"));
		const chunk = new Uint8Array(32 * 1024);
		for (let i = 0; i < 20; i++) {
			t.send({ type: "data", conn: 1, payload: chunk });
			await until(() => t.remotes[0].written.length === i + 2);
		}
		expect(t.frames().some((f) => f.type === "close")).toBe(false);
	});

	it("handles a handshake arriving one byte at a time", async () => {
		const t = setup([{ kind: "peer", chunked: true }]);
		t.send(open(1));
		await until(() => t.frames().some((f) => f.type === "opened"));
	});

	it("relays nothing before the handshake: early data closes the connection", async () => {
		const t = setup([{ kind: "silent" }]);
		t.send(open(1), { type: "data", conn: 1, payload: Uint8Array.of(1, 2, 3) });
		await tick(20);
		expect(t.frames()).toEqual([{ type: "close", conn: 1, reason: CloseReason.PROTOCOL_ERROR }]);
		// Only our own handshake ever reached the remote.
		expect(t.remotes[0].written.length).toBeLessThanOrEqual(1);
		expect(t.remotes[0].closed).toBe(true);
	});

	it("fails with not BitTorrent, info-hash mismatch, refused, timeout, remote closed", async () => {
		const t = setup([
			{ kind: "raw", bytes: new TextEncoder().encode("HTTP/1.1 400 Bad Request\r\n") },
			{ kind: "peer", infoHash: OTHER_HASH },
			{ kind: "refuse" },
			{ kind: "silent" },
			{ kind: "hang" },
			{ kind: "raw", bytes: Uint8Array.of(19) },
		]);
		t.send(open(1), open(2), open(3), open(4), open(5), open(6));
		await until(() => t.remotes[5]?.written.length === 1);
		t.remotes[5].close(); // hangs up after a partial handshake
		await until(() => t.frames().filter((f) => f.type === "failed").length === 6, 2000);
		const reasons = Object.fromEntries(t.frames().map((f) => [f.conn, (f as { reason: number }).reason]));
		expect(reasons).toEqual({
			1: FailReason.NOT_BITTORRENT,
			2: FailReason.INFOHASH_MISMATCH,
			3: FailReason.REFUSED,
			4: FailReason.TIMEOUT,
			5: FailReason.TIMEOUT,
			6: FailReason.REMOTE_CLOSED,
		});
		expect(t.frames().some((f) => f.type === "data" || f.type === "opened")).toBe(false);
		await until(() => t.session.activeSlots === 0);
	});

	it("refuses forbidden addresses without connecting", async () => {
		const t = setup([], { allowPrivate: false });
		t.send(open(1, [127, 0, 0, 1]), open(2, [10, 0, 0, 1]), open(3, [8, 8, 8, 8], 80), open(4, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 0, 1]));
		await tick();
		expect(t.connected).toHaveLength(0);
		expect(t.frames().map((f) => (f as { reason: number }).reason)).toEqual(Array(4).fill(FailReason.FORBIDDEN_ADDRESS));
	});

	it("allows at most 6 connections per WebSocket", async () => {
		const t = setup(() => ({ kind: "peer" }));
		t.send(...[1, 2, 3, 4, 5, 6, 7].map((c) => open(c)));
		await until(() => t.frames().filter((f) => f.type === "opened").length === 6);
		expect(t.connected).toHaveLength(6);
		expect(t.frames().find((f) => f.conn === 7)).toEqual({ type: "failed", conn: 7, reason: FailReason.TOO_MANY_CONNECTIONS });
		// Closing one frees a slot once its socket has closed.
		t.send({ type: "close", conn: 1, reason: CloseReason.NORMAL });
		await until(() => t.session.activeSlots === 5);
		t.send(open(8));
		await until(() => t.frames().some((f) => f.type === "opened" && f.conn === 8));
		expect(t.remotes[0].closed).toBe(true);
	});

	it("rejects a conn id still in use", async () => {
		const t = setup(() => ({ kind: "silent" }));
		t.send(open(1), open(1));
		await tick();
		expect(t.frames()).toEqual([{ type: "failed", conn: 1, reason: FailReason.CONN_IN_USE }]);
	});

	it("a malformed message or a gateway-only frame closes the session and every socket", async () => {
		const t = setup(() => ({ kind: "peer" }));
		t.send(open(1));
		await until(() => t.frames().some((f) => f.type === "opened"));
		t.session.onMessage(Uint8Array.of(0x42, 0, 1, 0, 0, 0, 0));
		expect(t.wsClosed[0].code).toBe(1002);
		await until(() => t.remotes[0].closed);

		const u = setup(() => ({ kind: "peer" }));
		u.send({ type: "opened", conn: 1, reserved: RESERVED, peerId: PEER_ID });
		expect(u.wsClosed[0].code).toBe(1002);

		const v = setup([]);
		v.session.onMessage("hello");
		expect(v.wsClosed[0].code).toBe(1003);
	});

	it("closing the WebSocket closes every TCP socket", async () => {
		const t = setup(() => ({ kind: "peer" }));
		t.send(open(1), open(2));
		await until(() => t.frames().filter((f) => f.type === "opened").length === 2);
		t.session.onClose();
		await until(() => t.remotes.every((r) => r.closed));
		await until(() => t.session.activeSlots === 0);
	});

	it("ignores ping and frames for unknown connections", async () => {
		const t = setup([]);
		t.send({ type: "ping", conn: 0, payload: new Uint8Array(0) }, { type: "data", conn: 9, payload: Uint8Array.of(1) }, { type: "close", conn: 9, reason: 0 });
		await tick();
		expect(t.frames()).toEqual([]);
		expect(t.wsClosed).toEqual([]);
	});

	it("logs no addresses, info-hashes or peer ids", async () => {
		const t = setup([{ kind: "peer" }, { kind: "refuse" }], { allowPrivate: false });
		t.send(open(1, [8, 8, 4, 4], 51413), open(2, [9, 9, 9, 9], 51414));
		await until(() => t.frames().length === 2);
		t.session.onClose();
		const text = JSON.stringify(t.logs);
		for (const secret of ["8.8.4.4", "9.9.9.9", "51413", "51414", "0102030405", "MS0001", "FP0001"]) expect(text).not.toContain(secret);
		expect(t.logs.map((l) => l.event)).toContain("tunnel.session_end");
	});

	it("never sends frames of types the client may not receive", async () => {
		const t = setup(() => ({ kind: "peer" }));
		t.send(open(1));
		await until(() => t.frames().some((f) => f.type === "opened"));
		const allowed = new Set(["opened", "failed", "data", "close", "ping"]);
		expect(t.frames().every((f) => allowed.has(f.type))).toBe(true);
		expect(FrameType.ACCEPT).toBe(6);
	});
});

describe("session lifetime", () => {
	function clock() {
		let t = 1_000_000;
		return { now: () => t, advance: (ms: number) => (t += ms) };
	}

	it("closes a connection idle for 90 s with reason idle, and keeps busy ones", async () => {
		const c = clock();
		const t = setup(() => ({ kind: "peer" }), { now: c.now });
		t.send(open(1), open(2));
		await until(() => t.frames().filter((f) => f.type === "opened").length === 2);
		c.advance(60_000);
		t.send({ type: "data", conn: 2, payload: Uint8Array.of(0, 0, 0, 0) }); // keep-alive on conn 2
		c.advance(29_999);
		t.session.sweep();
		expect(t.frames().some((f) => f.type === "close")).toBe(false);
		c.advance(1);
		t.session.sweep();
		expect(t.frames().filter((f) => f.type === "close")).toEqual([{ type: "close", conn: 1, reason: CloseReason.IDLE }]);
		await until(() => t.remotes[0].closed);
		expect(t.remotes[1].closed).toBe(false);
		// Bytes from the remote count as traffic too.
		c.advance(60_000);
		t.remotes[1].push(Uint8Array.of(0, 0, 0, 0));
		await until(() => t.frames().filter((f) => f.type === "data").length >= 1);
		t.send({ type: "ping", conn: 0, payload: new Uint8Array(0) });
		c.advance(60_000);
		t.session.sweep();
		expect(t.remotes[1].closed).toBe(false);
		expect(t.session.stats.idleClosed).toBe(1);
	});

	it("closes the WebSocket when the client sends nothing (not even ping) for the idle timeout", () => {
		const c = clock();
		const t = setup([], { now: c.now });
		c.advance(80_000);
		t.send({ type: "ping", conn: 0, payload: new Uint8Array(0) });
		c.advance(89_999);
		t.session.sweep();
		expect(t.wsClosed).toEqual([]);
		c.advance(1);
		t.session.sweep();
		expect(t.wsClosed).toEqual([{ code: SessionCloseCode.IDLE, reason: "idle in=1 up=0 down=0" }]);
		expect(t.session.isEnded).toBe(true);
	});

	it("shutdown closes every connection with the reason, then the WebSocket with the counters", async () => {
		const t = setup(() => ({ kind: "peer" }));
		t.send(open(1), open(2));
		await until(() => t.frames().filter((f) => f.type === "opened").length === 2);
		t.send({ type: "data", conn: 1, payload: Uint8Array.of(1, 2, 3) });
		t.session.shutdown("session_timeout", SessionCloseCode.SESSION_TIMEOUT, CloseReason.SESSION_TIMEOUT);
		expect(t.sent.at(-1)).toEqual([
			{ type: "close", conn: 1, reason: CloseReason.SESSION_TIMEOUT },
			{ type: "close", conn: 2, reason: CloseReason.SESSION_TIMEOUT },
		]);
		expect(t.wsClosed).toHaveLength(1);
		expect(t.wsClosed[0].code).toBe(SessionCloseCode.SESSION_TIMEOUT);
		expect(parseSessionEnd(t.wsClosed[0].reason)).toEqual({ reason: "session_timeout", counters: { messagesIn: 2, bytesUp: 3, bytesDown: 0 } });
		await until(() => t.remotes.every((r) => r.closed));
		await until(() => t.session.activeSlots === 0);
		expect(t.logs.find((l) => l.event === "tunnel.session_end")?.fields).toMatchObject({ reason: "session_timeout", messagesIn: 2, bytesUp: 3 });
	});

	it("keeps the slot of a timed-out connect taken for the hold time", async () => {
		const t = setup([{ kind: "hang" }, { kind: "silent" }], { connectTimeoutMs: 50, handshakeTimeoutMs: 50, failedConnectHoldMs: 400 });
		t.send(open(1), open(2));
		await until(() => t.frames().filter((f) => f.type === "failed").length === 2);
		// conn 2 connected (then timed out on the handshake): its slot comes back at once.
		await until(() => t.session.activeSlots === 1);
		await tick(150);
		expect(t.session.activeSlots).toBe(1);
		await until(() => t.session.activeSlots === 0, 1000);
	});
});
