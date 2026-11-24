// Hole punching in a session (M4-05): through a (fake) VPS gateway that
// announces, an inbound relay that names a peer behind NAT in ut_pex gets
// a rendezvous for it; its `connect` turns into a uTP `open` and the result
// is a peer like any other. An error, a silent relay or an unsolicited
// `connect` marks or ignores, and the session goes on.

import { describe, expect, it } from "vitest";
import { AnnounceStatus, CloseReason, FailReason, SUBPROTOCOL, Transport, decodeMessage, encodeFrames, parseAddress, type Frame } from "@magnet-stream/tunnel-protocol";
import { makeTorrent } from "@magnet-stream/fake-peer";
import {
	ConnectionManager,
	HolepunchError,
	LOCAL_EXTENSIONS,
	MSG,
	PEX_HOLEPUNCH,
	decodeHolepunch,
	encodeHolepunch,
	encodeMessage,
	parseInfoDict,
	type HolepunchMessage,
	type WebSocketLike,
} from "../src/index.ts";
import { encode } from "./helpers.ts";

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

const reserved = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
const RELAY = parseAddress("93.184.216.10:40000")!;
const TARGET = "93.184.216.20:51413";
const RELAY_HP = 7;
const RELAY_PEX = 8;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const extended = (id: number, payload: Uint8Array) => encodeMessage(MSG.EXTENDED, Uint8Array.of(id, ...payload));

/** ut_pex `added` with flags. */
function pex(keys: string[], flags: number): Uint8Array {
	const added: number[] = [];
	for (const k of keys) {
		const a = parseAddress(k)!;
		added.push(...a.ip, a.port >> 8, a.port & 0xff);
	}
	return encode({ added: Uint8Array.from(added), "added.f": Uint8Array.from(keys.map(() => flags)) });
}

/** Peer-wire messages the engine sent on `conn` (length prefix stripped). */
function messages(ws: FakeWebSocket, conn: number): Uint8Array[] {
	const parts = ws.sent.filter((f) => f.type === "data" && f.conn === conn).map((f) => (f as { payload: Uint8Array }).payload);
	const bytes = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		bytes.set(p, at);
		at += p.length;
	}
	const out: Uint8Array[] = [];
	for (let i = 0; i + 4 <= bytes.length; ) {
		const len = new DataView(bytes.buffer, i).getUint32(0);
		if (len > 0) out.push(bytes.slice(i + 4, i + 4 + len));
		i += 4 + len;
	}
	return out;
}

/** The ut_holepunch messages the engine sent to the relay. */
function holepunches(ws: FakeWebSocket, conn: number): HolepunchMessage[] {
	return messages(ws, conn)
		.filter((m) => m[0] === MSG.EXTENDED && m[1] === RELAY_HP)
		.map((m) => decodeHolepunch(m.slice(2)));
}

async function until(cond: () => boolean, ms = 2_000): Promise<void> {
	const end = Date.now() + ms;
	while (!cond()) {
		if (Date.now() > end) throw new Error("condition not met in time");
		await sleep(5);
	}
}

/**
 * A session on a VPS gateway that announces, with the metadata and a
 * recovery running, and the TCP dial to the target already failed.
 * `relay`: an inbound peer supporting ut_holepunch that named the target.
 */
async function session(options: { withdraw?: boolean; relayInbound?: boolean; answerTimeoutMs?: number } = {}) {
	const torrent = makeTorrent({ seed: 55, size: 512 * 1024, pieceLength: 256 * 1024 });
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const infoHash = torrent.infoHash;
	const sockets: FakeWebSocket[] = [];
	const url = "ws://vps.example/v1/tunnel";
	const mgr = new ConnectionManager({
		gatewayUrl: url,
		infoHash,
		batchMs: 0,
		sweepMs: 10,
		escalation: { gatewayUrl: url },
		holepunch: { intervalMs: 0, answerTimeoutMs: options.answerTimeoutMs ?? 5_000 },
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
	mgr.setInfo(meta);
	const recovery = mgr.recover({ offset: 0, size: meta.totalLength });
	await until(() => sockets.length > 0 && mgr.socketsOpen > 0);
	const ws = sockets[0];
	const status = mgr.announce([]);
	await until(() => ws.sent.some((f) => f.type === "announce"));
	ws.deliver({ type: "announced", conn: 0, infoHash, status: AnnounceStatus.ACTIVE });
	expect(await status).toBe(AnnounceStatus.ACTIVE);
	// The relay: inbound (it reached the announced port) or dialled over TCP.
	const relayConn = options.relayInbound === false ? 1 : 0x8000;
	const handshake = extended(0, encode({ m: { ut_holepunch: RELAY_HP, ut_pex: RELAY_PEX } }));
	const haveNone = encodeMessage(MSG.HAVE_NONE);
	const named = extended(LOCAL_EXTENSIONS.ut_pex, pex([TARGET], PEX_HOLEPUNCH));
	if (relayConn === 0x8000) {
		ws.deliver(
			{ type: "accept", conn: relayConn, infoHash, reserved, peerId: new Uint8Array(20).fill(7), address: RELAY },
			{ type: "data", conn: relayConn, payload: Uint8Array.of(...haveNone, ...handshake, ...named) },
		);
	} else {
		mgr.addPeers(["93.184.216.10:40000"]);
		await until(() => ws.sent.some((f) => f.type === "open" && f.conn === 1));
		ws.deliver({ type: "opened", conn: 1, reserved, peerId: new Uint8Array(20).fill(7) }, { type: "data", conn: 1, payload: Uint8Array.of(...haveNone, ...handshake, ...named) });
	}
	// No longer announcing: the gateway would refuse a uTP open (accepted peers stay).
	if (options.withdraw) mgr.withdraw();
	// The target was dialled over TCP at once: it is behind NAT, the dial fails.
	await until(() => ws.sent.some((f) => f.type === "open" && formatOpen(f) === TARGET && f.transport === undefined));
	const tcp = ws.sent.find((f) => f.type === "open" && formatOpen(f) === TARGET)!;
	ws.deliver({ type: "failed", conn: tcp.conn, reason: FailReason.REFUSED });
	return { mgr, ws, relayConn, infoHash, recovery };
}

function formatOpen(f: Frame): string | undefined {
	if (f.type !== "open") return undefined;
	return `${[...f.address.ip].join(".")}:${f.address.port}`;
}

const utpOpen = (ws: FakeWebSocket) => ws.sent.find((f) => f.type === "open" && f.transport === Transport.UTP) as Extract<Frame, { type: "open" }> | undefined;
const target = parseAddress(TARGET)!;

describe("hole punching in a session", () => {
	it("sends a rendezvous through an inbound relay and turns its connect into a uTP open: a peer like any other", async () => {
		const { mgr, ws, relayConn } = await session();
		await until(() => holepunches(ws, relayConn).length > 0);
		expect(holepunches(ws, relayConn)).toEqual([{ type: "rendezvous", address: target }]);
		expect(mgr.book.get(TARGET)?.state).toBe("connecting");

		ws.deliver({ type: "data", conn: relayConn, payload: extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "connect", address: target })) });
		await until(() => utpOpen(ws) !== undefined);
		const open = utpOpen(ws)!;
		expect(formatOpen(open)).toBe(TARGET);
		ws.deliver({ type: "opened", conn: open.conn, reserved, peerId: new Uint8Array(20).fill(8) }, { type: "data", conn: open.conn, payload: encodeMessage(MSG.HAVE_ALL) });
		await until(() => mgr.liveCount === 2);
		expect(mgr.book.get(TARGET)?.state).toBe("connected");
		expect(mgr.diagnostics().holepunch).toMatchObject({ attempted: 1, connected: 1, failed: {} });
		// We are interested in what it has, and ask it for blocks like any peer.
		ws.deliver({ type: "data", conn: open.conn, payload: encodeMessage(MSG.UNCHOKE) });
		await until(() => messages(ws, open.conn).some((m) => m[0] === MSG.REQUEST));
		mgr.stop();
	});

	it("a rendezvous error marks the target and the session goes on", async () => {
		const { mgr, ws, relayConn } = await session();
		await until(() => holepunches(ws, relayConn).length > 0);
		ws.deliver({
			type: "data",
			conn: relayConn,
			payload: extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "error", address: target, error: HolepunchError.NO_SUCH_PEER })),
		});
		await until(() => mgr.diagnostics().holepunch.failed.no_such_peer === 1);
		expect(mgr.book.get(TARGET)).toMatchObject({ state: "failed", lastFailure: "hp_no_such_peer" });
		expect(utpOpen(ws)).toBeUndefined();
		// The relay is still a peer, and the session still works: another target is punched.
		expect(mgr.liveCount).toBe(1);
		const other = "93.184.216.21:51413";
		ws.deliver({ type: "data", conn: relayConn, payload: extended(LOCAL_EXTENSIONS.ut_pex, pex([other], PEX_HOLEPUNCH)) });
		const tcp = await (async () => {
			await until(() => ws.sent.some((f) => f.type === "open" && formatOpen(f) === other));
			return ws.sent.find((f) => f.type === "open" && formatOpen(f) === other)!;
		})();
		ws.deliver({ type: "failed", conn: tcp.conn, reason: FailReason.TIMEOUT });
		await until(() => holepunches(ws, relayConn).some((m) => m.type === "rendezvous" && `${[...m.address.ip].join(".")}:${m.address.port}` === other));
		mgr.stop();
	});

	it("an unanswered rendezvous times out and marks the target", async () => {
		const { mgr, ws, relayConn } = await session({ answerTimeoutMs: 150 });
		await until(() => holepunches(ws, relayConn).length > 0);
		await until(() => mgr.diagnostics().holepunch.failed.timeout === 1);
		expect(mgr.book.get(TARGET)).toMatchObject({ state: "failed", lastFailure: "hp_timeout" });
		expect(mgr.liveCount).toBe(1);
		mgr.stop();
	});

	it("a failed uTP open marks the target", async () => {
		const { mgr, ws, relayConn } = await session();
		await until(() => holepunches(ws, relayConn).length > 0);
		ws.deliver({ type: "data", conn: relayConn, payload: extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "connect", address: target })) });
		await until(() => utpOpen(ws) !== undefined);
		ws.deliver({ type: "failed", conn: utpOpen(ws)!.conn, reason: FailReason.TIMEOUT });
		await until(() => mgr.diagnostics().holepunch.failed.open_timeout === 1);
		expect(mgr.book.get(TARGET)?.lastFailure).toBe("hp_open_timeout");
		mgr.stop();
	});

	it("the target's SYN may come first: its inbound connection is the punch, and the open's is closed as a duplicate", async () => {
		const { mgr, ws, relayConn, infoHash } = await session();
		await until(() => holepunches(ws, relayConn).length > 0);
		ws.deliver({ type: "data", conn: relayConn, payload: extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "connect", address: target })) });
		await until(() => utpOpen(ws) !== undefined);
		ws.deliver({ type: "accept", conn: 0x8001, infoHash, reserved, peerId: new Uint8Array(20).fill(8), address: target, transport: Transport.UTP });
		await until(() => mgr.liveCount === 2);
		expect(mgr.diagnostics().holepunch.connected).toBe(1);
		const open = utpOpen(ws)!;
		ws.deliver({ type: "opened", conn: open.conn, reserved, peerId: new Uint8Array(20).fill(8) });
		await until(() => ws.sent.some((f) => f.type === "close" && f.conn === open.conn));
		expect(mgr.liveCount).toBe(2);
		mgr.stop();
	});

	it("ignores a connect it did not ask for, and relays nobody", async () => {
		const { mgr, ws, relayConn } = await session({ withdraw: true });
		await sleep(80);
		// Not announcing on the VPS: no punch at all.
		expect(holepunches(ws, relayConn)).toEqual([]);
		// A connect out of the blue does not make the gateway dial.
		ws.deliver({ type: "data", conn: relayConn, payload: extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "connect", address: target })) });
		// A rendezvous asks us to relay: refused.
		ws.deliver({ type: "data", conn: relayConn, payload: extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "rendezvous", address: target })) });
		await until(() => holepunches(ws, relayConn).length > 0);
		expect(holepunches(ws, relayConn)).toEqual([{ type: "error", address: target, error: HolepunchError.NOT_CONNECTED }]);
		expect(utpOpen(ws)).toBeUndefined();
		expect(mgr.diagnostics().holepunch.unsolicited).toBe(1);
		mgr.stop();
	});

	it("does not use a relay it dialled over TCP (it sees the gateway's ephemeral port)", async () => {
		const { mgr, ws, relayConn } = await session({ relayInbound: false });
		await sleep(100);
		expect(holepunches(ws, relayConn)).toEqual([]);
		expect(mgr.diagnostics().holepunch.attempted).toBe(0);
		mgr.stop();
	});

	it("a relay that leaves fails its punch without blaming the target", async () => {
		const { mgr, ws, relayConn } = await session();
		await until(() => holepunches(ws, relayConn).length > 0);
		ws.deliver({ type: "close", conn: relayConn, reason: CloseReason.REMOTE_CLOSED });
		await until(() => mgr.diagnostics().holepunch.failed.relay_closed === 1);
		expect(mgr.book.get(TARGET)?.lastFailure).toBe("refused");
		mgr.stop();
	});
});
