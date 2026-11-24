// ut_holepunch (BEP 55, M4-05): the message codec, the bookkeeping of
// punches (relays, targets, limits, errors, timeouts) and how a peer
// connection answers and forwards the messages.

import { describe, expect, it } from "vitest";
import { parseAddress } from "@magnet-stream/tunnel-protocol";
import {
	BufferedTransport,
	ExtensionError,
	HolePuncher,
	HolepunchError,
	LOCAL_EXTENSIONS,
	MSG,
	PEX_HOLEPUNCH,
	PeerConnection,
	decodeHolepunch,
	encodeHolepunch,
	encodeMessage,
	holepunchErrorName,
	type HolepunchMessage,
	type PexAddress,
} from "../src/index.ts";
import { encode } from "./helpers.ts";

const v4 = parseAddress("93.184.216.34:51413")!;
const v6 = parseAddress("[2001:4860::8888]:6881")!;

describe("ut_holepunch codec", () => {
	it("encodes the BEP 55 layout: msg_type, addr_type, addr, port, err_code", () => {
		expect([...encodeHolepunch({ type: "rendezvous", address: v4 })]).toEqual([0, 0, 93, 184, 216, 34, 0xc8, 0xd5, 0, 0, 0, 0]);
		expect([...encodeHolepunch({ type: "connect", address: v4 })].slice(0, 2)).toEqual([1, 0]);
		const err = encodeHolepunch({ type: "error", address: v6, error: HolepunchError.NO_SUPPORT });
		expect(err.length).toBe(24);
		expect([...err.slice(0, 2)]).toEqual([2, 1]);
		expect([...err.slice(18)]).toEqual([0x1a, 0xe1, 0, 0, 0, 3]);
	});

	it("decodes what it encodes, IPv4 and IPv6", () => {
		const messages: HolepunchMessage[] = [
			{ type: "rendezvous", address: v4 },
			{ type: "connect", address: v6 },
			{ type: "error", address: v4, error: HolepunchError.NO_SUCH_PEER },
			{ type: "error", address: v6, error: 0xdeadbeef },
		];
		for (const m of messages) expect(decodeHolepunch(encodeHolepunch(m))).toEqual(m);
	});

	it("ignores the error code of a non-error message", () => {
		const bytes = encodeHolepunch({ type: "connect", address: v4 });
		bytes[11] = 9;
		expect(decodeHolepunch(bytes)).toEqual({ type: "connect", address: v4 });
	});

	it("takes rendezvous and connect without the error code, as libtorrent sends them", () => {
		// libtorrent 2.0 (bt_peer_connection::write_holepunch_msg) writes err_code only in `error`.
		expect(decodeHolepunch(Uint8Array.of(1, 0, 93, 184, 216, 34, 0xc8, 0xd5))).toEqual({ type: "connect", address: v4 });
		expect(decodeHolepunch(encodeHolepunch({ type: "rendezvous", address: v6 }).slice(0, 20))).toEqual({ type: "rendezvous", address: v6 });
		// An error says which: its code must be there.
		expect(() => decodeHolepunch(Uint8Array.of(2, 0, 93, 184, 216, 34, 0xc8, 0xd5))).toThrow(ExtensionError);
	});

	it("rejects every malformed form", () => {
		const good = encodeHolepunch({ type: "connect", address: v4 });
		const bad: Uint8Array[] = [
			new Uint8Array(0),
			Uint8Array.of(1),
			good.slice(0, 11), // short
			Uint8Array.of(...good, 0), // trailing byte
			Uint8Array.of(1, 1, ...good.slice(2)), // says IPv6, IPv4 length
			Uint8Array.of(1, 2, ...good.slice(2)), // unknown addr_type
			Uint8Array.of(3, ...good.slice(1)), // unknown msg_type
			Uint8Array.of(...good.slice(0, 6), 0, 0, ...good.slice(8)), // port 0
		];
		for (const b of bad) expect(() => decodeHolepunch(b), `${[...b]}`).toThrow(ExtensionError);
		expect(() => encodeHolepunch({ type: "connect", address: { family: 4, ip: new Uint8Array(16), port: 1 } })).toThrow(RangeError);
		expect(() => encodeHolepunch({ type: "connect", address: { ...v4, port: 0 } })).toThrow(RangeError);
	});

	it("names the error codes", () => {
		expect([1, 2, 3, 4, 0, 99].map(holepunchErrorName)).toEqual(["no_such_peer", "not_connected", "no_support", "no_self", "error", "error"]);
	});
});

const pex = (key: string, flags = PEX_HOLEPUNCH): PexAddress => ({ ...parseAddress(key)!, flags });
const T1 = "93.184.216.34:51413";
const T2 = "93.184.216.35:51413";
const yes = () => true;

describe("HolePuncher", () => {
	it("knows only the targets a relay named with the holepunch flag, minus those it dropped", () => {
		const hp = new HolePuncher({ intervalMs: 0 });
		hp.learn("in:r1", [pex(T1, 0x10), pex(T2)]);
		expect(hp.next(0, yes, yes)).toMatchObject({ relay: "in:r1", target: T2, phase: "rendezvous" });
		hp.learn("in:r2", [pex(T1)]);
		hp.learn("in:r2", [], [pex(T1)]);
		expect(hp.next(0, yes, yes)).toBeUndefined();
	});

	it("bounds punches in flight, paces them and caps them per session", () => {
		const hp = new HolePuncher({ maxInFlight: 2, intervalMs: 1_000, maxPerSession: 3 });
		hp.learn("in:r", ["93.184.216.1:7000", "93.184.216.2:7000", "93.184.216.3:7000", "93.184.216.4:7000"].map((k) => pex(k)));
		expect(hp.next(0, yes, yes)).toBeDefined();
		expect(hp.next(500, yes, yes)).toBeUndefined(); // paced
		const second = hp.next(1_000, yes, yes)!;
		expect(hp.next(5_000, yes, yes)).toBeUndefined(); // two in flight
		hp.fail(second.target, "timeout");
		expect(hp.next(5_000, yes, yes)).toBeDefined();
		hp.fail("93.184.216.1:7000", "timeout");
		expect(hp.next(9_000, yes, yes)).toBeUndefined(); // three per session
		expect(hp.stats.attempted).toBe(3);
	});

	it("asks each relay once per target, and a target at most maxAttempts times", () => {
		const hp = new HolePuncher({ intervalMs: 0, maxAttempts: 2 });
		for (const r of ["in:a", "in:b", "in:c"]) hp.learn(r, [pex(T1)]);
		const seen: string[] = [];
		for (let i = 0; i < 3; i++) {
			const p = hp.next(i, yes, yes);
			if (!p) break;
			seen.push(p.relay);
			hp.fail(p.target, "timeout");
		}
		expect(seen).toEqual(["in:a", "in:b"]);
	});

	it("skips relays and targets the session refuses", () => {
		const hp = new HolePuncher({ intervalMs: 0 });
		hp.learn("in:a", [pex(T1), pex(T2)]);
		expect(hp.next(0, (r) => r !== "in:a", yes)).toBeUndefined();
		expect(hp.next(0, yes, (t) => t === T2)?.target).toBe(T2);
	});

	it("follows a connect only from the relay asked, once", () => {
		const hp = new HolePuncher({ intervalMs: 0 });
		hp.learn("in:a", [pex(T1)]);
		hp.next(0, yes, yes);
		expect(hp.onConnect("in:b", T1, 1)).toBe(false);
		expect(hp.onConnect("in:a", T2, 1)).toBe(false);
		expect(hp.onConnect("in:a", T1, 1)).toBe(true);
		expect(hp.pending(T1)?.phase).toBe("dialling");
		expect(hp.onConnect("in:a", T1, 2)).toBe(false);
		expect(hp.stats.unsolicited).toBe(3);
		hp.succeeded(T1);
		expect(hp.stats.connected).toBe(1);
		expect(hp.inFlight).toBe(0);
	});

	it("an error marks the punch failed; NoSuchPeer forgets the target at that relay, NoSupport everywhere", () => {
		const hp = new HolePuncher({ intervalMs: 0, maxAttempts: 4 });
		hp.learn("in:a", [pex(T1), pex(T2)]);
		hp.learn("in:b", [pex(T1), pex(T2)]);
		const p = hp.next(0, yes, (t) => t === T1)!;
		expect(hp.onError("in:b", T1, HolepunchError.NO_SUCH_PEER)).toBeUndefined(); // not that relay's
		expect(hp.onError(p.relay, T1, HolepunchError.NO_SUCH_PEER)).toBe("no_such_peer");
		expect(hp.next(1, yes, (t) => t === T1)?.relay).toBe("in:b");
		expect(hp.onError("in:b", T1, HolepunchError.NO_SUPPORT)).toBe("no_support");
		expect(hp.next(2, yes, (t) => t === T1)).toBeUndefined();
		expect(hp.stats.failed).toEqual({ no_such_peer: 1, no_support: 1 });
		// Another target is still punched.
		expect(hp.next(3, yes, yes)?.target).toBe(T2);
	});

	it("an unanswered rendezvous times out; a relay that leaves fails its punches", () => {
		const hp = new HolePuncher({ intervalMs: 0, answerTimeoutMs: 10_000 });
		hp.learn("in:a", [pex(T1)]);
		hp.learn("in:b", [pex(T2)]);
		hp.next(0, yes, yes);
		hp.next(0, yes, yes);
		hp.onConnect("in:b", T2, 5_000);
		expect(hp.expired(9_999)).toEqual([]);
		expect(hp.expired(10_000).map((p) => p.target)).toEqual([T1]);
		// Dialling already: the relay is no longer needed.
		expect(hp.forgetRelay("in:b")).toEqual([]);
		expect(hp.pending(T2)?.phase).toBe("dialling");
		hp.learn("in:c", [pex(T1)]);
		hp.next(20_000, yes, yes);
		expect(hp.forgetRelay("in:c").map((p) => p.target)).toEqual([T1]);
		expect(hp.stats.failed).toEqual({ timeout: 1, relay_closed: 1 });
	});

	it("bounds what it remembers per relay", () => {
		const hp = new HolePuncher({ intervalMs: 0, maxKnownPerRelay: 2 });
		hp.learn("in:a", ["93.184.216.1:7000", "93.184.216.2:7000", "93.184.216.3:7000"].map((k) => pex(k)));
		const targets: string[] = [];
		for (let i = 0; i < 5; i++) {
			const p = hp.next(i, yes, yes);
			if (p) targets.push(p.target);
		}
		expect(targets.sort()).toEqual(["93.184.216.2:7000", "93.184.216.3:7000"]);
	});
});

class Pipe extends BufferedTransport {
	readonly remote = { reserved: Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04), peerId: new Uint8Array(20) };
	readonly local = { reserved: Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04) };
	sent: Uint8Array[] = [];
	send(bytes: Uint8Array): void {
		this.sent.push(bytes);
	}
	sendLazy(bytes: Uint8Array): void {
		this.sent.push(bytes);
	}
	close(): void {
		this.finish();
	}
	push(bytes: Uint8Array): void {
		this.deliver(bytes);
	}
}

const extended = (id: number, payload: Uint8Array) => encodeMessage(MSG.EXTENDED, Uint8Array.of(id, ...payload));

/** The ut_holepunch messages written to `pipe` with the peer's id `id`. */
function holepunchesSent(pipe: Pipe, id: number): HolepunchMessage[] {
	const out: HolepunchMessage[] = [];
	for (const b of pipe.sent) if (b[4] === MSG.EXTENDED && b[5] === id) out.push(decodeHolepunch(b.slice(6)));
	return out;
}

describe("PeerConnection and ut_holepunch", () => {
	it("advertises ut_holepunch, answers a rendezvous with NotConnected and hands over connect and error", async () => {
		const pipe = new Pipe();
		const got: HolepunchMessage[] = [];
		const peer = new PeerConnection(pipe, { keepAliveMs: 0, events: { holepunch: (m) => got.push(m) } });
		await new Promise((r) => setTimeout(r, 0));
		expect(LOCAL_EXTENSIONS.ut_holepunch).toBe(3);
		pipe.push(extended(0, encode({ m: { ut_holepunch: 9 } })));
		expect(peer.supports("ut_holepunch")).toBe(true);
		const id = LOCAL_EXTENSIONS.ut_holepunch;
		pipe.push(extended(id, encodeHolepunch({ type: "rendezvous", address: v4 })));
		expect(holepunchesSent(pipe, 9)).toEqual([{ type: "error", address: v4, error: HolepunchError.NOT_CONNECTED }]);
		pipe.push(extended(id, encodeHolepunch({ type: "connect", address: v6 })));
		pipe.push(extended(id, Uint8Array.of(1, 0, 1))); // malformed: ignored
		pipe.push(extended(id, encodeHolepunch({ type: "error", address: v4, error: 4 })));
		expect(got).toEqual([
			{ type: "connect", address: v6 },
			{ type: "error", address: v4, error: 4 },
		]);
		expect(peer.closed).toBe(false);
		expect(peer.sendHolepunch({ type: "rendezvous", address: v6 })).toBe(true);
		expect(holepunchesSent(pipe, 9).at(-1)).toEqual({ type: "rendezvous", address: v6 });
		peer.close();
	});

	it("sends nothing to a peer that does not support it", async () => {
		const pipe = new Pipe();
		const peer = new PeerConnection(pipe, { keepAliveMs: 0 });
		await new Promise((r) => setTimeout(r, 0));
		pipe.push(extended(0, encode({ m: { ut_pex: 2 } })));
		const before = pipe.sent.length;
		expect(peer.sendHolepunch({ type: "rendezvous", address: v4 })).toBe(false);
		pipe.push(extended(LOCAL_EXTENSIONS.ut_holepunch, encodeHolepunch({ type: "rendezvous", address: v4 })));
		expect(pipe.sent.length).toBe(before);
		peer.close();
	});
});
