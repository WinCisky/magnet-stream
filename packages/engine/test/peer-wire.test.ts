import { describe, expect, it } from "vitest";
import {
	BufferedTransport,
	MSG,
	PeerConnection,
	WireParser,
	WireProtocolError,
	encodeMessage,
	encodeRequest,
	keepAlive,
	type HandshakeInfo,
	type WireMessage,
} from "../src/index.ts";

const geometry = { pieceCount: 10, pieceLength: 32 * 1024, totalLength: 9 * 32 * 1024 + 1000 };

function u32(n: number): number[] {
	return [n >>> 24, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff];
}
function piece(index: number, begin: number, data: number[]): Uint8Array {
	return encodeMessage(MSG.PIECE, Uint8Array.of(...u32(index), ...u32(begin), ...data));
}
function concat(...parts: Uint8Array[]): Uint8Array {
	return Uint8Array.from(parts.flatMap((p) => [...p]));
}

const stream = concat(
	encodeMessage(MSG.BITFIELD, Uint8Array.of(0xff, 0xc0)),
	keepAlive(),
	encodeMessage(MSG.HAVE, Uint8Array.of(...u32(3))),
	encodeMessage(MSG.UNCHOKE),
	piece(2, 16384, [1, 2, 3, 4]),
	encodeMessage(MSG.REJECT, Uint8Array.of(...u32(1), ...u32(0), ...u32(16384))),
	encodeMessage(MSG.EXTENDED, Uint8Array.of(0, 0x64, 0x65)),
	encodeMessage(MSG.CHOKE),
);

function parseIn(chunks: number): WireMessage[] {
	const parser = new WireParser(geometry);
	const out: WireMessage[] = [];
	const size = Math.ceil(stream.length / chunks);
	for (let at = 0; at < stream.length; at += size) {
		// Copy views out immediately, as consumers must.
		for (const m of parser.push(stream.subarray(at, at + size))) out.push(m.type === "piece" ? { ...m, data: m.data.slice() } : m);
	}
	return out;
}

describe("WireParser", () => {
	it("gives the same messages whatever the chunk boundaries", async () => {
		const whole = parseIn(1);
		expect(whole.map((m) => m.type)).toEqual(["bitfield", "keepalive", "have", "unchoke", "piece", "reject", "extended", "choke"]);
		for (let chunks = 2; chunks <= stream.length; chunks++) expect(parseIn(chunks)).toEqual(whole);
	});

	const bad: [string, Uint8Array][] = [
		["huge length", Uint8Array.of(0x7f, 0, 0, 0, 7)],
		["have out of range", encodeMessage(MSG.HAVE, Uint8Array.of(...u32(10)))],
		["have with a short payload", encodeMessage(MSG.HAVE, Uint8Array.of(0, 0, 1))],
		["bitfield of the wrong size", encodeMessage(MSG.BITFIELD, Uint8Array.of(0xff))],
		["bitfield with spare bits", encodeMessage(MSG.BITFIELD, Uint8Array.of(0xff, 0xc1))],
		["choke with a payload", encodeMessage(MSG.CHOKE, Uint8Array.of(1))],
		["block past the end of the piece", piece(0, 32 * 1024 - 2, [1, 2, 3])],
		["block past the last (short) piece", piece(9, 900, Array(101).fill(0))],
		["block larger than 16 KiB", piece(0, 0, Array(16 * 1024 + 1).fill(0))],
		["piece for an unknown index", piece(10, 0, [1])],
		["request of the wrong size", encodeMessage(MSG.REQUEST, Uint8Array.of(1, 2, 3))],
	];
	for (const [name, bytes] of bad) {
		it(`rejects ${name}`, () => {
			expect(() => new WireParser(geometry).push(bytes)).toThrow(WireProtocolError);
		});
	}

	it("encodes requests", async () => {
		expect([...encodeRequest(MSG.REQUEST, 1, 16384, 16384)]).toEqual([0, 0, 0, 13, 6, ...u32(1), ...u32(16384), ...u32(16384)]);
	});
});

class MemoryTransport extends BufferedTransport {
	readonly remote: HandshakeInfo;
	readonly local = { reserved: Uint8Array.of(0, 0, 0, 0, 0, 0, 0, 0x04) };
	sent: Uint8Array[] = [];
	constructor(remoteFast = true) {
		super();
		this.remote = { reserved: Uint8Array.of(0, 0, 0, 0, 0, 0, 0, remoteFast ? 0x04 : 0), peerId: new Uint8Array(20) };
	}
	send(b: Uint8Array) {
		this.sent.push(b);
	}
	close() {
		this.finish();
	}
	push(b: Uint8Array) {
		this.deliver(b);
	}
	end(e?: Error) {
		this.finish(e);
	}
}

describe("PeerConnection", () => {
	it("tracks pieces and choke state, delivers only requested blocks", async () => {
		const t = new MemoryTransport();
		const blocks: string[] = [];
		const conn = new PeerConnection(t, { geometry, keepAliveMs: 0, events: { block: (p, b, d) => blocks.push(`${p}:${b}:${d.length}`) } });
		await Promise.resolve();
		expect(t.sent[0]).toEqual(encodeMessage(MSG.HAVE_NONE));
		t.push(encodeMessage(MSG.BITFIELD, Uint8Array.of(0b1010_0000, 0)));
		expect([conn.has(0), conn.has(1), conn.has(2)]).toEqual([true, false, true]);
		t.push(encodeMessage(MSG.HAVE, Uint8Array.of(...u32(1))));
		expect(conn.has(1)).toBe(true);
		t.push(encodeMessage(MSG.UNCHOKE));
		expect(conn.peerChoking).toBe(false);
		expect(conn.request([[0, 0, 4], [0, 4, 4]])).toBe(2);
		t.push(concat(piece(0, 0, [1, 2, 3, 4]), piece(0, 8, [9, 9, 9, 9]), piece(0, 4, [5, 6, 7, 8])));
		expect(blocks).toEqual(["0:0:4", "0:4:4"]);
		expect(conn.unsolicited).toBe(1);
		// Asked once, delivered once.
		t.push(piece(0, 0, [1, 2, 3, 4]));
		expect(blocks).toHaveLength(2);
	});

	it("without an uploader it ignores the peer's requests (as M1-06)", async () => {
		const t = new MemoryTransport();
		new PeerConnection(t, { geometry, keepAliveMs: 0 });
		await Promise.resolve();
		t.push(encodeMessage(MSG.INTERESTED));
		t.push(encodeRequest(MSG.REQUEST, 0, 0, 16384));
		expect(t.sent).toEqual([encodeMessage(MSG.HAVE_NONE)]);
	});

	it("closes on a protocol violation", async () => {
		const t = new MemoryTransport();
		let error: Error | undefined;
		const conn = new PeerConnection(t, { geometry, keepAliveMs: 0, events: { close: (e) => (error = e) } });
		await Promise.resolve();
		t.push(encodeMessage(MSG.HAVE, Uint8Array.of(...u32(99))));
		expect(conn.closed).toBe(true);
		expect(error).toBeInstanceOf(WireProtocolError);
		expect(t.closed).toBe(true);
	});

	it("without fast, a choke drops pending requests", async () => {
		const t = new MemoryTransport(false);
		const conn = new PeerConnection(t, { geometry, keepAliveMs: 0 });
		await Promise.resolve();
		expect(t.sent).toEqual([]); // no have-none without fast on both sides
		t.push(encodeMessage(MSG.UNCHOKE));
		conn.request([[0, 0, 16384]]);
		t.push(encodeMessage(MSG.CHOKE));
		expect(conn.outstandingRequests).toBe(0);
	});

	it("delivers bytes that arrived before it attached", async () => {
		const t = new MemoryTransport();
		t.push(encodeMessage(MSG.HAVE_ALL));
		t.end();
		let closed = false;
		const conn = new PeerConnection(t, { geometry, keepAliveMs: 0, events: { close: () => (closed = true) } });
		await Promise.resolve();
		expect(conn.has(9)).toBe(true);
		expect(closed).toBe(true);
	});
});
