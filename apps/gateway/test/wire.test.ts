import { describe, expect, it } from "vitest";
import { decodeBencode, handshake, message, MSG, peerId, requests, SetupParser, WireError } from "../src/wire";

const INFO_HASH = Uint8Array.from({ length: 20 }, (_, i) => i + 1);

function peerHandshake(infoHash = INFO_HASH, { fast = true } = {}): Uint8Array {
	const hs = handshake(infoHash, new Uint8Array(20));
	if (!fast) hs[27] = 0;
	return hs;
}

function ext(dict: string): Uint8Array {
	const body = new TextEncoder().encode(dict);
	const payload = new Uint8Array(1 + body.length);
	payload.set(body, 1);
	return message(MSG.EXTENDED, payload);
}

function concat(...parts: Uint8Array[]): Uint8Array {
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
}

describe("handshake", () => {
	it("sets the extension and fast bits", () => {
		const hs = handshake(INFO_HASH, peerId());
		expect(hs.length).toBe(68);
		expect(hs[0]).toBe(19);
		expect(new TextDecoder().decode(hs.subarray(1, 20))).toBe("BitTorrent protocol");
		expect(hs[25] & 0x10).toBe(0x10);
		expect(hs[27] & 0x04).toBe(0x04);
		expect(hs.subarray(28, 48)).toEqual(INFO_HASH);
		expect(new TextDecoder().decode(hs.subarray(48, 56))).toBe("-MW0100-");
	});
});

describe("requests", () => {
	it("encodes one 17-byte request per block", () => {
		const buf = requests([
			[1, 0, 16384],
			[1, 16384, 100],
		]);
		expect(buf.length).toBe(34);
		const view = new DataView(buf.buffer);
		expect([view.getUint32(0), buf[4], view.getUint32(5), view.getUint32(9), view.getUint32(13)]).toEqual([
			13, 6, 1, 0, 16384,
		]);
		expect([view.getUint32(17 + 9), view.getUint32(17 + 13)]).toEqual([16384, 100]);
	});
});

describe("SetupParser", () => {
	it("reads handshake, extension handshake, bitfield and unchoke across arbitrary splits", () => {
		const stream = concat(
			peerHandshake(),
			ext("d1:md11:ut_metadatai2ee4:reqqi500e1:v13:qBittorrent/5e"),
			message(MSG.BITFIELD, Uint8Array.of(0b1010_0000)),
			message(MSG.HAVE, Uint8Array.of(0, 0, 0, 1)),
			message(MSG.UNCHOKE),
		);
		for (let step = 1; step <= stream.length; step += 7) {
			const parser = new SetupParser(INFO_HASH);
			for (let at = 0; at < stream.length; at += step) parser.push(stream.subarray(at, at + step));
			expect(parser.handshaken).toBe(true);
			expect(parser.fast).toBe(true);
			expect(parser.unchoked).toBe(true);
			expect(parser.reqq).toBe(500);
			expect(parser.client).toBe("qBittorrent/5");
			expect([0, 1, 2, 3].map((i) => parser.hasPiece(i))).toEqual([true, true, true, false]);
		}
	});

	it("treats have-all as every piece", () => {
		const parser = new SetupParser(INFO_HASH);
		parser.push(concat(peerHandshake(), message(MSG.HAVE_ALL)));
		expect(parser.hasPiece(123456)).toBe(true);
		expect(parser.unchoked).toBe(false);
	});

	it("stays choked, and notes a peer without the fast extension", () => {
		const parser = new SetupParser(INFO_HASH);
		parser.push(concat(peerHandshake(INFO_HASH, { fast: false }), message(MSG.CHOKE)));
		expect(parser.handshaken).toBe(true);
		expect(parser.fast).toBe(false);
		expect(parser.unchoked).toBe(false);
	});

	it("rejects a peer for another torrent", () => {
		const parser = new SetupParser(INFO_HASH);
		expect(() => parser.push(peerHandshake(new Uint8Array(20)))).toThrowError(WireError);
		try {
			new SetupParser(INFO_HASH).push(peerHandshake(new Uint8Array(20)));
		} catch (e) {
			expect((e as WireError).code).toBe("infohash_mismatch");
		}
	});

	it("rejects non-BitTorrent data and oversized messages", () => {
		const garbage = new TextEncoder().encode("HTTP/1.1 400 Bad Request\r\n".padEnd(68, "x"));
		expect(() => new SetupParser(INFO_HASH).push(garbage)).toThrowError("bad_handshake");
		const huge = new Uint8Array(4);
		new DataView(huge.buffer).setUint32(0, 10 * 1024 * 1024);
		expect(() => new SetupParser(INFO_HASH).push(concat(peerHandshake(), huge))).toThrowError("protocol");
	});
});

describe("decodeBencode", () => {
	it("decodes nested values", () => {
		const v = decodeBencode(new TextEncoder().encode("d1:ai-3e1:bl1:xi7eee")) as Map<string, unknown>;
		expect(v.get("a")).toBe(-3);
		const list = v.get("b") as unknown[];
		expect(new TextDecoder().decode(list[0] as Uint8Array)).toBe("x");
		expect(list[1]).toBe(7);
	});

	it("returns undefined for malformed or absurdly deep input", () => {
		expect(decodeBencode(new TextEncoder().encode("d1:a"))).toBeUndefined();
		expect(decodeBencode(new TextEncoder().encode("5:ab"))).toBeUndefined();
		expect(decodeBencode(new TextEncoder().encode("l".repeat(100) + "e".repeat(100)))).toBeUndefined();
	});
});
