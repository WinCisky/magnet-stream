import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
	AnnounceStatus,
	CloseReason,
	MAX_ANNOUNCE_TRACKERS,
	MAX_TRACKER_URL_LENGTH,
	FailReason,
	FrameDecoder,
	FrameError,
	HEADER_LENGTH,
	Transport,
	decodeMessage,
	encodeFrame,
	encodeFrames,
	frameFromJson,
	frameToJson,
	fromHex,
	type Frame,
	type JsonFrame,
} from "../src/index.ts";

const infoHash = Uint8Array.from({ length: 20 }, (_, i) => i + 1);
const peerId = Uint8Array.from({ length: 20 }, (_, i) => 100 + i);
const reserved = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);

const samples: Frame[] = [
	{ type: "open", conn: 1, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(1, 2, 3, 4), port: 6881 } },
	{
		type: "open",
		conn: 0x7fff,
		infoHash,
		reserved,
		peerId,
		address: { family: 6, ip: fromHex("2a0100000000000000000000000000ff"), port: 65535 },
	},
	{ type: "opened", conn: 5, reserved, peerId },
	{ type: "failed", conn: 6, reason: FailReason.NOT_BITTORRENT },
	{ type: "data", conn: 7, payload: Uint8Array.from({ length: 1000 }, (_, i) => i & 0xff) },
	{ type: "close", conn: 8, reason: CloseReason.PROTOCOL_ERROR },
	{ type: "accept", conn: 0xffff, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(8, 8, 4, 4), port: 1024 } },
	{ type: "open", conn: 2, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(1, 2, 3, 4), port: 6881 }, transport: Transport.UTP },
	{ type: "accept", conn: 0x8001, infoHash, reserved, peerId, address: { family: 6, ip: fromHex("2a0100000000000000000000000000ff"), port: 7000 }, transport: Transport.UTP },
	{ type: "ping", conn: 0, payload: Uint8Array.of(1, 2, 3) },
	{ type: "announce", conn: 0, infoHash, reserved, peerId, trackers: ["udp://t.example:6969/announce", "http://a.example/announce?x=1"] },
	{ type: "announce", conn: 0, infoHash, reserved, peerId, trackers: [] },
	{ type: "announced", conn: 0, infoHash, status: AnnounceStatus.LIMIT_REACHED },
	{ type: "withdraw", conn: 0, infoHash },
];

function json(frames: Frame[]) {
	return frames.map(frameToJson);
}

describe("round-trip", () => {
	for (const frame of samples) {
		it(`${frame.type} (conn ${frame.conn})`, () => {
			const result = decodeMessage(encodeFrame(frame));
			expect(result.ok).toBe(true);
			expect(json(result.frames)).toEqual(json([frame]));
		});
	}

	it("several frames in one message", () => {
		const message = encodeFrames(samples);
		const result = decodeMessage(message);
		expect(result.ok).toBe(true);
		expect(json(result.frames)).toEqual(json(samples));
	});

	it("uses the header layout [type u8][conn u16][length u32]", () => {
		const message = encodeFrame({ type: "data", conn: 0x0102, payload: Uint8Array.of(9, 9, 9) });
		expect(Array.from(message.subarray(0, HEADER_LENGTH))).toEqual([0x04, 0x01, 0x02, 0, 0, 0, 3]);
	});
});

describe("transport of open and accept", () => {
	const tcp: Frame = { type: "open", conn: 1, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(1, 2, 3, 4), port: 6881 } };

	it("TCP is the byte left out, so v1 frames are unchanged", () => {
		expect(encodeFrame(tcp).length).toBe(HEADER_LENGTH + 55);
		expect(encodeFrame({ ...tcp, transport: Transport.TCP })).toEqual(encodeFrame(tcp));
		const decoded = decodeMessage(encodeFrame(tcp));
		expect(decoded.ok && "transport" in decoded.frames[0]).toBe(false);
	});

	it("uTP adds one trailing byte", () => {
		const bytes = encodeFrame({ ...tcp, transport: Transport.UTP });
		expect(bytes.length).toBe(HEADER_LENGTH + 56);
		expect(bytes[bytes.length - 1]).toBe(Transport.UTP);
		const decoded = decodeMessage(bytes);
		expect(decoded.ok && decoded.frames[0]).toMatchObject({ type: "open", transport: Transport.UTP });
	});

	it("an unknown transport decodes (the gateway fails it), an explicit 0 does not", () => {
		const bytes = encodeFrame({ ...tcp, transport: 7 });
		const decoded = decodeMessage(bytes);
		expect(decoded.ok && decoded.frames[0]).toMatchObject({ transport: 7 });
		bytes[bytes.length - 1] = 0;
		const zero = decodeMessage(bytes);
		expect(zero.ok ? "ok" : zero.error.code).toBe("bad_payload");
		expect(() => encodeFrame({ ...tcp, transport: 256 })).toThrow(RangeError);
	});
});

describe("decoder errors", () => {
	const code = (bytes: Uint8Array, limits = {}) => {
		const r = decodeMessage(bytes, limits);
		return r.ok ? "ok" : r.error.code;
	};

	it("rejects unknown types, oversize lengths, truncation and bad open payloads", () => {
		expect(code(Uint8Array.of(0x42, 0, 1, 0, 0, 0, 0))).toBe("unknown_type");
		expect(code(Uint8Array.of(0x04, 0, 1, 0, 0, 0, 200, ...new Uint8Array(200)), { maxPayload: 100 })).toBe("too_long");
		expect(code(Uint8Array.of(0x04, 0, 1, 0, 0, 0, 5, 1, 2))).toBe("truncated");
		const open = encodeFrame(samples[0]);
		open[HEADER_LENGTH + 48] = 7; // address family
		expect(code(open)).toBe("bad_payload");
	});

	it("returns a typed FrameError with the offending offset", () => {
		const message = encodeFrames([samples[4], samples[5]]);
		const bad = Uint8Array.of(...message, 0x0b, 0, 1, 0, 0, 0, 0);
		const r = decodeMessage(bad);
		expect(r.ok).toBe(false);
		if (r.ok) return;
		expect(r.error).toBeInstanceOf(FrameError);
		expect(r.error.offset).toBe(message.length);
		expect(r.frames).toHaveLength(2);
	});

	it("rejects a length over the limit before waiting for the payload", () => {
		const decoder = new FrameDecoder({ maxPayload: 1024 });
		const r = decoder.push(Uint8Array.of(0x04, 0, 1, 0xff, 0xff, 0xff, 0xff));
		expect(r.ok ? "ok" : r.error.code).toBe("too_long");
		expect(decoder.buffered).toBe(0);
	});

	it("refuses to encode invalid frames", () => {
		expect(() => encodeFrame({ type: "data", conn: 0, payload: Uint8Array.of(1) })).toThrow(RangeError);
		expect(() => encodeFrame({ ...samples[0], conn: 0x8000 } as Frame)).toThrow(RangeError);
		expect(() => encodeFrame({ type: "ping", conn: 0, payload: new Uint8Array(9) })).toThrow(RangeError);
		expect(() => encodeFrames([])).toThrow(RangeError);
		const announce = samples.find((f) => f.type === "announce") as Frame & { type: "announce" };
		expect(() => encodeFrame({ ...announce, trackers: ["has space"] })).toThrow(RangeError);
		expect(() => encodeFrame({ ...announce, trackers: [""] })).toThrow(RangeError);
		expect(() => encodeFrame({ ...announce, trackers: ["u".repeat(MAX_TRACKER_URL_LENGTH + 1)] })).toThrow(RangeError);
		expect(() => encodeFrame({ ...announce, trackers: Array<string>(MAX_ANNOUNCE_TRACKERS + 1).fill("udp://t:1") })).toThrow(RangeError);
		expect(() => encodeFrame({ ...announce, trackers: ["http://é.example"] })).toThrow(RangeError);
	});

	it("accepts the longest announce the limits allow", () => {
		const announce = samples.find((f) => f.type === "announce") as Frame & { type: "announce" };
		const trackers = Array<string>(MAX_ANNOUNCE_TRACKERS).fill("u".repeat(MAX_TRACKER_URL_LENGTH));
		const r = decodeMessage(encodeFrame({ ...announce, trackers }));
		expect(r.ok && (r.frames[0] as { trackers: string[] }).trackers).toEqual(trackers);
	});
});

describe("FrameDecoder (incremental)", () => {
	it("decodes the same frames whatever the chunking", () => {
		const message = encodeFrames(samples);
		for (const size of [1, 2, 3, 7, 13, 64, 999, message.length]) {
			const decoder = new FrameDecoder();
			const frames: Frame[] = [];
			for (let at = 0; at < message.length; at += size) {
				const r = decoder.push(message.subarray(at, at + size));
				expect(r.ok).toBe(true);
				frames.push(...r.frames);
			}
			expect(json(frames)).toEqual(json(samples));
			expect(decoder.buffered).toBe(0);
		}
	});

	it("returns copies, independent of later input", () => {
		const decoder = new FrameDecoder();
		const chunk = encodeFrame({ type: "data", conn: 1, payload: Uint8Array.of(1, 2, 3) });
		const r = decoder.push(chunk);
		chunk.fill(0);
		expect(r.ok && Array.from((r.frames[0] as { payload: Uint8Array }).payload)).toEqual([1, 2, 3]);
	});

	it("stays failed after an error, with stream offsets", () => {
		const decoder = new FrameDecoder();
		decoder.push(encodeFrame(samples[3]));
		const r1 = decoder.push(Uint8Array.of(0x00));
		expect(r1.ok).toBe(false);
		if (!r1.ok) expect(r1.error.offset).toBe(encodeFrame(samples[3]).length);
		const r2 = decoder.push(encodeFrame(samples[3]));
		expect(r2.ok).toBe(false);
	});
});

describe("fixtures", () => {
	interface Vectors {
		valid: { name: string; file: string; hex: string; frames: JsonFrame[] }[];
		invalid: { name: string; file: string; hex: string; error: string; limits?: object }[];
	}
	const dir = new URL("../fixtures/", import.meta.url);
	const vectors = JSON.parse(readFileSync(new URL("vectors.json", dir), "utf8")) as Vectors;

	it("has vectors for every frame type", () => {
		const types = new Set(vectors.valid.flatMap((v) => v.frames.map((f) => f.type)));
		expect([...types].sort()).toEqual(["accept", "announce", "announced", "close", "data", "failed", "open", "opened", "ping", "withdraw"]);
	});

	for (const v of vectors.valid) {
		it(`decodes and re-encodes ${v.name}`, () => {
			const bytes = new Uint8Array(readFileSync(new URL(v.file, dir)));
			expect(bytes).toEqual(fromHex(v.hex));
			const r = decodeMessage(bytes);
			expect(r.ok).toBe(true);
			expect(json(r.frames)).toEqual(v.frames);
			expect(encodeFrames(v.frames.map(frameFromJson))).toEqual(bytes);
		});
	}

	for (const v of vectors.invalid) {
		it(`rejects ${v.name} with ${v.error}`, () => {
			const bytes = new Uint8Array(readFileSync(new URL(v.file, dir)));
			const r = decodeMessage(bytes, v.limits);
			expect(r.ok ? "ok" : r.error.code).toBe(v.error);
		});
	}
});
