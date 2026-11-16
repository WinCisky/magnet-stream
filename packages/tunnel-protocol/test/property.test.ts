// Property tests: the decoder never throws, never loops, and agrees with
// the encoder on every well-formed message.

import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { FrameDecoder, decodeMessage, encodeFrames, frameToJson, type Frame } from "../src/index.ts";

const bytes = (n: number) => fc.uint8Array({ minLength: n, maxLength: n });
const clientConn = fc.integer({ min: 1, max: 0x7fff });
const anyConn = fc.integer({ min: 1, max: 0xffff });
const address = fc.oneof(
	fc.record({ family: fc.constant(4 as const), ip: bytes(4), port: fc.integer({ min: 1, max: 65535 }) }),
	fc.record({ family: fc.constant(6 as const), ip: bytes(16), port: fc.integer({ min: 1, max: 65535 }) }),
);

const frame: fc.Arbitrary<Frame> = fc.oneof(
	fc.record({ type: fc.constant("open" as const), conn: clientConn, infoHash: bytes(20), reserved: bytes(8), peerId: bytes(20), address }),
	fc.record({ type: fc.constant("opened" as const), conn: anyConn, reserved: bytes(8), peerId: bytes(20) }),
	fc.record({ type: fc.constant("failed" as const), conn: anyConn, reason: fc.integer({ min: 0, max: 255 }) }),
	fc.record({ type: fc.constant("data" as const), conn: anyConn, payload: fc.uint8Array({ minLength: 1, maxLength: 300 }) }),
	fc.record({ type: fc.constant("close" as const), conn: anyConn, reason: fc.integer({ min: 0, max: 255 }) }),
	fc.record({
		type: fc.constant("accept" as const),
		conn: fc.integer({ min: 0x8000, max: 0xffff }),
		infoHash: bytes(20),
		reserved: bytes(8),
		peerId: bytes(20),
		address,
	}),
	fc.record({ type: fc.constant("ping" as const), conn: fc.constant(0 as const), payload: fc.uint8Array({ maxLength: 8 }) }),
);

describe("properties", () => {
	it("decode(encode(frames)) == frames", () => {
		fc.assert(
			fc.property(fc.array(frame, { minLength: 1, maxLength: 20 }), (frames) => {
				const r = decodeMessage(encodeFrames(frames));
				expect(r.ok).toBe(true);
				expect(r.frames.map(frameToJson)).toEqual(frames.map(frameToJson));
			}),
			{ numRuns: 500 },
		);
	});

	it("random bytes never throw and finish quickly", () => {
		fc.assert(
			fc.property(fc.uint8Array({ maxLength: 2048 }), (input) => {
				const started = performance.now();
				const r = decodeMessage(input, { maxPayload: 4096, maxFrames: 64 });
				expect(typeof r.ok).toBe("boolean");
				expect(r.frames.length).toBeLessThanOrEqual(64);
				expect(performance.now() - started).toBeLessThan(250);
			}),
			{ numRuns: 3000 },
		);
	});

	it("mutated valid messages never throw", () => {
		fc.assert(
			fc.property(
				fc.array(frame, { minLength: 1, maxLength: 5 }),
				fc.array(fc.tuple(fc.nat(), fc.integer({ min: 0, max: 255 })), { maxLength: 8 }),
				fc.nat(),
				(frames, flips, cut) => {
					const message = encodeFrames(frames).slice();
					for (const [at, value] of flips) message[at % message.length] = value;
					const truncated = message.subarray(0, (cut % message.length) + 1);
					expect(() => decodeMessage(truncated)).not.toThrow();
					const decoder = new FrameDecoder({ maxPayload: 1024 });
					expect(() => decoder.push(truncated)).not.toThrow();
				},
			),
			{ numRuns: 2000 },
		);
	});

	it("streaming random chunks never throws and never buffers past one frame", () => {
		fc.assert(
			fc.property(fc.array(fc.uint8Array({ maxLength: 64 }), { maxLength: 50 }), (chunks) => {
				const decoder = new FrameDecoder({ maxPayload: 256 });
				for (const c of chunks) {
					decoder.push(c);
					expect(decoder.buffered).toBeLessThanOrEqual(256 + 7);
				}
			}),
			{ numRuns: 1000 },
		);
	});
});
