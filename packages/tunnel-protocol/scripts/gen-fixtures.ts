// Regenerates fixtures/: one .bin per vector plus vectors.json describing
// what each must decode to (or which error it must raise). The vectors are
// the contract; regenerate only when the spec changes.
//
//   npm run fixtures -w packages/tunnel-protocol

import { mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { encodeFrames, type Frame } from "../src/codec.ts";
import { CloseReason, FailReason } from "../src/constants.ts";
import { frameToJson, fromHex, toHex } from "../src/json.ts";

const dir = new URL("../fixtures/", import.meta.url);
const bytes = (n: number, from: number) => Uint8Array.from({ length: n }, (_, i) => (from + i) & 0xff);
const infoHash = bytes(20, 0xa0);
const peerId = new TextEncoder().encode("-MS0001-abcdefghijkl");
const reserved = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);

const valid: { name: string; description: string; frames: Frame[] }[] = [
	{
		name: "open-ipv4",
		description: "open conn 1 to 203.0.113.7:6881",
		frames: [{ type: "open", conn: 1, infoHash, reserved, peerId, address: { family: 4, ip: Uint8Array.of(203, 0, 113, 7), port: 6881 } }],
	},
	{
		name: "open-ipv6",
		description: "open conn 0x7fff to [2001:db8::1]:51413",
		frames: [
			{
				type: "open",
				conn: 0x7fff,
				infoHash,
				reserved,
				peerId,
				address: { family: 6, ip: fromHex("20010db8000000000000000000000001"), port: 51413 },
			},
		],
	},
	{ name: "opened", description: "opened conn 1 with the remote's reserved bytes and peer id", frames: [{ type: "opened", conn: 1, reserved, peerId: bytes(20, 0x30) }] },
	{ name: "failed-timeout", description: "failed conn 2, reason timeout", frames: [{ type: "failed", conn: 2, reason: FailReason.TIMEOUT }] },
	{ name: "data", description: "data conn 1 carrying a peer-wire `interested`", frames: [{ type: "data", conn: 1, payload: Uint8Array.of(0, 0, 0, 1, 2) }] },
	{ name: "close-remote", description: "close conn 3, the remote closed", frames: [{ type: "close", conn: 3, reason: CloseReason.REMOTE_CLOSED }] },
	{
		name: "accept-ipv4",
		description: "accept conn 0x8000 from 198.51.100.20:40000",
		frames: [{ type: "accept", conn: 0x8000, infoHash, reserved, peerId: bytes(20, 0x40), address: { family: 4, ip: Uint8Array.of(198, 51, 100, 20), port: 40000 } }],
	},
	{ name: "ping-empty", description: "ping without payload", frames: [{ type: "ping", conn: 0, payload: new Uint8Array(0) }] },
	{ name: "ping-nonce", description: "ping with an 8-byte payload", frames: [{ type: "ping", conn: 0, payload: bytes(8, 1) }] },
	{
		name: "batch",
		description: "several frames in one message: data on two connections, a close and a ping",
		frames: [
			{ type: "data", conn: 1, payload: bytes(5, 0x10) },
			{ type: "data", conn: 2, payload: bytes(3, 0x20) },
			{ type: "close", conn: 1, reason: CloseReason.NORMAL },
			{ type: "ping", conn: 0, payload: new Uint8Array(0) },
		],
	},
];

const header = (type: number, conn: number, length: number) =>
	toHex(Uint8Array.of(type, conn >> 8, conn & 0xff, length >>> 24, (length >> 16) & 0xff, (length >> 8) & 0xff, length & 0xff));

const invalid: { name: string; description: string; hex: string; error: string; limits?: { maxPayload?: number; maxFrames?: number } }[] = [
	{ name: "err-empty", description: "empty message", hex: "", error: "empty" },
	{ name: "err-unknown-type", description: "type 0x09", hex: header(0x09, 1, 0), error: "unknown_type" },
	{ name: "err-type-zero", description: "type 0x00", hex: header(0x00, 1, 0), error: "unknown_type" },
	{ name: "err-truncated-header", description: "message ends inside a header", hex: header(0x04, 1, 1).slice(0, 8), error: "truncated" },
	{ name: "err-truncated-payload", description: "length says 4, 2 bytes follow", hex: header(0x04, 1, 4) + "aabb", error: "truncated" },
	{ name: "err-too-long", description: "length above maxPayload (16)", hex: header(0x04, 1, 17) + "00".repeat(17), error: "too_long", limits: { maxPayload: 16 } },
	{ name: "err-too-many-frames", description: "3 frames, maxFrames 2", hex: (header(0x04, 1, 1) + "00").repeat(3), error: "too_many_frames", limits: { maxFrames: 2 } },
	{ name: "err-open-family", description: "open with address family 5", hex: header(0x01, 1, 55) + "00".repeat(48) + "05" + "00".repeat(6), error: "bad_payload" },
	{ name: "err-open-short", description: "open with a 10-byte payload", hex: header(0x01, 1, 10) + "00".repeat(10), error: "bad_payload" },
	{ name: "err-open-length", description: "IPv4 open one byte too long", hex: header(0x01, 1, 56) + "00".repeat(48) + "04" + "cb007107" + "1ae1" + "00", error: "bad_payload" },
	{ name: "err-open-port-zero", description: "open to port 0", hex: header(0x01, 1, 55) + "00".repeat(48) + "04" + "cb007107" + "0000", error: "bad_payload" },
	{ name: "err-open-conn-zero", description: "open on conn 0", hex: header(0x01, 0, 55) + "00".repeat(48) + "04" + "cb007107" + "1ae1", error: "bad_conn" },
	{ name: "err-open-gateway-conn", description: "open on a gateway-range conn (0x8000)", hex: header(0x01, 0x8000, 55) + "00".repeat(48) + "04" + "cb007107" + "1ae1", error: "bad_conn" },
	{ name: "err-data-empty", description: "data frame with no payload", hex: header(0x04, 1, 0), error: "bad_payload" },
	{ name: "err-data-conn-zero", description: "data on conn 0", hex: header(0x04, 0, 1) + "00", error: "bad_conn" },
	{ name: "err-ping-conn", description: "ping on conn 1", hex: header(0x07, 1, 0), error: "bad_conn" },
	{ name: "err-ping-long", description: "ping with 9 bytes", hex: header(0x07, 0, 9) + "00".repeat(9), error: "bad_payload" },
	{ name: "err-close-reason-length", description: "close with a 2-byte reason", hex: header(0x05, 1, 2) + "0000", error: "bad_payload" },
	{ name: "err-opened-length", description: "opened with 27 bytes", hex: header(0x02, 1, 27) + "00".repeat(27), error: "bad_payload" },
	{ name: "err-after-valid", description: "a valid data frame followed by an unknown type", hex: header(0x04, 1, 1) + "ff" + header(0x0a, 1, 0), error: "unknown_type" },
];

mkdirSync(dir, { recursive: true });
for (const f of readdirSync(dir)) if (f.endsWith(".bin") || f === "vectors.json") rmSync(new URL(f, dir));

const manifest = {
	"//": "Generated by scripts/gen-fixtures.ts. Each vector's bytes are in <name>.bin (and inline as hex).",
	valid: valid.map(({ name, description, frames }) => {
		const message = encodeFrames(frames);
		writeFileSync(new URL(`${name}.bin`, dir), message);
		return { name, description, file: `${name}.bin`, hex: toHex(message), frames: frames.map(frameToJson) };
	}),
	invalid: invalid.map(({ name, description, hex, error, limits }) => {
		writeFileSync(new URL(`${name}.bin`, dir), fromHex(hex));
		return { name, description, file: `${name}.bin`, hex, error, ...(limits ? { limits } : {}) };
	}),
};
writeFileSync(new URL("vectors.json", dir), JSON.stringify(manifest, null, "\t") + "\n");
console.log(`wrote ${manifest.valid.length} valid and ${manifest.invalid.length} invalid vectors`);
