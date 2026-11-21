import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { nativePlayable, type MediaCodecs } from "./codecs";
import { probeCodecs } from "./probe";
import type { ByteSource } from "./source";

/** Reads of at most `chunk` bytes, as a recovering torrent file gives them. */
function source(bytes: Uint8Array, chunk = 1000): ByteSource & { reads: number[] } {
	const reads: number[] = [];
	return {
		reads,
		size: async () => bytes.length,
		read: async (offset, into) => {
			reads.push(offset);
			const n = Math.max(0, Math.min(into.length, chunk, bytes.length - offset));
			into.set(bytes.subarray(offset, offset + n));
			return n;
		},
	};
}

const concat = (...parts: Uint8Array[]) => {
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
};
const ascii = (s: string) => new TextEncoder().encode(s);
const be32 = (n: number) => Uint8Array.of(n >>> 24, (n >>> 16) & 255, (n >>> 8) & 255, n & 255);

// --- MP4 ---------------------------------------------------------------------

const box = (type: string, ...body: Uint8Array[]) => {
	const payload = concat(...body);
	return concat(be32(payload.length + 8), ascii(type), payload);
};
const full = (type: string, ...body: Uint8Array[]) => box(type, new Uint8Array(4), ...body);
const hdlr = (handler: string) => full("hdlr", new Uint8Array(4), ascii(handler), new Uint8Array(13));
/** A sample entry: its fields, then child boxes. */
const entry = (type: string, fields: number, ...children: Uint8Array[]) => box(type, new Uint8Array(fields), ...children);
const esds = (objectType: number) => full("esds", Uint8Array.of(0x03, 0x80, 0x80, 0x80, 25, 0, 1, 0), Uint8Array.of(0x04, 17, objectType, 0x15), new Uint8Array(15));
const trak = (handler: string, sample: Uint8Array) =>
	box("trak", box("tkhd", new Uint8Array(84)), box("mdia", hdlr(handler), box("minf", box("stbl", full("stsd", be32(1), sample), full("stts", be32(0))))));
const ftyp = box("ftyp", ascii("isom"), be32(512), ascii("isomiso2"));

function mp4(traks: Uint8Array[], { moovFirst = false, mdat = 5000 } = {}) {
	const moov = box("moov", full("mvhd", new Uint8Array(96)), ...traks);
	const data = box("mdat", new Uint8Array(mdat));
	return moovFirst ? concat(ftyp, moov, data) : concat(ftyp, data, moov);
}

const video = (type: string) => entry(type, 78);
const audio = (type: string, ...children: Uint8Array[]) => entry(type, 28, ...children);

describe("probeCodecs (MP4)", () => {
	it("reads the codecs of the real test clip (moov after mdat)", async () => {
		const clip = new Uint8Array(readFileSync(new URL("../../../../../tools/e2e/fixtures/clip-vp9.mp4", import.meta.url)));
		const src = source(clip, 64 * 1024);
		expect(await probeCodecs(src, "mp4")).toEqual({ container: "mp4", video: ["vp9"], audio: [], subtitles: 0 });
		// It jumped over mdat: a handful of reads, not the whole file.
		expect(src.reads.length).toBeLessThan(10);
	});

	it("names video, audio and subtitle tracks, wherever moov is", async () => {
		const file = mp4([trak("vide", video("avc1")), trak("soun", audio("mp4a", esds(0x40))), trak("soun", audio("ac-3")), trak("sbtl", entry("tx3g", 30))]);
		expect(await probeCodecs(source(file), "mp4")).toEqual({ container: "mp4", video: ["h264"], audio: ["aac", "ac3"], subtitles: 1 });
		const first = mp4([trak("vide", video("hvc1")), trak("soun", audio("mp4a", esds(0x6b)))], { moovFirst: true });
		expect(await probeCodecs(source(first), "m4v")).toEqual({ container: "mp4", video: ["hevc"], audio: ["mp3"], subtitles: 0 });
		const odd = mp4([trak("vide", video("encv")), trak("soun", audio("mp4a"))]);
		expect(await probeCodecs(source(odd), "mp4")).toEqual({ container: "mp4", video: ["?encv"], audio: ["aac"], subtitles: 0 });
	});

	it("follows a 64-bit box size", async () => {
		const big = concat(be32(1), ascii("mdat"), be32(0), be32(16 + 3000), new Uint8Array(3000));
		const moov = box("moov", trak("vide", video("av01")));
		expect(await probeCodecs(source(concat(ftyp, big, moov)), "mp4")).toMatchObject({ video: ["av1"] });
	});

	it("gives null for anything it can't read, never a guess", async () => {
		const good = mp4([trak("vide", video("vp09"))]);
		// Not an MP4 at all, a truncated file, a box size past the end, no moov.
		expect(await probeCodecs(source(concat(box("free", new Uint8Array(8)), good)), "mp4")).toBeNull();
		expect(await probeCodecs(source(good.subarray(0, good.length - 10)), "mp4")).toBeNull();
		const broken = good.slice();
		broken.set(be32(0x7fffffff), ftyp.length);
		expect(await probeCodecs(source(broken), "mp4")).toBeNull();
		expect(await probeCodecs(source(concat(ftyp, box("mdat", new Uint8Array(10)))), "mp4")).toBeNull();
		expect(await probeCodecs(source(good), "mkv")).toBeNull();
		expect(await probeCodecs(source(new Uint8Array(0)), "mp4")).toBeNull();
	});
});

// --- WebM --------------------------------------------------------------------

/** An element with an 8-byte size (`unknown`: all ones). */
function el(id: number[], body: Uint8Array, unknown = false) {
	const size = unknown ? Uint8Array.of(0x01, 255, 255, 255, 255, 255, 255, 255) : Uint8Array.of(0x01, 0, 0, 0, (body.length >>> 24) & 255, (body.length >>> 16) & 255, (body.length >>> 8) & 255, body.length & 255);
	return concat(Uint8Array.from(id), size, body);
}
const uint = (id: number[], n: number) => concat(Uint8Array.from(id), Uint8Array.of(0x81, n));
const str = (id: number[], s: string) => concat(Uint8Array.from(id), Uint8Array.of(0x80 | s.length), ascii(s));
const track = (type: number, codec: string) => el([0xae], concat(uint([0xd7], 1), uint([0x83], type), str([0x86], codec)));
const header = el([0x1a, 0x45, 0xdf, 0xa3], str([0x42, 0x82], "webm"));

function webm(tracks: Uint8Array[], { unknownSegment = false, clusterFirst = false } = {}) {
	const info = el([0x15, 0x49, 0xa9, 0x66], uint([0x2a, 0xd7, 0xb1], 1));
	const cluster = el([0x1f, 0x43, 0xb6, 0x75], new Uint8Array(2000));
	const tracksEl = el([0x16, 0x54, 0xae, 0x6b], concat(...tracks));
	const children = clusterFirst ? [info, cluster, tracksEl] : [el([0x11, 0x4d, 0x9b, 0x74], new Uint8Array(40)), info, el([0xec], new Uint8Array(100)), tracksEl, cluster];
	return concat(header, el([0x18, 0x53, 0x80, 0x67], concat(...children), unknownSegment));
}

describe("probeCodecs (WebM)", () => {
	it("reads the tracks' codecs, with a known or unknown segment size", async () => {
		const file = webm([track(1, "V_VP9"), track(2, "A_OPUS"), track(0x11, "S_TEXT/WEBVTT")]);
		expect(await probeCodecs(source(file, 7), "webm")).toEqual({ container: "webm", video: ["vp9"], audio: ["opus"], subtitles: 1 });
		const live = webm([track(1, "V_AV1"), track(2, "A_AAC/MPEG4/LC"), track(2, "A_DTS")], { unknownSegment: true });
		expect(await probeCodecs(source(live), "webm")).toEqual({ container: "webm", video: ["av1"], audio: ["aac", "dts"], subtitles: 0 });
	});

	it("gives null when the tracks aren't before the first cluster, or the file isn't EBML", async () => {
		expect(await probeCodecs(source(webm([track(1, "V_VP8")], { clusterFirst: true })), "webm")).toBeNull();
		expect(await probeCodecs(source(mp4([trak("vide", video("vp09"))])), "webm")).toBeNull();
		const truncated = webm([track(1, "V_VP8")]);
		expect(await probeCodecs(source(truncated.subarray(0, 80)), "webm")).toBeNull();
	});
});

// --- the choice --------------------------------------------------------------

describe("nativePlayable", () => {
	/** Chromium without proprietary codecs or HEVC hardware, roughly. */
	const chromium = (type: string) => (/codecs="(vp8|vp9|vp09[^"]*|av01[^"]*|opus|vorbis|flac|mp4a\.40\.2|mp4a\.6B|avc1[^"]*)"/.test(type) ? "probably" : "");
	const codecs = (over: Partial<MediaCodecs>): MediaCodecs => ({ container: "mp4", video: ["h264"], audio: ["aac"], subtitles: 0, ...over });

	it("plays natively what the browser decodes", () => {
		expect(nativePlayable(codecs({}), chromium)).toEqual({ native: true });
		expect(nativePlayable(codecs({ video: ["vp9"], audio: [] }), chromium)).toEqual({ native: true });
		expect(nativePlayable(codecs({ container: "webm", video: ["vp9"], audio: ["opus"] }), chromium)).toEqual({ native: true });
		expect(nativePlayable(codecs({ video: [], audio: ["mp3"] }), chromium)).toEqual({ native: true });
	});

	it("leaves HEVC, AC3, unknown codecs, several audio tracks and inner subtitles to libmedia", () => {
		expect(nativePlayable(codecs({ video: ["hevc"] }), chromium)).toEqual({ native: false, reason: "the browser can't decode hevc" });
		expect(nativePlayable(codecs({ audio: ["ac3"] }), chromium)).toMatchObject({ native: false });
		expect(nativePlayable(codecs({ audio: ["eac3"] }), chromium)).toMatchObject({ native: false });
		expect(nativePlayable(codecs({ video: ["?encv"] }), chromium)).toMatchObject({ native: false });
		expect(nativePlayable(codecs({ container: "webm", video: ["h264"] }), chromium)).toMatchObject({ native: false });
		expect(nativePlayable(codecs({ audio: ["aac", "aac"] }), chromium)).toEqual({ native: false, reason: "several audio tracks" });
		expect(nativePlayable(codecs({ subtitles: 1 }), chromium)).toEqual({ native: false, reason: "subtitle tracks" });
		expect(nativePlayable(codecs({ video: [], audio: [] }), chromium)).toMatchObject({ native: false });
		// A browser with HEVC hardware decoding plays it natively.
		expect(nativePlayable(codecs({ video: ["hevc"] }), () => "probably")).toEqual({ native: true });
		expect(nativePlayable(codecs({ video: ["toString"] }), () => "probably")).toMatchObject({ native: false });
	});
});
