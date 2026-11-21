// Which codecs an MP4 or WebM file holds, read from its own index (MP4:
// the sample descriptions in `moov`; WebM: the `Tracks` element) through
// the player's ByteSource, so the page can pick the browser's own player
// or libmedia before either loads (codecs.ts `nativePlayable`). Reads wait
// for their pieces like any player read (the picker fetches a file's head
// and tail first); nothing here touches the network. Every size read from
// the file is checked: a malformed or unusual file gives null (libmedia
// plays it), never a wrong answer.

import type { MediaCodecs } from "./codecs";
import type { ByteSource } from "./source";

/** Largest `moov` read (a 3 h film's is a few MiB). */
const MAX_MOOV = 64 * 1024 * 1024;
/** Largest WebM `Tracks` read. */
const MAX_TRACKS = 1024 * 1024;
/** Top-level boxes (or Segment children) looked at before giving up. */
const MAX_ELEMENTS = 64;
/** Tracks reported, at most. */
const MAX_TRACK_COUNT = 64;

/** The codecs of an MP4 (`mp4`, `m4v`) or WebM (`webm`) file; null: another container, or unreadable. */
export async function probeCodecs(source: ByteSource, ext: string, signal?: AbortSignal): Promise<MediaCodecs | null> {
	try {
		if (ext === "mp4" || ext === "m4v") return await probeMp4(source, signal);
		if (ext === "webm") return await probeWebm(source, signal);
	} catch (e) {
		if (signal?.aborted) throw e;
	}
	return null;
}

/** Exactly `length` bytes at `offset` (fewer only at the end of the file). */
async function readAt(source: ByteSource, offset: number, length: number, signal?: AbortSignal): Promise<Uint8Array> {
	const out = new Uint8Array(length);
	let got = 0;
	while (got < length) {
		const n = await source.read(offset + got, out.subarray(got), signal);
		if (n <= 0) break;
		got += n;
	}
	return out.subarray(0, got);
}

const fourcc = (d: Uint8Array, at: number) => String.fromCharCode(d[at], d[at + 1], d[at + 2], d[at + 3]);
const u32 = (d: Uint8Array, at: number) => ((d[at] << 24) | (d[at + 1] << 16) | (d[at + 2] << 8) | d[at + 3]) >>> 0;

// --- MP4 ---------------------------------------------------------------------

const MP4_CODECS: Record<string, string> = {
	avc1: "h264",
	avc3: "h264",
	hvc1: "hevc",
	hev1: "hevc",
	vp09: "vp9",
	vp08: "vp8",
	av01: "av1",
	mp4v: "mpeg4",
	"ac-3": "ac3",
	"ec-3": "eac3",
	Opus: "opus",
	fLaC: "flac",
	".mp3": "mp3",
};

async function probeMp4(source: ByteSource, signal?: AbortSignal): Promise<MediaCodecs | null> {
	const size = await source.size(signal);
	let at = 0;
	for (let i = 0; i < MAX_ELEMENTS && at + 8 <= size; i++) {
		const head = await readAt(source, at, 16, signal);
		if (head.length < 8) return null;
		let boxSize = u32(head, 0);
		let headerSize = 8;
		if (boxSize === 1) {
			if (head.length < 16) return null;
			boxSize = u32(head, 8) * 2 ** 32 + u32(head, 12);
			headerSize = 16;
		} else if (boxSize === 0) {
			boxSize = size - at;
		}
		if (boxSize < headerSize || at + boxSize > size) return null;
		const type = fourcc(head, 4);
		if (i === 0 && type !== "ftyp") return null;
		if (type === "moov") {
			if (boxSize - headerSize > MAX_MOOV) return null;
			const body = await readAt(source, at + headerSize, boxSize - headerSize, signal);
			if (body.length !== boxSize - headerSize) return null;
			return parseMoov(body);
		}
		at += boxSize;
	}
	return null;
}

interface Box {
	type: string;
	/** Payload [start, end) within the buffer. */
	start: number;
	end: number;
}

/** The boxes in d[start, end), or null if a size doesn't fit. */
function boxes(d: Uint8Array, start: number, end: number): Box[] | null {
	const out: Box[] = [];
	for (let at = start; at + 8 <= end; ) {
		let size = u32(d, at);
		let header = 8;
		if (size === 1) {
			if (at + 16 > end) return null;
			size = u32(d, at + 8) * 2 ** 32 + u32(d, at + 12);
			header = 16;
		} else if (size === 0) size = end - at;
		if (size < header || at + size > end) return null;
		out.push({ type: fourcc(d, at + 4), start: at + header, end: at + size });
		at += size;
		if (out.length > 10_000) return null;
	}
	return out;
}

function child(d: Uint8Array, parent: Box, type: string): Box | null {
	return boxes(d, parent.start, parent.end)?.find((b) => b.type === type) ?? null;
}

function parseMoov(d: Uint8Array): MediaCodecs | null {
	const top = boxes(d, 0, d.length);
	if (!top) return null;
	const result: MediaCodecs = { container: "mp4", video: [], audio: [], subtitles: 0 };
	for (const trak of top.filter((b) => b.type === "trak").slice(0, MAX_TRACK_COUNT)) {
		const mdia = child(d, trak, "mdia");
		const hdlr = mdia && child(d, mdia, "hdlr");
		const stsd = mdia && pathOf(d, mdia, ["minf", "stbl", "stsd"]);
		if (!hdlr || !stsd || hdlr.end - hdlr.start < 12 || stsd.end - stsd.start < 8) return null;
		const handler = fourcc(d, hdlr.start + 8);
		// Full box: version and flags, then the entry count, then the entries.
		const entries = boxes(d, stsd.start + 8, stsd.end);
		if (!entries || entries.length === 0) return null;
		const codec = sampleCodec(d, entries[0]);
		if (handler === "vide") result.video.push(codec);
		else if (handler === "soun") result.audio.push(codec);
		else if (handler === "subt" || handler === "text" || handler === "sbtl") result.subtitles++;
	}
	return result;
}

function pathOf(d: Uint8Array, from: Box, path: string[]): Box | null {
	let box: Box | null = from;
	for (const type of path) {
		box = box && child(d, box, type);
		if (!box) return null;
	}
	return box;
}

/** A sample entry's codec name ("h264", "aac"…), or "?" and its fourcc. */
function sampleCodec(d: Uint8Array, entry: Box): string {
	if (entry.type === "mp4a") return mp4aCodec(d, entry);
	return MP4_CODECS[entry.type] ?? `?${entry.type.replace(/[^\x20-\x7e]/g, "_")}`;
}

/** AAC or MP3 in `mp4a`, from the esds object type (AAC if it can't be read). */
function mp4aCodec(d: Uint8Array, entry: Box): string {
	// AudioSampleEntry: 28 bytes of fields, then its boxes (esds).
	const inner = boxes(d, entry.start + 28, entry.end);
	const esds = inner?.find((b) => b.type === "esds");
	if (!esds) return "aac";
	let at = esds.start + 4; // version and flags
	const descriptor = (tag: number): boolean => {
		if (at >= esds.end || d[at] !== tag) return false;
		at++;
		for (let i = 0; i < 4 && at < esds.end; i++) if ((d[at++] & 0x80) === 0) break;
		return true;
	};
	if (!descriptor(0x03)) return "aac";
	if (at + 3 > esds.end) return "aac";
	const flags = d[at + 2];
	at += 3; // ES_ID, flags
	if (flags & 0x80) at += 2; // dependsOn_ES_ID
	if (flags & 0x40) at += 1 + (d[at] ?? 0); // URL
	if (flags & 0x20) at += 2; // OCR_ES_ID
	if (!descriptor(0x04) || at >= esds.end) return "aac";
	const objectType = d[at];
	return objectType === 0x69 || objectType === 0x6b ? "mp3" : "aac";
}

// --- WebM (Matroska) ---------------------------------------------------------

const EBML = 0x1a45dfa3;
const SEGMENT = 0x18538067;
const TRACKS = 0x1654ae6b;
const CLUSTER = 0x1f43b675;
const TRACK_ENTRY = 0xae;
const TRACK_TYPE = 0x83;
const CODEC_ID = 0x86;

const WEBM_CODECS: Record<string, string> = {
	V_VP8: "vp8",
	V_VP9: "vp9",
	V_AV1: "av1",
	"V_MPEG4/ISO/AVC": "h264",
	"V_MPEGH/ISO/HEVC": "hevc",
	A_OPUS: "opus",
	A_VORBIS: "vorbis",
	A_AC3: "ac3",
	A_EAC3: "eac3",
	A_FLAC: "flac",
	"A_MPEG/L3": "mp3",
};

interface Vint {
	value: number;
	length: number;
	/** All value bits set: an unknown size. */
	unknown: boolean;
}

/** An EBML variable-size integer at d[at] (`raw`: keep the length marker, as element IDs do). */
function vint(d: Uint8Array, at: number, raw = false): Vint | null {
	const first = d[at];
	if (first === undefined || first === 0) return null;
	const length = Math.clz32(first) - 23;
	if (length > 8 || at + length > d.length) return null;
	let value = raw ? first : first & (0xff >> length);
	let ones = value === 0xff >> length;
	for (let i = 1; i < length; i++) {
		value = value * 256 + d[at + i];
		ones &&= d[at + i] === 0xff;
	}
	return { value, length, unknown: !raw && ones };
}

interface Element {
	id: number;
	/** Header length; the payload follows. */
	header: number;
	/** Payload size (-1: unknown). */
	size: number;
}

function element(d: Uint8Array, at: number): Element | null {
	const id = vint(d, at, true);
	if (!id || id.length > 4) return null;
	const size = vint(d, at + id.length);
	if (!size) return null;
	return { id: id.value, header: id.length + size.length, size: size.unknown ? -1 : size.value };
}

async function probeWebm(source: ByteSource, signal?: AbortSignal): Promise<MediaCodecs | null> {
	const size = await source.size(signal);
	const head = await readAt(source, 0, 64, signal);
	const ebml = element(head, 0);
	if (!ebml || ebml.id !== EBML || ebml.size < 0) return null;
	let at = ebml.header + ebml.size;
	const segHead = await readAt(source, at, 12, signal);
	const segment = element(segHead, 0);
	if (!segment || segment.id !== SEGMENT) return null;
	at += segment.header;
	for (let i = 0; i < MAX_ELEMENTS && at < size; i++) {
		const el = element(await readAt(source, at, 12, signal), 0);
		if (!el || el.id === CLUSTER || el.size < 0) return null;
		if (el.id === TRACKS) {
			if (el.size > MAX_TRACKS) return null;
			const body = await readAt(source, at + el.header, el.size, signal);
			return body.length === el.size ? parseTracks(body) : null;
		}
		at += el.header + el.size;
	}
	return null;
}

function parseTracks(d: Uint8Array): MediaCodecs | null {
	const result: MediaCodecs = { container: "webm", video: [], audio: [], subtitles: 0 };
	let count = 0;
	for (let at = 0; at < d.length; ) {
		const el = element(d, at);
		if (!el || el.size < 0 || at + el.header + el.size > d.length) return null;
		const start = at + el.header;
		const end = start + el.size;
		if (el.id === TRACK_ENTRY) {
			if (++count > MAX_TRACK_COUNT) break;
			let type = 0;
			let codec = "";
			for (let p = start; p < end; ) {
				const f = element(d, p);
				if (!f || f.size < 0 || p + f.header + f.size > end) return null;
				const body = d.subarray(p + f.header, p + f.header + f.size);
				if (f.id === TRACK_TYPE && f.size <= 4) type = body.reduce((n, b) => n * 256 + b, 0);
				if (f.id === CODEC_ID && f.size <= 64) codec = String.fromCharCode(...body).replace(/\0+$/, "");
				p += f.header + f.size;
			}
			const name = codec.startsWith("A_AAC") ? "aac" : codec.startsWith("A_DTS") ? "dts" : (WEBM_CODECS[codec] ?? `?${codec.replace(/[^\x20-\x7e]/g, "_")}`);
			if (type === 1) result.video.push(name);
			else if (type === 2) result.audio.push(name);
			else if (type === 0x11) result.subtitles++;
		}
		at = end;
	}
	return result;
}
