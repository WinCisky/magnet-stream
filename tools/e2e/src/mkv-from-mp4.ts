// A Matroska file from the MP4 test clip (tools/e2e/fixtures), for the
// libmedia playback e2e: the same VP9 frames, one cluster per keyframe,
// each opening with a Void of `gap` bytes per frame it holds, so the 40 s
// span megabytes (as mp4-spread.ts does for the MP4) and bytes keep pace
// with time cluster by cluster (2 s). The Void sits in the cluster's
// header, before its first block: libmedia skips it there, while one
// between blocks would make it resynchronise on the next cluster. Cues come last, like ffmpeg writes
// them by default, so the index is in the file's tail. No ffmpeg needed:
// the fixture's sample table says where each frame is and when it plays.
//
// Handles what the fixture is: one VP9 video track, `stts`, `stss`, no
// composition offsets. Anything else throws.

import { concat, find, sampleTable, u32 } from "./mp4-spread.ts";

const ID = {
	EBML: [0x1a, 0x45, 0xdf, 0xa3],
	EBMLVersion: [0x42, 0x86],
	EBMLReadVersion: [0x42, 0xf7],
	EBMLMaxIDLength: [0x42, 0xf2],
	EBMLMaxSizeLength: [0x42, 0xf3],
	DocType: [0x42, 0x82],
	DocTypeVersion: [0x42, 0x87],
	DocTypeReadVersion: [0x42, 0x85],
	Segment: [0x18, 0x53, 0x80, 0x67],
	SeekHead: [0x11, 0x4d, 0x9b, 0x74],
	Seek: [0x4d, 0xbb],
	SeekID: [0x53, 0xab],
	SeekPosition: [0x53, 0xac],
	Info: [0x15, 0x49, 0xa9, 0x66],
	TimestampScale: [0x2a, 0xd7, 0xb1],
	Duration: [0x44, 0x89],
	MuxingApp: [0x4d, 0x80],
	WritingApp: [0x57, 0x41],
	Tracks: [0x16, 0x54, 0xae, 0x6b],
	TrackEntry: [0xae],
	TrackNumber: [0xd7],
	TrackUID: [0x73, 0xc5],
	TrackType: [0x83],
	FlagLacing: [0x9c],
	CodecID: [0x86],
	Video: [0xe0],
	PixelWidth: [0xb0],
	PixelHeight: [0xba],
	Cluster: [0x1f, 0x43, 0xb6, 0x75],
	Timestamp: [0xe7],
	SimpleBlock: [0xa3],
	Void: [0xec],
	Cues: [0x1c, 0x53, 0xbb, 0x6b],
	CuePoint: [0xbb],
	CueTime: [0xb3],
	CueTrackPositions: [0xb7],
	CueTrack: [0xf7],
	CueClusterPosition: [0xf1],
} as const;

/** An 8-byte EBML size. */
function size8(n: number): Uint8Array {
	const out = new Uint8Array(8);
	out[0] = 0x01;
	let v = n;
	for (let i = 7; i >= 1; i--) {
		out[i] = v % 256;
		v = Math.floor(v / 256);
	}
	return out;
}

const el = (id: readonly number[], ...body: Uint8Array[]) => {
	const payload = concat(body);
	return concat([Uint8Array.from(id), size8(payload.length), payload]);
};
/** An unsigned integer in `bytes` bytes (fixed width keeps sizes known ahead). */
const uint = (id: readonly number[], n: number, bytes = 8) => {
	const b = new Uint8Array(bytes);
	let v = n;
	for (let i = bytes - 1; i >= 0; i--) {
		b[i] = v % 256;
		v = Math.floor(v / 256);
	}
	return el(id, b);
};
const str = (id: readonly number[], s: string) => el(id, new TextEncoder().encode(s));
const float = (id: readonly number[], n: number) => {
	const b = new Uint8Array(8);
	new DataView(b.buffer).setFloat64(0, n);
	return el(id, b);
};
/** A Void element of exactly `total` bytes (header included). */
const voidOf = (total: number) => el(ID.Void, new Uint8Array(total - 1 - 8));

export function mp4ToMkv(mp4: Uint8Array, gap: number): Uint8Array {
	if (gap < 9) throw new Error("mkv: gap too small for a Void element");
	const { moov, table, offsets, sizes } = sampleTable(mp4);
	const trak = find(moov.children!, ["trak"]);
	const mdhd = find(trak.children!, ["mdia", "mdhd"]);
	if (mp4[mdhd.start + 8] !== 0) throw new Error("mkv: mdhd version 1 unsupported");
	const timescale = u32(mp4, mdhd.start + 20);
	const tkhd = find(trak.children!, ["tkhd"]);
	const width = u32(mp4, tkhd.start + tkhd.size - 8) >>> 16;
	const height = u32(mp4, tkhd.start + tkhd.size - 4) >>> 16;
	const stsd = find(table, ["stsd"]);
	const codec = String.fromCharCode(...mp4.subarray(stsd.start + 20, stsd.start + 24));
	if (codec !== "vp09") throw new Error(`mkv: ${codec} unsupported`);
	if (table.some((b) => b.type === "ctts")) throw new Error("mkv: composition offsets unsupported");

	// Decode times (ms) from stts, keyframes from stss.
	const stts = find(table, ["stts"]);
	const times: number[] = [];
	let t = 0;
	for (let e = 0, n = u32(mp4, stts.start + 12); e < n; e++) {
		const count = u32(mp4, stts.start + 16 + 8 * e);
		const delta = u32(mp4, stts.start + 20 + 8 * e);
		for (let i = 0; i < count; i++, t += delta) times.push(Math.round((t * 1000) / timescale));
	}
	const durationMs = (t * 1000) / timescale;
	if (times.length !== sizes.length) throw new Error("mkv: stts doesn't match the samples");
	const stss = find(table, ["stss"]);
	const keys = new Set(Array.from({ length: u32(mp4, stss.start + 12) }, (_, i) => u32(mp4, stss.start + 16 + 4 * i) - 1));

	// Clusters: one per keyframe, padding first.
	const clusters: { ms: number; bytes: Uint8Array }[] = [];
	let frames: Uint8Array[] = [];
	let clusterMs = 0;
	const close = () => {
		if (frames.length > 0) clusters.push({ ms: clusterMs, bytes: el(ID.Cluster, uint(ID.Timestamp, clusterMs, 4), voidOf(gap * frames.length), ...frames) });
		frames = [];
	};
	for (let i = 0; i < sizes.length; i++) {
		if (keys.has(i)) {
			close();
			clusterMs = times[i];
		}
		const rel = times[i] - clusterMs;
		if (rel > 32767) throw new Error("mkv: cluster too long");
		const head = Uint8Array.of(0x81, (rel >> 8) & 0xff, rel & 0xff, keys.has(i) ? 0x80 : 0);
		frames.push(el(ID.SimpleBlock, head, mp4.subarray(offsets[i], offsets[i] + sizes[i])));
	}
	close();
	if (!keys.has(0)) throw new Error("mkv: the first frame isn't a keyframe");

	const info = el(ID.Info, uint(ID.TimestampScale, 1_000_000, 4), float(ID.Duration, durationMs), str(ID.MuxingApp, "magnet-stream e2e"), str(ID.WritingApp, "magnet-stream e2e"));
	const tracks = el(
		ID.Tracks,
		el(
			ID.TrackEntry,
			uint(ID.TrackNumber, 1, 1),
			uint(ID.TrackUID, 1, 1),
			uint(ID.TrackType, 1, 1),
			uint(ID.FlagLacing, 0, 1),
			str(ID.CodecID, "V_VP9"),
			el(ID.Video, uint(ID.PixelWidth, width, 2), uint(ID.PixelHeight, height, 2)),
		),
	);
	// Positions are from the start of the Segment's payload; fixed-width
	// integers keep every size known before the positions are.
	const seek = (id: readonly number[], position: number) => el(ID.Seek, el(ID.SeekID, Uint8Array.from(id)), uint(ID.SeekPosition, position));
	const seekHeadSize = ID.SeekHead.length + 8 + 3 * seek(ID.Info, 0).length;
	const infoAt = seekHeadSize;
	const tracksAt = infoAt + info.length;
	let at = tracksAt + tracks.length;
	const cuePoints: Uint8Array[] = [];
	for (const c of clusters) {
		cuePoints.push(el(ID.CuePoint, uint(ID.CueTime, c.ms), el(ID.CueTrackPositions, uint(ID.CueTrack, 1, 1), uint(ID.CueClusterPosition, at))));
		at += c.bytes.length;
	}
	const cues = el(ID.Cues, ...cuePoints);
	const seekHead = el(ID.SeekHead, seek(ID.Info, infoAt), seek(ID.Tracks, tracksAt), seek(ID.Cues, at));
	if (seekHead.length !== seekHeadSize) throw new Error("mkv: seek head size");

	const header = el(
		ID.EBML,
		uint(ID.EBMLVersion, 1, 1),
		uint(ID.EBMLReadVersion, 1, 1),
		uint(ID.EBMLMaxIDLength, 4, 1),
		uint(ID.EBMLMaxSizeLength, 8, 1),
		str(ID.DocType, "matroska"),
		uint(ID.DocTypeVersion, 4, 1),
		uint(ID.DocTypeReadVersion, 2, 1),
	);
	return concat([header, el(ID.Segment, seekHead, info, tracks, ...clusters.map((c) => c.bytes), cues)]);
}
