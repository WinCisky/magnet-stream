// Codec names for the UI and diagnostics, and where libmedia finds its
// wasm decoders: served by the site itself (scripts/fetch-codecs.mjs).

/** libmedia's `${base}/decode/<codec>-simd.wasm` and friends. */
export function wasmBaseUrl(): string {
	return new URL(`${import.meta.env.BASE_URL.replace(/\/?$/, "/")}libmedia`, location.href).href;
}

/** FFmpeg `AVCodecID`s (libmedia uses the same numbers). */
const NAMES: Record<number, string> = {
	1: "mpeg1video",
	2: "mpeg2video",
	3: "h261",
	4: "h263",
	5: "rv10",
	6: "rv20",
	7: "mjpeg",
	12: "mpeg4",
	14: "msmpeg4v1",
	15: "msmpeg4v2",
	16: "msmpeg4v3",
	17: "wmv1",
	18: "wmv2",
	27: "h264",
	30: "theora",
	70: "vc1",
	71: "wmv3",
	139: "vp8",
	167: "vp9",
	173: "hevc",
	196: "vvc",
	225: "av1",
	86016: "mp2",
	86017: "mp3",
	86018: "aac",
	86019: "ac3",
	86020: "dts",
	86021: "vorbis",
	86022: "dvaudio",
	86023: "wmav1",
	86024: "wmav2",
	86028: "flac",
	86056: "eac3",
	86060: "truehd",
	86076: "opus",
	94208: "dvd_subtitle",
	94209: "dvb_subtitle",
	94210: "text",
	94212: "ssa",
	94213: "mov_text",
	94214: "hdmv_pgs_subtitle",
	94216: "srt",
	94225: "srt",
	94226: "webvtt",
	94230: "ass",
	94232: "ttml",
};

export function codecName(id: number): string {
	if (id > 65536 && id <= 65572) return "pcm";
	if (id >= 69632 && id <= 69683) return "adpcm";
	return NAMES[id] ?? `codec ${id}`;
}

/** The extension that picks the demuxer ("mkv"), from a file name. */
export function extensionOf(name: string): string {
	const dot = name.lastIndexOf(".");
	return dot > 0 && dot < name.length - 1 ? name.slice(dot + 1).toLowerCase() : "";
}

/** The codecs of an MP4 or WebM file, by the names above (probe.ts); "?…": one we don't know. */
export interface MediaCodecs {
	container: "mp4" | "webm";
	video: string[];
	audio: string[];
	/** Subtitle tracks inside the file. */
	subtitles: number;
}

/**
 * The codecs string Chromium is asked about for each codec it may decode
 * itself, per container. One representative profile each: a file whose
 * profile the browser can't decode after all fails before anything plays,
 * and libmedia takes over (native.ts `onUnsupported`).
 */
const NATIVE_CODECS: Record<MediaCodecs["container"], Record<string, string>> = {
	mp4: {
		h264: "avc1.640028",
		hevc: "hvc1.1.6.L120.90",
		vp9: "vp09.00.10.08",
		av1: "av01.0.08M.08",
		aac: "mp4a.40.2",
		mp3: "mp4a.6B",
		opus: "opus",
		flac: "flac",
		ac3: "ac-3",
		eac3: "ec-3",
	},
	webm: {
		vp8: "vp8",
		vp9: "vp9",
		av1: "av01.0.08M.08",
		opus: "opus",
		vorbis: "vorbis",
	},
};

export type NativeVerdict = { native: true } | { native: false; reason: string };

/**
 * Whether the browser's own <video> should play a file with these codecs:
 * each one decodable natively (`canPlay`, the element's canPlayType), one
 * video and one audio track at most (Chromium offers no track choice) and
 * no subtitle tracks inside (only libmedia shows them). Everything else
 * (HEVC without hardware support, AC3, several audio tracks…) plays
 * through libmedia.
 */
export function nativePlayable(codecs: MediaCodecs, canPlay: (type: string) => string): NativeVerdict {
	if (codecs.video.length === 0 && codecs.audio.length === 0) return { native: false, reason: "no audio or video track" };
	if (codecs.video.length > 1) return { native: false, reason: "several video tracks" };
	if (codecs.audio.length > 1) return { native: false, reason: "several audio tracks" };
	if (codecs.subtitles > 0) return { native: false, reason: "subtitle tracks" };
	const table = NATIVE_CODECS[codecs.container];
	for (const codec of [...codecs.video, ...codecs.audio]) {
		const name = Object.hasOwn(table, codec) ? table[codec] : undefined;
		if (!name) return { native: false, reason: `${codec} in ${codecs.container}` };
		let answer: string;
		try {
			answer = canPlay(`video/${codecs.container}; codecs="${name}"`);
		} catch {
			answer = "";
		}
		if (answer === "") return { native: false, reason: `the browser can't decode ${codec}` };
	}
	return { native: true };
}
