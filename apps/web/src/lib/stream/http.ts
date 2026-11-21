// The HTTP side of the streaming service worker (sw.ts), kept pure so it can
// be tested: which URLs it answers, how a `Range` header is read, and the
// status and headers of each answer. Nothing here touches the network or
// the piece store.
//
//   /stream/<info-hash>/<file index>             the file, inline (a <video> src)
//   /stream/<info-hash>/<file index>?download=1  the same bytes as an attachment

/** Path segment under the service worker's scope. */
export const STREAM_PREFIX = "stream/";

/** The 40-hex info-hash the page uses (v1, or a v2 hash truncated as on the wire). */
const INFO_HASH = /^[0-9a-f]{40}$/;
/** A file index: no sign, no leading zero, at most 6 digits. */
const FILE_INDEX = /^(0|[1-9][0-9]{0,5})$/;

export interface StreamTarget {
	infoHash: string;
	file: number;
}

/**
 * The torrent and file a `/stream/…` path names, or null for anything else
 * (an invalid info-hash or file index included): the worker answers those
 * with 404 without asking anyone.
 */
export function parseStreamPath(pathname: string, scopePath = "/"): StreamTarget | null {
	const prefix = `${scopePath.replace(/\/?$/, "/")}${STREAM_PREFIX}`;
	if (!pathname.startsWith(prefix)) return null;
	const parts = pathname.slice(prefix.length).split("/");
	if (parts.length !== 2) return null;
	const [infoHash, file] = parts;
	if (!INFO_HASH.test(infoHash) || !FILE_INDEX.test(file)) return null;
	return { infoHash, file: Number(file) };
}

/** The path the page gives a `<video>` or a download link. */
export function streamPath(infoHash: string, file: number, scopePath = "/"): string {
	if (!INFO_HASH.test(infoHash) || !Number.isSafeInteger(file) || !FILE_INDEX.test(String(file))) {
		throw new RangeError("invalid stream target");
	}
	return `${scopePath.replace(/\/?$/, "/")}${STREAM_PREFIX}${infoHash}/${file}`;
}

export type RangeRequest =
	/** No usable `Range`: the whole file, 200. */
	| { kind: "full" }
	/** Bytes [start, end) of the file, 206. */
	| { kind: "partial"; start: number; end: number }
	/** A range wholly past the end of the file, 416. */
	| { kind: "unsatisfiable" };

const SINGLE_RANGE = /^bytes[ \t]*=[ \t]*([0-9]*)[ \t]*-[ \t]*([0-9]*)[ \t]*$/i;

/**
 * Read a `Range` header against a file of `size` bytes (RFC 9110 §14).
 * One range only: several ranges, another unit or a malformed value are
 * ignored (the whole file is sent), as the RFC allows. A range that starts
 * at or past the end, or an empty suffix, can't be satisfied.
 */
export function parseRange(header: string | null | undefined, size: number): RangeRequest {
	if (!header || header.length > 200) return { kind: "full" };
	const m = SINGLE_RANGE.exec(header.trim());
	if (!m) return { kind: "full" };
	const [, first, last] = m;
	if (first === "" && last === "") return { kind: "full" };
	if (first === "") {
		// Suffix: the last N bytes.
		const n = Number(last);
		if (n === 0 || size === 0) return { kind: "unsatisfiable" };
		return { kind: "partial", start: Math.max(0, size - n), end: size };
	}
	const start = Number(first);
	if (last !== "" && Number(last) < start) return { kind: "full" };
	if (start >= size) return { kind: "unsatisfiable" };
	const end = last === "" ? size : Math.min(size, Number(last) + 1);
	return { kind: "partial", start, end };
}

const TYPES: Record<string, string> = {
	mp4: "video/mp4",
	m4v: "video/mp4",
	webm: "video/webm",
	mkv: "video/x-matroska",
	mov: "video/quicktime",
	avi: "video/x-msvideo",
	ts: "video/mp2t",
	m2ts: "video/mp2t",
	ogv: "video/ogg",
	mpg: "video/mpeg",
	mpeg: "video/mpeg",
	wmv: "video/x-ms-wmv",
	flv: "video/x-flv",
	mp3: "audio/mpeg",
	m4a: "audio/mp4",
	aac: "audio/aac",
	flac: "audio/flac",
	ogg: "audio/ogg",
	opus: "audio/ogg",
	wav: "audio/wav",
};

/** Content-Type from a fixed table by extension; anything else is plain bytes. */
export function contentTypeFor(name: string): string {
	const dot = name.lastIndexOf(".");
	const ext = dot > 0 ? name.slice(dot + 1).toLowerCase() : "";
	return Object.hasOwn(TYPES, ext) ? TYPES[ext] : "application/octet-stream";
}

/** Longest file name put in a header. */
const MAX_NAME = 180;

/**
 * A file name safe for a header: the last path segment, no control or
 * format characters (bidi overrides), no path separators, bounded; "video"
 * if nothing is left.
 */
export function sanitizeName(name: string): string {
	const base = String(name).split(/[/\\]/).at(-1) ?? "";
	const clean = [...base.replace(/[\p{Cc}\p{Cf}]/gu, "").trim()].slice(0, MAX_NAME).join("");
	return clean === "" || clean === "." || clean === ".." ? "video" : clean;
}

/** `inline` or `attachment`, with an ASCII fallback name and the UTF-8 one (RFC 6266). */
export function contentDisposition(name: string, attachment: boolean): string {
	const safe = sanitizeName(name);
	const ascii = safe.replace(/[^\x20-\x7e]/g, "_").replace(/["\\%;]/g, "_");
	const utf8 = encodeURIComponent(safe).replace(/['()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
	return `${attachment ? "attachment" : "inline"}; filename="${ascii}"; filename*=UTF-8''${utf8}`;
}

export interface ResponsePlan {
	status: 200 | 206 | 416;
	headers: Record<string, string>;
	/** Bytes [start, end) of the file to send (start === end: no body). */
	start: number;
	end: number;
}

/** Status, headers and byte range of the answer to a request for a file of `size` bytes. */
export function planResponse({ range, size, name, download }: { range: string | null; size: number; name: string; download: boolean }): ResponsePlan {
	const headers: Record<string, string> = {
		"Accept-Ranges": "bytes",
		"Content-Type": contentTypeFor(sanitizeName(name)),
		"Content-Disposition": contentDisposition(name, download),
		"X-Content-Type-Options": "nosniff",
		"Cache-Control": "no-store",
		"Cross-Origin-Resource-Policy": "same-origin",
	};
	const asked = parseRange(range, size);
	if (asked.kind === "unsatisfiable") {
		return { status: 416, headers: { ...headers, "Content-Range": `bytes */${size}`, "Content-Length": "0" }, start: 0, end: 0 };
	}
	if (asked.kind === "partial") {
		const { start, end } = asked;
		return {
			status: 206,
			headers: { ...headers, "Content-Range": `bytes ${start}-${end - 1}/${size}`, "Content-Length": String(end - start) },
			start,
			end,
		};
	}
	return { status: 200, headers: { ...headers, "Content-Length": String(size) }, start: 0, end: size };
}
