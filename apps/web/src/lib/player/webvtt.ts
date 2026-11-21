// Subtitle files for the native <video> player, which shows only WebVTT:
// SRT is converted (the two differ in the header and the decimal comma),
// WebVTT passes through, the rest (ASS, SSA, TTML) it can't show.

import { normalizeSubtitle } from "./quirks";

/** Extensions the native player can show. */
export function nativeSubtitle(ext: string): boolean {
	return ext === "srt" || ext === "vtt";
}

/** The file as WebVTT text, or null if its format can't be shown natively. */
export function toWebVtt(bytes: Uint8Array, ext: string): string | null {
	if (!nativeSubtitle(ext)) return null;
	const text = new TextDecoder().decode(normalizeSubtitle(bytes));
	if (ext === "vtt") return /^WEBVTT/.test(text) ? text : `WEBVTT\n\n${text}`;
	// SRT: "00:01:02,500 --> 00:01:04,000" becomes "00:01:02.500 --> 00:01:04.000".
	const cues = text.replace(/(\d{1,2}:\d{2}:\d{2}),(\d{3})/g, "$1.$2");
	return `WEBVTT\n\n${cues}`;
}
