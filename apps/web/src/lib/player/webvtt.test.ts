import { describe, expect, it } from "vitest";
import { nativeSubtitle, toWebVtt } from "./webvtt";

const bytes = (s: string) => new TextEncoder().encode(s);

describe("toWebVtt", () => {
	it("converts SRT timestamps and adds the header", () => {
		const srt = "1\r\n00:00:01,500 --> 00:00:03,000\r\nHello\r\n\r\n2\r\n00:01:02,000 --> 00:01:04,250\r\n<i>World</i>\r\n";
		expect(toWebVtt(bytes(srt), "srt")).toBe(
			"WEBVTT\n\n1\n00:00:01.500 --> 00:00:03.000\nHello\n\n2\n00:01:02.000 --> 00:01:04.250\n<i>World</i>\n\n",
		);
	});

	it("passes WebVTT through, adding a missing header", () => {
		expect(toWebVtt(bytes("WEBVTT\n\n00:01.000 --> 00:02.000\nHi\n"), "vtt")).toBe("WEBVTT\n\n00:01.000 --> 00:02.000\nHi\n\n");
		expect(toWebVtt(bytes("00:01.000 --> 00:02.000\nHi"), "vtt")).toBe("WEBVTT\n\n00:01.000 --> 00:02.000\nHi\n\n");
	});

	it("leaves formats the browser can't show", () => {
		expect(nativeSubtitle("ass")).toBe(false);
		expect(toWebVtt(bytes("[Script Info]"), "ass")).toBeNull();
	});
});
