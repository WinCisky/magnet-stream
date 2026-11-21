import { describe, expect, it } from "vitest";
import { contentDisposition, contentTypeFor, parseRange, parseStreamPath, planResponse, sanitizeName, streamPath } from "./http";

const IH = "0123456789abcdef0123456789abcdef01234567";

describe("parseStreamPath", () => {
	it("reads the info-hash and file index", () => {
		expect(parseStreamPath(`/stream/${IH}/0`)).toEqual({ infoHash: IH, file: 0 });
		expect(parseStreamPath(`/stream/${IH}/42`)).toEqual({ infoHash: IH, file: 42 });
		expect(parseStreamPath(`/app/stream/${IH}/3`, "/app/")).toEqual({ infoHash: IH, file: 3 });
		expect(parseStreamPath(`/app/stream/${IH}/3`, "/app")).toEqual({ infoHash: IH, file: 3 });
	});

	it("rejects anything else", () => {
		for (const path of [
			"/stream/",
			`/stream/${IH}`,
			`/stream/${IH}/`,
			`/stream/${IH}/1/name.mp4`,
			`/stream/${IH.toUpperCase()}/1`,
			`/stream/${IH.slice(1)}/1`,
			`/stream/${IH}0/1`,
			`/stream/${"g".repeat(40)}/1`,
			`/stream/${IH}/01`,
			`/stream/${IH}/-1`,
			`/stream/${IH}/1.5`,
			`/stream/${IH}/1e3`,
			`/stream/${IH}/1234567`,
			`/stream/${IH}/%31`,
			`/stream/${IH}/..`,
			`/other/${IH}/1`,
			`/stream/${IH}/1`.replace("/stream", "/app/stream"),
		]) {
			expect(parseStreamPath(path), path).toBeNull();
		}
	});

	it("builds the same paths", () => {
		expect(streamPath(IH, 7)).toBe(`/stream/${IH}/7`);
		expect(parseStreamPath(streamPath(IH, 7))).toEqual({ infoHash: IH, file: 7 });
		expect(() => streamPath("nope", 1)).toThrow(RangeError);
		expect(() => streamPath(IH, -1)).toThrow(RangeError);
		expect(() => streamPath(IH, 1.5)).toThrow(RangeError);
	});
});

describe("parseRange", () => {
	const size = 1000;
	it("reads a single byte range", () => {
		expect(parseRange("bytes=0-", size)).toEqual({ kind: "partial", start: 0, end: 1000 });
		expect(parseRange("bytes=0-0", size)).toEqual({ kind: "partial", start: 0, end: 1 });
		expect(parseRange("bytes=100-199", size)).toEqual({ kind: "partial", start: 100, end: 200 });
		expect(parseRange("bytes=500-", size)).toEqual({ kind: "partial", start: 500, end: 1000 });
		expect(parseRange("BYTES = 5 - 9", size)).toEqual({ kind: "partial", start: 5, end: 10 });
	});

	it("clamps the end and reads suffixes", () => {
		expect(parseRange("bytes=900-5000", size)).toEqual({ kind: "partial", start: 900, end: 1000 });
		expect(parseRange("bytes=999-99999999999999999999999", size)).toEqual({ kind: "partial", start: 999, end: 1000 });
		expect(parseRange("bytes=-100", size)).toEqual({ kind: "partial", start: 900, end: 1000 });
		expect(parseRange("bytes=-5000", size)).toEqual({ kind: "partial", start: 0, end: 1000 });
	});

	it("can't satisfy a range past the end", () => {
		expect(parseRange("bytes=1000-", size)).toEqual({ kind: "unsatisfiable" });
		expect(parseRange("bytes=1000-2000", size)).toEqual({ kind: "unsatisfiable" });
		expect(parseRange("bytes=99999999999999999999-", size)).toEqual({ kind: "unsatisfiable" });
		expect(parseRange("bytes=-0", size)).toEqual({ kind: "unsatisfiable" });
		expect(parseRange("bytes=0-", 0)).toEqual({ kind: "unsatisfiable" });
		expect(parseRange("bytes=-10", 0)).toEqual({ kind: "unsatisfiable" });
	});

	it("ignores what it doesn't read (the whole file is sent)", () => {
		for (const header of [null, undefined, "", "bytes=", "bytes=-", "items=0-1", "bytes=0-1,5-6", "bytes=5-1", "bytes=a-b", "bytes 0-1", `bytes=0-${"9".repeat(300)}`]) {
			expect(parseRange(header, size), String(header)).toEqual({ kind: "full" });
		}
	});
});

describe("headers", () => {
	it("types by extension from a fixed table", () => {
		expect(contentTypeFor("a.mp4")).toBe("video/mp4");
		expect(contentTypeFor("A.M4V")).toBe("video/mp4");
		expect(contentTypeFor("a.webm")).toBe("video/webm");
		expect(contentTypeFor("a.mkv")).toBe("video/x-matroska");
		expect(contentTypeFor("a.html")).toBe("application/octet-stream");
		expect(contentTypeFor("a.svg")).toBe("application/octet-stream");
		expect(contentTypeFor("constructor")).toBe("application/octet-stream");
		expect(contentTypeFor("a.toString")).toBe("application/octet-stream");
	});

	it("sanitizes names", () => {
		expect(sanitizeName("dir/sub/Film (2020).mp4")).toBe("Film (2020).mp4");
		expect(sanitizeName("..\\..\\evil.mp4")).toBe("evil.mp4");
		expect(sanitizeName("a\r\nSet-Cookie: x.mp4")).toBe("aSet-Cookie: x.mp4");
		expect(sanitizeName("\u202Egpj.exe")).toBe("gpj.exe");
		expect(sanitizeName("..")).toBe("video");
		expect(sanitizeName("   ")).toBe("video");
		expect([...sanitizeName("x".repeat(1000))].length).toBe(180);
	});

	it("writes a Content-Disposition with ASCII and UTF-8 names", () => {
		expect(contentDisposition("Film.mp4", false)).toBe(`inline; filename="Film.mp4"; filename*=UTF-8''Film.mp4`);
		expect(contentDisposition('Città "bella"; 100%.mp4', true)).toBe(
			`attachment; filename="Citt_ _bella__ 100_.mp4"; filename*=UTF-8''Citt%C3%A0%20%22bella%22%3B%20100%25.mp4`,
		);
		const header = contentDisposition("a\nb.mp4", true);
		expect(header).not.toMatch(/[\r\n]/);
	});
});

describe("planResponse", () => {
	const base = { size: 1000, name: "clip.mp4", download: false };

	it("answers 200 with the whole file without a Range", () => {
		const plan = planResponse({ ...base, range: null });
		expect(plan.status).toBe(200);
		expect([plan.start, plan.end]).toEqual([0, 1000]);
		expect(plan.headers["Content-Length"]).toBe("1000");
		expect(plan.headers["Accept-Ranges"]).toBe("bytes");
		expect(plan.headers["Content-Type"]).toBe("video/mp4");
		expect(plan.headers["X-Content-Type-Options"]).toBe("nosniff");
		expect(plan.headers["Content-Range"]).toBeUndefined();
		expect(plan.headers["Content-Disposition"]).toMatch(/^inline;/);
	});

	it("answers 206 with Content-Range for a range", () => {
		const plan = planResponse({ ...base, range: "bytes=100-" });
		expect(plan.status).toBe(206);
		expect([plan.start, plan.end]).toEqual([100, 1000]);
		expect(plan.headers["Content-Range"]).toBe("bytes 100-999/1000");
		expect(plan.headers["Content-Length"]).toBe("900");
		expect(plan.headers["Accept-Ranges"]).toBe("bytes");
		const tail = planResponse({ ...base, range: "bytes=-1" });
		expect(tail.headers["Content-Range"]).toBe("bytes 999-999/1000");
	});

	it("answers 416 for a range past the end", () => {
		const plan = planResponse({ ...base, range: "bytes=1000-" });
		expect(plan.status).toBe(416);
		expect(plan.headers["Content-Range"]).toBe("bytes */1000");
		expect(plan.start).toBe(plan.end);
	});

	it("sends an attachment for a download", () => {
		expect(planResponse({ ...base, range: null, download: true }).headers["Content-Disposition"]).toMatch(/^attachment;/);
	});
});
