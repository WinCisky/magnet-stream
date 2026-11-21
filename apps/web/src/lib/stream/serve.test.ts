import { describe, expect, it, vi } from "vitest";
import { parseOpenReply, parsePortRequest, parseReadReply, parseOpenRequest } from "./protocol";
import { serveStream, type FileLink } from "./serve";

const IH = "0123456789abcdef0123456789abcdef01234567";
const DATA = new Uint8Array(600_000).map((_, i) => (i * 7) % 256);
const ORIGIN = "http://localhost";

/** A file that hands out at most `chunk` bytes per read, as the page may. */
function fakeFile(chunk = 100_000) {
	const reads: [number, number][] = [];
	let closed = 0;
	const link: FileLink = {
		size: DATA.length,
		name: "dir/clip.mp4",
		read: async (offset, length) => {
			reads.push([offset, length]);
			return DATA.slice(offset, offset + Math.min(length, chunk));
		},
		close: () => closed++,
	};
	return { link, reads, closed: () => closed };
}

function get(path: string, headers: Record<string, string> = {}, method = "GET"): Request {
	return new Request(`${ORIGIN}${path}`, { method, headers });
}

describe("serveStream", () => {
	it("answers 200 with the whole file", async () => {
		const file = fakeFile();
		const res = await serveStream(get(`/stream/${IH}/0`), "/", async () => file.link);
		expect(res.status).toBe(200);
		expect(res.headers.get("Content-Length")).toBe(String(DATA.length));
		expect(res.headers.get("Content-Type")).toBe("video/mp4");
		expect(res.headers.get("Accept-Ranges")).toBe("bytes");
		expect(new Uint8Array(await res.arrayBuffer())).toEqual(DATA);
		expect(file.closed()).toBe(1);
	});

	it("answers 206 with the range asked, read as it is pulled", async () => {
		const file = fakeFile();
		const res = await serveStream(get(`/stream/${IH}/0`, { Range: "bytes=250000-449999" }), "/", async () => file.link);
		expect(res.status).toBe(206);
		expect(res.headers.get("Content-Range")).toBe(`bytes 250000-449999/${DATA.length}`);
		expect(res.headers.get("Content-Length")).toBe("200000");
		expect(res.headers.get("X-Content-Type-Options")).toBe("nosniff");
		expect(file.reads).toEqual([]);
		expect(new Uint8Array(await res.arrayBuffer())).toEqual(DATA.slice(250_000, 450_000));
		expect(file.reads[0][0]).toBe(250_000);
		expect(file.closed()).toBe(1);
	});

	it("answers 416 to a range past the end", async () => {
		const file = fakeFile();
		const res = await serveStream(get(`/stream/${IH}/0`, { Range: `bytes=${DATA.length}-` }), "/", async () => file.link);
		expect(res.status).toBe(416);
		expect(res.headers.get("Content-Range")).toBe(`bytes */${DATA.length}`);
		expect(file.reads).toEqual([]);
		expect(file.closed()).toBe(1);
	});

	it("answers 404 to an invalid info-hash or file index without asking for the file", async () => {
		const find = vi.fn(async () => fakeFile().link);
		for (const path of [`/stream/${IH.slice(2)}/0`, `/stream/${IH}/x`, `/stream/${IH}/01`, `/stream/${IH}/0/extra`, `/stream/../${IH}/0`]) {
			const res = await serveStream(get(path), "/", find);
			expect(res.status, path).toBe(404);
		}
		expect(find).not.toHaveBeenCalled();
	});

	it("answers 404 to a file no page has open", async () => {
		expect((await serveStream(get(`/stream/${IH}/3`), "/", async () => null)).status).toBe(404);
		const failing = await serveStream(get(`/stream/${IH}/3`), "/", () => Promise.reject(new Error("gone")));
		expect(failing.status).toBe(404);
	});

	it("answers HEAD without reading, and refuses other methods", async () => {
		const file = fakeFile();
		const head = await serveStream(get(`/stream/${IH}/0`, { Range: "bytes=0-9" }, "HEAD"), "/", async () => file.link);
		expect(head.status).toBe(206);
		expect(head.headers.get("Content-Length")).toBe("10");
		expect(file.reads).toEqual([]);
		const find = vi.fn(async () => file.link);
		const post = await serveStream(get(`/stream/${IH}/0`, {}, "POST"), "/", find);
		expect(post.status).toBe(405);
		expect(find).not.toHaveBeenCalled();
	});

	it("sends an attachment with ?download", async () => {
		const res = await serveStream(get(`/stream/${IH}/0?download=1`, { Range: "bytes=0-0" }), "/", async () => fakeFile().link);
		expect(res.headers.get("Content-Disposition")).toBe(`attachment; filename="clip.mp4"; filename*=UTF-8''clip.mp4`);
	});

	it("stops reading when the browser cancels", async () => {
		const file = fakeFile();
		const res = await serveStream(get(`/stream/${IH}/0`, { Range: "bytes=0-" }), "/", async () => file.link);
		const reader = res.body!.getReader();
		await reader.read();
		await reader.cancel();
		expect(file.closed()).toBe(1);
		expect(file.reads.length).toBe(1);
	});

	it("errors the body if a read fails or the file is shorter than it said", async () => {
		const failing: FileLink = { ...fakeFile().link, read: () => Promise.reject(new Error("page closed")) };
		const res = await serveStream(get(`/stream/${IH}/0`), "/", async () => failing);
		await expect(res.arrayBuffer()).rejects.toThrow();
		const short: FileLink = { ...fakeFile().link, read: async () => new Uint8Array(0) };
		await expect((await serveStream(get(`/stream/${IH}/0`), "/", async () => short)).arrayBuffer()).rejects.toThrow();
	});
});

describe("protocol", () => {
	it("checks every message", () => {
		expect(parseOpenRequest({ type: "magnet-stream:open", infoHash: IH, file: 2 })).toEqual({ type: "magnet-stream:open", infoHash: IH, file: 2 });
		expect(parseOpenRequest({ type: "magnet-stream:open", infoHash: "x", file: 2 })).toBeNull();
		expect(parseOpenRequest({ type: "magnet-stream:open", infoHash: IH, file: -1 })).toBeNull();
		expect(parseOpenReply({ type: "head", size: 10, name: "a.mp4" })).toEqual({ type: "head", size: 10, name: "a.mp4" });
		expect(parseOpenReply({ type: "head", size: 1.5, name: "a" })).toBeNull();
		expect(parseOpenReply({ type: "head", size: 1, name: "a".repeat(5000) })).toBeNull();
		expect(parseOpenReply({ type: "none" })).toEqual({ type: "none" });
		expect(parsePortRequest({ type: "read", id: 1, offset: 0, length: 1024 })).toEqual({ type: "read", id: 1, offset: 0, length: 1024 });
		expect(parsePortRequest({ type: "read", id: 1, offset: 0, length: 0 })).toBeNull();
		expect(parsePortRequest({ type: "read", id: 1, offset: 0, length: 2 * 1024 * 1024 })).toBeNull();
		expect(parsePortRequest({ type: "close" })).toEqual({ type: "close" });
		expect(parsePortRequest("read")).toBeNull();
		expect(parseReadReply({ type: "data", id: 1, bytes: new ArrayBuffer(4) })?.type).toBe("data");
		expect(parseReadReply({ type: "data", id: 1, bytes: new Uint8Array(4) })).toBeNull();
		expect(parseReadReply({ type: "error", id: 1 })).toEqual({ type: "error", id: 1 });
		expect(parseReadReply([1])).toBeNull();
	});
});
