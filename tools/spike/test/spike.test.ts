import { describe, expect, it } from "vitest";
import { parseInfoDict } from "@magnet-stream/engine";
import { bencode, makeTorrent } from "@magnet-stream/fake-peer";
import { infoDictOf, piecesFor, torrentSource, tunnelUrl } from "../src/spike.ts";

describe("spike helpers", () => {
	it("normalises the gateway URL", () => {
		expect(tunnelUrl("ws://127.0.0.1:8787")).toBe("ws://127.0.0.1:8787/v1/tunnel");
		expect(tunnelUrl("https://gw.example")).toBe("wss://gw.example/v1/tunnel");
		expect(tunnelUrl("wss://gw.example/custom")).toBe("wss://gw.example/custom");
		expect(() => tunnelUrl("ftp://x")).toThrow();
	});

	it("picks the first pieces covering N bytes, in rounds past the end", async () => {
		const t = makeTorrent({ seed: 1, size: 1000_000, pieceLength: 256 * 1024 });
		const meta = await parseInfoDict(t.info, t.infoHashHex);
		expect(piecesFor(meta, 1)).toEqual([0]);
		expect(piecesFor(meta, 256 * 1024 + 1)).toEqual([0, 1]);
		expect(piecesFor(meta, 1000_000)).toEqual([0, 1, 2, 3]);
		expect(piecesFor(meta, 1000_001)).toEqual([0, 1, 2, 3, 0]);
	});

	it("extracts the info dict of a .torrent file", async () => {
		const t = makeTorrent({ seed: 3, size: 100_000, pieceLength: 16384 });
		const file = bencode({ announce: "http://tracker.invalid/announce", "creation date": 1, info: Buffer.from(t.info) as unknown as Uint8Array });
		// `info` must be embedded raw, not as a string: build it by hand.
		const raw = Buffer.concat([Buffer.from("d8:announce31:http://tracker.invalid/announce4:info"), Buffer.from(t.info), Buffer.from("e")]);
		expect(Buffer.from(infoDictOf(raw)).equals(Buffer.from(t.info))).toBe(true);
		expect(() => infoDictOf(file.subarray(1))).toThrow();
		const source = await torrentSource(raw, ["1.2.3.4:6881", "1.2.3.4:6881"]);
		expect(source.meta.infoHash).toBe(t.infoHashHex);
		expect(source.candidates).toEqual(["1.2.3.4:6881"]);
	});
});
