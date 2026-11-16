import { describe, expect, it } from "vitest";
import { parseInfoDict } from "@magnet-stream/engine";
import { makeTorrent } from "@magnet-stream/fake-peer";
import { piecesFor, tunnelUrl } from "../src/spike.ts";

describe("spike helpers", () => {
	it("normalises the gateway URL", () => {
		expect(tunnelUrl("ws://127.0.0.1:8787")).toBe("ws://127.0.0.1:8787/v1/tunnel");
		expect(tunnelUrl("https://gw.example")).toBe("wss://gw.example/v1/tunnel");
		expect(tunnelUrl("wss://gw.example/custom")).toBe("wss://gw.example/custom");
		expect(() => tunnelUrl("ftp://x")).toThrow();
	});

	it("picks the first pieces covering N bytes", async () => {
		const t = makeTorrent({ seed: 1, size: 1000_000, pieceLength: 256 * 1024 });
		const meta = await parseInfoDict(t.info, t.infoHashHex);
		expect(piecesFor(meta, 1)).toEqual([0]);
		expect(piecesFor(meta, 256 * 1024 + 1)).toEqual([0, 1]);
		expect(piecesFor(meta, 10 ** 9)).toEqual([0, 1, 2, 3]);
	});
});
