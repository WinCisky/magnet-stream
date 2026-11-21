// The video's bitrate (M3-06): known once the player parsed the container,
// it sizes the picker's playback window in bytes and sets the rate under
// which another socket opens (twice the bitrate, OQ-11).

import { describe, expect, it } from "vitest";
import { makeTorrent } from "@magnet-stream/fake-peer";
import { ConnectionManager } from "../src/connection-manager.ts";
import { parseInfoDict } from "../src/metainfo.ts";
import { DEFAULT_BITRATE, PickLevel, Picker } from "../src/picker.ts";
import type { WebSocketLike } from "../src/tunnel-client.ts";

const PL = 64 * 1024;

/** A WebSocket that never opens: the session stays offline. */
const offline = (): WebSocketLike => ({ binaryType: "arraybuffer", readyState: 0, protocol: "", send() {}, close() {}, addEventListener() {} });

describe("bitrate → playback window", () => {
	it("is 30 s of video at the default bitrate, and follows the bitrate set", () => {
		const picker = new Picker({ pieceCount: 1000, pieceLength: PL, totalLength: 1000 * PL });
		const id = picker.addTarget({ offset: 0, size: 1000 * PL });
		expect(picker.windowBytesOf(id)).toBe(30 * DEFAULT_BITRATE);
		picker.setBitrate(250_000);
		expect(picker.windowBytesOf(id)).toBe(7_500_000);
		expect(picker.scarceAheadBytes).toBe(30_000_000);
		picker.setBitrate(2_000_000);
		expect(picker.windowBytesOf(id)).toBe(60_000_000);
		// Nonsense is ignored.
		for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) picker.setBitrate(bad);
		expect(picker.windowBytesOf(id)).toBe(60_000_000);
		// A window set in bytes wins over the bitrate.
		picker.setWindowBytes(PL, id);
		picker.setBitrate(100_000);
		expect(picker.windowBytesOf(id)).toBe(PL);
	});

	it("puts more pieces in the window level at a higher bitrate", () => {
		const windowPieces = (bitrate: number) => {
			const picker = new Picker({ pieceCount: 200, pieceLength: PL, totalLength: 200 * PL }, { headBytes: 1, tailBytes: 1, windowSeconds: 10 });
			picker.setBitrate(bitrate);
			picker.addTarget({ offset: 0, size: 200 * PL }, { position: 50 * PL });
			// Index pieces (0 and 199) verified: the first picks are the window.
			picker.markVerified(0);
			picker.markVerified(199);
			const peer = { id: "a", rate: 1e9, reqq: 100_000, unchoked: true, has: () => true };
			const picked = picker.plan([peer], 0).get("a") ?? [];
			return new Set(picked.filter((b) => b.level === PickLevel.Window).map((b) => b.piece)).size;
		};
		expect(windowPieces(PL / 2)).toBe(5);
		expect(windowPieces(2 * PL)).toBe(20);
	});
});

describe("bitrate → sockets", () => {
	it("sets the rate threshold to twice the bitrate (configurable) and reaches a picker made later", async () => {
		const torrent = makeTorrent({ seed: 9, size: 40 * PL, pieceLength: PL });
		const mgr = new ConnectionManager({ gatewayUrl: "http://127.0.0.1:9", infoHash: torrent.infoHash, factory: offline, rateThreshold: 1_250_000 });
		try {
			expect(mgr.rateThreshold).toBe(1_250_000);
			mgr.setBitrate(400_000);
			expect(mgr.bitrate).toBe(400_000);
			expect(mgr.rateThreshold).toBe(800_000);
			mgr.setBitrate(Number.NaN);
			expect(mgr.rateThreshold).toBe(800_000);
			mgr.setInfo(await parseInfoDict(torrent.info, torrent.infoHashHex));
			const recovery = mgr.recover({ offset: 0, size: 40 * PL });
			expect(recovery.windowBytes()).toBe(30 * 400_000);
			mgr.setBitrate(100_000);
			expect(recovery.windowBytes()).toBe(3_000_000);
			recovery.stop();
		} finally {
			mgr.stop();
		}
		const triple = new ConnectionManager({ gatewayUrl: "http://127.0.0.1:9", infoHash: torrent.infoHash, factory: offline, socketRateFactor: 3 });
		triple.setBitrate(100_000);
		expect(triple.rateThreshold).toBe(300_000);
		triple.stop();
	});
});
