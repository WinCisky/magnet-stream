import { describe, expect, it } from "vitest";
import { MetadataFetcher } from "@magnet-stream/engine";
import { MetadataRace } from "./metadata-race";
import { encode, sha1, toHex } from "./test-helpers";

async function torrent(name: string): Promise<{ info: Uint8Array; hash: string }> {
	const info = encode({ length: 32_768, name, "piece length": 16_384, pieces: new Uint8Array(40).fill(7) });
	return { info, hash: toHex(await sha1(info)) };
}

describe("MetadataRace", () => {
	it("names the first verified source: the cache", async () => {
		const t = await torrent("a.mkv");
		const race = new MetadataRace(new MetadataFetcher({ expected: { v1: t.hash } }));
		expect(await race.offer(t.info, "cache")).toBe(true);
		const { source, result } = await race.done;
		expect(source).toBe("cache");
		expect(result.meta.name).toBe("a.mkv");
		// The VPS line that comes later changes nothing.
		expect(await race.offer(t.info, "vps")).toBe(false);
		expect(race.rejected).toEqual({ cache: 0, vps: 0 });
	});

	it("drops a tampered cache and a forged info line, and takes the right one", async () => {
		const t = await torrent("right.mkv");
		const forged = await torrent("forged.mkv");
		const race = new MetadataRace(new MetadataFetcher({ expected: { v1: t.hash } }));
		const tampered = t.info.slice();
		tampered[tampered.length - 2] ^= 1;
		expect(await race.offer(tampered, "cache")).toBe(false);
		expect(await race.offer(forged.info, "vps")).toBe(false);
		expect(race.rejected).toEqual({ cache: 1, vps: 1 });
		expect(race.checking).toBe(false);
		const pending = race.offer(t.info, "vps");
		expect(race.checking).toBe(true);
		expect(await pending).toBe(true);
		const { source, result } = await race.done;
		expect(source).toBe("vps");
		expect(result.meta.name).toBe("right.mkv");
	});

	it("says peer when BEP 9 won", async () => {
		const done = Promise.resolve({ meta: {} as never, bytes: new Uint8Array(1), source: "peer" as const });
		const race = new MetadataRace({ done, isDone: true, offerBytes: async () => false });
		expect((await race.done).source).toBe("peer");
	});
});
