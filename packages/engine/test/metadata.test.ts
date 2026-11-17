// ut_metadata (BEP 9) against the fake peer over TCP: the happy path,
// rejects, a wrong info dict, a peer hanging up midway, size limits, v2.

import { afterEach, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent, type PeerBehaviour } from "@magnet-stream/fake-peer";
import { MetadataFetcher, PeerConnection, type ExpectedHashes } from "../src/index.ts";
import { TcpTransport } from "../examples/tcp-transport.ts";

const EXT_FAST = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
// 2048 pieces of 16 KiB: a 40 KiB info dict, 3 metadata pieces.
const big = makeTorrent({ seed: 21, size: 32 * 1024 * 1024, pieceLength: 16 * 1024 });
const peers: FakePeer[] = [];
afterEach(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
});

/** Connect to a fake peer and route its events to the fetcher. */
async function connect(fetcher: MetadataFetcher, torrent: FakeTorrent, b: PeerBehaviour = {}): Promise<{ peer: FakePeer; conn: PeerConnection }> {
	const peer = await startFakePeer({ torrent, ...b });
	peers.push(peer);
	const transport = await TcpTransport.connect({ port: peer.port, infoHash: torrent.infoHash, reserved: EXT_FAST });
	const conn: PeerConnection = new PeerConnection(transport, {
		keepAliveMs: 0,
		events: {
			extensions: () => fetcher.offer(conn),
			metadata: (m) => fetcher.onMessage(conn, m),
			close: () => fetcher.onClose(conn),
		},
	});
	return { peer, conn };
}

const v1 = (t: FakeTorrent): ExpectedHashes => ({ v1: t.infoHashHex });

describe("metadata from peers", () => {
	it("fetches and verifies the info dict, piece by piece", async () => {
		const f = new MetadataFetcher({ expected: v1(big) });
		const { peer, conn } = await connect(f, big);
		const { meta, bytes } = await f.done;
		expect(Buffer.from(bytes).equals(Buffer.from(big.info))).toBe(true);
		expect(meta.pieceCount).toBe(2048);
		expect(peer.stats.metadataRequests).toBe(3);
		conn.close();
	});

	it("moves on from a peer serving a dict that doesn't hash right, or hanging up midway", async () => {
		const f = new MetadataFetcher({ expected: v1(big), parallel: 1 });
		const bad = await connect(f, big, { metadataCorrupt: true });
		await new Promise((r) => setTimeout(r, 200));
		const quitter = await connect(f, big, { closeAfterMetadataPieces: 1 });
		const good = await connect(f, big);
		const { bytes } = await f.done;
		expect(Buffer.from(bytes).equals(Buffer.from(big.info))).toBe(true);
		expect(f.failures.hash_mismatch).toBe(1);
		expect(f.failures.closed).toBe(1);
		for (const c of [bad, quitter, good]) c.conn.close();
	});

	it("skips peers that reject, go silent, or announce an oversized dict", async () => {
		const small = makeTorrent({ seed: 22, size: 1_000_000, pieceLength: 32 * 1024 });
		const f = new MetadataFetcher({ expected: v1(small), maxSize: 64 * 1024, pieceTimeoutMs: 300, parallel: 1 });
		const huge = await connect(f, small, { metadataSize: 10 * 1024 * 1024 });
		const lying = await connect(f, small, { metadataSize: small.info.length + 5 });
		const good = await connect(f, small);
		await f.done;
		expect(f.failures.too_large).toBe(1);
		expect(f.failures.bad_data + f.failures.timeout).toBe(1);
		expect(huge.peer.stats.metadataRequests).toBe(0);
		for (const c of [huge, lying, good]) c.conn.close();
	});

	it("fetches a v2-only info dict, checked with SHA-256", async () => {
		const t = makeTorrent({ seed: 23, files: [{ path: ["a.mkv"], length: 200_000 }, { path: ["b.srt"], length: 900 }], pieceLength: 32 * 1024, version: 2 });
		const f = new MetadataFetcher({ expected: { v2: Buffer.from(t.infoHashV2!).toString("hex") } });
		const { conn } = await connect(f, t);
		const { meta } = await f.done;
		expect(meta.version).toBe("v2");
		expect(meta.files.map((x) => x.path)).toEqual(["a.mkv", "b.srt"]);
		conn.close();
	});
});
