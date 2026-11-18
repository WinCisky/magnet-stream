// Session start without the legacy endpoints: one discovery read, then the
// file list from the peer (BEP 9) for v1, hybrid and v2 magnets.

import { afterEach, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { MetadataFetcher, PeerConnection, consumePeerBody, listing, parseMagnet } from "../src/index.ts";
import { TcpTransport } from "../examples/tcp-transport.ts";

const EXT = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
const peers: FakePeer[] = [];
afterEach(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
});

function stream(text: string): ReadableStream<Uint8Array> {
	return new ReadableStream({
		start(controller) {
			controller.enqueue(new TextEncoder().encode(text));
			controller.close();
		},
	});
}

describe("session start", () => {
	for (const version of [1, "hybrid", 2] as const) {
		it(`lists files for a ${version} magnet from the peer, reading discovery once`, async () => {
			const torrent = makeTorrent({ seed: 80 + (version === 2 ? 2 : version === "hybrid" ? 1 : 0), name: "movie.mkv", size: 40_000, pieceLength: 16 * 1024, version });
			const peer = await startFakePeer({ torrent });
			peers.push(peer);
			const magnet = parseMagnet(peer.magnet);
			let reads = 0;
			const found: string[] = [];
			reads++;
			await consumePeerBody(stream(`{"peer":"${peer.addr}"}\n{"peer":"${peer.addr}"}\n{"done":true}\n`), (a) => found.push(a));
			expect(reads).toBe(1);
			expect(found).toEqual([peer.addr, peer.addr]);

			const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, v2: magnet.v2, wire: magnet.infoHashHex } });
			const transport = await TcpTransport.connect({ port: peer.port, infoHash: torrent.infoHash, reserved: EXT });
			const conn = new PeerConnection(transport, {
				keepAliveMs: 0,
				events: { extensions: () => fetcher.offer(conn), metadata: (m) => fetcher.onMessage(conn, m), close: () => fetcher.onClose(conn) },
			});
			const { meta } = await fetcher.done;
			expect(listing(meta).files.map((f) => f.path)).toContain("movie.mkv");
			expect(meta.infoHash).toBe(magnet.infoHashHex);
			conn.close();
		});
	}
});
