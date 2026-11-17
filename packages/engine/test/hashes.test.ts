// Piece layers from several peers: bad answers and rejects move on to the
// next peer.

import { afterEach, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type PeerBehaviour } from "@magnet-stream/fake-peer";
import { HashFetcher, PeerConnection, V2Verifier, geometryOf, parseInfoDict } from "../src/index.ts";
import { TcpTransport } from "../examples/tcp-transport.ts";

const t = makeTorrent({ seed: 51, files: [{ path: ["a.mkv"], length: 30 * 32 * 1024 }], pieceLength: 32 * 1024, version: 2 });
const peers: FakePeer[] = [];
afterEach(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
});

describe("HashFetcher", () => {
	it("rejects layers that don't match the pieces root and asks another peer", async () => {
		const meta = await parseInfoDict(t.info, t.infoHashHex);
		const verifier = new V2Verifier(meta);
		let layers = 0;
		const fetcher = new HashFetcher(verifier, { onLayer: () => layers++ });
		const connect = async (b: PeerBehaviour) => {
			const peer = await startFakePeer({ torrent: t, ...b });
			peers.push(peer);
			const transport = await TcpTransport.connect({ port: peer.port, infoHash: t.infoHash });
			const conn: PeerConnection = new PeerConnection(transport, {
				geometry: geometryOf(meta),
				keepAliveMs: 0,
				events: { hashes: (r, h) => void fetcher.onHashes(conn, r, h), hashReject: (r) => fetcher.onReject(conn, r), close: () => fetcher.onClose(conn) },
			});
			return { peer, conn };
		};
		fetcher.want(verifier.missingLayers());
		const liar = await connect({ hashes: "corrupt" });
		const refuser = await connect({ hashes: "reject" });
		const good = await connect({});
		fetcher.offer(liar.conn);
		await new Promise((r) => setTimeout(r, 200));
		fetcher.offer(refuser.conn);
		await new Promise((r) => setTimeout(r, 200));
		fetcher.offer(good.conn);
		const until = Date.now() + 5000;
		while (layers === 0 && Date.now() < until) await new Promise((r) => setTimeout(r, 20));
		expect(layers).toBe(1);
		expect(fetcher.failures).toMatchObject({ bad_hashes: 1, reject: 1 });
		expect(verifier.canVerify(5)).toBe(true);
		for (const c of [liar, refuser, good]) c.conn.close();
	});
});
