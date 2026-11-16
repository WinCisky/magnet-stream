// Integration: PeerConnection + downloadAll over a direct TCP transport to
// the fake peer.

import { afterEach, describe, expect, it } from "vitest";
import { makeTorrent, startFakePeer, type FakePeer, type PeerBehaviour } from "@magnet-stream/fake-peer";
import { MSG, PeerConnection, WireParser, downloadAll, parseInfoDict, type Metainfo } from "../src/index.ts";
import { TcpTransport } from "./tcp-transport.ts";

const torrent = makeTorrent({ seed: 5, size: 3 * 1024 * 1024 + 12_345, pieceLength: 256 * 1024 });
let meta: Metainfo;
const peers: FakePeer[] = [];

async function connect(b: PeerBehaviour = {}, reserved?: Uint8Array) {
	meta ??= await parseInfoDict(torrent.info, torrent.infoHashHex);
	const peer = await startFakePeer({ torrent, ...b });
	peers.push(peer);
	const transport = await TcpTransport.connect({ port: peer.port, infoHash: torrent.infoHash, reserved });
	const conn = new PeerConnection(transport, { pieceCount: meta.pieceCount, pieceLength: meta.pieceLength, totalLength: meta.totalLength });
	return { peer, transport, conn };
}

afterEach(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
});

/** Ids of every message we sent (after the handshake). */
function sentIds(t: TcpTransport): number[] {
	const parser = new WireParser({ pieceCount: torrent.pieceCount, pieceLength: torrent.pieceLength, totalLength: torrent.totalLength });
	const ids: number[] = [];
	for (const chunk of t.sent) {
		for (const m of parser.push(chunk)) {
			const id = { choke: 0, unchoke: 1, interested: 2, not_interested: 3, have: 4, bitfield: 5, request: 6, piece: 7, cancel: 8, have_all: 14, have_none: 15, reject: 16 }[m.type as string];
			if (id !== undefined) ids.push(id);
		}
	}
	return ids;
}

describe("download from one peer", () => {
	it("downloads every piece and verifies each SHA-1", async () => {
		const { conn, transport } = await connect();
		const delivered = new Map<number, Uint8Array>();
		const result = await downloadAll(conn, meta, { onPiece: (p, data) => delivered.set(p, data) });
		expect(result.verified).toEqual([...Array(torrent.pieceCount).keys()]);
		expect(result.corrupt).toEqual([]);
		expect(result.missing).toEqual([]);
		for (const [p, data] of delivered) {
			expect(Buffer.from(data).equals(Buffer.from(torrent.data.subarray(p * torrent.pieceLength, (p + 1) * torrent.pieceLength)))).toBe(true);
		}
		// Fast on both sides: have-none first; never unchoke, bitfield, have or piece (no upload).
		const ids = sentIds(transport);
		expect(ids[0]).toBe(MSG.HAVE_NONE);
		expect(ids.filter((id) => [MSG.UNCHOKE, MSG.PIECE, MSG.BITFIELD, MSG.HAVE, MSG.HAVE_ALL].includes(id as 1))).toEqual([]);
		conn.close();
	});

	it("works without the fast extension", async () => {
		const { conn, transport } = await connect({ fast: false }, new Uint8Array(8));
		expect(conn.fast).toBe(false);
		const result = await downloadAll(conn, meta);
		expect(result.verified).toHaveLength(torrent.pieceCount);
		expect(sentIds(transport)[0]).toBe(MSG.INTERESTED);
		conn.close();
	});

	it("drops corrupt pieces, reports them, never delivers them", async () => {
		const { conn } = await connect({ corrupt: [1, 4] });
		const delivered: number[] = [];
		const corrupt: number[] = [];
		const result = await downloadAll(conn, meta, { onPiece: (p) => delivered.push(p), onCorrupt: (p) => corrupt.push(p) });
		expect(result.corrupt.sort()).toEqual([1, 4]);
		expect(delivered).not.toContain(1);
		expect(delivered).not.toContain(4);
		expect(delivered).toHaveLength(torrent.pieceCount - 2);
		// Each was tried twice (one retry) before giving up.
		expect(corrupt.sort()).toEqual([1, 1, 4, 4]);
		conn.close();
	});

	it("reports the pieces a partial peer lacks", async () => {
		const { conn } = await connect({ have: [0, 2, 3] });
		const result = await downloadAll(conn, meta);
		expect(result.verified).toEqual([0, 2, 3]);
		expect(result.missing).toEqual([...Array(torrent.pieceCount).keys()].filter((p) => ![0, 2, 3].includes(p)));
		conn.close();
	});

	it("waits for a slow unchoke", async () => {
		const { conn } = await connect({ unchokeDelayMs: 300 });
		const result = await downloadAll(conn, meta, { pieces: [0] });
		expect(result.verified).toEqual([0]);
		conn.close();
	});

	it("fails on a stall (never unchoked, or choked midway)", async () => {
		const a = await connect({ unchoke: false });
		await expect(downloadAll(a.conn, meta, { stallTimeoutMs: 300 })).rejects.toThrow(/no progress/);
		const b = await connect({ chokeAfterBlocks: 20 });
		const err = await downloadAll(b.conn, meta, { stallTimeoutMs: 300 }).catch((e) => e);
		expect(err.message).toMatch(/no progress/);
		expect(err.result.verified.length).toBeGreaterThan(0);
	});

	it("fails when the peer hangs up, keeping what was verified", async () => {
		const { conn } = await connect({ closeAfterBlocks: 40 });
		const err = await downloadAll(conn, meta).catch((e) => e);
		expect(err.name).toBe("DownloadError");
		expect(err.result.verified.length).toBeGreaterThanOrEqual(2);
	});

	it("can be aborted", async () => {
		const { conn } = await connect({ unchoke: false });
		const controller = new AbortController();
		const p = downloadAll(conn, meta, { signal: controller.signal });
		controller.abort();
		await expect(p).rejects.toThrow(/aborted/);
		expect(conn.closed).toBe(true);
	});
});
