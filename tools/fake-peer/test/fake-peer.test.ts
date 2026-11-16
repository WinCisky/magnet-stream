import { createHash } from "node:crypto";
import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { bdecode, bencode, makeTorrent, parseSize, pexMessage, startFakePeer, type FakePeer, type PeerBehaviour } from "../src/index.ts";
import { download } from "./client.ts";

const torrent = makeTorrent({ seed: 7, size: 600_000, pieceLength: 64 * 1024 });
const peers: FakePeer[] = [];
async function peer(b: PeerBehaviour = {}): Promise<FakePeer> {
	const p = await startFakePeer({ torrent, ...b });
	peers.push(p);
	return p;
}
afterEach(async () => {
	await Promise.all(peers.splice(0).map((p) => p.close()));
});

describe("makeTorrent", () => {
	it("is deterministic per seed", () => {
		const a = makeTorrent({ seed: 1, size: 100_000, pieceLength: 16384 });
		const b = makeTorrent({ seed: 1, size: 100_000, pieceLength: 16384 });
		const c = makeTorrent({ seed: 2, size: 100_000, pieceLength: 16384 });
		expect(a.infoHashHex).toBe(b.infoHashHex);
		expect(a.data).toEqual(b.data);
		expect(c.infoHashHex).not.toBe(a.infoHashHex);
		// A fixed vector, so a change to the generator is noticed.
		expect(makeTorrent({ seed: 1, size: 8 * 1024 * 1024 }).infoHashHex).toMatchInlineSnapshot(`"aed25b198b720409b5b1e88fb9fd73ffa537e631"`);
	});

	it("produces a real info dict whose SHA-1 is the info-hash", () => {
		const decoded = bdecode(torrent.info);
		expect(decoded?.end).toBe(torrent.info.length);
		const info = decoded!.value as Map<string, unknown>;
		expect(info.get("length")).toBe(600_000);
		expect(info.get("piece length")).toBe(64 * 1024);
		expect((info.get("pieces") as Uint8Array).length).toBe(torrent.pieceCount * 20);
		expect(createHash("sha1").update(torrent.info).digest("hex")).toBe(torrent.infoHashHex);
		expect(torrent.magnet).toBe(`magnet:?xt=urn:btih:${torrent.infoHashHex}&dn=fake-7.bin`);
		for (let p = 0; p < torrent.pieceCount; p++) {
			const bytes = torrent.data.subarray(p * torrent.pieceLength, (p + 1) * torrent.pieceLength);
			expect(createHash("sha1").update(bytes).digest().equals(Buffer.from(torrent.hashes[p]))).toBe(true);
		}
	});

	it("parses sizes", () => {
		expect(parseSize("8MiB")).toBe(8 * 1024 * 1024);
		expect(parseSize("512KiB")).toBe(512 * 1024);
		expect(parseSize("1000")).toBe(1000);
		expect(() => parseSize("lots")).toThrow();
	});
});

describe("encoding", () => {
	it("encodes bitfields MSB first", async () => {
		const p = await peer({ have: [0, 2, 9], fast: false });
		const r = await download(torrent, { port: p.port, pieces: [] });
		expect(r.bitfield && Array.from(r.bitfield)).toEqual([0b1010_0000, 0b0100_0000]);
	});

	it("builds ut_pex messages with compact peers", () => {
		const msg = bdecode(bencode(pexMessage(["1.2.3.4:6881", "[2001:db8::1]:51413"])))!.value as Map<string, Uint8Array>;
		expect(Array.from(msg.get("added")!)).toEqual([1, 2, 3, 4, 0x1a, 0xe1]);
		expect(Array.from(msg.get("added6")!)).toEqual([0x20, 0x01, 0x0d, 0xb8, ...Array(11).fill(0), 1, 0xc8, 0xd5]);
	});
});

describe("fake peer over TCP", () => {
	it("listens on loopback, on an ephemeral port, by default", async () => {
		const p = await peer();
		expect(p.host).toBe("127.0.0.1");
		expect(p.port).toBeGreaterThan(1023);
		expect(p.magnet).toContain(`x.pe=${encodeURIComponent(p.addr)}`);
	});

	it("seed: handshake, extension handshake, have-all, then every piece verified", async () => {
		const p = await peer();
		const r = await download(torrent, { port: p.port });
		expect(r.handshake!.subarray(28, 48).equals(Buffer.from(torrent.infoHash))).toBe(true);
		expect(r.extHandshake?.get("reqq")).toBe(500);
		expect(r.haveAll).toBe(true);
		expect(r.pieces.size).toBe(torrent.pieceCount);
		expect([...r.pieces.values()].every(Boolean)).toBe(true);
		expect(p.stats.firstMessages.slice(0, 1)).toEqual([0x0f]);
	});

	it("reqq is configurable", async () => {
		const p = await peer({ reqq: 42 });
		const r = await download(torrent, { port: p.port, pieces: [0] });
		expect(r.extHandshake?.get("reqq")).toBe(42);
	});

	it("partial peer: only serves the pieces of its bitfield", async () => {
		const p = await peer({ have: [1, 3], fast: false });
		const r = await download(torrent, { port: p.port });
		expect([...r.pieces.keys()].sort()).toEqual([1, 3]);
		expect([...r.pieces.values()].every(Boolean)).toBe(true);
	});

	it("unchoke delay", async () => {
		const p = await peer({ unchokeDelayMs: 300 });
		const started = Date.now();
		const r = await download(torrent, { port: p.port, pieces: [0] });
		expect(r.pieces.get(0)).toBe(true);
		expect(Date.now() - started).toBeGreaterThanOrEqual(280);
	});

	it("never unchokes", async () => {
		const p = await peer({ unchoke: false });
		const r = await download(torrent, { port: p.port, timeoutMs: 400 });
		expect(r.unchoked).toBe(false);
		expect(r.pieces.size).toBe(0);
	});

	it("chokes midway and rejects the rest", async () => {
		const p = await peer({ chokeAfterBlocks: 5 });
		const r = await download(torrent, { port: p.port, timeoutMs: 600 });
		expect(r.choked).toBe(true);
		expect(p.stats.served).toBe(5);
		expect(r.rejects).toBeGreaterThan(0);
	});

	it("closes after the handshake or after N blocks", async () => {
		const a = await peer({ closeAfterHandshake: true });
		const ra = await download(torrent, { port: a.port });
		expect(ra.handshake).toBeDefined();
		expect(ra.closed).toBe(true);
		const b = await peer({ closeAfterBlocks: 3 });
		const rb = await download(torrent, { port: b.port });
		expect(rb.closed).toBe(true);
		expect(b.stats.served).toBe(3);
	});

	it("serves corrupted data for the chosen pieces", async () => {
		const p = await peer({ corrupt: [2] });
		const r = await download(torrent, { port: p.port });
		expect(r.pieces.get(2)).toBe(false);
		expect(r.pieces.get(1)).toBe(true);
	});

	it("answers with the wrong info-hash", async () => {
		const p = await peer({ wrongInfoHash: true });
		const r = await download(torrent, { port: p.port, timeoutMs: 300 });
		expect(r.handshake!.subarray(28, 48).equals(Buffer.from(torrent.infoHash))).toBe(false);
	});

	it("hangs up on a handshake for another torrent", async () => {
		const p = await peer();
		const r = await download(torrent, { port: p.port, infoHash: new Uint8Array(20) });
		expect(r.handshake).toBeUndefined();
		expect(r.closed).toBe(true);
	});

	it("silent, delayed and non-BitTorrent answers", async () => {
		const silent = await peer({ silent: true });
		expect((await download(torrent, { port: silent.port, timeoutMs: 300 })).handshake).toBeUndefined();
		const slow = await peer({ handshakeDelayMs: 200 });
		const started = Date.now();
		expect((await download(torrent, { port: slow.port, pieces: [0] })).pieces.get(0)).toBe(true);
		expect(Date.now() - started).toBeGreaterThanOrEqual(190);
		const http = await peer({ respondWith: Buffer.from("HTTP/1.1 400 Bad Request\r\n\r\n".padEnd(80, " ")) });
		const r = await download(torrent, { port: http.port, timeoutMs: 300 });
		expect(r.handshake?.subarray(0, 4).toString()).toBe("HTTP");
	});

	it("serves the info dict over ut_metadata (BEP 9)", async () => {
		const big = makeTorrent({ seed: 3, size: 20 * 1024 * 1024, pieceLength: 16 * 1024 }); // info > 16 KiB: several metadata pieces
		const p = await startFakePeer({ torrent: big });
		peers.push(p);
		const r = await download(big, { port: p.port, metadata: true });
		expect(r.extHandshake?.get("metadata_size")).toBe(big.info.length);
		expect(r.metadata?.equals(Buffer.from(big.info))).toBe(true);
		expect(p.stats.metadataRequests).toBe(Math.ceil(big.info.length / 16384));
	});

	it("rejects ut_metadata requests when metadata is off", async () => {
		const p = await peer({ metadata: false });
		// metadata_size is absent then; ask for piece 0 anyway.
		const r = await rawMetadataRequest(p.port);
		expect(r).toBe(2);
	});

	it("announces peers with ut_pex (BEP 11)", async () => {
		const p = await peer({ pex: ["10.1.2.3:6881", "[2001:db8::2]:7000"] });
		const r = await download(torrent, { port: p.port, pieces: [0] });
		expect(Array.from((r.pex?.get("added") as Uint8Array) ?? [])).toEqual([10, 1, 2, 3, 0x1a, 0xe1]);
		expect(p.stats.pexSent).toBe(1);
	});
});

/** Send a ut_metadata request for piece 0 using id 1 (the fake peer's), return the reply's msg_type. */
function rawMetadataRequest(port: number): Promise<number | undefined> {
	return new Promise((resolve) => {
		const s = net.connect({ port, host: "127.0.0.1" });
		let buf = Buffer.alloc(0);
		let hs = false;
		const timer = setTimeout(() => {
			s.destroy();
			resolve(undefined);
		}, 1000);
		const msg = (id: number, payload: Uint8Array) => {
			const b = Buffer.alloc(5 + payload.length);
			b.writeUInt32BE(1 + payload.length);
			b[4] = id;
			b.set(payload, 5);
			return b;
		};
		s.on("connect", () => {
			const h = Buffer.alloc(68);
			h[0] = 19;
			h.write("BitTorrent protocol", 1, "latin1");
			Buffer.from(torrent.infoHash).copy(h, 28);
			s.write(h);
			s.write(msg(20, Buffer.concat([Buffer.of(0), Buffer.from(bencode({ m: { ut_metadata: 9 } }))])));
			s.write(msg(20, Buffer.concat([Buffer.of(1), Buffer.from(bencode({ msg_type: 0, piece: 0 }))])));
		});
		s.on("data", (c) => {
			buf = Buffer.concat([buf, c]);
			if (!hs && buf.length >= 68) {
				hs = true;
				buf = buf.subarray(68);
			}
			while (hs && buf.length >= 4 && buf.length >= 4 + buf.readUInt32BE(0)) {
				const len = buf.readUInt32BE(0);
				const p = buf.subarray(4, 4 + len);
				buf = buf.subarray(4 + len);
				if (p[0] === 20 && p[1] === 9) {
					clearTimeout(timer);
					s.destroy();
					resolve((bdecode(p.subarray(2))?.value as Map<string, number>).get("msg_type"));
				}
			}
		});
	});
}
