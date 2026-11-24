import { createHash } from "node:crypto";
import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { MSG, bdecode, bencode, hashAnswer, makeTorrent, parseSize, pexMessage, startFakePeer, type FakePeer, type PeerBehaviour } from "../src/index.ts";
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

/**
 * A peer that holds the pieces `holds` says, unchokes the other side, serves
 * what it asks, and asks for piece 0 once unchoked.
 */
function trader(port: number, holds: (piece: number) => boolean): Promise<{ leecherAsked: number; unchoked: boolean; gotGood: boolean }> {
	const out = { leecherAsked: 0, unchoked: false, gotGood: false };
	return new Promise((resolve) => {
		const socket = net.connect({ port, host: "127.0.0.1" });
		const done = () => {
			clearTimeout(timer);
			socket.destroy();
			resolve(out);
		};
		const timer = setTimeout(done, 3_000);
		socket.on("error", () => {});
		const send = (id: number, payload: Uint8Array = new Uint8Array(0)) => {
			const b = Buffer.alloc(5 + payload.length);
			b.writeUInt32BE(1 + payload.length);
			b[4] = id;
			b.set(payload, 5);
			socket.write(b);
		};
		let buf = Buffer.alloc(0);
		let handshaken = false;
		socket.on("connect", () => {
			const hs = Buffer.alloc(68);
			hs[0] = 19;
			hs.write("BitTorrent protocol", 1, "latin1");
			hs[27] = 0x04;
			Buffer.from(torrent.infoHash).copy(hs, 28);
			Buffer.from("-TT0001-000000000000").copy(hs, 48);
			socket.write(hs);
		});
		socket.on("data", (chunk) => {
			buf = Buffer.concat([buf, chunk]);
			if (!handshaken) {
				if (buf.length < 68) return;
				handshaken = true;
				buf = buf.subarray(68);
				const bits = Buffer.alloc(Math.ceil(torrent.pieceCount / 8));
				for (let p = 0; p < torrent.pieceCount; p++) if (holds(p)) bits[p >> 3] |= 0x80 >> (p & 7);
				send(MSG.BITFIELD, bits);
				send(MSG.INTERESTED);
			}
			while (buf.length >= 4 && buf.length >= 4 + buf.readUInt32BE(0)) {
				const len = buf.readUInt32BE(0);
				const id = len ? buf[4] : -1;
				const p = buf.subarray(5, 4 + len);
				buf = buf.subarray(4 + len);
				if (id === MSG.INTERESTED) send(MSG.UNCHOKE);
				else if (id === MSG.REQUEST) {
					out.leecherAsked++;
					const piece = p.readUInt32BE(0);
					const begin = p.readUInt32BE(4);
					const length = p.readUInt32BE(8);
					const start = piece * torrent.pieceLength + begin;
					send(MSG.PIECE, Buffer.concat([p.subarray(0, 8), torrent.data.subarray(start, start + length)]));
				} else if (id === MSG.UNCHOKE) {
					out.unchoked = true;
					const r = Buffer.alloc(12);
					r.writeUInt32BE(16384, 8);
					send(MSG.REQUEST, r);
				} else if (id === MSG.PIECE) {
					out.gotGood = Buffer.from(p.subarray(8)).equals(Buffer.from(torrent.data.subarray(0, 16384)));
					done();
				}
			}
		});
	});
}

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

	it("carries given bytes instead of noise", () => {
		const content = new Uint8Array(40_000).map((_, i) => i % 251);
		const t = makeTorrent({ content, name: "clip.mp4", pieceLength: 16384 });
		expect(t.totalLength).toBe(40_000);
		expect(t.data).toEqual(content);
		expect(t.pieceCount).toBe(3);
		expect(() => makeTorrent({ content, files: [{ path: ["a"], length: 1 }] })).toThrow(RangeError);
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

	it("tit-for-tat leecher: unchokes only a peer that uploads to it", async () => {
		const mine = [0, 1, 2, 3];
		const p = await peer({ titForTat: true, have: mine });
		// Interest alone gets nothing.
		const idle = await download(torrent, { port: p.port, timeoutMs: 400 });
		expect(idle.unchoked).toBe(false);
		expect(p.stats.titForTatUnchokes).toBe(0);
		// A peer that has the other pieces and serves them gets unchoked, then served.
		const r = await trader(p.port, (piece) => !mine.includes(piece));
		expect(r.leecherAsked).toBeGreaterThan(0);
		expect(r.unchoked).toBe(true);
		expect(r.gotGood).toBe(true);
		expect(p.stats.titForTatUnchokes).toBe(1);
		expect(p.stats.received.unchoke).toBe(1);
		expect(p.stats.received.bitfield).toBe(1);
		expect(p.stats.received.bytes).toBeGreaterThanOrEqual(16 * 1024);
		expect(p.stats.received.badBlocks).toBe(0);
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

describe("v2, hybrid and multi-file torrents", () => {
	const files = [
		{ path: ["b", "movie.mkv"], length: 300_000 },
		{ path: ["a.srt"], length: 1_000 },
		{ path: ["empty.txt"], length: 0 },
		{ path: ["b", "extra.bin"], length: 70_000 },
	];

	it("lays v1 multi-file torrents back to back", () => {
		const t = makeTorrent({ seed: 3, files, pieceLength: 32 * 1024 });
		expect(t.files.map((f) => [f.path, f.offset, f.length])).toEqual([
			["b/movie.mkv", 0, 300_000],
			["a.srt", 300_000, 1_000],
			["empty.txt", 301_000, 0],
			["b/extra.bin", 301_000, 70_000],
		]);
		expect(t.totalLength).toBe(371_000);
		const info = bdecode(t.info)!.value as Map<string, unknown>;
		expect((info.get("files") as unknown[]).length).toBe(4);
	});

	it("aligns v2 files to pieces, in file-tree order, with shorter last pieces", () => {
		const t = makeTorrent({ seed: 3, files, pieceLength: 32 * 1024, version: 2 });
		expect(t.files.map((f) => [f.path, f.offset])).toEqual([
			["a.srt", 0],
			["b/extra.bin", 32 * 1024],
			["b/movie.mkv", 32 * 1024 * 4],
			["empty.txt", 32 * 1024 * 4 + 300_000],
		]);
		expect(t.pieceSize(0)).toBe(1_000);
		expect(t.pieceSize(3)).toBe(70_000 - 2 * 32 * 1024);
		expect(t.pieceSize(4)).toBe(32 * 1024);
		expect(t.infoHashV1).toBeUndefined();
		expect(createHash("sha256").update(t.info).digest().subarray(0, 20).equals(Buffer.from(t.infoHash))).toBe(true);
		expect(t.magnet).toMatch(/^magnet:\?xt=urn:btmh:1220[0-9a-f]{64}&dn=/);
		const info = bdecode(t.info)!.value as Map<string, unknown>;
		expect(info.get("meta version")).toBe(2);
		expect(info.has("pieces")).toBe(false);
		// Piece layers only for files larger than one piece; each matches its root.
		const movie = t.files.find((f) => f.path === "b/movie.mkv")!;
		const layer = t.pieceLayers.get(Buffer.from(movie.piecesRoot!).toString("hex"))!;
		expect(layer.length).toBe(Math.ceil(300_000 / (32 * 1024)) * 32);
		expect(t.pieceLayers.has(Buffer.from(t.files[0].piecesRoot!).toString("hex"))).toBe(false);
	});

	it("serves hash requests with proofs up to the root", () => {
		const t = makeTorrent({ seed: 3, files, pieceLength: 32 * 1024, version: 2 });
		const movie = t.files.find((f) => f.path === "b/movie.mkv")!;
		const tree = t.trees.get(Buffer.from(movie.piecesRoot!).toString("hex"))!;
		const req = Buffer.alloc(48);
		Buffer.from(movie.piecesRoot!).copy(req);
		req.writeUInt32BE(tree.pieceLayer, 32);
		req.writeUInt32BE(4, 36);
		req.writeUInt32BE(4, 40);
		req.writeUInt32BE(10, 44);
		const answer = hashAnswer(t, req, "serve")!;
		// 4 hashes, then uncles from the subtree's level up to (not including) the root.
		const hashes = (answer.length - 48) / 32;
		expect(hashes).toBe(4 + (tree.layers.length - 1 - (tree.pieceLayer + 2)));
		let node = createHash("sha256")
			.update(Buffer.concat([createHash("sha256").update(answer.subarray(48, 112)).digest(), createHash("sha256").update(answer.subarray(112, 176)).digest()]))
			.digest();
		for (let i = 4, at = 1; i < hashes; i++, at >>= 1) {
			const uncle = answer.subarray(48 + i * 32, 80 + i * 32);
			node = createHash("sha256").update(at & 1 ? Buffer.concat([uncle, node]) : Buffer.concat([node, uncle])).digest();
		}
		expect(node.equals(Buffer.from(movie.piecesRoot!))).toBe(true);
		expect(hashAnswer(t, req, "reject")).toBeUndefined();
		req.writeUInt32BE(3, 36); // not a multiple of the length
		expect(hashAnswer(t, req, "serve")).toBeUndefined();
	});

	it("hybrids pad v1 pieces with zeros and carry both hashes", () => {
		const t = makeTorrent({ seed: 3, files, pieceLength: 32 * 1024, version: "hybrid" });
		expect(t.files.filter((f) => f.pad).map((f) => f.length)).toEqual([32 * 1024 - 1_000, 32 * 1024 * 3 - 70_000]);
		expect(t.pieceSize(0)).toBe(32 * 1024);
		expect(t.data.subarray(1_000, 32 * 1024).every((b) => b === 0)).toBe(true);
		expect(createHash("sha1").update(t.info).digest().equals(Buffer.from(t.infoHash))).toBe(true);
		expect(t.infoHashV2).toHaveLength(32);
		expect(t.magnet).toContain("urn:btih:");
		expect(t.magnet).toContain("urn:btmh:1220");
	});
});

describe("outbound only (behind NAT)", () => {
	/** A listener that answers a handshake and reports the peer's first bytes. */
	function listener(): Promise<{ port: number; got: Promise<Buffer>; server: net.Server }> {
		return new Promise((resolve) => {
			let deliver!: (b: Buffer) => void;
			const got = new Promise<Buffer>((r) => (deliver = r));
			const server = net.createServer((socket) => {
				let buf = Buffer.alloc(0);
				socket.on("data", (chunk) => {
					buf = Buffer.concat([buf, chunk]);
					if (buf.length >= 68 && buf.length - chunk.length < 68) {
						const hs = Buffer.from(buf.subarray(0, 68));
						hs.write("-GW0001-gatewaygatewa", 48, "latin1");
						socket.write(hs);
					}
					// The handshake, then the extension handshake and have-all after ours.
					if (buf.length > 68 + 5) deliver(buf);
				});
			});
			server.listen(0, "127.0.0.1", () => resolve({ port: (server.address() as net.AddressInfo).port, got, server }));
		});
	}

	it("does not listen, dials out and starts the handshake", async () => {
		const gw = await listener();
		const p = await peer({ listen: false, connectTo: [`127.0.0.1:${gw.port}`] });
		expect(p.port).toBe(0);
		expect(p.addr).toBe("");
		const bytes = await gw.got;
		expect(bytes[0]).toBe(19);
		expect(Buffer.from(bytes.subarray(28, 48)).equals(Buffer.from(torrent.infoHash))).toBe(true);
		await until(() => p.stats.handshakes === 1);
		gw.server.close();
	});

	it("dials the peers an HTTP tracker names", async () => {
		const gw = await listener();
		let announces = 0;
		const tracker = net.createServer((s) =>
			s.once("data", () => {
				announces++;
				const body = Buffer.concat([Buffer.from("d8:intervali60e5:peers6:"), Buffer.from([127, 0, 0, 1, gw.port >> 8, gw.port & 0xff]), Buffer.from("e")]);
				s.end(Buffer.concat([Buffer.from(`HTTP/1.1 200 OK\r\nContent-Length: ${body.length}\r\nConnection: close\r\n\r\n`), body]));
			}),
		);
		await new Promise<void>((r) => tracker.listen(0, "127.0.0.1", () => r()));
		const url = `http://127.0.0.1:${(tracker.address() as net.AddressInfo).port}/announce`;
		const p = await peer({ listen: false, tracker: url, announceIntervalMs: 100 });
		await gw.got;
		await until(() => p.stats.handshakes === 1 && announces >= 2);
		// One connection per address, however often the tracker names it.
		expect(p.stats.connections).toBe(1);
		tracker.close();
		gw.server.close();
	});
});

async function until(pred: () => boolean, ms = 5_000): Promise<void> {
	const end = Date.now() + ms;
	while (!pred()) {
		if (Date.now() > end) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 10));
	}
}
