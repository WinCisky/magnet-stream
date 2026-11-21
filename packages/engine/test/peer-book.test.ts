// Peer book and ut_pex (BEP 11): compact messages, dedup, the address
// filter, back-off, dial priority, and learning peers from a connected
// peer with no discovery service involved.

import { afterEach, describe, expect, it } from "vitest";
import { formatIp } from "@magnet-stream/tunnel-protocol";
import { bencode, makeTorrent, pexMessage, startFakePeer, type FakePeer } from "@magnet-stream/fake-peer";
import { PeerConnection, decodePex } from "../src/index.ts";
import { MAX_PEX_PER_MESSAGE } from "../src/extension.ts";
import { PeerBook, PexSource, addPex, type PeerSource } from "../src/peer-book.ts";
import { encodeBencode } from "../src/bencode.ts";
import { TcpTransport } from "../examples/tcp-transport.ts";

const EXT = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
const V6 = "[2606:4700:4700::1111]:51413";

function clock(start = 1_000_000) {
	const now = { t: start };
	return { now, book: new PeerBook({ now: () => now.t }) };
}

describe("ut_pex messages", () => {
	it("decodes compact IPv4 and IPv6, and caps a flooded message", () => {
		const v6 = new Uint8Array(18);
		v6.set([0x26, 0x06, 0x47, 0x00, 0x47, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0x11, 0x11, 0x14, 0x15]);
		const msg = decodePex(
			encodeBencode({
				added: Uint8Array.of(1, 2, 3, 4, 0x1a, 0xe1),
				"added.f": Uint8Array.of(0x02),
				added6: v6,
				"added6.f": Uint8Array.of(0x10),
				dropped: Uint8Array.of(9, 9, 9, 9, 0x1a, 0xe1, 1),
			}),
		);
		expect(msg.added.map((a) => [a.family, a.port, a.flags])).toEqual([
			[4, 6881, 0x02],
			[6, 5141, 0x10],
		]);
		expect(msg.dropped).toHaveLength(1);

		const flood = new Uint8Array((MAX_PEX_PER_MESSAGE + 40) * 6);
		for (let i = 0; i < MAX_PEX_PER_MESSAGE + 40; i++) {
			flood[i * 6] = 1;
			flood[i * 6 + 1] = 2;
			flood[i * 6 + 2] = 3;
			flood[i * 6 + 3] = i & 0xff;
			flood[i * 6 + 4] = 0x1a;
			flood[i * 6 + 5] = 0xe1;
		}
		expect(decodePex(encodeBencode({ added: flood })).added).toHaveLength(MAX_PEX_PER_MESSAGE);
		expect(() => decodePex(Uint8Array.of(1, 2, 3))).toThrow(/extension/);
	});
});

describe("PeerBook", () => {
	it("keeps an IPv4 and an IPv6 address once, merging origins and flags", () => {
		const book = new PeerBook();
		const added = decodePex(bencode(pexMessage(["1.2.3.4:6881", V6]))).added;
		expect(addPex(book, added)).toBe(2);
		expect(addPex(book, added)).toBe(0);
		expect(book.size).toBe(2);
		expect(book.add("1.2.3.4:6881", "discovery")).toBe(false);
		expect(book.add("[2606:4700:4700:0:0:0:0:1111]:51413", "magnet")).toBe(false);
		const v4 = book.get("1.2.3.4:6881")!;
		const v6 = book.get(V6)!;
		expect([...v4.origins].sort()).toEqual(["discovery", "pex"]);
		expect(v4.seedHint).toBe(true);
		expect([...v6.origins].sort()).toEqual(["magnet", "pex"]);
		expect(v6.state).toBe("new");
		expect(book.rejected).toEqual({ invalid: 0, forbidden: 0, banned: 0, full: 0, batch: 0 });
	});

	it("discards private, reserved and low-port addresses before they can be dialled", () => {
		const book = new PeerBook();
		const rejected = [
			"10.1.2.3:6881",
			"127.0.0.1:6881",
			"192.168.0.1:6881",
			"100.64.0.1:6881",
			"169.254.1.1:6881",
			"203.0.113.7:51413",
			"[::1]:6881",
			"[fe80::1]:6881",
			"[2001:db8::1]:6881",
			"[::ffff:10.0.0.1]:6881",
			"1.2.3.4:80",
			"1.2.3.4:1023",
			"8.8.8.8:0",
			"not-an-address",
			"example.com:6881",
		];
		expect(book.addMany(rejected, "pex")).toBe(0);
		expect(book.size).toBe(0);
		expect(book.candidates(10)).toEqual([]);
		expect(book.addressForOpen("10.1.2.3:6881")).toBeUndefined();
		expect(book.rejected.forbidden).toBeGreaterThan(0);
		expect(book.rejected.invalid).toBeGreaterThan(0);
		// The port rule stays even when loopback is allowed (local tests).
		const local = new PeerBook({ allowPrivate: true });
		expect(local.add("127.0.0.1:6881", "manual")).toBe(true);
		expect(local.add("127.0.0.1:80", "manual")).toBe(false);
		expect(local.addressForOpen("127.0.0.1:6881")?.port).toBe(6881);
	});

	it("never proposes a peer banned for corrupt data again, even once evicted and named again by pex", () => {
		const book = new PeerBook({ maxPeers: 2 });
		book.add("1.2.3.4:6881", "discovery");
		book.markConnecting("1.2.3.4:6881");
		book.markConnected("1.2.3.4:6881");
		book.markFailed("1.2.3.4:6881", "corrupt");
		expect(book.isBanned("1.2.3.4:6881")).toBe(true);
		expect(book.addressForOpen("1.2.3.4:6881")).toBeUndefined();
		book.add("1.2.3.5:6881", "pex");
		// Full: the banned entry is the one evicted, but the ban stays.
		expect(book.add("1.2.3.6:6881", "pex")).toBe(true);
		expect(book.get("1.2.3.4:6881")).toBeUndefined();
		expect(book.add("1.2.3.4:6881", "pex")).toBe(false);
		expect(book.rejected.banned).toBe(1);
		expect(book.candidates(10).map((p) => p.key)).not.toContain("1.2.3.4:6881");
		book.ban("1.2.3.5:6881", "corrupt");
		expect(book.get("1.2.3.5:6881")?.lastFailure).toBe("corrupt");
		expect(book.candidates(10).map((p) => p.key)).toEqual(["1.2.3.6:6881"]);
	});

	it("does not propose a failed address again before its back-off", () => {
		const { now, book } = clock();
		book.add("1.2.3.4:6881", "discovery");
		book.add("1.2.3.5:6881", "discovery");
		book.markFailed("1.2.3.4:6881", "refused");
		book.markFailed("1.2.3.5:6881", "not_bittorrent");
		expect(book.candidates(10)).toEqual([]);
		expect(book.get("1.2.3.5:6881")?.banned).toBe(true);
		now.t = book.get("1.2.3.4:6881")!.retryAt - 1;
		expect(book.candidates(10)).toEqual([]);
		now.t = book.get("1.2.3.4:6881")!.retryAt;
		expect(book.candidates(10).map((p) => p.key)).toEqual(["1.2.3.4:6881"]);
		// A second refusal waits longer; a peer that is not BitTorrent never returns.
		book.markFailed("1.2.3.4:6881", "refused");
		expect(book.get("1.2.3.4:6881")!.retryAt - now.t).toBe(2 * 10 * 60_000);
		now.t += 24 * 60 * 60_000;
		expect(book.candidates(10).map((p) => p.key)).toEqual(["1.2.3.4:6881"]);
		expect(book.addressForOpen("1.2.3.5:6881")).toBeUndefined();
	});

	it("puts back a peer whose dial never happened (no free tunnel slot) without a penalty", () => {
		const { book } = clock();
		book.add("8.8.8.8:6881", "discovery");
		const [p] = book.candidates(1);
		book.markConnecting(p.key);
		book.markNotDialled(p.key);
		const again = book.get(p.key)!;
		expect(again.state).toBe("new");
		expect(again.failures).toBe(0);
		expect(book.untried).toBe(1);
		expect(book.candidates(1)[0]?.key).toBe(p.key);
	});

	it("orders candidates by what they have done, not by arrival", () => {
		const { now, book } = clock();
		book.add("1.2.3.1:6881", "discovery");
		now.t += 1;
		book.add("1.2.3.2:6881", "pex", 0x02);
		now.t += 1;
		book.add("1.2.3.3:6881", "discovery");
		book.markConnected("1.2.3.3:6881");
		book.markDelivered("1.2.3.3:6881", 1_000_000);
		book.markClosed("1.2.3.3:6881");
		now.t += 1;
		book.add("1.2.3.4:6881", "discovery", 0x10);
		now.t += 1;
		book.add("1.2.3.5:6881", "discovery");
		book.markFailed("1.2.3.5:6881", "timeout");
		now.t = book.get("1.2.3.5:6881")!.retryAt;
		book.add("1.2.3.6:6881", "discovery");
		book.markConnecting("1.2.3.6:6881");
		expect(book.candidates(10).map((p) => p.key)).toEqual(["1.2.3.3:6881", "1.2.3.2:6881", "1.2.3.4:6881", "1.2.3.1:6881", "1.2.3.5:6881"]);
		// A peer that delivered comes back sooner than the reason's full back-off.
		book.markFailed("1.2.3.3:6881", "remote_closed");
		expect(book.get("1.2.3.3:6881")!.retryAt - now.t).toBe(15_000);
	});

	it("caps a batch and the book, and evicts a failed peer before a new one", () => {
		const book = new PeerBook({ maxPeers: 2, maxPerBatch: 2 });
		expect(book.addMany(["1.2.3.1:6881", "1.2.3.2:6881", "1.2.3.3:6881", "nope"], "discovery")).toBe(2);
		expect(book.rejected.batch).toBe(2);
		expect(book.add("1.2.3.9:6881", "discovery")).toBe(false);
		expect(book.rejected.full).toBe(1);
		book.markFailed("1.2.3.1:6881", "timeout");
		expect(book.add("1.2.3.9:6881", "discovery")).toBe(true);
		expect(book.get("1.2.3.1:6881")).toBeUndefined();
		expect(book.size).toBe(2);
		expect(book.addressForOpen("1.2.3.9:6881")?.port).toBe(6881);
	});

	it("takes addresses from any PeerSource through one sink", () => {
		const book = new PeerBook();
		const source: PeerSource = {
			start(sink) {
				sink(["1.2.3.4:6881", "1.2.3.4:6881", "10.0.0.1:6881"], "discovery");
			},
			stop() {},
		};
		source.start(book.sink);
		source.stop();
		expect(book.size).toBe(1);
		expect(book.get("1.2.3.4:6881")?.origins.has("discovery")).toBe(true);
		expect(book.counts()).toMatchObject({ known: 1, untried: 1, connected: 0 });
	});
});

describe("discovery by peer exchange", () => {
	const peers: FakePeer[] = [];
	afterEach(async () => {
		await Promise.all(peers.splice(0).map((p) => p.close()));
	});

	async function until(pred: () => boolean, ms = 2_000): Promise<void> {
		const deadline = Date.now() + ms;
		while (!pred()) {
			if (Date.now() > deadline) throw new Error("timed out waiting for pex");
			await new Promise((r) => setTimeout(r, 10));
		}
	}

	it("learns the peers a connected peer announces, and dials them, with no discovery request", async () => {
		const torrent = makeTorrent({ seed: 61, size: 32 * 1024, pieceLength: 16 * 1024 });
		const v4 = await startFakePeer({ torrent });
		const v6 = await startFakePeer({ torrent, host: "::1" });
		const announcer = await startFakePeer({
			torrent,
			pex: [v4.addr, v6.addr, v4.addr, "1.2.3.4:80", V6],
		});
		peers.push(v4, v6, announcer);

		const book = new PeerBook({ allowPrivate: true });
		const source = new PexSource();
		source.start(book.sink);
		const transport = await TcpTransport.connect({ port: announcer.port, infoHash: torrent.infoHash, reserved: EXT });
		const conn = new PeerConnection(transport, { keepAliveMs: 0, events: { pex: (m) => source.ingest(m) } });

		await until(() => book.get(v4.addr) !== undefined && book.get(v6.addr) !== undefined);
		expect(announcer.stats.pexSent).toBe(1);
		expect(book.get(v4.addr)?.seedHint).toBe(true);
		expect(book.get(v6.addr)?.origins.has("pex")).toBe(true);
		expect([...book.entries()].filter((p) => p.key === v4.addr)).toHaveLength(1);
		// Port < 1024 is refused even when loopback peers are allowed for the test.
		expect(book.get("1.2.3.4:80")).toBeUndefined();
		expect(book.rejected.forbidden).toBe(1);
		expect(book.get(V6)?.state).toBe("new");
		source.stop();
		expect(source.ingest({ added: [{ family: 4, ip: Uint8Array.of(8, 8, 8, 8), port: 6881, flags: 0 }], dropped: [] })).toBe(0);

		const wanted = new Set([v4.addr, v6.addr]);
		for (const entry of book.candidates(10)) {
			if (!wanted.has(entry.key)) continue;
			const address = book.addressForOpen(entry.key);
			expect(address, entry.key).toBeDefined();
			book.markConnecting(entry.key);
			const dialed = await TcpTransport.connect({
				host: formatIp(address!.family, address!.ip),
				port: address!.port,
				infoHash: torrent.infoHash,
				reserved: EXT,
			});
			const peer = new PeerConnection(dialed, { keepAliveMs: 0 });
			expect(peer.closed).toBe(false);
			book.markConnected(entry.key);
			peer.close();
			wanted.delete(entry.key);
		}
		expect(wanted.size).toBe(0);
		expect(book.counts().connected).toBe(2);
		conn.close();
	});

	it("never offers a private or low-port address learned by pex for dial", async () => {
		const torrent = makeTorrent({ seed: 62, size: 16 * 1024, pieceLength: 16 * 1024 });
		const announcer = await startFakePeer({
			torrent,
			pex: ["127.0.0.1:6881", "[::1]:6881", "10.1.2.3:6881", "1.2.3.4:80", "[2001:db8::5]:51413"],
		});
		peers.push(announcer);
		const book = new PeerBook();
		const source = new PexSource();
		source.start(book.sink);
		const transport = await TcpTransport.connect({ port: announcer.port, infoHash: torrent.infoHash, reserved: EXT });
		const conn = new PeerConnection(transport, { keepAliveMs: 0, events: { pex: (m) => source.ingest(m) } });
		await until(() => announcer.stats.pexSent === 1 && book.rejected.forbidden > 0);
		expect(book.size).toBe(0);
		expect(book.candidates(10)).toEqual([]);
		expect(book.addressForOpen("127.0.0.1:6881")).toBeUndefined();
		conn.close();
	});
});

describe("PeerBook and the VPS scout", () => {
	it("dials live seeds, then live partial peers, then unknown addresses; dead ones only once pex names them again", () => {
		const { book } = clock();
		book.add("8.8.0.1:6881", "discovery", 0, { score: 0.02, alive: false });
		book.add("8.8.0.2:6881", "discovery", 0, { score: 0.2 });
		book.add("8.8.0.3:6881", "discovery");
		book.add("8.8.0.4:6881", "discovery", 0, { score: 0.6, alive: true, have: 0.9, md: true, unchoked: true });
		book.add("8.8.0.5:6881", "discovery", 0, { score: 0.55, alive: true, seed: true, have: 1 });
		book.add("8.8.0.6:6881", "discovery", 0, { score: 0.9, alive: true, seed: true, have: 1, md: true });
		book.add("8.8.0.7:6881", "pex", 0x02);
		expect(book.candidates(10).map((p) => p.key)).toEqual([
			"8.8.0.6:6881",
			"8.8.0.5:6881",
			"8.8.0.4:6881",
			"8.8.0.7:6881",
			"8.8.0.2:6881",
			"8.8.0.3:6881",
		]);
		// A peer names the dead address: it may have come back, and goes last.
		book.add("8.8.0.1:6881", "pex");
		expect(book.candidates(10).map((p) => p.key).at(-1)).toBe("8.8.0.1:6881");
		// Or every dead one goes last when the book is told so.
		const all = new PeerBook({ dialScoutDead: true });
		all.add("8.8.0.1:6881", "discovery", 0, { score: 0.02, alive: false });
		all.add("8.8.0.2:6881", "discovery");
		expect(all.candidates(10).map((p) => p.key)).toEqual(["8.8.0.2:6881", "8.8.0.1:6881"]);
		expect(book.dialClass(book.get("8.8.0.6:6881")!)).toBe("scout");
		expect(book.dialClass(book.get("8.8.0.2:6881")!)).toBe("untried");
		expect(book.dialClass(book.get("8.8.0.1:6881")!)).toBe("untried");
	});

	it("replaces the scout's word with what the engine measured", () => {
		const { now, book } = clock();
		const live = "8.8.0.1:6881";
		const other = "8.8.0.2:6881";
		book.add(live, "discovery", 0, { score: 0.95, alive: true, seed: true });
		book.add(other, "discovery", 0, { score: 0.6, alive: true });
		book.markConnecting(live);
		book.markFailed(live, "timeout");
		const entry = book.get(live)!;
		// Untried from now on, normal back-off, below the live partial peer.
		expect(book.dialClass(entry)).toBe("untried");
		expect(entry.retryAt).toBe(now.t + 5 * 60_000);
		now.t += 5 * 60_000;
		expect(book.candidates(2).map((p) => p.key)).toEqual([other, live]);
		// A later discovery line does not resurrect it.
		book.add(live, "discovery", 0, { score: 0.95, alive: true, seed: true });
		expect(book.dialClass(entry)).toBe("untried");

		book.markConnecting(other);
		book.markConnected(other);
		expect(book.dialClass(book.get(other)!)).toBe("known");
	});

	it("takes scout facts from discovery only, and drops facts out of range", () => {
		const { book } = clock();
		book.add("8.8.0.1:6881", "pex", 0, { score: 0.9, alive: true, seed: true });
		expect(book.get("8.8.0.1:6881")!.scout).toBeUndefined();
		book.add("8.8.0.2:6881", "discovery", 0, { score: 3, alive: true });
		book.add("8.8.0.3:6881", "discovery", 0, { score: 0.5, alive: true, have: Number.NaN });
		book.add("8.8.0.4:6881", "discovery", 0, { score: 0.5, alive: "yes" as unknown as boolean });
		for (const k of ["8.8.0.2:6881", "8.8.0.3:6881", "8.8.0.4:6881"]) {
			expect(book.get(k)!.scout, k).toBeUndefined();
			expect(book.dialClass(book.get(k)!)).toBe("untried");
		}
		// A known address learns the scout's facts from a later discovery line.
		book.add("8.8.0.1:6881", "discovery", 0, { score: 0.9, alive: true, seed: true });
		expect(book.dialClass(book.get("8.8.0.1:6881")!)).toBe("scout");
	});

	it("ranks a peer vouched for by a deliverer above other pex addresses", () => {
		const { book } = clock();
		book.add("8.8.0.1:6881", "pex");
		book.add("8.8.0.2:6881", "pex");
		book.add("8.8.0.3:6881", "discovery", 0, { score: 0.6, alive: true });
		book.vouch("8.8.0.2:6881");
		expect(book.candidates(10).map((p) => p.key)).toEqual(["8.8.0.3:6881", "8.8.0.2:6881", "8.8.0.1:6881"]);
	});
});
