import net from "node:net";
import { describe, expect, it } from "vitest";
import { makeTorrent, trackerPeers } from "@magnet-stream/fake-peer";
import { assertLoopbackUrl, closedPort, freePort, isLoopbackHost, reservePorts, startFakeTracker, startSwarm, startVpsGateway } from "../src/index.ts";

describe("harness", () => {
	it("finds free, distinct ports", async () => {
		const port = await freePort();
		expect(port).toBeGreaterThan(1023);
		const ports = await reservePorts(5);
		expect(new Set(ports).size).toBe(5);
	});

	it("stays on loopback: trackers and VPS environments that reach out are refused", async () => {
		for (const host of ["127.0.0.1", "127.8.9.10", "localhost", "::1", "[::1]"]) expect(isLoopbackHost(host)).toBe(true);
		for (const host of ["10.0.0.1", "192.168.1.2", "tracker.opentrackr.org", "0.0.0.0", "1.127.0.0"]) expect(isLoopbackHost(host)).toBe(false);
		expect(() => assertLoopbackUrl("http://127.0.0.1:1/announce")).not.toThrow();
		expect(() => assertLoopbackUrl("udp://tracker.opentrackr.org:1337/announce")).toThrow(/loopback/);
		// Refused before anything is built or started.
		await expect(startVpsGateway({ vars: { DEFAULT_TRACKERS: "udp://tracker.opentrackr.org:1337/announce" } })).rejects.toThrow(/loopback/);
		await expect(startVpsGateway({ vars: { DHT_BOOTSTRAP: "router.bittorrent.com:6881" } })).rejects.toThrow(/DHT_BOOTSTRAP/);
		await expect(startVpsGateway({ vars: { DHT_BOOTSTRAP: "" } })).rejects.toThrow(/DHT_BOOTSTRAP/);
	});

	it("closedPort refuses connections", async () => {
		const port = await closedPort();
		const err = await new Promise<string>((resolve) => {
			const s = net.connect({ port, host: "127.0.0.1" });
			s.on("connect", () => resolve("connected"));
			s.on("error", (e: NodeJS.ErrnoException) => resolve(e.code ?? "error"));
		});
		expect(err).toBe("ECONNREFUSED");
	});

	it("starts and stops a swarm of fake peers", async () => {
		const torrent = makeTorrent({ seed: 2, size: 100_000, pieceLength: 16384 });
		const swarm = await startSwarm(torrent, [{}, { have: [0] }, { unchoke: false }]);
		expect(swarm.peers).toHaveLength(3);
		expect(new Set(swarm.peers.map((p) => p.port)).size).toBe(3);
		for (const p of swarm.peers) expect(p.host).toBe("127.0.0.1");
		await swarm.close();
		const refused = await new Promise<boolean>((resolve) => {
			const s = net.connect({ port: swarm.peers[0].port, host: "127.0.0.1" });
			s.on("connect", () => resolve(false));
			s.on("error", () => resolve(true));
		});
		expect(refused).toBe(true);
	});

	it("a fake tracker records announces and answers with the other peers", async () => {
		const tracker = await startFakeTracker();
		const ih = new Uint8Array(20).fill(0xab);
		const id = new Uint8Array(20).fill(1);
		expect(await trackerPeers(tracker.url, ih, id, 7001)).toEqual([]);
		expect(await trackerPeers(tracker.url, ih, id, 7002)).toEqual(["127.0.0.1:7001"]);
		expect(await trackerPeers(tracker.url, new Uint8Array(20), id, 7003)).toEqual([]);
		expect(tracker.announces.map((a) => [a.infoHash.slice(0, 4), a.port, a.event])).toEqual([
			["abab", 7001, "started"],
			["abab", 7002, "started"],
			["0000", 7003, "started"],
		]);
		await tracker.close();
	});
});
