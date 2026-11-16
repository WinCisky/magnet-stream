import net from "node:net";
import { describe, expect, it } from "vitest";
import { makeTorrent } from "@magnet-stream/fake-peer";
import { closedPort, freePort, reservePorts, startSwarm } from "../src/index.ts";

describe("harness", () => {
	it("finds free, distinct ports", async () => {
		const port = await freePort();
		expect(port).toBeGreaterThan(1023);
		const ports = await reservePorts(5);
		expect(new Set(ports).size).toBe(5);
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
});
