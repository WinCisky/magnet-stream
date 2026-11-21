// @magnet-stream/harness: helpers shared by integration and e2e tests.
//   - freePort / reservePorts: free loopback TCP ports
//   - startSwarm: several fake peers for one torrent, stopped together
//   - startWranglerDev: the gateway under `wrangler dev`, stopped with its workerd

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { startFakePeer, type FakePeer, type FakeTorrent, type PeerBehaviour } from "@magnet-stream/fake-peer";

/** A TCP port free on `host` right now (ephemeral range, chosen by the OS). */
export function freePort(host = "127.0.0.1"): Promise<number> {
	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.unref();
		server.once("error", reject);
		server.listen(0, host, () => {
			const { port } = server.address() as net.AddressInfo;
			server.close(() => resolve(port));
		});
	});
}

/** `n` distinct free ports. */
export async function reservePorts(n: number, host = "127.0.0.1"): Promise<number[]> {
	const ports = new Set<number>();
	while (ports.size < n) ports.add(await freePort(host));
	return [...ports];
}

/** A port nobody listens on (connections are refused). */
export async function closedPort(host = "127.0.0.1"): Promise<number> {
	return freePort(host);
}

export interface Swarm {
	peers: FakePeer[];
	close(): Promise<void>;
}

/** One fake peer per behaviour, all serving `torrent`. */
export async function startSwarm(torrent: FakeTorrent, behaviours: PeerBehaviour[]): Promise<Swarm> {
	const peers: FakePeer[] = [];
	try {
		for (const b of behaviours) peers.push(await startFakePeer({ torrent, ...b }));
	} catch (e) {
		await Promise.all(peers.map((p) => p.close()));
		throw e;
	}
	return { peers, close: async () => void (await Promise.all(peers.map((p) => p.close()))) };
}

export interface WranglerDev {
	/** `http://127.0.0.1:<port>` */
	url: string;
	port: number;
	/** Everything wrangler printed (for failure reports; may include local addresses, never secrets we pass). */
	output(): string;
	stop(): Promise<void>;
}

export interface WranglerDevOptions {
	/** Worker directory (default: apps/gateway of this monorepo). */
	cwd?: string;
	port?: number;
	/** `--var KEY:VALUE` bindings (local dev only). */
	vars?: Record<string, string>;
	/** Health URL path polled until it answers 200 (default /health). */
	healthPath?: string;
	timeoutMs?: number;
}

export const GATEWAY_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../../../apps/gateway");

/** Start `wrangler dev` and wait until it answers. */
export async function startWranglerDev(opts: WranglerDevOptions = {}): Promise<WranglerDev> {
	const port = opts.port ?? (await freePort());
	// Own local state per instance: parallel instances would lock each other's SQLite files.
	const state = mkdtempSync(join(tmpdir(), "wrangler-dev-"));
	const args = ["wrangler", "dev", "--ip", "127.0.0.1", "--port", String(port), "--inspector-port", String(await freePort()), "--persist-to", state];
	for (const [k, v] of Object.entries(opts.vars ?? {})) args.push("--var", `${k}:${v}`);
	let log = "";
	// Own process group, so the workerd grandchild dies with it.
	const child: ChildProcess = spawn("npx", args, {
		cwd: opts.cwd ?? GATEWAY_DIR,
		stdio: ["ignore", "pipe", "pipe"],
		detached: true,
		env: { ...process.env, WRANGLER_SEND_METRICS: "false", NO_COLOR: "1" },
	});
	// Never leave a workerd behind, whatever happens to the test process.
	const killOnExit = () => {
		try {
			if (child.pid !== undefined) process.kill(-child.pid, "SIGKILL");
		} catch {}
	};
	process.once("exit", killOnExit);
	child.stdout?.on("data", (d) => (log += d));
	child.stderr?.on("data", (d) => (log += d));
	const url = `http://127.0.0.1:${port}`;
	const stop = async () => {
		const cleanup = () => {
			process.off("exit", killOnExit);
			rmSync(state, { recursive: true, force: true });
		};
		if (child.exitCode !== null || child.pid === undefined) return cleanup();
		const exited = new Promise<void>((r) => child.once("exit", () => r()));
		try {
			process.kill(-child.pid, "SIGTERM");
		} catch {}
		await Promise.race([exited, new Promise((r) => setTimeout(r, 5_000))]);
		try {
			process.kill(-child.pid, "SIGKILL");
		} catch {}
		cleanup();
	};
	const until = Date.now() + (opts.timeoutMs ?? 90_000);
	while (Date.now() < until) {
		if (child.exitCode !== null) {
			await stop();
			throw new Error(`wrangler dev exited (${child.exitCode}):\n${log}`);
		}
		try {
			if ((await fetch(`${url}${opts.healthPath ?? "/health"}`)).ok) return { url, port, output: () => log, stop };
		} catch {}
		await new Promise((r) => setTimeout(r, 300));
	}
	await stop();
	throw new Error(`wrangler dev did not come up:\n${log}`);
}

export { startPeersStub, type PeersStub, type StubLine, type StubLines } from "./peers-stub.ts";
