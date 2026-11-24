// @magnet-stream/harness: helpers shared by integration and e2e tests.
//   - freePort / reservePorts: free loopback TCP ports
//   - startSwarm: several fake peers for one torrent, stopped together
//   - startWranglerDev: the gateway under `wrangler dev`, stopped with its workerd
//   - startVpsGateway: the VPS service (services/seeders) and its tunnel gateway
//   - startGateway: either, by E2E_GATEWAY (`cloudflare`, the default, or `vps`)
//   - startFakeTracker: a BEP 3 HTTP tracker that records announces
//   - startFewSeedersStack: both gateways (Cloudflare's under wrangler dev,
//     the VPS one with an inbound port) and a fake tracker, all on loopback
//
// Everything stays on loopback: the VPS gateway's DHT bootstraps from
// nobody and nothing is announced to public trackers or the DHT
// (HERMETIC_VPS_VARS, checked at start).

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
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

export const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

let vpsBinary: string | undefined;

/** The service binary: VPS_GATEWAY_BIN, or a debug build made once per process. */
function vpsGatewayBinary(): string {
	if (process.env.VPS_GATEWAY_BIN) return process.env.VPS_GATEWAY_BIN;
	if (vpsBinary) return vpsBinary;
	const build = spawnSync("cargo", ["build", "--quiet", "-p", "magnet-seeders"], { cwd: REPO_ROOT, stdio: "inherit" });
	if (build.status !== 0) throw new Error(`cargo build failed (${build.status ?? build.signal})`);
	vpsBinary = join(REPO_ROOT, "target", "debug", "magnet-seeders");
	return vpsBinary;
}

export interface VpsGatewayOptions {
	port?: number;
	/**
	 * Environment of the service (`TUNNEL_*`, `ALLOWED_ORIGINS`,
	 * `ALLOW_PRIVATE_PEERS`, …), the same names as the Cloudflare gateway's
	 * vars, over HERMETIC_VPS_VARS.
	 */
	vars?: Record<string, string>;
	timeoutMs?: number;
}

/**
 * What every VPS gateway of the tests starts with: its DHT node bootstraps
 * from nobody, announces go to no DHT and to no default tracker. Only the
 * trackers a test names (on loopback) are ever contacted.
 */
export const HERMETIC_VPS_VARS: Readonly<Record<string, string>> = {
	DHT_PORT: "0",
	DHT_BOOTSTRAP: "none",
	TUNNEL_ANNOUNCE_DHT: "0",
	DEFAULT_TRACKERS: "",
};

/** A host the tests may reach: loopback only. */
export function isLoopbackHost(host: string): boolean {
	const h = host.replace(/^\[|\]$/g, "").toLowerCase();
	return h === "localhost" || h === "::1" || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(h);
}

/** Throws unless `url` (a tracker, a gateway) points at loopback. */
export function assertLoopbackUrl(url: string): void {
	let host: string;
	try {
		host = new URL(url).hostname;
	} catch {
		throw new Error(`not a URL: ${url}`);
	}
	if (!isLoopbackHost(host)) throw new Error(`the test harness reaches loopback only, not ${host}`);
}

/** Refuse a VPS environment that would announce to, or bootstrap from, the public DHT or trackers. */
function checkHermetic(env: Record<string, string | undefined>): void {
	if (env.DHT_BOOTSTRAP?.trim().toLowerCase() !== "none") {
		for (const node of (env.DHT_BOOTSTRAP ?? "").split(",").map((n) => n.trim())) {
			const host = node.slice(0, node.lastIndexOf(":"));
			if (!node || !isLoopbackHost(host)) throw new Error(`DHT_BOOTSTRAP must be "none" or loopback nodes in tests, got ${JSON.stringify(env.DHT_BOOTSTRAP)}`);
		}
	}
	for (const tracker of (env.DEFAULT_TRACKERS ?? "").split(",").map((t) => t.trim())) if (tracker) assertLoopbackUrl(tracker);
}

/** The VPS gateway of a test: also crashes (SIGKILL) and comes back on the same port and environment. */
export interface VpsGateway extends WranglerDev {
	/** Kill the process at once (no graceful shutdown: sockets reset, no `stopped` announce). */
	crash(): Promise<void>;
	/** Start it again after a crash or a stop, same port and environment (output() keeps both runs). */
	restart(): Promise<void>;
}

/**
 * Start the VPS service (its `/v1/tunnel` is the fallback gateway) on a
 * loopback port and wait until it answers. Its DHT node takes an ephemeral
 * UDP port and bootstraps from nobody (HERMETIC_VPS_VARS). Same interface
 * as startWranglerDev, plus crash() and restart().
 */
export async function startVpsGateway(opts: VpsGatewayOptions = {}): Promise<VpsGateway> {
	const port = opts.port ?? (await freePort());
	let log = "";
	// A clean environment: only what the test chooses (no stray ALLOWED_ORIGINS, TUNNEL_*, ...).
	const env: Record<string, string | undefined> = {
		PATH: process.env.PATH,
		RUST_LOG: "info",
		NO_COLOR: "1",
		BIND_ADDR: `127.0.0.1:${port}`,
		// Every session of a test run comes from 127.0.0.1: the per-IP announce
		// limits are sized for one browser, not for a whole e2e suite.
		TUNNEL_ANNOUNCES_PER_IP: "64",
		TUNNEL_ANNOUNCE_ROUNDS_PER_IP_PER_MINUTE: "120",
		...HERMETIC_VPS_VARS,
		...opts.vars,
	};
	checkHermetic(env);
	const bin = vpsGatewayBinary();
	const url = `http://127.0.0.1:${port}`;
	let child: ChildProcess | undefined;
	const killOnExit = () => {
		try {
			child?.kill("SIGKILL");
		} catch {}
	};
	const running = () => child !== undefined && child.exitCode === null && child.signalCode === null;
	const exited = (c: ChildProcess) => new Promise<void>((r) => (c.exitCode !== null || c.signalCode !== null ? r() : c.once("exit", () => r())));
	const stop = async () => {
		process.off("exit", killOnExit);
		const c = child;
		if (!c || !running()) return;
		c.kill("SIGTERM");
		await Promise.race([exited(c), new Promise((r) => setTimeout(r, 5_000))]);
		if (c.exitCode === null && c.signalCode === null) {
			c.kill("SIGKILL");
			await exited(c);
		}
	};
	const crash = async () => {
		process.off("exit", killOnExit);
		const c = child;
		if (!c || !running()) return;
		c.kill("SIGKILL");
		await exited(c);
	};
	const launch = async () => {
		if (running()) throw new Error("the VPS gateway is running");
		const c = spawn(bin, [], { stdio: ["ignore", "pipe", "pipe"], env: env as NodeJS.ProcessEnv });
		child = c;
		process.once("exit", killOnExit);
		c.stdout?.on("data", (d) => (log += d));
		c.stderr?.on("data", (d) => (log += d));
		const until = Date.now() + (opts.timeoutMs ?? 60_000);
		while (Date.now() < until) {
			if (c.exitCode !== null) {
				await stop();
				throw new Error(`VPS gateway exited (${c.exitCode}):\n${log}`);
			}
			try {
				if ((await fetch(`${url}/health`)).ok) return;
			} catch {}
			await new Promise((r) => setTimeout(r, 200));
		}
		await stop();
		throw new Error(`VPS gateway did not come up:\n${log}`);
	};
	await launch();
	return { url, port, output: () => log, stop, crash, restart: launch };
}

export type GatewayKind = "cloudflare" | "vps";

/** Which gateway the e2e suites run against: E2E_GATEWAY=vps, or Cloudflare's (wrangler dev). */
export function gatewayKind(): GatewayKind {
	const kind = process.env.E2E_GATEWAY ?? "cloudflare";
	if (kind !== "cloudflare" && kind !== "vps") throw new Error(`E2E_GATEWAY must be cloudflare or vps, not ${kind}`);
	return kind;
}

/** A local gateway of the kind gatewayKind() names, with `vars` (the same names on both). */
export function startGateway(opts: { vars?: Record<string, string> } = {}): Promise<WranglerDev> {
	return gatewayKind() === "vps" ? startVpsGateway(opts) : startWranglerDev(opts);
}

export { startPeersStub, type PeersStub, type StubLine, type StubLines } from "./peers-stub.ts";
export { startFakeTracker, type FakeTracker, type FakeTrackerOptions, type TrackerAnnounce } from "./tracker.ts";
export { startFewSeedersStack, type FewSeedersStack, type FewSeedersStackOptions } from "./few-seeders.ts";
