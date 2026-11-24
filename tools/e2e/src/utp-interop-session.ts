// uTP interoperability with real clients (M4-04f; docs/decisions/utp.md ›
// Interoperability, requirement R1): the VPS gateway (services/seeders)
// and the engine download a whole torrent, every piece verified, from
//   - libtorrent-rasterbar (its Python bindings; qBittorrent, Deluge), and
//   - Transmission's transmission-daemon (its bundled libutp fork),
// each seeding with TCP off, so every byte rides uTP, in both directions:
//   - outbound: the session announces the info-hash (no tracker) and
//     `open`s the client with transport uTP: our SYN, the client's ST_STATE;
//   - inbound: the session announces to a fake tracker, the client finds
//     the gateway there and connects to the announced port over uTP: the
//     client's SYN, our ST_STATE; the engine gets `accept` (uTP).
//
// And hole punching (M4-05, BEP 55) with libtorrent as the relay and as
// the target: a libtorrent leecher (the relay) is connected to a libtorrent
// seed (the target); the session reaches the relay over uTP from the
// announced port, learns the target from the relay's ut_pex (holepunch
// flag), sends it a `rendezvous`, and on the relay's `connect` opens uTP to
// the target while the target, told by the relay, connects to the announced
// port: the download goes over whichever of the two connections comes up.
// The target never learns the gateway's address but from the relay (the
// session announces to no tracker). There is no NAT in between (both
// containers share the host's network): what this shows is that real
// clients relay, answer and connect as BEP 55 says, with both SYNs in
// flight; the NAT itself is simulated in services/seeders (punch_tests.rs).
//
// Confined to this machine. The clients run in the Docker image of
// tools/e2e/interop (built on first use) in the host's network namespace.
// Everything peer-facing (the gateway's announced port, the fake tracker,
// the clients' ports) binds the host's address on an `--internal` Docker
// bridge made for the run, which no container joins and nothing routes
// to: a host-local address that is not loopback, because Transmission
// drops 127.0.0.0/8 addresses that trackers hand it ("martian",
// libtransmission/net.cc) and would never dial the gateway. Traffic to
// it stays on the host's loopback path (and host firewalls that drop
// container-to-host traffic do not interfere). The WebSocket stays on
// 127.0.0.1. No DHT, no public tracker, no LSD/UPnP/NAT-PMP. Without
// Docker the test is skipped, unless REQUIRE_INTEROP_E2E=1 (CI) makes that
// a failure.
//
//   npm run e2e:interop -w tools/e2e
//   INTEROP_CLIENTS=libtorrent npm run e2e:interop -w tools/e2e (one client)
//   E2E_SIZE=64MiB npm run e2e:interop -w tools/e2e

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import dgram from "node:dgram";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	AnnounceStatus,
	PEX_HOLEPUNCH,
	PeerConnection,
	TunnelClient,
	downloadAll,
	geometryOf,
	holepunchErrorName,
	parseInfoDict,
	type AcceptedPeer,
	type HolepunchMessage,
	type Metainfo,
	type PexAddress,
	type Transport as PeerTransport,
} from "@magnet-stream/engine";
import { bencode, makeTorrent, parseSize, type FakeTorrent } from "@magnet-stream/fake-peer";
import { freePort, startFakeTracker, startVpsGateway, type FakeTracker, type WranglerDev } from "@magnet-stream/harness";
import { Transport, formatAddress } from "@magnet-stream/tunnel-protocol";

const MiB = 1024 * 1024;
const INTEROP_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../interop");
const IMAGE = "magnet-stream-interop";
const size = parseSize(process.env.E2E_SIZE ?? "16MiB");
const required = Boolean(process.env.REQUIRE_INTEROP_E2E);

let failed = false;
const cleanups: (() => Promise<void> | void)[] = [];

// ---------------------------------------------------------------- docker

/** The clients' containers, and the host-local address everything peer-facing binds. */
class Docker {
	readonly network = `magnet-stream-interop-${process.pid}`;
	/** The host's address on the internal bridge: gateway, tracker and clients listen there. */
	hostIp = "";
	private n = 0;

	static available(): boolean {
		return spawnSync("docker", ["info"], { stdio: "ignore" }).status === 0;
	}

	setup(): void {
		const build = spawnSync("docker", ["build", "-q", "-t", IMAGE, INTEROP_DIR], { stdio: ["ignore", "ignore", "inherit"] });
		if (build.status !== 0) throw new Error(`docker build of ${INTEROP_DIR} failed (${build.status ?? build.signal})`);
		docker(["network", "create", "--internal", this.network]);
		cleanups.push(() => void spawnSync("docker", ["network", "rm", "-f", this.network], { stdio: "ignore" }));
		this.hostIp = docker(["network", "inspect", this.network, "-f", "{{(index .IPAM.Config 0).Gateway}}"]).trim();
		if (!/^\d+\.\d+\.\d+\.\d+$/.test(this.hostIp)) throw new Error(`no IPv4 gateway on the ${this.network} network`);
	}

	/** Run `cmd` in a new container in the host's network namespace, `dirs` mounted at the same paths. */
	start(cmd: string[], dirs: string[]): Container {
		const name = `${this.network}-${this.n++}`;
		const user = `${process.getuid?.() ?? 0}:${process.getgid?.() ?? 0}`;
		const mounts = [...dirs, INTEROP_DIR].flatMap((d) => ["-v", `${d}:${d}`]);
		const child = spawn("docker", ["run", "--rm", "-i", "--name", name, "--network", "host", "--user", user, "-e", "HOME=/tmp", ...(process.env.INTEROP_PEER_LOG ? ["-e", "INTEROP_PEER_LOG=1"] : []), ...mounts, IMAGE, ...cmd], {
			stdio: ["pipe", "pipe", "pipe"],
		});
		const kill = () => {
			spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
			child.kill("SIGKILL");
		};
		cleanups.push(kill);
		return { child, kill };
	}

	/** Run once in a throwaway container (no network), return its output. */
	run(cmd: string[]): string {
		const r = spawnSync("docker", ["run", "--rm", "--network", "none", IMAGE, ...cmd], { encoding: "utf8" });
		return r.stdout + r.stderr;
	}
}

interface Container {
	child: ChildProcess;
	kill(): void;
}

function docker(args: string[]): string {
	const r = spawnSync("docker", args, { encoding: "utf8" });
	if (r.status !== 0) throw new Error(`docker ${args.join(" ")}: ${r.stderr}`);
	return r.stdout;
}

// ---------------------------------------------------------------- clients

/** A real client seeding one torrent, uTP only, in a container. */
interface Seed {
	/** `ip:port` it listens on. */
	addr: string;
	/** Connections it has (or had), by transport and direction. */
	peers(): Promise<{ utp: boolean; incoming: boolean }[]>;
	uploaded(): Promise<number>;
	output(): string;
	stop(): Promise<void>;
}

interface Client {
	name: string;
	version(d: Docker): string;
	/** Start seeding `torrentFile` (its data already in `dir/data`). */
	seed(d: Docker, dir: string, torrentFile: string): Promise<Seed>;
}

/** A libtorrent peer, uTP only: a seed, or a leecher with `--leech` (tools/e2e/interop/libtorrent-seed.py). */
async function startLibtorrent(d: Docker, dir: string, torrentFile: string, extra: string[] = []): Promise<Seed & { connections(): number }> {
	const port = await freeUdpAndTcpPort(d.hostIp);
	const c = d.start(["python3", join(INTEROP_DIR, "libtorrent-seed.py"), torrentFile, join(dir, "data"), `${d.hostIp}:${port}`, ...extra], [dir]);
	let log = "";
	const events: Record<string, unknown>[] = [];
	let partial = "";
	c.child.stdout?.on("data", (b: Buffer) => {
		partial += b.toString();
		const lines = partial.split("\n");
		partial = lines.pop() ?? "";
		for (const line of lines) {
			log += `${line}\n`;
			try {
				events.push(JSON.parse(line) as Record<string, unknown>);
			} catch {}
		}
	});
	c.child.stderr?.on("data", (b: Buffer) => (log += b.toString()));
	const exited = new Promise<void>((r) => c.child.once("exit", () => r()));
	const stop = async () => {
		c.child.stdin?.end();
		await Promise.race([exited, sleep(3_000)]);
		c.kill();
	};
	await waitFor(() => events.some((e) => e.event === "ready"), 60_000, () => `libtorrent not ready:\n${log}`, () => c.child.exitCode !== null);
	return {
		addr: `${d.hostIp}:${port}`,
		peers: async () => events.filter((e) => e.event === "peer").map((e) => ({ utp: e.utp === true, incoming: e.outgoing !== true })),
		uploaded: async () => Number(events.filter((e) => e.event === "stats").at(-1)?.uploaded ?? 0),
		connections: () => Number(events.filter((e) => e.event === "stats").at(-1)?.peers ?? 0),
		output: () => log,
		stop,
	};
}

const libtorrent: Client = {
	name: "libtorrent",
	version: (d) => `libtorrent-rasterbar ${d.run(["python3", "-c", "import libtorrent as lt; print(lt.__version__)"]).trim()} (Python bindings)`,
	seed: (d, dir, torrentFile) => startLibtorrent(d, dir, torrentFile),
};

const transmission: Client = {
	name: "transmission",
	version: (d) => `${d.run(["transmission-daemon", "--version"]).trim().split("\n")[0]} (bundled libutp)`,
	seed: async (d, dir, torrentFile) => {
		const config = join(dir, "transmission");
		mkdirSync(config, { recursive: true });
		const port = await freeUdpAndTcpPort(d.hostIp);
		const rpcPort = await freePort();
		writeFileSync(
			join(config, "settings.json"),
			JSON.stringify({
				"peer-port": port,
				"peer-port-random-on-start": false,
				"bind-address-ipv4": d.hostIp,
				"bind-address-ipv6": "::1",
				"tcp-enabled": false,
				"utp-enabled": true,
				"dht-enabled": false,
				"lpd-enabled": false,
				"pex-enabled": false,
				"port-forwarding-enabled": false,
				// Plaintext preferred: the gateway's handshake gate speaks no MSE (BEP 8 obfuscation).
				encryption: 0,
				"rpc-enabled": true,
				"rpc-bind-address": "127.0.0.1",
				"rpc-port": rpcPort,
				"rpc-authentication-required": false,
				"rpc-whitelist-enabled": true,
				"rpc-whitelist": "127.0.0.1",
				"rpc-host-whitelist-enabled": false,
				"download-dir": join(dir, "data"),
				"incomplete-dir-enabled": false,
				"watch-dir-enabled": false,
				"speed-limit-up-enabled": false,
				"speed-limit-down-enabled": false,
				"ratio-limit-enabled": false,
				"idle-seeding-limit-enabled": false,
				"start-added-torrents": true,
				"message-level": Number(process.env.INTEROP_TRANSMISSION_LOG ?? 4),
			}),
		);
		const c = d.start(["transmission-daemon", "-f", "-g", config], [dir]);
		let log = "";
		c.child.stdout?.on("data", (b: Buffer) => (log += b.toString()));
		c.child.stderr?.on("data", (b: Buffer) => (log += b.toString()));
		const exited = new Promise<void>((r) => c.child.once("exit", () => r()));
		const url = `http://127.0.0.1:${rpcPort}/transmission/rpc`;
		let session = "";
		const rpc = async (method: string, args: Record<string, unknown>): Promise<Record<string, unknown>> => {
			for (let attempt = 0; attempt < 2; attempt++) {
				const res = await fetch(url, {
					method: "POST",
					headers: { "content-type": "application/json", "X-Transmission-Session-Id": session },
					body: JSON.stringify({ method, arguments: args }),
				});
				if (res.status === 409) {
					session = res.headers.get("X-Transmission-Session-Id") ?? "";
					continue;
				}
				const body = (await res.json()) as { result: string; arguments: Record<string, unknown> };
				if (body.result !== "success") throw new Error(`transmission ${method}: ${body.result}`);
				return body.arguments;
			}
			throw new Error(`transmission ${method}: no session id`);
		};
		await waitFor(
			() =>
				rpc("session-get", { fields: ["version"] }).then(
					() => true,
					() => false,
				),
			30_000,
			() => `transmission-daemon RPC did not come up:\n${log}`,
			() => c.child.exitCode !== null,
		);
		await rpc("torrent-add", { metainfo: readFileSync(torrentFile).toString("base64"), "download-dir": join(dir, "data"), paused: false });
		type Torrent = { percentDone: number; status: number; uploadedEver: number; peers: { isUTP: boolean; isIncoming: boolean }[] };
		const get = async (): Promise<Torrent | undefined> => {
			const args = await rpc("torrent-get", { fields: ["percentDone", "status", "uploadedEver", "peers"] });
			return (args.torrents as Torrent[])[0];
		};
		// Status 6 = seeding, once the existing data has been checked.
		await waitFor(
			async () => {
				const t = await get();
				return t !== undefined && t.percentDone === 1 && t.status === 6;
			},
			60_000,
			() => `transmission did not seed:\n${log}`,
			() => c.child.exitCode !== null,
		);
		const seen: { utp: boolean; incoming: boolean }[] = [];
		const poll = setInterval(() => {
			get()
				.then((t) => {
					for (const p of t?.peers ?? []) if (!seen.some((s) => s.utp === p.isUTP && s.incoming === p.isIncoming)) seen.push({ utp: p.isUTP, incoming: p.isIncoming });
				})
				.catch(() => {});
		}, 200);
		cleanups.push(() => clearInterval(poll));
		return {
			addr: `${d.hostIp}:${port}`,
			peers: async () => seen,
			uploaded: async () => (await get())?.uploadedEver ?? 0,
			output: () => log,
			stop: async () => {
				clearInterval(poll);
				c.child.kill("SIGTERM");
				await Promise.race([exited, sleep(3_000)]);
				c.kill();
			},
		};
	},
};

// ---------------------------------------------------------------- the test

async function main(): Promise<void> {
	if (!Docker.available()) {
		console.error(`SKIP uTP interop: docker is not available${required ? " (REQUIRE_INTEROP_E2E is set: failing)" : ""}`);
		process.exit(required ? 1 : 0);
	}
	const d = new Docker();
	d.setup();
	const only = process.env.INTEROP_CLIENTS?.split(",").map((s) => s.trim());
	const clients = [libtorrent, transmission].filter((c) => !only || only.includes(c.name));
	const inboundPort = await freePort();
	const gateway = await startVpsGateway({
		vars: {
			ALLOW_PRIVATE_PEERS: "1",
			// The announced port (TCP and uTP) on the host's side of the internal bridge.
			TUNNEL_INBOUND_ADDR: `${d.hostIp}:${inboundPort}`,
			// Hermetic: no public DHT, no public trackers.
			TUNNEL_ANNOUNCE_DHT: "0",
			DEFAULT_TRACKERS: "",
		},
	});
	cleanups.push(() => gateway.stop());
	const tracker = await startFakeTracker({ host: d.hostIp, interval: 2 });
	cleanups.push(() => tracker.close());
	let seedNo = 400;
	for (const client of clients) {
		console.log(`--- ${client.version(d)}`);
		await outbound(d, client, gateway, makeTorrent({ seed: seedNo++, size, pieceLength: 256 * 1024 }));
		await inbound(d, client, gateway, tracker, inboundPort, makeTorrent({ seed: seedNo++, size, pieceLength: 256 * 1024 }));
	}
	// Transmission does not implement BEP 55 (no ut_holepunch): libtorrent only.
	if (clients.includes(libtorrent)) await holepunch(d, gateway, tracker, makeTorrent({ seed: seedNo, size, pieceLength: 256 * 1024 }));
	const logs = gateway.output();
	const tunnelLines = logs.split("\n").filter((l) => l.includes("tunnel."));
	// The peers' address is the bridge's; only the config line at start-up may name it (TUNNEL_INBOUND_ADDR).
	check(tunnelLines.length > 0 && !tunnelLines.some((l) => l.includes(d.hostIp)), "no peer address in the gateway's tunnel logs");
}

/** The gateway dials the client over uTP (`open`, transport uTP). */
async function outbound(d: Docker, client: Client, gateway: WranglerDev, torrent: FakeTorrent): Promise<void> {
	const dir = workdir(torrent);
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const tunnel = await TunnelClient.connect(`${gateway.url.replace(/^http/, "ws")}/v1/tunnel`);
	let seed: Seed | undefined;
	try {
		seed = await client.seed(d, dir, writeTorrent(dir, torrent));
		const id = peerId();
		// uTP leaves from the announced port: the session announces first (no trackers).
		const status = await tunnel.announce({ infoHash: torrent.infoHash, peerId: id, trackers: [] });
		check(status === AnnounceStatus.ACTIVE, `${client.name} outbound: announce is active (got ${status})`);
		const started = Date.now();
		const transport = await tunnel.open({ address: seed.addr, infoHash: torrent.infoHash, peerId: id, transport: Transport.UTP, timeoutMs: 30_000 });
		await download(`${client.name} outbound`, transport, meta, torrent, started);
		const peers = await seed.peers();
		check(peers.length > 0 && peers.every((p) => p.utp && p.incoming), `${client.name} outbound: the client saw one incoming uTP peer (${JSON.stringify(peers)})`);
		// The clients' counters lag a little behind the bytes.
		const counted = await waitFor(async () => (await seed!.uploaded()) >= size, 5_000, () => "").then(
			() => true,
			() => false,
		);
		check(counted, `${client.name} outbound: the client counts the whole torrent as uploaded`);
	} catch (e) {
		report(`${client.name} outbound`, e, seed, gateway);
	} finally {
		tunnel.close();
		await seed?.stop();
		rmSync(dir, { recursive: true, force: true });
	}
}

/** The client finds the gateway at the fake tracker and connects in over uTP (`accept`). */
async function inbound(d: Docker, client: Client, gateway: WranglerDev, tracker: FakeTracker, inboundPort: number, torrent: FakeTorrent): Promise<void> {
	const dir = workdir(torrent);
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const tunnel = await TunnelClient.connect(`${gateway.url.replace(/^http/, "ws")}/v1/tunnel`);
	let seed: Seed | undefined;
	try {
		const accepted: AcceptedPeer[] = [];
		tunnel.onAccept = (peer) => accepted.push(peer);
		const status = await tunnel.announce({ infoHash: torrent.infoHash, peerId: peerId(), trackers: [tracker.url] });
		check(status === AnnounceStatus.ACTIVE, `${client.name} inbound: announce is active (got ${status})`);
		// The gateway is in the tracker's swarm before the client asks for peers.
		await waitFor(
			() => tracker.announces.some((a) => a.infoHash === torrent.infoHashHex && a.port === inboundPort),
			20_000,
			() => "the gateway never announced to the tracker",
		);
		const started = Date.now();
		seed = await client.seed(d, dir, writeTorrent(dir, torrent, tracker.url));
		await waitFor(() => accepted.length > 0, 60_000, () => `${client.name} never connected in`);
		const peer = accepted[0];
		console.log(`${client.name} inbound: connected in ${((Date.now() - started) / 1000).toFixed(2)} s after it started`);
		check(peer.peerTransport === Transport.UTP, `${client.name} inbound: the accepted connection is uTP (got ${peer.peerTransport})`);
		await download(`${client.name} inbound`, peer.transport, meta, torrent, Date.now());
		const peers = await seed.peers();
		check(peers.length > 0 && peers.every((p) => p.utp && !p.incoming), `${client.name} inbound: the client saw one outgoing uTP peer (${JSON.stringify(peers)})`);
	} catch (e) {
		report(`${client.name} inbound`, e, seed, gateway);
	} finally {
		tunnel.close();
		await seed?.stop();
		rmSync(dir, { recursive: true, force: true });
	}
}

/**
 * BEP 55 with libtorrent on both other ends: the relay (a leecher kept slow,
 * so it stays connected to the seed) and the target (the seed). See the
 * header for what this shows and what it does not.
 */
async function holepunch(d: Docker, gateway: WranglerDev, tracker: FakeTracker, torrent: FakeTorrent): Promise<void> {
	const what = "libtorrent hole punch";
	const targetDir = workdir(torrent);
	const relayDir = mkdtempSync(join(tmpdir(), "utp-interop-"));
	mkdirSync(join(relayDir, "data"));
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const tunnel = await TunnelClient.connect(`${gateway.url.replace(/^http/, "ws")}/v1/tunnel`);
	let target: (Seed & { connections(): number }) | undefined;
	let relay: (Seed & { connections(): number }) | undefined;
	let relayConn: PeerConnection | undefined;
	try {
		// The two clients find each other at the tracker; the gateway is not there.
		target = await startLibtorrent(d, targetDir, writeTorrent(targetDir, torrent, tracker.url));
		relay = await startLibtorrent(d, relayDir, writeTorrent(relayDir, torrent, tracker.url), ["--leech", "--download-limit", "16384"]);
		await waitFor(() => relay!.connections() > 0 && target!.connections() > 0, 30_000, () => "the relay never connected to the target");
		const id = peerId();
		const accepted: AcceptedPeer[] = [];
		tunnel.onAccept = (peer) => accepted.push(peer);
		const status = await tunnel.announce({ infoHash: torrent.infoHash, peerId: id, trackers: [] });
		check(status === AnnounceStatus.ACTIVE, `${what}: announce is active (got ${status})`);
		// The relay, reached over uTP from the announced port: it sees the gateway there.
		const relayTransport = await tunnel.open({ address: relay.addr, infoHash: torrent.infoHash, peerId: id, transport: Transport.UTP, timeoutMs: 30_000 });
		let named: PexAddress | undefined;
		let answer: HolepunchMessage | undefined;
		relayConn = new PeerConnection(relayTransport, {
			geometry: geometryOf(meta),
			events: {
				pex: (m) => {
					named ??= m.added.find((a) => formatAddress(a) === target!.addr && (a.flags & PEX_HOLEPUNCH) !== 0);
				},
				holepunch: (m) => {
					if (formatAddress(m.address) === target!.addr) answer ??= m;
				},
			},
		});
		// libtorrent sends its first ut_pex within about a minute.
		await waitFor(() => named !== undefined, 90_000, () => "the relay never named the target in ut_pex with the holepunch flag");
		check(true, `${what}: the relay names the target in ut_pex with the holepunch flag (0x${named!.flags.toString(16)})`);
		check(relayConn.sendHolepunch({ type: "rendezvous", address: named! }), `${what}: the relay takes ut_holepunch`);
		await waitFor(() => answer !== undefined, 15_000, () => "the relay did not answer the rendezvous");
		const reply = answer!;
		check(reply.type === "connect", `${what}: the relay answers connect (got ${reply.type === "error" ? `error ${holepunchErrorName(reply.error)}` : reply.type})`);
		if (reply.type !== "connect") throw new Error("no connect from the relay");
		// Our SYN and the target's, at once. As the engine does: a target whose
		// SYN reached the announced port before the relay's `connect` reached
		// us is already an `accept`, and is not dialled again (a second
		// connection would be a duplicate the target may keep instead).
		const started = Date.now();
		const fromTarget = () => accepted.find((p) => p.address === target!.addr);
		let first: { how: "open" | "accept"; transport: PeerTransport };
		const early = fromTarget();
		if (early) first = { how: "accept", transport: early.transport };
		else {
			// The open's dial holds the gateway's acceptor for the target's address:
			// the target's SYN, if it comes now, is this open's connection.
			const transport = await tunnel.open({ address: target.addr, infoHash: torrent.infoHash, peerId: id, transport: Transport.UTP, timeoutMs: 15_000 });
			first = { how: "open", transport };
		}
		console.log(
			`${what}: connected ${((Date.now() - started) / 1000).toFixed(2)} s after the relay's connect, ${first.how === "open" ? "on our open (the gateway's dial, holding the target's SYN)" : "by an accept (the target's SYN came first)"}`,
		);
		await download(what, first.transport, meta, torrent, started);
		const peers = await target.peers();
		check(peers.some((p) => p.utp), `${what}: the target saw the session over uTP (${JSON.stringify(peers)})`);
	} catch (e) {
		report(what, e, target, gateway);
		if (relay) console.error(`--- relay ---\n${relay.output().slice(-20000)}`);
	} finally {
		relayConn?.close();
		tunnel.close();
		await relay?.stop();
		await target?.stop();
		rmSync(targetDir, { recursive: true, force: true });
		rmSync(relayDir, { recursive: true, force: true });
	}
}

async function download(what: string, transport: PeerTransport, meta: Metainfo, torrent: FakeTorrent, started: number): Promise<void> {
	const conn = new PeerConnection(transport, { geometry: geometryOf(meta) });
	const result = await downloadAll(conn, meta, {
		stallTimeoutMs: 30_000,
		onPiece: (piece, data) => {
			const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
			if (!Buffer.from(data).equals(Buffer.from(expected))) throw new Error(`piece ${piece} differs from the source`);
		},
	});
	conn.close();
	const seconds = (Date.now() - started) / 1000;
	console.log(`${what}: verified ${result.verified.length}/${meta.pieceCount} pieces (${(size / MiB).toFixed(1)} MiB in ${seconds.toFixed(2)} s, ${(size / MiB / seconds).toFixed(1)} MiB/s)`);
	check(result.verified.length === meta.pieceCount && result.corrupt.length === 0 && result.missing.length === 0, `${what}: every piece verified`);
}

function report(what: string, e: unknown, seed: Seed | undefined, gateway: WranglerDev): void {
	failed = true;
	console.error(`FAIL ${what}: ${String(e)}\n--- client ---\n${seed?.output().slice(-6000) ?? ""}\n--- gateway ---\n${gateway.output().slice(-3000)}`);
}

// ---------------------------------------------------------------- helpers

function workdir(torrent: FakeTorrent): string {
	const dir = mkdtempSync(join(tmpdir(), "utp-interop-"));
	mkdirSync(join(dir, "data"));
	writeFileSync(join(dir, "data", torrent.name), torrent.data.subarray(0, torrent.totalLength));
	return dir;
}

/** A .torrent (BEP 3) for `torrent`, with `announce` when given. */
function writeTorrent(dir: string, torrent: FakeTorrent, announce?: string): string {
	const head = announce ? bencode({ announce }).subarray(0, -1) : Buffer.from("d");
	const file = join(dir, "test.torrent");
	writeFileSync(file, Buffer.concat([head, Buffer.from("4:info"), torrent.info, Buffer.from("e")]));
	return file;
}

/** A port free for both TCP and UDP on `host` (the clients bind both, even with TCP off). */
async function freeUdpAndTcpPort(host: string): Promise<number> {
	for (;;) {
		const port = await freePort(host);
		const free = await new Promise<boolean>((r) => {
			const s = dgram.createSocket("udp4");
			s.once("error", () => r(false));
			s.bind(port, host, () => s.close(() => r(true)));
		});
		if (free) return port;
	}
}

function peerId(): Uint8Array {
	return Buffer.concat([Buffer.from("-MS0001-", "latin1"), randomBytes(12)]);
}

async function waitFor(cond: () => boolean | Promise<boolean>, ms: number, what: () => string, dead?: () => boolean): Promise<void> {
	const until = Date.now() + ms;
	while (Date.now() < until) {
		if (await cond()) return;
		if (dead?.()) break;
		await sleep(100);
	}
	throw new Error(what());
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

function check(ok: boolean, what: string): void {
	if (!ok) {
		failed = true;
		console.error(`FAIL ${what}`);
	} else console.log(`ok   ${what}`);
}

async function cleanup(): Promise<void> {
	for (const c of cleanups.splice(0).reverse()) await Promise.resolve(c()).catch(() => {});
}

// Interrupted: still remove the containers and the network.
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => void cleanup().finally(() => process.exit(1)));

main()
	.catch((e: unknown) => {
		failed = true;
		console.error(e);
	})
	.finally(async () => {
		await cleanup();
		process.exit(failed ? 1 : 0);
	});
