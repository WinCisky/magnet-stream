// A BEP 3 HTTP tracker for tests (compact answers, IPv4): remembers who
// announced each info-hash and answers with the others. `stopped` removes
// the announcer. Every announce is recorded, so tests can check what a
// gateway announced, and when.

import http from "node:http";

export interface TrackerAnnounce {
	/** Info-hash, lowercase hex. */
	infoHash: string;
	/** `ip:port` the announcer says it listens on (the request's source IP). */
	peer: string;
	port: number;
	left: number;
	/** `started`, `stopped`, `completed`, or `` for a regular announce. */
	event: string;
	at: number;
}

export interface FakeTracker {
	/** `http://<host>:<port>/announce` (host 127.0.0.1 by default) */
	url: string;
	announces: TrackerAnnounce[];
	close(): Promise<void>;
}

export interface FakeTrackerOptions {
	port?: number;
	/** Announce interval it answers, in seconds (default 60). */
	interval?: number;
	/** IPv4 address to listen on (default 127.0.0.1; the uTP interop e2e uses a host-only Docker bridge). */
	host?: string;
}

export function startFakeTracker(opts: FakeTrackerOptions = {}): Promise<FakeTracker> {
	const host = opts.host ?? "127.0.0.1";
	const announces: TrackerAnnounce[] = [];
	const swarms = new Map<string, Map<string, number>>();
	const server = http.createServer((req, res) => {
		const raw = req.url ?? "";
		const query = raw.includes("?") ? raw.slice(raw.indexOf("?") + 1) : "";
		const params = new Map<string, string>();
		for (const kv of query.split("&")) {
			const at = kv.indexOf("=");
			if (at > 0) params.set(kv.slice(0, at), kv.slice(at + 1));
		}
		const infoHash = percentBytes(params.get("info_hash") ?? "");
		const port = Number(params.get("port"));
		const ip = (req.socket.remoteAddress ?? "").replace(/^::ffff:/, "");
		if (!infoHash || infoHash.length !== 20 || !Number.isInteger(port) || port < 1 || port > 65535 || !/^\d+\.\d+\.\d+\.\d+$/.test(ip)) {
			res.writeHead(400);
			res.end();
			return;
		}
		const ih = Buffer.from(infoHash).toString("hex");
		const peer = `${ip}:${port}`;
		const event = params.get("event") ?? "";
		announces.push({ infoHash: ih, peer, port, left: Number(params.get("left") ?? 0), event, at: Date.now() });
		const swarm = swarms.get(ih) ?? new Map<string, number>();
		swarms.set(ih, swarm);
		if (event === "stopped") swarm.delete(peer);
		else swarm.set(peer, Date.now());
		const others = [...swarm.keys()].filter((p) => p !== peer);
		const compact = Buffer.alloc(others.length * 6);
		others.forEach((p, i) => {
			const [a, b] = p.split(":");
			Buffer.from(a.split(".").map(Number)).copy(compact, i * 6);
			compact.writeUInt16BE(Number(b), i * 6 + 4);
		});
		const body = Buffer.concat([Buffer.from(`d8:intervali${opts.interval ?? 60}e5:peers${compact.length}:`), compact, Buffer.from("e")]);
		res.writeHead(200, { "content-type": "text/plain", "content-length": body.length });
		res.end(body);
	});
	return new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(opts.port ?? 0, host, () => {
			const { port } = server.address() as { port: number };
			resolve({
				url: `http://${host}:${port}/announce`,
				announces,
				close: () =>
					new Promise((r) => {
						server.closeAllConnections();
						server.close(() => r());
					}),
			});
		});
	});
}

/** Percent-decoded bytes (the info-hash is binary). */
function percentBytes(text: string): Uint8Array | undefined {
	const out: number[] = [];
	for (let i = 0; i < text.length; i++) {
		if (text[i] === "%") {
			const byte = parseInt(text.slice(i + 1, i + 3), 16);
			if (Number.isNaN(byte)) return undefined;
			out.push(byte);
			i += 2;
		} else out.push(text.charCodeAt(i) & 0xff);
	}
	return Uint8Array.from(out);
}
