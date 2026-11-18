// A `/peers` NDJSON stub for tests (docs/discovery-api.md). One request
// writes the configured addresses and closes. It does not serve /files,
// /metadata or /swarm.

import http from "node:http";

export interface PeersStub {
	url: string;
	port: number;
	/** How many `/peers` streams were started. */
	requests(): number;
	close(): Promise<void>;
}

/** Serve `peers` as one NDJSON stream on `GET /peers`. */
export function startPeersStub(peers: readonly string[], opts: { port?: number } = {}): Promise<PeersStub> {
	let requests = 0;
	const server = http.createServer((req, res) => {
		const url = new URL(req.url ?? "/", "http://127.0.0.1");
		res.setHeader("access-control-allow-origin", "*");
		res.setHeader("access-control-allow-methods", "GET, OPTIONS");
		if (req.method === "OPTIONS") {
			res.writeHead(204);
			res.end();
			return;
		}
		if (req.method !== "GET" || url.pathname !== "/peers" || !url.searchParams.has("ih")) {
			res.writeHead(404, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: "not_found" }));
			return;
		}
		requests++;
		res.writeHead(200, {
			"content-type": "application/x-ndjson",
			"cache-control": "no-store",
			"x-accel-buffering": "no",
		});
		for (const peer of peers) res.write(`${JSON.stringify({ peer })}\n`);
		res.end(`${JSON.stringify({ done: true, count: peers.length, cached: false, complete: true })}\n`);
	});
	return new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(opts.port ?? 0, "127.0.0.1", () => {
			const { port } = server.address() as { port: number };
			resolve({
				url: `http://127.0.0.1:${port}`,
				port,
				requests: () => requests,
				close: () => new Promise((done) => server.close(() => done())),
			});
		});
	});
}
