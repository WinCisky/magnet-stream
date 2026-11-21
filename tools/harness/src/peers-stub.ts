// A `/peers` NDJSON stub for tests (docs/discovery-api.md). One request
// writes the configured lines and closes. It does not serve /files,
// /metadata or /swarm.
//
// A line is an address (`"ip:port"`, written as `{"peer":…}`), an address
// with the scout's facts (`{ peer, score, alive, … }`, written as is), the
// info dict (`{ info: bytes }`, written base64 with its size), or a pause
// (`{ pauseMs }`) before the next line. `lines` may also be a function of
// the request (its `ih`, and how many streams that info-hash had before),
// to serve several torrents or change the answer on a second request.

import http from "node:http";

export type StubLine = string | ({ peer: string } & Record<string, unknown>) | { info: Uint8Array } | { pauseMs: number };

/** What to answer one request: `ih` as asked, `n` the streams that info-hash had before (0 for the first). */
export type StubLines = readonly StubLine[] | ((request: { ih: string; n: number }) => readonly StubLine[]);

export interface PeersStub {
	url: string;
	port: number;
	/** How many `/peers` streams were started. */
	requests(): number;
	close(): Promise<void>;
}

/** Serve `lines` as one NDJSON stream on `GET /peers`. */
export function startPeersStub(lines: StubLines, opts: { port?: number } = {}): Promise<PeersStub> {
	let requests = 0;
	const perHash = new Map<string, number>();
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
		const ih = url.searchParams.get("ih")!.toLowerCase();
		const n = perHash.get(ih) ?? 0;
		perHash.set(ih, n + 1);
		res.writeHead(200, {
			"content-type": "application/x-ndjson",
			"cache-control": "no-store",
			"x-accel-buffering": "no",
		});
		void write(res, typeof lines === "function" ? lines({ ih, n }) : lines);
	});
	return new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(opts.port ?? 0, "127.0.0.1", () => {
			const { port } = server.address() as { port: number };
			resolve({
				url: `http://127.0.0.1:${port}`,
				port,
				requests: () => requests,
				close: () =>
					new Promise((done) => {
						server.closeAllConnections();
						server.close(() => done());
					}),
			});
		});
	});
}

async function write(res: http.ServerResponse, lines: readonly StubLine[]): Promise<void> {
	let count = 0;
	for (const line of lines) {
		if (res.destroyed) return;
		if (typeof line === "string") {
			res.write(`${JSON.stringify({ peer: line })}\n`);
			count++;
		} else if ("peer" in line) {
			res.write(`${JSON.stringify(line)}\n`);
			count++;
		} else if ("pauseMs" in line) {
			await new Promise((r) => setTimeout(r, line.pauseMs));
		} else {
			res.write(`${JSON.stringify({ info: Buffer.from(line.info).toString("base64"), size: line.info.length })}\n`);
		}
	}
	res.end(`${JSON.stringify({ done: true, count, cached: false, complete: true })}\n`);
}
