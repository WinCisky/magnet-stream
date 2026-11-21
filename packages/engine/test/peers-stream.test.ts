import { describe, expect, it } from "vitest";
import { PeerBook } from "../src/peer-book.ts";
import { MAX_STREAM_INFO_BYTES, consumePeerBody, parsePeerLine, parseStreamLine, readPeerBody, type ScoutFacts } from "../src/peers-stream.ts";

function body(...chunks: string[]): ReadableStream<Uint8Array> {
	return new ReadableStream({
		start(controller) {
			for (const c of chunks) controller.enqueue(new TextEncoder().encode(c));
			controller.close();
		},
	});
}

const b64 = (bytes: Uint8Array) => Buffer.from(bytes).toString("base64");

describe("peers stream", () => {
	it("reads each address once and ignores the done line and garbage", async () => {
		const got: string[] = [];
		const n = await consumePeerBody(
			body('{"peer":"1.2.3.4:6881"}\n{"peer":"1.2.3.4:6881"}\nnot json\n{"done":true,"count":1}\n{"peer":"[2606:4700:4700::1111]:51413"}\n'),
			(a) => got.push(a),
		);
		expect(n).toBe(3);
		expect(got).toEqual(["1.2.3.4:6881", "1.2.3.4:6881", "[2606:4700:4700::1111]:51413"]);
		expect(parsePeerLine('{"peer":"10.0.0.1:80"}')).toBe("10.0.0.1:80");
		expect(parsePeerLine('{"peer":"example.com:6881"}')).toBeNull();
		expect(parsePeerLine('{"peer":"1.2.3.4:0"}')).toBeNull();
	});

	it("reads the scout's facts and ignores unknown fields", () => {
		expect(parseStreamLine('{"peer":"203.0.113.7:51413","score":0.91,"alive":true,"seed":true,"md":true,"unchoked":true,"have":1.00,"rtt":38,"future":[1,2]}')).toEqual({
			kind: "peer",
			peer: "203.0.113.7:51413",
			scout: { score: 0.91, alive: true, seed: true, md: true, unchoked: true, have: 1, rtt: 38 },
		});
		expect(parseStreamLine('{"peer":"[2001:db8::1]:6881","score":0.20}')).toEqual({ kind: "peer", peer: "[2001:db8::1]:6881", scout: { score: 0.2 } });
		expect(parseStreamLine('{"peer":"192.0.2.9:40000","score":0.02,"alive":false}')).toEqual({
			kind: "peer",
			peer: "192.0.2.9:40000",
			scout: { score: 0.02, alive: false },
		});
		expect(parseStreamLine('{"done":true,"count":4,"alive":2}')).toEqual({ kind: "done" });
	});

	it("drops lines with values of the wrong type or out of range", () => {
		const bad = [
			'{"peer":"1.2.3.4:6881","score":1.5}',
			'{"peer":"1.2.3.4:6881","score":-0.1}',
			'{"peer":"1.2.3.4:6881","score":"0.9"}',
			'{"peer":"1.2.3.4:6881","score":1e400}',
			'{"peer":"1.2.3.4:6881","score":0.5,"have":1.01}',
			'{"peer":"1.2.3.4:6881","score":0.5,"have":-1}',
			'{"peer":"1.2.3.4:6881","score":0.5,"alive":"yes"}',
			'{"peer":"1.2.3.4:6881","score":0.5,"alive":true,"seed":1}',
			'{"peer":"1.2.3.4:6881","score":0.5,"rtt":-3}',
			'{"peer":"1.2.3.4:6881","score":0.5,"rtt":1e12}',
			// Facts that only a live probe can give, on an address not seen alive.
			'{"peer":"1.2.3.4:6881","score":0.9,"seed":true}',
			'{"peer":"1.2.3.4:6881","score":0.9,"alive":false,"md":true}',
			'{"peer":"1.2.3.4:6881","score":0.9,"alive":true,"seed":true,"have":0.5}',
			// Facts without a score.
			'{"peer":"1.2.3.4:6881","alive":true}',
			'{"peer":"1.2.3.4:6881","score":null}',
			'{"peer":["1.2.3.4:6881"],"score":0.5}',
			'{"peer":"1.2.3.4:99999","score":0.5}',
			`{"peer":"1.2.3.4:6881","score":0.5,"pad":"${"x".repeat(600)}"}`,
			"[1,2,3]",
			"null",
			'"peer"',
			'{"peer":"1.2.3.4:6881"',
		];
		for (const line of bad) expect(parseStreamLine(line), line.slice(0, 80)).toBeNull();
	});

	it("decodes the info line, once, within its size limit", async () => {
		const info = new Uint8Array(1000).map((_, i) => i * 7);
		expect(parseStreamLine(JSON.stringify({ info: b64(info), size: 1000 }))).toEqual({ kind: "info", info });
		expect(parseStreamLine(JSON.stringify({ info: b64(info) }))).toEqual({ kind: "info", info });
		// Wrong size, bad base64, empty, unpadded, not a string.
		expect(parseStreamLine(JSON.stringify({ info: b64(info), size: 999 }))).toBeNull();
		expect(parseStreamLine(JSON.stringify({ info: "ZDY6bGVuZ3Ro*A==" }))).toBeNull();
		expect(parseStreamLine(JSON.stringify({ info: "" }))).toBeNull();
		expect(parseStreamLine(JSON.stringify({ info: "ZDY" }))).toBeNull();
		expect(parseStreamLine(JSON.stringify({ info: 12 }))).toBeNull();
		expect(parseStreamLine(JSON.stringify({ info: "ZA=a" }))).toBeNull();
		// Over 16 MiB decoded.
		const huge = "A".repeat((Math.ceil(MAX_STREAM_INFO_BYTES / 3) + 1) * 4);
		expect(parseStreamLine(JSON.stringify({ info: huge }))).toBeNull();

		const infos: Uint8Array[] = [];
		const other = Uint8Array.of(1, 2, 3);
		const json = JSON.stringify({ info: b64(info), size: info.length });
		// Split across chunks anywhere; a second info line is ignored.
		const r = await readPeerBody(body(json.slice(0, 17), json.slice(17, 900), `${json.slice(900)}\n{"info":"${b64(other)}"}\n`), {
			onPeer: () => {},
			onInfo: (i) => infos.push(i),
		});
		expect(r.info).toBe(true);
		expect(infos).toEqual([info]);
	});

	it("skips a line that grows past the limit without a newline, and reads on", async () => {
		const got: string[] = [];
		const chunk = "x".repeat(1 << 20);
		const chunks = ['{"peer":"1.2.3.4:6881","score":0.5}\n{"info":"'];
		for (let i = 0; i < 24; i++) chunks.push(chunk);
		chunks.push('"}\n{"peer":"5.6.7.8:6881","score":0.6,"alive":true}\n');
		const r = await readPeerBody(body(...chunks), { onPeer: (a) => got.push(a) });
		expect(got).toEqual(["1.2.3.4:6881", "5.6.7.8:6881"]);
		expect(r.rejected).toBe(1);
		expect(r.info).toBe(false);
	});

	it("keeps malformed or out-of-range lines out of the book", async () => {
		const book = new PeerBook();
		const lines = [
			'{"peer":"8.8.8.8:6881","score":0.9,"alive":true,"seed":true}',
			'{"peer":"8.8.4.4:6881","score":7,"alive":true,"seed":true}',
			'{"peer":"1.1.1.1:6881","score":0.5,"have":2}',
			'{"peer":"9.9.9.9:6881","score":0.02,"alive":false}',
			'{"peer":"10.0.0.1:6881","score":0.9,"alive":true}',
			'{"peer":"8.8.8.9:80","score":0.9,"alive":true}',
			"garbage",
		];
		await consumePeerBody(body(lines.join("\n")), (address: string, scout: ScoutFacts | undefined) => book.add(address, "discovery", 0, scout));
		// Private address and port < 1024 are refused by the book itself.
		expect([...book.entries()].map((p) => p.key).sort()).toEqual(["8.8.8.8:6881", "9.9.9.9:6881"]);
		expect(book.get("8.8.8.8:6881")!.scout).toEqual({ score: 0.9, alive: true, seed: true });
		expect(book.counts()).toMatchObject({ scoutAlive: 1, scoutSeeds: 1, scoutDead: 1, scoutUnknown: 0 });
	});
});
