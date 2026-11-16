import { describe, expect, it } from "vitest";
import { formatWant, Geometry, parseBlocksRequest, parseWant, planBlocks, ParamError } from "../src/params";

const IH = "c9e15763f722f23e98a29decdfae341b98d53056";
const TOKEN = "sRR_H4vDqUIPeXLWwQoGjkaZxnZT1mEvVTqga86kR18"; // gitleaks:allow (HMAC test vector for "test-secret")

function url(params: Record<string, string | string[]>): URL {
	const u = new URL("https://w.example/v1/blocks");
	for (const [k, v] of Object.entries(params)) for (const x of [v].flat()) u.searchParams.append(k, x);
	return u;
}

// 3 pieces of 40000 bytes (3 blocks: 16384+16384+7232) and a last piece of 1000 bytes.
const geometry = new Geometry(40_000, 121_000);

describe("Geometry", () => {
	it("handles the short last piece and last block", () => {
		expect(geometry.pieceCount).toBe(4);
		expect(geometry.pieceSize(0)).toBe(40_000);
		expect(geometry.pieceSize(3)).toBe(1_000);
		expect(geometry.blockCount(0)).toBe(3);
		expect(geometry.blockCount(3)).toBe(1);
		expect(geometry.block(0, 2)).toEqual([0, 32768, 7232]);
		expect(geometry.block(3, 0)).toEqual([3, 0, 1000]);
	});
});

describe("parseWant / formatWant", () => {
	it("accepts whole pieces, ranges and single blocks", () => {
		expect(parseWant("0,1:1-2,3:0", geometry)).toEqual([
			{ piece: 0, first: 0, last: 2 },
			{ piece: 1, first: 1, last: 2 },
			{ piece: 3, first: 0, last: 0 },
		]);
	});

	it("rejects out-of-range pieces and blocks", () => {
		expect(() => parseWant("4", geometry)).toThrow(ParamError);
		expect(() => parseWant("0:0-3", geometry)).toThrow(ParamError);
		expect(() => parseWant("0:2-1", geometry)).toThrow(ParamError);
		expect(() => parseWant("x", geometry)).toThrow(ParamError);
		expect(() => parseWant("", geometry)).toThrow(ParamError);
	});

	it("caps the number of blocks per request", () => {
		const big = new Geometry(16384 * 100, 16384 * 100 * 30);
		expect(() => parseWant(Array.from({ length: 21 }, (_, i) => i).join(","), big)).toThrow(/at most/);
		expect(parseWant(Array.from({ length: 20 }, (_, i) => i).join(","), big)).toHaveLength(20);
	});

	it("round-trips through planned blocks", () => {
		const want = parseWant("0,1:1-2,3", geometry);
		const blocks = planBlocks({ want, geometry }, { reqq: undefined, hasPiece: () => true });
		expect(formatWant(blocks, geometry)).toBe("0,1:1-2,3");
	});
});

describe("planBlocks", () => {
	it("skips pieces the peer lacks and caps at its reqq", () => {
		const want = parseWant("0,1,2", geometry);
		const peer = { reqq: 4, hasPiece: (p: number) => p !== 1 };
		const blocks = planBlocks({ want, geometry }, peer);
		expect(blocks).toEqual([
			[0, 0, 16384],
			[0, 16384, 16384],
			[0, 32768, 7232],
			[2, 0, 16384],
		]);
		expect(formatWant(blocks, geometry)).toBe("0,2:0");
	});

	it("assumes a queue depth of 250 when the peer doesn't say", () => {
		const big = new Geometry(16384 * 400, 16384 * 400);
		const blocks = planBlocks({ want: parseWant("0", big), geometry: big }, { reqq: undefined, hasPiece: () => true });
		expect(blocks).toHaveLength(250);
	});
});

describe("parseBlocksRequest", () => {
	const base = { ih: IH, pl: "40000", len: "121000", want: "0", c: `1.2.3.4:6881|1700000000|${TOKEN}` };

	it("parses a valid request", () => {
		const req = parseBlocksRequest(url({ ...base, c: [base.c, `[2001:db8::1]:51413|1700000000|${TOKEN}`], wait: "5000" }));
		expect(req.infoHashHex).toBe(IH);
		expect(req.infoHash[0]).toBe(0xc9);
		expect(req.candidates.map((c) => [c.hostname, c.port])).toEqual([
			["1.2.3.4", 6881],
			["[2001:db8::1]", 51413],
		]);
		expect(req.candidates[1].addr).toBe("[2001:db8::1]:51413");
		expect(req.waitMs).toBe(5000);
	});

	it("rejects bad input", () => {
		expect(() => parseBlocksRequest(url({ ...base, ih: "zz" }))).toThrow(ParamError);
		expect(() => parseBlocksRequest(url({ ...base, pl: "0" }))).toThrow(ParamError);
		expect(() => parseBlocksRequest(url({ ...base, c: "1.2.3.4:6881" }))).toThrow(ParamError);
		expect(() => parseBlocksRequest(url({ ...base, c: `example.com:80|1|${TOKEN}` }))).toThrow(ParamError);
		expect(() => parseBlocksRequest(url({ ...base, c: Array(41).fill(base.c) }))).toThrow(ParamError);
		expect(parseBlocksRequest(url({ ...base, c: Array(40).fill(base.c) })).candidates).toHaveLength(40);
		expect(parseBlocksRequest(url({ ...base, wait: "9000" })).holdMs).toBe(9000);
		expect(parseBlocksRequest(url({ ...base, wait: "9000", hold: "2000" })).holdMs).toBe(2000);
		expect(() => parseBlocksRequest(url({ ...base, hold: "10" }))).toThrow(ParamError);
		expect(parseBlocksRequest(url(base)).staggerMs).toBe(0);
		expect(parseBlocksRequest(url({ ...base, stagger: "800" })).staggerMs).toBe(800);
		expect(() => parseBlocksRequest(url({ ...base, wait: "99999999" }))).toThrow(ParamError);
		expect(() => parseBlocksRequest(url({ ...base, c: `1.2.3.4:0|1|${TOKEN}` }))).toThrow(ParamError);
	});
});
