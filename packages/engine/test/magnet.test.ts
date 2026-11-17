import { describe, expect, it } from "vitest";
import { MagnetError, parseMagnet } from "../src/magnet.ts";

const HEX = "c9e15763f722f23e98a29decdfae341b98d53056";
const B32 = "ZHQVOY7XELZD5GFCTXWN7LRUDOMNKMCW";
const V2 = "caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e";

describe("parseMagnet", () => {
	it("reads btih in hex or base32, trackers, name and peer hints", () => {
		const m = parseMagnet(`magnet:?xt=urn:btih:${HEX.toUpperCase()}&dn=My%20Movie&tr=udp%3A%2F%2Ft.example%3A1337&tr=udp%3A%2F%2Ft.example%3A1337&x.pe=1.2.3.4%3A6881`);
		expect(m.infoHashHex).toBe(HEX);
		expect(m.v1).toBe(HEX);
		expect(m.v2).toBeUndefined();
		expect(m.name).toBe("My Movie");
		expect(m.trackers).toEqual(["udp://t.example:1337"]);
		expect(m.peers).toEqual(["1.2.3.4:6881"]);
		expect(parseMagnet(`magnet:?xt=urn:btih:${B32}`).infoHashHex).toBe(HEX);
	});

	it("reads v2-only (btmh, truncated on the wire) and hybrid magnets", () => {
		const v2 = parseMagnet(`magnet:?xt=urn:btmh:1220${V2}`);
		expect(v2.infoHashHex).toBe(V2.slice(0, 40));
		expect(v2.infoHash).toHaveLength(20);
		expect(v2.v2).toBe(V2);
		const hybrid = parseMagnet(`magnet:?xt=urn:btmh:1220${V2}&xt=urn:btih:${HEX}`);
		expect(hybrid.infoHashHex).toBe(HEX);
		expect([hybrid.v1, hybrid.v2]).toEqual([HEX, V2]);
	});

	it("rejects anything else", () => {
		for (const bad of ["", "http://x", "magnet:?dn=x", `magnet:?xt=urn:btih:${HEX.slice(1)}`, `magnet:?xt=urn:btmh:1114${V2}`, `magnet:?xt=urn:btih:${HEX}&dn=${"a".repeat(9000)}`]) {
			expect(() => parseMagnet(bad), bad.slice(0, 40)).toThrow(MagnetError);
		}
	});

	it("caps and filters trackers", () => {
		const many = Array.from({ length: 80 }, (_, i) => `&tr=udp%3A%2F%2Ft${i}.example%3A1`).join("");
		const m = parseMagnet(`magnet:?xt=urn:btih:${HEX}${many}&tr=javascript%3Aalert(1)&tr=file%3A%2F%2F%2Fetc`);
		expect(m.trackers).toHaveLength(50);
		expect(m.trackers.every((t) => t.startsWith("udp://"))).toBe(true);
	});
});
