import { describe, expect, it } from "vitest";
import { formatAddress, formatIp, parseAddress, parseIp } from "../src/index.ts";

describe("addresses", () => {
	it("parses and formats IPv4", () => {
		expect(parseIp("203.0.113.7")).toEqual({ family: 4, ip: Uint8Array.of(203, 0, 113, 7) });
		for (const bad of ["1.2.3", "1.2.3.256", "01.2.3.4", "1.2.3.4.5", "a.b.c.d", ""]) expect(parseIp(bad)).toBeUndefined();
	});

	it("parses and formats IPv6 (RFC 5952)", () => {
		for (const [text, canonical] of [
			["2001:db8::1", "2001:db8::1"],
			["::1", "::1"],
			["::", "::"],
			["2001:0db8:0000:0000:0000:ff00:0042:8329", "2001:db8::ff00:42:8329"],
			["::ffff:1.2.3.4", "::ffff:102:304"],
			["fe80::1:0:0:1", "fe80::1:0:0:1"],
		]) {
			const parsed = parseIp(text);
			expect(parsed?.family).toBe(6);
			expect(formatIp(6, parsed!.ip)).toBe(canonical);
		}
		for (const bad of ["1::2::3", "12345::", ":::", "g::1", "1:2:3:4:5:6:7:8:9", "::1.2.3"]) expect(parseIp(bad)).toBeUndefined();
	});

	it("round-trips host:port", () => {
		for (const text of ["1.2.3.4:6881", "[2001:db8::1]:51413"]) expect(formatAddress(parseAddress(text)!)).toBe(text);
		for (const bad of ["1.2.3.4", "1.2.3.4:0", "1.2.3.4:65536", "[1.2.3.4]:80", "2001:db8::1:80", "host:80"]) {
			expect(parseAddress(bad)).toBeUndefined();
		}
	});
});
