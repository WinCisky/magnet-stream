import { describe, expect, it } from "vitest";
import { BencodeError, decodeBencode, decodeBencodePrefix, encodeBencode } from "../src/bencode.ts";

describe("bencode", () => {
	it("round-trips, with keys sorted by bytes", () => {
		const bytes = encodeBencode({ b: [1, "x"], a: new Uint8Array([0, 255]), "a.utf-8": "y", skip: undefined });
		expect(new TextDecoder().decode(bytes.subarray(0, 11))).toBe("d1:a2:\u0000\uFFFD7:a");
		const value = decodeBencode(bytes) as Map<string, unknown>;
		expect([...value.keys()]).toEqual(["a", "a.utf-8", "b"]);
	});

	it("decodes a prefix and says where it ended", () => {
		const dict = encodeBencode({ msg_type: 1, piece: 0 });
		const data = new Uint8Array([...dict, 1, 2, 3]);
		const { value, length } = decodeBencodePrefix(data);
		expect(length).toBe(dict.length);
		expect((value as Map<string, number>).get("piece")).toBe(0);
		expect(() => decodeBencode(data)).toThrow(/trailing/);
	});

	it("enforces limits and rejects duplicate keys", () => {
		const many = encodeBencode(Array.from({ length: 100 }, (_, i) => i));
		expect(() => decodeBencode(many, { maxItems: 50 })).toThrow(/too many/);
		expect(() => decodeBencode(encodeBencode([[[1]]]), { maxDepth: 2 })).toThrow(/deeply/);
		expect(() => decodeBencode(new TextEncoder().encode("d1:ai1e1:ai2ee"))).toThrow(BencodeError);
		expect(() => decodeBencode(new TextEncoder().encode("i123456789012345678901234e"))).toThrow(BencodeError);
	});
});
