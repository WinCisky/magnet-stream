import { describe, expect, it } from "vitest";
import { consumePeerBody, parsePeerLine } from "../src/peers-stream.ts";

function body(text: string): ReadableStream<Uint8Array> {
	return new ReadableStream({
		start(controller) {
			controller.enqueue(new TextEncoder().encode(text));
			controller.close();
		},
	});
}

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
});
