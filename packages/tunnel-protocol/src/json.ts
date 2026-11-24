// JSON form of frames (byte fields as lowercase hex), used by the fixtures
// in ../fixtures so other implementations (e.g. the Rust gateway) can
// check themselves against the same vectors.

import type { PeerAddress } from "./address.ts";
import type { Frame } from "./codec.ts";

export type JsonFrame = Record<string, string | number | string[] | JsonAddress>;
export interface JsonAddress {
	family: 4 | 6;
	ip: string;
	port: number;
}

export function toHex(bytes: Uint8Array): string {
	return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

export function fromHex(hex: string): Uint8Array {
	if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) throw new RangeError("bad hex");
	const out = new Uint8Array(hex.length / 2);
	for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	return out;
}

export function frameToJson(frame: Frame): JsonFrame {
	const out: JsonFrame = {};
	for (const [key, value] of Object.entries(frame)) {
		if (value instanceof Uint8Array) out[key] = toHex(value);
		else if (Array.isArray(value)) out[key] = [...(value as string[])];
		else if (key === "address") {
			const a = value as PeerAddress;
			out[key] = { family: a.family, ip: toHex(a.ip), port: a.port };
		} else out[key] = value as string | number;
	}
	return out;
}

export function frameFromJson(json: JsonFrame): Frame {
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(json)) {
		if (key === "address") {
			const a = value as JsonAddress;
			out[key] = { family: a.family, ip: fromHex(a.ip), port: a.port };
		} else if (Array.isArray(value)) out[key] = [...value];
		else if (key === "type" || typeof value === "number") out[key] = value;
		else out[key] = fromHex(value as string);
	}
	return out as unknown as Frame;
}
