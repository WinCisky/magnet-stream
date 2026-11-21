// Messages between the streaming service worker (sw.ts) and the pages it
// serves (bridge.ts). The worker holds no files: for each request it asks
// the page (over a MessagePort of its own) whether that file is open, then
// pulls the bytes it needs. Every message read on either side is checked
// here first: a port is an input like any other.

/** Worker → page (window `message` event, one port attached): is this file open? */
export interface OpenRequest {
	type: "magnet-stream:open";
	infoHash: string;
	file: number;
}

/** Page → worker, on the port: the file's size and name, or not open here. */
export type OpenReply = { type: "head"; size: number; name: string } | { type: "none" };

/** Worker → page, on the port. */
export type PortRequest =
	/** Up to `length` bytes at `offset` (a read waits for missing pieces). */
	| { type: "read"; id: number; offset: number; length: number }
	/** The request is over (sent, cancelled): stop reading, drop the port. */
	| { type: "close" };

/** Page → worker, on the port: bytes for read `id` (empty: past the end), or a failure. */
export type ReadReply = { type: "data"; id: number; bytes: ArrayBuffer } | { type: "error"; id: number };

/** Page → worker (global `message`): keeps the worker alive while bytes still flow. */
export const KEEPALIVE = "magnet-stream:keepalive";

/** Largest read asked for at once. */
export const MAX_READ = 1024 * 1024;
/** Longest file name accepted from a page. */
const MAX_NAME = 4096;
const HEX40 = /^[0-9a-f]{40}$/;

function record(data: unknown): Record<string, unknown> | null {
	return typeof data === "object" && data !== null && !Array.isArray(data) ? (data as Record<string, unknown>) : null;
}

const count = (v: unknown, max = Number.MAX_SAFE_INTEGER): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0 && v <= max;

export function parseOpenRequest(data: unknown): OpenRequest | null {
	const m = record(data);
	if (!m || m.type !== "magnet-stream:open" || typeof m.infoHash !== "string" || !HEX40.test(m.infoHash) || !count(m.file, 999_999)) return null;
	return { type: m.type, infoHash: m.infoHash, file: m.file };
}

export function parseOpenReply(data: unknown): OpenReply | null {
	const m = record(data);
	if (!m) return null;
	if (m.type === "none") return { type: "none" };
	if (m.type !== "head" || !count(m.size) || typeof m.name !== "string" || m.name.length > MAX_NAME) return null;
	return { type: "head", size: m.size, name: m.name };
}

export function parsePortRequest(data: unknown): PortRequest | null {
	const m = record(data);
	if (!m) return null;
	if (m.type === "close") return { type: "close" };
	if (m.type !== "read" || !count(m.id) || !count(m.offset) || !count(m.length, MAX_READ) || m.length === 0) return null;
	return { type: "read", id: m.id, offset: m.offset, length: m.length };
}

export function parseReadReply(data: unknown): ReadReply | null {
	const m = record(data);
	if (!m || !count(m.id)) return null;
	if (m.type === "error") return { type: "error", id: m.id };
	if (m.type !== "data" || !(m.bytes instanceof ArrayBuffer) || m.bytes.byteLength > MAX_READ) return null;
	return { type: "data", id: m.id, bytes: m.bytes };
}
