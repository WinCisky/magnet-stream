// Bare tunnel client for gateway tests: frames in, frames out.

import { SUBPROTOCOL, decodeMessage, encodeFrames, type Frame } from "@magnet-stream/tunnel-protocol";

export interface RawTunnel {
	frames: Frame[];
	closed: Promise<{ code: number; reason: string }>;
	send(...frames: Frame[]): void;
	waitFor(pred: (f: Frame) => boolean, ms?: number): Promise<Frame>;
	close(): void;
}

export async function openTunnel(base: string, session?: string): Promise<RawTunnel> {
	const query = session === undefined ? "" : `?session=${session}`;
	const ws = new WebSocket(base.replace(/^http/, "ws") + "/v1/tunnel" + query, SUBPROTOCOL);
	ws.binaryType = "arraybuffer";
	const frames: Frame[] = [];
	const waiters = new Set<() => void>();
	ws.addEventListener("message", (e) => {
		const r = decodeMessage(new Uint8Array(e.data as ArrayBuffer));
		if (!r.ok) throw new Error(`gateway sent garbage: ${r.error.message}`);
		frames.push(...r.frames);
		for (const w of waiters) w();
	});
	const closed = new Promise<{ code: number; reason: string }>((resolve) => ws.addEventListener("close", (e) => resolve({ code: e.code, reason: e.reason })));
	await new Promise<void>((resolve, reject) => {
		ws.addEventListener("open", () => resolve(), { once: true });
		ws.addEventListener("error", () => reject(new Error("websocket error")), { once: true });
	});
	return {
		frames,
		closed,
		send: (...fs) => ws.send(encodeFrames(fs)),
		close: () => ws.close(1000),
		waitFor: (pred, ms = 15_000) =>
			new Promise((resolve, reject) => {
				const check = () => {
					const f = frames.find(pred);
					if (f) {
						waiters.delete(check);
						clearTimeout(timer);
						resolve(f);
					}
				};
				const timer = setTimeout(() => {
					waiters.delete(check);
					reject(new Error(`timed out; frames so far: ${JSON.stringify(frames.map((f) => [f.type, f.conn]))}`));
				}, ms);
				waiters.add(check);
				check();
			}),
	};
}
