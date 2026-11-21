// The streaming service worker (docs/architecture.md §Playback), built to
// /stream-sw.js by scripts/stream-sw.mjs and registered from
// src/pages/index.astro. It answers `/stream/<info-hash>/<file>` with range
// support, so MP4 and WebM play in a plain <video> and the same URL is a
// download link; every other request goes to the network untouched.
//
// It stores nothing and knows no torrent: the page that has the file open
// (bridge.ts) reads it from the piece store through the recovery's
// `read(offset, length)`, and a read of missing data moves the recovery
// there and waits. A file no page of this origin has open is a 404.

import { MAX_READ, parseOpenReply, parseReadReply, type OpenRequest, type PortRequest } from "./protocol.ts";
import type { StreamTarget } from "./http.ts";
import { STREAM_PREFIX } from "./http.ts";
import { serveStream, type FileLink } from "./serve.ts";

// The worker's globals, as far as they're used (the DOM typings don't have them).
interface WindowClient {
	id: string;
	postMessage(message: unknown, transfer: Transferable[]): void;
}
interface FetchEvent extends Event {
	request: Request;
	clientId: string;
	respondWith(response: Promise<Response>): void;
}
interface ExtendableEvent extends Event {
	waitUntil(promise: Promise<unknown>): void;
}
interface WorkerScope {
	location: Location;
	registration: { scope: string };
	clients: {
		get(id: string): Promise<WindowClient | undefined>;
		matchAll(options: { type: "window" }): Promise<WindowClient[]>;
		claim(): Promise<void>;
	};
	skipWaiting(): Promise<void>;
	addEventListener(type: "install" | "activate" | "message", listener: (event: ExtendableEvent) => void): void;
	addEventListener(type: "fetch", listener: (event: FetchEvent) => void): void;
}

const scope = self as unknown as WorkerScope;
/** A page has this long to say whether it has the file open. */
const OPEN_TIMEOUT_MS = 10_000;
/** A read still unanswered after this long: is its page still there? */
const LIVENESS_MS = 30_000;

scope.addEventListener("install", (event) => event.waitUntil(scope.skipWaiting()));
// Control the open pages at once: the first visit can stream without a reload.
scope.addEventListener("activate", (event) => event.waitUntil(scope.clients.claim()));
// Pages ping while bytes flow (protocol.ts KEEPALIVE): receiving a message keeps the worker alive.
scope.addEventListener("message", () => {});

scope.addEventListener("fetch", (event) => {
	const url = new URL(event.request.url);
	const scopePath = new URL(scope.registration.scope).pathname;
	if (url.origin !== scope.location.origin || !url.pathname.startsWith(scopePath + STREAM_PREFIX)) return;
	const clientId = event.clientId;
	event.respondWith(serveStream(event.request, scopePath, (target) => findFile(target, clientId)));
});

/** Ask the requesting page first, then every other page of the site, which one has the file open. */
async function findFile(target: StreamTarget, clientId: string): Promise<FileLink | null> {
	const own = clientId ? await scope.clients.get(clientId) : undefined;
	if (own) {
		const link = await ask(own, target);
		if (link) return link;
	}
	const others = (await scope.clients.matchAll({ type: "window" })).filter((c) => c.id !== own?.id);
	const links = await Promise.all(others.map((c) => ask(c, target)));
	const found = links.find((l) => l !== null) ?? null;
	for (const l of links) if (l !== found) l?.close();
	return found;
}

/** One page's answer for the file, on a port of this request's own. */
function ask(client: WindowClient, target: StreamTarget): Promise<FileLink | null> {
	const channel = new MessageChannel();
	const port = channel.port1;
	return new Promise((resolve) => {
		const timer = setTimeout(() => settle(null), OPEN_TIMEOUT_MS);
		let settled = false;
		const settle = (link: FileLink | null) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			// A link has taken the port over (its own onmessage); otherwise drop it.
			if (!link) {
				port.onmessage = null;
				port.close();
			}
			resolve(link);
		};
		port.onmessage = (event: MessageEvent) => {
			const reply = parseOpenReply(event.data);
			settle(reply?.type === "head" ? openLink(client, port, reply.size, reply.name) : null);
		};
		const request: OpenRequest = { type: "magnet-stream:open", infoHash: target.infoHash, file: target.file };
		try {
			client.postMessage(request, [channel.port2]);
		} catch {
			settle(null);
		}
	});
}

function openLink(client: WindowClient, port: MessagePort, size: number, name: string): FileLink {
	let nextId = 1;
	let closed = false;
	const pending = new Map<number, { resolve(bytes: Uint8Array): void; reject(e: Error): void }>();
	const send = (message: PortRequest) => port.postMessage(message);
	const close = () => {
		if (closed) return;
		closed = true;
		for (const p of pending.values()) p.reject(new Error("stream closed"));
		pending.clear();
		try {
			send({ type: "close" });
		} catch {
			// The page is gone.
		}
		port.close();
	};
	port.onmessage = (event: MessageEvent) => {
		const reply = parseReadReply(event.data);
		const waiting = reply && pending.get(reply.id);
		if (!reply || !waiting) return;
		pending.delete(reply.id);
		if (reply.type === "data") waiting.resolve(new Uint8Array(reply.bytes));
		else waiting.reject(new Error("read failed"));
	};
	return {
		size,
		name,
		close,
		read(offset, length) {
			if (closed) return Promise.reject(new Error("stream closed"));
			const id = nextId++;
			return new Promise<Uint8Array>((resolve, reject) => {
				// A read may wait a long time for its pieces; it fails only if the page went away.
				const check = setInterval(() => {
					void scope.clients.get(client.id).then((c) => {
						if (!c && pending.delete(id)) {
							clearInterval(check);
							reject(new Error("page closed"));
						}
					});
				}, LIVENESS_MS);
				pending.set(id, {
					resolve: (bytes) => {
						clearInterval(check);
						resolve(bytes);
					},
					reject: (e) => {
						clearInterval(check);
						reject(e);
					},
				});
				send({ type: "read", id, offset, length: Math.min(length, MAX_READ) });
			});
		},
	};
}
