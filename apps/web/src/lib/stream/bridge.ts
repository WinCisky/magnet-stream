// The page's side of the streaming service worker (sw.ts): the files this
// page has open, by info-hash and file index, and the answers to the
// worker's questions about them. Only files handed to `serveFile` are ever
// served, and only while they stay open; the bytes come from the file's
// `read(offset, into)` (a recovering torrent file: a read of missing data
// moves the recovery there and waits).

import { KEEPALIVE, MAX_READ, parseOpenRequest, parsePortRequest, type OpenReply, type ReadReply } from "./protocol.ts";
import { streamPath } from "./http.ts";

/** Anything with a size and range reads (a torrent file's stream). */
export interface StreamSource {
	size(signal?: AbortSignal): Promise<number>;
	read(offset: number, into: Uint8Array, signal?: AbortSignal): Promise<number>;
}

interface OpenFile {
	source: StreamSource;
	name: string;
	/** Reads in progress for the worker, stopped when the file is closed. */
	readonly ports: Set<{ abort(): void }>;
}

const files = new Map<string, OpenFile>();
/** Ports with a response in progress (they keep the worker alive). */
let streaming = 0;
let keepalive: ReturnType<typeof setInterval> | undefined;
/** A ping this often while bytes flow (the worker may be stopped after 30 s without events). */
const KEEPALIVE_MS = 20_000;
let listening = false;

const key = (infoHash: string, file: number) => `${infoHash}/${file}`;

/** The site's base path ("/" at the root). */
function basePath(): string {
	return import.meta.env.BASE_URL.replace(/\/?$/, "/");
}

/** The URL of an open file: a <video> src, or a download link with `?download=1`. */
export function streamUrl(infoHash: string, file: number): string {
	return streamPath(infoHash, file, basePath());
}

/**
 * Whether the streaming worker controls this page (it may take a moment on
 * the first visit, never after a hard reload). Resolves false after `ms`.
 */
export async function streamWorkerReady(ms = 3_000): Promise<boolean> {
	const sw = typeof navigator !== "undefined" ? navigator.serviceWorker : undefined;
	if (!sw || typeof window === "undefined" || !window.isSecureContext) return false;
	listen();
	if (sw.controller) return true;
	// Activated yet not in control: a hard reload, which no worker sees until
	// the next load. Otherwise it is being installed (the first visit): wait.
	const registration = await sw.getRegistration().catch(() => undefined);
	const settled = registration?.active?.state === "activated" && !registration.installing && !registration.waiting;
	const wait = settled ? Math.min(ms, 300) : ms;
	if (sw.controller) return true;
	return new Promise((resolve) => {
		const done = (ok: boolean) => {
			clearTimeout(timer);
			sw.removeEventListener("controllerchange", onChange);
			resolve(ok);
		};
		const onChange = () => done(sw.controller !== null);
		const timer = setTimeout(() => done(sw.controller !== null), wait);
		sw.addEventListener("controllerchange", onChange);
	});
}

/**
 * Serve a file at `streamUrl(infoHash, file)` until the returned function is
 * called (the reads in progress are stopped then).
 */
export function serveFile(infoHash: string, file: number, source: StreamSource, name: string): () => void {
	listen();
	const k = key(infoHash, file);
	const entry: OpenFile = { source, name, ports: new Set() };
	files.set(k, entry);
	return () => {
		if (files.get(k) === entry) files.delete(k);
		for (const p of entry.ports) p.abort();
		entry.ports.clear();
	};
}

function listen(): void {
	if (listening || typeof navigator === "undefined" || !navigator.serviceWorker) return;
	listening = true;
	navigator.serviceWorker.addEventListener("message", (event: MessageEvent) => {
		const request = parseOpenRequest(event.data);
		const port = event.ports[0];
		if (!request || !port) return;
		// Only our own worker asks (a message from elsewhere has no business here).
		if (!(event.source instanceof ServiceWorker) || event.source !== navigator.serviceWorker.controller) {
			port.close();
			return;
		}
		void answer(port, files.get(key(request.infoHash, request.file)));
	});
}

async function answer(port: MessagePort, file: OpenFile | undefined): Promise<void> {
	const reply = (message: OpenReply) => port.postMessage(message);
	if (!file) {
		reply({ type: "none" });
		port.close();
		return;
	}
	const controller = new AbortController();
	let size: number;
	try {
		size = await file.source.size(controller.signal);
	} catch {
		reply({ type: "none" });
		port.close();
		return;
	}
	const handle = {
		abort: () => {
			if (controller.signal.aborted) return;
			controller.abort();
			port.close();
			file.ports.delete(handle);
			stopped();
		},
	};
	file.ports.add(handle);
	started();
	port.onmessage = (event: MessageEvent) => {
		const request = parsePortRequest(event.data);
		if (!request) return;
		if (request.type === "close") handle.abort();
		else void read(port, file.source, request.id, request.offset, request.length, controller.signal);
	};
	reply({ type: "head", size, name: file.name });
}

async function read(port: MessagePort, source: StreamSource, id: number, offset: number, length: number, signal: AbortSignal): Promise<void> {
	let message: ReadReply;
	try {
		const into = new Uint8Array(Math.min(length, MAX_READ));
		const n = await source.read(offset, into, signal);
		const bytes = into.slice(0, n).buffer;
		message = { type: "data", id, bytes };
	} catch {
		message = { type: "error", id };
	}
	if (signal.aborted) return;
	if (message.type === "data") port.postMessage(message, [message.bytes]);
	else port.postMessage(message);
}

function started(): void {
	streaming++;
	keepalive ??= setInterval(() => navigator.serviceWorker?.controller?.postMessage(KEEPALIVE), KEEPALIVE_MS);
}

function stopped(): void {
	streaming = Math.max(0, streaming - 1);
	if (streaming === 0 && keepalive !== undefined) {
		clearInterval(keepalive);
		keepalive = undefined;
	}
}

/** Hidden frames of downloads in progress are removed after this long (the download goes on without them). */
const DOWNLOAD_FRAME_MS = 60_000;

/**
 * Download an open file from its stream URL. Chromium sends a link with a
 * `download` attribute straight to the network, past the service worker; a
 * navigation is seen by the worker, and its `attachment` answer turns into
 * a download. It runs in a hidden frame, so the page stays put even if the
 * answer is not a download.
 */
export function downloadStream(url: string): void {
	const target = new URL(url, location.href);
	if (target.origin !== location.origin) throw new Error("not a stream URL");
	target.searchParams.set("download", "1");
	const frame = document.createElement("iframe");
	frame.hidden = true;
	frame.src = target.pathname + target.search;
	document.body.append(frame);
	setTimeout(() => frame.remove(), DOWNLOAD_FRAME_MS);
}
