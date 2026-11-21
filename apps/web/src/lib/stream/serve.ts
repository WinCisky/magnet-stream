// How the streaming service worker answers one request, apart from where
// the bytes come from (sw.ts asks the pages; tests hand in a fake). An
// invalid path is answered 404 before anything is asked of a page, so no
// store is ever touched for it.

import { MAX_READ } from "./protocol.ts";
import { parseStreamPath, planResponse, type StreamTarget } from "./http.ts";

/** An open file of a page, as the worker sees it. */
export interface FileLink {
	size: number;
	name: string;
	/** Up to `length` bytes at `offset`, waiting for missing pieces; empty past the end. Rejects if the file went away. */
	read(offset: number, length: number): Promise<Uint8Array>;
	/** The response is over: the page may forget this request. */
	close(): void;
}

/** Find the file a page has open (null: no page has it). */
export type FindFile = (target: StreamTarget) => Promise<FileLink | null>;

/** How much each pull asks of the page. */
const CHUNK = 256 * 1024;

const plain = (status: number, text: string, extra: Record<string, string> = {}) =>
	new Response(text, {
		status,
		headers: { "Content-Type": "text/plain; charset=utf-8", "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store", ...extra },
	});

export async function serveStream(request: Request, scopePath: string, find: FindFile): Promise<Response> {
	if (request.method !== "GET" && request.method !== "HEAD") return plain(405, "Method not allowed", { Allow: "GET, HEAD" });
	const url = new URL(request.url);
	const target = parseStreamPath(url.pathname, scopePath);
	if (!target) return plain(404, "Not found");
	let link: FileLink | null;
	try {
		link = await find(target);
	} catch {
		link = null;
	}
	if (!link) return plain(404, "Not found");
	const plan = planResponse({ range: request.headers.get("Range"), size: link.size, name: link.name, download: url.searchParams.has("download") });
	if (request.method === "HEAD" || plan.start >= plan.end) {
		link.close();
		return new Response(null, { status: plan.status, headers: plan.headers });
	}
	return new Response(body(link, plan.start, plan.end), { status: plan.status, headers: plan.headers });
}

/** The bytes [start, end) as they're pulled: nothing is read ahead of what the browser takes. */
function body(link: FileLink, start: number, end: number): ReadableStream<Uint8Array> {
	let at = start;
	let done = false;
	const finish = () => {
		if (done) return;
		done = true;
		link.close();
	};
	return new ReadableStream<Uint8Array>(
		{
			async pull(controller) {
				if (at >= end) {
					finish();
					controller.close();
					return;
				}
				let bytes: Uint8Array;
				try {
					bytes = await link.read(at, Math.min(CHUNK, MAX_READ, end - at));
				} catch (e) {
					finish();
					controller.error(e);
					return;
				}
				if (bytes.length === 0 || bytes.length > end - at) {
					// The file is shorter than it said, or the page sent too much.
					finish();
					controller.error(new Error("stream ended early"));
					return;
				}
				at += bytes.length;
				controller.enqueue(bytes);
				if (at >= end) {
					finish();
					controller.close();
				}
			},
			cancel() {
				finish();
			},
		},
		{ highWaterMark: 0 },
	);
}
