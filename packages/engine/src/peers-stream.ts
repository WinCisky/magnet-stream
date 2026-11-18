// One `GET /peers` NDJSON stream (docs/discovery-api.md). Each `peer` line
// is an address to dial; anything else (the closing `done` line, garbage)
// is ignored. The caller makes a single request: this reader does not retry.

const PEER_LINE = /^(?:(?:\d{1,3}\.){3}\d{1,3}|\[[0-9a-fA-F:.]+\]):\d{1,5}$/;

/** A `{"peer":"ip:port"}` line, or null (done, empty, malformed). */
export function parsePeerLine(line: string): string | null {
	const text = line.trim();
	if (text.length === 0 || text.length > 256) return null;
	let value: unknown;
	try {
		value = JSON.parse(text);
	} catch {
		return null;
	}
	if (!value || typeof value !== "object") return null;
	const peer = (value as { peer?: unknown }).peer;
	if (typeof peer !== "string" || !PEER_LINE.test(peer)) return null;
	const port = Number(peer.slice(peer.lastIndexOf(":") + 1));
	if (!Number.isInteger(port) || port < 1 || port > 65535) return null;
	return peer;
}

/**
 * Read a peers stream. `fetchImpl` defaults to the platform `fetch` (one
 * request). Returns how many addresses were accepted.
 */
export async function readPeerStream(
	url: string,
	onPeer: (address: string) => void,
	opts: { signal?: AbortSignal; fetch?: typeof fetch } = {},
): Promise<number> {
	const res = await (opts.fetch ?? fetch)(url, { signal: opts.signal, headers: { accept: "application/x-ndjson" } });
	if (!res.ok || !res.body) throw new Error(`peers request failed (${res.status})`);
	return consumePeerBody(res.body, onPeer);
}

/** Pull NDJSON peer lines from a body. */
export async function consumePeerBody(body: ReadableStream<Uint8Array>, onPeer: (address: string) => void): Promise<number> {
	const reader = body.getReader();
	const decode = new TextDecoder();
	let buf = "";
	let n = 0;
	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		buf += decode.decode(value, { stream: true });
		let nl = buf.indexOf("\n");
		while (nl >= 0) {
			const peer = parsePeerLine(buf.slice(0, nl));
			buf = buf.slice(nl + 1);
			if (peer) {
				onPeer(peer);
				n++;
			}
			nl = buf.indexOf("\n");
		}
	}
	const tail = parsePeerLine(buf);
	if (tail) {
		onPeer(tail);
		n++;
	}
	return n;
}
