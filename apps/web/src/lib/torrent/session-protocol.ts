// Messages between the page and the session worker. Both sides validate
// before trusting anything: a bad message is ignored, never interpreted.

export const MAX_WANT_BLOCKS = 512;
export const MAX_BLOCK = 16 * 1024;

export interface SessionFile {
	path: string;
	size: number;
	offset: number;
}

export interface SessionListing {
	infoHash: string;
	name: string;
	size: number;
	pieceLength: number;
	files: SessionFile[];
}

export type PageMessage =
	| { type: "start"; magnet: string; gatewayUrl: string; seedersUrl: string }
	| { type: "want"; id: number; blocks: { piece: number; begin: number; length: number }[] }
	/** The page gave up on a want (seek, batch done): cancel what is still outstanding. */
	| { type: "cancel"; id: number }
	/** Nothing is being recovered: close the WebSockets, keep metadata and peers for a later want. */
	| { type: "idle" }
	| { type: "stop" };

export type WorkerMessage =
	| { type: "files"; listing: SessionListing; info: ArrayBuffer }
	| { type: "peers"; count: number }
	| { type: "block"; id: number; piece: number; begin: number; data: ArrayBuffer }
	| { type: "want-done"; id: number; received: number; missing: number }
	/**
	 * The tunnel so far: upgrades attempted and WebSocket messages sent
	 * (billing), peers known and peers that delivered data.
	 */
	| { type: "stats"; upgrades: number; messages: number; known: number; delivered: number }
	| { type: "error"; message: string };

function record(value: unknown): Record<string, unknown> | null {
	return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : null;
}

function httpUrl(value: unknown): string | null {
	if (typeof value !== "string" || value.length > 2048) return null;
	try {
		const url = new URL(value);
		if (url.protocol !== "http:" && url.protocol !== "https:" && url.protocol !== "ws:" && url.protocol !== "wss:") return null;
		return value;
	} catch {
		return null;
	}
}

export function parsePageMessage(value: unknown): PageMessage | null {
	const msg = record(value);
	if (!msg) return null;
	if (msg.type === "stop") return { type: "stop" };
	if (msg.type === "idle") return { type: "idle" };
	if (msg.type === "cancel") return Number.isInteger(msg.id) && (msg.id as number) >= 0 ? { type: "cancel", id: msg.id as number } : null;
	if (msg.type === "start") {
		if (typeof msg.magnet !== "string" || msg.magnet.length === 0 || msg.magnet.length > 8 * 1024) return null;
		const gatewayUrl = httpUrl(msg.gatewayUrl);
		const seedersUrl = httpUrl(msg.seedersUrl);
		if (!gatewayUrl || !seedersUrl) return null;
		return { type: "start", magnet: msg.magnet, gatewayUrl, seedersUrl };
	}
	if (msg.type === "want") {
		if (!Number.isInteger(msg.id) || (msg.id as number) < 0) return null;
		if (!Array.isArray(msg.blocks) || msg.blocks.length === 0 || msg.blocks.length > MAX_WANT_BLOCKS) return null;
		const blocks: { piece: number; begin: number; length: number }[] = [];
		for (const raw of msg.blocks) {
			const block = record(raw);
			if (!block) return null;
			const { piece, begin, length } = block;
			if (!Number.isInteger(piece) || !Number.isInteger(begin) || !Number.isInteger(length)) return null;
			if ((piece as number) < 0 || (begin as number) < 0 || (length as number) < 1 || (length as number) > MAX_BLOCK) return null;
			blocks.push({ piece: piece as number, begin: begin as number, length: length as number });
		}
		return { type: "want", id: msg.id as number, blocks };
	}
	return null;
}

export function parseWorkerMessage(value: unknown): WorkerMessage | null {
	const msg = record(value);
	if (!msg || typeof msg.type !== "string") return null;
	if (msg.type === "error") return typeof msg.message === "string" && msg.message.length < 300 ? { type: "error", message: msg.message } : null;
	if (msg.type === "peers") return Number.isInteger(msg.count) && (msg.count as number) >= 0 ? { type: "peers", count: msg.count as number } : null;
	if (msg.type === "stats") {
		const counts = [msg.upgrades, msg.messages, msg.known, msg.delivered];
		if (!counts.every((n) => Number.isInteger(n) && (n as number) >= 0)) return null;
		const [upgrades, messages, known, delivered] = counts as number[];
		return { type: "stats", upgrades, messages, known, delivered };
	}
	if (msg.type === "want-done") {
		if (!Number.isInteger(msg.id) || !Number.isInteger(msg.received) || !Number.isInteger(msg.missing)) return null;
		return { type: "want-done", id: msg.id as number, received: msg.received as number, missing: msg.missing as number };
	}
	if (msg.type === "block") {
		if (!Number.isInteger(msg.id) || !Number.isInteger(msg.piece) || !Number.isInteger(msg.begin)) return null;
		if (!(msg.data instanceof ArrayBuffer) || msg.data.byteLength < 1 || msg.data.byteLength > MAX_BLOCK) return null;
		return { type: "block", id: msg.id as number, piece: msg.piece as number, begin: msg.begin as number, data: msg.data };
	}
	if (msg.type === "files") {
		const listing = parseListing(msg.listing);
		if (!listing || !(msg.info instanceof ArrayBuffer) || msg.info.byteLength < 1 || msg.info.byteLength > 16 * 1024 * 1024) return null;
		return { type: "files", listing, info: msg.info };
	}
	return null;
}

function parseListing(value: unknown): SessionListing | null {
	const listing = record(value);
	if (!listing) return null;
	if (typeof listing.infoHash !== "string" || !/^[0-9a-f]{40}$/.test(listing.infoHash)) return null;
	if (typeof listing.name !== "string" || listing.name.length > 1024) return null;
	if (!Number.isInteger(listing.size) || (listing.size as number) < 0) return null;
	if (!Number.isInteger(listing.pieceLength) || (listing.pieceLength as number) < 1) return null;
	if (!Array.isArray(listing.files) || listing.files.length > 100_000) return null;
	const files: SessionFile[] = [];
	for (const raw of listing.files) {
		const file = record(raw);
		if (!file || typeof file.path !== "string" || file.path.length > 4096) return null;
		if (!Number.isInteger(file.size) || !Number.isInteger(file.offset)) return null;
		if ((file.size as number) < 0 || (file.offset as number) < 0) return null;
		files.push({ path: file.path, size: file.size as number, offset: file.offset as number });
	}
	return { infoHash: listing.infoHash, name: listing.name, size: listing.size as number, pieceLength: listing.pieceLength as number, files };
}
