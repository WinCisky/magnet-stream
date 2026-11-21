// Spread an MP4's samples apart: each sample becomes its own chunk, with
// `gap` zero bytes before it inside `mdat`, and the sample table points at
// the new places. The video plays the same, but its bytes span far more of
// the file, so a seek of a few seconds is a jump of megabytes. Chromium
// then opens a new range request for it instead of waiting for the one in
// flight (it waits when the target is within a few MiB), which is what the
// stream e2e needs to show a seek to data not recovered yet.
//
// Handles what tools/e2e/fixtures holds: one track, `moov` after `mdat`,
// 32-bit `stco` offsets and box sizes. Anything else throws.

export type Box = { type: string; start: number; size: number; children?: Box[] };

const CONTAINERS = new Set(["moov", "trak", "mdia", "minf", "stbl", "edts", "dinf"]);

export function u32(d: Uint8Array, at: number): number {
	return new DataView(d.buffer, d.byteOffset, d.byteLength).getUint32(at);
}

export function parse(d: Uint8Array, start: number, end: number): Box[] {
	const out: Box[] = [];
	for (let at = start; at < end; ) {
		const size = u32(d, at);
		if (size < 8 || at + size > end) throw new Error(`mp4: unsupported box size at ${at}`);
		const type = String.fromCharCode(...d.subarray(at + 4, at + 8));
		const box: Box = { type, start: at, size };
		if (CONTAINERS.has(type)) box.children = parse(d, at + 8, at + size);
		out.push(box);
		at += size;
	}
	return out;
}

export function find(boxes: Box[], path: string[]): Box {
	let level = boxes;
	let box: Box | undefined;
	for (const type of path) {
		box = level.find((b) => b.type === type);
		if (!box) throw new Error(`mp4: no ${path.join("/")}`);
		level = box.children ?? [];
	}
	return box!;
}

function be32(n: number): Uint8Array {
	if (!Number.isInteger(n) || n < 0 || n > 0xffffffff) throw new Error("mp4: value out of 32-bit range");
	const b = new Uint8Array(4);
	new DataView(b.buffer).setUint32(0, n);
	return b;
}

export function concat(parts: Uint8Array[]): Uint8Array {
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
}

/** A box rebuilt with some leaves replaced (by their start offset), sizes recomputed. */
function rebuild(d: Uint8Array, box: Box, replace: Map<number, Uint8Array>): Uint8Array {
	const leaf = replace.get(box.start);
	if (leaf) return leaf;
	if (!box.children) return d.subarray(box.start, box.start + box.size);
	const body = concat(box.children.map((c) => rebuild(d, c, replace)));
	return concat([be32(body.length + 8), d.subarray(box.start + 4, box.start + 8), body]);
}

function fullBox(type: string, payload: Uint8Array): Uint8Array {
	const head = new Uint8Array(12);
	head.set(be32(payload.length + 12), 0);
	head.set(new TextEncoder().encode(type), 4);
	return concat([head, payload]);
}

/** Where each sample of the fixture's one track is, and how big. */
export function sampleTable(mp4: Uint8Array): { top: Box[]; mdat: Box; moov: Box; table: Box[]; offsets: number[]; sizes: number[]; description: number } {
	const top = parse(mp4, 0, mp4.length);
	const mdat = find(top, ["mdat"]);
	const moov = find(top, ["moov"]);
	if (moov.start < mdat.start) throw new Error("mp4: moov must follow mdat");
	if (moov.children!.filter((b) => b.type === "trak").length !== 1) throw new Error("mp4: one track only");
	const stbl = find(moov.children!, ["trak", "mdia", "minf", "stbl"]);
	const table = stbl.children!;
	const stsz = find(table, ["stsz"]);
	const stsc = find(table, ["stsc"]);
	const stco = find(table, ["stco"]);

	// Sample sizes.
	const fixed = u32(mp4, stsz.start + 12);
	const count = u32(mp4, stsz.start + 16);
	const sizes = Array.from({ length: count }, (_, i) => (fixed !== 0 ? fixed : u32(mp4, stsz.start + 20 + 4 * i)));
	// Sample offsets, from the chunks and how many samples each holds.
	const chunks = Array.from({ length: u32(mp4, stco.start + 12) }, (_, i) => u32(mp4, stco.start + 16 + 4 * i));
	const runs = Array.from({ length: u32(mp4, stsc.start + 12) }, (_, i) => ({
		first: u32(mp4, stsc.start + 16 + 12 * i),
		perChunk: u32(mp4, stsc.start + 20 + 12 * i),
		description: u32(mp4, stsc.start + 24 + 12 * i),
	}));
	if (runs.length === 0 || new Set(runs.map((r) => r.description)).size !== 1) throw new Error("mp4: one sample description only");
	const offsets: number[] = [];
	chunks.forEach((chunkOffset, c) => {
		const run = [...runs].reverse().find((r) => r.first <= c + 1)!;
		let at = chunkOffset;
		for (let k = 0; k < run.perChunk && offsets.length < count; k++) {
			offsets.push(at);
			at += sizes[offsets.length - 1];
		}
	});
	if (offsets.length !== count) throw new Error("mp4: sample table doesn't add up");
	return { top, mdat, moov, table, offsets, sizes, description: runs[0].description };
}

export function spreadSamples(mp4: Uint8Array, gap: number): Uint8Array {
	const { top, mdat, moov, table, offsets, sizes, description } = sampleTable(mp4);
	const count = sizes.length;
	const stsc = find(table, ["stsc"]);
	const stco = find(table, ["stco"]);

	// New mdat: a gap, then the sample, for each sample.
	const parts: Uint8Array[] = [];
	const placed: number[] = [];
	let at = mdat.start + 8;
	const zeros = new Uint8Array(gap);
	for (let i = 0; i < count; i++) {
		parts.push(zeros);
		at += gap;
		placed.push(at);
		parts.push(mp4.subarray(offsets[i], offsets[i] + sizes[i]));
		at += sizes[i];
	}
	const body = concat(parts);
	const newMdat = concat([be32(body.length + 8), new TextEncoder().encode("mdat"), body]);

	// One sample per chunk, at its new place.
	const stscBody = concat([new Uint8Array(4), be32(1), be32(1), be32(1), be32(description)]);
	const stcoBody = concat([new Uint8Array(4), be32(count), ...placed.map(be32)]);
	const replace = new Map([
		[stsc.start, fullBox("stsc", stscBody.subarray(4))],
		[stco.start, fullBox("stco", stcoBody.subarray(4))],
	]);
	const out: Uint8Array[] = [];
	for (const box of top) {
		if (box === mdat) out.push(newMdat);
		else if (box === moov) out.push(rebuild(mp4, moov, replace));
		else out.push(mp4.subarray(box.start, box.start + box.size));
	}
	return concat(out);
}
