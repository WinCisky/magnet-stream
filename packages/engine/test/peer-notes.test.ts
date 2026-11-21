// Peer notes (M3-04): what a session learned about addresses, kept by the
// page in IndexedDB and handed to the next session of the same torrent.
// Delivering peers come back first, failed ones last, banned ones never;
// stored notes are untrusted and re-validated.

import { describe, expect, it } from "vitest";
import { MAX_PEER_NOTES, PeerBook, SCOUT_NOTE_TTL_MS, parsePeerNote, type PeerNote } from "../src/peer-book.ts";

function clock(start = 10_000_000) {
	const now = { t: start };
	return { now, book: () => new PeerBook({ now: () => now.t }) };
}

describe("peer notes", () => {
	it("keep what was learned, most useful first, and leave plain untried addresses out", () => {
		const { now, book: make } = clock();
		const book = make();
		book.addMany(["1.1.1.1:6881", "2.2.2.2:6881", "3.3.3.3:6881", "4.4.4.4:6881", "5.5.5.5:6881", "6.6.6.6:6881"], "discovery", undefined, [
			undefined,
			undefined,
			undefined,
			{ score: 0.9, alive: true, seed: true },
			undefined,
			undefined,
		]);
		book.markConnecting("1.1.1.1:6881");
		book.markConnected("1.1.1.1:6881");
		book.markDelivered("1.1.1.1:6881", 1 << 20);
		book.markConnecting("2.2.2.2:6881");
		book.markConnected("2.2.2.2:6881");
		book.markClosed("2.2.2.2:6881");
		book.markConnecting("3.3.3.3:6881");
		book.markFailed("3.3.3.3:6881", "refused");
		book.ban("5.5.5.5:6881", "corrupt");
		now.t += 1000;

		const notes = book.notes();
		expect(notes.map((n) => n.addr)).toEqual(["1.1.1.1:6881", "2.2.2.2:6881", "4.4.4.4:6881", "3.3.3.3:6881", "5.5.5.5:6881"]);
		expect(notes[0]).toEqual({ addr: "1.1.1.1:6881", delivered: 1 << 20, connected: true, failures: 0 });
		expect(notes[2].scout).toEqual({ score: 0.9, alive: true, seed: true });
		expect(notes[2].scoutAt).toBe(10_000_000);
		expect(notes[3]).toMatchObject({ connected: false, failures: 1, failure: "refused" });
		expect(notes[4]).toMatchObject({ banned: true, failure: "corrupt" });
		expect(book.notes(2)).toHaveLength(2);
		// Survives a JSON round trip (what IndexedDB stores is structured-cloned; JSON is stricter).
		expect(JSON.parse(JSON.stringify(notes)).map(parsePeerNote)).toEqual(notes);
	});

	it("restore: delivering peers are dialled first and as known, failed ones last, banned never", () => {
		const { book: make } = clock();
		const notes: PeerNote[] = [
			{ addr: "3.3.3.3:6881", delivered: 0, connected: false, failures: 2, failure: "timeout" },
			{ addr: "1.1.1.1:6881", delivered: 5 << 20, connected: true, failures: 0 },
			{ addr: "5.5.5.5:6881", delivered: 0, connected: true, failures: 1, failure: "corrupt", banned: true },
			{ addr: "2.2.2.2:6881", delivered: 0, connected: true, failures: 0 },
		];
		const book = make();
		expect(book.restore(notes)).toBe(4);
		book.addMany(["9.9.9.9:6881"], "discovery");
		expect(book.candidates(10).map((p) => p.key)).toEqual(["1.1.1.1:6881", "2.2.2.2:6881", "9.9.9.9:6881", "3.3.3.3:6881"]);
		expect(book.dialClass(book.get("1.1.1.1:6881")!)).toBe("known");
		expect(book.isBanned("5.5.5.5:6881")).toBe(true);
		// Earlier sessions' bytes are a prior, not this session's count.
		expect(book.counts().delivered).toBe(0);
		expect(book.best(1)[0].key).toBe("1.1.1.1:6881");
		// And they are carried into the next notes.
		expect(book.notes().find((n) => n.addr === "1.1.1.1:6881")?.delivered).toBe(5 << 20);
	});

	it("restore: the scout's facts come back as the prior, unless stale", () => {
		const { now, book: make } = clock();
		const notes: PeerNote[] = [
			{ addr: "1.1.1.1:6881", delivered: 0, connected: false, failures: 0, scout: { score: 0.4, alive: true }, scoutAt: now.t - 60_000 },
			{ addr: "2.2.2.2:6881", delivered: 0, connected: false, failures: 0, scout: { score: 0.9, alive: true, seed: true }, scoutAt: now.t - 60_000 },
			{ addr: "3.3.3.3:6881", delivered: 0, connected: false, failures: 0, scout: { score: 0.02, alive: false }, scoutAt: now.t - 60_000 },
			{ addr: "4.4.4.4:6881", delivered: 0, connected: false, failures: 0, scout: { score: 0.95, alive: true, seed: true }, scoutAt: now.t - SCOUT_NOTE_TTL_MS - 1 },
		];
		const book = make();
		book.restore(notes);
		expect(book.candidates(10).map((p) => p.key)).toEqual(["2.2.2.2:6881", "1.1.1.1:6881", "4.4.4.4:6881"]);
		expect(book.get("4.4.4.4:6881")!.scout).toBeUndefined();
		expect(book.dialClass(book.get("2.2.2.2:6881")!)).toBe("scout");
		expect(book.get("2.2.2.2:6881")!.scoutAt).toBe(now.t - 60_000);
	});

	it("rejects malformed notes and keeps the rest", () => {
		const { book: make } = clock();
		const book = make();
		const good = { addr: "1.1.1.1:6881", delivered: 10, connected: true, failures: 0 };
		const bad: unknown[] = [
			null,
			"1.1.1.1:6881",
			{ ...good, addr: "not an address" },
			{ ...good, addr: "1.1.1.1:6881".padEnd(80, "0") },
			{ ...good, delivered: -1 },
			{ ...good, delivered: 1.5 },
			{ ...good, connected: "yes" },
			{ ...good, failures: 1e9 },
			{ ...good, failure: "<script>" },
			{ ...good, banned: 1 },
			{ ...good, scout: { score: 2 }, scoutAt: 1 },
			{ ...good, scout: { score: 0.5 } },
		];
		for (const b of bad) expect(parsePeerNote(b)).toBeNull();
		expect(book.restore([...bad, good])).toBe(1);
		expect(book.rejected.invalid).toBe(bad.length);
		// Private addresses are refused by the book's own rule.
		expect(book.restore([{ ...good, addr: "192.168.1.2:6881" }])).toBe(0);
		// A flood is capped.
		const flood = Array.from({ length: MAX_PEER_NOTES + 50 }, (_, i) => ({ ...good, addr: `8.8.${i >> 8}.${i & 0xff}:6881` }));
		expect(make().restore(flood)).toBe(MAX_PEER_NOTES);
	});
});
