// The file list from whoever answers first (docs/architecture.md › Session
// start, step 3): the info dict kept in session memory, the `/peers`
// stream's `info` line, or BEP 9 from the session's own peers. Every one
// goes through the same check against the info-hash (MetadataFetcher): a
// forged dict from the VPS or a tampered cache is dropped and the peers
// carry on. This only remembers who offered what, to name the source.

import type { MetadataFetcher, MetadataResult } from "@magnet-stream/engine";
import type { MetadataOrigin } from "./session-protocol.ts";

export type ExternalOrigin = Exclude<MetadataOrigin, "peer">;

export class MetadataRace {
	private readonly offered: { bytes: Uint8Array; origin: ExternalOrigin }[] = [];
	private checks = 0;
	/** Outside offers refused (did not hash right), by origin. */
	readonly rejected: Record<ExternalOrigin, number> = { cache: 0, vps: 0 };
	/** The verified metadata and where it came from. */
	readonly done: Promise<{ result: MetadataResult; source: MetadataOrigin }>;

	constructor(private readonly fetcher: Pick<MetadataFetcher, "offerBytes" | "isDone" | "done">) {
		this.done = fetcher.done.then((result) => {
			const source = this.originOf(result);
			this.offered.length = 0;
			return { result, source };
		});
		this.done.catch(() => {});
	}

	/** An info dict from outside is being checked. */
	get checking(): boolean {
		return this.checks > 0;
	}

	/** Offer an info dict from outside; true if it verified and ended the search. */
	async offer(bytes: Uint8Array, origin: ExternalOrigin): Promise<boolean> {
		if (this.fetcher.isDone) return false;
		const entry = { bytes, origin };
		this.offered.push(entry);
		this.checks++;
		try {
			const ok = await this.fetcher.offerBytes(bytes);
			if (!ok && !this.fetcher.isDone) this.rejected[origin]++;
			return ok;
		} finally {
			this.checks--;
			const at = this.offered.indexOf(entry);
			if (!this.fetcher.isDone && at >= 0) this.offered.splice(at, 1);
		}
	}

	private originOf(result: MetadataResult): MetadataOrigin {
		if (result.source === "peer") return "peer";
		const match = this.offered.find((o) => same(o.bytes, result.bytes));
		return match?.origin ?? "vps";
	}
}

function same(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}
