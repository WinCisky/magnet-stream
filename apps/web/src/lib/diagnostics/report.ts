// The export: every visit record, plus a verdict per part of the app
// (good / fair / poor) over all of them, so the weak spots and what works
// fine show at a glance. Loaded only when the user exports.

import type { Environment } from "./env";
import { measurementRow, phaseMetrics } from "./phase";
import type { Milestone, PlaybackRecord, RecoveryRecord, SessionRecord, Timed, VisitRecord } from "./records";
import { MS_BOUNDS, RATE_BOUNDS, add, bump, hist, merge, mergeCounts, quantile, type Counts, type Hist } from "./stats";

export type Status = "good" | "fair" | "poor" | "no data";

export interface PartHealth {
	title: string;
	status: Status;
	summary: string;
	details: Record<string, unknown>;
}

const MB = 1024 * 1024;
const GiB = 1024 * MB;

/**
 * What "good" and "poor" mean for each part (in between is "fair").
 * Latency limits sit on histogram bucket bounds (MS_BOUNDS), rates on
 * RATE_BOUNDS, so a verdict never hinges on where a value fell in a bucket.
 */
export const THRESHOLDS = {
	lookup: { goodSuccess: 0.95, poorSuccess: 0.8, goodP90Ms: 8_000, poorP90Ms: 30_000 },
	/** Sessions that reached at least one peer, and the first unchoke. */
	tunnel: { goodReached: 0.95, poorReached: 0.8, goodUnchokeP90Ms: 8_000, poorUnchokeP90Ms: 30_000 },
	/** `/peers` streams answered, and sessions the scout found a live peer for. */
	scout: { goodAnswered: 0.95, poorAnswered: 0.8, goodAlive: 0.8, poorAlive: 0.5 },
	startup: { goodP90Ms: 8_000, poorP90Ms: 30_000, goodFailShare: 0.05, poorFailShare: 0.25, failAfterMs: 30_000 },
	throughput: { goodMedian: 3 * MB, poorMedian: 1 * MB, goodStallShare: 0.05, poorStallShare: 0.2 },
	seeking: { goodP90Ms: 5_000, poorP90Ms: 13_000 },
	integrity: { goodPerGiB: 1, poorPerGiB: 5, minBytes: 64 * MB },
	playback: {
		goodStartP90Ms: 3_000,
		poorStartP90Ms: 8_000,
		goodWaitShare: 0.02,
		poorWaitShare: 0.1,
		goodStuttersPerMin: 1,
		poorStuttersPerMin: 6,
		poorFailShare: 0.2,
	},
	app: { poorErrorsPerVisit: 0.2 },
};

export interface ReportInput {
	visits: VisitRecord[];
	installId: string;
	build: string;
	environment: Environment;
	storage: { usageMb: number; quotaMb: number; persisted: boolean } | null;
	now: number;
}

export function buildReport(input: ReportInput) {
	const visits = [...input.visits].sort((a, b) => a.startedAt - b.startedAt);
	const recoveries = visits.flatMap((v) => v.recoveries);
	const sessions = visits.flatMap((v) => v.sessions ?? []);
	const health: Record<string, PartHealth> = {
		lookup: lookup(mergeTimed(visits.map((v) => v.metadataApi)), sessions),
		tunnel: tunnel(sessions),
		scout: scout(sessions),
		startup: startup(recoveries),
		throughput: throughput(recoveries),
		seeking: seeking(recoveries),
		integrity: integrity(recoveries),
		playback: playback(recoveries),
		storage: storage(recoveries, input.storage),
		app: app(visits),
	};
	const rank: Record<Status, number> = { poor: 0, fair: 1, good: 2, "no data": 3 };
	const parts = Object.entries(health).sort(([, a], [, b]) => rank[a.status] - rank[b.status]);
	return {
		format: "magnet-watcher-diagnostics",
		version: 2,
		exportedAt: new Date(input.now).toISOString(),
		installId: input.installId,
		build: input.build,
		privacy:
			"Anonymous: no magnet links, names, info-hashes or IP addresses. Torrents appear only as hashes salted " +
			"with a secret that stays in the browser; sizes are rounded.",
		environment: { ...input.environment, storage: input.storage },
		overview: {
			from: visits.length ? new Date(visits[0].startedAt).toISOString() : null,
			to: visits.length ? new Date(visits[visits.length - 1].updatedAt).toISOString() : null,
			visits: visits.length,
			sessions: sessions.length,
			recoveries: recoveries.length,
			completed: recoveries.filter((r) => r.outcome === "complete").length,
			failed: recoveries.filter((r) => r.outcome === "error").length,
			downloadedMb: Math.round(sum(recoveries, (r) => r.transfer.bytes) / MB),
			recoveringMinutes: Math.round(sum(recoveries, (r) => r.transfer.recoveringMs) / 60_000),
			torrents: new Set(recoveries.map((r) => r.torrent).filter(Boolean)).size,
			browsers: [...new Set(visits.map((v) => `${v.env.browser} on ${v.env.os}`))],
			builds: [...new Set(visits.map((v) => v.build))],
		},
		/** The three numbers each phase is measured on (docs/measurements.md). */
		phaseMetrics: phaseMetrics(recoveries),
		/** Rows for the real-world table of docs/measurements.md, newest last. */
		measurementRows: recoveries.filter((r) => r.outcome !== "running").map((r) => measurementRow(r, input.build)),
		weakSpots: parts
			.filter(([, p]) => p.status === "poor" || p.status === "fair")
			.map(([part, p]) => ({ part, title: p.title, status: p.status, summary: p.summary })),
		workingWell: parts.filter(([, p]) => p.status === "good").map(([part, p]) => ({ part, title: p.title, summary: p.summary })),
		health,
		thresholds: THRESHOLDS,
		buckets: { ms: MS_BOUNDS, bytesPerSecond: RATE_BOUNDS },
		visits,
	};
}

function requests(
	title: string,
	t: Timed,
	th: { goodSuccess: number; poorSuccess: number; goodP90Ms: number; poorP90Ms: number },
): PartHealth {
	const n = t.ok + t.fail;
	if (n === 0) return noData(title);
	const success = t.ok / n;
	const p50 = quantile(t.ms, MS_BOUNDS, 0.5);
	const p90 = quantile(t.ms, MS_BOUNDS, 0.9);
	const top = topKey(t.errors);
	return {
		title,
		status: grade(
			success >= th.goodSuccess && (p90 ?? 0) <= th.goodP90Ms,
			success < th.poorSuccess || (p90 ?? 0) > th.poorP90Ms,
		),
		summary:
			`${t.ok} of ${n} requests ok (${pct(success)})` +
			(p50 !== null ? `; half answered within ${sec(p50)}, 90% within ${sec(p90)}` : "") +
			(top ? `; most common error: ${top}` : ""),
		details: { requests: n, ok: t.ok, failed: t.fail, successRate: round(success), p50Ms: p50, p90Ms: p90, errors: t.errors },
	};
}

/** Magnet → file list: each attempt's outcome and time, where the file list came from, why lookups gave up. */
function lookup(t: Timed, sessions: SessionRecord[]): PartHealth {
	const part = requests("Torrent lookup (magnet → file list: cache, VPS info line, BEP 9 from peers)", t, THRESHOLDS.lookup);
	if (part.status === "no data" && sessions.length === 0) return part;
	const sources: Counts = {};
	const failures: Counts = {};
	for (const s of sessions) {
		if (s.metadata) bump(sources, s.metadata.source);
		mergeCounts(failures, s.failures);
	}
	const retries = sum(sessions, (s) => s.retries);
	const listedAfterRetry = sessions.filter((s) => s.retries > 0 && s.lookup === "listed").length;
	const from = Object.entries(sources)
		.map(([k, v]) => `${SOURCE_NAMES[k] ?? k} ${v}`)
		.join(", ");
	return {
		...part,
		summary:
			part.summary +
			(from ? `; file list from ${from}` : "") +
			(retries ? `; ${retries} retr${retries === 1 ? "y" : "ies"}, ${listedAfterRetry} listed after one` : ""),
		details: {
			...part.details,
			sessions: sessions.length,
			sources,
			failures,
			retries,
			listedAfterRetry,
			lookupP50Ms: percentile(listedMs(sessions), 0.5),
			lookupP90Ms: percentile(listedMs(sessions), 0.9),
			metadataP50MsBySource: Object.fromEntries(
				Object.keys(sources).map((k) => [k, percentile(sessions.filter((s) => s.metadata?.source === k).map((s) => s.metadata!.ms), 0.5)]),
			),
		},
	};
}

const SOURCE_NAMES: Record<string, string> = { cache: "the cache", vps: "the VPS", peer: "peers" };

function listedMs(sessions: SessionRecord[]): number[] {
	return sessions.map((s) => s.lookupMs).filter((ms): ms is number => ms !== null);
}

/** The tunnel gateway: dials by class and outcome, peers reached, unchoked, delivering. */
function tunnel(sessions: SessionRecord[]): PartHealth {
	const title = "Tunnel (gateway dials and peers)";
	const used = sessions.filter((s) => dialsOf(s).attempted > 0);
	if (used.length === 0) return noData(title);
	const th = THRESHOLDS.tunnel;
	const reached = used.filter((s) => s.tunnel.connected > 0).length / used.length;
	const unchokes = used.map((s) => s.tunnel.firstUnchokeMs).filter((ms): ms is number => ms !== null);
	const unchokeP50 = percentile(unchokes, 0.5);
	const unchokeP90 = percentile(unchokes, 0.9);
	const byClass = Object.fromEntries(
		(["scout", "untried", "known"] as const).map((k) => {
			const failed: Counts = {};
			let attempted = 0;
			let succeeded = 0;
			for (const s of used) {
				const d = s.tunnel.dials[k];
				attempted += d.attempted;
				succeeded += d.succeeded;
				mergeCounts(failed, d.failed);
			}
			return [k, { attempted, succeeded, failed }];
		}),
	);
	const allFailed: Counts = {};
	for (const c of Object.values(byClass)) mergeCounts(allFailed, c.failed);
	const top = topKey(allFailed);
	const share = (k: string) => {
		const c = byClass[k];
		return c.attempted ? `${pct(c.succeeded / c.attempted)} (${c.succeeded}/${c.attempted})` : "none";
	};
	return {
		title,
		status: grade(
			reached >= th.goodReached && (unchokeP90 ?? 0) <= th.goodUnchokeP90Ms,
			reached < th.poorReached || (unchokeP90 ?? 0) > th.poorUnchokeP90Ms,
		),
		summary:
			`${pct(reached)} of ${used.length} sessions reached a peer; dials connected: scout-alive ${share("scout")}, ` +
			`untried ${share("untried")}, known ${share("known")}` +
			(unchokeP50 !== null ? `; first unchoke within ${sec(unchokeP50)} for half, ${sec(unchokeP90)} for 90%` : "; never unchoked") +
			`; median peers connected ${median(used.map((s) => s.tunnel.connected))}, delivering ${median(used.map((s) => s.tunnel.delivering))}` +
			(top ? `; most common dial failure: ${top}` : ""),
		details: {
			sessions: used.length,
			reachedShare: round(reached),
			dials: byClass,
			medianConnected: median(used.map((s) => s.tunnel.connected)),
			medianUnchoked: median(used.map((s) => s.tunnel.unchoked)),
			medianDelivering: median(used.map((s) => s.tunnel.delivering)),
			medianSeeds: median(used.map((s) => s.tunnel.seeds)),
			firstUnchokeP50Ms: unchokeP50,
			firstUnchokeP90Ms: unchokeP90,
			maxSockets: Math.max(...used.map((s) => s.tunnel.sockets)),
			recycles: sum(used, (s) => s.tunnel.recycles),
			untestedHighWater: Math.max(...used.map((s) => s.tunnel.untestedHighWater)),
			upload: {
				sessionsSharing: used.filter((s) => s.tunnel.upload?.enabled === true).length,
				sessionsUploaded: used.filter((s) => (s.tunnel.upload?.bytes ?? 0) > 0).length,
				bytes: sum(used, (s) => s.tunnel.upload?.bytes ?? 0),
				messages: sum(used, (s) => s.tunnel.upload?.messages ?? 0),
				maxUnchoked: Math.max(0, ...used.map((s) => s.tunnel.upload?.unchoked ?? 0)),
			},
		},
	};
}

function dialsOf(s: SessionRecord): { attempted: number; succeeded: number } {
	const d = s.tunnel.dials;
	return { attempted: d.scout.attempted + d.untried.attempted + d.known.attempted, succeeded: d.scout.succeeded + d.untried.succeeded + d.known.succeeded };
}

/** The VPS `/peers` stream: answered, what the scout said about the addresses, the `info` line. */
function scout(sessions: SessionRecord[]): PartHealth {
	const title = "Discovery and scout (VPS /peers stream)";
	const asked = sessions.filter((s) => s.scout.streams > 0 && s.scout.state !== "open");
	if (asked.length === 0) return noData(title);
	const th = THRESHOLDS.scout;
	const answered = asked.filter((s) => s.scout.state === "done");
	const answeredShare = answered.length / asked.length;
	const aliveShare = answered.length ? answered.filter((s) => s.scout.alive > 0).length / answered.length : 0;
	const info: Counts = {};
	for (const s of asked) bump(info, s.scout.info);
	const rejected = info.rejected ?? 0;
	const first = asked.map((s) => s.scout.firstPeerMs).filter((ms): ms is number => ms !== null);
	return {
		title,
		status: grade(
			answeredShare >= th.goodAnswered && aliveShare >= th.goodAlive && rejected === 0,
			answeredShare < th.poorAnswered || aliveShare < th.poorAlive || rejected > 0,
		),
		summary:
			`${answered.length} of ${asked.length} streams answered (${pct(answeredShare)}); a live peer for ${pct(aliveShare)}` +
			`; median ${median(asked.map((s) => s.scout.peers))} peers (alive ${median(asked.map((s) => s.scout.alive))}, ` +
			`dead ${median(asked.map((s) => s.scout.dead))}, unknown ${median(asked.map((s) => s.scout.unknown))})` +
			(first.length ? `; first peer within ${sec(percentile(first, 0.5))} for half` : "") +
			`; info line accepted ${info.accepted ?? 0}, rejected ${rejected}`,
		details: {
			sessions: asked.length,
			streams: sum(asked, (s) => s.scout.streams),
			failed: asked.filter((s) => s.scout.state === "failed").length,
			answeredShare: round(answeredShare),
			aliveShare: round(aliveShare),
			medianPeers: median(asked.map((s) => s.scout.peers)),
			medianAlive: median(asked.map((s) => s.scout.alive)),
			medianSeeds: median(asked.map((s) => s.scout.seeds)),
			medianDead: median(asked.map((s) => s.scout.dead)),
			medianUnknown: median(asked.map((s) => s.scout.unknown)),
			rejectedLines: sum(asked, (s) => s.scout.rejected),
			info,
			firstPeerP50Ms: percentile(first, 0.5),
			firstPeerP90Ms: percentile(first, 0.9),
		},
	};
}

function startup(recoveries: RecoveryRecord[]): PartHealth {
	const title = "Startup (first 4 MiB of a video not saved before)";
	const th = THRESHOLDS.startup;
	const fresh = recoveries.filter((r) => r.traits.freshStart === true);
	const times = hist(MS_BOUNDS);
	let failed = 0;
	for (const r of fresh) {
		const ready = r.milestones.ready4MiB;
		if (ready !== null) add(times, MS_BOUNDS, ready);
		else if (r.seeks.count === 0 && r.durationMs >= th.failAfterMs) failed++;
	}
	const n = times.n + failed;
	if (n === 0) return noData(title);
	const failShare = failed / n;
	const p90 = quantile(times, MS_BOUNDS, 0.9);
	const steps: Milestone[] = ["recovering", "firstByte", "firstPiece", "ready1MiB", "ready4MiB", "ready16MiB"];
	const medians = Object.fromEntries(steps.map((m) => [m, quantile(milestoneHist(fresh, m), MS_BOUNDS, 0.5)]));
	return {
		title,
		status: grade(
			(p90 ?? 0) <= th.goodP90Ms && failShare <= th.goodFailShare,
			(p90 ?? Infinity) > th.poorP90Ms || failShare >= th.poorFailShare,
		),
		summary:
			(times.n
				? `4 MiB ready within ${sec(quantile(times, MS_BOUNDS, 0.5))} for half the videos, ${sec(p90)} for 90%`
				: "4 MiB never became ready") +
			(failed ? `; ${failed} of ${n} never got there in ${sec(th.failAfterMs)}+` : "") +
			`; typical path: first data ${sec(medians.firstByte)}, first verified piece ${sec(medians.firstPiece)}`,
		details: { videos: n, failed, p50Ms: quantile(times, MS_BOUNDS, 0.5), p90Ms: p90, medianMsByStep: medians },
	};
}

function throughput(recoveries: RecoveryRecord[]): PartHealth {
	const title = "Download speed";
	const windows = hist(RATE_BOUNDS);
	for (const r of recoveries) merge(windows, r.transfer.windows);
	if (windows.n === 0) return noData(title);
	const th = THRESHOLDS.throughput;
	const recovering = sum(recoveries, (r) => r.transfer.recoveringMs);
	const stallShare = recovering > 0 ? sum(recoveries, (r) => r.transfer.stallMs) / recovering : 0;
	const [medianLow, medianHigh] = bucket(windows, RATE_BOUNDS, 0.5);
	const [slowLow, slowHigh] = bucket(windows, RATE_BOUNDS, 0.1);
	const average = recovering > 0 ? (sum(recoveries, (r) => r.transfer.bytes) * 1000) / recovering : 0;
	return {
		title,
		status: grade(
			medianLow >= th.goodMedian && stallShare < th.goodStallShare,
			medianHigh <= th.poorMedian || stallShare >= th.poorStallShare,
		),
		summary:
			`average ${(average / MB).toFixed(1)} MB/s; per 10 s window: median ${rate(medianLow, medianHigh)}, ` +
			`slowest 10% ${rate(slowLow, slowHigh)}; ` +
			`no data at all for ${pct(stallShare)} of the recovering time ` +
			`(${sum(recoveries, (r) => r.transfer.stalls)} stalls of 5 s or more)`,
		details: {
			windows: windows.n,
			averageBytesPerSecond: Math.round(average),
			medianBytesPerSecond: [medianLow, medianHigh],
			p10BytesPerSecond: [slowLow, slowHigh],
			stallShare: round(stallShare),
			stalls: sum(recoveries, (r) => r.transfer.stalls),
			recoveringMinutes: Math.round(recovering / 60_000),
		},
	};
}

function seeking(recoveries: RecoveryRecord[]): PartHealth {
	const title = "Seeking (4 MiB ready after a seek)";
	const ready = hist(MS_BOUNDS);
	for (const r of recoveries) merge(ready, r.seeks.readyMs);
	const abandoned = sum(recoveries, (r) => r.seeks.abandoned);
	if (ready.n === 0) return noData(title);
	const th = THRESHOLDS.seeking;
	const p90 = quantile(ready, MS_BOUNDS, 0.9);
	return {
		title,
		status: grade((p90 ?? 0) <= th.goodP90Ms, (p90 ?? 0) > th.poorP90Ms),
		summary:
			`ready within ${sec(quantile(ready, MS_BOUNDS, 0.5))} for half the seeks, ${sec(p90)} for 90%` +
			(abandoned ? `; ${abandoned} seek(s) replaced by another before ready` : ""),
		details: { seeks: ready.n + abandoned, p50Ms: quantile(ready, MS_BOUNDS, 0.5), p90Ms: p90, abandoned },
	};
}

function playback(recoveries: RecoveryRecord[]): PartHealth {
	const title = "Playback (the player: start, waits for data, decoding)";
	const opened = recoveries
		.map((r) => r.playback)
		.filter((p): p is PlaybackRecord => !!p && (p.loadMs !== null || p.error !== null));
	if (opened.length === 0) return noData(title);
	const th = THRESHOLDS.playback;
	const failed = opened.filter((p) => p.error !== null);
	const starts = opened.map((p) => p.startMs).filter((ms): ms is number => ms !== null);
	const startP50 = percentile(starts, 0.5);
	const startP90 = percentile(starts, 0.9);
	const playedMs = sum(opened, (p) => p.playedMs);
	const waits = hist(MS_BOUNDS);
	const seeks = hist(MS_BOUNDS);
	for (const p of opened) {
		merge(waits, p.waits);
		merge(seeks, p.seeks);
	}
	// Waiting happens while "playing": it's part of the time played.
	const waitShare = playedMs > 0 ? Math.min(1, waits.sum / playedMs) : 0;
	const stutters = sum(opened, (p) => p.videoStutters + p.audioStutters);
	const perMin = playedMs >= 60_000 ? stutters / (playedMs / 60_000) : null;
	const failShare = failed.length / opened.length;
	const count = (key: (p: PlaybackRecord) => string | number | null) => {
		const out: Counts = {};
		for (const p of opened) {
			const k = key(p);
			if (k !== null) out[String(k)] = (out[String(k)] ?? 0) + 1;
		}
		return out;
	};
	const errors = count((p) => p.error);
	const modes = count((p) => p.mode);
	const top = topKey(errors);
	// Subtitle files fetched from the torrent when picked.
	const loads = { ok: 0, fail: 0, ms: hist(MS_BOUNDS), errors: {} as Counts };
	for (const p of opened) {
		if (!p.subtitleLoads) continue;
		loads.ok += p.subtitleLoads.ok;
		loads.fail += p.subtitleLoads.fail;
		merge(loads.ms, p.subtitleLoads.ms);
		mergeCounts(loads.errors, p.subtitleLoads.errors);
	}
	const changes = { audio: 0, subtitle: 0, external: 0 };
	for (const p of opened) {
		changes.audio += p.trackChanges?.audio ?? 0;
		changes.subtitle += p.trackChanges?.subtitle ?? 0;
		changes.external += p.trackChanges?.external ?? 0;
	}
	const offered = (key: "audio" | "subtitles" | "imageSubtitles" | "external", min = 1) =>
		opened.filter((p) => (p.tracks?.[key] ?? 0) >= min).length;
	return {
		title,
		status: grade(
			failShare === 0 &&
				loads.fail === 0 &&
				(startP90 ?? 0) <= th.goodStartP90Ms &&
				waitShare <= th.goodWaitShare &&
				(perMin ?? 0) <= th.goodStuttersPerMin,
			failShare > th.poorFailShare ||
				(startP90 ?? 0) > th.poorStartP90Ms ||
				waitShare > th.poorWaitShare ||
				(perMin ?? 0) > th.poorStuttersPerMin,
		),
		summary:
			`${opened.length} video(s) opened, ${starts.length} played` +
			(Object.keys(modes).length ? ` (${Object.entries(modes).map(([m, n]) => `${m}: ${n}`).join(", ")})` : "") +
			(startP50 !== null ? `; first picture within ${sec(startP50)} for half, ${sec(startP90)} for 90%` : "") +
			(playedMs > 0 ? `; waited for data ${pct(waitShare)} of ${Math.round(playedMs / 60_000)} min played` : "") +
			(perMin !== null ? `; ${perMin.toFixed(1)} decoder stutters/min` : "") +
			(failed.length ? `; ${failed.length} couldn't play (${top})` : "") +
			(loads.fail ? `; ${loads.fail} of ${loads.ok + loads.fail} subtitle files failed to load (${topKey(loads.errors)})` : ""),
		details: {
			opened: opened.length,
			played: starts.length,
			failed: failed.length,
			modes,
			videoCodecs: count((p) => p.videoCodec),
			audioCodecs: count((p) => p.audioCodec),
			heights: count((p) => p.height),
			loadP50Ms: percentile(
				opened.map((p) => p.loadMs).filter((ms): ms is number => ms !== null),
				0.5,
			),
			startP50Ms: startP50,
			startP90Ms: startP90,
			playedMinutes: round(playedMs / 60_000),
			waits: waits.n,
			waitShare: round(waitShare),
			seeks: seeks.n,
			seekP50Ms: quantile(seeks, MS_BOUNDS, 0.5),
			stuttersPerMin: perMin === null ? null : round(perMin),
			errors,
			tracks: {
				videosWithSeveralAudioTracks: offered("audio", 2),
				videosWithSubtitles: offered("subtitles"),
				videosWithImageSubtitles: offered("imageSubtitles"),
				videosWithSubtitleFiles: offered("external"),
				picked: changes,
			},
			subtitleFiles: {
				loaded: loads.ok,
				failed: loads.fail,
				p50Ms: quantile(loads.ms, MS_BOUNDS, 0.5),
				errors: loads.errors,
			},
		},
	};
}

function integrity(recoveries: RecoveryRecord[]): PartHealth {
	const title = "Data integrity (piece checks)";
	const bytes = sum(recoveries, (r) => r.transfer.bytes);
	const th = THRESHOLDS.integrity;
	if (bytes < th.minBytes) return noData(title);
	const failures = sum(recoveries, (r) => r.integrity.hashFailures);
	const perGiB = failures / (bytes / GiB);
	const banned = sum(recoveries, (r) => r.integrity.banned);
	return {
		title,
		status: grade(perGiB < th.goodPerGiB, perGiB >= th.poorPerGiB),
		summary: `${failures} failed piece check(s) in ${Math.round(bytes / MB)} MB (${perGiB.toFixed(2)} per GiB); ${banned} peer(s) banned`,
		details: { failedChecks: failures, perGiB: round(perGiB), bannedPeers: banned, downloadedMb: Math.round(bytes / MB) },
	};
}

function storage(recoveries: RecoveryRecord[], estimate: ReportInput["storage"]): PartHealth {
	const title = "Saving in the browser";
	const seen = recoveries.filter((r) => r.storage.kind !== null);
	if (seen.length === 0) return noData(title);
	const putErrors = sum(seen, (r) => r.storage.putErrors);
	const unsaved = seen.filter((r) => r.storage.kind === "memory").length;
	const nearlyFull = estimate !== null && estimate.quotaMb > 0 && estimate.usageMb / estimate.quotaMb > 0.9;
	return {
		title,
		status: putErrors > 0 ? "poor" : unsaved > 0 || nearlyFull ? "fair" : "good",
		summary:
			`${putErrors} piece(s) the browser refused to store; ${unsaved} of ${seen.length} video(s) couldn't be saved` +
			(estimate ? `; using ${estimate.usageMb} of ${estimate.quotaMb} MB available` : ""),
		details: { putErrors, unsavedVideos: unsaved, videos: seen.length, estimate },
	};
}

function app(visits: VisitRecord[]): PartHealth {
	const title = "Page errors";
	if (visits.length === 0) return noData(title);
	const errors = visits.flatMap((v) => v.errors);
	const total = errors.reduce((n, e) => n + e.count, 0);
	const byMessage: Counts = {};
	for (const e of errors) byMessage[`${e.where}: ${e.message}`] = (byMessage[`${e.where}: ${e.message}`] ?? 0) + e.count;
	const top = topKey(byMessage);
	const flow: Counts = {};
	for (const v of visits) mergeCounts(flow, v.flow, 60);
	return {
		title,
		status: grade(total === 0, total / visits.length >= THRESHOLDS.app.poorErrorsPerVisit),
		summary: total === 0 ? `no errors in ${visits.length} visit(s)` : `${total} error(s) in ${visits.length} visit(s); most common: ${top}`,
		details: { errors: total, visits: visits.length, byMessage, flow },
	};
}

function grade(good: boolean, poor: boolean): Status {
	return poor ? "poor" : good ? "good" : "fair";
}

function noData(title: string): PartHealth {
	return { title, status: "no data", summary: "not used yet", details: {} };
}

function mergeTimed(list: Timed[]): Timed {
	const out: Timed = { ok: 0, fail: 0, ms: hist(MS_BOUNDS), errors: {} };
	for (const t of list) {
		out.ok += t.ok;
		out.fail += t.fail;
		merge(out.ms, t.ms);
		mergeCounts(out.errors, t.errors);
	}
	return out;
}

function milestoneHist(recoveries: RecoveryRecord[], milestone: Milestone): Hist {
	const h = hist(MS_BOUNDS);
	for (const r of recoveries) {
		const at = r.milestones[milestone];
		if (at !== null) add(h, MS_BOUNDS, at);
	}
	return h;
}

/** The bucket (low, high] a share `q` of the values reach. */
function bucket(h: Hist, bounds: readonly number[], q: number): [number, number] {
	const high = quantile(h, bounds, q) ?? 0;
	const i = bounds.findIndex((b) => b >= high);
	return [i > 0 ? bounds[i - 1] : i === 0 ? 0 : bounds[bounds.length - 1], high];
}

function topKey(counts: Counts): string | null {
	let best: string | null = null;
	for (const [k, v] of Object.entries(counts)) if (best === null || v > counts[best]) best = k;
	return best;
}

function median(values: number[]): number | null {
	if (values.length === 0) return null;
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
}

/** Nearest-rank percentile. */
function percentile(values: number[], q: number): number | null {
	if (values.length === 0) return null;
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1))];
}

function sum<T>(items: T[], value: (item: T) => number): number {
	return items.reduce((n, item) => n + value(item), 0);
}

function round(n: number): number {
	return Math.round(n * 1000) / 1000;
}

function pct(share: number): string {
	return `${Math.round(share * 100)}%`;
}

function sec(ms: number | null | undefined): string {
	if (ms === null || ms === undefined) return "n/a";
	if (ms < 100) return `${Math.round(ms)} ms`;
	return ms < 10_000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms / 1000)} s`;
}

function rate(low: number, high: number): string {
	const mb = (b: number) => (b / MB >= 1 ? `${(b / MB).toFixed(b % MB ? 1 : 0)}` : (b / MB).toFixed(2));
	return low === 0 ? `under ${mb(high)} MB/s` : `${mb(low)}–${mb(high)} MB/s`;
}
