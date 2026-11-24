// What diagnostics keep: one record per visit (page load) with any activity,
// holding one record per recovery (a video opened). Aggregates only, no
// per-request logs, and nothing that names a torrent, file or person.

import type { Environment } from "./env";
import { emptyPaths, type DialClassName, type DialStats, type EscalationDiag, type MetadataOrigin, type PathsDiag, type ScoutStreamStats } from "$lib/torrent/session-protocol";
import { MS_BOUNDS, RATE_BOUNDS, hist, type Counts, type Hist } from "./stats";

/** Requests to one service: outcomes, latency, and why the failures failed. */
export interface Timed {
	ok: number;
	fail: number;
	ms: Hist;
	errors: Counts;
}

export interface ErrorEntry {
	/** The part of the app ("uncaught", "library"…). */
	where: string;
	message: string;
	count: number;
	firstAt: number;
}

export interface VisitRecord {
	v: 1;
	/** Starts with the time, so ids sort by age. */
	id: string;
	build: string;
	startedAt: number;
	updatedAt: number;
	env: Environment;
	/** What the user did: "magnet_submitted", "file_picked"… */
	flow: Counts;
	/** Each magnet → file list attempt (a lookup or a retry; the key predates the session worker). */
	metadataApi: Timed;
	errors: ErrorEntry[];
	/** One per torrent session (a magnet pasted): lookup, tunnel and scout counters (absent in older records). */
	sessions?: SessionRecord[];
	recoveries: RecoveryRecord[];
	/** Recoveries past the per-visit cap, not kept. */
	droppedRecoveries: number;
}

export type Milestone =
	| "recovering"
	| "firstByte"
	| "firstPiece"
	| "ready1MiB"
	| "ready4MiB"
	| "ready16MiB"
	| "complete";

export interface RecoveryRecord {
	id: string;
	startedAt: number;
	durationMs: number;
	/**
	 * A hash of the info-hash salted with a secret that never leaves the
	 * browser: it groups one torrent's recoveries, and can't be looked up.
	 */
	torrent: string;
	traits: {
		/** Sizes rounded to two significant digits. */
		fileSize: number;
		torrentSize: number;
		pieceLength: number;
		files: number;
		videos: number;
		/** File extension ("mkv"). */
		ext: string;
		/** Share of the file saved before this session (0–1). */
		resumed: number;
		/** Nothing was saved at the start position: startup times count. */
		freshStart: boolean | null;
	};
	outcome: "running" | "complete" | "left" | "error";
	error: string | null;
	/** Ms from opening the video until each step (null: it never happened). */
	milestones: Record<Milestone, number | null>;
	transfer: {
		bytes: number;
		recoveringMs: number;
		/** Time with no data at all while recovering (after 5 s of silence). */
		stallMs: number;
		stalls: number;
		/** Download rate per 10 s window while recovering (bytes/s). */
		windows: Hist;
	};
	seeks: {
		count: number;
		/** From a seek until 4 MiB are ready past it. */
		readyMs: Hist;
		/** Seeks superseded by another before they were ready. */
		abandoned: number;
	};
	integrity: { hashFailures: number; banned: number };
	storage: { kind: "opfs" | "cache" | "memory" | null; putErrors: number };
	/** The swarm as the engine last saw it. */
	peers: { known: number; reachable: number; usable: number; active: number; backedOff: number; banned: number };
	/** The player (absent in records from before it existed). */
	playback?: PlaybackRecord;
	/**
	 * Ms the magnet → file list lookup took before this video was opened
	 * (absent in older records; null if the video was not opened from a lookup).
	 */
	lookupMs?: number | null;
	/**
	 * Tunnel billing counters charged to this recovery, and the most peers
	 * the session knew and saw deliver (absent in older records).
	 */
	tunnel?: { upgrades: number; messages: number; known: number; delivered: number };
}

export interface PlaybackRecord {
	/** "native": the browser's own player (MSE); "decode": WebCodecs or wasm decoders. */
	mode: "native" | "decode" | null;
	videoCodec: string | null;
	audioCodec: string | null;
	/** Picture height, as a common step (480, 720, 1080…). */
	height: number | null;
	/** From opening the video until the player had read its header. */
	loadMs: number | null;
	/** From the first press of play until the first picture (or sound). */
	startMs: number | null;
	playedMs: number;
	/** Each time playback waited for data, by how long. */
	waits: Hist;
	/** Seeks, by how long they took to resume. */
	seeks: Hist;
	/** Frames the decoders were late with (decoding too slow). */
	videoStutters: number;
	audioStutters: number;
	error: string | null;
	/** What the video offered: audio tracks, subtitles (text / image), subtitle files. */
	tracks?: { audio: number; subtitles: number; imageSubtitles: number; external: number };
	/** Tracks the viewer picked. */
	trackChanges?: { audio: number; subtitle: number; external: number };
	/** Subtitle files fetched and read. */
	subtitleLoads?: { ok: number; fail: number; ms: Hist; errors: Counts };
}

/**
 * One torrent session (docs/architecture.md › Session start, Connection
 * management): how the file list was found, what the tunnel dialled and
 * what the VPS scout said. Counts and reason names only.
 */
export interface SessionRecord {
	id: string;
	startedAt: number;
	/** "running": still looking; "listed": the file list arrived; "failed": the last lookup gave up; "left": closed while looking. */
	lookup: "running" | "listed" | "failed" | "left";
	/** Ms from the paste to the file list, retries included (null: not listed). */
	lookupMs: number | null;
	/** Lookups that gave up, by reason ("exhausted", "cap", "tunnel"). */
	failures: Counts;
	retries: number;
	/** What the lookup had tried when it ended (the most seen). */
	progress: { found: number; tried: number; connected: number };
	/** Where the verified info dict came from, and ms from the session start. */
	metadata: { source: MetadataOrigin; ms: number } | null;
	tunnel: {
		/** Dials by class (scout-alive, untried, known) and outcome. */
		dials: Record<DialClassName, DialStats>;
		/** The most peers at once: connected, unchoking us, delivering, seeds. */
		connected: number;
		unchoked: number;
		delivering: number;
		seeds: number;
		firstUnchokeMs: number | null;
		sockets: number;
		upgrades: number;
		messages: number;
		recycles: number;
		untestedHighWater: number;
		/**
		 * Reciprocation (M3-12): the setting as last seen, piece bytes sent,
		 * the most peers unchoked at once, upstream messages that carried only
		 * upload traffic. Absent in records from before it.
		 */
		upload?: { enabled: boolean; bytes: number; unchoked: number; messages: number };
		/**
		 * Swarms with few seeders (M4-06): the escalation to the VPS gateway
		 * as last seen, why, ms from the session start, WebSockets opened to
		 * it, inbound peers it handed over. Absent in records from before it.
		 */
		escalation?: EscalationDiag;
		/**
		 * Bytes by the gateway that carried them (M4-08): received and sent
		 * through the Cloudflare gateway and through the VPS gateway, and the
		 * connections on each. Absent in records from before it.
		 */
		paths?: PathsDiag;
	};
	/** The `/peers` stream. */
	scout: ScoutStreamStats;
}

export function newSession(now = Date.now()): SessionRecord {
	const dials = (): DialStats => ({ attempted: 0, succeeded: 0, failed: {} });
	return {
		id: newId(now),
		startedAt: now,
		lookup: "running",
		lookupMs: null,
		failures: {},
		retries: 0,
		progress: { found: 0, tried: 0, connected: 0 },
		metadata: null,
		tunnel: {
			dials: { scout: dials(), untried: dials(), known: dials() },
			connected: 0,
			unchoked: 0,
			delivering: 0,
			seeds: 0,
			firstUnchokeMs: null,
			sockets: 0,
			upgrades: 0,
			messages: 0,
			recycles: 0,
			untestedHighWater: 0,
			upload: { enabled: false, bytes: 0, unchoked: 0, messages: 0 },
			escalation: { state: "idle", reason: null, afterMs: null, upgrades: 0, accepted: 0 },
			paths: emptyPaths(),
		},
		scout: { state: "idle", streams: 0, peers: 0, alive: 0, seeds: 0, dead: 0, unknown: 0, rejected: 0, info: "none", firstPeerMs: null },
	};
}

export function newPlayback(): PlaybackRecord {
	return {
		mode: null,
		videoCodec: null,
		audioCodec: null,
		height: null,
		loadMs: null,
		startMs: null,
		playedMs: 0,
		waits: hist(MS_BOUNDS),
		seeks: hist(MS_BOUNDS),
		videoStutters: 0,
		audioStutters: 0,
		error: null,
		tracks: { audio: 0, subtitles: 0, imageSubtitles: 0, external: 0 },
		trackChanges: { audio: 0, subtitle: 0, external: 0 },
		subtitleLoads: { ok: 0, fail: 0, ms: hist(MS_BOUNDS), errors: {} },
	};
}

export function timed(): Timed {
	return { ok: 0, fail: 0, ms: hist(MS_BOUNDS), errors: {} };
}

export function newId(now = Date.now()): string {
	const random = Math.floor(Math.random() * 36 ** 6)
		.toString(36)
		.padStart(6, "0");
	return `${now.toString(36).padStart(9, "0")}-${random}`;
}

export function newVisit(env: Environment, build: string, now = Date.now()): VisitRecord {
	return {
		v: 1,
		id: newId(now),
		build,
		startedAt: now,
		updatedAt: now,
		env,
		flow: {},
		metadataApi: timed(),
		errors: [],
		sessions: [],
		recoveries: [],
		droppedRecoveries: 0,
	};
}

export function newRecovery(init: { fileSize: number; videos: number; ext: string }, now = Date.now()): RecoveryRecord {
	return {
		id: newId(now),
		startedAt: now,
		durationMs: 0,
		torrent: "",
		traits: {
			fileSize: init.fileSize,
			torrentSize: 0,
			pieceLength: 0,
			files: 0,
			videos: init.videos,
			ext: init.ext,
			resumed: 0,
			freshStart: null,
		},
		outcome: "running",
		error: null,
		milestones: {
			recovering: null,
			firstByte: null,
			firstPiece: null,
			ready1MiB: null,
			ready4MiB: null,
			ready16MiB: null,
			complete: null,
		},
		transfer: { bytes: 0, recoveringMs: 0, stallMs: 0, stalls: 0, windows: hist(RATE_BOUNDS) },
		seeks: { count: 0, readyMs: hist(MS_BOUNDS), abandoned: 0 },
		integrity: { hashFailures: 0, banned: 0 },
		storage: { kind: null, putErrors: 0 },
		peers: { known: 0, reachable: 0, usable: 0, active: 0, backedOff: 0, banned: 0 },
	};
}
