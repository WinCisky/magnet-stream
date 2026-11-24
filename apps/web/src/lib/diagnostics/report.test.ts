import { describe, expect, it } from "vitest";
import { newPlayback, newRecovery, newSession, newVisit, type RecoveryRecord, type SessionRecord, type VisitRecord } from "./records";
import { buildReport } from "./report";
import { MS_BOUNDS, RATE_BOUNDS, add, hist } from "./stats";

const MiB = 1024 * 1024;
const ENV = { browser: "Chrome 140", os: "Android", mobile: true, cores: 8, memoryGb: 4, connection: "4g", downlinkMbps: 10 };

function recovery(tune: (r: RecoveryRecord) => void): RecoveryRecord {
	const r = newRecovery({ fileSize: 1_200_000_000, videos: 1, ext: "mkv" });
	r.torrent = "a1b2c3d4e5f6";
	r.traits.freshStart = true;
	r.storage.kind = "cache";
	tune(r);
	return r;
}

function visit(recoveries: RecoveryRecord[], tune: (v: VisitRecord) => void = () => {}): VisitRecord {
	const v = newVisit(ENV, "test");
	v.recoveries = recoveries;
	tune(v);
	return v;
}

/** A session where the lookup, the tunnel and the scout went well. */
function session(tune: (s: SessionRecord) => void = () => {}): SessionRecord {
	const s = newSession();
	s.lookup = "listed";
	s.lookupMs = 1_400;
	s.metadata = { source: "vps", ms: 1_200 };
	s.progress = { found: 60, tried: 9, connected: 4 };
	s.tunnel.dials.scout = { attempted: 6, succeeded: 5, failed: { timeout: 1 } };
	s.tunnel.dials.untried = { attempted: 3, succeeded: 0, failed: { timeout: 2, refused: 1 } };
	Object.assign(s.tunnel, { connected: 5, unchoked: 3, delivering: 3, seeds: 2, firstUnchokeMs: 1_500, sockets: 1, upgrades: 1, messages: 40 });
	s.scout = { state: "done", streams: 1, peers: 60, alive: 8, seeds: 3, dead: 40, unknown: 12, rejected: 0, info: "accepted", firstPeerMs: 200 };
	tune(s);
	return s;
}

function report(visits: VisitRecord[]) {
	return buildReport({
		visits,
		installId: "id",
		build: "test",
		environment: ENV,
		storage: { usageMb: 500, quotaMb: 10_000, persisted: false },
		now: Date.UTC(2026, 8, 29),
	});
}

/** A recovery where everything went well. */
function healthy(r: RecoveryRecord) {
	r.milestones = { ...r.milestones, recovering: 300, firstByte: 1_800, firstPiece: 2_000, ready4MiB: 3_500 };
	for (let i = 0; i < 30; i++) add(r.transfer.windows, RATE_BOUNDS, 4.5 * MiB);
	r.transfer.recoveringMs = 300_000;
	r.transfer.stallMs = 3_000;
	r.transfer.bytes = 1_000 * MiB;
	add(r.seeks.readyMs, MS_BOUNDS, 2_500);
}

describe("buildReport", () => {
	it("calls a healthy recovery good across the board", () => {
		const out = report([
			visit([recovery(healthy)], (v) => {
				v.metadataApi.ok = 2;
				add(v.metadataApi.ms, MS_BOUNDS, 1_500);
				v.sessions = [session()];
			}),
		]);
		for (const part of ["lookup", "tunnel", "scout", "startup", "throughput", "seeking", "integrity", "storage", "app"]) {
			expect([part, out.health[part].status]).toEqual([part, "good"]);
		}
		expect(out.weakSpots).toEqual([]);
		expect(out.overview).toMatchObject({ visits: 1, recoveries: 1, torrents: 1, downloadedMb: 1_000 });
	});

	it("puts the poor parts first among the weak spots, with a reason", () => {
		const slow = recovery((r) => {
			healthy(r);
			r.transfer.windows.b.fill(0);
			r.transfer.windows.n = 0;
			for (let i = 0; i < 30; i++) add(r.transfer.windows, RATE_BOUNDS, 1.5 * MiB);
		});
		const lonely = (s: SessionRecord) => {
			s.tunnel.connected = 0;
			s.tunnel.firstUnchokeMs = null;
			s.tunnel.dials.scout = { attempted: 0, succeeded: 0, failed: {} };
		};
		const out = report([visit([slow], (v) => (v.sessions = [session(lonely), session(lonely), session()]))]);
		expect(out.health.tunnel.status).toBe("poor");
		expect(out.health.tunnel.summary).toContain("33% of 3 sessions reached a peer");
		expect(out.health.tunnel.summary).toContain("most common dial failure: timeout");
		expect(out.health.throughput.status).toBe("fair");
		expect(out.health.throughput.summary).toContain("median 1–2 MB/s");
		expect(out.weakSpots.map((w) => w.part)).toEqual(["tunnel", "throughput"]);
		expect(out.workingWell.map((w) => w.part)).toContain("startup");
	});

	it("counts starts that never got going, and skips resumed ones", () => {
		const stuck = recovery((r) => {
			r.durationMs = 60_000;
		});
		const resumed = recovery((r) => {
			r.traits.freshStart = false;
			r.milestones.ready4MiB = 0;
		});
		const out = report([visit([stuck, resumed, recovery(healthy)])]);
		expect(out.health.startup.status).toBe("poor");
		expect(out.health.startup.details).toMatchObject({ videos: 2, failed: 1 });
	});

	it("flags page errors and storage trouble", () => {
		const unsaved = recovery((r) => {
			healthy(r);
			r.storage.kind = "memory";
		});
		const out = report([
			visit([unsaved], (v) => v.errors.push({ where: "uncaught", message: "TypeError: x is undefined", count: 3, firstAt: 0 })),
		]);
		expect(out.health.app.status).toBe("poor");
		expect(out.health.app.summary).toContain("uncaught: TypeError: x is undefined");
		expect(out.health.storage.status).toBe("fair");
	});

	it("exports nothing that names a torrent, file or peer", () => {
		const out = JSON.stringify(report([visit([recovery(healthy)], (v) => (v.sessions = [session()]))]));
		expect(out).not.toMatch(/magnet:\?xt|[0-9a-f]{40}|\d+\.\d+\.\d+\.\d+:\d+|\.mkv"/i);
	});

	it("reports the lookup, tunnel and scout sections, and none of the proof of concept's", () => {
		const failedOnce = session((s) => {
			s.retries = 1;
			s.failures = { exhausted: 1 };
			s.metadata = { source: "peer", ms: 9_000 };
			s.scout.info = "rejected";
		});
		const out = report([visit([recovery(healthy)], (v) => (v.sessions = [session(), failedOnce, session((s) => (s.metadata = { source: "cache", ms: 5 }))]))]);
		expect(Object.keys(out.health)).not.toEqual(expect.arrayContaining(["seedersCount"]));
		for (const gone of ["seedersCount", "infoDict", "swarm", "worker", "metadataApi"]) expect(out.health[gone]).toBeUndefined();
		expect(JSON.stringify(out.health)).not.toMatch(/magnet-worker|magnet-seeders|\/swarm|\/metadata/);
		expect(out.health.lookup.details).toMatchObject({ sources: { vps: 1, peer: 1, cache: 1 }, failures: { exhausted: 1 }, retries: 1, listedAfterRetry: 1 });
		expect(out.health.lookup.summary).toContain("file list from the VPS 1, peers 1, the cache 1");
		expect(out.health.tunnel.details).toMatchObject({
			sessions: 3,
			dials: { scout: { attempted: 18, succeeded: 15, failed: { timeout: 3 } }, untried: { attempted: 9, succeeded: 0, failed: { timeout: 6, refused: 3 } } },
			medianDelivering: 3,
		});
		expect(out.health.tunnel.summary).toContain("scout-alive 83% (15/18)");
		// A forged info line is a poor scout, however the rest went.
		expect(out.health.scout.status).toBe("poor");
		expect(out.health.scout.details).toMatchObject({ info: { accepted: 2, rejected: 1 }, medianAlive: 8, medianDead: 40, medianUnknown: 12 });
		expect(out.overview).toMatchObject({ sessions: 3 });
	});

	it("reports the bytes that went through each gateway (M4-08), including a session whose peers only came in through the VPS", () => {
		const mixed = session((s) => {
			s.tunnel.paths = { cloudflare: { downloaded: 3 * MiB, uploaded: 1_000, connections: 2 }, vps: { downloaded: MiB, uploaded: 0, connections: 1 } };
		});
		const inboundOnly = session((s) => {
			s.tunnel.dials.scout = { attempted: 0, succeeded: 0, failed: {} };
			s.tunnel.dials.untried = { attempted: 0, succeeded: 0, failed: {} };
			s.tunnel.paths = { cloudflare: { downloaded: 0, uploaded: 0, connections: 0 }, vps: { downloaded: 4 * MiB, uploaded: 0, connections: 1 } };
		});
		const older = session((s) => delete s.tunnel.paths);
		const out = report([visit([recovery(healthy)], (v) => (v.sessions = [mixed, inboundOnly, older]))]);
		expect(out.health.tunnel.details).toMatchObject({
			sessions: 3,
			paths: {
				cloudflare: { downloaded: 3 * MiB, uploaded: 1_000, connections: 2, sessions: 1 },
				vps: { downloaded: 5 * MiB, uploaded: 0, connections: 2, sessions: 2 },
				vpsShare: 0.625,
			},
		});
		expect(out.health.tunnel.summary).toContain("received 3.0 MB through Cloudflare, 5.0 MB through the VPS");
	});

	it("grades playback: quick starts are good, failures and long waits are not", () => {
		const watched = (tune: (p: NonNullable<RecoveryRecord["playback"]>) => void) =>
			recovery((r) => {
				healthy(r);
				const p = newPlayback();
				p.loadMs = 900;
				p.startMs = 1_800;
				p.mode = "decode";
				p.videoCodec = "hevc";
				p.audioCodec = "eac3";
				p.playedMs = 600_000;
				add(p.waits, MS_BOUNDS, 2_000);
				tune(p);
				r.playback = p;
			});
		const good = report([visit([watched(() => {}), recovery(healthy)])]);
		expect(good.health.playback.status).toBe("good");
		expect(good.health.playback.summary).toContain("1 video(s) opened, 1 played (decode: 1); first picture within 1.8 s");
		expect(good.health.playback.details).toMatchObject({ played: 1, videoCodecs: { hevc: 1 }, modes: { decode: 1 } });

		const waiting = report([
			visit([
				watched((p) => {
					for (let i = 0; i < 30; i++) add(p.waits, MS_BOUNDS, 3_000);
				}),
			]),
		]);
		expect(waiting.health.playback.status).toBe("poor");
		expect(waiting.health.playback.summary).toContain("waited for data 15% of 10 min played");

		const broken = report([
			visit([
				watched(() => {}),
				watched((p) => {
					p.startMs = null;
					p.error = "load: open stream failed";
				}),
			]),
		]);
		expect(broken.health.playback.status).toBe("poor");

		const subtitleTrouble = report([
			visit([
				watched((p) => {
					p.tracks = { audio: 2, subtitles: 0, imageSubtitles: 1, external: 4 };
					p.subtitleLoads = { ok: 1, fail: 1, ms: hist(MS_BOUNDS), errors: { "Timed out fetching the file": 1 } };
				}),
			]),
		]);
		expect(subtitleTrouble.health.playback.status).toBe("fair");
		expect(subtitleTrouble.health.playback.summary).toContain("1 of 2 subtitle files failed to load (Timed out fetching the file)");
		expect(subtitleTrouble.health.playback.details.tracks).toMatchObject({ videosWithSeveralAudioTracks: 1, videosWithImageSubtitles: 1, videosWithSubtitleFiles: 1 });
		expect(broken.health.playback.summary).toContain("1 couldn't play (load: open stream failed)");
	});
});
