// The browser's own player: a plain <video> over a URL that serves the
// file with range requests (the streaming service worker). Chromium then
// buffers and seeks by itself, and each of its range reads reaches the
// file's `read`. Same controls, state and events as the libmedia player
// (player.ts); a file the browser turns out not to decode is handed back
// with `onUnsupported` before anything played, so the page can fall back.
//
// The first play waits for the container index (the browser read it) and
// about 10 s of video verified where it starts; when the browser runs out
// of data, the video is held paused until 5 s are verified from there
// (buffering.ts). Which files come here is decided from their codecs
// (codecs.ts `nativePlayable`, probe.ts).

import { averageBitrate, bufferReady, byteAt, waitForBuffer, type BufferView } from "./buffering";
import { extensionOf } from "./codecs";
import type { MediaPlayer, PlayerEvent, PlayerState } from "./player";
import type { ByteSource, SubtitleSource } from "./source";
import type { SubtitleTrack } from "./tracks";
import { nativeSubtitle, toWebVtt } from "./webvtt";

/** Containers Chromium plays itself (with codecs it decodes: checked by playing). */
const NATIVE_CONTAINERS: Record<string, string> = { mp4: "video/mp4", m4v: "video/mp4", webm: "video/webm" };

/** Whether a file of this name may play natively: MP4 or WebM the browser says it can play. */
export function nativeCandidate(name: string, canPlay: (type: string) => string = defaultCanPlay): boolean {
	const type = NATIVE_CONTAINERS[extensionOf(name)];
	return type !== undefined && canPlay(type) !== "";
}

function defaultCanPlay(type: string): string {
	try {
		return document.createElement("video").canPlayType(type);
	} catch {
		return "";
	}
}

export interface NativePlayerOptions {
	container: HTMLDivElement;
	/** Where the file is served with range support. */
	url: string;
	/**
	 * The same file's bytes: only its size and what is verified are asked
	 * (start and rebuffer thresholds), plus one read where playback starts
	 * (a hint for the recovery). Absent: no thresholds.
	 */
	source?: ByteSource;
	/** File name (its extension, for the diagnostics). */
	name: string;
	subtitles?: SubtitleSource[];
	onChange: (state: PlayerState) => void;
	onEvent?: (event: PlayerEvent) => void;
	/** The codecs the probe found (diagnostics). */
	codecs?: { video: string | null; audio: string | null };
	/** The browser can't play this file (nothing played yet): use another player. */
	onUnsupported?: (reason: string) => void;
}

/** MediaError codes. */
const MEDIA_ERR_DECODE = 3;
const MEDIA_ERR_SRC_NOT_SUPPORTED = 4;

export class NativePlayer implements MediaPlayer {
	private readonly video: HTMLVideoElement;
	private state: PlayerState;
	private destroyed = false;
	private readonly createdAt = performance.now();
	private playPressedAt: number | null = null;
	private started = false;
	private playingSince: number | null = null;
	private playedMs = 0;
	private waitingSince: number | null = null;
	private seekingSince: number | null = null;
	private resumeMs = 0;
	private size = 0;
	/** Resolves true once the browser read the container index, false if it can't. */
	private readonly metadata: Promise<boolean>;
	private metadataDone: (ok: boolean) => void = () => {};
	/** The start threshold was met (the first play may go). */
	private startHeld = false;
	private starting: Promise<boolean> | null = null;
	/** A rebuffer in progress: the video is paused until it is let go. */
	private rebuffering: AbortController | null = null;
	private lastSubtitle: string | null = null;
	/** Subtitle files as <track>s, once read. */
	private readonly tracks = new Map<string, Promise<HTMLTrackElement>>();
	private readonly blobUrls: string[] = [];
	private readonly abort = new AbortController();
	private readonly listeners: [string, () => void][] = [];
	private readonly onHidden = () => {
		if (document.visibilityState === "hidden") this.reportStats();
	};

	constructor(private readonly opts: NativePlayerOptions) {
		const files: SubtitleTrack[] = (opts.subtitles ?? []).map((source, i) => ({
			key: `x${i}`,
			label: source.label,
			detail: extensionOf(source.name).toUpperCase(),
			language: source.language,
			default: false,
			forced: false,
			supported: nativeSubtitle(extensionOf(source.name)),
			external: true,
		}));
		this.state = {
			status: "loading",
			currentMs: 0,
			durationMs: 0,
			waiting: false,
			needsGesture: false,
			volume: 1,
			muted: false,
			mode: null,
			videoCodec: null,
			audioCodec: null,
			height: null,
			audioTracks: [],
			audioId: -1,
			subtitleTracks: files,
			subtitle: null,
			subtitleLoading: null,
			subtitleError: null,
			subtitlesBlocked: false,
			error: null,
		};
		this.metadata = new Promise((resolve) => (this.metadataDone = resolve));
		void opts.source?.size(this.abort.signal).then(
			(size) => (this.size = size),
			() => {},
		);
		const video = document.createElement("video");
		video.playsInline = true;
		video.preload = "metadata";
		this.video = video;
		this.on("loadedmetadata", () => this.loaded());
		this.on("playing", () => {
			this.endWait();
			this.markStarted();
			if (this.state.status !== "playing") this.set({ status: "playing" });
		});
		this.on("waiting", () => {
			this.waitingSince ??= performance.now();
			this.set({ waiting: true });
			this.rebuffer();
		});
		this.on("canplay", () => this.endWait());
		this.on("timeupdate", () => this.set({ currentMs: video.currentTime * 1000 }));
		this.on("seeked", () => {
			if (this.seekingSince === null) return;
			this.emit({ type: "seeked", ms: performance.now() - this.seekingSince });
			this.seekingSince = null;
		});
		this.on("ended", () => {
			this.set({ status: "ended", currentMs: this.state.durationMs });
			this.reportStats();
		});
		this.on("error", () => this.failed());
		video.src = opts.url;
		opts.container.append(video);
		document.addEventListener("visibilitychange", this.onHidden);
		opts.onChange(this.state);
	}

	get current(): PlayerState {
		return this.state;
	}

	resumeAt(ms: number): void {
		if (this.playPressedAt !== null || !Number.isFinite(ms) || ms <= 0) return;
		this.resumeMs = ms;
		this.set({ currentMs: ms });
	}

	async play(): Promise<void> {
		if (this.destroyed || this.state.status === "error") return;
		this.playPressedAt ??= performance.now();
		const resume = this.resumeMs;
		this.resumeMs = 0;
		if (resume > 0) this.video.currentTime = resume / 1000;
		if (!this.startHeld && !(await this.holdStart())) return;
		// The hold took a while: the player may have closed or failed meanwhile.
		if (this.destroyed || (this.state as PlayerState).status === "error") return;
		try {
			await this.video.play();
			this.set({ status: "playing", needsGesture: false });
		} catch (e) {
			if (this.destroyed) return;
			// Sound blocked without a click: play muted and offer to turn it on.
			if (e instanceof DOMException && e.name === "NotAllowedError" && !this.video.muted) {
				this.video.muted = true;
				try {
					await this.video.play();
					this.set({ status: "playing", needsGesture: true });
					return;
				} catch {
					// Reported below.
				}
			}
			// A pause or a new source interrupted play(): not an error.
			if (e instanceof DOMException && e.name === "AbortError") return;
			if (this.video.error) return; // The error event handles it.
			this.fail("play", e);
		}
	}

	async pause(): Promise<void> {
		this.rebuffering?.abort();
		this.rebuffering = null;
		if (this.state.status !== "playing") return;
		this.video.pause();
		this.set({ status: "paused" });
		this.reportStats();
	}

	async resumeSound(): Promise<void> {
		this.video.muted = this.state.muted;
		this.set({ needsGesture: false });
	}

	async seek(ms: number): Promise<void> {
		if (this.destroyed || this.state.status === "loading" || this.state.status === "error") return;
		const target = Math.max(0, Math.min(ms, this.state.durationMs || ms));
		this.resumeMs = 0;
		this.seekingSince = performance.now();
		this.set({ currentMs: target });
		this.video.currentTime = target / 1000;
		if (this.state.status === "ended") await this.play();
	}

	setVolume(volume: number): void {
		const v = Math.max(0, Math.min(1, volume));
		this.video.volume = v;
		this.set({ volume: v });
	}

	setMuted(muted: boolean): void {
		this.video.muted = muted;
		this.set({ muted, needsGesture: muted ? this.state.needsGesture : false });
	}

	/** The browser plays the file's default audio track (Chromium offers no choice). */
	async selectAudio(): Promise<void> {}

	selectSubtitle(key: string | null): void {
		this.set({ subtitle: key, subtitleError: null });
		if (key !== null) this.emit({ type: "trackChanged", kind: "subtitle", external: true });
		void this.showSubtitle(key);
	}

	toggleSubtitles(): void {
		if (this.state.subtitle !== null) return this.selectSubtitle(null);
		const key = this.lastSubtitle ?? this.state.subtitleTracks.find((t) => t.supported)?.key;
		if (key) this.selectSubtitle(key);
	}

	async destroy(): Promise<void> {
		if (this.destroyed) return;
		this.reportStats();
		this.destroyed = true;
		this.abort.abort();
		this.metadataDone(false);
		document.removeEventListener("visibilitychange", this.onHidden);
		for (const [type, listener] of this.listeners) this.video.removeEventListener(type, listener);
		this.video.pause();
		// Drop the source: its range requests end, and so do their reads.
		this.video.removeAttribute("src");
		this.video.load();
		this.video.remove();
		for (const url of this.blobUrls) URL.revokeObjectURL(url);
	}

	private on(type: string, listener: () => void): void {
		this.video.addEventListener(type, listener);
		this.listeners.push([type, listener]);
	}

	/** What the thresholds are measured against: verified bytes, at the video's average bitrate. */
	private bufferView(): BufferView {
		const source = this.opts.source;
		const size = this.size;
		return {
			size,
			bitrate: averageBitrate(size, this.state.durationMs),
			available: (offset, limit) => (source?.available && size > 0 ? source.available(offset, limit) : Math.max(0, size - offset)),
		};
	}

	/** The file byte playing now (estimated from the time). */
	private positionByte(): number {
		return byteAt(this.video.currentTime * 1000, this.state.durationMs, this.size);
	}

	/**
	 * Before the first play: the container index read, and ~10 s verified
	 * where playback starts. A one-byte read there tells the recovery where
	 * that is (the browser reads nothing past the index before playing).
	 */
	private holdStart(): Promise<boolean> {
		this.starting ??= (async () => {
			try {
				if (!(await this.metadata) || this.destroyed) return false;
				const source = this.opts.source;
				if (source?.available) void source.read(this.positionByte(), new Uint8Array(1), this.abort.signal).catch(() => {});
				this.set({ waiting: true });
				const held = await waitForBuffer("start", () => this.bufferView(), () => this.positionByte(), this.abort.signal);
				this.startHeld = true;
				this.emit({ type: "buffered", ...held });
				return true;
			} catch {
				return false;
			} finally {
				this.starting = null;
				if (!this.destroyed && this.waitingSince === null) this.set({ waiting: false });
			}
		})();
		return this.starting;
	}

	/**
	 * The browser ran out of data while playing: unless ~5 s are verified
	 * from here already (it only has to fetch them), hold the video paused
	 * until they are, then play on. Status stays "playing" (the controls
	 * show it waiting); a pause by the viewer ends the hold.
	 */
	private rebuffer(): void {
		if (!this.started || this.rebuffering || this.destroyed || this.state.status !== "playing") return;
		if (bufferReady("resume", this.bufferView(), this.positionByte())) return;
		const hold = new AbortController();
		this.rebuffering = hold;
		this.video.pause();
		const signal = AbortSignal.any([hold.signal, this.abort.signal]);
		waitForBuffer("resume", () => this.bufferView(), () => this.positionByte(), signal).then(
			(held) => {
				if (this.rebuffering !== hold) return;
				this.rebuffering = null;
				this.emit({ type: "buffered", ...held });
				if (this.state.status === "playing") void this.video.play().catch(() => {});
			},
			() => {
				if (this.rebuffering === hold) this.rebuffering = null;
			},
		);
	}

	private loaded(): void {
		const video = this.video;
		const durationMs = Number.isFinite(video.duration) ? video.duration * 1000 : 0;
		this.set({
			status: this.state.status === "loading" ? "ready" : this.state.status,
			durationMs,
			height: video.videoHeight || null,
			mode: "native",
		});
		this.emit({
			type: "loaded",
			ms: performance.now() - this.createdAt,
			durationMs,
			mode: "native",
			videoCodec: this.opts.codecs?.video ?? null,
			audioCodec: this.opts.codecs?.audio ?? null,
			height: video.videoHeight || null,
		});
		this.set({ videoCodec: this.opts.codecs?.video ?? null, audioCodec: this.opts.codecs?.audio ?? null });
		this.metadataDone(true);
		const files = this.state.subtitleTracks.length;
		this.emit({ type: "tracks", audio: 0, subtitles: 0, imageSubtitles: 0, external: files });
		if (this.resumeMs > 0 && this.playPressedAt === null) video.currentTime = this.resumeMs / 1000;
	}

	private markStarted(): void {
		if (this.started || this.playPressedAt === null) return;
		this.started = true;
		this.emit({ type: "started", ms: performance.now() - this.playPressedAt, mode: "native" });
	}

	private endWait(): void {
		if (this.waitingSince === null) return;
		const ms = performance.now() - this.waitingSince;
		this.waitingSince = null;
		this.set({ waiting: false });
		if (this.state.status === "playing") this.emit({ type: "waited", ms });
	}

	/** Show one subtitle file (read into a <track> the first time), or none. */
	private async showSubtitle(key: string | null): Promise<void> {
		for (const track of this.video.textTracks) track.mode = "disabled";
		if (key === null) return;
		let element: HTMLTrackElement;
		try {
			element = await this.track(key);
		} catch (e) {
			if (this.destroyed || this.state.subtitle !== key) return;
			this.set({ subtitleError: `Couldn't load these subtitles: ${describe(e)}`, subtitle: this.lastSubtitle });
			return this.showSubtitle(this.lastSubtitle);
		}
		if (this.destroyed || this.state.subtitle !== key) return;
		element.track.mode = "showing";
		this.lastSubtitle = key;
	}

	private track(key: string): Promise<HTMLTrackElement> {
		let pending = this.tracks.get(key);
		if (!pending) {
			pending = this.loadTrack(key);
			this.tracks.set(key, pending);
			pending.catch(() => this.tracks.delete(key));
		}
		return pending;
	}

	private async loadTrack(key: string): Promise<HTMLTrackElement> {
		const source = this.opts.subtitles?.[Number(key.slice(1))];
		if (!source) throw new Error("Unknown subtitles");
		const startedAt = performance.now();
		this.set({ subtitleLoading: key });
		try {
			const text = toWebVtt(await source.load(this.abort.signal), extensionOf(source.name));
			if (text === null) throw new Error("this format can't be shown here");
			if (this.destroyed) throw new Error("Player closed");
			const url = URL.createObjectURL(new Blob([text], { type: "text/vtt" }));
			this.blobUrls.push(url);
			const element = document.createElement("track");
			element.kind = "subtitles";
			element.label = source.label;
			if (source.language) element.srclang = source.language;
			element.src = url;
			this.video.append(element);
			this.emit({ type: "subtitleLoad", ms: performance.now() - startedAt, ok: true });
			return element;
		} catch (e) {
			if (!this.destroyed) this.emit({ type: "subtitleLoad", ms: performance.now() - startedAt, ok: false, message: describe(e) });
			throw e;
		} finally {
			if (this.state.subtitleLoading === key) this.set({ subtitleLoading: null });
		}
	}

	private failed(): void {
		const error = this.video.error;
		if (!error || this.destroyed) return;
		this.metadataDone(false);
		const unsupported = error.code === MEDIA_ERR_SRC_NOT_SUPPORTED || error.code === MEDIA_ERR_DECODE;
		const reason = error.message || (unsupported ? "The browser can't decode this file" : "Playback failed");
		if (unsupported && !this.started && this.opts.onUnsupported) {
			this.opts.onUnsupported(reason);
			return;
		}
		this.fail(this.state.status === "loading" ? "load" : "play", new Error(reason));
	}

	private reportStats(): void {
		if (this.destroyed || this.playPressedAt === null) return;
		const playing = this.playingSince !== null ? performance.now() - this.playingSince : 0;
		let dropped = 0;
		try {
			dropped = this.video.getVideoPlaybackQuality().droppedVideoFrames;
		} catch {
			// No stats.
		}
		this.emit({ type: "stats", playedMs: this.playedMs + playing, videoStutters: dropped, audioStutters: 0 });
	}

	private fail(stage: "load" | "play", e: unknown): void {
		if (this.destroyed) return;
		const message = describe(e);
		this.set({ status: "error", error: message, waiting: false });
		this.emit({ type: "error", stage, message });
	}

	private set(patch: Partial<PlayerState>): void {
		if (this.destroyed) return;
		if (patch.status && patch.status !== this.state.status) {
			const now = performance.now();
			if (this.playingSince !== null) this.playedMs += now - this.playingSince;
			this.playingSince = patch.status === "playing" ? now : null;
		}
		this.state = { ...this.state, ...patch };
		this.opts.onChange(this.state);
	}

	private emit(event: PlayerEvent): void {
		try {
			this.opts.onEvent?.(event);
		} catch {
			// Diagnostics must never get in the way of playback.
		}
	}
}

function describe(e: unknown): string {
	const text = e instanceof Error ? e.message || e.name : typeof e === "string" ? e : "";
	return text.trim() || "Playback failed";
}
