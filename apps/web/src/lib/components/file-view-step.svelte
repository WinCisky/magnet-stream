<script lang="ts">
	import { onMount, tick } from "svelte";
	import LoaderCircleIcon from "@lucide/svelte/icons/loader-circle";
	import HouseIcon from "@lucide/svelte/icons/house";
	import ListVideoIcon from "@lucide/svelte/icons/list-video";
	import SearchIcon from "@lucide/svelte/icons/search";
	import XIcon from "@lucide/svelte/icons/x";
	import DownloadIcon from "@lucide/svelte/icons/download";
	import { Button } from "$lib/components/ui/button/index.js";
	import { formatBytes } from "$lib/magnet/files";
	import type { TorrentFile } from "$lib/magnet/api";
	import type { Metainfo } from "@magnet-stream/engine/metainfo";
	import { RecoveryEngine, type EngineSnapshot } from "$lib/torrent/engine";
	import type { PageSession } from "$lib/torrent/page-session";
	import { recoverFile } from "$lib/torrent/fetch-file";
	import { recoveredRanges } from "$lib/torrent/ranges";
	import { subtitleFilesFor } from "$lib/magnet/subtitles";
	import type { SubtitleSource } from "$lib/player/source";
	import VideoPlayer from "$lib/player/video-player.svelte";
	import { nativeCandidate } from "$lib/player/native";
	import { averageBitrate } from "$lib/player/buffering";
	import type { PlayerEvent } from "$lib/player/player";
	import { downloadStream, serveFile, streamUrl, streamWorkerReady } from "$lib/stream/bridge";
	import { diagnostics } from "$lib/diagnostics/recorder";
	import { savedProgress, type SavedProgress } from "$lib/torrent/library";
	import { sessionMemory } from "$lib/torrent/memory-db";
	import { resumePoint } from "$lib/player/resume";
	import { mode } from "mode-watcher";
	import { LEGEND, palette } from "$lib/torrent/palette";
	import FileName from "./file-name.svelte";
	import FileTree from "./file-tree.svelte";
	import DownloadButton from "./download-button.svelte";
	import PieceMap from "./piece-map.svelte";
	import PageShell from "./page-shell.svelte";
	import PieceStrip from "./piece-strip.svelte";

	let {
		file,
		fileIndex,
		files,
		allFiles,
		torrentName,
		magnet,
		infoHash,
		session = null,
		knownMeta = null,
		seeders,
		onSelect,
		onHome,
		onSearch,
	}: {
		file: TorrentFile;
		fileIndex: number;
		/** Every video of the torrent, to switch to another. */
		files: { file: TorrentFile; index: number }[];
		/** Everything in the torrent (subtitle files included). */
		allFiles: TorrentFile[];
		torrentName: string;
		magnet: string;
		infoHash: string;
		session?: PageSession | null;
		knownMeta?: Metainfo | null;
		seeders: number;
		onSelect: (index: number) => void;
		onHome: () => void;
		/** Go look up another magnet (the home page). */
		onSearch: () => void;
	} = $props();

	const folders = $derived(file.path.split("/").slice(0, -1).join(" / "));
	const baseName = $derived(file.path.split("/").at(-1) ?? file.path);

	const colors = $derived(palette(mode.current !== "light"));
	/** The sticky timeline that moves the recovery front: hidden for now. */
	const SHOW_PIECE_STRIP = false;

	let picker: HTMLDialogElement | undefined = $state();
	let pickerOpen = $state(false);
	let saved = $state.raw(new Map<number, SavedProgress>());

	async function openPicker() {
		if (!picker) return;
		diagnostics.count("other_videos_opened");
		pickerOpen = true;
		picker.showModal();
		saved = await savedProgress(infoHash).catch(() => saved);
		await tick();
		picker.querySelector('[aria-current="true"]')?.scrollIntoView({ block: "center" });
	}

	function pick(index: number) {
		picker?.close();
		if (index !== fileIndex) onSelect(index);
	}

	let snapshot: EngineSnapshot | null = $state.raw(null);
	/** File byte the player last read (-1: none yet). */
	let playhead = $state(-1);
	let available: [number, number][] = $state.raw([]);

	const busy = $derived(
		!snapshot || snapshot.phase === "metadata" || snapshot.phase === "peers",
	);
	const percent = $derived(
		snapshot && snapshot.fileSize > 0 ? (100 * snapshot.verifiedBytes) / snapshot.fileSize : 0,
	);
	const contiguousPercent = $derived(
		snapshot && snapshot.fileSize > 0 ? (100 * snapshot.contiguousBytes) / snapshot.fileSize : 0,
	);

	function formatEta(seconds: number): string {
		const s = Math.round(seconds);
		const h = Math.floor(s / 3600);
		const m = Math.floor((s % 3600) / 60);
		const rest = String(s % 60).padStart(2, "0");
		return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${rest}` : `${m}:${rest}`;
	}

	/** `localStorage.setItem("mw-debug", "1")` logs every worker request. */
	function debugLogger(): ((line: string) => void) | undefined {
		try {
			return localStorage.getItem("mw-debug") === "1" ? (line) => console.debug(`[recovery] ${line}`) : undefined;
		} catch {
			return undefined;
		}
	}

	// `file`, `magnet` and `infoHash` are fixed for this component's
	// lifetime: the parent re-mounts it when they change. The engine exists
	// before the player mounts: the player reads the file from it.
	const { engine, recorder } = createEngine();
	const subtitles = subtitleSources();

	/** The torrent's subtitle files for this video, each recovered when picked. */
	function subtitleSources(): SubtitleSource[] {
		return subtitleFilesFor(file, allFiles, files.length).map((s) => ({
			name: s.file.path.split("/").at(-1) ?? s.file.path,
			label: s.label,
			language: s.language,
			load: (signal) => {
				if (!session || !knownMeta) return Promise.reject(new Error("No torrent session"));
				return recoverFile({ infoHash, magnet, file: s.file, session, meta: knownMeta, signal });
			},
		}));
	}

	function createEngine() {
		const recorder = diagnostics.recovery({ infoHash, path: file.path, fileSize: file.size, videos: files.length });
		const engine: RecoveryEngine = new RecoveryEngine({
			infoHash,
			magnet,
			file: { index: fileIndex, path: file.path, offset: file.offset, size: file.size },
			session: session ?? undefined,
			knownMeta: knownMeta ?? undefined,
			onSnapshot: (s) => {
				snapshot = s;
				playhead = engine.stream.lastRead;
				available = recoveredRanges(s);
				recorder.sample(s);
			},
			debug: debugLogger(),
			observer: recorder,
		});
		return { engine, recorder };
	}

	/** Playback position: kept in the session memory at most this often (M3-04). */
	const POSITION_SAVE_MS = 5_000;
	/** Where the first play starts: the position kept from last time. */
	let resumeMs = $state(0);
	let position = { ms: -1, durationMs: 0 };
	let positionSavedAt = 0;
	let positionSaved = -1;

	function onTime(ms: number, durationMs: number) {
		position = { ms, durationMs };
		if (Date.now() - positionSavedAt >= POSITION_SAVE_MS) savePosition();
	}

	function savePosition() {
		const { ms, durationMs } = position;
		if (ms < 0 || durationMs <= 0 || Math.abs(ms - positionSaved) < 1000) return;
		positionSavedAt = Date.now();
		positionSaved = ms;
		void sessionMemory()
			.then((memory) => memory?.rememberPosition(infoHash, fileIndex, ms, durationMs))
			.catch(() => {});
	}

	/**
	 * Where the streaming service worker serves this file (M3-05): the
	 * native player's source and the download link. Null: not served (no
	 * worker in control, as after a hard reload): libmedia plays, and the
	 * download reads the pieces itself.
	 */
	let served: string | null = $state(null);
	/** The player waits for that answer (a moment on the first visit). */
	let playerReady = $state(false);
	/** MP4 and WebM the browser plays itself go to the native <video>. */
	const nativeUrl = $derived(served && nativeCandidate(baseName) ? served : null);

	/**
	 * What the player reports goes to the diagnostics; once it has read
	 * the container, the video's average bitrate goes to the recovery (the
	 * picker's window in bytes, the threshold for another socket).
	 */
	function onPlayerEvent(event: PlayerEvent) {
		if (event.type === "loaded") {
			const bitrate = averageBitrate(file.size, event.durationMs);
			if (bitrate !== null) engine.setBitrate(bitrate);
		}
		recorder.playback(event);
	}

	function downloadBytes(): ReadableStream<Uint8Array> {
		const bytes = engine.fileBytes();
		if (!bytes) throw new Error("The video isn't fully recovered yet");
		return bytes;
	}

	onMount(() => {
		void engine.start();
		let unserve: (() => void) | undefined;
		let gone = false;
		void streamWorkerReady().then((ok) => {
			if (gone) return;
			if (ok) {
				// Served only while this page has the file open.
				unserve = serveFile(infoHash, fileIndex, engine.stream, baseName);
				served = streamUrl(infoHash, fileIndex);
			}
			playerReady = true;
		});
		void sessionMemory()
			.then((memory) => memory?.position(infoHash, fileIndex))
			.then((kept) => (resumeMs = resumePoint(kept)))
			.catch(() => {});
		// The tab may never come back: keep where playback is.
		const onHide = () => savePosition();
		window.addEventListener("pagehide", onHide);
		return () => {
			gone = true;
			unserve?.();
			window.removeEventListener("pagehide", onHide);
			savePosition();
			engine.stop();
			recorder.end();
		};
	});
</script>

<PageShell>
	{#snippet actions()}
		<Button variant="outline" size="icon-sm" onclick={onHome} aria-label="Home" title="Home">
			<HouseIcon />
		</Button>
		{#if files.length > 1}
			<Button variant="outline" size="sm" onclick={openPicker}>
				<ListVideoIcon />
				Other videos
				<span class="text-muted-foreground tabular-nums">{files.length}</span>
			</Button>
		{/if}
	{/snippet}
	{#snippet end()}
		{#if snapshot?.phase === "complete"}
			{#if served}
				<!-- The stream URL as an attachment: the browser saves it as it reads. -->
				<Button
					variant="outline"
					size="sm"
					href={`${served}?download=1`}
					onclick={(e: MouseEvent) => {
						// Through the service worker (a plain link click would leave the page).
						e.preventDefault();
						if (served) downloadStream(served);
						diagnostics.count("video_downloaded");
					}}
					aria-label={`Download ${baseName}`}
					title={`Download ${baseName}`}
					class="max-sm:w-8 max-sm:px-0"
				>
					<DownloadIcon />
					<span class="max-sm:sr-only">Download</span>
				</Button>
			{:else}
				<DownloadButton name={baseName} size={file.size} open={downloadBytes} />
			{/if}
		{/if}
		<Button
			variant="outline"
			size="icon-sm"
			onclick={() => {
				diagnostics.count("video_search_opened");
				onSearch();
			}}
			aria-label="Search another magnet"
			title="Search another magnet"
		>
			<SearchIcon />
		</Button>
	{/snippet}

	<!-- On wider screens the title gets some room from the top bar. -->
	<div class="text-center md:mt-6">
		{#if folders}
			<p class="text-sm text-muted-foreground"><FileName name={folders} /></p>
		{/if}
		<p class="text-lg font-medium"><FileName name={baseName} /></p>
		<p class="text-sm text-muted-foreground">
			{formatBytes(file.size)}
			{#if snapshot && snapshot.states.length > 0}
				· {snapshot.states.length} pieces of {formatBytes(snapshot.pieceLength)}
			{/if}
		</p>
	</div>

	{#if playerReady}
		<VideoPlayer
			source={engine.stream}
			url={nativeUrl}
			name={baseName}
			{available}
			{subtitles}
			startMs={resumeMs}
			{onTime}
			onEvent={onPlayerEvent}
		/>
	{:else}
		<div class="aspect-video w-full rounded-lg bg-black"></div>
	{/if}

	<div class="mx-auto flex w-full max-w-3xl flex-col gap-5">
		{#if busy}
			<div class="flex items-center justify-center gap-2 text-sm text-muted-foreground">
				<LoaderCircleIcon class="size-4 animate-spin" />
				<span>{snapshot?.message ?? "Starting"}…</span>
			</div>
		{:else if snapshot?.phase === "error"}
			<p class="text-center text-sm text-destructive">{snapshot.message}</p>
		{/if}

		{#if snapshot && snapshot.states.length > 0}
			<div class="flex flex-col gap-1 text-center">
				<p class="text-2xl font-semibold tabular-nums">
					{percent.toFixed(1)}%
					<span class="text-base font-normal text-muted-foreground">recovered</span>
				</p>
				<p class="text-sm text-muted-foreground tabular-nums">
					{snapshot.verifiedPieces} / {snapshot.states.length} pieces · {formatBytes(snapshot.verifiedBytes)} of
					{formatBytes(snapshot.fileSize)}
					{#if snapshot.phase === "complete"}
						· complete
					{:else if snapshot.phase === "recovering"}
						· {formatBytes(Math.round(snapshot.rate))}/s{#if snapshot.etaSeconds !== null}&nbsp;· ETA {formatEta(snapshot.etaSeconds)}{/if}
					{/if}
				</p>
				<p class="text-sm text-muted-foreground tabular-nums">
					Playable from the start: {formatBytes(snapshot.contiguousBytes)} ({contiguousPercent.toFixed(1)}%)
				</p>
			</div>

			<PieceMap {snapshot} />

			<ul class="flex flex-wrap justify-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
				{#each LEGEND as item (item.state)}
					<li class="flex items-center gap-1.5">
						<span class="inline-block size-2.5 rounded-[2px]" style:background-color={colors.pieces[item.state]}></span>
						{item.label}
					</li>
				{/each}
				<!-- These two are drawn only on the timeline. -->
				{#if SHOW_PIECE_STRIP}
					<li class="flex items-center gap-1.5">
						<span class="inline-block h-2.5 w-0.5" style:background-color={colors.cursor}></span>
						Recovery front
					</li>
				{/if}
				{#if SHOW_PIECE_STRIP && playhead >= 0}
					<li class="flex items-center gap-1.5">
						<span class="inline-block h-2.5 w-1 border border-black bg-white outline outline-foreground/20"></span>
						Playback reads here
					</li>
				{/if}
			</ul>

			<div class="flex flex-col gap-1 text-center text-xs text-muted-foreground tabular-nums">
				<p>
					Peers: {snapshot.peers.active} fetching ({snapshot.peers.connections} connections) · {snapshot.peers.reachable} reachable · {snapshot.peers.seeds}
					seeds · {snapshot.peers.known || seeders} known{#if snapshot.peers.backedOff > 0}&nbsp;· {snapshot.peers.backedOff}
						resting{/if}{#if snapshot.peers.banned > 0}&nbsp;· {snapshot.peers.banned} banned{/if}
				</p>
				{#if snapshot.windowBytes > 0}
					<p data-testid="playback-window" data-window-bytes={snapshot.windowBytes} data-bitrate={snapshot.bitrate}>
						Playback window {formatBytes(snapshot.windowBytes)}{#if snapshot.bitrate > 0}&nbsp;· video
							{((snapshot.bitrate * 8) / 1e6).toFixed(1)} Mbit/s{:else}&nbsp;· bitrate not known yet{/if}
					</p>
				{/if}
				<p>
					{snapshot.activeRequests} requests in flight · {snapshot.requests} worker requests{#if snapshot.hashFailures > 0}&nbsp;·
						{snapshot.hashFailures} failed checks{/if}{#if snapshot.storage === "memory" && !snapshot.storageError}&nbsp;· not saved{/if}
				</p>
				{#if snapshot.storageError}
					<p class="text-destructive" data-testid="storage-error">{snapshot.storageError}</p>
				{/if}
				{#if snapshot.swarmError}
					<p class="text-destructive">{snapshot.swarmError}</p>
				{/if}
			</div>
		{/if}
	</div>

	{#snippet footer()}
		{#if SHOW_PIECE_STRIP && snapshot && snapshot.states.length > 0}
			<div class="sticky bottom-0 w-full bg-background/80 border-t px-4 pt-2 pb-4 backdrop-blur">
				<div class="mx-auto flex max-w-3xl flex-col gap-1">
					<PieceStrip {snapshot} {playhead} onSeek={(byte) => engine.seek(byte)} />
					<div class="flex justify-between text-[11px] text-muted-foreground tabular-nums">
						<span>0</span>
						<span>Click to move the recovery front</span>
						<span>{formatBytes(snapshot.fileSize)}</span>
					</div>
				</div>
			</div>
		{/if}
	{/snippet}
</PageShell>

<dialog
	bind:this={picker}
	onclose={() => (pickerOpen = false)}
	onclick={(e) => e.target === picker && picker.close()}
	aria-label="Choose another video"
	class="bg-background text-foreground m-auto w-[min(42rem,calc(100vw-2rem))] max-w-none rounded-lg border p-0 backdrop:bg-black/50"
>
	<div class="flex max-h-[85dvh] flex-col">
		<div class="flex items-start gap-2 border-b p-4">
			<div class="min-w-0 flex-1">
				<h2 class="font-medium">Choose another video</h2>
				<p class="text-muted-foreground text-sm"><FileName name={torrentName} /></p>
			</div>
			<Button variant="ghost" size="icon-sm" aria-label="Close" onclick={() => picker?.close()}>
				<XIcon />
			</Button>
		</div>
		<div class="overflow-y-auto px-4 pb-4">
			{#if pickerOpen}
				<div class="pt-4">
					<FileTree {files} current={fileIndex} {saved} sticky onSelect={pick} />
				</div>
			{/if}
		</div>
	</div>
</dialog>
