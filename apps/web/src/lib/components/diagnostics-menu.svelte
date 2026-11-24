<script lang="ts">
	import ActivityIcon from "@lucide/svelte/icons/activity";
	import CopyIcon from "@lucide/svelte/icons/copy";
	import DownloadIcon from "@lucide/svelte/icons/download";
	import TrashIcon from "@lucide/svelte/icons/trash";
	import { Button } from "$lib/components/ui/button/index.js";
	import * as DropdownMenu from "$lib/components/ui/dropdown-menu/index.js";
	import { diagnostics } from "$lib/diagnostics/recorder";
	import type { PhaseMetrics } from "$lib/diagnostics/phase";
	import type { PathsDiag } from "$lib/torrent/session-protocol";

	let visits: number | null = $state(null);
	let metrics: PhaseMetrics | null = $state(null);
	let lastRow: string | null = $state(null);
	let upload: { enabled: boolean; bytes: number; unchoked: number; messages: number } | null = $state(null);
	let paths: PathsDiag | null = $state(null);
	let copied = $state(false);
	let busy = $state(false);
	let error: string | null = $state(null);

	async function refresh(open: boolean) {
		if (!open) return;
		error = null;
		copied = false;
		visits = await diagnostics.visits();
		const phase = await diagnostics.phase().catch(() => null);
		metrics = phase?.metrics ?? null;
		lastRow = phase?.lastRow ?? null;
		upload = await diagnostics.lastUpload().catch(() => null);
		paths = await diagnostics.lastPaths().catch(() => null);
	}

	const mb = (bytes: number) => (bytes / 1e6).toFixed(1);

	async function copyRow() {
		if (!lastRow) return;
		try {
			await navigator.clipboard.writeText(lastRow);
			copied = true;
		} catch {
			error = "Couldn't copy to the clipboard";
		}
	}

	const show = (value: number | null | undefined, digits = 1) => (value === null || value === undefined ? "n/a" : value.toFixed(digits));

	async function exportNow() {
		busy = true;
		error = null;
		try {
			const json = await diagnostics.exportJson();
			const url = URL.createObjectURL(new Blob([json], { type: "application/json" }));
			const link = document.createElement("a");
			link.href = url;
			link.download = `magnet-watcher-diagnostics-${new Date().toISOString().slice(0, 10)}.json`;
			link.click();
			setTimeout(() => URL.revokeObjectURL(url), 10_000);
			diagnostics.count("diagnostics_exported");
		} catch (e) {
			error = e instanceof Error ? e.message : "Export failed";
		} finally {
			busy = false;
		}
	}

	async function clear() {
		if (!confirm("Delete all diagnostics recorded in this browser?")) return;
		await diagnostics.clear();
		visits = 0;
	}
</script>

<DropdownMenu.Root onOpenChange={refresh}>
	<DropdownMenu.Trigger>
		{#snippet child({ props })}
			<Button {...props} variant="outline" size="sm" disabled={busy}>
				<ActivityIcon />
				Diagnostics
			</Button>
		{/snippet}
	</DropdownMenu.Trigger>
	<DropdownMenu.Content align="start" class="w-72">
		<DropdownMenu.Label class="flex flex-col gap-1 font-normal">
			<span>
				Anonymous measurements of how each part of the app performs for you. They stay in this browser until you
				export them.
			</span>
			<span class="tabular-nums">
				{visits === null ? "…" : `${visits} visit${visits === 1 ? "" : "s"} recorded`}
			</span>
			{#if metrics && metrics.recoveries > 0}
				<span class="mt-1 font-medium">Phase measurements</span>
				<span class="grid grid-cols-[1fr_auto] gap-x-3 tabular-nums" data-testid="phase-metrics">
					<span>First verified piece</span>
					<span data-metric="first-piece-s">{show(metrics.firstPieceSeconds.median)} s</span>
					<span>Sustained speed</span>
					<span data-metric="mbps">{show(metrics.sustainedMBps)} MB/s</span>
					<span>Billed requests per GB</span>
					<span data-metric="requests-per-gb" class:text-destructive={metrics.withinBudget === false}>
						{show(metrics.billedRequestsPerGB)} / {metrics.budgetPerGB}
					</span>
				</span>
			{/if}
			{#if upload}
				<span class="mt-1 font-medium">Sharing with peers (last session)</span>
				<span class="grid grid-cols-[1fr_auto] gap-x-3 tabular-nums" data-testid="upload-metrics">
					<span>Uploaded</span>
					<span data-metric="upload-mb" data-bytes={upload.bytes}>{(upload.bytes / 1e6).toFixed(1)} MB</span>
					<span>Peers unchoked by us (most at once)</span>
					<span data-metric="upload-unchoked">{upload.unchoked}</span>
					<span>Messages sent only for the upload</span>
					<span data-metric="upload-messages">{upload.messages}</span>
					<span>Setting</span>
					<span data-metric="upload-enabled">{upload.enabled ? "on" : "off"}</span>
				</span>
			{/if}
			{#if paths}
				<span class="mt-1 font-medium">Data path (last session)</span>
				<span class="grid grid-cols-[1fr_auto] gap-x-3 tabular-nums" data-testid="path-metrics">
					<span>Received through Cloudflare</span>
					<span data-metric="path-cloudflare-mb" data-bytes={paths.cloudflare.downloaded}>{mb(paths.cloudflare.downloaded)} MB</span>
					<span>Received through the VPS</span>
					<span data-metric="path-vps-mb" data-bytes={paths.vps.downloaded}>{mb(paths.vps.downloaded)} MB</span>
				</span>
			{/if}
			{#if error}
				<span class="text-destructive">{error}</span>
			{/if}
		</DropdownMenu.Label>
		<DropdownMenu.Separator />
		<DropdownMenu.Item onclick={exportNow}>
			<DownloadIcon />
			Export as JSON
		</DropdownMenu.Item>
		<DropdownMenu.Item disabled={!lastRow} onclick={copyRow}>
			<CopyIcon />
			{copied ? "Copied" : "Copy measurement row (last video)"}
		</DropdownMenu.Item>
		<DropdownMenu.Item variant="destructive" disabled={!visits} onclick={clear}>
			<TrashIcon />
			Delete diagnostics
		</DropdownMenu.Item>
	</DropdownMenu.Content>
</DropdownMenu.Root>
