<script lang="ts">
	import { onMount } from "svelte";
	import { readShare, writeShare } from "$lib/settings/share";

	// On by default (OQ-04); read once mounted (localStorage is the browser's).
	let on = $state(true);
	onMount(() => {
		on = readShare();
	});

	function change(event: Event) {
		on = (event.currentTarget as HTMLInputElement).checked;
		writeShare(on);
	}
</script>

<label
	class="flex h-8 cursor-pointer items-center gap-2 rounded-md border px-3 text-sm select-none"
	title="Upload the pieces you already have to the peers that send you pieces: they then send you more. Off: nothing is uploaded."
>
	<input type="checkbox" class="size-4 accent-current" checked={on} onchange={change} />
	Share with peers
</label>
