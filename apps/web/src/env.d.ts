interface ImportMetaEnv {
	/** magnet-seeders base URL (defaults to production). */
	readonly PUBLIC_SEEDERS_URL?: string;
	/** magnet-worker base URL (defaults to production). */
	readonly PUBLIC_WORKER_URL?: string;
	/** Hard cap on one file-list lookup, seconds (default 120). */
	readonly PUBLIC_LOOKUP_CAP_SECONDS?: string;
}

/** When this build was made (set in astro.config.mjs); diagnostics report it. */
declare const __MW_BUILD__: string | undefined;

/** Vite bundles the session worker into a blob so COEP does not block the script. */
declare module "*?worker&inline" {
	const worker: new () => Worker;
	export default worker;
}
