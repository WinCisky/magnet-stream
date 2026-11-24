interface ImportMetaEnv {
	/** magnet-seeders base URL (defaults to production). */
	readonly PUBLIC_SEEDERS_URL?: string;
	/** Cloudflare tunnel gateway base URL (defaults to production). */
	readonly PUBLIC_WORKER_URL?: string;
	/** VPS tunnel gateway base URL (defaults to PUBLIC_SEEDERS_URL; `off`: none, no escalation). */
	readonly PUBLIC_VPS_GATEWAY_URL?: string;
	/** Primary tunnel gateway: `cloudflare` (default) or `vps`; the other is the fallback. */
	readonly PUBLIC_DEFAULT_GATEWAY?: string;
	/** Escalation to the VPS gateway after this many seconds of few peers or a rate under the bitrate (default 90). */
	readonly PUBLIC_ESCALATION_AFTER_SECONDS?: string;
	/** The page says the rate is under the bitrate after this many seconds (default 30). */
	readonly PUBLIC_SLOW_NOTICE_SECONDS?: string;
	/** "Few peers": fewer than this many delivering (default 2). */
	readonly PUBLIC_ESCALATION_MIN_PEERS?: string;
	readonly PUBLIC_ESCALATION_WITHDRAW_SECONDS?: string;
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
