// Stand-in for `cloudflare:sockets` under vitest (see vitest.config.ts).
export function connect(): never {
	throw new Error("no sockets in unit tests");
}
