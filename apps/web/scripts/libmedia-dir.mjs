// Where npm installed @libmedia/avplayer: in a workspace it is usually
// hoisted to the monorepo root's node_modules, so look upwards from here.

import { existsSync } from "node:fs";

/** URL of the package directory (with a trailing slash). */
export function libmediaDir() {
	for (let dir = new URL("../", import.meta.url); ; dir = new URL("../", dir)) {
		const candidate = new URL("node_modules/@libmedia/avplayer/", dir);
		if (existsSync(new URL("package.json", candidate))) return candidate;
		if (dir.pathname === "/") throw new Error("@libmedia/avplayer is not installed (run npm ci)");
	}
}
