// The "Share with peers" setting (decision OQ-04, M3-12): the engine
// uploads verified pieces to peers that upload to it, unless the user turns
// it off. On by default; kept in localStorage. What is read back is
// untrusted: only the two exact values count, anything else (missing,
// garbled, storage unavailable) is the documented default, on.

export const SHARE_KEY = "mw-share-with-peers";
export const SHARE_DEFAULT = true;

type Storage = Pick<globalThis.Storage, "getItem" | "setItem">;

/** The stored value, validated: "on" / "off", else the default. */
export function parseShare(raw: unknown): boolean {
	if (raw === "off") return false;
	if (raw === "on") return true;
	return SHARE_DEFAULT;
}

function storage(): Storage | undefined {
	try {
		return typeof localStorage === "undefined" ? undefined : localStorage;
	} catch {
		return undefined;
	}
}

const listeners = new Set<(on: boolean) => void>();

export function readShare(store: Storage | undefined = storage()): boolean {
	try {
		return parseShare(store?.getItem(SHARE_KEY));
	} catch {
		return SHARE_DEFAULT;
	}
}

/** Keep the setting and tell the open sessions. */
export function writeShare(on: boolean, store: Storage | undefined = storage()): void {
	try {
		store?.setItem(SHARE_KEY, on ? "on" : "off");
	} catch {
		// Storage refused: the change holds for this page only.
	}
	for (const listener of [...listeners]) listener(on);
}

/** Called on every change; returns the unsubscribe. */
export function onShareChange(listener: (on: boolean) => void): () => void {
	listeners.add(listener);
	return () => listeners.delete(listener);
}
