#!/usr/bin/env python3
"""A libtorrent-rasterbar peer that speaks uTP only (M4-04f, M4-05 interop tests).

    libtorrent-seed.py <file.torrent> <save dir> <listen ip:port> [--leech] [--download-limit B/s]

Seeds the torrent from <save dir> on <listen ip:port>, uTP in and out,
TCP off; no DHT, LSD, UPnP or NAT-PMP, so it reaches only what the
torrent's own tracker (the e2e's fake tracker, if any) names. With
--leech it starts from an empty <save dir> and downloads instead ("ready"
once it listens), at most --download-limit bytes per second (the hole
punching test keeps such a leecher connected to the seed, as a relay). Run in a
container on an internal Docker network (tools/e2e/src/utp-interop-session.ts). Prints one
JSON line per event of interest on stdout (version, ready, peer connects
with their transport, stats) until stdin closes or it is killed.
"""

import json
import os
import select
import sys
import time

import libtorrent as lt


# peer_info flags (libtorrent/peer_info.hpp): the 2.0 bindings name
# local_connection (we dialled) but not utp_socket (17_bit).
UTP_SOCKET = int(getattr(lt.peer_info, "utp_socket", 1 << 17))
OUTGOING = int(lt.peer_info.local_connection)


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def main():
    torrent, save, listen = sys.argv[1], sys.argv[2], sys.argv[3]
    extra = sys.argv[4:]
    leech = "--leech" in extra
    download_limit = int(extra[extra.index("--download-limit") + 1]) if "--download-limit" in extra else 0
    settings = {
        "listen_interfaces": listen,
        "enable_outgoing_tcp": False,
        "enable_incoming_tcp": False,
        "enable_outgoing_utp": True,
        "enable_incoming_utp": True,
        "enable_dht": False,
        "enable_lsd": False,
        "enable_upnp": False,
        "enable_natpmp": False,
        # Loopback peers: no per-IP dedup, no local-network rate class surprises.
        "allow_multiple_connections_per_ip": True,
        "download_rate_limit": download_limit,
        # Seeds re-announce quickly so a late tracker entry is picked up.
        "min_announce_interval": 1,
        "alert_mask": lt.alert.category_t.status_notification
        | lt.alert.category_t.error_notification
        | lt.alert.category_t.peer_notification
        | lt.alert.category_t.connect_notification
        | lt.alert.category_t.tracker_notification,
    }
    if os.environ.get("INTEROP_PEER_LOG"):
        settings["alert_mask"] |= lt.alert.category_t.peer_log_notification
    ses = lt.session(settings)
    emit("version", libtorrent=lt.__version__)
    info = lt.torrent_info(torrent)
    params = lt.add_torrent_params()
    params.ti = info
    params.save_path = save
    handle = ses.add_torrent(params)
    if not leech:
        handle.force_recheck()
    if download_limit:
        # The session-wide limit spares local-network peers (libtorrent's
        # local peer class); the torrent's own limit does not.
        handle.set_download_limit(download_limit)
    ready = False
    listening = False
    connected = set()
    last_stats = 0.0
    while True:
        if select.select([sys.stdin], [], [], 0)[0] and not os.read(sys.stdin.fileno(), 1024):
            break
        ses.wait_for_alert(200)
        for a in ses.pop_alerts():
            kind = type(a).__name__
            if kind in ("peer_connect_alert", "incoming_connection_alert", "peer_disconnected_alert", "peer_error_alert"):
                emit(kind, message=a.message())
            elif kind in ("tracker_error_alert", "listen_failed_alert", "torrent_error_alert", "file_error_alert"):
                emit(kind, message=a.message())
            elif kind == "listen_succeeded_alert":
                listening = True
                emit(kind, message=a.message())
            elif kind == "peer_log_alert":
                emit(kind, message=a.message())
        status = handle.status()
        if leech and not ready and listening:
            ready = True
            emit("ready", state=str(status.state))
        if not leech and not ready and status.is_seeding:
            ready = True
            emit("ready", state=str(status.state))
            handle.force_reannounce()
        for p in handle.get_peer_info():
            key = (int(p.flags) & UTP_SOCKET != 0, int(p.flags) & OUTGOING != 0)
            if key not in connected:
                connected.add(key)
                emit("peer", utp=key[0], outgoing=key[1])
        now = time.monotonic()
        if now - last_stats > 1:
            last_stats = now
            emit("stats", uploaded=status.total_upload, peers=status.num_peers, seeding=status.is_seeding, progress=round(status.progress, 4), state=str(status.state))


if __name__ == "__main__":
    main()
