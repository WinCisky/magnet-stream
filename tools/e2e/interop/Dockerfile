# The real BitTorrent clients of the uTP interop e2e (M4-04f,
# docs/decisions/utp.md › Interoperability), for machines that do not have
# them installed: libtorrent-rasterbar's Python bindings and
# transmission-daemon (its bundled libutp), the same Ubuntu 24.04 packages
# the CI job installs natively. Test-only: never part of a deployed image.
# The e2e runs it with --network host, confined to loopback.
#
#   docker build -t magnet-stream-interop tools/e2e/interop
FROM ubuntu:24.04@sha256:9c7f200c07e3d4a03e7249d2c16bf66a51120fb876aa2242cf31451412543289
RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3-libtorrent transmission-daemon \
	&& rm -rf /var/lib/apt/lists/*
