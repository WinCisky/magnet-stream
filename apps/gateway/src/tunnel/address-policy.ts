// Which addresses a tunnel may connect to (SSRF guard): the shared policy
// of @magnet-stream/tunnel-protocol (public unicast IPv4/IPv6 only, ports
// >= 1024), re-exported here where the gateway enforces it.

export { MIN_PORT, checkAddress, isPublicIpv4, isPublicIpv6, type AddressVerdict } from "@magnet-stream/tunnel-protocol";
