//! SSRF guard: tracker URLs are supplied by whoever calls our API (via the
//! magnet link), and the tunnel gateway dials addresses a browser names. Refuse
//! to connect to anything that isn't a public, routable unicast address.
//!
//! The same policy as `packages/tunnel-protocol/src/policy.ts` (which the
//! Cloudflare gateway enforces and the engine follows), so both gateways
//! refuse the same addresses: no private, loopback, link-local, CGNAT,
//! multicast, documentation or otherwise reserved ranges, for IPv4, IPv6 and
//! IPv4 embedded in IPv6 (mapped, NAT64, 6to4); in IPv6 only global unicast
//! (2000::/3).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// Lowest port a tunnel may dial (the protocol's rule).
pub const MIN_PORT: u16 = 1024;

pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

/// Whether a tunnel may dial `addr`: a public address (unless
/// `allow_private`, local tests only) on a port >= 1024 (always).
pub fn tunnel_may_dial(addr: &SocketAddr, allow_private: bool) -> bool {
    addr.port() >= MIN_PORT && (allow_private || is_public(addr.ip()))
}

/// `[network, prefix length]`
const V4_BLOCKED: &[(u32, u32)] = &[
    (0x0000_0000, 8),  // 0.0.0.0/8 "this network"
    (0x0a00_0000, 8),  // 10.0.0.0/8 private
    (0x6440_0000, 10), // 100.64.0.0/10 CGNAT
    (0x7f00_0000, 8),  // 127.0.0.0/8 loopback
    (0xa9fe_0000, 16), // 169.254.0.0/16 link-local
    (0xac10_0000, 12), // 172.16.0.0/12 private
    (0xc000_0000, 24), // 192.0.0.0/24 IETF protocol assignments
    (0xc000_0200, 24), // 192.0.2.0/24 TEST-NET-1
    (0xc058_6300, 24), // 192.88.99.0/24 6to4 relay anycast
    (0xc0a8_0000, 16), // 192.168.0.0/16 private
    (0xc612_0000, 15), // 198.18.0.0/15 benchmarking
    (0xc633_6400, 24), // 198.51.100.0/24 TEST-NET-2
    (0xcb00_7100, 24), // 203.0.113.0/24 TEST-NET-3
    (0xe000_0000, 4),  // 224.0.0.0/4 multicast
    (0xf000_0000, 4),  // 240.0.0.0/4 reserved + 255.255.255.255 broadcast
];

fn is_public_v4(v4: Ipv4Addr) -> bool {
    let n = u32::from(v4);
    !V4_BLOCKED
        .iter()
        .any(|&(base, bits)| n & (u32::MAX << (32 - bits)) == base)
}

/// `[prefix bytes, prefix length in bits]`
const V6_BLOCKED: &[(&[u8], u32)] = &[
    (&[0x01, 0x00], 64),                         // 100::/64 discard
    (&[0x20, 0x01, 0x00, 0x00], 32),             // 2001::/32 Teredo (embeds arbitrary IPv4)
    (&[0x20, 0x01, 0x00, 0x02, 0x00, 0x00], 48), // 2001:2::/48 benchmarking
    (&[0x20, 0x01, 0x0d, 0xb8], 32),             // 2001:db8::/32 documentation
    (&[0x20, 0x01, 0x00, 0x10], 28),             // 2001:10::/28 ORCHID
    (&[0x20, 0x01, 0x00, 0x20], 28),             // 2001:20::/28 ORCHIDv2
    (&[0x3f, 0xff], 20),                         // 3fff::/20 documentation
    (&[0x5f, 0x00], 16),                         // 5f00::/16 SRv6 SIDs
    (&[0xfc, 0x00], 7),                          // fc00::/7 unique local
    (&[0xfe, 0x80], 10),                         // fe80::/10 link-local
    (&[0xfe, 0xc0], 10),                         // fec0::/10 site-local (deprecated)
    (&[0xff, 0x00], 8),                          // ff00::/8 multicast
];

fn prefix_equals(ip: &[u8; 16], prefix: &[u8], bits: u32) -> bool {
    (0..bits as usize).all(|i| {
        let bit = 0x80u8 >> (i & 7);
        ip[i >> 3] & bit == prefix.get(i >> 3).copied().unwrap_or(0) & bit
    })
}

fn is_public_v6(v6: Ipv6Addr) -> bool {
    let ip = v6.octets();
    let zeros = |from: usize, to: usize| ip[from..to].iter().all(|&b| b == 0);
    let v4 = |at: usize| Ipv4Addr::new(ip[at], ip[at + 1], ip[at + 2], ip[at + 3]);
    // ::/96: unspecified, loopback, IPv4-compatible (deprecated): never a peer.
    if zeros(0, 12) {
        return false;
    }
    // ::ffff:0:0/96 IPv4-mapped: judge the IPv4 address.
    if zeros(0, 10) && ip[10] == 0xff && ip[11] == 0xff {
        return is_public_v4(v4(12));
    }
    // 64:ff9b::/96 NAT64: judge the embedded IPv4.
    if prefix_equals(&ip, &[0x00, 0x64, 0xff, 0x9b], 32) && zeros(4, 12) {
        return is_public_v4(v4(12));
    }
    // 64:ff9b:1::/48 local-use NAT64.
    if prefix_equals(&ip, &[0x00, 0x64, 0xff, 0x9b, 0x00, 0x01], 48) {
        return false;
    }
    // 2002::/16 6to4: judge the embedded IPv4.
    if ip[0] == 0x20 && ip[1] == 0x02 {
        return is_public_v4(v4(2));
    }
    if V6_BLOCKED
        .iter()
        .any(|&(prefix, bits)| prefix_equals(&ip, prefix, bits))
    {
        return false;
    }
    // Only global unicast (2000::/3) is reachable on the Internet.
    ip[0] & 0xe0 == 0x20
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn ip(s: &str) -> IpAddr {
        IpAddr::from_str(s).unwrap()
    }

    // The cases of apps/gateway/test/tunnel/address-policy.test.ts: both
    // gateways must agree.

    #[test]
    fn accepts_public_unicast() {
        for s in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "100.63.255.255",
            "100.128.0.1",
            "172.32.0.1",
            "2a00:1450:4001::1",
            "2606:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(is_public(ip(s)), "{s} should be public");
        }
    }

    #[test]
    fn rejects_private_and_reserved_v4() {
        for s in [
            "0.0.0.0",
            "0.1.2.3",
            "10.1.2.3",
            "100.64.0.1",
            "127.0.0.1",
            "127.255.255.254",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.9",
            "224.0.0.1",
            "239.255.255.250",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(!is_public(ip(s)), "{s} should not be public");
        }
    }

    #[test]
    fn rejects_special_v6() {
        for s in [
            "::",
            "::1",
            "::2",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "2001:db8::1",
            "2001::1",
            "100::1",
            "3fff::1",
            "64:ff9b:1::1",
            "4000::1",
        ] {
            assert!(!is_public(ip(s)), "{s} should not be public");
        }
    }

    #[test]
    fn judges_embedded_v4_by_the_v4_address() {
        assert!(!is_public(ip("::ffff:127.0.0.1")));
        assert!(!is_public(ip("::ffff:10.0.0.1")));
        assert!(!is_public(ip("::ffff:169.254.169.254")));
        assert!(is_public(ip("::ffff:8.8.8.8")));
        assert!(!is_public(ip("64:ff9b::192.168.0.1")));
        assert!(is_public(ip("64:ff9b::8.8.8.8")));
        assert!(!is_public(ip("2002:c0a8:0101::1"))); // 6to4 of 192.168.1.1
        assert!(is_public(ip("2002:0808:0808::1")));
    }

    #[test]
    fn tunnel_ports_below_1024_are_forbidden_even_with_private_allowed() {
        let a = |s: &str| SocketAddr::from_str(s).unwrap();
        assert!(!tunnel_may_dial(&a("8.8.8.8:80"), false));
        assert!(!tunnel_may_dial(&a("8.8.8.8:1023"), false));
        assert!(tunnel_may_dial(&a("8.8.8.8:1024"), false));
        assert!(!tunnel_may_dial(&a("127.0.0.1:22"), true));
        assert!(!tunnel_may_dial(&a("127.0.0.1:6881"), false));
        assert!(tunnel_may_dial(&a("127.0.0.1:6881"), true));
        assert!(tunnel_may_dial(&a("[::1]:6881"), true));
    }
}
