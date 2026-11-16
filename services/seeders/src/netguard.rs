//! SSRF guard: tracker URLs are supplied by whoever calls our API (via the
//! magnet link), and we fetch them from the server. Refuse to connect to
//! anything that isn't a public, routable address.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub fn is_public(ip: IpAddr) -> bool {
    !is_disallowed(ip)
}

fn is_disallowed(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_disallowed_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_disallowed_v4(mapped);
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || is_unique_local_v6(v6)
                || is_unicast_link_local_v6(v6)
        }
    }
}

fn is_disallowed_v4(v4: Ipv4Addr) -> bool {
    v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_multicast()
        || v4.is_broadcast()
        || v4.is_unspecified()
        || is_shared_cgnat(v4)
        || is_benchmarking(v4)
}

/// 100.64.0.0/10 (RFC 6598, Carrier-Grade NAT)
fn is_shared_cgnat(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000
}

/// 198.18.0.0/15 (RFC 2544, benchmarking)
fn is_benchmarking(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    o[0] == 198 && (o[1] == 18 || o[1] == 19)
}

/// fc00::/7 (RFC 4193, unique local addresses)
fn is_unique_local_v6(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xfe00) == 0xfc00
}

/// fe80::/10 (link-local unicast)
fn is_unicast_link_local_v6(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn ip(s: &str) -> IpAddr {
        IpAddr::from_str(s).unwrap()
    }

    #[test]
    fn rejects_private_and_local_v4() {
        for s in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "169.254.1.1",
            "0.0.0.0",
            "255.255.255.255",
            "224.0.0.1",
            "100.64.0.5",
            "198.18.0.1",
        ] {
            assert!(!is_public(ip(s)), "{s} should not be public");
        }
    }

    #[test]
    fn accepts_public_v4() {
        for s in ["8.8.8.8", "1.1.1.1", "93.184.216.34"] {
            assert!(is_public(ip(s)), "{s} should be public");
        }
    }

    #[test]
    fn rejects_private_and_local_v6() {
        for s in [
            "::1",
            "::",
            "fe80::1",
            "fc00::1",
            "ff02::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public(ip(s)), "{s} should not be public");
        }
    }

    #[test]
    fn accepts_public_v6() {
        assert!(is_public(ip("2001:4860:4860::8888"))); // Google public DNS
    }
}
