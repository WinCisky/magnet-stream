//! HTTP(S) tracker protocol (BEP 3, with the BEP 23 compact extension and
//! the BEP 7 IPv6 `peers6` extension).

use super::{AnnounceParams, TrackerError};
use crate::bencode::{self, Value};
use crate::netguard;
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

pub async fn announce(
    client: &reqwest::Client,
    url: &url::Url,
    params: &AnnounceParams,
    per_call_timeout: Duration,
) -> Result<Vec<SocketAddr>, TrackerError> {
    // The client's DNS resolver applies the SSRF guard to host names, but an
    // IP literal never goes through a resolver: check it here.
    let literal = match url.host() {
        Some(url::Host::Ipv4(ip)) => Some(std::net::IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => Some(std::net::IpAddr::V6(ip)),
        _ => None,
    };
    if let Some(ip) = literal {
        if !params.allow_private && !netguard::is_public(ip) {
            return Err(TrackerError::NoPublicAddress);
        }
    }
    let info_hash_enc = percent_encode(&params.info_hash, NON_ALPHANUMERIC).to_string();
    let peer_id_enc = percent_encode(&params.peer_id, NON_ALPHANUMERIC).to_string();
    let sep = if url.query().is_some() { '&' } else { '?' };

    let request_url = format!(
        "{base}{sep}info_hash={ih}&peer_id={pid}&port={port}&uploaded=0&downloaded=0&left=0\
         &compact=1&numwant={numwant}&key={key}&event=started",
        base = url,
        sep = sep,
        ih = info_hash_enc,
        pid = peer_id_enc,
        port = params.port,
        numwant = params.numwant,
        key = params.key,
    );

    let resp = client
        .get(&request_url)
        .timeout(per_call_timeout)
        .send()
        .await
        .map_err(|e| TrackerError::Io(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(TrackerError::Io(format!("http status {}", resp.status())));
    }

    let body = resp
        .bytes()
        .await
        .map_err(|e| TrackerError::Io(e.to_string()))?;

    let value = bencode::parse(&body).map_err(|e| TrackerError::Malformed(e.to_string()))?;

    if let Some(reason) = value.get("failure reason").and_then(Value::as_bytes) {
        return Err(TrackerError::TrackerFailure(
            String::from_utf8_lossy(reason).into_owned(),
        ));
    }

    let mut peers = Vec::new();
    match value.get("peers") {
        Some(Value::Bytes(b)) => peers.extend(parse_compact_v4(b)),
        Some(Value::List(list)) => peers.extend(parse_dict_peers(list)),
        _ => {}
    }
    if let Some(Value::Bytes(b)) = value.get("peers6") {
        peers.extend(parse_compact_v6(b));
    }

    Ok(peers)
}

fn parse_compact_v4(data: &[u8]) -> Vec<SocketAddr> {
    data.chunks_exact(6)
        .map(|c| {
            let ip = Ipv4Addr::new(c[0], c[1], c[2], c[3]);
            let port = u16::from_be_bytes([c[4], c[5]]);
            SocketAddr::from((ip, port))
        })
        .collect()
}

fn parse_compact_v6(data: &[u8]) -> Vec<SocketAddr> {
    data.chunks_exact(18)
        .map(|c| {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&c[0..16]);
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([c[16], c[17]]);
            SocketAddr::from((ip, port))
        })
        .collect()
}

fn parse_dict_peers(list: &[Value]) -> Vec<SocketAddr> {
    list.iter()
        .filter_map(|entry| {
            let ip_bytes = entry.get("ip")?.as_bytes()?;
            let ip_str = std::str::from_utf8(ip_bytes).ok()?;
            let ip: std::net::IpAddr = ip_str.parse().ok()?;
            let port = entry.get("port")?.as_int()? as u16;
            Some(SocketAddr::from((ip, port)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compact_v4() {
        let data = [1, 2, 3, 4, 0x1A, 0xE1];
        let peers = parse_compact_v4(&data);
        assert_eq!(peers[0].to_string(), "1.2.3.4:6881");
    }

    #[test]
    fn parses_compact_v6() {
        let mut data = [0u8; 18];
        data[15] = 1; // ::1
        data[16..18].copy_from_slice(&6881u16.to_be_bytes());
        let peers = parse_compact_v6(&data);
        assert_eq!(peers[0].ip().to_string(), "::1");
        assert_eq!(peers[0].port(), 6881);
    }

    #[test]
    fn parses_non_compact_dict_peers() {
        let body = b"d5:peersld2:ip7:1.2.3.44:porti6881eeee";
        let value = bencode::parse(body).unwrap();
        let list = value.get("peers").unwrap().as_list().unwrap();
        let peers = parse_dict_peers(list);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].to_string(), "1.2.3.4:6881");
    }
}
