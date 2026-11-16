//! Minimal bencode decoder (plus a tiny encoder), just enough to read HTTP
//! tracker responses (BEP 3 / BEP 23), extension-protocol messages (BEP 10)
//! and torrent info dicts (BEP 9). We don't need the full generality of a
//! serde-based bencode crate — trackers and peers occasionally send
//! slightly-off data, so a small hand-rolled reader that's easy to reason
//! about beats fighting a derive macro.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BencodeError {
    #[error("unexpected end of bencode data")]
    UnexpectedEof,
    #[error("invalid bencode at byte offset {0}")]
    Invalid(usize),
    #[error("trailing data after top-level bencode value")]
    TrailingData,
    #[error("bencode nested too deeply")]
    TooDeep,
}

/// Peers are untrusted: without a cap, a message of nothing but `l`s would
/// recurse once per byte and overflow the stack.
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(Vec<(Vec<u8>, Value)>),
}

impl Value {
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&[(Vec<u8>, Value)]> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Dict(entries) => entries
                .iter()
                .find(|(k, _)| k == key.as_bytes())
                .map(|(_, v)| v),
            _ => None,
        }
    }
}

/// Parse a single top-level bencode value, rejecting trailing garbage.
pub fn parse(data: &[u8]) -> Result<Value, BencodeError> {
    let (value, consumed) = parse_prefix(data)?;
    if consumed != data.len() {
        return Err(BencodeError::TrailingData);
    }
    Ok(value)
}

/// Parse the bencode value at the start of `data` and return it together
/// with the number of bytes it took. Used for ut_metadata data messages
/// (BEP 9), where a raw metadata block follows the bencoded dict.
pub fn parse_prefix(data: &[u8]) -> Result<(Value, usize), BencodeError> {
    let mut pos = 0;
    let value = parse_value(data, &mut pos, 0)?;
    Ok((value, pos))
}

/// Encode `value`. Dict entries are written in the order given, so callers
/// must list keys sorted, as the spec requires.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(i) => out.extend_from_slice(format!("i{i}e").as_bytes()),
        Value::Bytes(b) => {
            out.extend_from_slice(format!("{}:", b.len()).as_bytes());
            out.extend_from_slice(b);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(entries) => {
            out.push(b'd');
            for (key, item) in entries {
                out.extend_from_slice(format!("{}:", key.len()).as_bytes());
                out.extend_from_slice(key);
                encode_into(item, out);
            }
            out.push(b'e');
        }
    }
}

fn parse_value(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value, BencodeError> {
    if depth > MAX_DEPTH {
        return Err(BencodeError::TooDeep);
    }
    match peek(data, *pos)? {
        b'i' => parse_int(data, pos),
        b'l' => parse_list(data, pos, depth),
        b'd' => parse_dict(data, pos, depth),
        b'0'..=b'9' => parse_bytes(data, pos).map(Value::Bytes),
        _ => Err(BencodeError::Invalid(*pos)),
    }
}

fn peek(data: &[u8], pos: usize) -> Result<u8, BencodeError> {
    data.get(pos).copied().ok_or(BencodeError::UnexpectedEof)
}

fn parse_int(data: &[u8], pos: &mut usize) -> Result<Value, BencodeError> {
    debug_assert_eq!(data[*pos], b'i');
    let start = *pos + 1;
    let end = find(data, start, b'e')?;
    let s = std::str::from_utf8(&data[start..end]).map_err(|_| BencodeError::Invalid(start))?;
    let n: i64 = s.parse().map_err(|_| BencodeError::Invalid(start))?;
    *pos = end + 1;
    Ok(Value::Int(n))
}

fn parse_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, BencodeError> {
    let colon = find(data, *pos, b':')?;
    let len_str =
        std::str::from_utf8(&data[*pos..colon]).map_err(|_| BencodeError::Invalid(*pos))?;
    let len: usize = len_str.parse().map_err(|_| BencodeError::Invalid(*pos))?;
    let start = colon + 1;
    let end = start.checked_add(len).ok_or(BencodeError::Invalid(*pos))?;
    if end > data.len() {
        return Err(BencodeError::UnexpectedEof);
    }
    *pos = end;
    Ok(data[start..end].to_vec())
}

fn parse_list(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value, BencodeError> {
    debug_assert_eq!(data[*pos], b'l');
    *pos += 1;
    let mut items = Vec::new();
    loop {
        if peek(data, *pos)? == b'e' {
            *pos += 1;
            return Ok(Value::List(items));
        }
        items.push(parse_value(data, pos, depth + 1)?);
    }
}

fn parse_dict(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value, BencodeError> {
    debug_assert_eq!(data[*pos], b'd');
    *pos += 1;
    let mut entries = Vec::new();
    loop {
        if peek(data, *pos)? == b'e' {
            *pos += 1;
            return Ok(Value::Dict(entries));
        }
        let key = parse_bytes(data, pos)?;
        let value = parse_value(data, pos, depth + 1)?;
        entries.push((key, value));
    }
}

fn find(data: &[u8], from: usize, needle: u8) -> Result<usize, BencodeError> {
    data[from..]
        .iter()
        .position(|&b| b == needle)
        .map(|i| from + i)
        .ok_or(BencodeError::UnexpectedEof)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_int() {
        assert_eq!(parse(b"i42e").unwrap(), Value::Int(42));
        assert_eq!(parse(b"i-7e").unwrap(), Value::Int(-7));
    }

    #[test]
    fn parses_bytes() {
        assert_eq!(parse(b"4:spam").unwrap(), Value::Bytes(b"spam".to_vec()));
        assert_eq!(parse(b"0:").unwrap(), Value::Bytes(vec![]));
    }

    #[test]
    fn parses_list() {
        assert_eq!(
            parse(b"l4:spam4:eggse").unwrap(),
            Value::List(vec![
                Value::Bytes(b"spam".to_vec()),
                Value::Bytes(b"eggs".to_vec())
            ])
        );
    }

    #[test]
    fn parses_dict_and_get() {
        let v = parse(b"d8:completei5e10:incompletei2e5:peers0:e").unwrap();
        assert_eq!(v.get("complete").and_then(Value::as_int), Some(5));
        assert_eq!(v.get("incomplete").and_then(Value::as_int), Some(2));
        assert_eq!(v.get("peers").and_then(Value::as_bytes), Some(&b""[..]));
        assert_eq!(v.get("missing"), None);
    }

    #[test]
    fn parses_binary_compact_peers() {
        // 2 compact IPv4 peers: 1.2.3.4:6881, 5.6.7.8:6882
        let mut peers = vec![1u8, 2, 3, 4, 0x1A, 0xE1];
        peers.extend_from_slice(&[5, 6, 7, 8, 0x1A, 0xE2]);
        let mut data = b"d5:peers12:".to_vec();
        data.extend_from_slice(&peers);
        data.push(b'e');
        let v = parse(&data).unwrap();
        assert_eq!(v.get("peers").and_then(Value::as_bytes), Some(&peers[..]));
    }

    #[test]
    fn rejects_trailing_data() {
        assert_eq!(parse(b"i1ee"), Err(BencodeError::TrailingData));
    }

    #[test]
    fn rejects_truncated_input() {
        assert_eq!(parse(b"d5:peers"), Err(BencodeError::UnexpectedEof));
        assert_eq!(parse(b"i42"), Err(BencodeError::UnexpectedEof));
    }

    #[test]
    fn rejects_deep_nesting_instead_of_overflowing_the_stack() {
        let mut data = vec![b'l'; 100_000];
        data.extend(std::iter::repeat_n(b'e', 100_000));
        assert_eq!(parse(&data), Err(BencodeError::TooDeep));
    }

    #[test]
    fn parse_prefix_reports_consumed_length() {
        let data = b"d8:msg_typei1e5:piecei0eeRAWBYTES";
        let (value, consumed) = parse_prefix(data).unwrap();
        assert_eq!(value.get("msg_type").and_then(Value::as_int), Some(1));
        assert_eq!(&data[consumed..], b"RAWBYTES");
    }

    #[test]
    fn encode_round_trips() {
        let value = Value::Dict(vec![
            (
                b"m".to_vec(),
                Value::Dict(vec![(b"ut_metadata".to_vec(), Value::Int(1))]),
            ),
            (b"v".to_vec(), Value::Bytes(b"x".to_vec())),
            (b"z".to_vec(), Value::List(vec![Value::Int(-3)])),
        ]);
        let encoded = encode(&value);
        assert_eq!(encoded, b"d1:md11:ut_metadatai1ee1:v1:x1:zli-3eee".to_vec());
        assert_eq!(parse(&encoded).unwrap(), value);
    }
}
