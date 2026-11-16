//! A torrent's file listing, read from its verified info dict: what `/files`
//! serves so the browser can list a magnet's files before one is chosen.
//!
//! The shape and order are those of the metadata API magnet-watcher used
//! before (its file indices are positions in this list, saved in browsers):
//! largest file first, same-size files in torrent order, paths relative to
//! the torrent's root. Padding files (BEP 47) are left out, though their
//! bytes still count in the offsets.

use crate::bencode::{self, Value};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ListingError {
    #[error("not a valid info dict")]
    Invalid,
    #[error("torrents with only a BitTorrent v2 file tree aren't supported")]
    V2Only,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    /// Byte offset of the file within the torrent's content.
    pub offset: u64,
}

#[derive(Debug, Serialize)]
pub struct Listing {
    pub info_hash: String,
    pub name: String,
    /// Total size of the listed files.
    pub size: u64,
    pub piece_length: u64,
    pub files: Vec<FileEntry>,
}

pub fn listing(info_hash: &str, info: &[u8]) -> Result<Listing, ListingError> {
    let dict = bencode::parse(info).map_err(|_| ListingError::Invalid)?;
    let name = text(&dict, "name").ok_or(ListingError::Invalid)?;
    let piece_length = dict
        .get("piece length")
        .and_then(Value::as_int)
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(ListingError::Invalid)?;

    let mut files = Vec::new();
    if let Some(list) = dict.get("files").and_then(Value::as_list) {
        let mut offset = 0u64;
        for entry in list {
            let size = length(entry)?;
            let parts = entry
                .get("path.utf-8")
                .or_else(|| entry.get("path"))
                .and_then(Value::as_list)
                .ok_or(ListingError::Invalid)?;
            let path = parts
                .iter()
                .map(|p| {
                    p.as_bytes()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                })
                .collect::<Option<Vec<_>>>()
                .ok_or(ListingError::Invalid)?
                .join("/");
            if !is_padding(entry, &path) {
                files.push(FileEntry { path, size, offset });
            }
            offset = offset.checked_add(size).ok_or(ListingError::Invalid)?;
        }
    } else if dict.get("length").is_some() {
        files.push(FileEntry {
            path: name.clone(),
            size: length(&dict)?,
            offset: 0,
        });
    } else if dict.get("file tree").is_some() {
        return Err(ListingError::V2Only);
    } else {
        return Err(ListingError::Invalid);
    }

    // Stable: files of the same size keep their order in the torrent.
    files.sort_by_key(|f| std::cmp::Reverse(f.size));
    let size = files.iter().map(|f| f.size).sum();
    Ok(Listing {
        info_hash: info_hash.to_string(),
        name,
        size,
        piece_length,
        files,
    })
}

fn text(dict: &Value, key: &str) -> Option<String> {
    let bytes = dict
        .get(&format!("{key}.utf-8"))
        .or_else(|| dict.get(key))
        .and_then(Value::as_bytes)?;
    Some(String::from_utf8_lossy(bytes).into_owned())
}

fn length(entry: &Value) -> Result<u64, ListingError> {
    entry
        .get("length")
        .and_then(Value::as_int)
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(ListingError::Invalid)
}

/// BEP 47 padding: `attr` holds `p`; older clients named them instead.
fn is_padding(entry: &Value, path: &str) -> bool {
    let flagged = entry
        .get("attr")
        .and_then(Value::as_bytes)
        .is_some_and(|attr| attr.contains(&b'p'));
    flagged || path.starts_with(".pad/") || path.contains("_____padding_file_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::encode;

    fn bytes(s: &str) -> Value {
        Value::Bytes(s.as_bytes().to_vec())
    }

    fn dict(entries: Vec<(&str, Value)>) -> Value {
        // Bencode dicts are sorted by key.
        let mut entries: Vec<(Vec<u8>, Value)> = entries
            .into_iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Value::Dict(entries)
    }

    fn file(path: &[&str], length: i64, attr: Option<&str>) -> Value {
        let mut entries = vec![
            ("length", Value::Int(length)),
            ("path", Value::List(path.iter().map(|p| bytes(p)).collect())),
        ];
        if let Some(a) = attr {
            entries.push(("attr", bytes(a)));
        }
        dict(entries)
    }

    #[test]
    fn single_file_torrent_lists_its_name() {
        let info = dict(vec![
            ("length", Value::Int(1_000)),
            ("name", bytes("Sintel.mp4")),
            ("piece length", Value::Int(16_384)),
            ("pieces", Value::Bytes(vec![0; 20])),
        ]);
        let l = listing("ab", &encode(&info)).unwrap();
        assert_eq!(l.name, "Sintel.mp4");
        assert_eq!(l.size, 1_000);
        assert_eq!(l.piece_length, 16_384);
        assert_eq!(
            l.files,
            vec![FileEntry {
                path: "Sintel.mp4".into(),
                size: 1_000,
                offset: 0
            }]
        );
    }

    #[test]
    fn multi_file_torrent_largest_first_without_padding() {
        let info = dict(vec![
            (
                "files",
                Value::List(vec![
                    file(&["Sintel.de.srt"], 100, None),
                    file(&["Season 1", "E01.mkv"], 5_000, None),
                    file(&[".pad", "3"], 3, Some("p")),
                    file(&["Sintel.en.srt"], 100, None),
                    file(&["Season 1", "E02.mkv"], 7_000, None),
                    file(&["_____padding_file_1_"], 10, None),
                ]),
            ),
            ("name", bytes("Show")),
            ("piece length", Value::Int(16_384)),
            ("pieces", Value::Bytes(vec![0; 20])),
        ]);
        let l = listing("ab", &encode(&info)).unwrap();
        let got: Vec<(&str, u64, u64)> = l
            .files
            .iter()
            .map(|f| (f.path.as_str(), f.size, f.offset))
            .collect();
        assert_eq!(
            got,
            vec![
                ("Season 1/E02.mkv", 7_000, 5_203),
                ("Season 1/E01.mkv", 5_000, 100),
                // Same size: torrent order.
                ("Sintel.de.srt", 100, 0),
                ("Sintel.en.srt", 100, 5_103),
            ]
        );
        assert_eq!(l.size, 12_200);
    }

    #[test]
    fn prefers_utf8_names_and_rejects_junk() {
        let info = dict(vec![
            ("length", Value::Int(5)),
            ("name", bytes("Caf?")),
            ("name.utf-8", bytes("Café")),
            ("piece length", Value::Int(16_384)),
            ("pieces", Value::Bytes(vec![0; 20])),
        ]);
        assert_eq!(listing("ab", &encode(&info)).unwrap().name, "Café");
        assert_eq!(listing("ab", b"i3e").unwrap_err(), ListingError::Invalid);
        let v2 = dict(vec![
            ("file tree", dict(vec![])),
            ("name", bytes("x")),
            ("piece length", Value::Int(16_384)),
        ]);
        assert_eq!(
            listing("ab", &encode(&v2)).unwrap_err(),
            ListingError::V2Only
        );
    }
}
