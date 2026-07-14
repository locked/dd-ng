//! Wire messages used on:
//!   * the SSH control channel (sender stdin/stdout <-> remote ctrl stdin/stdout)
//!   * the local unix rendezvous socket (ctrl <-> data procs on the receiver)
//!
//! Everything except Mode-B data frames is newline-delimited JSON.

use serde::{Deserialize, Serialize};

pub const MAGIC: &str = "DDNG1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Mode {
    /// Seekable input, known size, fixed byte range per stream, no framing.
    Range,
    /// Unknown-size / non-seekable input; each frame carries (offset,len,crc).
    Framed,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub magic: String,
    pub token: String,
    pub mode: Mode,
    pub n_streams: u32,
    /// 0 in Framed mode (unknown).
    pub total_size: u64,
    pub block_size: u64,
    pub output_path: String,
    /// If true, receiver fsyncs the output file before acking Done.
    #[serde(default)]
    pub sync: bool,
    /// If true, receiver opens output with O_DIRECT (bypass page cache).
    /// Range mode only; requires 4 KiB alignment of offsets, lengths, buffers.
    #[serde(default)]
    pub direct: bool,
    /// Empty in Framed mode.
    pub ranges: Vec<Range>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Range {
    pub offset: u64,
    pub length: u64,
}

/// Messages on the SSH ctrl channel (receiver-ctrl <-> sender).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CtrlMsg {
    Ready,
    /// Sender -> ctrl at end of transfer.
    /// Mode Range: per-stream CRC32C of the bytes each stream sent.
    /// Mode Framed: total_bytes lets ctrl verify aggregate size.
    SenderReport {
        stream_crcs: Vec<u32>,
        total_bytes: u64,
    },
    Done {
        bytes: u64,
    },
    Abort {
        reason: String,
    },
}

/// Messages on the local unix rendezvous socket (ctrl <-> data procs).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RvMsg {
    Hello {
        stream_id: u32,
    },
    /// Mode Range assignment.
    AssignRange {
        output_path: String,
        offset: u64,
        length: u64,
        #[serde(default)]
        direct: bool,
    },
    /// Mode Framed assignment.
    AssignFramed {
        output_path: String,
    },
    Done {
        stream_id: u32,
        bytes: u64,
        /// CRC32C of bytes actually written (payload order in framed mode).
        crc: u32,
    },
    Failed {
        stream_id: u32,
        reason: String,
    },
}

// ---------- Framed-mode data-stream frame header (binary, 16 bytes LE) ----------
// [ u64 offset ][ u32 length ][ u32 crc32c_of_payload ]
pub const FRAME_HDR_LEN: usize = 16;

pub fn encode_frame_hdr(offset: u64, length: u32, crc: u32) -> [u8; FRAME_HDR_LEN] {
    let mut b = [0u8; FRAME_HDR_LEN];
    b[0..8].copy_from_slice(&offset.to_le_bytes());
    b[8..12].copy_from_slice(&length.to_le_bytes());
    b[12..16].copy_from_slice(&crc.to_le_bytes());
    b
}

pub fn decode_frame_hdr(b: &[u8; FRAME_HDR_LEN]) -> (u64, u32, u32) {
    let offset = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let length = u32::from_le_bytes(b[8..12].try_into().unwrap());
    let crc = u32::from_le_bytes(b[12..16].try_into().unwrap());
    (offset, length, crc)
}
