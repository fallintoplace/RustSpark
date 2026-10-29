use std::io::{Read, Write};

use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"RSP1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub stage: u64,
    pub task: u64,
    pub attempt: u32,
    pub payload: Vec<u8>,
}

const CRC32_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0xedb88320 } else { 0 };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

fn checksum(bytes: &[u8]) -> u32 {
    let mut value = !0u32;
    for &byte in bytes {
        let index = ((value ^ u32::from(byte)) & 0xff) as usize;
        value = (value >> 8) ^ CRC32_TABLE[index];
    }
    !value
}

pub fn write_frame(output: &mut impl Write, frame: &Frame, max_bytes: usize) -> Result<()> {
    if frame.payload.len() > max_bytes {
        return Err(Error::Execution("exchange frame exceeds memory budget".into()));
    }
    let len = u32::try_from(frame.payload.len())
        .map_err(|_| Error::Execution("exchange frame exceeds wire length".into()))?;
    let mut header = [0u8; 32];
    header[..4].copy_from_slice(MAGIC);
    header[4..12].copy_from_slice(&frame.stage.to_le_bytes());
    header[12..20].copy_from_slice(&frame.task.to_le_bytes());
    header[20..24].copy_from_slice(&frame.attempt.to_le_bytes());
    header[24..28].copy_from_slice(&len.to_le_bytes());
    header[28..32].copy_from_slice(&checksum(&frame.payload).to_le_bytes());
    output.write_all(&header)?;
    output.write_all(&frame.payload)?;
    Ok(())
}

pub fn read_frame(input: &mut impl Read, max_bytes: usize) -> Result<Option<Frame>> {
    let mut magic = [0u8; 4];
    if input.read(&mut magic[..1])? == 0 {
        return Ok(None);
    }
    input.read_exact(&mut magic[1..])?;
    if &magic != MAGIC {
        return Err(Error::Parse("invalid exchange frame magic or version".into()));
    }
    fn read_u32(input: &mut impl Read) -> Result<u32> {
        let mut bytes = [0u8; 4];
        input.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }
    fn read_u64(input: &mut impl Read) -> Result<u64> {
        let mut bytes = [0u8; 8];
        input.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }
    let stage = read_u64(input)?;
    let task = read_u64(input)?;
    let attempt = read_u32(input)?;
    let len = read_u32(input)? as usize;
    let expected_checksum = read_u32(input)?;
    if len > max_bytes {
        return Err(Error::Execution("received exchange frame exceeds memory budget".into()));
    }
    let mut payload = vec![0; len];
    input.read_exact(&mut payload)?;
    if checksum(&payload) != expected_checksum {
        return Err(Error::Execution("exchange frame checksum mismatch".into()));
    }
    Ok(Some(Frame { stage, task, attempt, payload }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_multiple_frames_and_reject_corruption() {
        let frame = Frame { stage: 42, task: 8, attempt: 3, payload: b"abc".to_vec() };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame, 32).unwrap();
        write_frame(&mut bytes, &frame, 32).unwrap();
        let mut reader = bytes.as_slice();
        assert_eq!(read_frame(&mut reader, 32).unwrap(), Some(frame.clone()));
        assert_eq!(read_frame(&mut reader, 32).unwrap(), Some(frame));
        assert_eq!(read_frame(&mut reader, 32).unwrap(), None);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let mut reader = &bytes[bytes.len() / 2..];
        assert!(read_frame(&mut reader, 32).is_err());
    }

    #[test]
    fn receive_rejects_oversized_frames_before_allocation() {
        let frame = Frame { stage: 1, task: 1, attempt: 1, payload: vec![1; 9] };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame, 10).unwrap();
        assert!(read_frame(&mut bytes.as_slice(), 8).is_err());
    }
}

 
/// Decode a sequence of checksummed exchange frames under an aggregate payload budget.
pub fn read_frames(
    input: &mut impl Read,
    max_frame_bytes: usize,
    max_total_bytes: usize,
) -> Result<Vec<Frame>> {
    read_frames_limited(input, max_frame_bytes, max_total_bytes, 1_000_000)
}

/// Bound decoded frame count as well as cumulative payload bytes.
pub fn read_frames_limited(
    input: &mut impl Read,
    max_frame_bytes: usize,
    max_total_bytes: usize,
    max_frames: usize,
) -> Result<Vec<Frame>> {
    let mut frames = Vec::new();
    let mut total = 0_usize;
    while let Some(frame) = read_frame(
        input, max_frame_bytes.min(max_total_bytes.saturating_sub(total))
    )? {
        if frames.len() >= max_frames {
            return Err(Error::Execution("exchange stream frame count limit exceeded".into()));
        }
        total = total.checked_add(frame.payload.len())
            .filter(|used| *used <= max_total_bytes)
            .ok_or_else(|| Error::Execution("exchange stream byte budget exceeded".into()))?;
        frames.push(frame);
    }
    Ok(frames)
}

#[cfg(test)]
mod exchange_stream_feature_tests {
    use super::*;

    #[test]
    fn enforces_total_payload_budget_over_many_valid_frames() {
        let mut wire = Vec::new();
        for task in 0..3 {
            write_frame(&mut wire, &Frame {
                stage: 1, task, attempt: 0, payload: vec![42; 4]
            }, 8).unwrap();
        }
        assert_eq!(read_frames(&mut wire.as_slice(), 8, 12).unwrap().len(), 3);
        assert!(read_frames(&mut wire.as_slice(), 8, 11).is_err());
        assert!(read_frames(&mut wire.as_slice(), 3, 20).is_err());
    }
}


#[cfg(test)]
mod frame_count_limit_tests {
    use super::*;

    #[test]
    fn rejects_too_many_empty_payload_frames() {
        let mut wire = Vec::new();
        for task in 0..3 {
            write_frame(&mut wire, &Frame {
                stage: 1, task, attempt: 0, payload: Vec::new()
            }, 0).unwrap();
        }
        assert_eq!(read_frames_limited(&mut wire.as_slice(), 0, 0, 3)
            .unwrap().len(), 3);
        assert!(read_frames_limited(&mut wire.as_slice(), 0, 0, 2).is_err());
    }
}

#[cfg(test)]
mod consolidated_exchange_budget_tests {
    use super::*;
    #[test]
    fn rejects_frame_payload_over_remaining_budget_at_header() {
        let mut wire = Vec::new();
        write_frame(&mut wire, &Frame { stage: 1, task: 1, attempt: 1, payload: vec![1; 4] }, 16).unwrap();
        write_frame(&mut wire, &Frame { stage: 1, task: 2, attempt: 1, payload: vec![2; 8] }, 16).unwrap();
        assert!(read_frames(&mut wire.as_slice(), 16, 10).unwrap_err().to_string().contains("memory budget"));
        assert_eq!(read_frames(&mut wire.as_slice(), 16, 12).unwrap().len(), 2);
    }
}

#[cfg(test)]
mod transport_crc32_perf_tests {
    use super::*;

    #[test]
    fn retains_standard_crc32_wire_checksum() {
        assert_eq!(checksum(b""), 0);
        assert_eq!(checksum(b"123456789"), 0xcbf4_3926);
        assert_eq!(checksum(b"RustSpark"), checksum(b"RustSpark"));
    }
}

#[cfg(test)]
mod exchange_header_write_coalescing_tests {
    use super::*;
    struct CountingWriter { bytes: Vec<u8>, calls: usize }
    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    #[test]
    fn nonempty_frame_emits_one_header_and_one_payload_write() {
        let frame = Frame { stage: u64::MAX, task: 17, attempt: 3, payload: vec![5; 4096] };
        let mut writer = CountingWriter { bytes: Vec::new(), calls: 0 };
        write_frame(&mut writer, &frame, 4096).unwrap();
        assert_eq!(writer.calls, 2);
        assert_eq!(writer.bytes.len(), 32 + 4096);
        assert_eq!(read_frame(&mut writer.bytes.as_slice(), 4096).unwrap(), Some(frame));
    }
}
