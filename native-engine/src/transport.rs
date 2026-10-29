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

fn checksum(bytes: &[u8]) -> u32 {
    let mut value = !0u32;
    for byte in bytes {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = (value >> 1) ^ if value & 1 != 0 { 0xedb88320 } else { 0 };
        }
    }
    !value
}

pub fn write_frame(output: &mut impl Write, frame: &Frame, max_bytes: usize) -> Result<()> {
    if frame.payload.len() > max_bytes {
        return Err(Error::Execution("exchange frame exceeds memory budget".into()));
    }
    let len = u32::try_from(frame.payload.len())
        .map_err(|_| Error::Execution("exchange frame exceeds wire length".into()))?;
    output.write_all(MAGIC)?;
    output.write_all(&frame.stage.to_le_bytes())?;
    output.write_all(&frame.task.to_le_bytes())?;
    output.write_all(&frame.attempt.to_le_bytes())?;
    output.write_all(&len.to_le_bytes())?;
    output.write_all(&checksum(&frame.payload).to_le_bytes())?;
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
    let mut frames = Vec::new();
    let mut total = 0_usize;
    while let Some(frame) = read_frame(input, max_frame_bytes.min(max_total_bytes.saturating_sub(total)))? {
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
mod exchange_preflight_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn oversized_second_frame_does_not_read_payload() {
        let mut encoded = Vec::new();
        for task in 0..2 {
            write_frame(&mut encoded, &Frame { stage: 1, task, attempt: 1,
                payload: vec![7; 8] }, 8).unwrap();
        }
        let mut reader = Cursor::new(&encoded);
        assert!(read_frames(&mut reader, 8, 8).is_err());
        assert!(reader.position() < encoded.len() as u64);
    }
}
