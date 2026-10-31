use std::io::{Read, Write};

use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"RSP2";
const HEADER_BYTES: usize = 32;
const CHECKSUM_OFFSET: usize = 28;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub stage: u64,
    pub task: u64,
    pub attempt: u32,
    pub payload: Vec<u8>,
}

const CRC32_TABLES: [[u32; 256]; 8] = {
    let mut tables = [[0u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0xedb88320 } else { 0 };
            bit += 1;
        }
        tables[0][index] = crc;
        index += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut index = 0;
        while index < 256 {
            let crc = tables[slice - 1][index];
            tables[slice][index] = (crc >> 8) ^ tables[0][(crc & 0xff) as usize];
            index += 1;
        }
        slice += 1;
    }
    tables
};

fn crc32_update(mut value: u32, mut bytes: &[u8]) -> u32 {
    while bytes.len() >= 8 {
        let low = u32::from_le_bytes(bytes[..4].try_into().expect("eight bytes")) ^ value;
        value = CRC32_TABLES[7][(low & 0xff) as usize]
            ^ CRC32_TABLES[6][((low >> 8) & 0xff) as usize]
            ^ CRC32_TABLES[5][((low >> 16) & 0xff) as usize]
            ^ CRC32_TABLES[4][(low >> 24) as usize]
            ^ CRC32_TABLES[3][bytes[4] as usize]
            ^ CRC32_TABLES[2][bytes[5] as usize]
            ^ CRC32_TABLES[1][bytes[6] as usize]
            ^ CRC32_TABLES[0][bytes[7] as usize];
        bytes = &bytes[8..];
    }
    for &byte in bytes {
        let index = ((value ^ u32::from(byte)) & 0xff) as usize;
        value = (value >> 8) ^ CRC32_TABLES[0][index];
    }
    value
}

fn checksum_parts(header: &[u8], payload: &[u8]) -> u32 {
    !crc32_update(crc32_update(!0u32, header), payload)
}

#[cfg(test)]
fn checksum(bytes: &[u8]) -> u32 {
    checksum_parts(&[], bytes)
}

/// Memory-bounded gate for stale shuffle task attempts.
///
/// A higher attempt replaces the tracked attempt. Multiple frames from the
/// same attempt remain valid; a frame sequence ID would be needed to reject
/// duplicate frames. Callers must commit task results exactly once.
pub struct AttemptTracker {
    attempts: std::collections::HashMap<(u64, u64), u32>,
    max_tasks: usize,
}

impl AttemptTracker {
    pub fn new(max_tasks: usize) -> Result<Self> {
        if max_tasks == 0 {
            return Err(Error::Execution("exchange task tracking budget must be positive".into()));
        }
        Ok(Self { attempts: std::collections::HashMap::new(), max_tasks })
    }

    pub fn accept(&mut self, frame: &Frame) -> Result<bool> {
        let key = (frame.stage, frame.task);
        let at_capacity = self.attempts.len() >= self.max_tasks;
        match self.attempts.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if frame.attempt < *entry.get() { return Ok(false); }
                if frame.attempt > *entry.get() { *entry.get_mut() = frame.attempt; }
                Ok(true)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if at_capacity {
                    return Err(Error::Execution("exchange task tracking budget exceeded".into()));
                }
                entry.insert(frame.attempt);
                Ok(true)
            }
        }
    }
}

pub fn write_frame(output: &mut impl Write, frame: &Frame, max_bytes: usize) -> Result<()> {
    if frame.payload.len() > max_bytes {
        return Err(Error::Execution("exchange frame exceeds memory budget".into()));
    }
    let len = u32::try_from(frame.payload.len())
        .map_err(|_| Error::Execution("exchange frame exceeds wire length".into()))?;
    let mut header = [0u8; HEADER_BYTES];
    header[..4].copy_from_slice(MAGIC);
    header[4..12].copy_from_slice(&frame.stage.to_le_bytes());
    header[12..20].copy_from_slice(&frame.task.to_le_bytes());
    header[20..24].copy_from_slice(&frame.attempt.to_le_bytes());
    header[24..CHECKSUM_OFFSET].copy_from_slice(&len.to_le_bytes());
    let frame_checksum = checksum_parts(&header[..CHECKSUM_OFFSET], &frame.payload);
    header[CHECKSUM_OFFSET..].copy_from_slice(&frame_checksum.to_le_bytes());
    output.write_all(&header)?;
    output.write_all(&frame.payload)?;
    Ok(())
}

/// Encode multiple native exchange frames under one aggregate budget.
/// Validate all sizes before writing to avoid partial output for admission failures.
pub fn write_frames_limited(
    output: &mut impl Write, frames: &[Frame],
    max_frame_bytes: usize, max_total_bytes: usize, max_frames: usize,
) -> Result<()> {
    if frames.len() > max_frames {
        return Err(Error::Execution("exchange stream frame count limit exceeded".into()));
    }
    let mut total = 0usize;
    for frame in frames {
        if frame.payload.len() > max_frame_bytes ||
            u32::try_from(frame.payload.len()).is_err() {
            return Err(Error::Execution("exchange frame exceeds memory or wire budget".into()));
        }
        total = total.checked_add(frame.payload.len())
            .filter(|used| *used <= max_total_bytes)
            .ok_or_else(|| Error::Execution("exchange stream byte budget exceeded".into()))?;
    }
    for frame in frames {
        write_frame(output, frame, max_frame_bytes)?;
    }
    Ok(())
}

pub fn read_frame(input: &mut impl Read, max_bytes: usize) -> Result<Option<Frame>> {
    // Read one byte first to preserve clean EOF at frame boundaries.
    let mut header = [0u8; HEADER_BYTES];
    if input.read(&mut header[..1])? == 0 { return Ok(None); }
    input.read_exact(&mut header[1..])?;
    if &header[..4] != MAGIC {
        return Err(Error::Parse("invalid exchange frame magic or version".into()));
    }
    let stage = u64::from_le_bytes(header[4..12].try_into().expect("fixed u64 header"));
    let task = u64::from_le_bytes(header[12..20].try_into().expect("fixed u64 header"));
    let attempt = u32::from_le_bytes(header[20..24].try_into().expect("fixed u32 header"));
    let len = u32::from_le_bytes(header[24..CHECKSUM_OFFSET].try_into().expect("fixed u32 header")) as usize;
    let expected_checksum = u32::from_le_bytes(header[CHECKSUM_OFFSET..].try_into().expect("fixed u32 header"));
    if len > max_bytes {
        return Err(Error::Execution("received exchange frame exceeds memory budget".into()));
    }
    let mut payload = vec![0; len];
    input.read_exact(&mut payload)?;
    if checksum_parts(&header[..CHECKSUM_OFFSET], &payload) != expected_checksum {
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

 
/// Bounded, incremental reader for native exchange frames.
pub struct FrameReader<R> {
    input: R,
    max_frame_bytes: usize,
    max_total_bytes: usize,
    max_frames: usize,
    bytes_seen: usize,
    frames_seen: usize,
    finished: bool,
}

impl<R: Read> FrameReader<R> {
    pub fn new(input: R, max_frame_bytes: usize, max_total_bytes: usize, max_frames: usize) -> Self {
        Self {
            input, max_frame_bytes, max_total_bytes, max_frames,
            bytes_seen: 0, frames_seen: 0, finished: false,
        }
    }
}

impl<R: Read> Iterator for FrameReader<R> {
    type Item = Result<Frame>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        // Probe for trailing bytes before decoding another unadmitted frame.
        // An attacker must not force a payload allocation after max_frames.
        if self.frames_seen >= self.max_frames {
            self.finished = true;
            let mut extra = [0u8; 1];
            return match self.input.read(&mut extra) {
                Ok(0) => None,
                Ok(_) => Some(Err(Error::Execution("exchange stream frame count limit exceeded".into()))),
                Err(error) => Some(Err(error.into())),
            };
        }
        let remaining = self.max_total_bytes.saturating_sub(self.bytes_seen);
        match read_frame(&mut self.input, self.max_frame_bytes.min(remaining)) {
            Ok(None) => { self.finished = true; None }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
            Ok(Some(frame)) => {
                let Some(total) = self.bytes_seen.checked_add(frame.payload.len())
                    .filter(|&bytes| bytes <= self.max_total_bytes) else {
                    self.finished = true;
                    return Some(Err(Error::Execution("exchange stream byte budget exceeded".into())));
                };
                self.bytes_seen = total;
                self.frames_seen += 1;
                Some(Ok(frame))
            }
        }
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
    loop {
        if frames.len() >= max_frames {
            // EOF at the exact limit is valid; any next byte proves an extra
            // frame without allocating its declared payload.
            let mut extra = [0u8; 1];
            if input.read(&mut extra)? == 0 { break; }
            return Err(Error::Execution("exchange stream frame count limit exceeded".into()));
        }
        let Some(frame) = read_frame(
            input, max_frame_bytes.min(max_total_bytes.saturating_sub(total))
        )? else { break; };
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

#[cfg(test)]
mod exchange_header_read_coalescing_tests {
    use super::*;
    struct CountingReader<'a> { inner: &'a [u8], calls: usize }
    impl Read for CountingReader<'_> {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.calls += 1;
            self.inner.read(bytes)
        }
    }
    #[test]
    fn complete_header_and_payload_take_three_reads() {
        let frame = Frame { stage: 1024, task: 512, attempt: 7, payload: vec![9; 48] };
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &frame, 48).unwrap();
        let mut reader = CountingReader { inner: &encoded, calls: 0 };
        assert_eq!(read_frame(&mut reader, 48).unwrap(), Some(frame));
        assert_eq!(reader.calls, 3);
        assert_eq!(read_frame(&mut reader, 48).unwrap(), None);
    }
}

#[cfg(test)]
mod native_incremental_shuffle_reader_tests {
    use super::*;

    #[test]
    fn reads_each_frame_without_materializing_entire_stream() {
        let frames = (0..3).map(|n| Frame {
            stage: 5, task: n, attempt: 1, payload: vec![n as u8; 4],
        }).collect::<Vec<_>>();
        let mut bytes = Vec::new();
        for frame in &frames { write_frame(&mut bytes, frame, 4).unwrap(); }
        let decoded = FrameReader::new(bytes.as_slice(), 4, 12, 3)
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(decoded, frames);
        let mut bounded = FrameReader::new(bytes.as_slice(), 4, 12, 2);
        assert_eq!(bounded.next().unwrap().unwrap(), frames[0]);
        assert_eq!(bounded.next().unwrap().unwrap(), frames[1]);
        assert!(bounded.next().unwrap().is_err());
        assert!(bounded.next().is_none());
        let mut byte_limited = FrameReader::new(bytes.as_slice(), 4, 7, 3);
        assert!(byte_limited.next().unwrap().is_ok());
        assert!(byte_limited.next().unwrap().is_err());
        assert!(byte_limited.next().is_none());
    }
}

#[cfg(test)]
mod native_shuffle_attempt_gate_tests {
    use super::*;

    #[test]
    fn tracks_monotonic_attempts_without_accepting_replays() {
        let mut gate = AttemptTracker::new(2).unwrap();
        let mut frame = Frame { stage: 1, task: 7, attempt: 2, payload: vec![1] };
        assert!(gate.accept(&frame).unwrap());
        // One task attempt can emit several independent exchange frames.
        assert!(gate.accept(&frame).unwrap());
        frame.attempt = 1;
        assert!(!gate.accept(&frame).unwrap());
        frame.attempt = 3;
        assert!(gate.accept(&frame).unwrap());
        frame.stage = 2;
        assert!(gate.accept(&frame).unwrap());
        frame.task = 8;
        assert!(gate.accept(&frame).is_err());
        frame.stage = 1;
        frame.task = 7;
        frame.attempt = 4;
        assert!(gate.accept(&frame).unwrap());
        assert!(AttemptTracker::new(0).is_err());
    }
}

#[cfg(test)]
mod bounded_native_exchange_writer_tests {
    use super::*;

    #[test]
    fn validates_all_frames_before_emitting_wire_bytes() {
        let frames = (0..3).map(|task| Frame {
            stage: 1, task, attempt: 0, payload: vec![42; 4],
        }).collect::<Vec<_>>();
        let mut wire = Vec::new();
        assert!(write_frames_limited(&mut wire, &frames, 4, 11, 3).is_err());
        assert!(wire.is_empty());
        assert!(write_frames_limited(&mut wire, &frames, 4, 12, 2).is_err());
        assert!(wire.is_empty());
        write_frames_limited(&mut wire, &frames, 4, 12, 3).unwrap();
        assert_eq!(read_frames_limited(&mut wire.as_slice(), 4, 12, 3).unwrap(), frames);
    }

    #[test]
    fn accepts_empty_wire_stream_with_zero_budget() {
        let mut wire = Vec::new();
        write_frames_limited(&mut wire, &[], 0, 0, 0).unwrap();
        assert!(wire.is_empty());
    }
}

#[cfg(test)]
mod shuffle_header_integrity_tests {
    use super::*;

    #[test]
    fn routing_header_corruption_never_redirects_a_valid_payload() {
        let frame = Frame { stage: 17, task: 93, attempt: 2, payload: b"dataset".to_vec() };
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &frame, 128).unwrap();
        assert_eq!(&encoded[..4], b"RSP2");
        for offset in [4, 11, 12, 19, 20, 23] {
            let mut corrupted = encoded.clone();
            corrupted[offset] ^= 0x01;
            assert!(read_frame(&mut corrupted.as_slice(), 128).unwrap_err()
                .to_string().contains("checksum"), "header byte {offset}");
        }
        let mut legacy = encoded;
        legacy[..4].copy_from_slice(b"RSP1");
        assert!(read_frame(&mut legacy.as_slice(), 128).is_err());
    }
}

#[cfg(test)]
mod frame_count_preallocation_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn iterator_rejects_next_frame_after_only_one_extra_byte() {
        let mut wire = Vec::new();
        for task in 0..2 {
            write_frame(&mut wire, &Frame {
                stage: 3, task, attempt: 1, payload: vec![7; 128],
            }, 256).unwrap();
        }
        let mut reader = FrameReader::new(Cursor::new(wire), 256, 256, 1);
        assert!(reader.next().unwrap().is_ok());
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
        assert_eq!(reader.input.position(), (HEADER_BYTES + 128 + 1) as u64);
    }

    #[test]
    fn eager_reader_does_not_read_over_limit_payload() {
        let mut wire = Vec::new();
        for task in 0..2 {
            write_frame(&mut wire, &Frame {
                stage: 5, task, attempt: 1, payload: vec![11; 128],
            }, 256).unwrap();
        }
        let mut input = Cursor::new(wire);
        assert!(read_frames_limited(&mut input, 256, 256, 1).is_err());
        assert_eq!(input.position(), (HEADER_BYTES + 128 + 1) as u64);

        let mut only_one = Vec::new();
        write_frame(&mut only_one, &Frame {
            stage: 5, task: 1, attempt: 1, payload: vec![11; 128],
        }, 256).unwrap();
        assert_eq!(read_frames_limited(&mut only_one.as_slice(), 256, 256, 1)
            .unwrap().len(), 1);
    }
}

#[cfg(test)]
mod slicing_by_eight_crc_regressions {
    use super::*;

    fn reference(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in bytes {
            let mut entry = (crc ^ u32::from(byte)) & 0xff;
            for _ in 0..8 {
                entry = (entry >> 1) ^ if entry & 1 != 0 { 0xedb88320 } else { 0 };
            }
            crc = (crc >> 8) ^ entry;
        }
        !crc
    }

    #[test]
    fn matches_bitwise_crc_for_misaligned_exchange_headers_and_payloads() {
        assert_eq!(checksum(b"123456789"), 0xcbf43926);
        for len in [0, 1, 2, 7, 8, 9, 15, 16, 23, 31, 32, 63, 64, 65, 4097] {
            let header = [0x41u8; 28];
            let payload = (0..len).map(|index| (index * 37 % 251) as u8).collect::<Vec<_>>();
            let mut full = header.to_vec();
            full.extend_from_slice(&payload);
            assert_eq!(checksum_parts(&header, &payload), reference(&full), "payload length {len}");
            assert_eq!(checksum(&payload), reference(&payload));
        }
    }
}
