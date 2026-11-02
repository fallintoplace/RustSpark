//! Stateful, budgeted writer for native exchange frames.
use std::io::Write;

use crate::{
    transport::{write_frame, Frame}, Error, Result,
};

/// Track limits across separately produced frames. Limits count payload bytes,
/// while wire headers remain fixed-size per admitted frame.
///
/// A failed I/O operation can emit a partial frame, so the writer is poisoned
/// on that path and must never be reused as a valid exchange stream.
pub struct ExchangeWriter<W> {
    output: W,
    max_frame_bytes: usize,
    max_total_bytes: usize,
    max_frames: usize,
    expected_stage: Option<u64>,
    frames_written: usize,
    payload_bytes_written: usize,
    poisoned: bool,
}

impl<W: Write> ExchangeWriter<W> {
    pub fn new(
        output: W, max_frame_bytes: usize,
        max_total_bytes: usize, max_frames: usize,
    ) -> Self {
        Self {
            output, max_frame_bytes, max_total_bytes, max_frames,
            expected_stage: None, frames_written: 0,
            payload_bytes_written: 0, poisoned: false,
        }
    }

    pub fn for_stage(
        output: W, stage: u64, max_frame_bytes: usize,
        max_total_bytes: usize, max_frames: usize,
    ) -> Self {
        let mut writer = Self::new(
            output, max_frame_bytes, max_total_bytes, max_frames
        );
        writer.expected_stage = Some(stage);
        writer
    }

    pub fn frames_written(&self) -> usize { self.frames_written }
    pub fn payload_bytes_written(&self) -> usize { self.payload_bytes_written }
    pub fn is_poisoned(&self) -> bool { self.poisoned }

    /// Admission errors do not emit bytes. I/O failures poison the writer.
    pub fn send(&mut self, frame: &Frame) -> Result<()> {
        if self.poisoned {
            return Err(Error::Execution("exchange writer is poisoned".into()));
        }
        if self.expected_stage.is_some_and(|stage| stage != frame.stage) {
            return Err(Error::Execution("exchange writer stage mismatch".into()));
        }
        if self.frames_written >= self.max_frames {
            return Err(Error::Execution("exchange stream frame count limit exceeded".into()));
        }
        if frame.payload.len() > self.max_frame_bytes
            || u32::try_from(frame.payload.len()).is_err() {
            return Err(Error::Execution("exchange frame exceeds memory or wire budget".into()));
        }
        let total = self.payload_bytes_written.checked_add(frame.payload.len())
            .filter(|&sum| sum <= self.max_total_bytes)
            .ok_or_else(|| Error::Execution("exchange stream byte budget exceeded".into()))?;

        // Once any byte might be emitted, do not allow a failed send to
        // leave a re-usable writer with an ambiguous wire boundary.
        self.poisoned = true;
        write_frame(&mut self.output, frame, self.max_frame_bytes)?;
        self.frames_written += 1;
        self.payload_bytes_written = total;
        self.poisoned = false;
        Ok(())
    }

    /// Recover the inner sink only when all emitted frames are complete.
    pub fn finish(self) -> Result<W> {
        if self.poisoned {
            return Err(Error::Execution("cannot finish poisoned exchange stream".into()));
        }
        Ok(self.output)
    }
}
