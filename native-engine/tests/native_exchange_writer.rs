use std::io::{self, Write};
use rustspark_native::{
    exchange_writer::ExchangeWriter,
    transport::{read_frames_limited, Frame},
};

fn frame(stage: u64, task: u64, bytes: usize) -> Frame {
    Frame { stage, task, attempt: 1, payload: vec![task as u8; bytes] }
}

#[test]
fn stateful_writer_preserves_frames_and_limits_across_multiple_sends() {
    let mut writer = ExchangeWriter::for_stage(Vec::new(), 4, 8, 10, 3);
    writer.send(&frame(4, 1, 4)).unwrap();
    writer.send(&frame(4, 2, 6)).unwrap();
    assert_eq!((writer.frames_written(), writer.payload_bytes_written()), (2, 10));
    assert!(writer.send(&frame(4, 3, 1)).is_err());
    assert!(writer.send(&frame(5, 3, 0)).is_err());
    assert!(writer.send(&frame(4, 3, 9)).is_err());
    assert!(!writer.is_poisoned());
    // Rejected frames do not consume quotas or pollute the wire.
    writer.send(&frame(4, 3, 0)).unwrap();
    assert!(writer.send(&frame(4, 4, 0)).is_err());
    let bytes = writer.finish().unwrap();
    assert_eq!(read_frames_limited(&mut bytes.as_slice(), 8, 10, 3).unwrap(), vec![
        frame(4, 1, 4), frame(4, 2, 6), frame(4, 3, 0),
    ]);
}

struct FailAfterHeader { writes: usize }

impl Write for FailAfterHeader {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.writes == 0 {
            self.writes += 1;
            Ok(data.len())
        } else {
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[test]
fn io_errors_poison_writer_and_prevent_reusing_partial_exchange_stream() {
    let mut writer = ExchangeWriter::new(
        FailAfterHeader { writes: 0 }, 10, 100, 10
    );
    assert!(writer.send(&frame(1, 1, 5)).is_err());
    assert!(writer.is_poisoned());
    assert_eq!(writer.frames_written(), 0);
    assert_eq!(writer.payload_bytes_written(), 0);
    assert!(writer.send(&frame(1, 2, 5)).unwrap_err().to_string()
        .contains("poisoned"));
    assert!(writer.finish().is_err());
}

#[test]
fn zero_budget_only_admits_empty_frames_and_empty_streams() {
    let mut writer = ExchangeWriter::new(Vec::new(), 0, 0, 2);
    writer.send(&frame(1, 1, 0)).unwrap();
    writer.send(&frame(2, 2, 0)).unwrap();
    assert!(writer.send(&frame(3, 3, 0)).is_err());
    assert_eq!(read_frames_limited(&mut writer.finish().unwrap().as_slice(),
        0, 0, 2).unwrap().len(), 2);
    let empty = ExchangeWriter::new(Vec::new(), 0, 0, 0).finish().unwrap();
    assert!(empty.is_empty());
}
