# Release RustSpark binaries

The release workflow builds the local RustSpark CLI for Linux x86-64, Windows
x86-64 and macOS arm64. It does **not** publish a Cargo crate
(`publish = false` in `Cargo.toml`).

## Before tagging

1. Complete [V1 acceptance](v1-acceptance.md) and resolve release blockers.
2. Choose an experimental release version. Keep the tag in sync with the
   package version in `Cargo.toml` (currently `0.1.0`, tag `v0.1.0`).
3. Confirm that the intended commit is on `main` and has **completed,
   successful** Rust CI on Linux, Rust 1.95 and Windows. Pending or cancelled
   CI runs do not count.
4. Verify the existing Apache-2.0 `LICENSE` is included with distributable
   binaries. The tag workflow also rejects a missing LICENSE.
5. Trigger **Release binaries** with `workflow_dispatch` on the intended
   commit before tagging. This builds archives without publishing a release.
   Confirm storage qualification passes on Linux and Windows, all three
   platform builds complete, and each extracted archive passes its smoke test.
6. If making performance or Spark compatibility claims, run the actual optional
   comparison workflows and include the measured results and differences.
   Passing harness unit tests alone does not establish Spark parity.

## Publish

Only tag the validated `main` commit. For example, when the Cargo package
version is still `0.1.0`:

```sh
git switch main
git pull --ff-only
git tag v0.1.0
git push origin v0.1.0
```

The tag workflow verifies the version, `main` ancestry and a successful
Rust CI push run for the exact tagged commit. Pending, failed or cancelled
runs cannot satisfy the release gate. It qualifies
storage recovery on Linux and Windows, builds each binary with `--locked`,
verifies the **packaged** binary matches the build output, and runs CLI smoke
tests from the extracted archive. Publication depends on these jobs succeeding.

Successful tag builds publish:

- `rustspark-linux-x86_64.tar.gz`
- `rustspark-windows-x86_64.zip`
- `rustspark-macos-arm64.tar.gz`
- `SHA256SUMS`

Each platform archive includes the binary, README and LICENSE. Verify the
published checksums when downloading. If a tag is already published, do not
move it to a different commit. Fix issues on `main` and release a new version.

## Release notes

Describe RustSpark as an experimental **local DataFusion SQL engine**, not a
Spark replacement. The persisted-table protocol supports tested process-crash
recovery, but does not guarantee OS power-loss durability, ACID transactions,
multi-writer operation or protection from external file modification.
Distributed Ballista support remains experimental and separate from the
default binary. Link the known limitations in [V1 acceptance](v1-acceptance.md).
