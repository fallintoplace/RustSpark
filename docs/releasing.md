# Release RustSpark binaries

The binary release workflow builds the local RustSpark CLI for 64-bit Linux and
Windows from a Git tag. It does **not** publish a Cargo crate.

## Before tagging

1. Complete [V1 acceptance](v1-acceptance.md) and resolve release-blocking bugs.
2. Confirm the version in `Cargo.toml` matches the intended tag, for example
   `version = "0.1.0"` for tag `v0.1.0`.
3. Confirm the tag points to a commit on `main` with passing CI.
4. Choose and add the repository license before public redistribution. The
   repository currently does not include a LICENSE file, so this workflow
   does not make a licensing decision.
5. Run a dry build with GitHub Actions `workflow_dispatch`, or validate the
   workflow in a PR. Neither path publishes a release.

## Publish

Create and push a tag pointing to the validated main-branch commit:

```sh
git switch main
git pull --ff-only
git tag v0.1.0
git push origin v0.1.0
```

Use the current `Cargo.toml` version rather than copying the example blindly.
On a tag push, the workflow verifies the exact version and main ancestry,
builds both executables with `--locked`, and smoke-tests each executable.
The final job uploads:

- `rustspark-linux-x86_64.tar.gz`
- `rustspark-windows-x86_64.zip`
- `SHA256SUMS`

A workflow failure does not publish a successful release. If a tag has been
published, do not move it to a different commit. Fix the issue on main and
publish a new version instead.

## What to communicate

RustSpark is an experimental local SQL engine built on DataFusion, **not a
drop-in Apache Spark replacement**. Its local persistent store is append-only
and not transactional. Include the known data durability and distributed
execution limitations in the release notes.
