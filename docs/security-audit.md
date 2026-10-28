# Dependency security audit

The scheduled `Rust dependency security audit` workflow compares
the committed `Cargo.lock` against RustSec advisories every Monday.
It can also run manually via GitHub Actions.

A vulnerability makes the audit job fail and publishes a JSON artifact
for investigation. This is an **alert**, not an auto-update or proof that
all dependencies are secure. Review advisory context, exploitability and
upstream fixes before changing a version.

The audit is intentionally not on every pull request: the repository
has an existing locked Rust test workflow, and advisory-database
outages or unrelated new disclosures should not block all development.

The audit does not assess RustSpark's SQL sandboxing, access control,
persistence recovery, supply-chain provenance, operating-system images,
or the safety of any publicly exposed API. Those need separate reviews.
