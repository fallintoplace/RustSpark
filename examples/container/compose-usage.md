# Run a repeatable local RustSpark query with Compose

After the rootless Docker image is merged, run from the repository root:

```sh
docker compose -f examples/container/compose.yaml \
  up --build --abort-on-container-exit --exit-code-from query
```

The job mounts sample CSV input read-only, uses no network, and keeps
the root filesystem read-only. It has a 1 GiB memory limit and a
bounded writable temporary filesystem for small query spills.

The command returns JSON rows on stdout. This is a one-shot SQL job,
not a continuously running service. To run a larger query, replace the
sample bind mount and set suitable memory and spill volume limits.

The Compose file depends on the repository's root `Dockerfile`.
