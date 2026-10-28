# RustSpark as a Kubernetes batch Job

The example runs a **one-shot SQL query** against sample CSV input.
This is not a Kubernetes-native SQL service, shared database, scheduler
or cluster scale-out implementation.

1. Build and push a rootless RustSpark image using the root Dockerfile.
2. Replace the placeholder image in `query-job.yaml` with your own
   immutable registry digest.
3. Apply and inspect the job:

```sh
kubectl apply -f examples/kubernetes/query-job.yaml
kubectl wait --for=condition=complete job/rustspark-sample-query --timeout=300s
kubectl logs job/rustspark-sample-query
```

The pod runs as UID 10001, has no service-account token, no extra Linux
capabilities, a read-only root filesystem and a limited scratch volume.
Input is read-only. Adjust resource limits and spill space for larger
queries. This manifest does not configure network policies, storage
credentials, persistent volumes, multi-node execution or retries.

Do not use `--database-dir` over a multi-writer network mount:
RustSpark's current persistence model is local and single-writer.
