# RustSpark CSV Explorer

A small **local demo**, not a hosted BI platform. It lets a trusted
developer upload a CSV, query it using RustSpark, and inspect or export
a bounded result. A new isolated RustSpark process runs for each query.

```sh
cargo build --release --locked
python3 -m pip install streamlit
streamlit run examples/streamlit/app.py
```

The app limits CSV uploads to 32 MiB and returned rows to 1000.
The SQL text is supplied by the local user, so do not expose this demo
to untrusted users or the public internet. It does not implement auth,
SQL sandboxing, saved dashboards, or a service-level query queue.

```sh
cd examples/streamlit && python3 -m unittest -v test_app.py
```
