# TPC-H Data Generator Crate 

This crate provides the core data generator logic for TPC-H. It has no
dependencies and is easy to embed in any other Rust projects.

Distributions can be overridden at runtime using `Distributions::init_from_path` to load values from a custom file.

See the [docs.rs page](https://docs.rs/tpchgen/latest/tpchgen/) for API and the
the tpchgen [README.md](https://github.com/clflushopt/tpchgen-rs) for more
information on the project.
