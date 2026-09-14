# RRT

Imported from `adx/api/rust/rrt-daemon` at the commit recorded in `docs/migration/sources.json`. Build with the root Cargo workspace: `cargo build --locked -p rrt-daemon`.

The `rrt`, `rrt-probe`, `rrtctl`, and `rrt-runtime` binary names are preserved. Existing RuntimeRPC remains active. Protocol sources now live in `platform/api/proto/legacy/rrt`; the new Node Manager/RRT control protocol is not implemented by this migration.
