# Agent DX development

## Boundaries

- `agent/` owns Template/Environment APIs and stateless Activators. Platform lifecycle access goes through the Sandbox capability interface; user Harness traffic uses shared Gateway forwarding.
- `platform/` owns Instance scheduling and execution, the Sandbox SDK, Master, Node Manager and RRT.
- `gateway/` owns the public Sandbox API Server, shared entrypoints, routing and node forwarding; it does not own Instance lifecycle.
- Root `crates/` owns product-wide error semantics, observability, process bootstrap support and transport mechanics. Platform domain models, protocol, scheduling and discovery remain under `platform/crates/`.
- Sandbox SDK uses adx-sandbox / adx_sandbox / ADX_ naming. Owned Agent namespaces use adx, Gateway commands use adx-, and config/headers use ADX. External runtime dependencies require functional replacement, not fabricated import renames. `docs/migration/sources.json` records exact provenance.
- Keep the Rust API Server small: public HTTP types, validation and direct Instance RPC clients. Preserve Sandbox and Agent entrypoints. Do not reintroduce the removed runtime SDK, function/Job packages or metadata watchers to satisfy a helper import. Agent business logic belongs under `agent/`.

## Builds and tests

- Internal gRPC contracts are Instance-centric; no legacy Frontend or POSIX protobuf adapters. Design new internal RPCs around Instance responsibilities; do not reuse old POSIX/function services. RRT operations and Node Manager runtime cooperation use HTTP; shared payloads live in adx-core runtime types.

- Root Cargo workspace includes the API Server; Python packages build independently.
- Use Makefile/native package commands. `build/` contains tracked scripts; outputs go to `out/` or explicitly configured external caches.
- Run focused checks for changed components; report runtime/cluster validation separately.
- Delegate long builds/tests/packaging to a narrow-context subagent when available: exact repo, commands, concurrency, success criteria and log path. The worker does not edit source. Keep complete logs, poll every 60–180 seconds and return compact results.

## Programming standards

- Rust changes follow [the repository Rust coding guidelines](docs/development/rust-coding-guidelines.md), which adopt the applicable rules from the Rust Coding Guidelines. The root `rustfmt.toml` and workspace lint configuration are authoritative; do not introduce crate-local formatting or broad lint exceptions.
- Develop behavior changes test-first. Add a failing test that expresses the contract, implement the smallest coherent change, then run focused tests before the workspace gate.
- New or modified production paths return structured errors for recoverable failures. Do not add unexplained `unwrap`, `expect`, `panic!`, `todo!`, `unimplemented!` or `dbg!`; tests may use assertion-oriented `unwrap` and `expect`.
- Keep unsafe blocks minimal and add a local `SAFETY:` comment that states the concrete invariant. Validate external values at the boundary and use checked numeric conversions where narrowing can overflow.
- Keep public APIs typed and narrow. Document caller-visible errors and intentional panics, preserve naming across API, configuration and storage boundaries, and avoid adding an abstraction until at least two real callers share the same semantics.
- Rust completion requires `cargo fmt --all -- --check`, focused tests, and `cargo clippy --workspace --all-targets --all-features -- -D warnings`. Use `make rust-check JOBS=<n>` when validating the complete repository baseline.

## Documentation consistency

- Documentation describes the checked-in implementation. A behavior, API, configuration, component, deployment mode or test is not documented as available until its implementation and required validation exist.
- Any change to public behavior or architecture updates the affected README, API/error contract, configuration examples, deployment/use instructions and architecture material in the same change. Delete stale text instead of preserving historical behavior in current-state documents; keep history in migration reports and label future work explicitly in roadmaps.
- Code names, paths, defaults, environment variables, ports, process roles and package names in documentation must match source and generated configuration exactly. Examples must remain executable and must not imply a stronger validation boundary than the recorded evidence.
- When architecture Markdown changes, regenerate its HTML with `python3 build/docs/render_architecture.py`. Before completion run `python3 build/docs/check.py`, inspect the resulting errors, and run `git diff --check`.
- Report verification evidence and material gaps together with the change. Local unit/component tests must not be presented as standalone, multi-VM, Kubernetes or full end-to-end proof.

## Commits

Use conventional commit subjects and a single CLA-compatible Signed-off-by trailer. Do not mix migration with unrelated behavior changes.
