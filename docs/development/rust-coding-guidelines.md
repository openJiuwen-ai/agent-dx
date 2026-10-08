# Rust coding guidelines

ADX follows the applicable rules from the
[Rust Coding Guidelines](https://rust-coding-guidelines.github.io/rust-coding-guidelines-zh/)
and turns the high-value rules into repository checks. The upstream document
contains rules at different maturity levels, so this repository does not enable
the entire Clippy `pedantic` or `restriction` groups as a single switch.

## Enforced baseline

The root `rustfmt.toml` is the only formatting policy for the workspace. Run:

```sh
make rust-check JOBS=2
```

The command checks formatting and runs Clippy for the complete workspace with all
features and targets. It rejects:

- `dbg!`, `todo!` and `unimplemented!` in committed Rust targets;
- dependencies declared with a wildcard version;
- an `unsafe` block without a local `SAFETY:` explanation;
- an unsafe operation hidden inside an `unsafe fn` without an explicit block;
- a plain `unwrap` in control-plane or shared-crate production targets;
- every ordinary compiler or Clippy warning because CI passes `-D warnings`.

Each crate inherits the workspace Rust version, edition, license, repository and
lint policy. Crate-specific `allow` attributes require a local reason and should
be narrower than a module whenever practical.

## Error handling

Production code should return structured errors for invalid input, unavailable
dependencies and recoverable state. An invariant that cannot be represented as a
recoverable result may use `expect` with a message that states the invariant.
Tests may use `unwrap` and `expect` when failure is the assertion.

Control-plane and shared-crate production paths contain no plain `unwrap` calls:
recoverable failures return errors and internal invariants use explained
`expect` calls. Existing runtime and gateway code still contains inherited
panic-on-poison and invariant checks. They are audited separately from the
enforced baseline so this change does not silently alter process recovery
behavior. New or modified production paths must not add an unexplained `unwrap`.

## Public APIs and naming

- Public `Result` APIs document caller-visible failure conditions when the name
  and return type do not already make them clear.
- Public APIs that intentionally panic document `# Panics`.
- Names follow Rust casing and keep word order consistent across related types and
  operations. Getters omit a `get_` prefix unless `get` is part of the domain term.
- Imports name their dependencies explicitly. Prelude imports are allowed when a
  library defines the prelude as its supported trait-import surface.
- Integer conversions at external boundaries use checked conversion when the
  source value can exceed the destination range.

## Unsafe code

Every unsafe block documents the concrete pointer, lifetime, initialization or
OS-handle invariant that makes the operation valid. A comment that merely repeats
the call is insufficient. Keep the block around only the operation that requires
it and convert the OS error immediately after the call.

## CI evidence

The Buildkite release step runs the same `make rust-check` command before creating
the release. Its complete output is written to
`out/buildkite/logs/rust-check.log`; a failed gate stops packaging.
