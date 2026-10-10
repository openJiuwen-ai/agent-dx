# AFS acceptance runner contract

`runner.py` is a conservative dispatcher. It can report a selected smoke or
full run as `PASS`, but `summary.full_release_gate_pass` is only true when the
full profile covers every active manifest case, every required matrix value,
and the frozen lock matches observed identities.

## CLI

```bash
python3 runner.py \
  [--case ID ...] [--category CATEGORY] \
  [--backend OwnerFs|DFS] [--meta etcd|Redis] [--transport MODE] \
  [--profile smoke|full] [--timeout SECONDS] \
  [--cases cases.json] --lock /path/to/run/acceptance.lock.json \
  [--contract PATH/docs/testing/afs.md] \
  [--identity-attestation observed-identity.json] \
  [--results-dir DIR]
```

Execution requires an explicit run-specific `--lock` before creating any results.
`--list` needs no lock and executes no driver. `acceptance.lock.example.json` is
a machine-independent PREPARING skeleton, not qualified environment evidence.

Driver commands in `cases.json` must be argv arrays. Shell strings are rejected.
The runner executes drivers with `shell=False`, streams stdout/stderr directly
to the immutable run directory, and terminates the whole process group on
timeout.

## Frozen lock identity

For release/full gates, the lock must be `FROZEN`, have
`verification.status=PASS`, and contain comparable identities for:

- `runner.sha256`
- `manifest.sha256`
- `source.sha256` or `source.git_commit`
- `binary.sha256`
- `contract.sha256`, compared to the actual `--contract` file

`runner` and `manifest` are always hashed by the runner. `source` and `binary`
must be observed from `--identity-attestation` paths. Attested digest or git
fields are ignored; the input is only a locator.

- `source.path` must be either a readable reproducible source archive/file,
  hashed by the runner, or a readable git directory. Git source identity requires
  `git rev-parse HEAD` and `git status --porcelain`; dirty source trees block
  full release.
- `binary.path` must be a readable package or binary file hashed by the runner.
  Future drivers still need to prove they executed the intended binary; a
  declared digest alone is not observed identity.

Missing paths, unreadable paths, unsupported directories, empty identity
dictionaries or mismatched observed digests block full runs.

`contract_sha` retains historical Git provenance; it is not the contract file
digest. The default contract path resolves to `docs/testing/afs.md` in the Agent DX repository. Pass `--contract` when inputs live elsewhere.

## Environment preparation and qualification

Full runs additionally require `lock.environment_evidence` with a relative
`path` and `sha256`. `environment.py` reads and hashes that bundle and its raw
artifacts, then evaluates mandatory environment predicates independently of
declared `FROZEN/PASS` fields. Missing, changed or unsupported proof blocks full
dispatch. A generic JSON record saying PASS is not environment qualification.

Optional `bundle.network` references relative probe, command and guest artifact
paths. Each consumed artifact is hash-bound by `artifact_references`. The
network evaluator independently checks the original exchange identities,
TLS errors, precise directed fault and restoration, rather than importing an
audit summary. A network preparation PASS does not establish independent
watchdog recovery, product authorization, verbs or the remaining ENV predicates.

The preparation evaluator is a separate Linux CLI. It does not modify the
lock or formal cases. It reports observed resource/storage facts and outstanding
checks. Fresh live environment qualification, backend restart, verbs,
reference-suite, comparator mount and frozen-input semantic verification is still required;
the current partial evaluator cannot qualify full acceptance. A future frozen
verifier must also check live identities instead of treating old inventories as
current observations.

Smoke remains a development result. Missing full environment prerequisites do
not prevent a registered local smoke driver from reporting its actual scoped
result; `full_release_gate_pass` remains false. Runner protocol unit tests that
mock environment validation are explicitly isolated dispatcher tests, not ENV
proof.

## Driver proof

The final non-empty JSON object on driver stdout is the proof. Minimum shape:

```json
{
  "case_id": "FUN-01",
  "profile": "smoke",
  "matrix": {"backend": "DFS", "meta": "etcd", "transport": "TCP"},
  "status": "PASS",
  "checks": [
    {"name": "digest", "status": "PASS", "evidence": "sha256 matched"}
  ]
}
```

Each `PASS` check needs a non-empty `name` and either a non-blank evidence string,
a non-empty structured object/list, or a non-empty run-local artifact file.
Boolean/numeric evidence, empty files and directories do not prove a check.
Artifact paths must stay inside the run directory.

`EXCLUDED` at proof or check level must include an `exclusion_id` that matches a
manifest `approved_exclusions` entry or an `exclusions` record with
`approved: true` or `pre_reviewed: true`. Excluded checks are reported in
accounting and do not silently disappear.

## Driver-owned matrix coverage

The runner expands only `backend`, `meta` and `transport`. Other manifest axes
are driver-owned. A PASS proof must cover each expected value explicitly:

```json
{
  "coverage": {
    "profile": "full",
    "axes": {
      "sizes_full": {
        "values": ["0", "1", "4KiB"],
        "checks": {
          "0": "size-0",
          "1": "size-1",
          "4KiB": "size-4KiB"
        }
      }
    }
  }
}
```

Every expected value must be present, every value must reference an existing
`PASS` or approved `EXCLUDED` check, and unexpected values are rejected. Profile
specific axes are scoped by name: `*_full` applies only to `--profile full` and
`*_smoke` applies only to `--profile smoke`.
