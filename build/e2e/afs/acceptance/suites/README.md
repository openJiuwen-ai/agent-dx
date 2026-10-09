# AFS Standard Suite Preparation

This directory contains Linux-only preparation assets for `STD-01` through `STD-05` from `docs/development/afs-plan.md`.

The scripts are preparation tooling, not product acceptance results. They freeze upstream identities, discover suite inventories, produce applicability/accounting inputs and run only short ext4 reference smoke checks on the ctl VM state disk.

## Boundary

- Run on `afs-accept-ctl` only.
- Use guest ext4 storage, preferably `/mnt/lima-afsctlstate`.
- Do not run on macOS and do not use a Lima shared mount for test data.
- Do not modify `build/e2e/afs/acceptance/cases.json`, the runner, or product code.
- Do not turn smoke reference output into full STD PASS.
- Do not add exclusions after a failure. Exclusions must be pre-reviewed and recorded separately.

## Files

- `prepare_ctl_reference.sh` downloads pinned upstream suites, records identities, discovers test inventories and runs short ext4 smoke checks.
- `random_model.py` is the fixed STD-04 random operation model used for smoke/reference generation before a full Hypothesis-style runner exists.
- `suite-contract.json` records the contract pins and first-stage suite boundaries in machine-readable form.
- `ltp-filesystem-selectors.txt` records the LTP subset selectors required by the contract. The preparation script expands them against the pinned LTP source.

## Output

The preparation script writes evidence under:

```text
.local/acceptance/suites-reference/
```

Expected outputs include:

- `summary.json`
- `upstream-identities.json`
- `inventory/*.txt`
- `applicability/*.tsv`
- `accounting/*.json`
- `logs/*.log`
- `short-ext4/*.json`
