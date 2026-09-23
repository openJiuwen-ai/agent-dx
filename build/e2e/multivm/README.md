# Three-VM process acceptance

This directory defines the result contract for one control VM and two worker
VMs. It does not provision machines. The deployment adapter must install one
verified release on all three machines, generate topology-specific mTLS
configuration, start external sandboxd on both workers, and run the public SDK
through the control VM's Ingress endpoint.

The inventory must record three unique machine IDs, hostnames and addresses,
plus the clean source commit, release SHA256 and target. A passing result must
run all checks in `contract.REQUIRED`, prove actual placement on both worker
VMs, stop workers before the control VM, and record empty sandboxd inventories
and an empty route catalog.

Validate completed evidence with:

```sh
python3 build/e2e/multivm/verify.py \
  --inventory out/e2e/3vm/inventory.json \
  --result out/e2e/3vm/result.json
```

The first automated deployment adapter should wrap the repository's public SDK
assertions and `adxctl`; it must not replace them with SSH process checks. A VM
ping, supervisor PID or three unique machine IDs is only deployment evidence,
not a passing business acceptance result.
