# Whole-device acceptance

This is an opt-in **real hardware** public-SDK test. It is separate from the
standalone and base Kubernetes gates, which have no GPU/NPU device fixture.
Install the matching `adx-sandbox` wheel and configure `ADX_SERVER_ADDRESS`,
`ADX_TOKEN`, and TLS settings for the deployment before running it. The selected
node needs the advertised device, host driver/firmware, sandboxd discovery,
guest device exposure, and the vendor inventory tool inside the test image.

Run each device type against an explicitly prepared worker and save a new
result directory. These are examples; choose the real node, runtime, image,
device path and inventory command for the installed hardware:

```sh
python3 build/e2e/device/verify.py \
  --image "$ADX_GPU_TEST_IMAGE" --runtime runsc \
  --node-id "$ADX_GPU_NODE_ID" --xpu gpu:l20:1 \
  --device-path /dev/nvidia0 --probe-command 'nvidia-smi -L' \
  --expect 'GPU [0-9]+:' --output out/e2e/device/gpu-001

python3 build/e2e/device/verify.py \
  --image "$ADX_NPU_TEST_IMAGE" --runtime "$ADX_NPU_TEST_RUNTIME" \
  --node-id "$ADX_NPU_NODE_ID" --xpu npu:ascend910b4:1 \
  --device-path /dev/davinci0 --probe-command 'npu-smi info' \
  --expect 'NPU' --output out/e2e/device/npu-001
```

The case requires a running sandbox, a guest character device, and inventory
output matching the specified pattern. It deletes the sandbox and waits for the
public lookup to stop returning a running instance. The result is recorded in
`result.json`; a cleanup failure fails the case. The test
must be run separately for GPU and NPU. A skipped or fixture-less run is not
device acceptance. Current cn-north-4 test workers advertise neither device,
so neither case has a real-hardware result there.
