# LiteLLM MaaS upstream example

[`litellm-maas.yaml`](litellm-maas.yaml) configures the Guiyang-1 MaaS
OpenAI-compatible endpoint. The four LiteLLM names route to four upstream
models. The old `deepseek-flash` alias is removed; the Jiuwen template defaults
to `deepseek-v4-flash`. Existing sandboxes using the removed alias must switch
to the new template before making further model calls.

The LiteLLM process must receive `MAAS_KEY` as an environment variable. In the
`adx-jiuwen-dev` CCE deployment, add `MAAS_KEY` to the existing
`litellm-credentials` Secret; `deployment/litellm` already imports that Secret
with `envFrom`. Put the YAML content into `configmap/litellm-config` as
`config.yaml`, then restart only `deployment/litellm` so both the updated
ConfigMap and environment variable are loaded. Keep the existing LiteLLM
master key, salt key, database URL, Service, and user-key database.

The four models use **quota accounting**, not upstream USD prices. Input,
output, cached input and cache creation each cost `0.000001` quota points per
token. A key budget of `5.0` therefore represents five million input + output
tokens. LiteLLM's spend fields and UI may still label these values as USD;
under this configuration they are quota points, not actual provider charges.
All newly added models must use the same rates to preserve this quota policy.

For this uniform-token-quota example, set `litellm.max_budget: 5.0`,
`litellm.points_per_budget_unit: 1.0`, and `litellm.budget_duration: "1d"`
in Ingress's `ADX_ACCOUNT_CONFIG`. These override the currency-to-points
values in [`accounts.json`](../../../gateway/examples/accounts.json).
That account example uses a 15-unit currency budget and two displayed points
per budget unit; it requires model prices expressed in the same currency.
Restart Ingress
after updating its configuration. ADX shares one persisted key across each
business user's bindings and sessions. Its existing-key reconciliation does
not change budgets: update existing business keys separately through LiteLLM's
`/key/update` with `max_budget: 5.0` and `budget_duration: "1d"`. At the switch
from currency to quota units, set `spend: 0` once to start a fresh quota cycle;
this leaves historical spend logs intact but those older entries use the old
pricing and must not be interpreted as token quota consumption.

The configuration sets `timezone: UTC` and `budget_reset_time: "00:00"`.
LiteLLM 1.104.2 aligns a `1d` budget to midnight UTC (08:00 Asia/Shanghai),
subject to its budget-reset worker running. Check each key's `budget_reset_at`
after updating it. Changing the timezone requires updating existing key reset
schedules as well. Budget accounting is not a strict token reservation system:
in-flight or concurrent calls may exceed the quota before usage is recorded.

ADX creates user keys with `models: ["all-proxy-models"]`; new proxy models
therefore do not require per-key updates. Existing keys are reconciled during
launch preparation or quota queries without resetting spend or budgets. Warm
bindings do not call the management API; deployments can reconcile them explicitly.
Jiuwen's selectable models are separately declared through the template's
`JIUWEN_PLATFORM_MODELS` JSON environment variable and compiled by its startup adapter.
