// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
package sandboxapi

import "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/instancecache"

// InstanceSummary is a cached observation, not a lifecycle controller.
type InstanceSummary = instancecache.Summary

// ObserveInstance is called by the host's state subscriber after an accepted update.
func ObserveInstance(s InstanceSummary) { instancecache.Default().PutSummary(s) }
func ForgetInstance(id string)          { instancecache.Default().Delete(id) }
