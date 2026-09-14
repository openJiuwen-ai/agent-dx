/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/instancecache"
)

func TestDeleteHandlerRetainedFailure(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		status               int32
		tenant               string
		deleted, unavailable bool
		want                 int
		kills, reads         int
	}{
		{"fatal already deleted", instancecache.StatusFatal, "tenant-owner", true, false, 200, 0, 1},
		{"failed already deleted", instancecache.StatusFailed, "tenant-owner", true, false, 200, 0, 1},
		{"fatal still exists", instancecache.StatusFatal, "tenant-owner", false, false, 500, 1, 1},
		{"authority unavailable", instancecache.StatusFatal, "tenant-owner", false, true, 503, 0, 1},
		{"cross tenant", instancecache.StatusFatal, "another-tenant", true, false, 403, 0, 0},
		{"running uses kill", instancecache.StatusRunning, "tenant-owner", true, false, 500, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "sandbox-retained-delete"
			instancecache.Default().PutSummary(instancecache.Summary{InstanceID: id, TenantID: "tenant-owner", StatusCode: tc.status, NodeID: "original-node"})
			t.Cleanup(func() { instancecache.Default().Delete(id) })
			original := confirmSandboxInstanceDeleted
			t.Cleanup(func() { confirmSandboxInstanceDeleted = original })
			reads, kills := 0, 0
			confirmSandboxInstanceDeleted = func(ctx context.Context, instanceID string) (bool, error) {
				reads++
				require.Equal(t, id, instanceID)
				if tc.unavailable {
					return false, errors.New("etcd unavailable")
				}
				return tc.deleted, nil
			}
			setAPIClientsForTest(t, &runtimeStub{kill: func(string, int, []byte, createOptions) error {
				kills++
				return errors.New("owner has no instance")
			}})
			// Repeated DELETE remains authorized through retained history and succeeds
			// without erasing the watcher's deletion/version fence.
			repeats := 1
			if tc.want == http.StatusOK {
				repeats = 2
			}
			for i := 0; i < repeats; i++ {
				ctx, rec := deleteTestContext(t, id, tc.tenant, backend.RoleTenant)
				DeleteHandler(ctx)
				require.Equal(t, tc.want, rec.Code)
				if tc.want == http.StatusOK {
					var response httpx.Response
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
					var data map[string]string
					require.NoError(t, json.Unmarshal(response.Data, &data))
					require.Equal(t, "deleted", data["status"])
				}
			}
			require.Equal(t, tc.kills*repeats, kills)
			require.Equal(t, tc.reads*repeats, reads)
			_, retained := instancecache.Default().GetSummary(id)
			require.True(t, retained)
		})
	}
}
