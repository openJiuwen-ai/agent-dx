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

// Package instancecache stores the instance summaries used by HTTP lifecycle handlers.
package instancecache

import "sync"

const (
	StatusRunning        int32 = 3
	StatusFailed         int32 = 4
	StatusFatal          int32 = 6
	StatusScheduleFailed int32 = 7
	StatusPaused         int32 = 13
	InstanceManagerOwner       = "InstanceManagerOwner"
)

type Summary struct {
	InstanceID, TenantID, NodeID, Function string
	StatusCode                             int32
	StatusMsg                              string
	ContainerID, ContainerIP               string
}
type Store struct {
	mu        sync.RWMutex
	summaries map[string]Summary
}

func NewStore() *Store { return &Store{summaries: make(map[string]Summary)} }
func (s *Store) PutSummary(v Summary) {
	if v.InstanceID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summaries[v.InstanceID] = v
}
func (s *Store) GetSummary(id string) (Summary, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.summaries[id]
	return v, ok
}
func (s *Store) Delete(id string) { s.mu.Lock(); defer s.mu.Unlock(); delete(s.summaries, id) }

var defaultStore = NewStore()

func Default() *Store { return defaultStore }
