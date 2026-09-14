// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package sandbox

import "strings"

const maxRouterNameLen = 200

func sanitizeInstanceID(id string) string {
	result := strings.ReplaceAll(id, "@", "-at-")
	result = strings.Map(func(r rune) rune {
		switch r {
		case '/', '.', '_':
			return '-'
		default:
			return r
		}
	}, result)
	if len(result) > maxRouterNameLen {
		result = result[:maxRouterNameLen]
	}
	return result
}
