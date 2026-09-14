// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package sandbox

// These affinity fields preserve the public Sandbox JSON contract.
// OperatorType operator type
type OperatorType int32

const (
	// LabelOpIn in
	LabelOpIn OperatorType = 0
	// LabelOpNotIn not in
	LabelOpNotIn OperatorType = 1
	// LabelOpExists exists
	LabelOpExists OperatorType = 2
	// LabelOpNotExists not exists
	LabelOpNotExists OperatorType = 3
)

// LabelOperator affinity label operator
type LabelOperator struct {
	Type        OperatorType
	LabelKey    string
	LabelValues []string
}

// AffinityKindType affinity type
type AffinityKindType int32

const (
	// AffinityKindResource resource
	AffinityKindResource AffinityKindType = 0
	// AffinityKindInstance instance
	AffinityKindInstance AffinityKindType = 1
)

// AffinityType affinity type
type AffinityType int32

const (
	// PreferredAffinity prefer
	PreferredAffinity AffinityType = 0
	// PreferredAntiAffinity prefer anti
	PreferredAntiAffinity AffinityType = 1
	// RequiredAffinity required
	RequiredAffinity AffinityType = 2
	// RequiredAntiAffinity required anti
	RequiredAntiAffinity AffinityType = 3
)

// Affinity -
type Affinity struct {
	Kind                     AffinityKindType
	Affinity                 AffinityType
	PreferredPriority        bool
	PreferredAntiOtherLabels bool
	LabelOps                 []LabelOperator
}

// createOptions is the HTTP adapter's request-encoding state.
type createOptions struct {
	Cpu, Memory, CpuLimit, MemoryLimit int
	CustomResources                    map[string]float64
	CustomExtensions, CreateOpt        map[string]string
	Labels                             []string
	ScheduleAffinities                 []Affinity
	ScheduleTimeoutMs                  int64
	RecoverRetryTimes, Priority        int
	TraceID                            string
	Timeout                            int
}
type inlineArg struct {
	Type            int32
	Data            []byte
	NestedObjectIDs []string
}

const inlineValue int32 = 0

type operationError struct {
	Code int
	Err  error
}

func (e operationError) Error() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}
func (e operationError) Unwrap() error { return e.Err }

type sandboxResourceSpec struct {
	CPU                 int64                  `json:"cpu"`
	Memory              int64                  `json:"memory"`
	InvokeLabel         string                 `json:"invokeLabels"`
	CustomResources     map[string]int64       `json:"customResources"`
	CustomResourcesSpec map[string]interface{} `json:"customResourcesSpec"`
	EphemeralStorage    int                    `json:"ephemeral_storage"`
}

const inlineHeaderSize = 16
