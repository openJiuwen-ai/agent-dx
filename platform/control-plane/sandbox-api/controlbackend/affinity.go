package controlbackend

import (
	old "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/affinity"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

const maxNodeTerms = 256

// The compatibility boundary expands bounded DNF. The scheduling kernel keeps
// its native OR-of-AND selectors; legacy NotIn additionally requires existence.
func expression(e *old.LabelExpression, negative bool) ([][]*pb.LabelRequirement, error) {
	if e == nil || strings.TrimSpace(e.Key) == "" || e.Op == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid affinity expression")
	}
	req := func(op pb.SelectorOp, values []string) *pb.LabelRequirement {
		return &pb.LabelRequirement{Key: e.Key, Op: op, Values: append([]string(nil), values...)}
	}
	switch op := e.Op.LabelOperator.(type) {
	case *old.LabelOperator_In:
		if op.In == nil || len(op.In.Values) == 0 {
			break
		}
		kind := pb.SelectorOp_SELECTOR_OP_IN
		if negative {
			kind = pb.SelectorOp_SELECTOR_OP_NOT_IN
		}
		return [][]*pb.LabelRequirement{{req(kind, op.In.Values)}}, nil
	case *old.LabelOperator_NotIn:
		if op.NotIn == nil || len(op.NotIn.Values) == 0 {
			break
		}
		if negative {
			return [][]*pb.LabelRequirement{{req(pb.SelectorOp_SELECTOR_OP_DOES_NOT_EXIST, nil)}, {req(pb.SelectorOp_SELECTOR_OP_IN, op.NotIn.Values)}}, nil
		}
		return [][]*pb.LabelRequirement{{req(pb.SelectorOp_SELECTOR_OP_EXISTS, nil), req(pb.SelectorOp_SELECTOR_OP_NOT_IN, op.NotIn.Values)}}, nil
	case *old.LabelOperator_Exists:
		if op.Exists == nil {
			break
		}
		kind := pb.SelectorOp_SELECTOR_OP_EXISTS
		if negative {
			kind = pb.SelectorOp_SELECTOR_OP_DOES_NOT_EXIST
		}
		return [][]*pb.LabelRequirement{{req(kind, nil)}}, nil
	case *old.LabelOperator_NotExist:
		if op.NotExist == nil {
			break
		}
		kind := pb.SelectorOp_SELECTOR_OP_DOES_NOT_EXIST
		if negative {
			kind = pb.SelectorOp_SELECTOR_OP_EXISTS
		}
		return [][]*pb.LabelRequirement{{req(kind, nil)}}, nil
	}
	return nil, status.Error(codes.InvalidArgument, "invalid affinity operator or operands")
}
func subconditions(s *old.Selector) ([]*old.SubCondition, error) {
	if s == nil {
		return nil, nil
	}
	if s.Condition == nil || len(s.Condition.SubConditions) == 0 || len(s.Condition.SubConditions) > maxNodeTerms {
		return nil, status.Error(codes.InvalidArgument, "invalid affinity condition")
	}
	for _, sub := range s.Condition.SubConditions {
		if sub == nil || len(sub.Expressions) == 0 || len(sub.Expressions) > 64 {
			return nil, status.Error(codes.InvalidArgument, "invalid affinity conjunction")
		}
	}
	return s.Condition.SubConditions, nil
}
func conjoin(left, right [][]*pb.LabelRequirement) ([][]*pb.LabelRequirement, error) {
	if len(left)*len(right) > maxNodeTerms {
		return nil, status.Error(codes.InvalidArgument, "affinity expands beyond 256 alternatives")
	}
	var out [][]*pb.LabelRequirement
	for _, a := range left {
		for _, b := range right {
			if len(a)+len(b) > 512 {
				return nil, status.Error(codes.InvalidArgument, "affinity conjunction too large")
			}
			out = append(out, append(append([]*pb.LabelRequirement{}, a...), b...))
		}
	}
	return out, nil
}
func translateNodeRequired(a *old.Affinity, p *pb.SchedulingPolicy) error {
	if a == nil {
		return nil
	}
	if a.Resource == nil {
		return nil
	}
	r := a.Resource
	required, err := subconditions(r.RequiredAffinity)
	if err != nil {
		return err
	}
	terms := [][]*pb.LabelRequirement{{}}
	if required != nil {
		terms = nil
		for _, sub := range required {
			var term []*pb.LabelRequirement
			for _, e := range sub.Expressions {
				values, eerr := expression(e, false)
				if eerr != nil {
					return eerr
				}
				term = append(term, values[0]...)
			}
			terms = append(terms, term)
		}
	}
	forbidden, err := subconditions(r.RequiredAntiAffinity)
	if err != nil {
		return err
	}
	// NOT (C1 OR C2) = NOT C1 AND NOT C2; each Ci is an AND of expressions.
	for _, sub := range forbidden {
		var alternatives [][]*pb.LabelRequirement
		for _, e := range sub.Expressions {
			values, eerr := expression(e, true)
			if eerr != nil {
				return eerr
			}
			alternatives = append(alternatives, values...)
		}
		terms, err = conjoin(terms, alternatives)
		if err != nil {
			return err
		}
	}
	if required == nil && forbidden == nil {
		return nil
	}
	for _, term := range terms {
		p.RequiredNode = append(p.RequiredNode, &pb.LabelSelector{Expressions: term})
	}
	return nil
}

// Groups preserve alternatives and order without turning peer OR into AND.
func translateNodeAffinity(a *old.Affinity, p *pb.SchedulingPolicy) error {
	if a == nil {
		return nil
	}
	if err := translateNodeRequired(a, p); err != nil {
		return err
	}
	add := func(selector *old.Selector, target pb.PlacementTarget, required, anti bool) error {
		subs, err := subconditions(selector)
		if err != nil {
			return err
		}
		if selector == nil {
			return nil
		}
		if target == pb.PlacementTarget_PLACEMENT_TARGET_NODE && required && !selector.Condition.OrderPriority {
			return nil
		}
		g := &pb.PlacementGroup{Target: target, Required: required, Anti: anti, Ordered: selector.Condition.OrderPriority}
		for _, sub := range subs {
			if sub.Weight < 0 || sub.Weight > 1000 {
				return status.Error(codes.InvalidArgument, "affinity weight must be between 0 and 1000")
			}
			weight := sub.Weight
			if weight == 0 {
				weight = 1
			}
			term := &pb.LabelSelector{}
			for _, e := range sub.Expressions {
				requirements, err := expression(e, false)
				if err != nil {
					return err
				}
				term.Expressions = append(term.Expressions, requirements[0]...)
			}
			g.Terms = append(g.Terms, &pb.WeightedSelector{Selector: term, Weight: uint32(weight)})
		}
		p.PlacementGroups = append(p.PlacementGroups, g)
		return nil
	}
	if r := a.Resource; r != nil {
		for _, entry := range []struct {
			s              *old.Selector
			required, anti bool
		}{{r.PreferredAffinity, false, false}, {r.PreferredAntiAffinity, false, true}, {r.RequiredAffinity, true, false}, {r.RequiredAntiAffinity, true, true}} {
			if err := add(entry.s, pb.PlacementTarget_PLACEMENT_TARGET_NODE, entry.required, entry.anti); err != nil {
				return err
			}
		}
	}
	if r := a.Instance; r != nil {
		// Public HTTP has no physical-host scope. Its local execution scope is
		// the Node Manager, regardless of process or Pod deployment.
		if r.Scope != old.AffinityScope_POD {
			return status.Error(codes.Unimplemented, "physical-host instance affinity scope is not configured")
		}
		for _, entry := range []struct {
			s              *old.Selector
			required, anti bool
		}{{r.PreferredAffinity, false, false}, {r.PreferredAntiAffinity, false, true}, {r.RequiredAffinity, true, false}, {r.RequiredAntiAffinity, true, true}} {
			if err := add(entry.s, pb.PlacementTarget_PLACEMENT_TARGET_INSTANCE, entry.required, entry.anti); err != nil {
				return err
			}
		}
	}
	return nil
}
