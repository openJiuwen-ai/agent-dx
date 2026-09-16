package controlbackend

import (
	old "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/affinity"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"testing"
)

func in(key string, values ...string) *old.LabelExpression {
	return &old.LabelExpression{Key: key, Op: &old.LabelOperator{LabelOperator: &old.LabelOperator_In{In: &old.LabelIn{Values: values}}}}
}
func selection(terms ...[]*old.LabelExpression) *old.Selector {
	s := &old.Selector{Condition: &old.Condition{}}
	for _, term := range terms {
		s.Condition.SubConditions = append(s.Condition.SubConditions, &old.SubCondition{Expressions: term})
	}
	return s
}
func matchesRequired(terms []*pb.LabelSelector, labels map[string]string) bool {
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		ok := true
		for _, r := range term.Expressions {
			value, present := labels[r.Key]
			found := false
			for _, v := range r.Values {
				if v == value {
					found = true
				}
			}
			switch r.Op {
			case pb.SelectorOp_SELECTOR_OP_IN:
				ok = ok && present && found
			case pb.SelectorOp_SELECTOR_OP_NOT_IN:
				ok = ok && (!present || !found)
			case pb.SelectorOp_SELECTOR_OP_EXISTS:
				ok = ok && present
			case pb.SelectorOp_SELECTOR_OP_DOES_NOT_EXIST:
				ok = ok && !present
			default:
				panic("unexpected operator")
			}
		}
		if ok {
			return true
		}
	}
	return false
}
func TestNodeAffinityPreservesOrMissingNotInAndAntiConjunction(t *testing.T) {
	notIn := &old.LabelExpression{Key: "pool", Op: &old.LabelOperator{LabelOperator: &old.LabelOperator_NotIn{NotIn: &old.LabelNotIn{Values: []string{"blocked"}}}}}
	input := &old.Affinity{Resource: &old.ResourceAffinity{
		RequiredAffinity:     selection([]*old.LabelExpression{in("NODE_ID", "a")}, []*old.LabelExpression{in("NODE_ID", "b"), notIn}),
		RequiredAntiAffinity: selection([]*old.LabelExpression{in("zone", "bad"), in("disk", "slow")}),
	}}
	output := &pb.SchedulingPolicy{}
	if err := translateNodeAffinity(input, output); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		labels map[string]string
		want   bool
	}{
		{map[string]string{"NODE_ID": "a"}, true},
		{map[string]string{"NODE_ID": "b"}, false},
		{map[string]string{"NODE_ID": "b", "pool": "good"}, true},
		{map[string]string{"NODE_ID": "b", "pool": "blocked"}, false},
		{map[string]string{"NODE_ID": "a", "zone": "bad"}, true},
		{map[string]string{"NODE_ID": "a", "zone": "bad", "disk": "slow"}, false},
		{map[string]string{"NODE_ID": "c", "pool": "good"}, false},
	} {
		if got := matchesRequired(output.RequiredNode, test.labels); got != test.want {
			t.Fatalf("labels %v: got %v want %v", test.labels, got, test.want)
		}
	}
}

func TestPlacementGroupsKeepOrOrderAndWeights(t *testing.T) {
	peer := selection([]*old.LabelExpression{in("app", "db")}, []*old.LabelExpression{in("app", "cache")})
	preferred := selection([]*old.LabelExpression{in("zone", "first")}, []*old.LabelExpression{in("zone", "second")})
	preferred.Condition.OrderPriority = true
	preferred.Condition.SubConditions[1].Weight = 17
	input := &old.Affinity{Resource: &old.ResourceAffinity{PreferredAffinity: preferred}, Instance: &old.InstanceAffinity{RequiredAffinity: peer}}
	output := &pb.SchedulingPolicy{}
	if err := translateNodeAffinity(input, output); err != nil {
		t.Fatal(err)
	}
	if len(output.PlacementGroups) != 2 {
		t.Fatalf("groups=%v", output.PlacementGroups)
	}
	var node, instance *pb.PlacementGroup
	for _, g := range output.PlacementGroups {
		if g.Target == pb.PlacementTarget_PLACEMENT_TARGET_NODE {
			node = g
		} else {
			instance = g
		}
	}
	if node == nil || !node.Ordered || node.Required || node.Terms[1].Weight != 17 {
		t.Fatalf("node=%v", node)
	}
	if instance == nil || !instance.Required || instance.Anti || len(instance.Terms) != 2 {
		t.Fatalf("peer OR lost: %v", instance)
	}
}
