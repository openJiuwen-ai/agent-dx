#!/usr/bin/env bash
# R82: a fresh two-Node cluster, both Nodes open the same file for writing (node-b's writes go through the owner
# node-a), then node-b reads it back through the owner. Every read must succeed, the first one included.
set -u
out=$(python3 "$FAULTS/owner_read.py" "$ROOT/mnt" "$ROOT/mnt-b" 2>&1); echo "$out"
n=$(echo "$out" | grep -c ': ok '); bad=$(echo "$out" | grep -vc ': ok ')
echo "R82 owner reads ok=$n other=$bad"
[ "$n" -eq 6 ] && [ "$bad" -eq 0 ]
