#!/usr/bin/env bash
# Check every Python file changed by the PR, including pending local edits.
set -euo pipefail

if [[ $# -ne 1 || -z $1 ]]; then
    echo 'Usage: make python-check PYTHON_CHECK_BASE=<target-branch-ref>' >&2
    exit 2
fi

repo=$(git rev-parse --show-toplevel)
cd "$repo"
base_commit=$(git rev-parse --verify --end-of-options "$1^{commit}")
merge_base=$(git merge-base HEAD "$base_commit")
file_list=$(mktemp)
trap 'rm -f "$file_list"' EXIT
git diff --name-only --diff-filter=ACMR -z "$merge_base" -- '*.py' > "$file_list"
git ls-files --others --exclude-standard -z -- '*.py' >> "$file_list"

files=()
while IFS= read -r -d '' path; do
    if [[ -f $path ]]; then
        files+=("$path")
    fi
done < "$file_list"

if [[ ${#files[@]} -eq 0 ]]; then
    echo "No changed Python files since $merge_base."
    exit 0
fi

echo "Python precheck: ${#files[@]} files since $merge_base"
printf '  %s\n' "${files[@]}"
check_status=0
uv tool run --from ruff==0.11.13 ruff check \
    --config build/ci/ruff-codecheck.toml "${files[@]}" || check_status=1
uv tool run --from pylint==3.3.8 pylint --rcfile=/dev/null \
    --disable=all --enable=redefined-outer-name,duplicate-except \
    --score=n --persistent=n "${files[@]}" || check_status=1
exit "$check_status"
