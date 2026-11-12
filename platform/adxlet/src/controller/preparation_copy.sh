# Executed only inside the temporary guest. Arguments are passed as quoted literals.
set -euo pipefail
staging=$1
source=$2
target=$3
owner=$4
mode=$5
trailing=$6
trap 'rm -rf -- "$staging"; rm -f -- "$staging.tar"' EXIT
mkdir -m 700 -- "$staging"
tar -xf "$staging.tar" -C "$staging" --no-same-owner
# Source is the literal archive prefix; SDK wildcard expansion belongs to the adapter.
entry=$staging/$source
[ -e "$entry" ] || { echo 'COPY source not found' >&2; exit 1; }
if [ "$trailing" = yes ]; then mkdir -p -- "$target"; fi
chown -R -- "$owner" "$entry"
if [ -n "$mode" ]; then chmod -R -- "$mode" "$entry"; fi
if [ -d "$entry" ]; then
    mkdir -p -- "$target"
    # Match E2B directory merge behavior without overwriting destination directory metadata.
    (cd "$entry" && tar -cf - .) | tar -xf - -C "$target" --keep-directory-symlink --no-overwrite-dir
elif [ -d "$target" ]; then
    mv -f -- "$entry" "$target/"
else
    mkdir -p -- "$(dirname -- "$target")"
    rm -f -- "$target"
    mv -- "$entry" "$target"
fi
