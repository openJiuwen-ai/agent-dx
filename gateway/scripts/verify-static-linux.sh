#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -eq 0 ]; then
    echo "usage: $0 <binary> [binary ...]" >&2
    exit 2
fi

for binary in "$@"; do
    [ -x "$binary" ] || {
        echo "missing executable: $binary" >&2
        exit 1
    }

    if command -v readelf >/dev/null 2>&1; then
        if ! readelf -h "$binary" >/dev/null 2>&1; then
            echo "data-plane binary must be a Linux ELF: $binary" >&2
            exit 1
        fi
        if readelf -l "$binary" | grep -q 'INTERP'; then
            echo "static binary unexpectedly has an ELF interpreter: $binary" >&2
            exit 1
        fi
        if readelf -d "$binary" 2>/dev/null | grep -q '(NEEDED)'; then
            echo "static binary unexpectedly has shared-library dependencies: $binary" >&2
            exit 1
        fi
    elif command -v file >/dev/null 2>&1; then
        description=$(file "$binary")
        case "$description" in
            *ELF*statically\ linked*) ;;
            *)
                echo "data-plane binary must be a statically linked Linux ELF: $description" >&2
                exit 1
                ;;
        esac
    else
        if [ "${ADX_STATIC_VERIFY_ALLOW_MISSING_TOOLS:-0}" = "1" ]; then
            echo "warning: skipping redundant ELF inspection because readelf and file are unavailable: $binary" >&2
        else
            echo "readelf or file is required to verify static data-plane binaries" >&2
            exit 1
        fi
    fi
done
