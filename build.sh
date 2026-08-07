#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

readonly USAGE="
Usage: bash build.sh [-v VERSION] [-o OUTPUT_DIR] [-p PYTHON] [-C] [-h]

Options:
    -v  wheel version, overrides ar_cli.__version__ (via BUILD_VERSION).
    -o  output directory for the built wheel (default: <repo>/output).
    -p  python interpreter to use (default: python3).
    -C  clean build/output/egg-info artifacts, then exit.
    -h  usage.
"

BASE_DIR=$(
    cd "$(dirname "$0")"
    pwd
)

CLI_DIR="$BASE_DIR/cli"          # ar_cli package + setup.py live here
SDK_DIR="$BASE_DIR/python"       # agent-dx SDK package
BUILD_DIR="$BASE_DIR/build"      # intermediate build dir (setup.py -b)
OUTPUT_DIR="$BASE_DIR/output"    # default wheel output dir
PYTHON3_BIN_PATH="python3"
BUILD_VERSION=""
COMMAND="build"

usage() {
    echo -e "$USAGE"
}

while getopts 'v:o:p:Ch' opt; do
    case "$opt" in
    v)
        BUILD_VERSION="${OPTARG}"
        ;;
    o)
        OUTPUT_DIR=$(readlink -f "${OPTARG}")
        ;;
    p)
        PYTHON3_BIN_PATH="${OPTARG}"
        ;;
    C)
        COMMAND="clean"
        ;;
    h)
        usage
        exit 0
        ;;
    *)
        echo "invalid command: -$opt" >&2
        usage
        exit 1
        ;;
    esac
done

if [ "$COMMAND" == "clean" ]; then
    echo "Cleaning build artifacts..."
    rm -rf "$BUILD_DIR" "$OUTPUT_DIR" \
        "$CLI_DIR"/build "$CLI_DIR"/dist "$CLI_DIR"/*.egg-info \
        "$SDK_DIR"/build "$SDK_DIR"/dist "$SDK_DIR"/src/*.egg-info
    exit 0
fi

# -v overrides the version baked into the wheel; otherwise setup.py falls back
# to ar_cli.__version__.
if [ -n "$BUILD_VERSION" ]; then
    export BUILD_VERSION
fi

mkdir -p "$OUTPUT_DIR"

# setup.py is under cli/ and uses find_packages() relative to the cwd, so it
# must run from there; the wheel is written to OUTPUT_DIR via -d.
cd "$CLI_DIR"
"$PYTHON3_BIN_PATH" setup.py bdist_wheel -b "$BUILD_DIR" -d "$OUTPUT_DIR"

cd "$SDK_DIR"
"$PYTHON3_BIN_PATH" setup.py bdist_wheel -b "$BUILD_DIR/sdk" -d "$OUTPUT_DIR"

echo "Build done. Wheel(s) in: $OUTPUT_DIR"
ls -1 "$OUTPUT_DIR"/*.whl 2>/dev/null || true
