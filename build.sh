#!/usr/bin/env bash
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

# Agent v2 原生构建；Sandbox SDK 打包使用 make package。
set -euo pipefail
cd "$(dirname "$0")"
case "${1:-}" in
  '') exec "${CARGO:-cargo}" build --locked -p adx-agent-core -p adx-agent-store -p adx-activator -p adx-agent-api -p data-plane-gateway -p adx-apiserver --features data-plane-gateway/agent-api -j "${JOBS:-2}" ;;
  -t) exec make agent-test ;;
  -h|--help) echo '用法：bash build.sh [-t|-h]；默认构建 Rust Agent，-t 执行组件测试。Sandbox SDK 打包使用 make package。' ;;
  *) echo '旧 Agent Python 打包入口已删除；使用 make package 打包 Sandbox SDK。' >&2; exit 2 ;;
esac
