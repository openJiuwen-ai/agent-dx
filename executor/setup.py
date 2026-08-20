#!/usr/bin/env python3
# coding=UTF-8
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

"""Build entrypoint for the platform-owned Agent executor package."""

import os
from pathlib import Path

from setuptools import Distribution, setup
from wheel.bdist_wheel import bdist_wheel


class PlatlibDistribution(Distribution):
    """Install this pure-Python package through the platform library scheme."""

    def has_ext_modules(self):
        return True


class PlatlibWheel(bdist_wheel):
    """Keep the platlib wheel portable across Python versions and platforms."""

    def get_tag(self):
        return "py3", "none", "any"


version_file = Path(__file__).resolve().parents[1] / "VERSION"
setup(
    version=os.getenv("BUILD_VERSION") or version_file.read_text(encoding="utf-8").strip(),
    distclass=PlatlibDistribution,
    cmdclass={"bdist_wheel": PlatlibWheel},
)
