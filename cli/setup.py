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

"""Packaging for the Agent Distributed Executor CLI (`adx`)."""

import os
import setuptools

ROOT_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.dirname(ROOT_DIR)


def get_version():
    """Read the repository-wide package version."""
    with open(os.path.join(REPO_ROOT, "VERSION"), "r", encoding="utf-8") as version_file:
        return os.getenv("BUILD_VERSION") or version_file.read().strip()


setuptools.setup(
    name="agent-dx-cli",
    version=get_version(),
    author="openyuanrong",
    description="Agent Distributed Executor (agent-dx) CLI: deploy and invoke agents",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=">=3.9",
    packages=setuptools.find_packages(exclude=("tests", "tests.*")),
    install_requires=[
        "click>=8.1",
        "requests>=2.28",
    ],
    entry_points={
        "console_scripts": [
            "adx=ar_cli.main:main",
        ]
    },
)
