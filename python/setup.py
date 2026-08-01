#!/usr/bin/env python3
"""Build entrypoint; reads the repository-wide version source."""

import os
from pathlib import Path
from setuptools import setup


setup(version=os.getenv("BUILD_VERSION") or (Path(__file__).parent / "VERSION").read_text().strip())
