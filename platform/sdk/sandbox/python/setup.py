"""Keep SDK versioning independent of the monorepo Git tags."""
import os
from pathlib import Path
from setuptools import setup

setup(version=os.getenv("SETUPTOOLS_SCM_PRETEND_VERSION") or os.getenv("BUILD_VERSION") or (Path(__file__).parent / "VERSION").read_text().strip())
