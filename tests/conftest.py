import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


REPO_ROOT = Path.cwd()

if REPO_ROOT.name == "src":
    REPO_ROOT = REPO_ROOT.parent

SRC_PATH = REPO_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()