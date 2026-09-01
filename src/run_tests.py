import os
import sys
from pathlib import Path

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True

import pytest


def main():
    repo_root = Path.cwd()

    if repo_root.name == "src":
        repo_root = repo_root.parent

    os.chdir(repo_root)

    exit_code = pytest.main([
        "-q",
        "--assert=plain",
        "tests",
    ])

    if exit_code != 0:
        raise RuntimeError(
            f"Tests failed with pytest exit code: {exit_code}"
        )

    print("All tests passed successfully")


if __name__ == "__main__":
    main()