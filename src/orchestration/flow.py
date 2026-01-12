from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

def run_step(module_path: str) -> None:
    """
    Runs a python module as: python <module_path>
    Using subprocess keeps each step isolated and mirrors how you'd run tasks in orchestration tools.
    """

    print(f"\n=== Running: {module_path} ===")

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / module_path)],
        cwd = str(REPO_ROOT),
        check=False,
        text=True,
        capture_output=True,
    )

    ## Print stdout / stderr to make debugging easy
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Step failed: {module_path} (exit code {result.returncode})")
    
def main() -> None:
    ## Run pipeline steps in order
    run_step("src/ingest/ingest.py")
    run_step("src/transform/transform.py")
    run_step("src/load/load_postgres.py")

    print("\n Pipeline complete: Bronze -> Silver -> Postgres")

if __name__ == "__main__":
    main()