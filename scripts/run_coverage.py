#!/usr/bin/env python3
"""
Generate pytest coverage report for PyStreamFlow.
Usage: python scripts/run_coverage.py
"""
import subprocess
import sys
from pathlib import Path

def main():
    repo_root = Path(__file__).resolve().parent.parent
    cmd = [
        sys.executable, "-m", "pytest",
        "tests",
        "--cov=pystreamflow",
        "--cov-report=xml",
        "--cov-report=html",
        "--cov-report=term-missing",
        "-q"
    ]
    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode != 0:
        sys.exit(result.returncode)
    print("\nCoverage report generated:")
    print(" - coverage.xml")
    print(" - coverage_html_report/index.html")

if __name__ == "__main__":
    main()
