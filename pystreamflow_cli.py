#!/usr/bin/env python3
"""
Executable PyStreamFlow CLI script for dev tree.
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from pystreamflow.cli.main import app

if __name__ == "__main__":
    app()
