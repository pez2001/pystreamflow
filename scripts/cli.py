#!/usr/bin/env python3
"""
PyStreamFlow CLI entry point for development tree.
Run as: python scripts/cli.py <command>
"""
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pystreamflow.cli.main import app

if __name__ == "__main__":
    app()
