"""
conftest.py – Ensures the project root is on sys.path for all tests.
SPDX-FileCopyrightText: 2025 Clemens Vasters
SPDX-License-Identifier: MIT
"""
import sys
import os
from pathlib import Path

# Always prepend the project root to sys.path for local package imports
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
