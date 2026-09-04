import os
import shutil
import subprocess
from pathlib import Path

import pytest


def find_jstruct() -> str | None:
    configured = os.environ.get("JSTRUCT")
    if configured:
        return configured

    executable = shutil.which("jstruct")
    if executable:
        return executable

    checkout_root = Path(__file__).resolve().parents[2]
    executable_name = "jstruct.exe" if os.name == "nt" else "jstruct"
    for profile in ("release", "debug"):
        candidate = checkout_root / "json-structure" / "sdk" / "rust" / "target" / profile / executable_name
        if candidate.is_file():
            return str(candidate)
    return None


def test_all_json_structure_schemas_are_valid() -> None:
    jstruct = find_jstruct()
    if not jstruct:
        pytest.fail("jstruct is required; install it or set JSTRUCT to its executable path")

    test_root = Path(__file__).parent
    schemas = sorted(test_root.rglob("*.struct.json"))
    assert schemas, "No JSON Structure schemas found"

    result = subprocess.run(
        [jstruct, "check", "--format", "text", *(str(schema) for schema in schemas)],
        capture_output=True,
        text=True,
        check=False,
    )
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    assert result.returncode == 0, output