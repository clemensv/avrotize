import sys, json, os
# Ensure repo root on path
repo_root = r"C:\git\avrotize"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
from avrotize.mcp_server import _execute_conversion
schema = "; Simple CDDL\nperson = {\n    name: tstr\n    age: uint\n}\n"
try:
    result = _execute_conversion(command_name='cddl2s', input_content=schema)
    print(json.dumps(result, ensure_ascii=False, indent=2))
except Exception as e:
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
