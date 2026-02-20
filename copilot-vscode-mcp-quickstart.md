# Quick Start: Using Avrotize MCP with GitHub Copilot in VS Code

This quick guide shows how to use `#avrotize-mcp` from Copilot Chat in VS Code to run schema conversions (like `json2s`) directly from natural language.

## What this enables

With the Avrotize MCP server connected, Copilot can:

- Discover available conversions (`list_conversions`)
- Inspect command details (`get_conversion`)
- Run conversions (`run_conversion`)

That means you can ask for tasks like:

- infer JSON Structure schema from JSON samples
- convert Avro ↔ JSON Schema/Proto/XSD
- generate output files directly from chat instructions

## Prerequisites

- VS Code with GitHub Copilot Chat enabled
- Python 3.10+
- Avrotize with MCP support installed:

```powershell
python -m pip install --upgrade "avrotize[mcp]"
```

Recommended version: `3.4.3+`.

## Register Avrotize MCP in VS Code

Add this MCP server configuration in your VS Code MCP settings (or through the MCP UI):

```json
{
  "mcpServers": {
    "avrotize-mcp": {
      "command": "python",
      "args": ["-m", "avrotize", "mcp"]
    }
  }
}
```

On Windows, if `python` is not on `PATH`, use a full Python path, for example:

```json
{
  "mcpServers": {
    "avrotize-mcp": {
      "command": "C:/Users/<you>/AppData/Local/Programs/Python/Python314/python.exe",
      "args": ["-m", "avrotize", "mcp"]
    }
  }
}
```

Then reload VS Code.

## Example prompt in Copilot Chat

Use a prompt like:

```text
infer json structure schema from #file:address.json with #avrotize-mcp
```

Or specify output and options:

```text
infer json structure schema from #file:address.json with #avrotize-mcp
save as address.jsons and use type-name Address
```

## What happens under the hood

Copilot typically performs this flow:

1. `describe_capabilities`
2. `get_conversion` / `list_conversions`
3. `run_conversion` with the right command (`json2s` in this case)

For `json2s`, both patterns are supported:

- `input_path`: pass file path(s)
- `input_content`: pass inline JSON content

## Tips for best results

- Keep prompts explicit about input file and desired output file.
- Provide `type-name` when you want stable root type naming.
- If you want the response content in chat, omit output file path.
- If you want an artifact on disk, include output file path/name.

## Troubleshooting

### “At least one input file is required”

Usually means an older Avrotize MCP runtime is being used. Upgrade and restart:

```powershell
python -m pip install --upgrade "avrotize[mcp]"
```

Then restart VS Code (or reconnect the MCP server).

### Copilot can’t find `#avrotize-mcp`

- Verify MCP config name is exactly `avrotize-mcp`
- Ensure the server starts without Python/module errors
- Reload VS Code after config changes

### Python not on PATH

Use absolute Python executable path in MCP config.

---

If you need deeper examples, see `README.md` and `server.json` in this repo.