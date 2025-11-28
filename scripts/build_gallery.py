#!/usr/bin/env python3
"""
Build script for generating gallery content for the GitHub Pages site.

This script runs various avrotize conversions and generates the gallery pages
with file trees and source content for the documentation site.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

# Directories - these are relative to the script location
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent

# Gallery output directory (in the project for local testing)
GALLERY_DIR = PROJECT_ROOT / "gallery"

# Test data directory (source schemas)
TEST_DIR = PROJECT_ROOT / "test"

# Temporary output directory
TMP_DIR = PROJECT_ROOT / "tmp" / "gallery"

# Conversion definitions
GALLERY_ITEMS = [
    # Avrotize examples (via Avro)
    {
        "id": "employee-to-python",
        "title": "Employee Schema → Python",
        "description": "JSON Schema to Python dataclasses via Avro Schema",
        "source_file": "employee.jsons",
        "source_path": TEST_DIR / "jsons" / "employee.jsons",
        "source_language": "json",
        "conversions": [
            {"cmd": "j2a", "args": ["--out", "{out}/employee.avsc"], "output": "employee.avsc"},
            {"cmd": "a2py", "args": ["--out", "{out}/python"], "input": "{out}/employee.avsc"},
        ]
    },
    {
        "id": "employee-to-csharp",
        "title": "Employee Schema → C#",
        "description": "JSON Schema to C# classes via Avro Schema",
        "source_file": "employee.jsons",
        "source_path": TEST_DIR / "jsons" / "employee.jsons",
        "source_language": "json",
        "conversions": [
            {"cmd": "j2a", "args": ["--out", "{out}/employee.avsc"], "output": "employee.avsc"},
            {"cmd": "a2cs", "args": ["--out", "{out}/csharp"], "input": "{out}/employee.avsc"},
        ]
    },
    {
        "id": "employee-to-java",
        "title": "Employee Schema → Java",
        "description": "JSON Schema to Java POJOs via Avro Schema",
        "source_file": "employee.jsons",
        "source_path": TEST_DIR / "jsons" / "employee.jsons",
        "source_language": "json",
        "conversions": [
            {"cmd": "j2a", "args": ["--out", "{out}/employee.avsc"], "output": "employee.avsc"},
            {"cmd": "a2java", "args": ["--out", "{out}/java"], "input": "{out}/employee.avsc"},
        ]
    },
    {
        "id": "person-to-typescript",
        "title": "Person Schema → TypeScript",
        "description": "JSON Schema with unions to TypeScript interfaces",
        "source_file": "person.jsons",
        "source_path": TEST_DIR / "jsons" / "person.jsons",
        "source_language": "json",
        "conversions": [
            {"cmd": "j2a", "args": ["--out", "{out}/person.avsc"], "output": "person.avsc"},
            {"cmd": "a2ts", "args": ["--out", "{out}/typescript"], "input": "{out}/person.avsc"},
        ]
    },
    {
        "id": "xsd-to-avro",
        "title": "ISO 20022 XSD → Avro",
        "description": "Complex XML Schema to Avro Schema conversion",
        "source_file": "acmt.003.001.08.xsd",
        "source_path": TEST_DIR / "xsd" / "acmt.003.001.08.xsd",
        "source_language": "xml",
        "conversions": [
            {"cmd": "x2a", "args": ["--out", "{out}/acmt.avsc"]},
        ]
    },
    {
        "id": "avro-to-proto",
        "title": "Avro → Protobuf",
        "description": "Convert Avro Schema to Protocol Buffers",
        "source_file": "employee.avsc",
        "source_path": None,  # Generated from employee.jsons first
        "source_language": "json",
        "setup": [
            {"cmd": "j2a", "input": TEST_DIR / "jsons" / "employee.jsons", "args": ["--out", "{out}/employee.avsc"]},
        ],
        "conversions": [
            {"cmd": "a2p", "args": ["--out", "{out}/proto"], "input": "{out}/employee.avsc"},
        ]
    },
    # Structurize examples (via JSON Structure)
    {
        "id": "struct-to-csharp",
        "title": "Structure → C#",
        "description": "JSON Structure to C# with validation attributes",
        "source_file": "employee-ref.struct.json",
        "source_path": TEST_DIR / "jsons" / "employee-ref.struct.json",
        "source_language": "json",
        "conversions": [
            {"cmd": "s2cs", "args": ["--out", "{out}/csharp"]},
        ]
    },
    {
        "id": "struct-to-python",
        "title": "Structure → Python",
        "description": "JSON Structure to Python dataclasses",
        "source_file": "employee-ref.struct.json",
        "source_path": TEST_DIR / "jsons" / "employee-ref.struct.json",
        "source_language": "json",
        "conversions": [
            {"cmd": "s2py", "args": ["--out", "{out}/python"]},
        ]
    },
    {
        "id": "struct-to-go",
        "title": "Structure → Go",
        "description": "JSON Structure to Go structs with JSON tags",
        "source_file": "employee-ref.struct.json",
        "source_path": TEST_DIR / "jsons" / "employee-ref.struct.json",
        "source_language": "json",
        "conversions": [
            {"cmd": "s2go", "args": ["--out", "{out}/go"]},
        ]
    },
    {
        "id": "struct-to-rust",
        "title": "Structure → Rust",
        "description": "JSON Structure to Rust structs with serde derives",
        "source_file": "employee-ref.struct.json",
        "source_path": TEST_DIR / "jsons" / "employee-ref.struct.json",
        "source_language": "json",
        "conversions": [
            {"cmd": "s2rust", "args": ["--out", "{out}/rust"]},
        ]
    },
    {
        "id": "struct-to-xsd",
        "title": "Structure → XSD",
        "description": "JSON Structure to XML Schema",
        "source_file": "employee-ref.struct.json",
        "source_path": TEST_DIR / "jsons" / "employee-ref.struct.json",
        "source_language": "json",
        "conversions": [
            {"cmd": "s2x", "args": ["--out", "{out}/employee.xsd"]},
        ]
    },
    {
        "id": "struct-to-graphql",
        "title": "Structure → GraphQL",
        "description": "JSON Structure to GraphQL type definitions",
        "source_file": "employee-ref.struct.json",
        "source_path": TEST_DIR / "jsons" / "employee-ref.struct.json",
        "source_language": "json",
        "conversions": [
            {"cmd": "struct2gql", "args": ["--out", "{out}/schema.graphql"]},
        ]
    },
]


def run_avrotize(cmd: str, input_file: Path | str, args: list[str], cwd: Path) -> bool:
    """Run an avrotize command."""
    full_cmd = ["avrotize", cmd, str(input_file)] + args
    print(f"  Running: {' '.join(full_cmd)}")
    try:
        result = subprocess.run(full_cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"  Error: {result.stderr}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print("  Error: Command timed out")
        return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def build_file_tree(directory: Path, base_path: Path) -> list[dict[str, Any]]:
    """Build a file tree structure from a directory."""
    tree = []
    
    if not directory.exists():
        return tree
    
    items = sorted(directory.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    
    for item in items:
        rel_path = item.relative_to(base_path)
        
        if item.is_dir():
            children = build_file_tree(item, base_path)
            if children:  # Only include non-empty directories
                tree.append({
                    "name": item.name,
                    "type": "folder",
                    "path": str(rel_path),
                    "children": children
                })
        else:
            tree.append({
                "name": item.name,
                "type": "file",
                "path": str(rel_path)
            })
    
    return tree


def get_language_for_extension(ext: str) -> str:
    """Get Prism.js language identifier for a file extension."""
    lang_map = {
        ".json": "json",
        ".avsc": "json",
        ".py": "python",
        ".cs": "csharp",
        ".java": "java",
        ".ts": "typescript",
        ".js": "javascript",
        ".go": "go",
        ".rs": "rust",
        ".cpp": "cpp",
        ".hpp": "cpp",
        ".h": "c",
        ".proto": "protobuf",
        ".sql": "sql",
        ".xsd": "xml",
        ".xml": "xml",
        ".md": "markdown",
        ".graphql": "graphql",
        ".gql": "graphql",
    }
    return lang_map.get(ext.lower(), "plaintext")


def escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))


def render_file_tree_html(tree: list[dict], base_url: str, indent: int = 0) -> str:
    """Render file tree as HTML."""
    html_parts = []
    
    for item in tree:
        if item["type"] == "folder":
            html_parts.append(f'''
<div class="tree-item folder expanded" style="padding-left: {indent * 16}px;">
  <span class="tree-icon">📁</span>
  <span class="tree-name">{escape_html(item["name"])}</span>
</div>
<div class="tree-children">
{render_file_tree_html(item["children"], base_url, indent + 1)}
</div>''')
        else:
            ext = Path(item["name"]).suffix
            icon = get_file_icon(ext)
            file_url = f"{base_url}/{item['path']}"
            html_parts.append(f'''
<div class="tree-item file" data-path="{file_url}" data-lang="{get_language_for_extension(ext)}" style="padding-left: {indent * 16}px;">
  <span class="tree-icon">{icon}</span>
  <span class="tree-name">{escape_html(item["name"])}</span>
</div>''')
    
    return "".join(html_parts)


def get_file_icon(ext: str) -> str:
    """Get emoji icon for a file extension."""
    icon_map = {
        ".json": "📄",
        ".avsc": "📋",
        ".py": "🐍",
        ".cs": "💜",
        ".java": "☕",
        ".ts": "💙",
        ".js": "💛",
        ".go": "🔵",
        ".rs": "🦀",
        ".cpp": "⚙️",
        ".hpp": "⚙️",
        ".proto": "📝",
        ".sql": "🗃️",
        ".xsd": "📐",
        ".xml": "📐",
        ".graphql": "◼️",
        ".gql": "◼️",
    }
    return icon_map.get(ext.lower(), "📄")


def generate_gallery_page(item: dict, output_dir: Path, files_base_url: str) -> None:
    """Generate a gallery page for an item."""
    page_dir = GALLERY_DIR / item["id"]
    page_dir.mkdir(parents=True, exist_ok=True)
    
    # Read source content
    source_path = item.get("source_path")
    if source_path and source_path.exists():
        source_content = source_path.read_text(encoding="utf-8")
    elif (output_dir / item["source_file"]).exists():
        source_content = (output_dir / item["source_file"]).read_text(encoding="utf-8")
    else:
        source_content = "# Source file not found"
    
    # Build file tree
    file_tree = build_file_tree(output_dir, output_dir)
    file_tree_html = render_file_tree_html(file_tree, files_base_url)
    
    # Generate the page
    page_content = f'''---
layout: gallery-viewer
title: "{item['title']}"
description: "{item['description']}"
source_file: "{item['source_file']}"
source_language: "{item['source_language']}"
permalink: /gallery/{item['id']}/
---

{file_tree_html}

<script>
// Store source content for this gallery item
window.gallerySourceContent = {json.dumps(escape_html(source_content))};
window.galleryFilesBaseUrl = "{files_base_url}";

document.addEventListener('DOMContentLoaded', function() {{
  // Set source content
  const sourcePanel = document.querySelector('.source-panel .panel-content');
  if (sourcePanel) {{
    sourcePanel.innerHTML = '<pre class="line-numbers"><code class="language-{item["source_language"]}">' + window.gallerySourceContent + '</code></pre>';
    if (window.Prism) {{
      Prism.highlightAllUnder(sourcePanel);
    }}
  }}
  
  // Handle file tree clicks
  document.querySelectorAll('.tree-item.file').forEach(function(el) {{
    el.addEventListener('click', async function() {{
      const path = this.dataset.path;
      const lang = this.dataset.lang;
      
      // Update active state
      document.querySelectorAll('.tree-item.active').forEach(function(item) {{
        item.classList.remove('active');
      }});
      this.classList.add('active');
      
      // Update header
      document.getElementById('outputFileName').textContent = '📄 ' + path.split('/').pop();
      
      // Load file content
      try {{
        const response = await fetch(path);
        if (!response.ok) throw new Error('Failed to load file');
        const content = await response.text();
        
        const outputContent = document.getElementById('outputContent');
        outputContent.innerHTML = '<pre class="line-numbers"><code class="language-' + lang + '">' + escapeHtml(content) + '</code></pre>';
        
        if (window.Prism) {{
          Prism.highlightAllUnder(outputContent);
        }}
      }} catch (error) {{
        document.getElementById('outputContent').innerHTML = '<div style="padding: 20px; color: var(--color-text-muted);">Failed to load file</div>';
      }}
    }});
  }});
  
  // Auto-select first file
  const firstFile = document.querySelector('.tree-item.file');
  if (firstFile) {{
    firstFile.click();
  }}
}});

function escapeHtml(text) {{
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}}
</script>
'''
    
    (page_dir / "index.html").write_text(page_content, encoding="utf-8")
    print(f"  Generated page: {page_dir / 'index.html'}")


def build_gallery() -> None:
    """Build all gallery items."""
    print("Building gallery content...")
    print(f"  Project root: {PROJECT_ROOT}")
    print(f"  Test directory: {TEST_DIR}")
    print(f"  Gallery directory: {GALLERY_DIR}")
    print(f"  Temp directory: {TMP_DIR}")
    
    # Ensure gallery directories exist
    GALLERY_DIR.mkdir(parents=True, exist_ok=True)
    
    for item in GALLERY_ITEMS:
        print(f"\nProcessing: {item['title']}")
        
        # Create temporary output directory
        output_dir = TMP_DIR / item["id"]
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)
        
        # Run setup commands if any
        setup_commands = item.get("setup", [])
        success = True
        for setup in setup_commands:
            input_file = setup.get("input", item["source_path"])
            args = [arg.replace("{out}", str(output_dir)) for arg in setup.get("args", [])]
            if not run_avrotize(setup["cmd"], input_file, args, output_dir):
                print(f"  Setup failed, skipping item")
                success = False
                break
        
        if not success:
            continue
        
        # Run conversion commands
        source_input = item["source_path"]
        for conv in item["conversions"]:
            if "input" in conv:
                source_input = Path(conv["input"].replace("{out}", str(output_dir)))
            
            args = [arg.replace("{out}", str(output_dir)) for arg in conv.get("args", [])]
            
            if not run_avrotize(conv["cmd"], source_input, args, output_dir):
                print(f"  Conversion failed, skipping item")
                success = False
                break
        
        if success:
            # Files base URL for the generated page - use 'files' subdirectory (not _data which Jekyll ignores)
            files_base_url = f"/avrotize/gallery/files/{item['id']}"
            
            # Generate the gallery page
            generate_gallery_page(item, output_dir, files_base_url)
    
    print("\nGallery build complete!")
    print(f"Generated pages are in: {GALLERY_DIR}")
    print(f"Output files are in: {TMP_DIR}")


if __name__ == "__main__":
    build_gallery()
