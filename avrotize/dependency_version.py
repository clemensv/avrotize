"""
Central dependency version resolver for Avrotize code generation templates.

This module provides functions to read dependency versions from central project files
located in avrotize/dependencies/{language}/{runtime_version}/. These files are
monitored by Dependabot, which proposes updates when new versions are available.

Supported languages:
- cs: C# (.NET) - reads from dependencies.csproj
- java: Java - reads from pom.xml
- py: Python - reads from requirements.txt
- ts: TypeScript - reads from package.json
- go: Go - reads from go.mod
- rust: Rust - reads from Cargo.toml
- cpp: C++ - reads from vcpkg.json
"""

import json
import os
import re
import xml.etree.ElementTree as ET
from typing import Optional


def get_dependency(language: str, runtime_version: str, dependency_name: str) -> str:
    """
    Get the dependency declaration for a specific language and dependency.
    
    Args:
        language: Language identifier ('cs', 'java', 'py/python', 'ts/typescript', 'go', 'rust', 'cpp')
        runtime_version: Runtime version identifier (e.g., 'net90', 'jdk21', 'py312')
        dependency_name: The dependency identifier. For Java, can use 'groupId:artifactId' 
                        syntax to disambiguate (e.g., 'org.junit.jupiter:junit-jupiter')
    
    Returns:
        The dependency declaration string in the appropriate format for the language.
        
    Raises:
        ValueError: If the language is unsupported or dependency is not found.
    """
    # Determine the path to the central dependency file
    deps_dir = os.path.join(os.path.dirname(__file__), 'dependencies')
    
    # Normalize language aliases
    lang = language.lower()
    if lang in ('python', 'py'):
        lang = 'py'
    elif lang in ('typescript', 'ts'):
        lang = 'ts'
    elif lang in ('csharp', 'c#'):
        lang = 'cs'
    
    if lang == 'cs':
        master_project_path = os.path.join(deps_dir, f'cs/{runtime_version}/dependencies.csproj')
    elif lang == 'java':
        master_project_path = os.path.join(deps_dir, f'java/{runtime_version}/pom.xml')
    elif lang == 'py':
        master_project_path = os.path.join(deps_dir, f'python/{runtime_version}/requirements.txt')
    elif lang == 'ts':
        master_project_path = os.path.join(deps_dir, f'typescript/{runtime_version}/package.json')
    elif lang == 'go':
        master_project_path = os.path.join(deps_dir, f'go/{runtime_version}/go.mod')
    elif lang == 'rust':
        master_project_path = os.path.join(deps_dir, f'rust/{runtime_version}/Cargo.toml')
    elif lang == 'cpp':
        master_project_path = os.path.join(deps_dir, f'cpp/{runtime_version}/vcpkg.json')
    else:
        raise ValueError(f"Unsupported language: {language}")
    
    if not os.path.exists(master_project_path):
        raise ValueError(f"Dependency file not found: {master_project_path}")
    
    if lang == 'cs':
        return _get_nuget_dependency(master_project_path, dependency_name)
    elif lang == 'java':
        return _get_maven_dependency(master_project_path, dependency_name)
    elif lang == 'py':
        return _get_python_dependency(master_project_path, dependency_name)
    elif lang == 'ts':
        return _get_npm_dependency(master_project_path, dependency_name)
    elif lang == 'go':
        return _get_go_dependency(master_project_path, dependency_name)
    elif lang == 'rust':
        return _get_cargo_dependency(master_project_path, dependency_name)
    elif lang == 'cpp':
        return _get_vcpkg_dependency(master_project_path, dependency_name)
    
    raise ValueError(f"Unsupported language: {language}")


def get_dependency_version(language: str, runtime_version: str, dependency_name: str) -> str:
    """
    Get just the version string for a dependency.
    
    Args:
        language: Language identifier (cs, java, py/python, ts/typescript, go, rust, cpp)
        runtime_version: Runtime version identifier
        dependency_name: The dependency identifier
        
    Returns:
        The version string only (e.g., '1.12.0', '2.18.2')
    """
    deps_dir = os.path.join(os.path.dirname(__file__), 'dependencies')
    
    # Normalize language aliases
    lang = language.lower()
    if lang in ('python', 'py'):
        lang = 'py'
    elif lang in ('typescript', 'ts'):
        lang = 'ts'
    elif lang in ('csharp', 'c#'):
        lang = 'cs'
    
    if lang == 'cs':
        master_project_path = os.path.join(deps_dir, f'cs/{runtime_version}/dependencies.csproj')
        return _get_nuget_version(master_project_path, dependency_name)
    elif lang == 'java':
        master_project_path = os.path.join(deps_dir, f'java/{runtime_version}/pom.xml')
        return _get_maven_version(master_project_path, dependency_name)
    elif lang == 'py':
        master_project_path = os.path.join(deps_dir, f'python/{runtime_version}/requirements.txt')
        return _get_python_version(master_project_path, dependency_name)
    elif lang == 'ts':
        master_project_path = os.path.join(deps_dir, f'typescript/{runtime_version}/package.json')
        return _get_npm_version(master_project_path, dependency_name)
    elif lang == 'go':
        master_project_path = os.path.join(deps_dir, f'go/{runtime_version}/go.mod')
        return _get_go_version(master_project_path, dependency_name)
    elif lang == 'rust':
        master_project_path = os.path.join(deps_dir, f'rust/{runtime_version}/Cargo.toml')
        return _get_cargo_version(master_project_path, dependency_name)
    elif lang == 'cpp':
        master_project_path = os.path.join(deps_dir, f'cpp/{runtime_version}/vcpkg.json')
        return _get_vcpkg_version(master_project_path, dependency_name)
    
    raise ValueError(f"Unsupported language: {language}")


def _get_nuget_dependency(project_path: str, package_name: str) -> str:
    """Get NuGet PackageReference XML element."""
    tree = ET.parse(project_path)
    root = tree.getroot()
    
    for package in root.findall(".//PackageReference"):
        if package.get('Include') == package_name:
            return ET.tostring(package, encoding='unicode').strip()
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_nuget_version(project_path: str, package_name: str) -> str:
    """Get NuGet package version."""
    tree = ET.parse(project_path)
    root = tree.getroot()
    
    for package in root.findall(".//PackageReference"):
        if package.get('Include') == package_name:
            return package.get('Version', '')
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_maven_dependency(project_path: str, dependency_name: str) -> str:
    """Get Maven dependency XML element."""
    tree = ET.parse(project_path)
    root = tree.getroot()
    
    # Support groupId:artifactId syntax for disambiguation
    if ':' in dependency_name:
        target_group_id, target_artifact_id = dependency_name.split(':', 1)
    else:
        target_group_id = None
        target_artifact_id = dependency_name
    
    for dependency in root.findall(".//dependency"):
        artifact_id = dependency.find('artifactId')
        group_id = dependency.find('groupId')
        
        if artifact_id is not None and artifact_id.text == target_artifact_id:
            # If groupId was specified, verify it matches
            if target_group_id is not None:
                if group_id is None or group_id.text != target_group_id:
                    continue
            return ET.tostring(dependency, encoding='unicode').strip()
    
    raise ValueError(f"Dependency '{dependency_name}' not found in {project_path}")


def _get_maven_version(project_path: str, dependency_name: str) -> str:
    """Get Maven dependency version."""
    tree = ET.parse(project_path)
    root = tree.getroot()
    
    if ':' in dependency_name:
        target_group_id, target_artifact_id = dependency_name.split(':', 1)
    else:
        target_group_id = None
        target_artifact_id = dependency_name
    
    for dependency in root.findall(".//dependency"):
        artifact_id = dependency.find('artifactId')
        group_id = dependency.find('groupId')
        
        if artifact_id is not None and artifact_id.text == target_artifact_id:
            if target_group_id is not None:
                if group_id is None or group_id.text != target_group_id:
                    continue
            version_elem = dependency.find('version')
            if version_elem is not None and version_elem.text:
                return version_elem.text
    
    raise ValueError(f"Dependency '{dependency_name}' not found in {project_path}")


def _get_python_dependency(project_path: str, package_name: str) -> str:
    """Get Python dependency from requirements.txt."""
    with open(project_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for line in content.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        # Match package name with version specifier
        match = re.match(r'^([a-zA-Z0-9_-]+)\s*([>=<!=]+.*)$', line)
        if match:
            pkg_name = match.group(1)
            version_spec = match.group(2)
            if pkg_name == package_name:
                return f"{pkg_name}{version_spec}"
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_python_version(project_path: str, package_name: str) -> str:
    """Get Python package version."""
    with open(project_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for line in content.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        match = re.match(r'^([a-zA-Z0-9_-]+)\s*[>=<!=]+\s*(.+)$', line)
        if match:
            pkg_name = match.group(1)
            version = match.group(2).strip()
            if pkg_name == package_name:
                return version
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_npm_dependency(project_path: str, package_name: str) -> str:
    """Get npm dependency as JSON key-value pair."""
    with open(project_path, 'r', encoding='utf-8') as f:
        package_data = json.load(f)
    
    dependencies = package_data.get('dependencies', {})
    dev_dependencies = package_data.get('devDependencies', {})
    
    if package_name in dependencies:
        version = dependencies[package_name]
        return f'"{package_name}": "{version}"'
    
    if package_name in dev_dependencies:
        version = dev_dependencies[package_name]
        return f'"{package_name}": "{version}"'
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_npm_version(project_path: str, package_name: str) -> str:
    """Get npm package version."""
    with open(project_path, 'r', encoding='utf-8') as f:
        package_data = json.load(f)
    
    dependencies = package_data.get('dependencies', {})
    dev_dependencies = package_data.get('devDependencies', {})
    
    if package_name in dependencies:
        return dependencies[package_name].lstrip('^~')
    
    if package_name in dev_dependencies:
        return dev_dependencies[package_name].lstrip('^~')
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_go_dependency(project_path: str, module_name: str) -> str:
    """Get Go module dependency line."""
    with open(project_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Parse require block
    require_pattern = re.compile(r'require\s*\(\s*([\s\S]*?)\s*\)', re.MULTILINE)
    require_match = require_pattern.search(content)
    
    if require_match:
        require_block = require_match.group(1)
        for line in require_block.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('//'):
                continue
            parts = line.split()
            if len(parts) >= 2:
                mod_path = parts[0]
                version = parts[1]
                if mod_path == module_name:
                    return f"\t{mod_path} {version}"
    
    raise ValueError(f"Dependency '{module_name}' not found in {project_path}")


def _get_go_version(project_path: str, module_name: str) -> str:
    """Get Go module version."""
    with open(project_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    require_pattern = re.compile(r'require\s*\(\s*([\s\S]*?)\s*\)', re.MULTILINE)
    require_match = require_pattern.search(content)
    
    if require_match:
        require_block = require_match.group(1)
        for line in require_block.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('//'):
                continue
            parts = line.split()
            if len(parts) >= 2:
                mod_path = parts[0]
                version = parts[1]
                if mod_path == module_name:
                    return version.lstrip('v')
    
    raise ValueError(f"Dependency '{module_name}' not found in {project_path}")


def _get_cargo_dependency(project_path: str, crate_name: str) -> str:
    """Get Rust/Cargo dependency as TOML line."""
    with open(project_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Look for dependencies in [dependencies] section
    in_deps_section = False
    for line in content.split('\n'):
        line_stripped = line.strip()
        
        if line_stripped == '[dependencies]':
            in_deps_section = True
            continue
        elif line_stripped.startswith('[') and line_stripped.endswith(']'):
            in_deps_section = False
            continue
        
        if in_deps_section and line_stripped:
            # Match simple version: crate = "version"
            simple_match = re.match(rf'^{re.escape(crate_name)}\s*=\s*"([^"]+)"', line_stripped)
            if simple_match:
                return line_stripped
            
            # Match complex version: crate = { version = "...", ... }
            complex_match = re.match(rf'^{re.escape(crate_name)}\s*=\s*\{{.*\}}', line_stripped)
            if complex_match:
                return line_stripped
    
    raise ValueError(f"Dependency '{crate_name}' not found in {project_path}")


def _get_cargo_version(project_path: str, crate_name: str) -> str:
    """Get Rust/Cargo crate version."""
    with open(project_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    in_deps_section = False
    for line in content.split('\n'):
        line_stripped = line.strip()
        
        if line_stripped == '[dependencies]':
            in_deps_section = True
            continue
        elif line_stripped.startswith('[') and line_stripped.endswith(']'):
            in_deps_section = False
            continue
        
        if in_deps_section and line_stripped:
            # Match simple version
            simple_match = re.match(rf'^{re.escape(crate_name)}\s*=\s*"([^"]+)"', line_stripped)
            if simple_match:
                return simple_match.group(1)
            
            # Match complex version with version key
            version_match = re.search(rf'^{re.escape(crate_name)}\s*=\s*\{{.*version\s*=\s*"([^"]+)"', line_stripped)
            if version_match:
                return version_match.group(1)
    
    raise ValueError(f"Dependency '{crate_name}' not found in {project_path}")


def _get_vcpkg_dependency(project_path: str, package_name: str) -> str:
    """Get vcpkg dependency as JSON object."""
    with open(project_path, 'r', encoding='utf-8') as f:
        vcpkg_data = json.load(f)
    
    dependencies = vcpkg_data.get('dependencies', [])
    for dep in dependencies:
        if isinstance(dep, dict):
            if dep.get('name') == package_name:
                return json.dumps(dep, indent=2)
        elif isinstance(dep, str) and dep == package_name:
            return f'"{package_name}"'
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")


def _get_vcpkg_version(project_path: str, package_name: str) -> str:
    """Get vcpkg package version."""
    with open(project_path, 'r', encoding='utf-8') as f:
        vcpkg_data = json.load(f)
    
    dependencies = vcpkg_data.get('dependencies', [])
    for dep in dependencies:
        if isinstance(dep, dict):
            if dep.get('name') == package_name:
                # Try various version keys
                for version_key in ['version>=', 'version', 'version>']:
                    if version_key in dep:
                        return dep[version_key]
    
    raise ValueError(f"Dependency '{package_name}' not found in {project_path}")
