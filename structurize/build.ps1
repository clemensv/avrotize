#!/usr/bin/env pwsh
# Build script for structurize package

$ErrorActionPreference = "Stop"

Write-Host "Building structurize package..." -ForegroundColor Green

# Get version from git tag in parent directory
Push-Location ..
$version = (git describe --tags --abbrev=0 2>$null) -replace '^v',''
if (-not $version) {
    $version = "0.0.0"
}
Pop-Location
$env:SETUPTOOLS_SCM_PRETEND_VERSION = $version
Write-Host "Using version: $version" -ForegroundColor Cyan

try {
    # Clean previous builds
    Write-Host "Cleaning previous builds..." -ForegroundColor Yellow
    if (Test-Path "dist") {
        Remove-Item -Recurse -Force "dist"
    }
    if (Test-Path "build") {
        Remove-Item -Recurse -Force "build"
    }
    if (Test-Path "structurize.egg-info") {
        Remove-Item -Recurse -Force "structurize.egg-info"
    }
    if (Test-Path "avrotize") {
        Remove-Item -Recurse -Force "avrotize"
    }
    
    # Copy necessary files from parent directory
    Write-Host "Copying source files..." -ForegroundColor Yellow
    Copy-Item "..\LICENSE" "LICENSE" -Force -ErrorAction SilentlyContinue
    Copy-Item "..\avrotize" "avrotize" -Recurse -Force
    
    # Build the package
    Write-Host "Building package..." -ForegroundColor Yellow
    python -m build
    
    Write-Host "Build complete! Package is in dist/" -ForegroundColor Green
}
finally {
    # Clean up copied files
    Write-Host "Cleaning up..." -ForegroundColor Yellow
    Remove-Item "LICENSE" -Force -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force "avrotize" -ErrorAction SilentlyContinue
}
