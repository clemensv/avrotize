#!/bin/bash
# Build script for structurize package

set -e

echo "Building structurize package..."

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    rm -f README.md LICENSE
    rm -rf avrotize
}

trap cleanup EXIT

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf dist build structurize.egg-info avrotize

# Copy necessary files from parent directory
echo "Copying source files..."
cp ../README.md README.md
cp ../LICENSE LICENSE 2>/dev/null || true
cp -r ../avrotize avrotize

# Build the package
echo "Building package..."
python -m build

echo "Build complete! Package is in dist/"
