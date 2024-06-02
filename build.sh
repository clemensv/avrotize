# Build the Avrotize project
# This script is intended to be run from the root of the repository

pip install build
python -m build

# Build the VS Code extension
./updatevsce.ps1
pushd vscode\avrotize
vsce package --out ..\..\dist\avrotize.vsix
popd

