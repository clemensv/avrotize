# Setup Instructions for Structurize Publishing

## Required GitHub Secret

To enable automatic publishing of the `structurize` package to PyPI, you need to add a GitHub repository secret:

### Steps:

1. **Create a PyPI API Token for Structurize:**
   - Go to https://pypi.org/manage/account/token/
   - Click "Add API token"
   - Set the token name: `structurize-github-actions`
   - Set scope to "Project: structurize" (after the first manual upload) or "Entire account" (for initial setup)
   - Copy the token (it starts with `pypi-`)

2. **Add the Secret to GitHub:**
   - Go to your repository on GitHub: https://github.com/clemensv/avrotize
   - Navigate to Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `PYPI_API_KEY_STRUCTURIZE`
   - Value: Paste the PyPI token
   - Click "Add secret"

## First Time Setup

For the very first publication of structurize to PyPI, you may need to:

1. Build the package locally:
   ```bash
   cd structurize
   python -m build
   ```

2. Manually upload once to register the package name:
   ```bash
   python -m twine upload dist/*
   ```

3. After the package exists on PyPI, configure the GitHub secret as described above

4. Future releases will be automatically published when you push version tags

## Project Structure

The structurize package is built from the `structurize/` subdirectory which contains:
- `pyproject.toml` - Package configuration for structurize
- `build.ps1` / `build.sh` - Local build scripts
- References source code from `../avrotize/`

This clean separation means no file swapping is needed during builds.

## Verification

After setting up the secret, the next tagged release (e.g., `v1.2.3`) will automatically:
- Build both `avrotize` and `structurize` packages
- Publish `avrotize` using `PYPI_API_KEY`
- Publish `structurize` using `PYPI_API_KEY_STRUCTURIZE`

Check the Actions tab on GitHub to monitor the deployment workflow.
