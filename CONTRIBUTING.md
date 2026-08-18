# Contributing to Avrotize

Thanks for helping improve Avrotize. Bug reports, ideas, documentation fixes,
tests, and code changes are welcome. They do not need to be perfect before you
share them; maintainers can help refine the problem, scope, or validation.

## The short path

1. **Found a problem?** Open the [Bug report](https://github.com/clemensv/avrotize/issues/new?template=bug.yml)
   and describe what you were trying to do and what happened. Share a small
   example if you have one, but do not delay the report if you do not.
2. **Have an idea?** Open a
   [Feature or transformation request](https://github.com/clemensv/avrotize/issues/new?template=feature.yml)
   and describe the outcome or use case you want.
3. **Not sure where it fits?** Use
   [Question or something else](https://github.com/clemensv/avrotize/issues/new?template=question.yml).
4. **Ready with a change?** Open a pull request that says what changed and why,
   and how you checked it. Draft and incomplete pull requests are welcome.

Please remove secrets and sensitive or proprietary data from public examples.
Suspected vulnerabilities belong in the private process in
[SECURITY.md](SECURITY.md).

For a small documentation or test fix, a pull request can be the first step.
For a larger behavior change, an issue first can help avoid wasted effort and
give maintainers a chance to clarify scope. Repository owners make final
decisions about scope, compatibility, merge, and release.

## Helpful detail, when you have it

You can add command names, flags, versions, input and output formats, generated
language or runtime, error text, and a small non-sensitive example. These details
can help, but you can open an issue without them.

If a maintainer later asks for reproduction preparation, they may request the
exact Avrotize command or API surface, the smallest input that shows the
problem, expected and actual behavior, and the relevant environment. The
automation records that information for manual review; it never runs reporter
input or Avrotize commands.

## Checking a change

Run the smallest existing test that covers your change. If you are unsure which
test applies, say what you tried in the pull request and ask for guidance. Common
starting points are:

```powershell
# Targeted Python test
python -m pytest test\test_<affected_area>.py

# Package build
python -m build --sdist --wheel --outdir dist

# VS Code extension
Push-Location vscode\avrotize
npm ci
npm test
Pop-Location
```

Changes to generated output are easier to review with a small source fixture
and the generated result. Changes to a public command, API, schema behavior, or
runtime requirement may also need documentation, tests, a changelog note, or a
migration note; maintainers can help identify the relevant evidence.

The detailed maintainer decision process is in
[GOVERNANCE.md](GOVERNANCE.md). Support routes are in [SUPPORT.md](SUPPORT.md).
