# The Klok fork of avrotize

This repository is `klok-opensource/klok-avrotize`, a fork of
[`clemensv/avrotize`](https://github.com/clemensv/avrotize). Everything outside this file is
upstream's, and upstream is where features and bug fixes belong. This file describes what Klok
adds on top, how the fork is consumed, and how to sync it.

## Why the fork exists

Klok generates its datamodels from XSD: the datamodel pipeline runs `avrotize x2a` to produce an
Avro schema and `avrotize a2java` to produce a Java project, which is then built and published
with Gradle. Upstream `a2java` emits a Maven project. Klok needs a **Gradle** project wired to the
Klok Gradle plugin and the Klok dependency catalog, so the fork adds that.

The fork also lets Klok pick up an Avro or Jackson security fix without waiting for an upstream
release.

## What Klok changes

The entire Klok delta is **two additive blocks in `avrotize/avrotojava.py`**. Nothing else in the
repository is modified, and nothing is deleted.

1. The constants `BUILD_GRADLE_CONTENT`, `SETTINGS_GRADLE_CONTENT` and
   `KLOK_LIBS_VERSIONS_TOML_CONTENT`, defined just after upstream's `POM_CONTENT`. They describe a
   Klok datamodel project: `alias klokLibs.plugins.gradle`, `projectType = ProjectType.DATAMODEL`,
   and `projectVisibility` read from the `KLOK_PROJECT_VISIBILITY` environment variable.
2. A block in `AvroToJava.convert_schema` that writes `build.gradle`, `settings.gradle` and
   `gradle/klok-libs.versions.toml` into the generated project, each guarded by
   `if not os.path.exists`, so a project that already has them is left alone.

Keep it that way. A change that could live upstream should be sent upstream instead, because
every line Klok adds here is a line that has to survive the next sync.

The versions in `KLOK_LIBS_VERSIONS_TOML_CONTENT` are a seed, not a source of truth: the datamodel
pipeline runs `gradle updateKlokLibsToml` straight afterwards, which replaces the file with the
live dependency catalog. The one exception is `klokGradlePluginVersion`, which has to resolve
before that task can run.

## How the fork is consumed

Klok does **not** install this from PyPI, and does not publish it anywhere.

```
klok/environment/build-runner-docker-images
  └── avrotize/                     git submodule, pinned to a commit on this fork's master
      → klok-build-runner.Dockerfile  COPY avrotize /prg/avrotize && pip install /prg/avrotize/
        → repo.klok.dev/.../klok-build-runner:latest
          → klok/environment/build-ci-templates/build-template-datamodel.yml
            → every datamodel repository's pipeline
```

Only three subcommands are used, with only `--out` and `--package`: `x2a`, `j2a` and `a2java`.

Two consequences worth remembering. The submodule is a plain commit pin with no `branch =` key, so
bumping it is a deliberate, manual commit in `build-runner-docker-images`. And every CI template
consumes the floating `:latest` tag, so the moment that pin bump merges, the new avrotize is live
for **every** datamodel pipeline at once — there is no per-project opt-in.

## Syncing with upstream

```bash
git remote add upstream https://github.com/clemensv/avrotize.git   # once
git fetch upstream
git switch -c agent/<task-id> origin/master
git merge upstream/master
```

Conflicts should only ever appear in `avrotize/avrotojava.py`. Take upstream verbatim everywhere
else; in that file, keep both sides. If upstream restructured `convert_schema`, re-attach the Klok
writes to the new structure rather than reverting upstream's refactor.

Then check that generated output has not drifted: run `x2a` and `a2java` over a real Klok datamodel
XSD with both the old pin and the merged tree, and diff the two trees. A difference in the
generated `build.gradle`, `settings.gradle`, `gradle/klok-libs.versions.toml` or the Java sources
will reach every datamodel pipeline, so it belongs in the pull request description rather than in a
red pipeline later.

Open a pull request. Do not push to `master`.

## CI in this fork

Upstream's workflows are written for a project that publishes a PyPI package, a VS Code extension
and an MCP server, and that runs its own issue governance. Klok publishes none of those. Those
workflows were left enabled after an earlier sync and their failures blocked a finished Klok
branch from being merged, so in this fork they are disabled: each keeps its original `on:` block
commented out above a `workflow_dispatch:`-only trigger, which documents the change and makes an
upstream edit to those triggers show up as a conflict.

| Workflow | Runs on pull requests | Why |
|---|---|---|
| `klok-verify.yml` | **yes** | The Java and XSD generators, plus an end-to-end `x2a` → `a2java` that asserts the Klok Gradle project is still emitted. This is the check that matters. |
| `python-runtime-versions-test.yml` | **yes** | The wheel builds, imports and the CLI works on Python 3.10–3.14. |
| `build_deploy.yml` | no | Publishes to PyPI and the VS Code marketplace, and runs a 14-way generator matrix for languages Klok does not consume. |
| `validate-mcp-server-json.yml` | no | Klok does not publish the MCP server. |
| `governance-ci.yml`, `governance-observe.yml` | no | Upstream's project governance. |
| `issue-intake.yml`, `repro-bug.yml`, `dependabot-intake.yml` | no | Upstream's Copilot-driven triage; needs credits and labels this fork does not have. |

If Klok ever starts depending on another generator, add it to `klok-verify.yml` rather than
re-enabling upstream's matrix.
