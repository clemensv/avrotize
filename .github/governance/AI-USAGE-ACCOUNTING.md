# AI Usage Accounting

Avrotize treats AIC only as the usage quantity reported by the GitHub/Copilot
execution platform. Repository workflows, documentation, and reviewers do not
calculate, normalize, estimate, or infer AIC from token counts or other inputs.

## Required run telemetry

For every AI-assisted logical run, including retries and descendant agents,
record when the platform exposes the data:

- GitHub/Copilot platform run and parent-run identifiers;
- workflow responsibility, exact repository revision, prompt version, and model;
- platform-reported AIC for the complete logical run;
- invocation, retry, and descendant counts;
- fresh input, cache-read, cache-write, output, and reasoning token counts as
  operational telemetry only;
- platform-configured per-run and daily AIC guardrails;
- completion, guardrail exhaustion, or escalation disposition.

Token counts remain separate operational signals. They are never used to derive
or adjust AIC.

## Projection policy

Issue intake makes one bounded Copilot CLI request when deterministic preflight
does not find an injection indicator or an oversized input. The observed sample
size is still zero because the workflow has not run after merge, so P50 and P95
remain `TBD`.

Future workflow projections must use observed platform-reported run
distributions:

| Responsibility | Sample size | Observed P50 AIC | Observed P95 AIC | Per-run guardrail | Daily guardrail |
| --- | ---: | ---: | ---: | ---: | ---: |
| Read-only issue semantic assistance | 0 | TBD | TBD | 30 | TBD |
| Avrotize Schema or JSON Structure semantic review | 0 | TBD | TBD | TBD | TBD |
| Generated language/runtime review | 0 | TBD | TBD | TBD | TBD |
| Failed command/test-cluster triage | 0 | TBD | TBD | TBD | TBD |
| Cross-command semantic campaign review | 0 | TBD | TBD | TBD | TBD |
| Scheduled command/documentation drift | 0 | TBD | TBD | TBD | TBD |
| Release-candidate compatibility | 0 | TBD | TBD | TBD | TBD |
| Guarded reproduction evidence review | 0 | TBD | TBD | TBD | TBD |
| Owner-launched external delivery supervision | 0 | TBD | TBD | Per delegation | TBD |

Dependabot intake, guarded bug reproduction, and reproduction label
reconciliation remain fully deterministic and invoke no model, so their AIC is
zero rather than uncalibrated. Guarded reproduction has no semantic phase today;
the row above exists only so a future owner-authorized, read-only evidence review
has a place to record platform-reported telemetry.

The issue-intake value of 30 is a configured soft per-run ceiling, not observed
usage or a projection. The current silent Copilot CLI output has no stable
machine-readable platform AIC field, so the intake artifact records AIC as not
exposed rather than estimating it. Organization billing and usage telemetry is
the authoritative source for later observed samples. Until telemetry exists,
projections remain explicitly uncalibrated. Do not substitute task scores or
token-derived estimates.

Passing commands and tests do not invoke AI. When issue intake reaches its
configured limit or the required structured result cannot complete, it records
semantic assistance as unavailable and preserves the issue for a human read.
Partial model output is not accepted as structured assistance.

The external delivery supervisor is a separate owner-launched project session,
not an Actions workflow or the issue-intake adviser. Its strict delegation may
configure a per-cycle maximum for platform-reported AIC. The reconciler compares
only the current-cycle platform-reported total and stops delegated work
at the configured limit; it never estimates AIC from tokens, model pricing,
elapsed time, or task scores. A configured maximum is a guardrail, not an
observation.

Calculate observed P50 and P95 only from comparable platform-reported runs.
Recalibrate after the first 20 representative activations and after material
workflow, model, prompt, context, or delegation changes.
