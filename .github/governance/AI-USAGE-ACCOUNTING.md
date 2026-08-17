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

No governance workflow currently invokes Copilot. The observed AIC sample size
is therefore zero, and P50 and P95 are `TBD`.

Future workflow projections must use observed platform-reported run
distributions:

| Responsibility | Sample size | Observed P50 AIC | Observed P95 AIC | Per-run guardrail | Daily guardrail |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cell intake or impact analysis | 0 | TBD | TBD | TBD | TBD |
| Target or type-system review | 0 | TBD | TBD | TBD | TBD |
| Failed-cell triage | 0 | TBD | TBD | TBD | TBD |
| Shared-model campaign review | 0 | TBD | TBD | TBD | TBD |
| Scheduled drift | 0 | TBD | TBD | TBD | TBD |
| Release-candidate compatibility | 0 | TBD | TBD | TBD | TBD |

Until telemetry exists, projections and guardrails remain explicitly
uncalibrated. Do not substitute task scores or token-derived estimates.

Passing cells do not invoke AI. Deterministic automation clusters related
failures before one AI invocation. When the platform reports guardrail
exhaustion or the required structured result cannot complete, return
`ESCALATE: AIC-GUARDRAIL-EXHAUSTED`; partial output is not success.

Calculate observed P50 and P95 only from comparable platform-reported runs.
Recalibrate after the first 20 representative activations and after material
workflow, model, prompt, context, or delegation changes.
