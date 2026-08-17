# AI Cost Accounting

Avrotize accounts for model inference in AI credits (AIC):

`1 AIC = USD 0.01`

For every invocation, retry, and descendant agent in one logical run:

```text
USD =
  uncached_input_tokens * input_price_per_token
  + output_tokens * output_price_per_token
  + cache_read_tokens * cache_read_price_per_token
  + cache_write_tokens * cache_write_price_per_token
  + reasoning_tokens * reasoning_price_per_token

AIC = USD / 0.01
```

If a provider's input total already includes cache reads, subtract cache-read
tokens before pricing uncached input. Do not double count. Reasoning tokens use
the provider's documented reasoning rate or, when explicitly stated by that
catalog, the output-token rate. If reported output already includes reasoning
tokens, subtract reasoning tokens before pricing non-reasoning output.

Every estimate and actual record includes:

- provider, exact model, and dated pricing catalog/version;
- fresh input, cache reads, cache writes, output, and reasoning tokens;
- per-million-token rates converted to per-token rates;
- invocation and retry count, descendant budgets, long-context thresholds,
  model auto-selection, and discount assumptions;
- deterministic baseline, typical cost when AI activates, P95, hard ceiling,
  exhaustion outcome, and actual AIC.

No model is currently selected for governance automation, no pricing catalog is
therefore implied, and deterministic passing paths consume `0 AIC`. A future
contract must name the model and pricing version before activation; arbitrary
task scores are not AIC.

## Worked projection

This non-activating example shows how a future cell-intake workflow would be
budgeted. It assumes Anthropic Claude Sonnet 5 on the first-party global API,
using the [official Anthropic pricing catalog](https://platform.claude.com/docs/en/about-claude/pricing)
captured on 2026-08-17: USD 2/MTok base input, USD 2.50/MTok five-minute cache
write, USD 0.20/MTok cache read, and USD 10/MTok output. Reasoning is priced at
the output rate. It assumes no batch discount, regional multiplier,
long-context premium, auto-selection, or negotiated discount.

| Token class | Assumption | USD |
| --- | ---: | ---: |
| Uncached input | 50,000 at USD 2/MTok | 0.100 |
| Cache read | 100,000 at USD 0.20/MTok | 0.020 |
| Five-minute cache write | 10,000 at USD 2.50/MTok | 0.025 |
| Non-reasoning output | 6,000 at USD 10/MTok | 0.060 |
| Reasoning output | 2,000 at USD 10/MTok | 0.020 |
| **One invocation** | **USD 0.225 / USD 0.01** | **22.5 AIC** |

One full retry makes the P95 planning case `45 AIC`. A descendant agent receives
its own sub-budget inside, not in addition to, the parent ceiling. Actual
provider usage replaces these assumptions after execution.

## Initial conversion-profile envelopes

| AI-assisted responsibility | Typical AIC | Ceiling AIC |
| --- | ---: | ---: |
| Cell intake or impact analysis | 30 | 150 |
| Target or type-system review | 120 | 500 |
| Failed-cell triage | 40 | 200 |
| Shared-model campaign review | 150 | 500 |
| Scheduled drift | 30 per finding cluster | 500 per matrix run |
| Release-candidate compatibility | 100 | 500 |

Passing cells do not invoke AI. Deterministic automation clusters related
failures before one AI invocation. At 80% of a ceiling, optional exploration
stops. If required structured output cannot be completed within the budget, the
only valid disposition is `ESCALATE: AIC-BUDGET-INSUFFICIENT`; partial output is
not success.

Recompute projections whenever model or pricing changes and calibrate typical
and P95 values after 20 representative activations.
