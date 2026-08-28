# External intelligence — the next wave

Three seams and a roadmap. **Nothing here is built**, and that is deliberate:
each section defines the contract precisely enough that building it later is
mechanical, and stops there. A seam that gets implemented speculatively is a
seam that gets implemented wrong, because nobody has yet needed it.

Companion to `docs/external-intelligence-hub-runbook.md`, which documents what
*is* built and running.

---

## 1. The LLM decision card seam (design only)

The original Smart Scanner concept ended with an LLM step. It was not built
earlier for a good reason: with one evidence source, an LLM has nothing to do
but paraphrase a verdict, and a paraphrase that sounds confident is worse than
no paraphrase at all.

That has changed. There are now six independent evidence sources — scanner
evidence, market context, earnings, news, SEC filings, and external signals —
which routinely disagree. Explaining a disagreement is a real task, and it is
one an LLM is genuinely good at.

### The task is EXPLAIN, never PREDICT

The card describes what the evidence says and where it conflicts. It does not
forecast, does not rank, does not recommend, and does not produce a number.
This is not caution for its own sake: the moment a card emits a score, that
score becomes the thing people sort on, and six carefully-separated evidence
sources collapse back into one opaque figure — which is precisely what this
architecture was built to avoid.

### Input contract

```
decision_card_input.v1
  symbol
  session                       the scan session
  anchor_session                the display anchor, when it differs
  scanner
    verdict, attention, setup_state, reason_code
    gate_progress[], blockers[]
    strategy_code, strategy_version, allow_enter   (always false today)
  market_context                benchmark/sector relative, breadth, regime
  catalysts
    earnings                    proximity, timing, certainty
    news                        bounded items with publisher + published_at
    sec_events                  bounded filings with item codes + accepted_at
  external_intelligence
    items[]                     source, raw + normalised semantics, provenance
    confluence                  the WORD, never a number
    sources[]                   registry, so "silent" and "never connected"
                                stay distinguishable
  disagreements[]               computed BEFORE the call, not left to the model
    { dimension, positions[], note }
  provenance
    every source's freshness verdict and last-success timestamp
    contract_version of each block
```

Two properties of this contract matter more than its field list.

**Disagreements are computed deterministically and passed IN.** If the model
were asked to find the conflicts itself, the set of conflicts would vary run to
run, and a conflict that quietly stopped being reported would be invisible. The
model explains a disagreement it is handed; it does not decide what counts as
one.

**Every block carries its own freshness.** A card that reasons over a stale
news feed without saying so is worse than one that says "news unavailable" —
absence measured through a blind source carries no information, and the model
must be able to say that rather than infer quiet.

### Output contract

```
decision_card.v1
  summary               2-4 sentences, descriptive
  evidence_for[]        { claim, source_block, cites[] }
  evidence_against[]    { claim, source_block, cites[] }
  unknowns[]            what could not be determined, and why
  disagreement_notes[]  one per input disagreement
  model, model_version, prompt_version, generated_at
```

Every claim cites the block it came from. A sentence with no citation is a
hallucination by construction, which makes the failure mode checkable rather
than a matter of taste.

Deliberately absent: `recommendation`, `score`, `confidence`, `rank`,
`target`, `stop`, `position_size`.

### Rollout

Behind a disabled flag, generating into a table nobody reads, for long enough
to answer one question: does the card ever assert something the evidence does
not support? Until that has been measured on real sessions, it does not reach
a screen.

---

## 2. The alert delivery seam (contract only)

Telegram and WhatsApp were in the original design. Nothing is built, and no
alert is enabled.

The reason to define the contract now rather than later is that the
interesting part is not the transport — it is deciding what is worth
interrupting somebody for. Getting that wrong produces a channel people mute,
after which the system is worse than having no alerts at all.

### The trigger contract

```
alert_trigger.v1
  trigger_code          a stable identifier, one per rule
  symbol
  session
  fired_at
  evidence_refs[]       the exact rows that caused it
  suppression_key       identity for deduplication
  channel_hints         { urgency: informational | timely }
```

### What may fire one

Only TRANSITIONS, never states. "AAPL is high attention" is true for days and
would fire every run; "AAPL entered high attention this session" happens once.

Candidate rules, each of which is a change in a fact we already record:

- a symbol enters `high_attention` for the first time in N sessions
- an external source reports on a symbol the scanner has flagged
  (`confluence` becomes `agreement` or `disagreement`)
- an 8-K with a primary item is accepted for a flagged symbol
- a scanner run fails, or the pipeline misses its session

### Rules that hold before any of this ships

1. **Suppression is part of the contract, not an afterthought.** Every trigger
   carries a `suppression_key`; a repeated firing for the same key inside a
   window is dropped. Without this the first noisy day trains the user to
   ignore the channel permanently.
2. **An alert states what happened and links to the detail screen.** It never
   contains a recommendation and never contains a verdict on its own — a
   notification is the worst possible surface for a nuanced claim.
3. **Unavailable sources never fire.** "News went quiet" must not be
   indistinguishable from "the news feed broke".
4. **No alert may be triggered by an external signal alone** until outcome
   linkage has actually measured whether external signals carry information.
   Alerting on unmeasured evidence is how a research tool turns into a
   superstition.

---

## 3. Roadmap — the next external data wave

Ordered by *information we do not already have*, divided by *friction*. The
first two entries are the only ones with a clear case today.

### Worth doing next

**Market movers / unusual volume — partially delivered.** Migration 023 records
the FMP movers feeds, which are free on the current entitlement and answer the
one question a 25-symbol universe structurally cannot: is any of our symbols in
the market's attention cohort today, and what else is? Not surfaced in the
product, because the provider's individual licence forbids exactly that. The
next step is either a display licence or a different source, not more code.

**Outcome linkage — the highest-value item on this list.** The measurement path
exists (`external_signal_session_links`), and what it needs now is *time*: a
few weeks of real signals before any cohort comparison means anything. This is
not a build item. It is a wait-and-then-look item, and the discipline it needs
is not tuning the windows while waiting.

### Genuinely new, higher friction

**Analyst actions** (upgrades, downgrades, target changes). A real event with a
defensible timestamp and an identifiable actor — the same shape as an 8-K, and
it would slot into the catalyst layer rather than this one. Entitlement:
available on FMP's paid tiers.

**Institutional and insider activity** (13F, Form 4). Form 4 is free from
EDGAR through the path migration 021 already uses, and is the stronger of the
two: an insider buying is a dated, attributable act. 13F is quarterly and
badly stale by publication — its reputation exceeds its usefulness.

**Macro calendar.** Cheap, and the only item here that is genuinely
market-wide rather than per-symbol. It belongs beside `market_regime`, not in
the catalyst layer: an FOMC date is not a fact about a company.

### Evaluate carefully

**Social / mention velocity.** The one item with a real chance of being
actively harmful. Mention counts are trivially manipulated and correlate with
volatility rather than direction, and the layer would arrive with the highest
noise-to-signal ratio of anything here. If it is ever built, it belongs behind
the same measurement discipline as external signals: recorded and measured for
months before it is shown.

**Fundamentals / micro.** Mostly duplicates what the scanner deliberately does
not use. A Wyckoff structural read does not become better for knowing a P/E
ratio, and adding one invites exactly the blending this architecture avoids.

### The rule this list is written under

Every entry here is a *source*, not a *feature*. The pattern established by
earnings, news, SEC and now external signals holds: one bounded dimension at a
time, with its own freshness, its own point-in-time gate, its own failure
boundary, and no path to the verdict. A source that cannot be measured
separately cannot be evaluated, and a source that cannot be evaluated
eventually gets trusted by default.
