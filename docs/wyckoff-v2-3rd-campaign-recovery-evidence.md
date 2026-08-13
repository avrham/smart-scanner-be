# Wyckoff v2 — 3rd Prospective Campaign: Recovery Evidence

Durable-evidence record for the third green prospective campaign
(`campaign_run_id` **9470418d…**) on the isolated staging Fly Postgres. Preserved
so the recovery path is auditable independently of any dashboard.

## Summary

The third campaign's 25 evaluation tasks initially **failed** because the
evaluation worker was running an older image whose **global**
`assert_no_outcomes` guard rejected every `prospective` evaluation. Redeploying
the worker to the corrected image and resetting the failed tasks to `queued`
produced a clean **25/25 succeeded** run with no duplicate campaign, pairs, or
evaluations.

## Timeline

| Step | What happened |
| ---- | ------------- |
| 1 | Third campaign registered + enqueued; 25 `prospective` evaluation tasks created. |
| 2 | Evaluation worker was on image `96771b7` (`fix(jobs): harden live worker execution`), which still carried the **global** `assert_no_outcomes_for_run` guard. That guard fired on every claim → all 25 tasks moved to `failed` (retryable exhausted). |
| 3 | Root cause: the guard must be **run-scoped** (assert no outcomes *for this run*), not global — a legitimate co-resident outcome row from another run was tripping it. |
| 4 | Redeployed the evaluation worker to `a5c1e23` (`feat(prospective): read-only evidence-dashboard payload…`), which carries the run-scoped guard. |
| 5 | Reset the 25 `failed` tasks back to `queued` using the **worker DB role** (least-privilege; no admin/superuser), clearing `last_error`/attempt bookkeeping so they were reclaimable. |
| 6 | Worker reclaimed and processed all 25 → **25/25 succeeded**. Campaign `9470418d…` reached the green (`completed`) terminal state. |

## Why this is safe / non-duplicating

- Evaluation-task idempotency keys are per-`(run, symbol, arm)`; a reset+replay
  maps to the **same** rows. The final campaign has exactly the expected pair
  and evaluation counts (no duplicate campaign registration, no duplicate
  pairs, no duplicate evaluations).
- The reset was a `failed → queued` status transition only. No evaluation
  outputs, outcomes, pairs, or bars were mutated by the operator.
- The fix is a **correctness narrowing** of an existing guard
  (`assert_no_outcomes_for_run` is scoped to the run under evaluation). No
  strategy, threshold, classifier, `allow_enter`, or outcome-formula behaviour
  changed.

## Provenance

- Failing image commit: `96771b7` — `fix(jobs): harden live worker execution`.
- Recovered image commit: `a5c1e23` — `feat(prospective): read-only
  evidence-dashboard payload for the UI tranche` (run-scoped guard).
- Campaign: `campaign_run_id = 9470418d…` (third green prospective campaign).
- Environment: isolated staging Fly Postgres (no shared Supabase credential
  present on the worker; reads local `daily_bars`/`market_bars_4h` only).

## Reproduce the audit (read-only)

```sql
-- campaign terminal state + counts
SELECT status, campaign_run_id
FROM   prospective_campaign_registrations
WHERE  campaign_run_id::text LIKE '9470418d%';

-- 25/25 evaluation tasks succeeded for that run's campaign job
SELECT t.status, count(*)
FROM   job_tasks t
JOIN   job_runs r ON r.id = t.job_id
WHERE  r.job_type = 'prospective_campaign'          -- campaign job
GROUP  BY t.status;

-- pair + evaluation counts (expect no duplicates)
SELECT count(*) AS pairs
FROM   strategy_shadow_run_pairs rp
WHERE  rp.run_id = (SELECT campaign_run_id
                    FROM prospective_campaign_registrations
                    WHERE campaign_run_id::text LIKE '9470418d%');
```
