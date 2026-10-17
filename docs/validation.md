# Validation record

Validated locally on 2026-10-14. The synthetic evaluations use deterministic research noise; they establish reproducibility and exploratory measurements, not deployment privacy or statistical confirmation of the hypotheses.

## Completed checks

- `CGO_ENABLED=1 go test -race ./...` — passes.
- `go vet ./...` — passes.
- Standalone Go build — passes.
- `nix build` — builds the embedded server and runs Go checks with pinned Go 1.25.10.
- `nix flake check` — the package/build checks and shell/application definitions pass on x86_64-linux; the other declared platforms were not built here.
- `nix develop` — Go tools, direnv 2.37.1, Docker client 29.6.0, and Compose 2.40.3 load successfully.
- Root `.envrc` — allowed and successfully evaluated with direnv's `use flake`.
- Optional `nix develop .#kafka` — Kafka 4.1.0 / Java 17 environment loads.
- Go tests exercise clipping/window expiry, invalid-batch atomicity, sketch decoding order, private tail fit/fallback, randomized-response likelihood ratios and unbiasedness, secure-share reconstruction, budget caps, stable reuse/max-staleness releases, parser formats, timed Kafka partial batches, and commit-after-processing behavior.
- Live browser at desktop and iPhone sizes — dashboard data loads, pause/step works, navigation and seven-method ledger render, settings reset the run, responsive layout has no page-wide horizontal overflow.
- JavaScript syntax, Python report compilation, Bash broker-script syntax, and Git whitespace checks — pass.

## Actual Kafka smoke test

A single-node local KRaft broker was started with `scripts/kafka-local.sh` in the optional Nix broker shell; no Docker daemon was available. The Kafka source and replay producer created the topic using the version-negotiating Kafka client API. A Kosarak fixture passed through the actual broker into the streaming processor:

```text
source: kafka
source error: none
fixture events: 29
processed events: 29
methods emitted: Shared, Fresh, Hybrid, adaptive, gated, strict LDP, PrivSketch
```

Consumer-group offsets were inspected after processing to verify commitment. Kafka's older connection-level CreateTopics API is not used: Kafka 4.1 rejects that old version. Topic setup uses the client's negotiated protocol. The Docker Compose path is provided but was not run here; the real broker path was tested with Nix Kafka 4.1.0.

## Synthetic evaluation

Scheduled drift: 60 rounds × 3 seeds × fixed widths 16/32/64/128 = **720 windows**, each evaluating seven methods. Stationary and frequent-drift comparisons add 180 windows each. Total: **1,080 windows / 7,560 method evaluations**.

These counts describe computation, not independent evidence. The width sweep changes only the fixed-width sketches; the adaptive, gated and local methods produce identical results in all four width runs. Rounds within a run share overlapping windows. Each stream scenario therefore has three independent runs, one per seed.

Parameters: 512 client groups, 256 item IDs, 4-cohort window, C=8 contributions/group/cohort, 5 sketch rows, private probe width 32, Gaussian multiplier 4, probe multiplier 8, epsilon cap 12, delta 1e-5, local epsilon 0.3, target error 0.025, gate maximum staleness 12 rounds. Seeds 26, 27, 28. Scheduled drift every 15 rounds; frequent drift every 5.

At fixed/initial width 64, means across three seeds:

| Stream | Method | Mean ℓ∞ | Final ε | Releases / 60 | Total payload MiB |
| --- | --- | --- | --- | --- | --- |
| Stationary | Hybrid | 0.0616 | 11.167 | 60.00 | 585.00 |
| Stationary | Adaptive width | 0.0723 | 11.965 | 54.00 | 427.08 |
| Stationary | Adaptive + gate | 0.0805 | 6.205 | 6.33 | 307.71 |
| Scheduled drift | Hybrid | 0.0616 | 11.167 | 60.00 | 585.00 |
| Scheduled drift | Adaptive width | 0.0690 | 11.965 | 54.00 | 475.42 |
| Scheduled drift | Adaptive + gate | 0.1061 | 6.367 | 7.33 | 315.21 |
| Frequent drift | Hybrid | 0.0618 | 11.167 | 60.00 | 585.00 |
| Frequent drift | Adaptive width | 0.0812 | 11.965 | 54.00 | 480.42 |
| Frequent drift | Adaptive + gate | 0.1376 | 7.074 | 12.00 | 336.88 |

**Interpretation:** the gate reduces privacy spend and payload but pays in stale error. Its advantage shrinks as drift becomes more frequent. The adaptive width heuristic does not outperform width-64 HybridSketch under these defaults. Thus H1 is not supported by this exploratory configuration, and H2's target-accuracy qualification is not established by epsilon savings alone. Full-width sweeps, matched-epsilon comparisons, and recovery records are included in the artifacts. The strict local-DP protocol's default small per-round epsilon produces large errors; the conditional PrivSketch result has a distinct presence target and must remain separate.

These measurements should remain visible even if later tuning improves the policy. Do not present favorable privacy totals without the corresponding error and communication costs.

Committed artifacts in `docs/evaluation/` include all scenario summaries, the width-sweep aggregate table, and one complete per-round CSV/config pair. Regenerate full CSVs and interactive reports with the README commands; full generated output stays under ignored `results/`.

## H3 evaluation completed on 2026-10-17

The dedicated `experiment-h3` command now calibrates central Gaussian noise to a public final budget and schedules strict local-DP reports to match cumulative epsilon at every round. Only HybridSketch and strict Sampled Count-Min execute; both release every round with the same stream, event-frequency target, rows and width. Conditional PrivSketch is excluded.

The completed sweep uses 60 rounds, five paired seeds, three budgets (1/6/12), two widths (16/64) and three drift scenarios: **90 paired runs, 5,400 windows and 10,800 method evaluations**. Per-prefix epsilon gaps and fresh releases are checked during execution. [H3 findings](evaluation/h3-findings.md) describe the conclusion and limitations; [the aggregate report](evaluation/h3-report.md) records all cells. This establishes an executable H3 evaluation, not a universal or deployment privacy claim.

Verification on Go 1.27.2: Go race tests and vet pass; the standalone binary builds; schedule tests cover prefix matching/final caps; engine tests preserve selected methods' results and random streams; CLI tests verify paired artifacts and refusal to overwrite existing results. Python report tests reject incomplete, duplicate and mismatched runs and check paired error differences. The report tests run through `make test` and CI. Dashboard type checking, six store tests, and the embedded build pass.

## Processing microbenchmark

`go test ./internal/engine -run '^$' -bench . -benchmem -benchtime=3x` measured approximately **75.2 ms/window** for all seven methods on an AMD EPYC 9645 host, corresponding to about **54,465 input events/processing-second** for 4,096 events per window. Approximately 40.3 MB were allocated per iteration. This three-iteration microbenchmark excludes source waiting and is not a sustained Kafka load test; the dashboard's memory metric is sketch payload size, not the total Go allocation count or heap peak.

## Deployment boundaries

Secure aggregation is simulated in one process. The dashboard exposes exact diagnostics. Privacy accounting is not durable across restarts. Deterministic research noise is not private. The width/gate are experimental. See `docs/implementation.md` before interpreting this as a production privacy system.
