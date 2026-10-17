# H3 findings: trust-model trade-offs

Evaluated on 2026-10-17 using Go 1.27.2, Linux x86_64, on an AMD EPYC 9645 host. H3 now has a dedicated executable experiment, matched privacy accounting, repeated-seed measurements, regression tests, and an explicit conclusion.

## Question and controlled comparison

Compare the implemented central HybridSketch and strict Sampled Count-Min LDP on the **same clipped event-frequency target** at matched cumulative epsilon. Measure accuracy, processing time and communication; do not assume the direction of the timing result. Conditional PrivSketch estimates client presence and is excluded.

The sweep covers stationary, scheduled drift every 15 rounds, and frequent drift every 5 rounds; final epsilon budgets 1, 6 and 12; equal sketch widths 16 and 64; five paired seeds 26–30; and 60 rounds per run. This is **90 paired runs / 5,400 windows / 10,800 method evaluations**. Each window uses 512 public client groups, a 256-item domain, five rows, four maximum sliding cohorts, 4,096 incoming events and a clipping cap of eight events per group/cohort. Warmup is included in all means.

Central delta is 1e-5. Constant central rho per round is calibrated to the final epsilon. Each local report spends the increase in central cumulative epsilon, ensuring the two ledgers match at every round, not just at the horizon. Both methods release in every round. The schedule is public and exported. Runtime checks fail on a privacy mismatch above 1e-9 or a withheld release. All evaluated pairs passed.

## Interpretation

HybridSketch has lower mean maximum frequency error in all 18 scenario/budget/width cells. The local-minus-central error gap is positive for every individual paired seed as well. This supports an accuracy advantage for the implemented central protocol **within this synthetic configuration and schedule**.

Strict LDP uses far less modeled communication and has lower per-method processing time in all evaluated cells. The central sketch sends two dense 64-bit additive-share vectors per client per cohort block; the local report sends a sampled coordinate and a randomized bit. Central secure-share simulation and dense sketch construction also require more work here. A claim that the central method has both better accuracy and lower latency is therefore **not supported** by these measurements.

Larger width modestly reduces central collision error but increases local sampling variance and payload in this experiment. Stronger privacy (smaller epsilon) increases error for both methods. Large local errors are reported without clipping: debiased signed estimators can fall outside [0,1], especially at small per-report epsilon. These results are not a claim about the best achievable local-DP estimator.

For a representative scheduled-drift setting (final epsilon 12, width 64), mean maximum frequency error is **0.0576 ± 0.0011** for Hybrid and **6.9305 ± 0.1855** for strict LDP (between-seed SD). Mean processing time is **18.694 ms** versus **0.156 ms** per method/window; modeled 60-round payload is **585 MiB** versus **0.0586 MiB**. These are raw signed-estimator errors and simulator timings, subject to the limits below. All 5,400 paired windows had exactly equal recorded cumulative epsilon; a second full sweep reproduced every non-timing summary exactly.

## Limits

- Central (epsilon, delta)-DP and pure local DP protect the same group-replacement target under different trust assumptions. Equal epsilon does not make their guarantees identical; pure local DP is stronger in this respect.
- The prefix-matched local schedule is designed to control cumulative privacy spending, not to optimize local accuracy. Alternative schedules, client populations, encoders and protocol-specific width tuning could change the trade-off. Equal widths do not equalize payload or work.
- Processing time is simulator wall time, including encoding, aggregation, decoding and evaluation. It excludes source waiting, real network latency, distributed client parallelism and secure-aggregation setup. Methods run in fixed Hybrid-then-local order. The final measurement sweep ran after the build/test jobs finished; this is still a shared host, not an isolated benchmark machine. A distributed end-to-end latency claim remains outside scope.
- Noise is deterministically seeded for reproducibility. Exact truth and metrics are exported. These artifacts carry no deployment privacy guarantee.
- Five seeds are exploratory evidence, not a general theorem or a statistical confirmation. Reported SD is between-seed sample SD; paired differences use seed-level time means. Overlapping windows are not independent statistical replicates.

## Reproduce and inspect

```sh
nix develop --command make experiment-h3
# Or select a new empty directory explicitly:
go run ./cmd/streamstar experiment-h3 \
  -config configs/reproducible.json -rounds 60 -seeds 5 -out results/h3-rerun
python3 scripts/report_h3.py results/h3-rerun
```

`experiment-h3` runs three stream scenarios automatically. `-budgets` and `-widths` control its sweeps; `-config` supplies population/window/row settings and noise mode. The report rejects incomplete pairs, missing seeds, failed release counts and mismatched final budgets. The runner also checks every prefix.

Saved evidence: [aggregate table](h3-report.md), [aggregate statistics](h3-aggregate.json), [all per-seed summaries](h3-summary.json), [manifest](h3-manifest.json), and one complete paired [CSV](h3-example-rounds.csv), [config](h3-example-config.json) and [local spending schedule](h3-example-local-epsilon.json). Full per-round data for all 90 paired runs stays under ignored `results/`; the commands regenerate it. The config's `local_epsilon` is the first report's epsilon; the schedule file gives the value for every round.
