# Implementation and research contract

The attached PDF is a mini-project proposal, not a new algorithm specification with a theorem or ready-to-run reference implementation. Streamstar implements the baseline constructions from its cited papers and makes its proposed streaming enhancements executable and measurable.

## Architecture

```text
synthetic / dataset replay / HTTP / Kafka
                  ↓
 public client groups → per-cohort C-event clipping → sliding M-cohort window
                  ↓
         client-local sketch encoders
                  ↓
      two-helper additive-share simulation
                  ↓
      Gaussian mechanism + privacy ledger
                  ↓
 private probe → Zipf fit / Page-Hinkley → width and publication decision
                  ↓
 frequency decoding → research truth evaluation → dashboard / CSV

local-DP path: clipped client window → sampled counter + RR → decode → aggregate
```

Go's standard library handles windows, concurrency, cryptographic randomness, HTTP, and the embedded frontend. `kafka-go` is the only direct Go dependency. The root Nix flake pins the development environment and the standalone binary build. Kafka is optional; processing and dashboard work without infrastructure.

## Baseline fidelity

Count Sketch assigns each integer item one signed bucket per row. The point estimate is the row median. Hash parameters are public and reused across clients; client identity does not change the sketch's coordinate system.

For M cohorts:

- **SharedSketch** uses the same bucket and sign hashes everywhere. Each client locally combines its window before sharing one vector. The server decodes the merged vector.
- **FreshSketch** changes both bucket and sign hashes per cohort. Decode each cohort by taking its row median, then sum.
- **HybridSketch** shares bucket hashes but changes signs per cohort. For each item, undo the cohort-specific sign and sum each row across cohorts; only then take the median.

For equal public cohort capacities, dividing summed estimates by nCM is algebraically the paper's cohort average. Width changes require clients to re-encode the bounded current window; incompatible layouts are never merged. Cohort sign seeds are based on global cohort indices, so the same cohort has stable public parameters while it moves through the window. The reproducible hash families use SplitMix64; this is practical experimental hashing, not a claim of fully independent random oracles.

The paper's distinct-client cohorts and its sampling assumption are different from repeatedly active streaming groups. Our conservative privacy bounds protect a group appearing throughout the entire horizon. Therefore a result should be reported as a streaming extension/reproduction of trends, not an exact reproduction of the paper's numerical tables.

## Privacy model

### Population, clipping, and adjacency

Configuration supplies n public client-group slots and a public item universe of size d. In each processing cohort, the latest C valid events per group are retained. The window has M cohorts (a deterministic warmup reaches the configured maximum). Replacing all contributions of one group over the run defines neighboring inputs.

The event target is `count(item) / (n*C*M)`. This denominator is public and fixed for a given round; a privately varying observed total is never used to normalize a private release. Empty contribution slots produce zero central sketch updates. Local Count-Min pads them using a public null symbol outside the queried item domain.

### Gaussian accounting

A client contributes at most C signed updates per row per cohort. A conservative replacement bound is:

```text
Hybrid / Fresh: Δ₂ ≤ 2*C*sqrt(M*rows) for the joint M-block vector
Shared:        Δ₂ ≤ 2*C*M*sqrt(rows) for the combined vector
σ = noise_multiplier * Δ₂
ρ_query = Δ₂²/(2σ²) = 1/(2*noise_multiplier²)
ε(δ) = ρ_total + 2*sqrt(ρ_total*log(1/δ)) + pure_epsilon_total
```

This is an analytic conservative zCDP accountant for Gaussian mechanisms, rather than a Python `dp-accounting` runtime dependency. It composes the entire query sequence, including every overlapping window. There is no assumption of disjoint windows or amplification from sampling. Adaptive mechanisms use a budget-only filter that reserves the cost of a possible release before buying another private probe. Once it cannot afford a probe plus release, it retains the previous estimate and stops querying. Fixed and local methods also stop at their caps.

For a Count Sketch with fixed rows and C, per-bucket Gaussian noise is **not** scaled up with width: the vector's L2 sensitivity is independent of width. Width affects collisions, memory, communication, and the behavior of decoded errors. The proposal's discussion of a width/noise trade-off does not license adding artificial width-dependent noise. This implementation allows measured hypotheses to fail.

### Adaptive probe, fit, and gate

Each adaptive round performs its own noisy Hybrid probe at a fixed probe width. Probe sensitivity and composition follow the same rules as a release. The published fit, drift statistic, resizing, and release timing are all postprocessing of that paid probe.

Width selection fits `beta * rank^-alpha` to up to 20 leading private estimates using value-space least squares. Estimates below twice the approximate decoded Gaussian noise scale are excluded. Fewer than three usable ranks cause a fallback to the existing width. A truncated model with total mass at most one supplies the oracle-inspired heuristic:

```text
desired width ≈ 2 * (number of model frequencies >= target
                    + sum(model frequencies < target)^2 / target^2)
```

The width is bounded and rounded to a power of two. A factor-of-two hysteresis avoids minor layout churn. It is a heuristic inspired by the baseline two-phase procedure, not a theorem that the selected width is optimal.

The drift score is maximum coordinate change relative to the last published private probe. A noise-scale adjustment accounts for both the current and reference probes. Page-Hinkley detects sustained increases; a sufficiently large single change also triggers. Identity shifts are visible even at the same tail exponent. A public maximum-estimate-age rule prevents indefinite silence. Stable rounds still pay for probes, and the old estimate's stale error is evaluated against the new window.

Finite privacy cannot provide unlimited fresh accurate releases on an arbitrary forever-changing stream. The cap makes that limitation explicit.

### Local-DP protocols

**Strict sampled Count-Min:** a client samples one data-independent coordinate from its count sketch, maps its bounded count to [0,1], stochastically encodes it, and applies binary randomized response. Both the sampled coordinate and the bit form the report. RR's likelihood ratio is bounded by exp(epsilon) for any two possible complete client inputs. Client reports decode before aggregation using Horvitz-Thompson weighting and count-mean collision correction. Its collision correction is unbiased over the public hash family; an individual fixed hash can retain a collision residual. Every repeated report consumes pure epsilon; composition is sequential across windows.

**PrivSketch reproduction:** boolean Count-Min encoding, random ties within the ordering matrix, one sampled RR counter, minimum-order decoding per client, then aggregation. Its target is the fraction of clients whose clipped window contains an item. The ordering matrix depends on unperturbed data. The paper argues privacy within the remaining indistinguishable input set; ordinary full-domain LDP for the complete ordering-plus-RR transcript does not follow. The UI and exports label this method conditional and show RR spend only. It must not be used as evidence of a standard-LDP guarantee or compared numerically to event frequency without noting its distinct target.

### Secure summation and evaluation boundary

Two helpers receive uniformly random additive shares in the 64-bit ring. They aggregate shares separately; the server reconstructs only the two aggregate sums. Negative signed counters round-trip exactly for configured contribution bounds. This models honest noncolluding helpers, but both helpers and the local source run in the same executable. It has no distributed isolation, authentication, dropout recovery, or malicious-client defense.

The dashboard exposes exact ground truth, errors, clipping counts, and all seven protocol outputs for research. Its entire output is not private. Per-method accounting describes a protocol considered independently; publishing several private protocols would require joint composition, and publishing exact diagnostics invalidates a DP claim altogether. Deterministically seeded privacy noise is for reproducibility only. Ledger state is in memory: restarts and research resets do not preserve a deployment budget.

## Sources and window semantics

- Synthetic: scheduled Zipf exponent changes, gradual within-phase exponent shifts, and periodic hot-item rotations. Drift labels come from the generator and are used only for evaluation; adaptive decisions never read them.
- Kosarak: one original user sequence per nonempty input row.
- Gowalla: user/time/latitude/longitude/location rows; location is the item.
- Sentiment-140: CSV author and lowercased alphanumeric word tokens from text.
- AOL: tab-separated user records; click URL is used when present, otherwise query text. Headers are skipped.
- JSONL/HTTP/Kafka: canonical `{client, item, timestamp}` event records. Client/item integers must satisfy public configured bounds; timestamp is optional Unix milliseconds.

Dataset users map deterministically into public groups, not one simulated group per original user. Item labels map to dense IDs with a hard error on domain overflow. Learned dictionaries are research metadata; deployment would need a previously fixed public dictionary. The replay converter can emit a JSON dictionary alongside JSONL. Direct replay runs once to EOF; run/replay it again explicitly to repeat a finite dataset.

Windows contain the last M **processing cohorts**. Synthetic and replay cohorts are event-count batches; Kafka cohorts are full count batches or partial batches flushed after the configured deadline. There is no event-time watermark, lateness sorting, or exact-duration window. Historical dataset timestamps are replaced with replay arrival timestamps. Kafka uses client-group partition keys, fetches before processing, and commits offsets afterward. Crash/restart is not exactly-once processing and does not durably preserve the privacy ledger.

## Measurements

- **L-infinity:** maximum signed-estimate absolute error over all d public items, including zero-truth items.
- **MSE:** mean squared signed-estimate error over all d items.
- **NCR:** Li et al.'s true-rank quality: a true top-k item at rank i has `q=k+1-i`; all others have q=0. Divide estimated top-k quality sum by `k*(k+1)/2`.
- **Top-k VAR:** the source paper's count-scale MSE on the intersection of estimated/true top-k. `top_k_intersection=0` marks undefined VAR (stored as 0). `top_k_error_variance` separately measures normalized error variance on the true top-k.
- **Top-k recall:** intersection cardinality divided by k.
- **Privacy:** cumulative epsilon at the configured delta; separate probe/release counts and rho/pure-epsilon contributions.
- **Communication:** two 64-bit helper-share payloads per client for central sketches/probes. Local reports include sampled coordinate and bit; PrivSketch includes the bit-packed ordering permutation. Network framing, setup, dictionary, and cryptographic overhead are excluded.
- **Memory:** the sketch matrix payload for the active layout/window plus adaptive probe matrices; not total Go heap. `go test -benchmem` gives allocation measurements separately.
- **Processing time:** per-method and total wall-clock processing time, excluding the source interval. Throughput is input events / total processing seconds across all methods.
- **End-to-end latency:** event arrival timestamp to publication, p50/p95, with unknown timestamps excluded. All synthetic timestamps are batch arrival times. These are evaluation diagnostics.
- **Recovery:** first post-drift round at/below target error, before the next drift; null for no recovery. An isolated target crossing does not establish sustained recovery.

To test H1, sweep fixed widths and compare across seeds, including the cost of probes. For H2, compare gated and always-release adaptive paths across drift rates and include stale error and probe spend. For H3, match cumulative epsilon on the same event target, compare the central path with strict sampled Count-Min, and report measured latency rather than assuming its direction. The conditional PrivSketch path is a separate reproduction.

### Executable H3 evaluation

`streamstar experiment-h3` runs only HybridSketch and strict sampled Count-Min on identical synthetic windows. It sweeps stationary, scheduled-drift (15-round), and frequent-drift (5-round) streams; final epsilon budgets default to 1, 6, 12, equal central/local widths to 16 and 64, and paired seeds to five. Rows, clipping, population, query domain and event-frequency normalization agree. Both methods publish every round; no adaptive probes or stale releases enter this comparison.

For a public horizon T and final budget E, let L = log(1/delta). Inverting the Gaussian accountant gives `rho_total = (E / (sqrt(L+E)+sqrt(L)))^2`. The central path spends `rho_total/T` per round. The local report at round t spends `epsilon_central(t) - epsilon_central(t-1)`. Thus cumulative epsilon matches at every prefix, including warmup. The local schedule depends only on public configuration, never stream values. Runtime checks reject missing releases or an epsilon gap above 1e-9; regression tests cover prefix equality and budget rounding.

Matching epsilon does not equate central (epsilon, delta)-DP and pure local DP: the latter has a stronger trust/perturbation requirement and zero delta. Equal widths control layout, not communication or computational work. This is a comparison of two implemented protocols, not optimized central/local frontiers. Timing is per-method processing wall time in a single process, including evaluation, with fixed Hybrid-then-local order; network latency, client parallelism and cryptographic setup are absent. Neither a lower-error claim nor a latency direction is assumed.

Each run exports a config, per-report local epsilon schedule and raw CSV; a manifest and paired summaries describe the entire sweep. `scripts/report_h3.py` checks pair completeness and release/budget invariants, then produces per-cell means, between-seed SD, and paired local-minus-central error differences. Overlapping rounds are averaged within seed, not treated as independent replicates. See [H3 findings](evaluation/h3-findings.md) and [all aggregate measurements](evaluation/h3-report.md).

## Production work deliberately outside this research implementation

Durable accounting/checkpoints, independent secure-aggregation processes, secure client endpoints, dropout protocols, malicious-contribution validation, private-only publication APIs, event-time watermarks, real-data ingestion at the original population scale, and independent formal privacy review are not implemented. InfluxDB/Grafana and Flink were replaced by the smaller Go processor and embedded dashboard, as permitted by stack freedom.
