# Streamstar

A runnable implementation of **Drift-Adaptive Federated Sketching for Privacy-Preserving Streaming Frequency Analytics**, the Group 26 IOE413 project proposal. Go implements the stream processor, sketches, privacy accounting, simulated secure summation, dataset replay, Kafka transport, and HTTP server. An Astro dashboard with Preact islands shows actual engine measurements.

The research question is whether private rolling width selection and change-triggered releases improve the accuracy/privacy/communication trade-off over fixed-width, release-every-round sketches. The implementation provides an experiment to test that question; it does not assume the hypotheses are true.

![Streamstar dashboard showing a live 24-round research evaluation](assets/dashboard.png)

## Run

All development dependencies live in the root `flake.nix`, pinned by `flake.lock`. `.envrc` enables direnv integration.

```sh
direnv allow           # when direnv is installed and hooked into your shell
make run
```

Without direnv:

```sh
nix develop --command make run
```

Open **http://localhost:5173**. The default stream has 512 public client groups, a 256-item domain, four sliding cohorts, and scheduled Zipf/hot-item drift. Pause, step, inject drift, change settings, inspect privacy ledgers, or export CSV from the dashboard. Settings start a new research run.

The application also builds as a standalone Nix package:

```sh
nix build
./result/bin/streamstar serve
# Or: nix run -- serve
```

A Go 1.23+ installation is sufficient to run `go run ./cmd/streamstar serve`; Kafka is an optional ingestion source. The server and the prebuilt dashboard ship in one binary. Running it needs no Node runtime, Flink, database, or frontend build.

## Implemented methods

| Method | Construction | Release policy | Target |
| --- | --- | --- | --- |
| SharedSketch | Shared buckets and shared signs across cohorts | Every round | Clipped event frequency |
| FreshSketch | Fresh buckets and signs; median per cohort, then aggregate | Every round | Clipped event frequency |
| HybridSketch | Shared buckets, fresh signs; aggregate rows, then median | Every round | Clipped event frequency |
| Adaptive width | HybridSketch + paid private probe + Zipf tail fit | Every round | Clipped event frequency |
| Adaptive + private gate | Adaptive width + Page-Hinkley on private probe changes | Change or maximum staleness | Clipped event frequency |
| Sampled Count-Min LDP | One sampled counter, stochastic bounded encoding, randomized response, count-mean correction | Every affordable round | Clipped event frequency |
| PrivSketch reproduction | Boolean sketch, ordering matrix, sampled RR, decode-first aggregation | Every affordable round | Client presence |

**Privacy boundary:** Gaussian and strict local-DP budgets describe each protocol independently. The dashboard and CSV expose exact truth and evaluation metrics, so they are research outputs, not private publication APIs. PrivSketch's ordering matrix has only the paper's conditional privacy argument and is explicitly labeled accordingly. Secure summation is a two-helper simulation in one process. [Protocol and assumptions](docs/implementation.md) explains the guarantees and limits.

## Reproducible experiments

```sh
nix develop --command make test

# 60 rounds × 3 seeds × widths 16, 32, 64, 128
go run ./cmd/streamstar experiment \
  -config configs/reproducible.json -rounds 60 -seeds 3 -out results/drifting

# Gate ablations under different drift rates
go run ./cmd/streamstar experiment \
  -config configs/stationary.json -rounds 60 -seeds 3 -widths 64 -out results/stationary
go run ./cmd/streamstar experiment \
  -config configs/fast-drift.json -rounds 60 -seeds 3 -widths 64 -out results/fast-drift

python3 scripts/report.py results/drifting
```

Each run writes per-round CSV and its complete config. `summary.json` records mean error, MSE, NCR, processing time, final epsilon, releases, bytes, and post-drift recovery rounds. A null recovery means the error never reached the configured target before the next drift or run end. `scripts/report.py` writes a standalone interactive HTML report and a Markdown aggregate table.

Synthetic event generation is always seeded. Privacy noise uses **crypto entropy by default**. The supplied reproducibility configs explicitly enable deterministic noise for research: those runs carry no deployment DP claim. Timing measurements naturally vary between runs.

### H3: central versus local privacy

```sh
# 3 stream scenarios × 3 budgets × 2 equal widths × 5 paired seeds × 60 rounds
# Use a new/empty output directory; existing results are never overwritten.
go run ./cmd/streamstar experiment-h3 \
  -config configs/reproducible.json -rounds 60 -seeds 5 -out results/h3
python3 scripts/report_h3.py results/h3
```

This compares **HybridSketch and strict Sampled Count-Min LDP**, excluding conditional PrivSketch. Both release every round on the same clipped event stream, with equal rows/widths and matched cumulative epsilon at every round. Central noise is calibrated to each final budget; local per-report epsilon follows a public schedule matching the central ledger. The comparison uses central approximate DP versus stronger pure local DP at the same epsilon, so the trust assumptions and guarantees remain different.

The runner writes per-round CSVs, effective configs, local spending schedules, a manifest, and per-seed summaries. The report checks complete pairs and budgets, then records error, MSE, NCR, processing time, payload, and between-seed variability. [H3 findings](docs/evaluation/h3-findings.md) explain the measured outcome and limitations; [aggregate results](docs/evaluation/h3-report.md) contain every scenario/budget/width combination. Processing time measures the simulator, not distributed network latency.

## Dashboard development

The dashboard source lives in `ui/` and is built with [Astro](https://astro.build) and Preact islands:

- **Static by default.** Page structure, explanatory text, and the protocol notes are plain HTML rendered at build time. The protocol page ships no JavaScript.
- **Islands for live data.** Each live widget (`ui/src/islands/`) subscribes only to the engine data it displays. The shared store (`ui/src/lib/store.ts`) polls `/api/state` once per second and replaces a slice only when its contents change, so a new round patches just the affected components and a paused stream causes no DOM updates.
- **Polling is bounded.** The first request fetches at most 120 rounds; later requests are incremental. Polling stops while the tab is hidden, and out-of-order responses are discarded.
- **Self-contained.** Fonts are bundled, so the dashboard makes no external requests. Hashed assets under `/assets/` are served as immutable.

```sh
make run              # engine on :5173
make ui-dev           # Astro dev server on :4321 with hot reload; proxies /api to the engine
make ui               # type-check, test, and rebuild internal/server/web
```

`make ui-dev` is the fastest loop. `make run` serves the embedded build, so restart it after `make ui` to see a rebuilt dashboard.

The build output in `internal/server/web` is committed so that `go build` works without Node. It is reproducible: any checkout produces identical files, CI fails if the committed copy is stale, and `nix build` compiles the dashboard from `ui/` instead of using the committed copy. Git treats the directory as generated (`.gitattributes`): it is hidden from diffs and never merged line by line, so resolve a conflict there by running `make ui`.

## Real datasets and Kafka

Replay accepts Kosarak user sequences, Gowalla check-ins, Sentiment-140 CSV, AOL tab-separated logs, and event JSONL. `.gz` files work. Original user IDs are mapped to fixed client groups; item labels receive dense IDs. Domain overflow fails explicitly, so increase the configured domain for full datasets. No real dataset is bundled or automatically downloaded.

```sh
# Small bundled fixture, direct to the engine
go run ./cmd/streamstar serve -source replay -file testdata/kosarak.txt -format kosarak

# Convert to canonical JSONL and retain an item dictionary
go run ./cmd/streamstar replay \
  -file testdata/kosarak.txt -format kosarak -out /tmp/streamstar-events.jsonl

# Optional actual Kafka pipeline (requires a running Docker daemon)
docker compose up -d --wait
go run ./cmd/streamstar serve -source kafka -brokers localhost:9092
# In another terminal:
go run ./cmd/streamstar replay -file testdata/kosarak.txt -format kosarak \
  -brokers localhost:9092 -topic streamstar-events
```

Client-group keys partition Kafka messages consistently. Consumer offsets commit only after a batch processes successfully; timed partial batches keep low traffic moving. These are processing-time/count-based windows, not event-time/watermark windows. See [dataset and transport notes](docs/implementation.md#sources-and-window-semantics).

Without a Docker daemon, the flake provides a broker shell:

```sh
nix develop .#kafka --command bash scripts/kafka-local.sh
```

This starts a local KRaft broker on `127.0.0.1:9092` and retains its state under ignored `.firecrawl/kafka-local/`. Use the same consumer and producer commands above. Missing research topics are created with four partitions and replication factor one; existing topic configuration is preserved.

## HTTP ingress

```sh
go run ./cmd/streamstar serve -source manual
curl -X POST http://localhost:5173/api/events \
  -H 'Content-Type: application/json' \
  -d '[{"client":0,"item":1},{"client":1,"item":2}]'
```

`client` and `item` must be inside the configured public bounds. Optional `timestamp` is Unix milliseconds. Invalid batches are rejected before advancing the window or privacy ledgers.

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Server health |
| `GET /api/state?since=ROUND&generation=GENERATION&limit=N` | Config, latest round, and incremental history (at most the last `N` rounds when `limit` is set) |
| `GET /api/config` | Complete effective config |
| `POST /api/control` | Synthetic/replay controls; JSON action `play`, `pause`, `step`, `drift`, or `reset` |
| `POST /api/events` | Manual-source batch ingestion |
| `GET /api/export.csv` | Last 512 rounds of dashboard evaluation |
| `GET /protocol.html` | Browser-readable algorithm and privacy notes |

The local server binds to `127.0.0.1:5173`. Use `-addr` to change the listen address. It is a local research tool without authentication.

## Verification

```sh
make test             # race detector and go vet
make ui               # dashboard type-check, unit tests, and build
make build
go test ./internal/engine -run '^$' -bench . -benchmem
```

Tests cover signed secret-share reconstruction, HybridSketch decoding order, tail fitting, RR likelihood ratios and unbiasedness, contribution clipping/window expiry, privacy composition/caps, withheld releases, source parsers, Kafka flush/commit semantics, invalid ingestion, the HTTP control/export flow, history limits, and asset caching. Dashboard tests (`ui/src/lib/store.test.ts`) cover incremental history merging, generation resets, out-of-order responses, change-only updates, hidden-tab pausing, and offline reporting. [Validation notes](docs/validation.md) record the completed checks and sample evaluation.

## Sources

- Wu et al., [Private Federated Frequency Estimation: Adapting to the Hardness of the Instance](https://arxiv.org/abs/2306.09396), NeurIPS 2023 — Shared/Fresh/Hybrid sketches and probe-and-fit width selection.
- Li et al., [PrivSketch: A Private Sketch-based Frequency Estimation Protocol for Data Streams](https://arxiv.org/abs/2306.12144), DEXA 2023 — boolean sketches, sampled randomized response, ordering matrix, and evaluation metrics.
- Zhang et al., [Private and Communication-Efficient Federated Learning based on Differentially Private Sketches](https://arxiv.org/abs/2410.05733) — cross-task motivation in the proposal; model-training code is outside this analytics implementation.
