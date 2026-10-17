package engine

import (
	"fmt"
	"math"
	"sort"
	"time"

	"streamstar/internal/privacy"
	"streamstar/internal/secureagg"
	"streamstar/internal/sketch"
	"streamstar/internal/stream"
)

type Method struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Color    string `json:"color"`
	Target   string `json:"target"`
	Privacy  string `json:"privacy"`
	Adaptive bool   `json:"adaptive"`
	Gated    bool   `json:"gated"`
	// Conditional marks a privacy argument weaker than full-domain DP.
	Conditional bool `json:"conditional"`
}

var Methods = []Method{
	{ID: "shared", Name: "SharedSketch", Color: "#94a3b8", Target: "event frequency", Privacy: "central DP"},
	{ID: "fresh", Name: "FreshSketch", Color: "#a78bfa", Target: "event frequency", Privacy: "central DP"},
	{ID: "hybrid", Name: "HybridSketch", Color: "#60a5fa", Target: "event frequency", Privacy: "central DP"},
	{ID: "adaptive", Name: "Adaptive width", Color: "#fbbf24", Target: "event frequency", Privacy: "central DP", Adaptive: true},
	{ID: "gated", Name: "Adaptive + private gate", Color: "#2dd4bf", Target: "event frequency", Privacy: "central DP", Adaptive: true, Gated: true},
	{ID: "local", Name: "Sampled Count-Min LDP", Color: "#fb7185", Target: "event frequency", Privacy: "full-domain local DP"},
	{ID: "privsketch", Name: "PrivSketch reproduction", Color: "#f97316", Target: "client presence", Privacy: "conditional only; ordering metadata", Conditional: true},
}

type Result struct {
	Method
	Round          int            `json:"round"`
	Width          int            `json:"width"`
	Epsilon        float64        `json:"epsilon"`
	Ledger         privacy.Ledger `json:"ledger"`
	Released       bool           `json:"released"`
	Reason         string         `json:"reason"`
	LastRelease    int            `json:"last_release"`
	Staleness      int            `json:"staleness"`
	Releases       int            `json:"releases"`
	Exhausted      bool           `json:"exhausted"`
	Detected       bool           `json:"detected"`
	Signal         float64        `json:"signal"`
	Fit            sketch.Fit     `json:"fit"`
	Metrics        Metrics        `json:"metrics"`
	Top            []Item         `json:"top"`
	LatencyMS      float64        `json:"latency_ms"`
	BytesPerClient int64          `json:"bytes_per_client"`
	TotalBytes     int64          `json:"total_bytes"`
	MemoryBytes    int64          `json:"memory_bytes"`
}
type Round struct {
	Number        int      `json:"number"`
	Timestamp     int64    `json:"timestamp"`
	Phase         string   `json:"phase"`
	InjectedDrift bool     `json:"injected_drift"`
	Events        int      `json:"events"`
	Clipped       int      `json:"clipped"`
	WindowEvents  int      `json:"window_events"`
	WindowCohorts int      `json:"window_cohorts"`
	LatencyMS     float64  `json:"latency_ms"`
	EndToEndP50MS float64  `json:"end_to_end_p50_ms"`
	EndToEndP95MS float64  `json:"end_to_end_p95_ms"`
	Throughput    float64  `json:"throughput"`
	Results       []Result `json:"results"`
}
type model struct {
	method                Method
	width                 int
	ledger                privacy.Ledger
	estimate, reference   []float64
	lastRelease, releases int
	totalBytes            int64
	detector              PageHinkley
	rng                   *privacy.Random
	referenceNoise        float64
}
type Engine struct {
	Config Config
	window stream.Window
	models []*model
	round  int
}

func New(c Config) (*Engine, error) {
	return NewForMethods(c)
}

// NewForMethods runs only the requested independent protocols. Empty ids selects
// all methods. Results follow Methods order, and random seeds retain their
// original method indices.
func NewForMethods(c Config, ids ...string) (*Engine, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	selected := make(map[string]bool)
	for _, id := range ids {
		if selected[id] {
			return nil, fmt.Errorf("duplicate method %q", id)
		}
		known := false
		for _, method := range Methods {
			if method.ID == id {
				known = true
				break
			}
		}
		if !known {
			return nil, fmt.Errorf("unknown method %q", id)
		}
		selected[id] = true
	}
	e := &Engine{Config: c, window: stream.Window{Capacity: c.WindowRounds}}
	for index, method := range Methods {
		if len(selected) > 0 && !selected[method.ID] {
			continue
		}
		e.models = append(e.models, &model{method: method, width: c.Width, rng: privacy.NewRandom(c.Seed+9109+int64(index)*7919, c.DeterministicNoise), ledger: privacy.Ledger{Delta: c.Delta, Limit: c.EpsilonLimit}})
	}
	return e, nil
}
func (e *Engine) Process(batch stream.Batch) (Round, error) {
	start := time.Now()
	c := e.Config
	clients, clipped, err := stream.Clip(batch.Events, c.Clients, c.Domain, c.ContributionCap)
	if err != nil {
		return Round{}, err
	}
	e.round++
	e.window.Push(clients)
	flat := e.window.Flatten(c.Clients)
	m := len(e.window.Batches)
	capacity := float64(c.Clients * c.ContributionCap * m)
	truth, presence := make([]float64, c.Domain), make([]float64, c.Domain)
	windowEvents := 0
	for _, items := range flat {
		seen := map[int]bool{}
		for _, id := range items {
			truth[id] += 1 / capacity
			seen[id] = true
			windowEvents++
		}
		for id := range seen {
			presence[id] += 1 / float64(c.Clients)
		}
	}
	r := Round{Number: e.round, Timestamp: time.Now().UnixMilli(), Phase: batch.Phase, InjectedDrift: batch.Drift, Events: len(batch.Events), Clipped: clipped, WindowEvents: windowEvents, WindowCohorts: m}
	for _, model := range e.models {
		result, err := e.processModel(model, flat, truth, presence)
		if err != nil {
			return Round{}, err
		}
		r.Results = append(r.Results, result)
	}
	r.LatencyMS = float64(time.Since(start).Microseconds()) / 1000
	r.Throughput = float64(len(batch.Events)) / max(time.Since(start).Seconds(), 1e-9)
	age := make([]float64, 0, len(batch.Events))
	published := time.Now().UnixMilli()
	for _, event := range batch.Events {
		if event.Timestamp > 0 {
			age = append(age, max(0, float64(published-event.Timestamp)))
		}
	}
	if len(age) > 0 {
		sort.Float64s(age)
		r.EndToEndP50MS = age[(len(age)-1)/2]
		r.EndToEndP95MS = age[int(float64(len(age)-1)*.95)]
	}
	return r, nil
}
func (e *Engine) processModel(model *model, flat [][]int, truth, presence []float64) (Result, error) {
	start := time.Now()
	c := e.Config
	m := len(e.window.Batches)
	result := Result{Method: model.method, Round: e.round, Reason: "scheduled"}
	bytes := int64(0)
	var probe []float64
	probeNoise := 0.0
	fit := sketch.Fit{Width: model.width}
	release := true
	if model.method.Adaptive {
		// Reserve release budget before paying for another private probe.
		prospective := model.ledger
		canContinue := prospective.Gaussian(c.ProbeMultiplier, true) && prospective.CanGaussian(c.NoiseMultiplier)
		if !canContinue {
			release = false
			result.Exhausted = true
			result.Reason = "privacy budget exhausted"
		} else {
			model.ledger.Gaussian(c.ProbeMultiplier, true)
			blocks, sigma, err := e.central("hybrid", c.ProbeWidth, c.ProbeMultiplier, 0x70be, model.rng)
			if err != nil {
				return result, err
			}
			probe = sketch.Decode(blocks, c.Domain, float64(c.Clients*c.ContributionCap*m), false)
			bytes += int64(2 * 8 * c.Rows * c.ProbeWidth * m)
			noiseFloor := sigma * math.Sqrt(float64(m)) / float64(c.Clients*c.ContributionCap*m) * math.Sqrt(math.Pi/(2*float64(c.Rows)))
			probeNoise = noiseFloor
			fit = sketch.FitWidth(probe, noiseFloor, c.TargetError, c.MinWidth, c.MaxWidth, model.width)
			result.Signal = distance(probe, model.reference)
			// Subtract a conservative maximum-noise scale from the score before
			// PH so independent probe noise does not masquerade as drift.
			adjusted := max(0, result.Signal-math.Hypot(noiseFloor, model.referenceNoise)*math.Sqrt(2*math.Log(float64(c.Domain))))
			result.Detected = model.detector.Update(adjusted, c.ChangeThreshold/2) || adjusted > c.ChangeThreshold
			if model.method.Gated {
				switch {
				case model.estimate == nil:
					result.Reason = "initial release"
				case result.Detected:
					result.Reason = "private drift detected"
				case e.round-model.lastRelease >= c.MaxStaleness:
					result.Reason = "maximum staleness"
				default:
					release = false
					result.Reason = "stable; reused previous estimate"
				}
			}
			// Resizing is postprocessing of the private probe. Only apply a new
			// layout at a release, and require a factor-of-two change.
			if release && (fit.Width >= 2*model.width || fit.Width*2 <= model.width) {
				model.width = fit.Width
			}
		}
	}
	if release {
		switch model.method.ID {
		case "local", "privsketch":
			if !model.ledger.Local(c.LocalEpsilon) {
				release = false
				result.Exhausted = true
				result.Reason = "privacy budget exhausted"
				break
			}
			p := sketch.Params{Rows: c.Rows, Width: c.LocalWidth, BucketSeed: uint64(c.Seed) + 0x1d9}
			model.width = c.LocalWidth
			if model.method.ID == "local" {
				model.estimate = sketch.SampleCountMin(flat, p, c.Domain, c.ContributionCap*m, c.LocalEpsilon, model.rng)
				bytes += int64((bitsFor(c.Rows*c.LocalWidth) + 1 + 7) / 8)
			} else {
				model.estimate = sketch.PrivSketch(flat, p, c.Domain, c.LocalEpsilon, model.rng)
				bytes += int64((bitsFor(c.Rows*c.LocalWidth) + 1 + c.Rows*c.LocalWidth*bitsFor(c.Rows*c.LocalWidth) + 7) / 8)
			}
		default:
			if !model.ledger.Gaussian(c.NoiseMultiplier, false) {
				release = false
				result.Exhausted = true
				result.Reason = "privacy budget exhausted"
				break
			}
			kind := model.method.ID
			if model.method.Adaptive {
				kind = "hybrid"
			}
			blocks, _, err := e.central(kind, model.width, c.NoiseMultiplier, 0xface, model.rng)
			if err != nil {
				return result, err
			}
			model.estimate = sketch.Decode(blocks, c.Domain, float64(c.Clients*c.ContributionCap*m), kind == "fresh")
			bytes += int64(2 * 8 * c.Rows * model.width * len(blocks))
		}
	}
	if release {
		model.lastRelease = e.round
		model.releases++
		if probe != nil {
			model.reference = probe
			model.referenceNoise = probeNoise
			model.detector = PageHinkley{}
		}
	}
	model.totalBytes += bytes * int64(c.Clients)
	result.Released = release
	result.Width = model.width
	result.Fit = fit
	result.Epsilon = model.ledger.Epsilon()
	result.Ledger = model.ledger
	result.LastRelease = model.lastRelease
	result.Staleness = e.round - model.lastRelease
	result.Releases = model.releases
	result.BytesPerClient = bytes
	result.TotalBytes = model.totalBytes
	result.MemoryBytes = int64(8 * c.Rows * model.width * m)
	if model.method.ID == "shared" || model.method.ID == "local" || model.method.ID == "privsketch" {
		result.MemoryBytes = int64(8 * c.Rows * model.width)
	}
	if model.method.Adaptive {
		result.MemoryBytes += int64(8 * c.Rows * c.ProbeWidth * m)
	}
	if model.estimate == nil {
		model.estimate = make([]float64, c.Domain)
	}
	if model.method.ID == "privsketch" {
		truth = presence
	}
	normalizer := float64(c.Clients * c.ContributionCap * m)
	if model.method.ID == "privsketch" {
		normalizer = float64(c.Clients)
	}
	result.Metrics = Evaluate(model.estimate, truth, c.TopK, normalizer)
	result.Top = Top(model.estimate, truth, c.TopK)
	result.LatencyMS = float64(time.Since(start).Microseconds()) / 1000
	return result, nil
}
func bitsFor(n int) int { return int(math.Ceil(math.Log2(float64(n)))) }

// central encodes the sliding window as M independently signed cohort blocks.
// Replacing a user's complete trajectory changes at most C counters' worth in
// each block: sensitivity <= 2*C*sqrt(M*rows). SharedSketch merges all blocks
// before noise, so its bound is 2*C*M*sqrt(rows). No overlap amplification.
func (e *Engine) central(kind string, width int, multiplier float64, salt uint64, rng *privacy.Random) ([]sketch.Block, float64, error) {
	c := e.Config
	m := len(e.window.Batches)
	blocks := make([]sketch.Block, 0, m)
	shared := secureagg.New(c.Rows * width)
	base := uint64(c.Seed) ^ salt
	batches := e.window.Batches
	if kind == "shared" {
		batches = [][][]int{e.window.Flatten(c.Clients)}
	}
	for cohort, clients := range batches {
		p := sketch.Params{Rows: c.Rows, Width: width, BucketSeed: base, SignSeed: base ^ 0x51a9}
		global := e.round - m + cohort + 1
		if kind != "shared" {
			p.SignSeed = sketch.Mix(base + uint64(global))
		}
		if kind == "fresh" {
			p.BucketSeed = sketch.Mix(base + uint64(global) + 0xf2e5)
		}
		sum := secureagg.New(c.Rows * width)
		for _, items := range clients {
			if kind == "shared" {
				if err := shared.Add(p.Encode(items)); err != nil {
					return nil, 0, err
				}
			} else {
				if err := sum.Add(p.Encode(items)); err != nil {
					return nil, 0, err
				}
			}
		}
		if kind != "shared" {
			blocks = append(blocks, sketch.Block{Params: p, Values: sum.Reveal()})
		}
	}
	sensitivity := 2 * float64(c.ContributionCap) * math.Sqrt(float64(m*c.Rows))
	if kind == "shared" {
		sensitivity = 2 * float64(c.ContributionCap*m) * math.Sqrt(float64(c.Rows))
		blocks = append(blocks, sketch.Block{Params: sketch.Params{Rows: c.Rows, Width: width, BucketSeed: base, SignSeed: base ^ 0x51a9}, Values: shared.Reveal()})
	}
	sigma := sensitivity * multiplier
	for i := range blocks {
		rng.Noise(blocks[i].Values, sigma)
	}
	if len(blocks) == 0 {
		return nil, 0, fmt.Errorf("empty sketch window")
	}
	return blocks, sigma, nil
}
