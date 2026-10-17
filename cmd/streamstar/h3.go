package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"streamstar/internal/engine"
	"streamstar/internal/privacy"
	"streamstar/internal/stream"
)

type h3Summary struct {
	Scenario         string  `json:"scenario"`
	Budget           float64 `json:"budget"`
	Width            int     `json:"width"`
	Seed             int64   `json:"seed"`
	Method           string  `json:"method"`
	MeanLInf         float64 `json:"mean_linf"`
	MeanMSE          float64 `json:"mean_mse"`
	MeanNCR          float64 `json:"mean_ncr"`
	MeanProcessingMS float64 `json:"mean_processing_ms"`
	FinalEpsilon     float64 `json:"final_epsilon"`
	Releases         int     `json:"releases"`
	TotalBytes       int64   `json:"total_bytes"`
	MaxEpsilonGap    float64 `json:"max_epsilon_gap"`
}

// h3Schedule inverts epsilon = rho + 2 sqrt(rho log(1/delta)).
// Local reports spend the public difference between successive prefix epsilons.
// Thus both methods release every round at the same cumulative epsilon. Pure
// local DP is stronger than central (epsilon, delta)-DP at the same epsilon.
func h3Schedule(budget, delta float64, rounds int) (float64, []float64, error) {
	if rounds < 1 || math.IsNaN(budget) || math.IsInf(budget, 0) || budget <= 0 || delta <= 0 || delta >= 1 || math.IsNaN(delta) {
		return 0, nil, fmt.Errorf("invalid H3 rounds, budget, or delta")
	}
	logInverseDelta := -math.Log(delta)
	sqrtRho := budget / (math.Sqrt(logInverseDelta+budget) + math.Sqrt(logInverseDelta))
	multiplier := math.Sqrt(float64(rounds)/2) / sqrtRho
	if multiplier > 1e6 || math.IsInf(multiplier, 0) {
		return 0, nil, fmt.Errorf("H3 noise multiplier exceeds supported range")
	}
	// Round upward if floating-point addition would otherwise exhaust the cap.
	for attempt := 0; attempt < 100; attempt++ {
		ledger := privacy.Ledger{Delta: delta, Limit: budget}
		increments := make([]float64, rounds)
		previous := 0.0
		ok := true
		for i := range increments {
			if !ledger.Gaussian(multiplier, false) {
				ok = false
				break
			}
			current := ledger.Epsilon()
			increments[i] = current - previous
			if increments[i] <= 0 || increments[i] > 16 {
				return 0, nil, fmt.Errorf("H3 per-round local epsilon outside (0,16]")
			}
			previous = current
		}
		if ok {
			return multiplier, increments, nil
		}
		multiplier = math.Nextafter(multiplier, math.Inf(1))
	}
	return 0, nil, fmt.Errorf("could not calibrate H3 budget")
}

func experimentH3(args []string) error {
	f := flag.NewFlagSet("experiment-h3", flag.ContinueOnError)
	config := f.String("config", "", "base JSON config (noise mode is preserved)")
	rounds := f.Int("rounds", 60, "rounds per paired run")
	seeds := f.Int("seeds", 5, "independent paired data/noise seeds")
	budgetsFlag := f.String("budgets", "1,6,12", "cumulative epsilon budgets, comma separated")
	widthsFlag := f.String("widths", "16,64", "equal central/local widths, comma separated")
	out := f.String("out", "results/h3", "new or empty output directory")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *rounds < 1 || *seeds < 2 {
		return fmt.Errorf("H3 requires positive rounds and at least two seeds")
	}
	c, err := engine.LoadConfig(*config)
	if err != nil {
		return err
	}
	budgets, err := parseH3Budgets(*budgetsFlag, c.Delta, *rounds)
	if err != nil {
		return err
	}
	widths, err := parseH3Widths(*widthsFlag, c)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(*out)
	if err == nil && len(entries) > 0 {
		return fmt.Errorf("H3 output directory must be empty: %s", *out)
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	budgetValues := make([]float64, len(budgets))
	for i, budget := range budgets {
		budgetValues[i] = budget.epsilon
	}
	manifest := struct {
		Rounds          int           `json:"rounds"`
		Seeds           int           `json:"seeds"`
		Budgets         []float64     `json:"budgets"`
		Widths          []int         `json:"widths"`
		Config          engine.Config `json:"base_config"`
		PrivacySchedule string        `json:"privacy_schedule"`
		GoVersion       string        `json:"go_version"`
		Platform        string        `json:"platform"`
	}{
		Rounds: *rounds, Seeds: *seeds, Budgets: budgetValues, Widths: widths,
		Config: c, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
		PrivacySchedule: "constant central rho per round; local epsilon increments match central cumulative epsilon at every round; no probes; both release every round",
	}
	if err := saveJSON(filepath.Join(*out, "manifest.json"), manifest); err != nil {
		return err
	}

	var summaries []h3Summary
	for _, scenario := range []struct {
		name  string
		drift int
	}{{"stationary", 0}, {"scheduled", 15}, {"frequent", 5}} {
		for _, budget := range budgets {
			for _, width := range widths {
				for index := 0; index < *seeds; index++ {
					runConfig := c
					runConfig.Seed += int64(index)
					runConfig.Width, runConfig.LocalWidth = width, width
					runConfig.MinWidth, runConfig.MaxWidth = width, width
					runConfig.DriftEvery = scenario.drift
					runConfig.EpsilonLimit = budget.epsilon
					runConfig.NoiseMultiplier = budget.multiplier
					runConfig.LocalEpsilon = budget.localEpsilon[0]
					history, pair, err := runH3Pair(runConfig, scenario.name, budget.localEpsilon)
					if err != nil {
						return err
					}
					prefix := fmt.Sprintf("%s-epsilon-%g-width-%d-seed-%d", scenario.name, budget.epsilon, width, runConfig.Seed)
					if err := writeH3Run(*out, prefix, runConfig, budget.localEpsilon, history); err != nil {
						return err
					}
					summaries = append(summaries, pair...)
				}
				log.Printf("H3 %s epsilon=%g width=%d: %d paired seeds complete", scenario.name, budget.epsilon, width, *seeds)
			}
		}
	}
	return saveJSON(filepath.Join(*out, "summary.json"), summaries)
}

// Keep a budget and its calibrated spending schedule together throughout the sweep.
type h3Budget struct {
	epsilon      float64
	multiplier   float64
	localEpsilon []float64
}

func parseH3Budgets(raw string, delta float64, rounds int) ([]h3Budget, error) {
	var budgets []h3Budget
	seen := make(map[float64]bool)
	for _, value := range strings.Split(raw, ",") {
		epsilon, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid H3 budget %q: %w", value, err)
		}
		if seen[epsilon] {
			return nil, fmt.Errorf("duplicate H3 budget %g", epsilon)
		}
		multiplier, schedule, err := h3Schedule(epsilon, delta, rounds)
		if err != nil {
			return nil, err
		}
		seen[epsilon] = true
		budgets = append(budgets, h3Budget{epsilon: epsilon, multiplier: multiplier, localEpsilon: schedule})
	}
	return budgets, nil
}

func parseH3Widths(raw string, c engine.Config) ([]int, error) {
	var widths []int
	seen := make(map[int]bool)
	for _, value := range strings.Split(raw, ",") {
		width, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid H3 width %q: %w", value, err)
		}
		if seen[width] {
			return nil, fmt.Errorf("duplicate H3 width %d", width)
		}
		check := c
		check.Width, check.LocalWidth = width, width
		check.MinWidth, check.MaxWidth = width, width
		if err := check.Validate(); err != nil {
			return nil, err
		}
		seen[width] = true
		widths = append(widths, width)
	}
	return widths, nil
}

func runH3Pair(c engine.Config, scenario string, localEpsilon []float64) ([]engine.Round, []h3Summary, error) {
	e, err := engine.NewForMethods(c, "hybrid", "local")
	if err != nil {
		return nil, nil, err
	}
	source := stream.NewSynthetic(c.Seed, c.Domain, c.Clients, c.EventsPerRound, c.DriftEvery)
	history := make([]engine.Round, 0, len(localEpsilon))
	pair := make([]h3Summary, 2)
	for i, method := range []string{"hybrid", "local"} {
		pair[i] = h3Summary{Scenario: scenario, Budget: c.EpsilonLimit, Width: c.Width, Seed: c.Seed, Method: method}
	}
	maxGap := 0.0
	for round, epsilon := range localEpsilon {
		e.Config.LocalEpsilon = epsilon
		r, err := e.Process(source.Next())
		if err != nil {
			return nil, nil, err
		}
		if len(r.Results) != 2 || r.Results[0].ID != "hybrid" || r.Results[1].ID != "local" {
			return nil, nil, fmt.Errorf("unexpected H3 methods")
		}
		gap := math.Abs(r.Results[0].Epsilon - r.Results[1].Epsilon)
		if gap > 1e-9 || !r.Results[0].Released || !r.Results[1].Released {
			return nil, nil, fmt.Errorf("H3 privacy/release mismatch at round %d", round+1)
		}
		maxGap = math.Max(maxGap, gap)
		history = append(history, r)
		for i, result := range r.Results {
			s := &pair[i]
			s.MeanLInf += result.Metrics.LInf / float64(len(localEpsilon))
			s.MeanMSE += result.Metrics.MSE / float64(len(localEpsilon))
			s.MeanNCR += result.Metrics.NCR / float64(len(localEpsilon))
			s.MeanProcessingMS += result.LatencyMS / float64(len(localEpsilon))
			s.FinalEpsilon = result.Epsilon
			s.Releases = result.Releases
			s.TotalBytes = result.TotalBytes
		}
	}
	for i := range pair {
		pair[i].MaxEpsilonGap = maxGap
	}
	return history, pair, nil
}

func writeH3Run(directory, prefix string, c engine.Config, schedule []float64, history []engine.Round) error {
	file, err := os.Create(filepath.Join(directory, prefix+".csv"))
	if err != nil {
		return err
	}
	writeErr := engine.WriteCSV(file, history, c.Seed)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := saveJSON(filepath.Join(directory, prefix+".config.json"), c); err != nil {
		return err
	}
	return saveJSON(filepath.Join(directory, prefix+".local-epsilon.json"), schedule)
}
