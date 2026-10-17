package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"streamstar/internal/engine"
	"streamstar/internal/privacy"
)

func TestH3MatchedSchedule(t *testing.T) {
	for _, rounds := range []int{1, 4, 60, 200} {
		for _, budget := range []float64{0.1, 1, 6, 12} {
			multiplier, schedule, err := h3Schedule(budget, 1e-5, rounds)
			if err != nil {
				t.Fatal(err)
			}
			central := privacy.Ledger{Delta: 1e-5, Limit: budget}
			local := central
			for _, epsilon := range schedule {
				if !central.Gaussian(multiplier, false) || !local.Local(epsilon) {
					t.Fatal("premature budget exhaustion")
				}
				if math.Abs(central.Epsilon()-local.Epsilon()) > 1e-10 {
					t.Fatal("prefix privacy budgets differ")
				}
			}
			if math.Abs(central.Epsilon()-budget) > 1e-10 {
				t.Fatal("final budget not reached")
			}
		}
	}
	for _, bad := range []float64{0, -1, math.NaN(), math.Inf(1), 1e9} {
		if _, _, err := h3Schedule(bad, 1e-5, 60); err == nil {
			t.Fatalf("accepted budget %g", bad)
		}
	}
}

func TestH3ExperimentArtifacts(t *testing.T) {
	root := t.TempDir()
	c := engine.DefaultConfig()
	c.Clients = 8
	c.Domain = 8
	c.EventsPerRound = 16
	c.TopK = 3
	c.DeterministicNoise = true
	config := filepath.Join(root, "config.json")
	if err := saveJSON(config, c); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "run")
	args := []string{"-config", config, "-rounds", "4", "-seeds", "2", "-budgets", "1,6", "-widths", "4", "-out", out}
	if err := experimentH3(args); err != nil {
		t.Fatal(err)
	}
	var summaries []h3Summary
	b, err := os.ReadFile(filepath.Join(out, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &summaries); err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 24 {
		t.Fatalf("got %d summaries", len(summaries))
	}
	for _, s := range summaries {
		if s.Releases != 4 || math.Abs(s.FinalEpsilon-s.Budget) > 1e-9 || s.MaxEpsilonGap > 1e-9 {
			t.Fatalf("invalid summary: %+v", s)
		}
		if s.Method != "hybrid" && s.Method != "local" {
			t.Fatal("unrelated method in H3")
		}
	}
	paths, _ := filepath.Glob(filepath.Join(out, "*.csv"))
	if len(paths) != 12 {
		t.Fatalf("got %d CSVs", len(paths))
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		records, err := csv.NewReader(file).ReadAll()
		file.Close()
		if err != nil || len(records) != 9 {
			t.Fatalf("incomplete CSV: %s %v", path, err)
		}
	}
	// Prevent stale output from an earlier run entering a newly generated report.
	if err := experimentH3(args); err == nil {
		t.Fatal("overwrote existing results")
	}
}

func TestH3RejectsInvalidSweepBeforeWriting(t *testing.T) {
	for _, flags := range [][]string{
		{"-budgets", "1,1"}, {"-budgets", "NaN"}, {"-budgets", ""},
		{"-widths", "16,16"}, {"-widths", "3"}, {"-widths", ""},
		{"-rounds", "0"}, {"-seeds", "1"},
	} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "output")
			args := append([]string{"-out", output}, flags...)
			if err := experimentH3(args); err == nil {
				t.Fatal("accepted invalid sweep")
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatal("created output for invalid sweep")
			}
		})
	}
}
