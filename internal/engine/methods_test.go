package engine

import (
	"reflect"
	"testing"

	"streamstar/internal/stream"
)

func TestSelectedMethodsPreserveResults(t *testing.T) {
	c := DefaultConfig()
	c.Clients = 8
	c.Domain = 8
	c.TopK = 3
	c.EventsPerRound = 16
	c.DeterministicNoise = true
	all, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := NewForMethods(c, "hybrid", "local")
	if err != nil {
		t.Fatal(err)
	}
	source := stream.NewSynthetic(c.Seed, c.Domain, c.Clients, c.EventsPerRound, c.DriftEvery)
	for i := 0; i < 3; i++ {
		batch := source.Next()
		full, err := all.Process(batch)
		if err != nil {
			t.Fatal(err)
		}
		pair, err := selected.Process(batch)
		if err != nil {
			t.Fatal(err)
		}
		if len(pair.Results) != 2 {
			t.Fatal("unexpected methods")
		}
		for j, index := range []int{2, 5} {
			a, b := full.Results[index], pair.Results[j]
			a.LatencyMS = 0
			b.LatencyMS = 0
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("selected %s differs from full engine", a.ID)
			}
		}
	}
	for _, ids := range [][]string{{"unknown"}, {"hybrid", "hybrid"}} {
		if _, err := NewForMethods(c, ids...); err == nil {
			t.Fatalf("accepted %v", ids)
		}
	}
}
