package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"streamstar/internal/engine"
	"streamstar/internal/server"
	"streamstar/internal/stream"
)

func main() {
	if err := run(os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Print(err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		args = []string{"serve"}
	}
	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "experiment":
		return experiment(args[1:])
	case "experiment-h3":
		return experimentH3(args[1:])
	case "replay":
		return replay(args[1:])
	case "help", "-h", "--help":
		fmt.Println("streamstar serve | experiment | experiment-h3 | replay\nUse a subcommand with -h for its flags.")
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func serve(args []string) error {
	f := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := f.String("addr", "127.0.0.1:5173", "HTTP listen address")
	config := f.String("config", "", "JSON config path")
	interval := f.Duration("interval", time.Second, "processing tick / partial Kafka batch timeout")
	source := f.String("source", "synthetic", "synthetic, replay, manual, or kafka")
	file := f.String("file", "", "replay file (.gz supported)")
	format := f.String("format", "kosarak", "jsonl, kosarak, gowalla, sentiment140, or aol")
	brokers := f.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := f.String("topic", "streamstar-events", "Kafka topic")
	group := f.String("group", "streamstar-research", "Kafka consumer group")
	if err := f.Parse(args); err != nil {
		return err
	}
	c, err := engine.LoadConfig(*config)
	if err != nil {
		return err
	}
	s, err := server.New(c, server.Options{Source: *source, File: *file, Format: *format, Interval: *interval, Brokers: strings.Split(*brokers, ","), Topic: *topic, Group: *group})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	h := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		if err := s.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("source stopped: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		h.Shutdown(shutdownCtx)
	}()
	log.Printf("Streamstar: http://%s · source=%s · research evaluation (truth metrics visible)", *addr, *source)
	if c.DeterministicNoise {
		log.Print("Deterministic privacy noise enabled: reproducibility mode; no deployment privacy guarantee")
	}
	err = h.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func experiment(args []string) error {
	f := flag.NewFlagSet("experiment", flag.ContinueOnError)
	config := f.String("config", "", "JSON config path")
	rounds := f.Int("rounds", 60, "number of rounds per run")
	seeds := f.Int("seeds", 3, "independent data/noise seeds")
	out := f.String("out", "results", "output directory")
	widths := f.String("widths", "16,32,64,128", "fixed-width sweep, comma separated")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *rounds < 1 || *seeds < 1 {
		return fmt.Errorf("rounds and seeds must be positive")
	}
	c, err := engine.LoadConfig(*config)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	type Summary struct {
		Seed           int64   `json:"seed"`
		InitialWidth   int     `json:"initial_width"`
		Method         string  `json:"method"`
		Target         string  `json:"target"`
		MeanLInf       float64 `json:"mean_linf"`
		MeanMSE        float64 `json:"mean_mse"`
		MeanNCR        float64 `json:"mean_ncr"`
		MeanLatencyMS  float64 `json:"mean_processing_ms"`
		FinalEpsilon   float64 `json:"final_epsilon"`
		Releases       int     `json:"releases"`
		TotalBytes     int64   `json:"total_bytes"`
		Exhausted      bool    `json:"exhausted"`
		RecoveryRounds []*int  `json:"recovery_rounds"`
	}
	summaries := []Summary{}
	for _, rawWidth := range strings.Split(*widths, ",") {
		width, err := strconv.Atoi(strings.TrimSpace(rawWidth))
		if err != nil {
			return err
		}
		for index := 0; index < *seeds; index++ {
			runConfig := c
			runConfig.Seed = c.Seed + int64(index)
			runConfig.Width = width
			if err := runConfig.Validate(); err != nil {
				return err
			}
			e, err := engine.New(runConfig)
			if err != nil {
				return err
			}
			source := stream.NewSynthetic(runConfig.Seed, runConfig.Domain, runConfig.Clients, runConfig.EventsPerRound, runConfig.DriftEvery)
			history := make([]engine.Round, 0, *rounds)
			for round := 0; round < *rounds; round++ {
				r, err := e.Process(source.Next())
				if err != nil {
					return err
				}
				history = append(history, r)
			}
			prefix := fmt.Sprintf("seed-%d-width-%d", runConfig.Seed, width)
			file, err := os.Create(filepath.Join(*out, prefix+".csv"))
			if err != nil {
				return err
			}
			err = engine.WriteCSV(file, history, runConfig.Seed)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			if err := saveJSON(filepath.Join(*out, prefix+".config.json"), runConfig); err != nil {
				return err
			}
			for methodIndex, method := range engine.Methods {
				s := Summary{Seed: runConfig.Seed, InitialWidth: width, Method: method.ID, Target: method.Target, RecoveryRounds: []*int{}}
				for _, round := range history {
					r := round.Results[methodIndex]
					s.MeanLInf += r.Metrics.LInf / float64(*rounds)
					s.MeanMSE += r.Metrics.MSE / float64(*rounds)
					s.MeanNCR += r.Metrics.NCR / float64(*rounds)
					s.MeanLatencyMS += r.LatencyMS / float64(*rounds)
				}
				last := history[len(history)-1].Results[methodIndex]
				s.FinalEpsilon = last.Epsilon
				s.Releases = last.Releases
				s.TotalBytes = last.TotalBytes
				s.Exhausted = last.Exhausted
				for driftIndex, round := range history {
					if !round.InjectedDrift {
						continue
					}
					var recovery *int
					for i := driftIndex; i < len(history); i++ {
						if i > driftIndex && history[i].InjectedDrift {
							break
						}
						if history[i].Results[methodIndex].Metrics.LInf <= runConfig.TargetError {
							value := i - driftIndex
							recovery = &value
							break
						}
					}
					s.RecoveryRounds = append(s.RecoveryRounds, recovery)
				}
				summaries = append(summaries, s)
			}
			log.Printf("completed %s: %d rounds", prefix, *rounds)
		}
	}
	if err := saveJSON(filepath.Join(*out, "summary.json"), summaries); err != nil {
		return err
	}
	fmt.Printf("Wrote measurements and summaries to %s\n", *out)
	return nil
}
func saveJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}
func replay(args []string) error {
	f := flag.NewFlagSet("replay", flag.ContinueOnError)
	config := f.String("config", "", "JSON config path")
	file := f.String("file", "", "dataset path (.gz supported)")
	format := f.String("format", "kosarak", "jsonl, kosarak, gowalla, sentiment140, or aol")
	out := f.String("out", "-", "JSONL output path, or - for stdout")
	brokers := f.String("brokers", "", "publish to comma-separated Kafka brokers instead of JSONL")
	topic := f.String("topic", "streamstar-events", "Kafka output topic")
	interval := f.Duration("interval", 0, "pause between batches")
	if err := f.Parse(args); err != nil {
		return err
	}
	c, err := engine.LoadConfig(*config)
	if err != nil {
		return err
	}
	r, err := stream.OpenReplay(*file, *format, c.Clients, c.Domain)
	if err != nil {
		return err
	}
	defer r.Close()
	var w io.Writer = os.Stdout
	if *brokers == "" && *out != "-" {
		file, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer file.Close()
		w = file
	}
	encoder := json.NewEncoder(w)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	for {
		b, err := r.Next(c.EventsPerRound)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if *brokers != "" {
			if err := stream.Publish(ctx, strings.Split(*brokers, ","), *topic, b.Events); err != nil {
				return err
			}
		} else {
			for _, event := range b.Events {
				if err := encoder.Encode(event); err != nil {
					return err
				}
			}
		}
		if *interval > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(*interval):
			}
		}
	}
	if *out != "-" && len(r.Labels) > 0 {
		return saveJSON(*out+".dictionary.json", r.Labels)
	}
	return nil
}
