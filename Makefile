.PHONY: run test build experiment experiment-h3 kafka-up kafka-down ui ui-dev
run:
	go run ./cmd/streamstar serve
test:
	CGO_ENABLED=1 go test -race ./...
	go vet ./...
	python3 -m unittest discover -s scripts -p 'test_*.py'
build:
	go build -o bin/streamstar ./cmd/streamstar
experiment:
	go run ./cmd/streamstar experiment -rounds 60 -seeds 3 -out results
experiment-h3:
	go run ./cmd/streamstar experiment-h3 -config configs/reproducible.json -out results/h3
	python3 scripts/report_h3.py results/h3
ui:
	cd ui && npm ci && npm run check && npm test && npm run build
ui-dev:
	cd ui && npm run dev
kafka-up:
	docker compose up -d
kafka-down:
	docker compose down
