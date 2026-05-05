.PHONY: build test clippy fmt fmt-check bench demo report replay clean

CARGO ?= cargo
PYTHON ?= python

build:
	$(CARGO) build --workspace --release

test:
	$(CARGO) test --workspace --release

clippy:
	$(CARGO) clippy --workspace --all-targets -- -D warnings

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

bench:
	$(CARGO) bench -p rts-bench --bench sync_shootout
	$(CARGO) bench -p rts-bench --bench tail_latency

demo:
	@echo "Run scripts/demo.ps1 (Windows) or scripts/demo.sh (Linux)"

report: bench
	$(PYTHON) reports/plots/plot.py

replay:
	$(CARGO) run --release -p rts-cli -- replay play --fixture fixtures/recentchange-60s.ndjson --rate 10x

clean:
	$(CARGO) clean
	rm -rf reports/runs/*.ndjson reports/csv/*.csv reports/plots/*.png dhat-heap.json
