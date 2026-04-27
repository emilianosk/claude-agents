.PHONY: up down logs extract features analyze test test-verbose test-debug shell

CONSENSUS ?= default

up:
	docker compose -f ../docker-compose.yml up --build -d claude-agents

down:
	docker compose -f ../docker-compose.yml stop claude-agents

logs:
	docker compose -f ../docker-compose.yml logs -f claude-agents

extract:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents python -m app.cli extract --run-id "$(RUN_ID)" --datasets "$(DATASETS)"

features:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents python -m app.cli features --run-id "$(RUN_ID)" --features "$(FEATURES)"

analyze:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents python -m app.cli analyze --run-id "$(RUN_ID)" --question "$(QUESTION)" --agents "$(AGENTS)" --consensus "$(CONSENSUS)"

test:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents pytest -q

test-verbose:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents pytest -vv -ra --durations=10

test-debug:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents pytest -vv -ra -s --log-cli-level=INFO --durations=10

shell:
	docker compose -f ../docker-compose.yml run --rm claude-agents bash
