.PHONY: up down logs test test-verbose test-debug shell

up:
	docker compose -f ../docker-compose.yml up --build -d claude-agents

down:
	docker compose -f ../docker-compose.yml stop claude-agents

logs:
	docker compose -f ../docker-compose.yml logs -f claude-agents

test:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents pytest -q

test-verbose:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents pytest -vv -ra --durations=10

test-debug:
	docker compose -f ../docker-compose.yml run --rm -e PYTHONPATH=/app -w /app claude-agents pytest -vv -ra -s --log-cli-level=INFO --durations=10

shell:
	docker compose -f ../docker-compose.yml run --rm claude-agents bash
