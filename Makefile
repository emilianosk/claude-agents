.PHONY: up down logs test shell

up:
	docker compose -f ../docker-compose.yml up --build -d claude-agents

down:
	docker compose -f ../docker-compose.yml stop claude-agents

logs:
	docker compose -f ../docker-compose.yml logs -f claude-agents

test:
	docker compose -f ../docker-compose.yml run --rm claude-agents pytest -q

shell:
	docker compose -f ../docker-compose.yml run --rm claude-agents bash
