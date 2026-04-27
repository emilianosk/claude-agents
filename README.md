# Claude Agents - SkinKandy Roster Analysis MVP

FastAPI service scaffold for weekly roster analysis with:
- multi-source dataset extraction (Databricks SQL + Deputy API)
- CSV upload + profiling pipeline
- Claude multi-agent orchestration with parallel execution, strict JSON outputs, and consensus synthesis
- dynamic agent count/names/specs loaded from YAML + Markdown prompts
- containerized runtime

## Quick start

1. Create env file:

```bash
cp .env.example .env
```

2. Set credentials in `.env`:
- Databricks (`DATABRICKS_*`)
- Deputy (`DEPUTY_*`) if using Deputy extraction
- Salesforce (`SALESFORCE_*`) if using Salesforce query endpoints
- Lightspeed (`LS_*`) if using Lightspeed service infrastructure
- Claude (`ANTHROPIC_API_KEY`)

3. Start service (from `claude-agents/`):

```bash
docker compose -f ../docker-compose.yml up --build claude-agents
```

4. Health check (host):

```bash
curl http://localhost:8084/api/v1/health
```

## API flow

1. Create run:

```bash
curl -X POST http://localhost:8084/api/v1/runs/init
```

2. Extract configured datasets to CSV:

```bash
curl -X POST http://localhost:8084/api/v1/extract/<run_id> \
  -H "content-type: application/json" \
  -d '{"datasets": ["DATALAKE.DATA_LAKE_CONVERSION", "DEPUTY.PIERCERS_TEAM"]}'
```

3. Upload additional CSV manually (optional):

```bash
curl -X POST "http://localhost:8084/api/v1/upload/<run_id>/DATALAKE.POS_TRANSACTIONS" \
  -F "file=@/path/to/file.csv"
```

4. Run analysis:

```bash
curl -X POST http://localhost:8084/api/v1/analyze/<run_id> \
  -H "content-type: application/json" \
  -d '{"question": "Where are the forward 2-week serviceability risks and what roster changes should we make?"}'
```

Select specific agents and one consensus profile for a run:

```bash
curl -X POST http://localhost:8084/api/v1/analyze/<run_id> \
  -H "content-type: application/json" \
  -d '{
    "question": "Where are the forward 2-week serviceability risks and what roster changes should we make?",
    "selected_agents": ["researcher", "skillset"],
    "consensus_profile": "default"
  }'
```

Result markdown is written to `app/storage/results/<run_id>/analysis_result.md`.

The same analysis can be run from the command line inside the app environment after datasets exist for the run:

```bash
python -m app.cli analyze \
  --run-id <run_id> \
  --question "Where are the forward 2-week serviceability risks and what roster changes should we make?" \
  --agents researcher,skillset \
  --consensus default
```

When running through Docker, use the same `run_id` folder shown under `app/storage/uploads`:

```bash
make extract \
  RUN_ID=95e92a93dc1d \
  DATASETS=DEPUTY.PIERCERS_TEAM

make analyze \
  RUN_ID=95e92a93dc1d \
  QUESTION="Where are the forward 2-week serviceability risks and what roster changes should we make?" \
  AGENTS=researcher,skillset \
  CONSENSUS=default
```

CLI output matches the API response shape by default. Add `--summary` to `python -m app.cli analyze` if you only want the compact JSON summary.

## Dataset catalog

Use `DATASETS_CONFIG_FILE` in `.env`:
- `config/datasets.yaml`

Each dataset entry defines:
- `key` (e.g. `DATALAKE.DATA_LAKE_CONVERSION`, `DEPUTY.PIERCERS_TEAM`)
- `service` (`databricks`, `deputy`)
- `type` (`sql`, `api`)
- source details (`query_file` for SQL or `openapi_file` + `endpoint` + `method` for API)

Query files are a single set under:
- `config/queries/*.sql`

## Agent config

Use `AGENTS_CONFIG_FILE` in `.env`:
- `config/agents.yaml`

Prompts are loaded from:
- `config/prompts/<agent>_prompt.md`

Agents are declared once in YAML. Runtime requests can select a subset with `selected_agents` or `--agents`.

Consensus profiles are declared under `consensus_profiles`. Runtime requests select exactly one profile with `consensus_profile` or `--consensus`.

## Salesforce endpoints

- `GET /api/v1/salesforce/test`
- `POST /api/v1/salesforce/query`

Example:

```bash
curl -X POST http://localhost:8084/api/v1/salesforce/query \
  -H "content-type: application/json" \
  -d '{"soql":"SELECT Id, Name FROM Account LIMIT 5","query_all":false}'
```
