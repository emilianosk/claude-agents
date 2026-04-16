# Claude Agents - SkinKandy Roster Analysis MVP

FastAPI service scaffold for weekly roster analysis with:
- dataset extraction from Databricks SQL Warehouse (configurable SQL per dataset and environment)
- CSV upload + profiling pipeline
- Claude multi-agent orchestration with parallel execution, strict JSON outputs, and consensus synthesis
- dynamic agent count/names/specs loaded from config YAML + Markdown prompt files
- containerized runtime for local and CI tests

## Quick start

1. Create env file:

```bash
cp .env.example .env
```

2. Set credentials in `.env`:
- Databricks (`DATABRICKS_*`)
- Deputy (`DEPUTY_*`) if using Deputy API integration
- Salesforce (`SALESFORCE_*`) if using Salesforce integration
- Lightspeed (`LS_*`) if using Lightspeed integration
- Claude (`ANTHROPIC_API_KEY`)

3. Start service:

```bash
docker compose -f ../docker-compose.yml up --build claude-agents
```

4. Health check:

```bash
curl http://localhost:8080/api/v1/health
```

## API flow

1. Create run:

```bash
curl -X POST http://localhost:8080/api/v1/runs/init
```

2. Extract configured datasets to CSV:

```bash
curl -X POST http://localhost:8080/api/v1/extract/<run_id> \
  -H "content-type: application/json" \
  -d '{"datasets": ["DATA_LAKE_CONVERSION", "TEAM_AVAILABILITY"]}'
```

3. Upload additional CSV manually (if needed):

```bash
curl -X POST "http://localhost:8080/api/v1/upload/<run_id>/TARGETS" \
  -F "file=@/path/to/targets.csv"
```

4. Run analysis:

```bash
curl -X POST http://localhost:8080/api/v1/analyze/<run_id> \
  -H "content-type: application/json" \
  -d '{"question": "Where are the forward 2-week serviceability risks and what roster changes should we make?"}'
```

Result markdown is written to `app/storage/results/<run_id>/analysis_result.md`.

The analyze response includes:
- `agent_outputs`: structured JSON from each configured agent (e.g. `researcher`, `skillset`, `strategist`, `operator`, `creative`)
- `consensus`: consensus-level, agreement count, dissenting agents, and final recommendation
- `final_decision`: same as `consensus.final_recommendation`

## Query configuration

Use `QUERY_MAP_FILE` in `.env`:
- `config/query-map.dev.json`
- `config/query-map.prod.json`

Each dataset label maps to a `.sql` file under:
- `config/queries/dev`
- `config/queries/prod`

Note: extraction now uses the dataset catalog (`DATASETS_CONFIG_FILE`) for routing. `QUERY_MAP_FILE` is legacy and can be phased out.

## Dataset catalog (multi-source)

Use `DATASETS_CONFIG_FILE` in `.env`:
- `config/datasets.dev.yaml`
- `config/datasets.prod.yaml`

Each dataset entry defines:
- `key` (e.g. `DATALAKE.DATA_LAKE_CONVERSION`, `DEPUTY.TEAM_AVAILABILITY`)
- `service` (`databricks`, `deputy`)
- `type` (`sql`, `api`)
- source details (`query_file` for SQL or `openapi_file` + `endpoint` + `method` for API)

## Dataset labels

- `KEPLER_HEATMAPS`
- `DATA_LAKE_CONVERSION`
- `POS_TRANSACTIONS`
- `TEAM_AVAILABILITY`
- `ROSTER_FORECAST`
- `TARGETS`
- `SERVICEABILITY_MATRIX`
- `CLINIC_UTILISATION`
- `NPS_DATA`
- `MARKETING_OPS_CALENDAR`

## Deputy service

- Python Deputy API client is available at:
`app/services/deputy_service.py`
- It mirrors key patterns from the PHP service:
employee lookup, operational unit queries, roster queries, training record queries, and store-by-LID lookup.

## Salesforce service

- Python Salesforce API client is available at:
`app/services/salesforce_service.py`
- It mirrors key PHP patterns:
OAuth refresh-token auth, generic REST requests, SOQL query/query-all infrastructure, appointment SOQL queries, and WorkType lookup by SID.
- API endpoints available:
  - `GET /api/v1/salesforce/test`
  - `POST /api/v1/salesforce/query`

Example SOQL query request:

```bash
curl -X POST http://localhost:8080/api/v1/salesforce/query \
  -H "content-type: application/json" \
  -d '{"soql":"SELECT Id, Name FROM Account LIMIT 5","query_all":false}'
```

## Lightspeed service

- Python Lightspeed API client is available at:
`app/services/lightspeed_service.py`
- It mirrors key PHP patterns:
multi-region config (AU/NZ), base URL/version handling, authenticated generic requests, and connection testing.

## Multi-agent behavior

- Agents run in parallel via `asyncio.gather` (single request fan-out).
- Agent definitions are loaded from `AGENTS_CONFIG_FILE` (default `config/agents.dev.yaml`).
- Each agent prompt is loaded from `AGENT_PROMPTS_DIR` (default `config/prompts`), using `<agent>_prompt.md` naming unless overridden in YAML.
- Each agent must return strict JSON:
`verdict`, `confidence`, `key_findings`, `risks`, `actions`, `data_caveats`.
- A consensus step consumes all agent outputs and returns strict JSON:
`consensus_level`, `agreement_count`, `dissenting_agents`, `common_actions`, `unresolved_risks`, `final_recommendation`.

## Agent configuration example

```yaml
agents:
  - name: researcher
    enabled: true
    prompt_file: researcher_prompt.md
    input_datasets: [KEPLER_HEATMAPS, DATA_LAKE_CONVERSION, POS_TRANSACTIONS]
consensus:
  prompt_file: consensus_prompt.md
  min_agents: 3
```
