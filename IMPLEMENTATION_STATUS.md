# SkinKandy Roster Analysis - Implementation Status

## Completed now

- FastAPI project scaffold in `claude-agents/app`
- Endpoints for run initialization, CSV upload, Databricks extraction, and analysis orchestration
- Databricks SQL API client with PAT and Azure OAuth client-credentials support
- Config-driven query mapping per environment (`dev` and `prod`)
- SQL template files for all 10 dataset labels
- CSV profiling layer (rows, columns, missing values, numeric summaries)
- Claude orchestration with parallel 5 specialist agents (researcher, skillset, strategist, operator, creative)
- Strict JSON output contract for each agent response
- Consensus synthesis step with agreement and dissent reporting
- Dynamic agent loading from YAML config (`AGENTS_CONFIG_FILE`)
- Prompt loading from Markdown files (`AGENT_PROMPTS_DIR`) with `<agent>_prompt.md` convention
- Result file output (`analysis_result.md`)
- Deputy API service scaffold added (`app/services/deputy_service.py`)
- Salesforce API service scaffold added (`app/services/salesforce_service.py`)
- Lightspeed API service scaffold added (`app/services/lightspeed_service.py`)
- Multi-source dataset catalog added (`DATASETS_CONFIG_FILE`) supporting SQL (Databricks) and API (Deputy) extracts
- Container setup (`Dockerfile`, `.dockerfile`, `docker-compose.yml`, `.dockerignore`)
- Environment template (`.env.example`)

## To define / finish next

1. Replace placeholder SQL with validated data-lake queries for each dataset/table owner.
2. Confirm HH:MM granularity for:
- `KEPLER_HEATMAPS`
- `POS_TRANSACTIONS`
3. Confirm exact source and extract logic for:
- `NPS_DATA` (Salesforce or other source)
- `MARKETING_OPS_CALENDAR`
4. Add deterministic KPI computation layer before Claude prompts:
- walk-in conversion (strict definition)
- Kepler vs walk-in delta
- serviceability threshold checks (85% min)
- clinic utilisation flags (60-90 target)
5. Tune role prompts and per-agent dataset mappings after first live runs.
6. Generate final deliverables in required formats:
- Word report (`.docx`)
- 4-sheet Excel workbook (`.xlsx`)
7. Add background jobs and queue for large runs (Redis + Celery/RQ).
8. Add persistence for run metadata and recommendations (PostgreSQL).
9. Add authentication/authorization and audit logging.
10. Add robust tests (unit, integration, contract tests for each dataset schema).

## Open data dependencies from briefing/email

- Confirm all data-lake tables and ownership for datasets marked "we have".
- Confirm team member hourly availability feed into Deputy for live pulls.
- Confirm forward window cadence: every Monday, 2 weeks ahead.

## Suggested next implementation increment

1. Lock real SQL for datasets 1-9.
2. Build deterministic KPI engine.
3. Add 5-agent prompts with strict output schema.
4. Generate `.docx` and `.xlsx` outputs from structured results.
