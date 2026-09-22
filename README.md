# SurrealDB Scenario Orchestrator for Buzz

A Python-based orchestration system that enables Buzz to run scenarios via SurrealDB, triggering Baseten chainlets for inference and Daytona sandboxes for code execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          SurrealDB                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Scenarios  │  │  Executions  │  │   Interests  │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│           │                │                                        │
│           ▼                ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  LIVE Queries  │  Custom Functions  │  Polling Queries       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Python Bridge Service                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Event Trigger│  │ Poll Trigger │  │Function Trig │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│           │                │                │                       │
│           └────────────────┼────────────────┘                       │
│                            ▼                                        │
│                   ┌──────────────┐                                  │
│                   │   Executor   │                                  │
│                   └──────────────┘                                  │
│                            │                                        │
│              ┌─────────────┼─────────────┐                         │
│              ▼             ▼             ▼                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   Baseten    │  │   Daytona    │  │    Buzz      │             │
│  │  Chainlets   │  │  Sandboxes   │  │  Publisher   │             │
│  └──────────────┘  └──────────────┘  └──────────────┘             │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

- **Three Trigger Mechanisms**:
  - **Event Trigger**: Real-time LIVE queries for immediate execution
  - **Poll Trigger**: Configurable polling for batch processing
  - **Function Trigger**: HTTP API for programmatic triggering

- **Baseten Integration**: Execute open-source models (Llama, Mistral, etc.)
- **Daytona Integration**: Isolated sandboxes for code execution
- **Buzz Integration**: Publish results as Nostr events to channels

## Installation

```bash
cd 02-scenario-orchestrator
pip install -e ".[dev]"
```

## Configuration

Copy the example environment file and configure:

```bash
cp .env.example .env
```

Required environment variables:

| Variable | Description |
|----------|-------------|
| `SURREALDB_URL` | SurrealDB WebSocket URL |
| `SURREALDB_NAMESPACE` | SurrealDB namespace |
| `SURREALDB_DATABASE` | SurrealDB database |
| `BASETEN_API_KEY` | Baseten API key |
| `BASETEN_CHAIN_ENDPOINT` | Deployed chainlet endpoint |
| `DAYTONA_API_KEY` | Daytona API key |
| `BUZZ_RELAY_URL` | Buzz relay WebSocket URL |
| `BUZZ_PRIVATE_KEY` | Nostr private key (hex) |

## Usage

### Start the Orchestrator

```bash
orchestrator
# or
python -m orchestrator.main
```

This starts:
- Event trigger (LIVE query listener)
- Poll trigger (30-second interval by default)
- Function trigger HTTP API (port 8080)

### Initialize SurrealDB Schema

```bash
surreal import --conn http://localhost:8000 surreal/schema.surql
surreal import --conn http://localhost:8000 surreal/functions.surql
```

### Create a Scenario

Using SurrealDB:

```sql
CREATE scenario CONTENT {
    name: "Test Scenario",
    interests: [
        { type: "prompt", name: "question", value: "What is machine learning?" }
    ],
    chainlet_config: {
        model: "meta-llama/Llama-3.1-8B-Instruct",
        parameters: { max_tokens: 500 }
    },
    buzz_config: {
        enabled: true,
        channel: "scenario-results"
    }
};
```

Using the HTTP API:

```bash
curl -X POST http://localhost:8080/trigger/create \
  -H "Content-Type: application/json" \
  -d '{
    "scenario": {
      "name": "API Test",
      "interests": [{"type": "prompt", "value": "Hello world"}],
      "chainlet_config": {"model": "llama-3"}
    }
  }'
```

### Trigger Execution

```bash
# Trigger a specific scenario
curl -X POST http://localhost:8080/trigger \
  -H "Content-Type: application/json" \
  -d '{"scenario_id": "scenario:abc123"}'

# Batch trigger
curl -X POST http://localhost:8080/trigger/batch \
  -H "Content-Type: application/json" \
  -d '{"scenario_ids": ["scenario:1", "scenario:2"]}'
```

### Get Statistics

```bash
curl http://localhost:8080/stats
```

## Durable Scenario Analysis Pipeline (MVP v1.1)

Extends the orchestrator with a **fixture-driven, Temporal-durable** analysis pipeline aligned to Clifford's Buzz Backend spec (sections 6–9, 16):

```
Trino/fixture → JSONL → Surreal load → NumPy draws → fn::score_v1 → TS gate → commit
```

| Spec section | MVP implementation |
|--------------|-------------------|
| §6 Extract | `POST /v1/extract` — Trino SQL or fixture JSONL path |
| §7 Load | `POST /v1/load` — idempotent batch upsert (`openapi_fact` + `scenario_entity`) |
| §8 Draw/Score | `POST /v1/draw`, `POST /v1/score` — seeded NumPy + `fn::score_one_v1` |
| §9 Gate/Commit | TypeScript `gate/` service (`POST /v1/gate`); `POST /v1/commit`, `/v1/compensate` |
| §16 Durability | `ScenarioPipelineWorkflow` + activities; worker via `pipeline-worker` |

**Division of responsibility:** Surreal holds data and light math; Python owns heavy math and I/O; **TypeScript is the keep-or-revert judge** (Python never decides keep).

### Pipeline schema

Import alongside the existing orchestrator schema (configurable ns/db via `SURREALDB_*`):

```bash
surreal import --conn http://localhost:8000 --ns buzz --db scenarios surreal/pipeline_schema.surql
surreal import --conn http://localhost:8000 --ns buzz --db scenarios surreal/pipeline_functions.surql
```

JSONL records follow the linkml-surreal contract: `id`, `source`, `pulled_at`, `payload_hash`, `payload`.

### Local stack (docker compose)

```bash
docker compose up -d surreal temporal gate
pip install -e ".[dev]"
orchestrator          # pipeline API on :8080 (/v1/* mounted)
pipeline-worker       # Temporal worker (optional for fixture demo)
```

### Promotion demo (golden fixture, no Trino/Baseten)

```bash
# 1. Extract fixture → JSONL
curl -s -X POST http://localhost:8080/v1/extract \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"demo-001","fixture_path":"fixtures/golden/entities.jsonl"}'

# 2. Load (batch 2 for resume demo)
curl -s -X POST http://localhost:8080/v1/load \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"demo-001","path":"/tmp/scenario-pipeline/demo-001.jsonl","batch_index":0,"batch_size":2}'

# 3. Draw (seeded)
curl -s -X POST 'http://localhost:8080/v1/draw?persist=true' \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"demo-001","seed":3,"bounds":[{"name":"multiplier","min":0.5,"max":2.0}],"num_samples":3}'

# 4. Score (SurrealQL — no keep/revert)
curl -s -X POST http://localhost:8080/v1/score \
  -H 'Content-Type: application/json' \
  -d @/tmp/score-request.json   # use draws from step 3

# 5. Gate (TypeScript judge on :8090)
curl -s -X POST http://localhost:8090/v1/gate \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"demo-001","scores":[...],"criteria":{"min_score":20.0}}'

# 6. Commit winners
curl -s -X POST http://localhost:8080/v1/commit \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"demo-001","winners":[...]}'
```

Run the golden e2e tests (embedded Surreal, no external services):

```bash
pytest tests/pipeline/ -v
```

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
02-scenario-orchestrator/
├── pyproject.toml           # Project configuration
├── .env.example             # Environment template
├── README.md
├── src/
│   ├── orchestrator/
│   │   ├── config.py        # Settings management
│   │   ├── main.py          # Entry point
│   │   ├── surrealdb/
│   │   │   ├── client.py    # SurrealDB client
│   │   │   ├── schema.py    # Pydantic models
│   │   │   ├── live_listener.py
│   │   │   └── functions.py
│   │   ├── triggers/
│   │   │   ├── event_trigger.py
│   │   │   ├── poll_trigger.py
│   │   │   └── function_trigger.py
│   │   └── executors/
│   │       ├── baseten.py
│   │       ├── daytona.py
│   │       └── buzz.py
│   └── chainlets/
│       ├── chain.yaml       # Baseten deployment config
│       └── chain_service.py
├── surreal/
│   ├── schema.surql         # SurrealDB schema
│   └── functions.surql      # Custom functions
└── tests/
```

## API Reference

### HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/stats` | GET | Execution statistics |
| `/pending` | GET | Pending execution count |
| `/trigger` | POST | Trigger single scenario |
| `/trigger/batch` | POST | Trigger multiple scenarios |
| `/trigger/create` | POST | Create and trigger scenario |
| `/v1/extract` | POST | Pipeline extract (Trino or fixture → JSONL) |
| `/v1/load` | POST | Batch upsert JSONL into Surreal |
| `/v1/draw` | POST | Seeded NumPy parameter draws |
| `/v1/score` | POST | SurrealQL scoring (no keep/revert) |
| `/v1/commit` | POST | Persist gate-approved winners |
| `/v1/compensate` | POST | Delete staged winners for run_id |

### SurrealDB Custom Functions

| Function | Description |
|----------|-------------|
| `fn::trigger_scenario` | Mark scenario for execution |
| `fn::complete_scenario` | Mark scenario completed |
| `fn::fail_scenario` | Mark scenario failed (with retry) |
| `fn::get_pending_scenarios` | Get pending by priority |
| `fn::get_ready_scenarios` | Get scenarios with met dependencies |
| `fn::check_dependencies_met` | Check if dependencies are completed |
| `fn::get_execution_stats` | Get aggregate statistics |

## License

Apache 2.0
