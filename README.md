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
