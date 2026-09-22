-- ============================================================================
-- Durable Scenario Analysis Pipeline Schema (MVP v1.1)
-- Aligns with linkml-surreal JSONL contract: id, source, pulled_at,
-- payload_hash, payload — plus generic scenario_entity rows for scoring fixtures.
-- ============================================================================

-- Pipeline run metadata
DEFINE TABLE scenario_run SCHEMAFULL;
DEFINE FIELD run_id ON scenario_run TYPE string
    ASSERT $value != NONE AND $value != "";
DEFINE FIELD status ON scenario_run TYPE string DEFAULT 'pending'
    ASSERT $value IN ['pending', 'extracting', 'loading', 'drawing', 'scoring', 'gating', 'committed', 'compensated', 'failed'];
DEFINE FIELD checksum ON scenario_run TYPE option<string>;
DEFINE FIELD seed ON scenario_run TYPE option<int>;
DEFINE FIELD jsonl_path ON scenario_run TYPE option<string>;
DEFINE FIELD created_at ON scenario_run TYPE datetime DEFAULT time::now();
DEFINE FIELD updated_at ON scenario_run TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_scenario_run_run_id ON scenario_run FIELDS run_id UNIQUE;

-- Ontology facts (linkml-surreal JSONL contract)
DEFINE TABLE openapi_fact SCHEMAFULL;
DEFINE FIELD id ON openapi_fact TYPE string;
DEFINE FIELD source ON openapi_fact TYPE string;
DEFINE FIELD pulled_at ON openapi_fact TYPE string
    COMMENT "ISO-8601 timestamp from JSONL; cast with type::datetime() in queries when needed";
DEFINE FIELD payload_hash ON openapi_fact TYPE string;
DEFINE FIELD payload ON openapi_fact TYPE object;
DEFINE FIELD run_id ON openapi_fact TYPE option<string>;
DEFINE INDEX idx_openapi_fact_run ON openapi_fact FIELDS run_id;

-- Generic entity rows for scenario scoring fixtures
DEFINE TABLE scenario_entity SCHEMAFULL;
DEFINE FIELD id ON scenario_entity TYPE string;
DEFINE FIELD run_id ON scenario_entity TYPE option<string>;
DEFINE FIELD value ON scenario_entity TYPE option<float>;
DEFINE FIELD weight ON scenario_entity TYPE option<float> DEFAULT 1.0;
DEFINE FIELD payload ON scenario_entity TYPE option<object>;
DEFINE FIELD source ON scenario_entity TYPE option<string>;
DEFINE FIELD pulled_at ON scenario_entity TYPE option<string>;
DEFINE FIELD payload_hash ON scenario_entity TYPE option<string>;
DEFINE INDEX idx_scenario_entity_run ON scenario_entity FIELDS run_id;

-- Parameter draws (NumPy-generated, stored for reproducibility)
DEFINE TABLE scenario_draw SCHEMAFULL;
DEFINE FIELD run_id ON scenario_draw TYPE string;
DEFINE FIELD draw_index ON scenario_draw TYPE int;
DEFINE FIELD scenario_id ON scenario_draw TYPE string;
DEFINE FIELD params ON scenario_draw TYPE object;
DEFINE FIELD seed ON scenario_draw TYPE int;
DEFINE INDEX idx_scenario_draw_run ON scenario_draw FIELDS run_id;

-- Shortlist (pre-gate scored candidates)
DEFINE TABLE scenario_shortlist SCHEMAFULL;
DEFINE FIELD run_id ON scenario_shortlist TYPE string;
DEFINE FIELD scenario_id ON scenario_shortlist TYPE string;
DEFINE FIELD draw_index ON scenario_shortlist TYPE int;
DEFINE FIELD score ON scenario_shortlist TYPE float;
DEFINE FIELD metrics ON scenario_shortlist TYPE option<object>;
DEFINE INDEX idx_scenario_shortlist_run ON scenario_shortlist FIELDS run_id;

-- Winners (post-gate, staged until commit)
DEFINE TABLE scenario_winner SCHEMAFULL;
DEFINE FIELD run_id ON scenario_winner TYPE string;
DEFINE FIELD scenario_id ON scenario_winner TYPE string;
DEFINE FIELD draw_index ON scenario_winner TYPE int;
DEFINE FIELD score ON scenario_winner TYPE float;
DEFINE FIELD metrics ON scenario_winner TYPE option<object>;
DEFINE FIELD committed ON scenario_winner TYPE bool DEFAULT false;
DEFINE FIELD staged_at ON scenario_winner TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_scenario_winner_run ON scenario_winner FIELDS run_id;

-- Load progress for idempotent resume
DEFINE TABLE load_progress SCHEMAFULL;
DEFINE FIELD run_id ON load_progress TYPE string;
DEFINE FIELD table_name ON load_progress TYPE string;
DEFINE FIELD last_batch_index ON load_progress TYPE int DEFAULT -1;
DEFINE FIELD records_loaded ON load_progress TYPE int DEFAULT 0;
DEFINE INDEX idx_load_progress_run ON load_progress FIELDS run_id, table_name UNIQUE;
