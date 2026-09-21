-- ============================================================================
-- SurrealDB Schema for Scenario Orchestrator
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Scenario Table
-- Represents a scenario/permutation of interests to execute
-- ----------------------------------------------------------------------------
DEFINE TABLE scenario SCHEMAFULL;

DEFINE FIELD name ON scenario TYPE string
    ASSERT $value != NONE AND $value != "";
    
DEFINE FIELD description ON scenario TYPE option<string>;

DEFINE FIELD status ON scenario TYPE string 
    DEFAULT 'pending'
    ASSERT $value IN ['pending', 'running', 'completed', 'failed', 'cancelled'];

DEFINE FIELD interests ON scenario TYPE array<object>
    DEFAULT []
    COMMENT "Array of interest objects that define the scenario permutation";

DEFINE FIELD chainlet_config ON scenario TYPE object
    COMMENT "Configuration for Baseten chainlet execution";

DEFINE FIELD sandbox_config ON scenario TYPE option<object>
    COMMENT "Optional Daytona sandbox configuration";

DEFINE FIELD buzz_config ON scenario TYPE option<object>
    COMMENT "Optional Buzz channel configuration for results";

DEFINE FIELD metadata ON scenario TYPE option<object>
    COMMENT "Additional metadata for the scenario";

DEFINE FIELD priority ON scenario TYPE int DEFAULT 0
    COMMENT "Execution priority (higher = more urgent)";

DEFINE FIELD max_retries ON scenario TYPE int DEFAULT 3;
DEFINE FIELD retry_count ON scenario TYPE int DEFAULT 0;

DEFINE FIELD created_at ON scenario TYPE datetime DEFAULT time::now();
DEFINE FIELD updated_at ON scenario TYPE datetime DEFAULT time::now();
DEFINE FIELD executed_at ON scenario TYPE option<datetime>;

-- Indexes for efficient queries
DEFINE INDEX idx_scenario_status ON scenario FIELDS status;
DEFINE INDEX idx_scenario_priority ON scenario FIELDS priority;
DEFINE INDEX idx_scenario_created ON scenario FIELDS created_at;

-- ----------------------------------------------------------------------------
-- Execution Table
-- Records the execution of a scenario
-- ----------------------------------------------------------------------------
DEFINE TABLE execution SCHEMAFULL;

DEFINE FIELD scenario ON execution TYPE record<scenario>;

DEFINE FIELD trigger_type ON execution TYPE string
    ASSERT $value IN ['event', 'poll', 'function'];

DEFINE FIELD status ON execution TYPE string 
    DEFAULT 'started'
    ASSERT $value IN ['started', 'chainlet_running', 'sandbox_running', 'publishing', 'completed', 'failed'];

DEFINE FIELD chainlet_request ON execution TYPE option<object>
    COMMENT "Request sent to Baseten chainlet";

DEFINE FIELD chainlet_result ON execution TYPE option<object>
    COMMENT "Response from Baseten chainlet";

DEFINE FIELD chainlet_latency_ms ON execution TYPE option<int>;

DEFINE FIELD sandbox_id ON execution TYPE option<string>
    COMMENT "Daytona sandbox ID if created";

DEFINE FIELD sandbox_output ON execution TYPE option<string>
    COMMENT "Output from sandbox execution";

DEFINE FIELD buzz_event_id ON execution TYPE option<string>
    COMMENT "Nostr event ID published to Buzz";

DEFINE FIELD error ON execution TYPE option<string>
    COMMENT "Error message if execution failed";

DEFINE FIELD started_at ON execution TYPE datetime DEFAULT time::now();
DEFINE FIELD completed_at ON execution TYPE option<datetime>;

-- Indexes
DEFINE INDEX idx_execution_scenario ON execution FIELDS scenario;
DEFINE INDEX idx_execution_status ON execution FIELDS status;
DEFINE INDEX idx_execution_trigger ON execution FIELDS trigger_type;

-- ----------------------------------------------------------------------------
-- Depends On Relation
-- Graph edge representing scenario dependencies
-- ----------------------------------------------------------------------------
DEFINE TABLE depends_on SCHEMAFULL;

DEFINE FIELD in ON depends_on TYPE record<scenario>;
DEFINE FIELD out ON depends_on TYPE record<scenario>;
DEFINE FIELD condition ON depends_on TYPE option<string>
    COMMENT "Optional condition that must be met";

-- ----------------------------------------------------------------------------
-- Interest Table
-- Reusable interest definitions that can be referenced in scenarios
-- ----------------------------------------------------------------------------
DEFINE TABLE interest SCHEMAFULL;

DEFINE FIELD name ON interest TYPE string
    ASSERT $value != NONE AND $value != "";

DEFINE FIELD type ON interest TYPE string
    ASSERT $value IN ['prompt', 'code', 'data', 'config'];

DEFINE FIELD value ON interest TYPE any
    COMMENT "The interest value - can be string, object, or array";

DEFINE FIELD metadata ON interest TYPE option<object>;

DEFINE FIELD created_at ON interest TYPE datetime DEFAULT time::now();

DEFINE INDEX idx_interest_name ON interest FIELDS name;
DEFINE INDEX idx_interest_type ON interest FIELDS type;

-- ----------------------------------------------------------------------------
-- Custom Functions
-- ----------------------------------------------------------------------------

-- Function to trigger scenario execution via the bridge service
DEFINE FUNCTION fn::trigger_scenario($scenario_id: record<scenario>) {
    -- Update scenario status to running
    UPDATE $scenario_id SET 
        status = 'running',
        updated_at = time::now();
    
    -- Return the scenario for the trigger service
    RETURN SELECT * FROM $scenario_id;
};

-- Function to mark scenario as completed
DEFINE FUNCTION fn::complete_scenario($scenario_id: record<scenario>, $execution_id: record<execution>) {
    UPDATE $scenario_id SET 
        status = 'completed',
        executed_at = time::now(),
        updated_at = time::now();
    
    UPDATE $execution_id SET 
        status = 'completed',
        completed_at = time::now();
    
    RETURN true;
};

-- Function to mark scenario as failed
DEFINE FUNCTION fn::fail_scenario($scenario_id: record<scenario>, $execution_id: record<execution>, $error: string) {
    LET $scenario = SELECT * FROM $scenario_id;
    LET $should_retry = $scenario[0].retry_count < $scenario[0].max_retries;
    
    IF $should_retry {
        UPDATE $scenario_id SET 
            status = 'pending',
            retry_count = retry_count + 1,
            updated_at = time::now();
    } ELSE {
        UPDATE $scenario_id SET 
            status = 'failed',
            updated_at = time::now();
    };
    
    UPDATE $execution_id SET 
        status = 'failed',
        error = $error,
        completed_at = time::now();
    
    RETURN $should_retry;
};

-- Function to get pending scenarios ordered by priority
DEFINE FUNCTION fn::get_pending_scenarios($limit: int) {
    RETURN SELECT * FROM scenario 
        WHERE status = 'pending'
        ORDER BY priority DESC, created_at ASC
        LIMIT $limit;
};

-- Function to get scenario with its dependencies
DEFINE FUNCTION fn::get_scenario_with_deps($scenario_id: record<scenario>) {
    RETURN SELECT 
        *,
        ->depends_on->scenario AS dependencies,
        <-depends_on<-scenario AS dependents
    FROM $scenario_id;
};
