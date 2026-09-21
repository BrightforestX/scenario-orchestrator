-- ============================================================================
-- Additional SurrealDB Custom Functions for Scenario Orchestrator
-- These functions provide HTTP-callable endpoints via SurrealDB Edge Functions
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Batch Operations
-- ----------------------------------------------------------------------------

-- Create multiple scenarios at once
DEFINE FUNCTION fn::batch_create_scenarios($scenarios: array<object>) {
    LET $created = [];
    
    FOR $scenario IN $scenarios {
        LET $new = CREATE scenario CONTENT {
            name: $scenario.name,
            description: $scenario.description,
            interests: $scenario.interests,
            chainlet_config: $scenario.chainlet_config,
            sandbox_config: $scenario.sandbox_config,
            buzz_config: $scenario.buzz_config,
            metadata: $scenario.metadata,
            priority: $scenario.priority OR 0
        };
        $created = array::push($created, $new);
    };
    
    RETURN $created;
};

-- Cancel all pending scenarios
DEFINE FUNCTION fn::cancel_pending_scenarios() {
    UPDATE scenario SET 
        status = 'cancelled',
        updated_at = time::now()
    WHERE status = 'pending';
    
    RETURN SELECT count() FROM scenario WHERE status = 'cancelled' GROUP ALL;
};

-- ----------------------------------------------------------------------------
-- Statistics Functions
-- ----------------------------------------------------------------------------

-- Get execution statistics
DEFINE FUNCTION fn::get_execution_stats() {
    RETURN {
        total_scenarios: (SELECT count() FROM scenario GROUP ALL)[0].count,
        pending: (SELECT count() FROM scenario WHERE status = 'pending' GROUP ALL)[0].count,
        running: (SELECT count() FROM scenario WHERE status = 'running' GROUP ALL)[0].count,
        completed: (SELECT count() FROM scenario WHERE status = 'completed' GROUP ALL)[0].count,
        failed: (SELECT count() FROM scenario WHERE status = 'failed' GROUP ALL)[0].count,
        total_executions: (SELECT count() FROM execution GROUP ALL)[0].count,
        avg_latency_ms: (SELECT math::mean(chainlet_latency_ms) AS avg FROM execution WHERE chainlet_latency_ms != NONE GROUP ALL)[0].avg
    };
};

-- Get recent executions
DEFINE FUNCTION fn::get_recent_executions($limit: int) {
    RETURN SELECT 
        *,
        scenario.name AS scenario_name,
        scenario.status AS scenario_status
    FROM execution 
    ORDER BY started_at DESC 
    LIMIT $limit;
};

-- ----------------------------------------------------------------------------
-- Dependency Management
-- ----------------------------------------------------------------------------

-- Check if all dependencies are completed
DEFINE FUNCTION fn::check_dependencies_met($scenario_id: record<scenario>) {
    LET $deps = SELECT ->depends_on->scenario AS deps FROM $scenario_id;
    LET $unmet = SELECT count() FROM scenario 
        WHERE id IN $deps[0].deps AND status != 'completed' 
        GROUP ALL;
    
    RETURN $unmet[0].count == 0 OR $unmet[0].count == NONE;
};

-- Get scenarios ready to execute (pending with met dependencies)
DEFINE FUNCTION fn::get_ready_scenarios($limit: int) {
    LET $pending = SELECT * FROM scenario WHERE status = 'pending';
    LET $ready = [];
    
    FOR $scenario IN $pending {
        IF fn::check_dependencies_met($scenario.id) {
            $ready = array::push($ready, $scenario);
        };
        IF array::len($ready) >= $limit {
            BREAK;
        };
    };
    
    RETURN $ready;
};

-- ----------------------------------------------------------------------------
-- Interest Permutation
-- ----------------------------------------------------------------------------

-- Generate scenario permutations from interests
DEFINE FUNCTION fn::generate_permutations($base_config: object, $interest_sets: array<array>) {
    -- This is a simplified version - full cartesian product would need more complex logic
    LET $scenarios = [];
    
    FOR $interest_set IN $interest_sets {
        LET $scenario = CREATE scenario CONTENT {
            name: $base_config.name + " - Variant " + string::concat(array::len($scenarios) + 1),
            description: $base_config.description,
            interests: $interest_set,
            chainlet_config: $base_config.chainlet_config,
            sandbox_config: $base_config.sandbox_config,
            buzz_config: $base_config.buzz_config,
            priority: $base_config.priority OR 0
        };
        $scenarios = array::push($scenarios, $scenario);
    };
    
    RETURN $scenarios;
};

-- ----------------------------------------------------------------------------
-- Cleanup Functions
-- ----------------------------------------------------------------------------

-- Archive old completed scenarios (move to archive table)
DEFINE FUNCTION fn::archive_old_scenarios($days_old: int) {
    LET $cutoff = time::now() - duration::from::days($days_old);
    
    LET $to_archive = SELECT * FROM scenario 
        WHERE status IN ['completed', 'failed', 'cancelled']
        AND updated_at < $cutoff;
    
    -- In a real implementation, you'd move these to an archive table
    -- For now, we just return the count
    RETURN {
        archived_count: array::len($to_archive),
        scenarios: $to_archive
    };
};

-- Delete orphaned executions (executions without scenarios)
DEFINE FUNCTION fn::cleanup_orphaned_executions() {
    DELETE execution WHERE scenario NOT IN (SELECT id FROM scenario);
    RETURN true;
};
