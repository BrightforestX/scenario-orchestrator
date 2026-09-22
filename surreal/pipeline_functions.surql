-- ============================================================================
-- Pipeline scoring functions (versioned)
-- ============================================================================

-- fn::score_one_v1 — score a single draw; Python aggregates into ScenarioScore[].
DEFINE FUNCTION fn::score_one_v1($run_id: string, $draw: object) {
    LET $entities = (SELECT * FROM scenario_entity WHERE run_id = $run_id);
    LET $params = IF type::is::object($draw.params) THEN $draw.params ELSE {} END;
    LET $multiplier = IF $params.multiplier != NONE THEN $params.multiplier ELSE 1.0 END;
    LET $parts = (
        SELECT
            (
                IF $this.payload != NONE AND $this.payload.value != NONE THEN $this.payload.value
                ELSE IF $this.value != NONE THEN $this.value
                ELSE 0.0 END
            ) * (
                IF $this.payload != NONE AND $this.payload.weight != NONE THEN $this.payload.weight
                ELSE IF $this.weight != NONE THEN $this.weight
                ELSE 1.0 END
            ) * $multiplier AS part
        FROM $entities
    );
    LET $sum = math::sum((SELECT $this.part AS p FROM $parts).p);
    LET $draw_index = IF $draw.draw_index != NONE THEN $draw.draw_index ELSE 0 END;
    LET $scenario_id = IF $draw.scenario_id != NONE THEN $draw.scenario_id ELSE string::concat($run_id, '_draw_', type::string($draw_index)) END;
    RETURN {
        scenario_id: $scenario_id,
        draw_index: $draw_index,
        score: $sum,
        metrics: { multiplier: $multiplier, entity_count: array::len($entities) }
    };
};

-- fn::score_v1 — batch wrapper kept for spec compatibility; iterates draws in SurrealQL.
DEFINE FUNCTION fn::score_v1($run_id: string, $draws: array) {
    LET $scores = [];
    FOR $draw IN $draws {
        LET $entry = fn::score_one_v1($run_id, $draw);
        LET $scores = array::append($scores, $entry);
    };
    RETURN $scores;
};

-- Delete staged winners for compensation
DEFINE FUNCTION fn::compensate_run($run_id: string) {
    DELETE scenario_winner WHERE run_id = $run_id AND committed = false;
    UPDATE scenario_run SET status = 'compensated', updated_at = time::now() WHERE run_id = $run_id;
    RETURN true;
};
