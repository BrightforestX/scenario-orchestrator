/** Keep-or-revert gate — TypeScript is the judge; Python never decides keep. */

export interface ScenarioScore {
  scenario_id: string;
  draw_index: number;
  score: number;
  metrics?: Record<string, unknown>;
}

export interface GateCriteria {
  min_score: number;
  max_variance?: number;
}

export interface GateDecision {
  scenario_id: string;
  draw_index: number;
  decision: "keep" | "revert";
  reason: string;
  score: number;
}

export interface WinnerRecord {
  scenario_id: string;
  draw_index: number;
  score: number;
  metrics: Record<string, unknown>;
}

export interface GateRequest {
  run_id: string;
  scores: ScenarioScore[];
  criteria: GateCriteria;
}

export interface GateResponse {
  run_id: string;
  decisions: GateDecision[];
  winners: WinnerRecord[];
}

export function applyKeepOrRevert(request: GateRequest): GateResponse {
  const { run_id, scores, criteria } = request;
  const decisions: GateDecision[] = [];
  const winners: WinnerRecord[] = [];

  const scoreValues = scores.map((s) => s.score);
  const mean =
    scoreValues.length > 0
      ? scoreValues.reduce((a, b) => a + b, 0) / scoreValues.length
      : 0;
  const variance =
    scoreValues.length > 1
      ? scoreValues.reduce((acc, v) => acc + (v - mean) ** 2, 0) / scoreValues.length
      : 0;

  for (const score of scores) {
    let keep = score.score >= criteria.min_score;
    let reason = keep ? "min_score" : "below_min_score";

    if (keep && criteria.max_variance !== undefined && variance > criteria.max_variance) {
      keep = false;
      reason = "variance_exceeded";
    }

    decisions.push({
      scenario_id: score.scenario_id,
      draw_index: score.draw_index,
      decision: keep ? "keep" : "revert",
      reason,
      score: score.score,
    });

    if (keep) {
      winners.push({
        scenario_id: score.scenario_id,
        draw_index: score.draw_index,
        score: score.score,
        metrics: score.metrics ?? {},
      });
    }
  }

  return { run_id, decisions, winners };
}
