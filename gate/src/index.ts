import express from "express";
import { applyKeepOrRevert, GateRequest } from "./decision.js";

const app = express();
app.use(express.json());

const port = Number(process.env.GATE_PORT ?? 8090);

app.get("/health", (_req, res) => {
  res.json({ status: "healthy", service: "keep-or-revert-gate" });
});

app.post("/v1/gate", (req, res) => {
  const body = req.body as GateRequest;
  if (!body?.run_id || !Array.isArray(body.scores) || !body.criteria) {
    res.status(400).json({ error: "run_id, scores, and criteria are required" });
    return;
  }
  res.json(applyKeepOrRevert(body));
});

app.listen(port, () => {
  console.log(`Keep-or-revert gate listening on :${port}`);
});
