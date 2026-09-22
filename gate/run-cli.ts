import { applyKeepOrRevert, GateRequest } from "./src/decision.js";

const input = JSON.parse(process.argv[2] ?? "{}") as GateRequest;
console.log(JSON.stringify(applyKeepOrRevert(input)));
