/** Hover copy for NBA decision record §8 scenario matrix and §9 agentic gameplay. */

export const SCENARIO_TOOLTIPS: Record<string, string> = {
  baseline:
    "Same COA and observed weather as the incident. Red doctrine evasive. Partner ISR on. Discovery = simulated 6-tick P(det) at T+6.",
  aggressive_red:
    "Red plays aggressive script (jam GPS, loiter, submerge). Model applies ×0.88 to final discovery vs baseline.",
  weather_stress:
    "Seastate forced to ≥5; partner ISR off (×0.92). Passive buoys and SAR cues are degraded in the sim.",
  c2_delay:
    "Assumes +2h Blue C2 decision delay (×0.9) with cooperative Red for contrast — slower NATO rollout, easier target posture.",
};

export const SCENARIO_DISCOVERY_HINT =
  "Final cumulative discovery probability after 6 ticks for the selected COA under this what-if (0–1).";

export const AGENTIC_SECTION_HINT =
  "Discrete 6-turn Blue vs Red replay for the playbook COA. Policies are deterministic (not live CrewAI). Compare doctrine what-ifs below.";

export const AGENTIC_COA_HINT =
  "Ranked / playbook COA whose Blue sensor and surface profile drives each tick’s Blue action.";

export const AGENTIC_RED_DOCTRINE_HINT =
  "Red script: evasive (go dark → halocline → surface), aggressive (jam → loiter → submerge), or cooperative (surface + AIS).";

export const AGENTIC_BLUE_REWARD_HINT =
  "Blue score: 70% of final P(det) plus 0.2 when the COA uses passive sonar — higher is better for NATO find/hold.";

export const AGENTIC_RED_REWARD_HINT =
  "Red score: 80% of (1 − P(det)) plus 0.1 if Red ends submerged — higher is better for adversary hide/evade.";

export const AGENTIC_TICK_HINT = "Simulation tick (typically 1h steps, 6 ticks).";

export const AGENTIC_PDET_HINT =
  "Cumulative P(det): probability Blue detects or holds the contact by this tick (starts ~0.08, capped at 1).";

const BLUE_ACTION_HINTS: Record<string, string> = {
  cue_sar_pass: "ICEYE / SAR cue over the grid box (tick 1 on COA A).",
  deploy_passive_buoy_line: "Passive sonar buoy line — strong gain unless Red is deep or seastate is high.",
  dispatch_k130_approach: "K130 surface approach from tick 3; adds modest discovery from tick 4.",
  active_sonar_ping: "Active sonar ping (COAs with active sonar); can force Red deeper.",
  hold_partner_isr: "Hold / partner ISR track when no scripted Blue move applies.",
};

const RED_ACTION_HINTS: Record<string, string> = {
  go_dark_ais: "Red drops AIS / goes electrically quiet.",
  halocline_dip: "Red uses halocline layer — submerged; attenuates passive and SAR gains.",
  surface_transit: "Red transits on or near the surface in the script.",
  jam_gps: "Aggressive: GPS jamming — complicates C2 but model mainly shifts doctrine multiplier.",
  pipeline_loiter: "Aggressive: loiter near infrastructure.",
  submerge: "Aggressive: full submerge — lowers detection while deep.",
  maintain_ais: "Cooperative: keeps AIS — easier to detect.",
  hold: "Cooperative: hold station — easier to detect.",
};

export function scenarioTooltip(scenarioId: string, notes?: string): string {
  const base = SCENARIO_TOOLTIPS[scenarioId];
  if (base && notes) return `${base} Run: ${notes}`;
  if (base) return base;
  return notes ?? "Scenario replay parameters from scenario_matrix.json.";
}

export function blueActionTooltip(action: string): string {
  return BLUE_ACTION_HINTS[action] ?? `Blue action: ${action}`;
}

export function redActionTooltip(action: string): string {
  return RED_ACTION_HINTS[action] ?? `Red action: ${action}`;
}

export const WHAT_IF_DOCTRINE_HINT =
  "Same Blue COA replayed with three Red doctrines; rewards summarize find/hold vs hide/evade at T+6.";