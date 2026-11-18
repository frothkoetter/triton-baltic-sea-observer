export type NbaRuleCitation = {
  rule_id: string;
  message?: string;
  ontology_term?: string;
  rulebook_ref?: string;
  full_text?: string;
};

export type NbaDecisionRecord = {
  flow_uuid: string;
  generated_at?: string;
  sections?: {
    "1_what_happened"?: Record<string, unknown>;
    "2_data_pin"?: Record<string, unknown>;
    "3_ranked_coas"?: {
      best_coa_id?: string;
      playbook_best_coa_id?: string;
      coa_scores?: CoaScoreRow[];
    };
    "4_rules_and_ontology"?: {
      grounding?: {
        rule_citations?: NbaRuleCitation[];
        incident_concept_label?: string;
        incident_concept?: string;
        coa_justifications?: {
          coa_id: string;
          rank?: number | null;
          gates_passed?: boolean;
          gate_fail_reasons?: string[];
          legal_note?: string;
        }[];
      };
    };
    "5_sim_outcomes"?: {
      simulations?: { coa_id: string; final_discovery?: number; tick_traces?: unknown[] }[];
      sensor_gates?: { messages?: string[] };
    };
    "6_narrative"?: { text?: string; llm_used?: boolean };
    "8_scenario_matrix"?: {
      runs?: { scenario_id: string; label: string; final_discovery: number; notes?: string }[];
    };
    "9_agentic_gameplay"?: {
      replay?: {
        coa_id: string;
        red_doctrine: string;
        blue_reward?: number;
        red_reward?: number;
        ticks?: {
          tick: number;
          blue_action: string;
          red_action: string;
          discovery_probability: number;
        }[];
      };
    };
  };
};

export type CoaScoreRow = {
  coa_id: string;
  title?: string;
  gates_passed?: boolean;
  gate_fail_reasons?: string[];
  score_latenz?: number;
  score_entdeckung?: number;
  score_eskalation?: number;
  score_kosten?: number;
  score_total?: number;
};

export function parseCoaDetail(detailJson?: string | null): CoaScoreRow | null {
  if (!detailJson) return null;
  try {
    return JSON.parse(detailJson) as CoaScoreRow;
  } catch {
    return null;
  }
}