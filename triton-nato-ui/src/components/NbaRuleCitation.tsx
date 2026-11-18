import { useEffect, useState, type ComponentProps } from "react";
import type { NbaRuleCitation } from "../lib/nba-decision";

type PlaybookRule = NbaRuleCitation & { id?: string };

type PlaybookResponse = {
  playbook?: {
    alert_rules?: PlaybookRule[];
  };
};

type Props = {
  ruleId: string;
  label?: string;
  fallback?: NbaRuleCitation;
};

let playbookCache: Map<string, NbaRuleCitation> | null = null;

async function ruleLookup(): Promise<Map<string, NbaRuleCitation>> {
  if (playbookCache) return playbookCache;
  const res = await fetch("/api/nba/rules-playbook");
  const data = (await res.json()) as PlaybookResponse;
  const map = new Map<string, NbaRuleCitation>();
  for (const r of data.playbook?.alert_rules ?? []) {
    const id = r.id ?? r.rule_id;
    if (id) map.set(id, { ...r, rule_id: id });
  }
  playbookCache = map;
  return map;
}

export function NbaRuleCitationButton({ ruleId, label, fallback }: Props) {
  const [open, setOpen] = useState(false);
  const [rule, setRule] = useState<NbaRuleCitation | null>(fallback ?? null);

  useEffect(() => {
    if (!open) return;
    if (fallback) {
      setRule(fallback);
      return;
    }
    void ruleLookup().then((map) => {
      setRule(map.get(ruleId) ?? { rule_id: ruleId, message: ruleId });
    });
  }, [open, ruleId, fallback]);

  const cite = rule ?? fallback;

  return (
    <>
      <button type="button" className="nba-rule-link" onClick={() => setOpen(true)}>
        {label ?? ruleId}
      </button>
      {open && (
        <div className="nba-rule-modal-backdrop" onClick={() => setOpen(false)} role="presentation">
          <ModalPanel onClick={(e) => e.stopPropagation()}>
            <h3 className="nba-rule-modal-title">{cite?.rule_id ?? ruleId}</h3>
            {cite?.ontology_term && (
              <p className="mono text-muted" style={{ fontSize: 11, marginTop: 0 }}>
                {cite.ontology_term}
                {cite.rulebook_ref ? ` · ${cite.rulebook_ref}` : ""}
              </p>
            )}
            <p className="nba-rule-modal-body">{cite?.full_text ?? cite?.message ?? "—"}</p>
            <button type="button" className="btn-approve" onClick={() => setOpen(false)}>
              Schließen
            </button>
          </ModalPanel>
        </div>
      )}
    </>
  );
}

function ModalPanel(props: ComponentProps<"div">) {
  return <div {...props} className={`nba-rule-modal ${props.className ?? ""}`} role="dialog" />;
}