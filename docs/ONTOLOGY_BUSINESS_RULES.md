# Ontologie: NATO Business Rules & Trino-Mapping

Stand: November 2026 · Training / NATO UNCLASSIFIED

## Zweck

Die Semantica-KG-Seed-Dateien verbinden **öffentlich zugängliche NATO-/UN-Texte** (Paraphrase, kein Ersatz für nationale Rechtslage) mit **durchsetzbaren Showcase-Regeln** und **Iceberg/Trino-Tabellen** im Schema `iceberg.defense`.

| Datei | Rolle |
|-------|--------|
| [`baltic_ontology.json`](../cai-workbench/next-best-action/data/baltic_ontology.json) | Konzepte, IRI, `business_rule_ids` pro Alert-Typ |
| [`baltic_business_rules.json`](../cai-workbench/next-best-action/data/baltic_business_rules.json) | BR-* Regeln, NATO-Quellen, Trino-Evidence-Templates |
| [`baltic_trino_mapping.json`](../cai-workbench/next-best-action/data/baltic_trino_mapping.json) | Semantische Entitäten → Tabellen & APIs |
| [`rules_playbook.json`](../cai-workbench/next-best-action/data/rules_playbook.json) | Deterministische NBA-Gates (COA-Flags) |

## Öffentliche NATO-Quellen (Auswahl)

| ID | Dokument | Nutzung im Showcase |
|----|----------|---------------------|
| NATO:AJP-3.10.2 | Allied Joint Doctrine OPSEC (Ed A) | AIS als technische OPSEC-Maßnahme; Schutz von EEFI |
| NATO:ATP-02.1 | NCAGS Ed B V1 | EMCON/AIS, Sailing Information, need-to-know |
| NATO:MC362-1-EXTRACT | ROE public extract | Deeskalation / Warnungen (Rahmen, nicht operational) |
| UNCLOS | Law of the Sea | Innocent passage, Küstenrechte (indikativ) |

Volltexte: URLs in `baltic_business_rules.json` → `nato_public_sources`. MC 362/2 wird nur referenziert, wenn kein öffentlicher Volltext vorliegt.

## Business Rules (Kurz)

| rule_id | Thema | Trino-Kern |
|---------|--------|------------|
| BR-OPSEC-AIS-001 | AIS/OPSEC | `ais_events_ice`, `vessel_outliers` |
| BR-CUI-PIPE-002 | CUI-Pipeline | `ostsee_pipelines`, `vessel_outliers` |
| BR-ROE-DEESCAL-003 | RF-Nähe | `rus_naval_fleet_positions`, `ships` |
| BR-RESTRICTED-AREA-004 | Beobachtungsgebiete | `observation_areas`, `area_violation` |
| BR-SANCTIONS-005 | Sanktionen (aggregiert) | `sanctioned_vessels` |
| BR-NBA-HITL-006 | Menschliche Freigabe | `nba_incidents`, `nba_coa_runs` |

Jede Regel enthält ein **read-only `evidence_template`** (SQL) für Agenten und Audits — nicht für freies UI-SQL.

## Semantische Entitäten → Trino

Siehe `semantic_entities` in `baltic_trino_mapping.json` (z. B. `MaritimeUnit` → `ships`, `german_navy_fleet`, …). Parallele **REST-APIs** der NATO UI sind unter `api_routes` dokumentiert (Lagebild ohne SQL-Tab).

## Agenten & NBA

- **Agent Console:** `GET /api/agents/business-rules`, Kontext in CrewAI-Prompts (`format_rules_context`).
- **NBA Grounding:** `grounding.json` → `business_rule_bindings` pro Incident.
- **Semantica:** Exportiere die drei JSON-Kataloge ins KG; SHACL shapes bleiben in `decision_record.ttl`.

## Semantica-Projekt einbinden

1. `export_graph` aus eurem Semantica-Projekt als RDF/JSON-LD.
2. Setze `TRITON_ONTOLOGY_PATH` auf den exportierten Seed **oder** merge `business_rules` / `trino_mapping` als named graphs.
3. Behalte `rule_id` / `entity_id` stabil, damit Trino-Templates gültig bleiben.

## Tests

```bash
cd cai-workbench/next-best-action
python3 -m unittest tests.test_business_rules_mapping -v
```