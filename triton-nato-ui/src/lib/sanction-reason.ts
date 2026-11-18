/** Mirror server-side `sanctionReasonShortExpr` for client-side labels. */
export function shortenSanctionReason(reason: unknown): string {
  if (reason == null || reason === "") return "Unknown";
  const s = String(reason);
  const lower = s.toLowerCase();
  if (lower.includes("833/2014") || lower.includes("annex xiii")) {
    return "EU Annex XIII (833/2014)";
  }
  if (lower.includes("e.o.") || lower.includes("executive order")) {
    const m = s.match(/e\.?\s*o\.?\s*(\d+)/i);
    return m ? `E.O. ${m[1]}` : "E.O. sanction";
  }
  if (lower.includes("irregular shipping")) return "Irregular shipping (EU)";
  if (lower.includes("defense sector")) return "Defense sector goods (EU)";
  if (s.length > 72) return `${s.slice(0, 69)}…`;
  return s;
}