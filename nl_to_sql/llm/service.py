"""
llm/service.py — Natural language → SQL via Gemini.
Uses shared client, retry, and response helpers in this package.
"""
from __future__ import annotations

import json
import logging
import os
import re

from google.genai import types

from llm.client import get_text_model
from llm.response import json_slice_from_text, text_from_generate_response
from llm.retry import generate_content_with_retry
from utils.constants import TABLE_SELECTOR_MAX_OUTPUT_TOKENS

_log = logging.getLogger(__name__)


# ── Agent Step 1: Table Selector ─────────────────────────────────────────────
# Small, fast LLM call — reads the full table catalog and picks only the tables
# needed to answer the question. User NEVER mentions table names.

_AGENT_SELECTOR_PROMPT = """You are a database schema expert.
Given a user question and a catalog of ALL available database tables, select EXACTLY the tables needed to answer the question.

Rules:
- Select tables that directly contain the needed data.
- Also include tables needed for JOINs to get names, labels, titles, or descriptions (not just raw foreign-key ids).
- If the user asks for "X with Y names" / "employees with department names" / "with their department" / "including category labels", you MUST include BOTH the main entity table (employees/users/…) AND the lookup/dimension table that stores those names (departments/categories/…), whenever such tables exist in the catalog.
- If the question can be answered from one table alone, return only that table.
- Never select tables that are not relevant to the question.
- Return ONLY a valid JSON array of table name strings. No explanation, no markdown.

Example output: ["invoices", "customers"]"""


def select_tables_agent(
    user_query: str,
    table_catalog: str,
    all_table_names: list[str],
) -> list[str]:
    """
    Agent Step 1 — LLM reads the full table catalog and picks which tables
    are relevant for the user's question. The user never needs to mention
    table names. Works automatically for any tables added to the DB.

    Returns a validated list of table names that exist in the database.
    Falls back to the first two tables if parsing fails.
    """
    message = (
        f"{table_catalog}\n"
        f"=== USER QUESTION ===\n{user_query}\n\n"
        "Select the table(s) needed. Return ONLY a JSON array of table names."
    )

    try:
        response = generate_content_with_retry(
            model=get_text_model(),
            contents=[types.Content(role="user", parts=[types.Part(text=message)])],
            config=types.GenerateContentConfig(
                system_instruction=_AGENT_SELECTOR_PROMPT,
                max_output_tokens=TABLE_SELECTOR_MAX_OUTPUT_TOKENS,
                temperature=0.0,   # deterministic — table selection is factual
            ),
        )
        raw = text_from_generate_response(response)
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```\s*$", "", raw).strip()
        raw = json_slice_from_text(raw)
        if not raw:
            raise ValueError("empty model output")
        selected = json.loads(raw)
        # Keep only names that actually exist in the DB
        valid = [t for t in selected if t in all_table_names]
        if valid:
            return valid
    except Exception:
        pass

    # Let the caller fall back to FAISS retrieval — do not guess arbitrary table names.
    return []


# ── SQL generation prompt (compact — saves input tokens on every request) ───
SYSTEM_PROMPT = """PostgreSQL SELECT-only writer. Schema blocks are dynamic for ANY database — never invent identifiers.

CORE
• [R1] SELECT only. No DML/DDL/admin verbs.
• [R2] Every table/column must appear verbatim in === RELEVANT SCHEMA === or === SEMANTIC COLUMN HINTS ===. Use hints for jargon→real names.
• [R3] Alias every table; qualify every column (t.col).
• [R4] Few columns only; SELECT * only if user asks for all columns; else never SELECT *.
• [R5] "with / having / along with" detail data → INNER JOIN + WHERE selected cols IS NOT NULL (names from schema only).
• [R6] Top-N / sort on text numbers: WHERE col IS NOT NULL AND ORDER BY CAST(col AS NUMERIC) DESC NULLS LAST.
• [R7] CAST text to NUMERIC for compare/sort on numeric-like text columns.
• [R8] JOIN direction: FROM the fact/detail table that owns the metric; LEFT JOIN lookup/master for labels.
• [R9] Every non-COUNT query ends LIMIT n OFFSET m (use footer defaults if no number in question).
• [R10] COUNT/SUM/AVG single-row aggregates → chart_suggestion kpi; no LIMIT/OFFSET on pure COUNT(*).
• [R11–R15] Meta questions: use information_schema / pg_catalog SELECTs with LIMIT+OFFSET as in standard PostgreSQL docs.
• [R16] If FK lines exist in schema → JOIN; fetch human-readable columns from related tables, not bare FK ids alone.
• [R17] LEFT JOIN = optional related data; INNER JOIN = user expects related rows to exist.
• [R18] Latest row per parent: DISTINCT ON (parent_pk) … ORDER BY parent_pk, child_time DESC NULLS LAST.
• [R19] Prefer === SEMANTIC COLUMN HINTS === (sorted best-first; +data = sample had a real value). If several columns match one user word, use the top +data line — never a shorter empty synonym. If hints empty, substring-match inside C:.
• [R20] PostgreSQL folds unquoted identifiers to lowercase. If a T: table key contains a dot (schema.table) OR any uppercase letter in the schema or table name, you MUST double-quote each part exactly as in metadata: FROM "ExactSchema"."exact_table" AS alias — never write ExactSchema.table without quotes (that resolves to wrong lower-case objects).
• [R21] Questions like "employees with department **names**" / "**display** X **with** Y" require JOIN(s) per FK lines and the SELECT list must include the **name/title/label** column from the related table — not only FK ids from the fact table.
• [R22] Every relation in FROM / JOIN MUST match a **T:** table line in === RELEVANT SCHEMA === (exact real table name). Never invent common names like ``clinics``, ``users``, ``orders`` unless that exact table appears in those T: lines.

META (information_schema / pg_catalog SELECTs only, with LIMIT+OFFSET): list tables → information_schema.tables; list columns → information_schema.columns WHERE table_name=<name>; users/roles → app tables if present else pg_catalog.pg_user.

OUTPUT JSON only (no markdown, no code fences, no commentary before or after):
{"sql":"…","explanation":"…","chart_suggestion":"bar|line|pie|scatter|heatmap|kpi|table","viz_config":{"x":"…","y":"…","color":null,"title":"…"}}
The "sql" value must be a single-line escaped string or use JSON-safe escaping for newlines. Keep explanation under 120 chars so the JSON fits in the output budget.
Charts: bar grouped/ranked; line time series; pie ≤20 slices; scatter 2 numerics; heatmap 2 categories + value; kpi single aggregate; else table. Never chart=table when 2+ numerics or grouped aggregates. viz_config uses SELECT aliases."""

# ── Structural stopwords only (no domain / metric vocabulary) ───────────────
# Used to tokenise the user question for automatic column-name matching.
_QUERY_STOPWORDS = frozenset({
    "the", "and", "for", "with", "from", "that", "this", "these", "those", "have", "has", "had",
    "get", "got", "give", "show", "list", "find", "tell", "need", "want", "like", "each", "also",
    "along", "their", "details", "detail", "data", "values", "value", "results", "result",
    "some", "any", "all", "both", "only", "just", "more", "most", "least", "than", "then",
    "them", "they", "are", "was", "were", "been", "being", "not", "but", "how", "what", "when",
    "where", "which", "who", "why", "can", "could", "should", "would", "will", "may", "into",
    "onto", "over", "under", "between", "among", "within", "about", "please", "give", "latest",
    "recent", "last", "first", "new", "old", "total", "count", "number", "rows", "row", "table",
})


def _tokens_from_query(text: str) -> list[str]:
    """Extract meaningful lowercase tokens (length ≥ 3) from the user question."""
    return [
        t for t in re.findall(r"[a-z0-9]+", text.lower())
        if len(t) >= 3 and t not in _QUERY_STOPWORDS
    ]


def inferred_top_k_for_query(user_query: str, base: int) -> int:
    """
    Widen FAISS retrieval when the question clearly needs multiple joined entities.
    """
    q = (user_query or "").lower()
    base = max(1, int(base))
    if re.search(
        r"\b(with|including|each|every|along with|together with|and their|display)\b",
        q,
    ):
        return max(base, 6)
    if re.search(
        r"\b(name|names|title|titles|label)\b.*\b(department|division|category|manager|supervisor|region)\b|"
        r"\b(department|division|category|employee|staff)\b.*\b(name|names)\b",
        q,
    ):
        return max(base, 6)
    return base


def expand_selected_tables_for_nl_query(
    user_query: str,
    selected_tables: list[str],
    schema: dict,
    *,
    max_tables: int = 22,
) -> list[str]:
    """
    Pull in extra tables when question tokens match table keys (e.g. *department* →
    ``public.departments``) so semantic hints and FK context include lookup tables.
    """
    sel = list(dict.fromkeys(selected_tables))
    have = set(sel)
    tokens = _tokens_from_query(user_query)
    extra: list[str] = []
    all_t = schema.get("tables", {}) or {}
    for tkey in all_t:
        if tkey in have:
            continue
        tl = tkey.lower()
        short = tl.split(".")[-1]
        hit = False
        for tok in tokens:
            if len(tok) < 4:
                continue
            if tok in tl or tok in short:
                hit = True
                break
            if tok.endswith("s") and len(tok) > 4:
                root = tok[:-1]
                if len(root) >= 4 and (root in short or root in tl):
                    hit = True
                    break
            if not tok.endswith("s") and len(tok) >= 4:
                plural = tok + "s"
                if plural in short or plural in tl:
                    hit = True
                    break
        if hit:
            extra.append(tkey)
    merged = sel + [t for t in extra if t not in have]
    merged = list(dict.fromkeys(merged))
    return merged[:max_tables]


_JUNK_SAMPLE = frozenset(
    {"", "none", "null", "na", "n/a", "[null]", "-", "undefined"}
)


def _sample_value_bonus(table_key: str, col_name: str, schema: dict) -> int:
    """
    Boost columns that actually have a non-junk value in recent sample rows.
    Helps disambiguate e.g. `sugar` (all null) vs `sugar_result` (has data) — no hardcoded names.
    """
    meta = schema.get("tables", {}).get(table_key, {})
    for row in (meta.get("sample_rows") or [])[:5]:
        v = row.get(col_name)
        if v is None:
            continue
        s = str(v).strip().lower()
        if s in _JUNK_SAMPLE:
            continue
        return 24
    return 0


def _semantic_column_matches(
    user_query: str,
    selected_tables: list[str],
    schema: dict,
    *,
    max_hints: int = 12,
) -> list[tuple[str, str, int, str]]:
    """
    Score (table_key, column_name) by token overlap + sample-data bonus + identifier specificity.
    Fully dynamic — prefers columns with real sample values and longer matching names
    when a short token (e.g. "sugar") matches both `sugar` and `sugar_result`.

    Returns list of (table_key, column_name, total_score, reason) sorted by total_score desc.
    """
    tokens = _tokens_from_query(user_query)
    if not tokens:
        return []

    q_lower = user_query.lower()
    scored: dict[tuple[str, str], tuple[int, str]] = {}

    for table_key in selected_tables:
        meta = schema.get("tables", {}).get(table_key, {})
        for col in meta.get("columns", []):
            cname = col.get("column_name", "")
            if not cname:
                continue
            c_low = cname.lower()
            best = 0
            best_tok = ""
            reason = ""

            for tok in tokens:
                if tok in c_low:
                    s = len(tok)
                    if s > best:
                        best = s
                        best_tok = tok
                        reason = f"{tok}->{cname}"

            for part in c_low.split("_"):
                if len(part) < 3:
                    continue
                if part in q_lower and part not in _QUERY_STOPWORDS:
                    s = len(part)
                    if s > best:
                        best = s
                        best_tok = part
                        reason = f"Q->{cname}"

            if best > 0:
                # Prefer longer column names when the same token matches many columns (sugar vs sugar_result).
                specificity = min(12, max(0, len(cname) - len(best_tok))) if best_tok else 0
                samp = _sample_value_bonus(table_key, cname, schema)
                total = best + specificity + samp
                key = (table_key, cname)
                prev = scored.get(key)
                if prev is None or total > prev[0]:
                    tag = []
                    if specificity:
                        tag.append(f"+spec{specificity}")
                    if samp:
                        tag.append("+data")
                    suf = f" ({','.join(tag)})" if tag else ""
                    scored[key] = (total, f"{reason}{suf}")

    rows = [(t, c, sc, r) for (t, c), (sc, r) in scored.items()]
    rows.sort(key=lambda x: -x[2])
    return rows[:max_hints]


def semantic_column_hints_block(
    user_query: str,
    selected_tables: list[str],
    schema: dict,
) -> str:
    """
    Auto-generated block: maps question tokens → real column names for this request.
    No domain-specific strings — works for any DB.
    """
    matches = _semantic_column_matches(user_query, selected_tables, schema)
    if not matches:
        return (
            "=== SEMANTIC COLUMN HINTS ===\n"
            "(No token↔column overlap — read each T:/C: block and copy identifiers verbatim.)\n"
        )

    lines = [
        "=== SEMANTIC COLUMN HINTS (token overlap; use these exact names) ===",
    ]
    for table_key, col, _score, reason in matches:
        lines.append(f"  • {table_key}.{col}  ({reason})")
    lines.append(
        "Ranking: token match + longer real column name + sample row has non-null/non-junk value (+data)."
    )
    return "\n".join(lines) + "\n"


_MAX_ENUM_INLINE = 4
_MAX_COMPACT_COL_CHARS = 2800  # soft cap on C: line length (very wide tables)


def _compact_table_for_llm(table_key: str, meta: dict, schema: dict) -> str:
    """
    Dense, token-efficient schema slice for SQL generation only.
    Built purely from `schema` dict — no table name / column name hardcoding.
    Format: T:<table_key> / C:col:type[*][E[enumvals]]|… / FK:… / S:{json}
    """
    enums = schema.get("enums", {})
    col_parts: list[str] = []
    for c in meta.get("columns", []):
        nm = c.get("column_name", "")
        if not nm:
            continue
        dt = (c.get("data_type") or "?").replace(" ", "")
        star = "*" if c.get("is_primary_key") else ""
        udt = c.get("udt_name", "")
        if udt in enums:
            vals = list(enums[udt])[:_MAX_ENUM_INLINE]
            extra = "…" if len(enums[udt]) > _MAX_ENUM_INLINE else ""
            ev = ",".join(str(v) for v in vals) + extra
            col_parts.append(f"{nm}:{dt}{star}E[{ev}]")
        else:
            col_parts.append(f"{nm}:{dt}{star}")

    col_blob = "|".join(col_parts)
    if len(col_blob) > _MAX_COMPACT_COL_CHARS:
        col_blob = col_blob[:_MAX_COMPACT_COL_CHARS] + "…(+more cols)"

    lines = [f"T:{table_key}", f"C:{col_blob}"]

    fks = meta.get("foreign_keys") or []
    if fks:
        fk_s = "|".join(
            f"{f['column_name']}->{f['foreign_table']}.{f['foreign_column']}"
            for f in fks[:16]
        )
        if len(fks) > 16:
            fk_s += "|…"
        lines.append(f"FK:{fk_s}")

    sample = meta.get("sample_rows") or []
    if sample:
        nn = {k: v for k, v in sample[0].items() if v is not None}
        preview = dict(list(nn.items())[:8])
        s = json.dumps(preview, default=str, separators=(",", ":"))
        if len(s) > 420:
            s = s[:420] + "…"
        lines.append(f"S:{s}")

    return "\n".join(lines)


def _infer_schema_notes(table: str, meta: dict, sample_rows: list[dict]) -> list[str]:
    """
    Automatically infer useful hints about any table based on its columns and
    sample data — works for any table shape.
    """
    notes = []
    col_names = {c["column_name"] for c in meta.get("columns", [])}

    # Detect ISO-text datetime columns
    ts_cols = [c for c in ("created_at", "updated_at", "deleted_at", "date", "datetime")
               if c in col_names]
    if ts_cols:
        notes.append(
            f"{table}: {', '.join(ts_cols)} stored as ISO text — sortable with ORDER BY."
        )

    # Detect nullable name/label columns from sample data
    if sample_rows:
        null_cols = [k for k, v in sample_rows[0].items() if v is None and k in col_names]
        if null_cols:
            notes.append(
                f"{table}: columns {null_cols[:4]} may contain NULL — use IS NOT NULL when filtering."
            )

        # Detect string enum-like columns from sample values (works for any schema)
        _ENUM_LIKE_COLS = {
            "gender", "status", "type", "category", "role", "state",
            "priority", "stage", "level", "mode", "kind", "group",
            "department", "division", "classification", "flag",
            "blood_group", "physique",   # kept for backward compatibility
        }
        enum_hints = {}
        for row in sample_rows[:3]:
            for k, v in row.items():
                if isinstance(v, str) and k in _ENUM_LIKE_COLS:
                    enum_hints.setdefault(k, set()).add(v)
        for col, values in enum_hints.items():
            notes.append(
                f"{table}.{col} sample values: {sorted(values)} — use exact case when filtering."
            )

    return notes


def build_schema_block(
    selected_tables: list[str],
    table_descriptions: dict[str, str],
    schema: dict,
    user_query: str = "",
) -> str:
    """
    Builds the dynamic schema context for SQL generation.

    Uses a **compact** encoding derived only from `schema` (no per-table ENUM spam,
    no indexes/CHECK/UNIQUE blocks) — large token savings vs full schema_to_text.

    `table_descriptions` is kept for API compatibility (FAISS still uses the rich
    text elsewhere); it is not embedded here.
    """
    lines = [
        "=== RELEVANT SCHEMA (compact) ===",
        "Legend: T=table key | C=col:type, *PK, E[enum labels truncated] | FK | S=sample JSON",
    ]
    if user_query.strip():
        lines.append(semantic_column_hints_block(user_query, selected_tables, schema).rstrip())
        lines.append("")

    all_notes: list[str] = []

    for table in selected_tables:
        meta = schema.get("tables", {}).get(table)
        if not meta:
            lines.append(f"T:{table}\n(missing from schema cache — reload schema)")
            lines.append("")
            continue
        sample = meta.get("sample_rows", [])
        lines.append(_compact_table_for_llm(table, meta, schema))
        lines.append("")
        all_notes.extend(_infer_schema_notes(table, meta, sample))

    if all_notes:
        lines.append("=== NOTES (auto, max 6) ===")
        for note in all_notes[:6]:
            lines.append(f"- {note}")

    return "\n".join(lines)

def _extract_sql_from_jsonish(raw: str) -> str:
    """
    Recover SQL from model output when JSON is truncated or malformed.
    """
    m = re.search(r'"sql"\s*:\s*"', raw)
    if not m:
        return ""
    body = raw[m.end() :]
    out: list[str] = []
    i = 0
    while i < len(body):
        if body[i] == "\\" and i + 1 < len(body):
            out.append(body[i : i + 2])
            i += 2
            continue
        if body[i] == '"':
            break
        out.append(body[i])
        i += 1
    if i < len(body) and body[i] == '"':
        text = "".join(out)
    else:
        text = body
        text = re.sub(r',?\s*"(?:explanation|chart_suggestion)"\s*:.*$', "", text, flags=re.DOTALL | re.IGNORECASE)
    return (
        text.replace("\\n", "\n")
        .replace('\\"', '"')
        .replace("\\\\", "\\")
        .strip()
    )


def _quote_ident_part(name: str) -> str:
    n = (name or "").strip().replace('"', '""')
    return f'"{n}"'


def _fallback_count_rows_sql(user_query: str, selected_tables: list[str], schema: dict) -> dict | None:
    """
    Deterministic fallback when model text is empty:
    handle prompts like "How many rows are in apk?".
    """
    q = (user_query or "").strip().lower()
    if not re.search(r"\bhow many\b", q) or "row" not in q:
        return None

    all_tables = list((schema.get("tables") or {}).keys())
    if not all_tables:
        return None

    m = re.search(r"\b(?:in|from)\s+([a-zA-Z0-9_.]+)\??", q)
    target_raw = (m.group(1) if m else "").strip().lower().strip(".")
    if not target_raw:
        return None

    candidates = selected_tables or all_tables
    by_key = {t.lower(): t for t in candidates}
    by_short: dict[str, list[str]] = {}
    for t in candidates:
        short = t.lower().split(".")[-1]
        by_short.setdefault(short, []).append(t)

    chosen = by_key.get(target_raw)
    if not chosen:
        short_hits = by_short.get(target_raw, [])
        if len(short_hits) == 1:
            chosen = short_hits[0]
    if not chosen:
        for t in candidates:
            tl = t.lower()
            if target_raw in tl or tl.endswith("." + target_raw):
                chosen = t
                break
    if not chosen:
        return None

    if "." in chosen:
        sch, tbl = chosen.split(".", 1)
        from_sql = f"{_quote_ident_part(sch)}.{_quote_ident_part(tbl)}"
    else:
        from_sql = _quote_ident_part(chosen)

    return {
        "sql": f"SELECT COUNT(*) AS row_count FROM {from_sql} AS t",
        "explanation": f"Count total rows in `{chosen}`.",
        "chart_suggestion": "kpi",
        "viz_config": {"x": None, "y": "row_count", "color": None, "title": "Row count"},
    }


def _tokenize_text(s: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", (s or "").lower())


def _singularize(tok: str) -> str:
    t = (tok or "").strip().lower()
    if t.endswith("ies") and len(t) > 3:
        return t[:-3] + "y"
    if t.endswith("es") and len(t) > 3:
        return t[:-2]
    if t.endswith("s") and len(t) > 2:
        return t[:-1]
    return t


def _pick_label_column(meta: dict) -> str | None:
    cols = meta.get("columns") or []
    names = [str(c.get("column_name") or "").strip() for c in cols if isinstance(c, dict)]
    low = {n.lower(): n for n in names if n}
    for cand in (
        "name",
        "full_name",
        "display_name",
        "title",
        "department_name",
        "doctor_name",
        "clinic_name",
        "employee_name",
        "patient_name",
    ):
        if cand in low:
            return low[cand]
    for n in names:
        nl = n.lower()
        if "name" in nl or "title" in nl or "label" in nl:
            return n
    return None


def _fallback_general_select_sql(
    user_query: str,
    selected_tables: list[str],
    schema: dict,
    row_limit: int,
    offset: int,
) -> dict | None:
    q = (user_query or "").strip()
    ql = q.lower()
    if not q:
        return None

    tables = selected_tables or list((schema.get("tables") or {}).keys())
    if not tables:
        return None

    q_tokens = _tokenize_text(ql)
    q_roots = {_singularize(t) for t in q_tokens}

    def _tbl_score(tkey: str) -> int:
        short = tkey.split(".")[-1].lower()
        toks = _tokenize_text(short.replace("_", " "))
        roots = {_singularize(t) for t in toks}
        score = 0
        if any(r in q_roots for r in roots):
            score += 5
        if any(t in q_tokens for t in toks):
            score += 3
        return score

    base = max(tables, key=_tbl_score)
    base_meta = (schema.get("tables") or {}).get(base) or {}
    if "." in base:
        bsch, btbl = base.split(".", 1)
        from_sql = f'{_quote_ident_part(bsch)}.{_quote_ident_part(btbl)}'
    else:
        from_sql = _quote_ident_part(base)
    base_cols = [str(c.get("column_name") or "").strip() for c in (base_meta.get("columns") or []) if isinstance(c, dict)]
    base_cols_l = {c.lower(): c for c in base_cols}

    # Aggregate fallback: "How many rows in <table> for each <x>, split by <y>"
    # Example: "How many rows in api_request_limits for each id, split by kiosk_id?"
    agg_m = re.search(r"\bfor each\s+([a-zA-Z0-9_]+)(?:\s*,\s*split by\s+([a-zA-Z0-9_]+))?", ql)
    if re.search(r"\bhow many\b", ql) and "row" in ql and agg_m:
        g1_raw = (agg_m.group(1) or "").strip().lower()
        g2_raw = (agg_m.group(2) or "").strip().lower()
        g1 = base_cols_l.get(g1_raw, "")
        g2 = base_cols_l.get(g2_raw, "") if g2_raw else ""
        if g1:
            grp_cols = [f"t0.{_quote_ident_part(g1)} AS {g1}"]
            grp_by = [f"t0.{_quote_ident_part(g1)}"]
            if g2 and g2.lower() != g1.lower():
                grp_cols.append(f"t0.{_quote_ident_part(g2)} AS {g2}")
                grp_by.append(f"t0.{_quote_ident_part(g2)}")
            sql = (
                f"SELECT {', '.join(grp_cols)}, COUNT(*) AS row_count "
                f"FROM {from_sql} AS t0 "
                f"GROUP BY {', '.join(grp_by)} "
                f"ORDER BY row_count DESC "
                f"LIMIT {max(1, int(row_limit or 20))} OFFSET {max(0, int(offset or 0))}"
            )
            return {
                "sql": sql,
                "explanation": f"Count rows in `{base}` grouped by {', '.join([g1] + ([g2] if g2 else []))}.",
                "chart_suggestion": "bar",
                "viz_config": {
                    "x": g1,
                    "y": "row_count",
                    "color": g2 or None,
                    "title": f"Rows grouped by {g1}" + (f" and {g2}" if g2 else ""),
                },
            }

    if not re.search(r"\b(show|list|display|get|find|top)\b", ql):
        return None

    select_cols: list[str] = []
    base_label = _pick_label_column(base_meta)
    if base_label:
        select_cols.append(f't0.{_quote_ident_part(base_label)} AS {base_label}')
    for c in base_cols:
        cl = c.lower()
        if c == base_label:
            continue
        if cl.endswith("_id") or cl == "id":
            continue
        select_cols.append(f"t0.{_quote_ident_part(c)} AS {c}")
        if len(select_cols) >= 4:
            break
    if not select_cols:
        select_cols = ["t0.*"]

    joins: list[str] = []
    requested_name_entities: list[str] = []
    m = re.search(r"with\s+their\s+(.+?)\s+names?\b", ql)
    if m:
        chunk = m.group(1)
        for part in re.split(r",|/| and |\s+", chunk):
            p = _singularize(part.strip())
            if p and p not in {"their", "with"}:
                requested_name_entities.append(p)

    fk_list = [fk for fk in (base_meta.get("foreign_keys") or []) if isinstance(fk, dict)]
    alias_idx = 1
    for ent in requested_name_entities:
        for fk in fk_list:
            fk_table = str(fk.get("foreign_table") or "").strip()
            if not fk_table:
                continue
            short = fk_table.split(".")[-1].lower()
            if ent not in _tokenize_text(short) and ent not in _singularize(short):
                continue
            ref_meta = (schema.get("tables") or {}).get(fk_table) or {}
            lbl = _pick_label_column(ref_meta)
            if "." in fk_table:
                rsch, rtbl = fk_table.split(".", 1)
                ref_sql = f'{_quote_ident_part(rsch)}.{_quote_ident_part(rtbl)}'
            else:
                ref_sql = _quote_ident_part(fk_table)
            fk_col = str(fk.get("column_name") or "").strip()
            ref_col = str(fk.get("foreign_column") or "id").strip()
            if not fk_col:
                continue
            alias = f"t{alias_idx}"
            joins.append(
                f"LEFT JOIN {ref_sql} AS {alias} ON t0.{_quote_ident_part(fk_col)} = {alias}.{_quote_ident_part(ref_col)}"
            )
            if lbl:
                select_cols.append(f"{alias}.{_quote_ident_part(lbl)} AS {ent}_name")
            alias_idx += 1
            break

    where_sql = ""
    if re.search(r"\bstatus\s+is\s+true\b", ql):
        status_col = None
        for c in base_cols:
            cl = c.lower()
            if cl in {"status", "is_active", "active", "enabled"}:
                status_col = c
                break
        if status_col:
            where_sql = f" WHERE t0.{_quote_ident_part(status_col)} = TRUE"

    sql = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {from_sql} AS t0 "
        + (" ".join(joins) + " " if joins else "")
        + where_sql
        + f" LIMIT {max(1, int(row_limit or 20))} OFFSET {max(0, int(offset or 0))}"
    )
    return {
        "sql": sql,
        "explanation": f"Fallback SQL generated from schema for `{base}`.",
        "chart_suggestion": "table",
        "viz_config": {"x": None, "y": None, "color": None, "title": "Query Results"},
    }


def _dedupe_prompt_list(prompts: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for p in prompts:
        s = (p or "").strip()
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def _parse_catalog_blocks(table_catalog: str) -> list[dict[str, str | list[str]]]:
    """
    Parse ``build_table_catalog`` output into table name + column list per block.
    """
    blocks: list[dict[str, str | list[str]]] = []
    lines = table_catalog.splitlines()
    i = 0
    while i < len(lines):
        m = re.match(r"^•\s+(.+?)\s+\(\d+\s+columns\)\s*$", lines[i])
        if not m:
            i += 1
            continue
        name = m.group(1).strip()
        cols: list[str] = []
        fk_line = ""
        i += 1
        while i < len(lines):
            ln = lines[i]
            if ln.startswith("• "):
                break
            cm = re.match(r"^\s+Columns\s*:\s*(.+)$", ln, re.IGNORECASE)
            if cm:
                raw = cm.group(1)
                if "…" in raw:
                    raw = raw.split("…", 1)[0]
                if "..." in raw:
                    raw = raw.split("...", 1)[0]
                cols = [x.strip() for x in raw.split(",") if x.strip()]
            fm = re.match(r"^\s+FK links:\s*(.+)$", ln, re.IGNORECASE)
            if fm:
                fk_line = fm.group(1).strip()
            i += 1
        blocks.append({"table": name, "columns": cols, "fk": fk_line})
    return blocks


def _varied_fallback_prompts(table_catalog: str, max_prompts: int = 6) -> list[str]:
    """
    When the LLM is unavailable, build six DISTINCT prompts from catalog text:
    rotate question patterns and use real column names so we do not repeat
    “count + latest 5” for every table.
    """
    blocks = _parse_catalog_blocks(table_catalog)
    if not blocks:
        return []

    n = len(blocks)
    out: list[str] = []

    def push(s: str) -> None:
        s = (s or "").strip()
        if not s:
            return
        if s.lower() in {x.lower() for x in out}:
            return
        out.append(s)

    # Six different “shapes”; table chosen round-robin so each table gets airtime.
    for slot in range(max_prompts):
        b = blocks[slot % n]
        t = str(b["table"])
        cols = b.get("columns") or []
        if isinstance(cols, str):
            cols = []
        c0 = cols[0] if cols else None
        c1 = cols[1] if len(cols) > 1 else None
        cl = cols[-1] if cols else None
        fk = str(b.get("fk") or "")
        kind = slot % 6

        if kind == 0:
            push(f"How many rows are in {t}?")
        elif kind == 1:
            push(f"Show 5 recent rows from {t}")
        elif kind == 2:
            if c0:
                push(f"What distinct {c0} values appear in {t}?")
            else:
                push(f"What unique values define rows in {t}?")
        elif kind == 3:
            if cl:
                push(f"Top 10 rows in {t} ordered by {cl} descending")
            else:
                push(f"Show an ordered preview of {t}")
        elif kind == 4:
            if c0 and c1:
                push(f"How many rows in {t} for each {c0}, split by {c1}?")
            elif c0:
                push(f"Count rows in {t} grouped by {c0}")
            else:
                push(f"Summarize counts by category in {t}")
        else:
            if fk and n > 1:
                push(f"Join {t} to related tables using foreign keys and show readable labels")
            elif c0:
                push(f"Find rows in {t} where {c0} is not null")
            else:
                push(f"What stands out in {t} compared to the other tables?")

    return _dedupe_prompt_list(out)[:max_prompts]


# Generates SQL query based on user prompt and selected tables.
# Build Prompt -> Combines: schema, user question, chat history.

def suggest_prompts(
    table_catalog: str,
    last_query: str = "",
) -> list[str]:
    """
    Generates 6 natural-language example prompts that a user could ask,
    based on the live DB schema and (optionally) the user's last question.

    - If last_query is provided → 3 follow-up questions + 3 discovery questions
    - Otherwise               → 6 varied discovery questions across all tables
    """
    follow_up_hint = (
        f"\nThe user's last question was: \"{last_query}\"\n"
        "Include 3 natural follow-up questions to that, "
        "and 3 fresh discovery questions about other tables.\n"
    ) if last_query.strip() else (
        "Generate 6 varied discovery questions spread across different tables.\n"
    )

    message = f"""{table_catalog}
{follow_up_hint}
Rules:
- Use ACTUAL fully-qualified table names (schema.table) and column names from the catalog.
- Mix query types: counts, filters, aggregates, joins, top-N, averages, group-bys.
- Keep each question short and natural (under 70 characters).
- All 6 strings must be DISTINCT — do not repeat the same or near-duplicate question.
- Do NOT use the same wording pattern more than once (e.g. avoid six questions that only swap the table name).
- When several tables exist, reference different tables AND different columns across the 6 questions.
- Do NOT repeat questions already implied by the schema headers.
- Return ONLY a valid JSON array of exactly 6 strings, no markdown.

Example output: ["How many orders last month?", "Top 10 products by revenue", ...]"""

    try:
        response = generate_content_with_retry(
            model=get_text_model(),
            contents=[types.Content(role="user", parts=[types.Part(text=message)])],
            config=types.GenerateContentConfig(
                system_instruction=(
                    "You are a helpful data analyst. "
                    "Suggest natural language questions a user might ask about this database. "
                    "Each question must be structurally different — vary intent (count vs filter vs join vs trend). "
                    "Return ONLY a JSON array of 6 strings."
                ),
                max_output_tokens=512,
                temperature=1.0,   # higher → more variety each time
            ),
        )
        raw = text_from_generate_response(response)
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```\s*$", "", raw).strip()
        raw = json_slice_from_text(raw)
        if not raw:
            raise ValueError("empty model output")
        prompts = json.loads(raw)
        if isinstance(prompts, list) and len(prompts) >= 3:
            merged = _dedupe_prompt_list([str(p) for p in prompts])
            if len(merged) >= 6:
                return merged[:6]
            merged = _dedupe_prompt_list(merged + _varied_fallback_prompts(table_catalog))
            if merged:
                return merged[:6]
    except Exception:
        pass

    # Fallback: column-aware varied prompts (no repeated count/latest pair per table)
    fallback = _varied_fallback_prompts(table_catalog)
    if fallback:
        return fallback[:6]
    return [
        "Show me all data",
        "How many records exist?",
        "What tables are available?",
        "Show recent records",
        "Count records grouped by type",
        "Show top 10 rows",
    ]


def generate_sql(
    user_query: str,
    selected_tables: list[str],
    table_descriptions: dict[str, str],
    schema: dict,
    chat_history: list[dict] | None = None,
    row_limit: int = 20,
    offset: int = 0,
    repair_hint: str | None = None,
) -> dict:
    """
    Returns {"sql": ..., "explanation": ..., "chart_suggestion": ..., "viz_config": {...}}
    row_limit controls the default LIMIT; offset controls the OFFSET injected into the query.
    Every generated query must include both LIMIT and OFFSET for mandatory pagination.
    """
    schema_block = build_schema_block(
        selected_tables, table_descriptions, schema, user_query=user_query
    )

    user_message = f"""{schema_block}

=== USER QUESTION ===
{user_query}

Default row limit : {row_limit}
Current page offset: {offset}

PAGINATION RULES (mandatory):
- ALWAYS end the query with LIMIT <n> OFFSET {offset}.
- If the question already contains an explicit number (top 5, show 3, last 20, etc.),
  use THAT number as LIMIT and still append OFFSET {offset}.
- If the question has NO explicit number, use LIMIT {row_limit} OFFSET {offset}.
- COUNT(*) queries are the only exception — they must NOT have LIMIT/OFFSET.

JOIN RULES (mandatory):
- Check Foreign Keys in the schema above. If FK links exist, write JOIN queries.
- Never return raw ID columns when the related name/label can be fetched via JOIN.

RELATED NAMES (mandatory):
- If the user asked for "department names", "category names", "manager names", or similar, the SELECT list MUST include the human-readable column from the joined table (e.g. department.name, d.title), not only *_id from the main table.

COLUMN PICK (mandatory):
- Use === SEMANTIC COLUMN HINTS === top entries; prefer lines with (+data). SELECT those exact names.

Generate the SQL query now."""

    if (repair_hint or "").strip():
        user_message = user_message + "\n\n=== CORRECTION (mandatory) ===\n" + repair_hint.strip()

    # Build conversation history in google-genai Content format
    contents: list[types.Content] = []
    if chat_history:
        for turn in chat_history[-4:]:  # last 4 turns keeps context without bloating tokens
            role = "model" if turn["role"] == "assistant" else "user"
            contents.append(
                types.Content(role=role, parts=[types.Part(text=turn["content"])])
            )

    contents.append(
        types.Content(role="user", parts=[types.Part(text=user_message)])
    )

    # Generate SQL query using Google Gemini.
    max_out = int(os.getenv("GEMINI_SQL_MAX_OUTPUT_TOKENS", "4096"))
    response = generate_content_with_retry(
        model=get_text_model(),
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            max_output_tokens=max(512, min(max_out, 8192)),
            temperature=0.1,
        ),
    )

    raw = text_from_generate_response(response)
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```\s*$", "", raw).strip()
    raw = json_slice_from_text(raw)

    if not raw:
        fb = _fallback_count_rows_sql(user_query, selected_tables, schema)
        if fb is None:
            fb = _fallback_general_select_sql(user_query, selected_tables, schema, row_limit, offset)
        if fb is not None:
            _log.warning("LLM empty response; using deterministic schema fallback.")
            return fb
        raise ValueError(
            "I could not generate SQL for this request right now. "
            "Please rephrase with clear table/metric terms and try again (for example: "
            "'Show top 20 sales by store for last month')."
        )

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        sql = _extract_sql_from_jsonish(raw)
        if not sql.strip():
            fb = _fallback_count_rows_sql(user_query, selected_tables, schema)
            if fb is None:
                fb = _fallback_general_select_sql(user_query, selected_tables, schema, row_limit, offset)
            if fb is not None:
                _log.warning("LLM malformed JSON; using deterministic schema fallback.")
                return fb
            raise ValueError(
                "The model did not return valid JSON. Raw preview (first 500 chars): "
                f"{raw[:500]!r}"
            ) from None
        result = {
            "sql": sql,
            "explanation": "SQL recovered from non-JSON model output.",
            "chart_suggestion": "table",
            "viz_config": {"x": None, "y": None, "color": None, "title": "Query Results"},
        }

    if not isinstance(result, dict) or "sql" not in result:
        raise ValueError(
            "Model JSON missing required 'sql' key. "
            f"Keys: {list(result.keys()) if isinstance(result, dict) else type(result)}"
        )

    return result
