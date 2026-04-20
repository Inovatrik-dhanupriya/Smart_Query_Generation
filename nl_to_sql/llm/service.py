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

_log = logging.getLogger(__name__)


# ── Agent Step 1: Table Selector ─────────────────────────────────────────────
# Small, fast LLM call — reads the full table catalog and picks only the tables
# needed to answer the question. User NEVER mentions table names.

_AGENT_SELECTOR_PROMPT = """You are a database schema expert.
Given a user question and a catalog of ALL available database tables, select EXACTLY the tables needed to answer the question.

Rules:
- Select tables that directly contain the needed data.
- Also include tables needed for JOINs to get names, labels or related data.
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
                max_output_tokens=150,
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

    # Fallback: return top-2 tables by catalog order
    return all_table_names[:2]


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
- Use ACTUAL table and column names from the catalog above.
- Mix query types: counts, filters, aggregates, joins, top-N, averages.
- Keep each question short and natural (under 70 characters).
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
                    "Return ONLY a JSON array of 6 strings."
                ),
                max_output_tokens=300,
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
            return [str(p) for p in prompts[:6]]
    except Exception:
        pass

    # Fallback: generic prompts derived from table names in catalog
    tables = re.findall(r"^• (\w+)", table_catalog, re.MULTILINE)
    fallback = []
    for t in tables[:3]:
        fallback += [f"How many records are in {t}?", f"Show the latest 5 rows from {t}"]
    return fallback[:6] or ["Show me all data", "How many records exist?",
                             "What tables are available?", "Show recent records",
                             "Count records grouped by type", "Show top 10 rows"]


def generate_sql(
    user_query: str,
    selected_tables: list[str],
    table_descriptions: dict[str, str],
    schema: dict,
    chat_history: list[dict] | None = None,
    row_limit: int = 20,
    offset: int = 0,
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

COLUMN PICK (mandatory):
- Use === SEMANTIC COLUMN HINTS === top entries; prefer lines with (+data). SELECT those exact names.

Generate the SQL query now."""

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
    max_out = int(os.getenv("GEMINI_SQL_MAX_OUTPUT_TOKENS", "2048"))
    response = generate_content_with_retry(
        model=get_text_model(),
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            max_output_tokens=max(256, min(max_out, 8192)),
            temperature=0.1,
        ),
    )

    raw = text_from_generate_response(response)
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```\s*$", "", raw).strip()
    raw = json_slice_from_text(raw)

    if not raw:
        raise ValueError(
            "The model returned an empty response (possible safety block or API issue). "
            "Try a shorter question or retry."
        )

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        # Last resort: pull a sql-ish string from messy output
        sql_match = re.search(r'"sql"\s*:\s*"(.*?)"\s*,\s*"(?:explanation|chart)', raw, re.DOTALL)
        if not sql_match:
            sql_match = re.search(r'"sql"\s*:\s*"(.*)"\s*}', raw, re.DOTALL)
        sql = sql_match.group(1) if sql_match else ""
        sql = sql.replace("\\n", "\n").replace('\\"', '"')
        if not sql.strip():
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
