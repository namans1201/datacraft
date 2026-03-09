# agents/dimensional_modeling_agent.py
from __future__ import annotations
from typing import Dict, List, Any
import re

from backend.agents.agent_state import AgentState
from langchain_core.prompts import PromptTemplate
from backend.llm_provider import llm

# ---------------- Prompt (SQL only) ----------------
data_modeling_prompt_sql = PromptTemplate.from_template("""
You are an expert data modeler specializing in Kimball's dimensional modeling.

Task: Generate a single cohesive set of SQL DDL statements (ONLY RAW SQL, no markdown, no comments)
for an optimal dimensional model (star or snowflake) based on the provided schema.

Source schema (consolidated view of available data):
---
{schema}
---

Rules:
1) Model design: derive dimension tables (descriptive entities) and fact tables (events/processes).
2) Column inclusion: include ALL columns from the source schema; place in appropriate dim/fact tables.
3) Naming: tables lower_snake_case; prefix dimensions with dim_, facts with fact_; columns lower_snake_case.
4) Keys: every dim/fact has BIGINT surrogate key named <table_name>_sk as PRIMARY KEY.
   Include natural keys where appropriate (NOT NULL UNIQUE if reliable).
5) Relationships: add FOREIGN KEY constraints referencing surrogate keys.
6) Types: suggest reasonable types (e.g., BIGINT for IDs, VARCHAR/TEXT for strings, TIMESTAMP/DATE).
7) No duplicates: each table defined once; consolidate columns.
8) Temporal: add dim_date if timestamps/dates suggest it.
9) Multiple sources: add data_source and granularity where applicable.

Output: ONLY the raw SQL DDL (no explanations).
""")

# ---------------- Helpers ----------------
def sanitize_name(name: str) -> str:
    """snake_case + safe identifier"""
    name = re.sub(r'[^0-9a-zA-Z]+', '_', name).strip('_').lower()
    if re.match(r'^\d', name):
        name = f"t_{name}"
    return name

def clean_llm_code_response(raw_text: str) -> str:
    """Strip ```sql fences if present."""
    blocks = re.findall(r"```(?:sql)?\s*(.*?)```", raw_text, re.DOTALL)
    return ("\n".join(blocks).strip() if blocks else raw_text.strip())

def split_top_level(definition_block: str) -> List[str]:
    """Split a CREATE TABLE body on top-level commas only."""
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in definition_block:
        if ch == "(":
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1
        if ch == "," and depth == 0:
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
            continue
        current.append(ch)
    token = "".join(current).strip()
    if token:
        parts.append(token)
    return parts

def strip_table_prefix(identifier: str) -> str:
    return identifier.strip().split(".")[-1].strip('"`[]')

def parse_sql_to_er_diagram(sql_text: str) -> Dict[str, Any]:
    """Extract table/column PK-FK structure from SQL DDL for UI ER rendering."""
    clean_sql = sql_text.replace("`", "")
    create_table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.]+)",
        re.IGNORECASE
    )

    tables: Dict[str, Dict[str, Any]] = {}
    relationships: List[Dict[str, str]] = []

    def extract_body(sql: str, start_idx: int) -> tuple[str, int] | None:
        depth = 0
        body_start = sql.find('(', start_idx)
        if body_start == -1:
            return None
        for idx in range(body_start, len(sql)):
            ch = sql[idx]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return sql[body_start + 1:idx], idx
        return None

    for match in create_table_pattern.finditer(clean_sql):
        raw_table_name = match.group(1)
        body_result = extract_body(clean_sql, match.end())
        if not body_result:
            continue

        body, body_end = body_result
        table_name = strip_table_prefix(raw_table_name)
        table = {"name": table_name, "columns": []}

        column_meta: Dict[str, Dict[str, Any]] = {}
        entries = split_top_level(body)

        for entry in entries:
            trimmed = entry.strip()
            upper = trimmed.upper()
            if not trimmed:
                continue

            if upper.startswith("PRIMARY KEY"):
                pk_match = re.search(r"\((.*?)\)", trimmed, re.IGNORECASE | re.DOTALL)
                if pk_match:
                    pk_columns = [strip_table_prefix(c.strip()) for c in pk_match.group(1).split(",")]
                    for pk_col in pk_columns:
                        if pk_col in column_meta:
                            column_meta[pk_col]["is_primary_key"] = True
                continue

            if "FOREIGN KEY" in upper and "REFERENCES" in upper:
                fk_match = re.search(
                    r"FOREIGN\s+KEY\s*\((.*?)\)\s*REFERENCES\s+([a-zA-Z0-9_.]+)\s*\((.*?)\)",
                    trimmed,
                    re.IGNORECASE | re.DOTALL,
                )
                if fk_match:
                    local_cols = [strip_table_prefix(c.strip()) for c in fk_match.group(1).split(",")]
                    ref_table = strip_table_prefix(fk_match.group(2))
                    ref_cols = [strip_table_prefix(c.strip()) for c in fk_match.group(3).split(",")]
                    for idx, local_col in enumerate(local_cols):
                        if local_col in column_meta:
                            column_meta[local_col]["is_foreign_key"] = True
                        ref_col = ref_cols[idx] if idx < len(ref_cols) else (ref_cols[0] if ref_cols else "")
                        relationships.append(
                            {
                                "from_table": table_name,
                                "from_column": local_col,
                                "to_table": ref_table,
                                "to_column": ref_col,
                            }
                        )
                continue

            col_match = re.match(r'^([a-zA-Z0-9_"\[\].]+)\s+([a-zA-Z0-9_()]+)', trimmed)
            if not col_match:
                continue
            column_name = strip_table_prefix(col_match.group(1))
            data_type = col_match.group(2)
            is_pk = "PRIMARY KEY" in upper
            is_fk = "REFERENCES" in upper

            column_meta[column_name] = {
                "name": column_name,
                "data_type": data_type,
                "is_primary_key": is_pk,
                "is_foreign_key": is_fk,
            }

            if is_fk:
                inline_fk = re.search(
                    r"REFERENCES\s+([a-zA-Z0-9_.]+)\s*\((.*?)\)",
                    trimmed,
                    re.IGNORECASE | re.DOTALL,
                )
                if inline_fk:
                    relationships.append(
                        {
                            "from_table": table_name,
                            "from_column": column_name,
                            "to_table": strip_table_prefix(inline_fk.group(1)),
                            "to_column": strip_table_prefix(inline_fk.group(2).split(",")[0].strip()),
                        }
                    )

        table["columns"] = list(column_meta.values())
        tables[table_name] = table

    return {
        "tables": list(tables.values()),
        "relationships": relationships,
    }


def classify_table_type(table_name: str) -> str:
    """Classify the table as a dimension, fact, or generic table."""
    normalized = (table_name or "").lower()
    if normalized.startswith("dim_"):
        return "dimension"
    if normalized.startswith("fact_"):
        return "fact"
    return "table"


def build_er_graph(er_diagram: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the ER metadata into a nodes/edges graph for UI rendering."""
    tables = er_diagram.get("tables", []) if er_diagram else []
    relationships = er_diagram.get("relationships", []) if er_diagram else []

    nodes = []
    for table in tables:
        nodes.append({
            "table_name": table.get("name"),
            "columns": table.get("columns", []),
            "table_type": classify_table_type(table.get("name", "")),
        })

    edges = []
    for rel in relationships:
        edges.append({
            "from_table": rel.get("from_table"),
            "from_column": rel.get("from_column"),
            "to_table": rel.get("to_table"),
            "to_column": rel.get("to_column"),
        })

    return {"nodes": nodes, "edges": edges}

def build_bronze_schema_text(dfs: Dict[str, Any]) -> str:
    parts: List[str] = []
    for name, df in dfs.items():
        t = sanitize_name(name)
        try:
            cols = [sanitize_name(c) for c in df.columns]
        except Exception:
            cols = []
        parts.append(f"Table: {t}\nColumns: {', '.join(cols)}")
    return "\n\n".join(parts)

def build_silver_schema_text(mapping_rows: List[Dict[str, Any]]) -> str:
    # Group silver columns by silver table, ignore Unknowns
    grouped: Dict[str, set] = {}
    for r in mapping_rows or []:
        stbl = str(r.get("silver_table", "")).strip()
        scol = str(r.get("silver_column", "")).strip()
        if not stbl or stbl.lower() == "unknown" or not scol or scol.lower() == "unknown":
            continue
        t = sanitize_name(stbl)
        c = sanitize_name(scol.replace(".", "_").replace("[", "_").replace("]", "_"))
        grouped.setdefault(t, set()).add(c)

    parts: List[str] = []
    for t, cols in grouped.items():
        parts.append(f"Table: {t}\nColumns: {', '.join(sorted(cols))}")
    return "\n\n".join(parts)

# ---------------- Agent Factory ----------------
def get_dimensional_modeling_agent(schema_view: str = "bronze"):
    """
    schema_view: "bronze" -> uses state.dfs
                 "silver" -> uses state.mapping_rows / state.fhir_mapping_rows
    Returns: (node_fn, description)
    """
    view = (schema_view or "bronze").strip().lower()

    def dimensional_modeling_agent_node(state: AgentState) -> AgentState:
        # normalize messages to dicts
        messages = [
            m if isinstance(m, dict) else {
                "role": getattr(m, "role", "assistant"),
                "content": getattr(m, "content", ""),
                "name": getattr(m, "name", None),
            }
            for m in (state.messages or [])
        ]

        # Build schema text based on chosen view
        if view == "silver":
            rows = getattr(state, "mapping_rows", []) or getattr(state, "fhir_mapping_rows", [])
            if not rows:
                return state.copy(update={
                    "messages": messages + [{
                        "role": "assistant",
                        "name": "Dimensional_Modeling_SQL",
                        "content": "No silver mappings found. Run the RAG Mapper first, then try again."
                    }]
                })
            schema_text = build_silver_schema_text(rows)
        else:
            if not state.dfs:
                return state.copy(update={
                    "messages": messages + [{
                        "role": "assistant",
                        "name": "Dimensional_Modeling_SQL",
                        "content": "No DataFrames in memory (bronze). Upload data first."
                    }]
                })
            schema_text = build_bronze_schema_text(state.dfs)

        # Generate SQL via LLM
        prompt = data_modeling_prompt_sql.format(schema=schema_text)
        raw = llm.invoke(prompt).content
        ddl = clean_llm_code_response(raw)
        er_diagram = parse_sql_to_er_diagram(ddl)

        return state.copy(update={
            "messages": messages + [{
                "role": "assistant",
                "name": "Dimensional_Modeling_SQL",
                "content": ddl  # SQL only
            }],
            "modeling_sql": ddl, # NEW for ER diagram                 
            "modeling_schema_view": view, 
            "modeling_er_diagram": er_diagram,
        })
        

    desc = f"Generate Kimball-style dimensional model SQL from {view} schema."
    return dimensional_modeling_agent_node, desc
