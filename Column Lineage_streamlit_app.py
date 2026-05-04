import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Column Lineage Explorer", page_icon="🔍", layout="wide")
session = get_active_session()


def run_query(sql):
    return session.sql(sql).to_pandas()


def get_full_source(row):
    """Build SCHEMA.OBJECT prefix from row data."""
    schema = row["SOURCE_SCHEMA"] if pd.notna(row.get("SOURCE_SCHEMA")) else None
    obj = row["SOURCE_OBJECT"] if pd.notna(row["SOURCE_OBJECT"]) else None
    if schema and obj:
        return f"{schema}.{obj}"
    return obj


def get_display_name(row):
    source = row["SOURCE_OBJECT"] if pd.notna(row["SOURCE_OBJECT"]) else None
    col_name = row["SOURCE_COLUMN"] if pd.notna(row["SOURCE_COLUMN"]) else None
    source_type = row["SOURCE_TYPE"] if pd.notna(row["SOURCE_TYPE"]) else "Unknown"
    snippet = row.get("SQL_SNIPPET")
    snippet = snippet if pd.notna(snippet) else None
    full_src = get_full_source(row)

    if source_type == "COPY_INTO":
        stage = source if source else "STAGE"
        col = col_name if col_name else ""
        return f"Stage: @{stage} -> {col}"
    if source and col_name:
        return f"{full_src}.{col_name}" if full_src else f"{source}.{col_name}"
    elif source:
        if source_type in ("PROCEDURE_CALL", "PROCEDURE", "PROCEDURE_WRITE"):
            return f"Procedure: {full_src or source}"
        if source_type == "VIEW_DEFINITION":
            return f"View: {full_src or source}"
        if source_type == "VIEW_SOURCE":
            label = full_src or source
            return f"{label}.{col_name}" if col_name else label
        return full_src or source
    elif snippet:
        preview = snippet[:80].replace("\n", " ").strip()
        return f"{source_type}: {preview}..."
    else:
        return f"{source_type}"


def get_node_label(row):
    source = row["SOURCE_OBJECT"] if pd.notna(row["SOURCE_OBJECT"]) else None
    col_name = row["SOURCE_COLUMN"] if pd.notna(row["SOURCE_COLUMN"]) else None
    source_type = row["SOURCE_TYPE"] if pd.notna(row["SOURCE_TYPE"]) else "Unknown"
    full_src = get_full_source(row)

    if source_type == "COPY_INTO":
        stage = source if source else "STAGE"
        return f"STAGE: @{stage}"
    if source and col_name:
        return f"{full_src}.{col_name}" if full_src else f"{source}.{col_name}"
    elif source:
        if source_type in ("PROCEDURE_CALL", "PROCEDURE", "PROCEDURE_WRITE"):
            return f"PROC: {full_src or source}"
        if source_type in ("VIEW_DEFINITION", "VIEW_SOURCE"):
            return f"VIEW: {full_src or source}"
        return full_src or source
    else:
        return f"{source_type}"


def get_relationship_text(rel, source_type):
    mapping = {
        "DIRECT_WRITE": "Data copied directly",
        "TRANSFORM": "Data calculated (SUM, AVG, etc.)",
        "DEPENDENCY": "Structural dependency",
        "REFERENCE": "Referenced in code",
        "PROCEDURE_WRITE": "Written via stored procedure",
        "VIEW_READS": "View reads from this table",
        "VIEW_READS_COLUMN": "View reads this column",
        "COPY_LOAD": "Loaded via COPY INTO from stage",
    }
    return mapping.get(rel, rel)


# ── Header ──
st.title("Column Lineage Explorer")
st.caption(
    "Trace where any column gets its data from — across queries, procedures, views, COPY INTO, and pipelines. "
    "No ACCESS_HISTORY wait required."
)

# ── Sidebar ──
with st.sidebar:
    st.header("Run New Trace")

    databases = run_query(
        "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME"
    )
    selected_db = st.selectbox("Database", databases["DATABASE_NAME"].tolist(), index=0)

    schemas = run_query(
        f"SELECT SCHEMA_NAME FROM {selected_db}.INFORMATION_SCHEMA.SCHEMATA "
        f"WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME"
    )
    selected_schema = st.selectbox("Schema", schemas["SCHEMA_NAME"].tolist(), index=0)

    tables = run_query(
        f"SELECT TABLE_NAME FROM {selected_db}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{selected_schema}' AND TABLE_TYPE = 'BASE TABLE' "
        f"ORDER BY TABLE_NAME"
    )
    selected_table = st.selectbox(
        "Table",
        tables["TABLE_NAME"].tolist() if not tables.empty else ["No tables found"],
    )

    if selected_table and selected_table != "No tables found":
        columns = run_query(
            f"SELECT COLUMN_NAME FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{selected_schema}' AND TABLE_NAME = '{selected_table}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        selected_column = st.selectbox(
            "Column",
            columns["COLUMN_NAME"].tolist() if not columns.empty else ["No columns"],
        )
    else:
        selected_column = None

    st.divider()

    if st.button("Trace Lineage", type="primary", disabled=(selected_column is None)):
        with st.spinner("Tracing lineage..."):
            result = run_query(
                f"CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE("
                f"'{selected_db}', '{selected_schema}', '{selected_table}', '{selected_column}')"
            )
            trace_id = result.iloc[0, 0]
            st.session_state["active_trace"] = trace_id
            st.success("Trace complete!")

    st.divider()
    st.header("Trace History")
    history = run_query(
        """
        SELECT TRACE_ID, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, TRACE_STATUS,
               TO_CHAR(STARTED_AT, 'YYYY-MM-DD HH24:MI') AS STARTED
        FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
        ORDER BY STARTED_AT DESC
        LIMIT 20
    """
    )
    if not history.empty:
        for _, row in history.iterrows():
            label = f"{row['TARGET_SCHEMA']}.{row['TARGET_TABLE']}.{row['TARGET_COLUMN']}"
            status_icon = "✅" if row["TRACE_STATUS"] == "COMPLETED" else "❌"
            if st.button(
                f"{status_icon} {label} ({row['STARTED']})", key=row["TRACE_ID"]
            ):
                st.session_state["active_trace"] = row["TRACE_ID"]
    else:
        st.info("No traces yet. Run one above.")

# ── Main Area ──
if "active_trace" not in st.session_state:
    st.info(
        "Select a column in the sidebar and click **Trace Lineage** to begin, "
        "or select a previous trace from history."
    )
    st.stop()

trace_id = st.session_state["active_trace"]

trace_meta = run_query(
    f"""
    SELECT TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN, TRACE_STATUS,
           TO_CHAR(STARTED_AT, 'YYYY-MM-DD HH24:MI:SS') AS STARTED,
           TO_CHAR(COMPLETED_AT, 'YYYY-MM-DD HH24:MI:SS') AS COMPLETED
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
    WHERE TRACE_ID = '{trace_id}'
"""
)

if trace_meta.empty:
    st.error("Trace not found.")
    st.stop()

meta = trace_meta.iloc[0]

results = run_query(
    f"""
    SELECT SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
           RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, QUERY_ID
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = '{trace_id}'
    ORDER BY CASE CONFIDENCE WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, SOURCE_TYPE
"""
)

high_count = len(results[results["CONFIDENCE"] == "HIGH"]) if not results.empty else 0
medium_count = len(results[results["CONFIDENCE"] == "MEDIUM"]) if not results.empty else 0
low_count = len(results[results["CONFIDENCE"] == "LOW"]) if not results.empty else 0

# ── KPI Row ──
st.subheader(f"Lineage: {meta['TARGET_SCHEMA']}.{meta['TARGET_TABLE']}.{meta['TARGET_COLUMN']}")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Status", meta["TRACE_STATUS"])
col2.metric("Total Findings", len(results))
col3.metric("HIGH", high_count)
col4.metric("MEDIUM", medium_count)
col5.metric("LOW", low_count)

if results.empty:
    st.warning(
        "No lineage sources found. Try running queries that write to this table first, "
        "then trace again."
    )
    st.stop()

# ── Tabs ──
tab_graph, tab_findings, tab_sql, tab_raw = st.tabs(
    ["Lineage Graph", "Findings", "SQL Inspector", "Raw Data"]
)

# ── Tab 1: Lineage Graph ──
with tab_graph:
    st.subheader("Data Flow Diagram")

    target_label = f"{meta['TARGET_SCHEMA']}.{meta['TARGET_TABLE']}.{meta['TARGET_COLUMN']}"

    dot = "digraph lineage {\n"
    dot += "  rankdir=LR;\n"
    dot += '  node [shape=box, style=filled, fontname="Helvetica"];\n'
    dot += f'  target [label="{target_label}", fillcolor="#FF6B6B", fontcolor="white"];\n'

    colors = {"HIGH": "#51CF66", "MEDIUM": "#FCC419", "LOW": "#868E96"}
    added_nodes = set()

    for _, row in results.iterrows():
        node_label = get_node_label(row)
        node_id = "".join(c if c.isalnum() else "_" for c in node_label)
        confidence = row["CONFIDENCE"] if pd.notna(row["CONFIDENCE"]) else "LOW"
        relationship = row["RELATIONSHIP"] if pd.notna(row["RELATIONSHIP"]) else ""
        source_type = row["SOURCE_TYPE"] if pd.notna(row["SOURCE_TYPE"]) else ""

        if source_type == "COPY_INTO":
            shape = "cylinder"
        elif source_type in ("PROCEDURE", "PROCEDURE_CALL"):
            shape = "hexagon"
        elif source_type in ("VIEW_DEFINITION", "VIEW_SOURCE", "OBJECT_DEPENDENCY"):
            shape = "diamond"
        else:
            shape = "box"

        if node_id not in added_nodes:
            fill = colors.get(confidence, "#868E96")
            dot += f'  {node_id} [label="{node_label}", fillcolor="{fill}", shape={shape}];\n'
            added_nodes.add(node_id)

        # Determine arrow direction based on relationship and context
        src_schema = row["SOURCE_SCHEMA"] if pd.notna(row.get("SOURCE_SCHEMA")) else ""
        target_schema = meta["TARGET_SCHEMA"]

        if relationship == "COPY_LOAD":
            dot += f'  {node_id} -> target [label="COPY INTO", color="#E64980", penwidth=2];\n'
        elif relationship == "REFERENCE":
            dot += f'  target -> {node_id} [label="ref", style=dashed, color="#868E96"];\n'
        elif relationship == "PROCEDURE_WRITE":
            # Procedure in same schema as target = writes TO target (upstream)
            # Procedure in different schema = reads FROM target (downstream consumer)
            if source_type == "PROCEDURE_CALL" and src_schema != target_schema:
                dot += f'  target -> {node_id} [label="read by", color="#20C997", penwidth=2];\n'
            else:
                dot += f'  {node_id} -> target [label="proc write", color="#9C36B5", penwidth=2];\n'
        elif relationship in ("VIEW_READS", "VIEW_READS_COLUMN"):
            dot += f'  target -> {node_id} [label="read by", color="#20C997"];\n'
        elif relationship == "DEPENDENCY":
            dot += f'  target -> {node_id} [label="depends", style=dashed, color="#868E96"];\n'
        elif relationship == "DIRECT_WRITE":
            dot += f'  {node_id} -> target [label="source", color="#339AF0", penwidth=2];\n'
        elif relationship == "TRANSFORM":
            dot += f'  {node_id} -> target [label="transform", color="#F06595", penwidth=2];\n'
        else:
            dot += f'  {node_id} -> target [label="{relationship}", color="#339AF0"];\n'

    dot += "}\n"

    st.graphviz_chart(dot, use_container_width=True)

    leg1, leg2, leg3, leg4, leg5, leg6, leg7 = st.columns(7)
    leg1.markdown("🟢 **HIGH**")
    leg2.markdown("🟡 **MEDIUM**")
    leg3.markdown("⚪ **LOW**")
    leg4.markdown("🔴 **Target**")
    leg5.markdown("⬡ **Procedure**")
    leg6.markdown("◇ **View**")
    leg7.markdown("🛢 **Stage/COPY**")

# ── Tab 2: Findings ──
with tab_findings:
    st.subheader("All Findings")

    confidence_filter = st.multiselect(
        "Filter by Confidence",
        ["HIGH", "MEDIUM", "LOW"],
        default=["HIGH", "MEDIUM", "LOW"],
    )
    filtered = results[results["CONFIDENCE"].isin(confidence_filter)]

    for _, row in filtered.iterrows():
        display_name = get_display_name(row)
        confidence = row["CONFIDENCE"]
        source_type = row["SOURCE_TYPE"] if pd.notna(row["SOURCE_TYPE"]) else ""

        if confidence == "HIGH":
            icon = "🟢"
            explanation = "Verified by Snowflake ACCESS_HISTORY"
        elif confidence == "MEDIUM":
            icon = "🟡"
            if source_type == "COPY_INTO":
                explanation = "Data loaded via COPY INTO from external stage"
            elif source_type in ("PROCEDURE_CALL", "PROCEDURE", "PROCEDURE_WRITE"):
                explanation = "Stored procedure writes to this table"
            elif source_type in ("VIEW_DEFINITION", "VIEW_SOURCE"):
                explanation = "Found by parsing view definition"
            elif source_type == "OBJECT_DEPENDENCY":
                explanation = "Structural dependency detected"
            else:
                explanation = "Found in query history (SQL text analysis)"
        else:
            icon = "⚪"
            explanation = "Found by scanning code references"

        rel_text = get_relationship_text(row["RELATIONSHIP"], source_type)

        with st.container(border=True):
            c1, c2, c3 = st.columns([2, 2, 1])
            c1.markdown(f"**{icon} {display_name}**")
            c2.markdown(f"{rel_text} via `{source_type}`")
            c3.markdown(f"**{confidence}**")
            st.caption(explanation)

# ── Tab 3: SQL Inspector ──
with tab_sql:
    st.subheader("SQL Snippets")
    st.caption("View the actual SQL that wrote data into the target column.")

    sql_results = results[
        results["SQL_SNIPPET"].notna() & (results["SQL_SNIPPET"] != "")
    ]

    if sql_results.empty:
        st.info("No SQL snippets captured for this trace.")
    else:
        for _, row in sql_results.iterrows():
            schema = row["SOURCE_SCHEMA"] if pd.notna(row.get("SOURCE_SCHEMA")) else ""
            source = row["SOURCE_OBJECT"] if pd.notna(row["SOURCE_OBJECT"]) else row["SOURCE_TYPE"]
            full_source = f"{schema}.{source}" if schema else source
            source_type = row["SOURCE_TYPE"] if pd.notna(row["SOURCE_TYPE"]) else ""
            if source_type == "COPY_INTO":
                label_prefix = "COPY"
            elif source_type in ("PROCEDURE", "PROCEDURE_CALL", "PROCEDURE_WRITE"):
                label_prefix = "PROC"
            elif source_type in ("VIEW_DEFINITION", "VIEW_SOURCE"):
                label_prefix = "VIEW"
            else:
                label_prefix = row["CONFIDENCE"]
            with st.expander(f"{label_prefix} | {full_source} | {row['RELATIONSHIP']}"):
                st.code(row["SQL_SNIPPET"], language="sql")
                if pd.notna(row["QUERY_ID"]):
                    st.caption(f"Query ID: `{row['QUERY_ID']}`")

# ── Tab 4: Raw Data ──
with tab_raw:
    st.subheader("Raw Results Table")
    display_df = results[
        ["SOURCE_TYPE", "SOURCE_SCHEMA", "SOURCE_OBJECT", "SOURCE_COLUMN", "RELATIONSHIP", "CONFIDENCE"]
    ].copy()
    display_df["SOURCE_OBJECT"] = display_df.apply(
        lambda r: f"@{r['SOURCE_OBJECT']}" if r["SOURCE_TYPE"] == "COPY_INTO" and pd.notna(r["SOURCE_OBJECT"])
        else f"{r['SOURCE_SCHEMA']}.{r['SOURCE_OBJECT']}" if pd.notna(r["SOURCE_SCHEMA"]) and pd.notna(r["SOURCE_OBJECT"])
        else r["SOURCE_OBJECT"] if pd.notna(r["SOURCE_OBJECT"])
        else f"({r['SOURCE_TYPE']} — see SQL Inspector)" if pd.notna(r["SOURCE_TYPE"])
        else "(unknown)", axis=1
    )
    display_df = display_df.drop(columns=["SOURCE_SCHEMA"])

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Trace Metadata")
    st.json(
        {
            "trace_id": trace_id,
            "target": f"{meta['TARGET_DATABASE']}.{meta['TARGET_SCHEMA']}.{meta['TARGET_TABLE']}.{meta['TARGET_COLUMN']}",
            "status": meta["TRACE_STATUS"],
            "started": meta["STARTED"],
            "completed": meta["COMPLETED"],
            "total_findings": len(results),
            "high": int(high_count),
            "medium": int(medium_count),
            "low": int(low_count),
        }
    )
