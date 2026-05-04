Here are all the commands, one step at a time. Execute each in your Snowflake worksheet, confirm it works, then move to the next.
────────────────────────────────────────
--Step 1
    -- ============================================================
    -- Step 1: CREATE DEDICATED WAREHOUSE
    -- ============================================================
    -- Purpose: Creates a Medium-sized virtual warehouse for running
    --          lineage traces and Streamlit queries.
    -- AUTO_SUSPEND = 60: Suspends after 60 seconds idle to save credits.
    -- AUTO_RESUME = TRUE: Auto-starts when a query arrives.
    -- INITIALLY_SUSPENDED: Don't start billing until first use.
    -- ============================================================
    CREATE WAREHOUSE IF NOT EXISTS LINEAGE_WH
      WAREHOUSE_SIZE = 'MEDIUM'
      AUTO_SUSPEND = 60
      AUTO_RESUME = TRUE
      INITIALLY_SUSPENDED = TRUE;

--  Say done when ready for Step 2.
-- > done
-- Step 2
    -- ============================================================
    -- STEP 2: CREATE FRAMEWORK DATABASE & SET CONTEXT
    -- ============================================================
    -- Purpose: Creates the database that holds ALL lineage framework
    --          objects (tables, procedures). Separate from your
    --          business data (RETAIL_DB) so the framework is reusable.
    -- USE statements set the session context so subsequent commands
    -- don't need fully qualified names during setup.
    -- ============================================================
    CREATE DATABASE IF NOT EXISTS LINEAGE_FRAMEWORK_DB;
    USE DATABASE LINEAGE_FRAMEWORK_DB;
    USE SCHEMA PUBLIC;
    USE WAREHOUSE LINEAGE_WH;
--  Say done when ready for Step 3.
-- > done
-- Step 3
    -- ============================================================
    -- STEP 3: CREATE LINEAGE TRACES TABLE
    -- ============================================================
    -- Purpose: Stores one row per lineage trace execution.
    --          This is the "header" table — records WHAT was traced,
    --          WHEN, and the final STATUS.
    --
    -- Columns:
    --   TRACE_ID       : Unique ID (format: DB.SCHEMA.TABLE.COLUMN.TIMESTAMP)
    --   TARGET_*       : The column being traced (database, schema, table, column)
    --   STARTED_AT     : When the trace began (auto-set to current time)
    --   COMPLETED_AT   : When the trace finished (set by orchestrator)
    --   TRACE_STATUS   : RUNNING → COMPLETED or FAILED: <error>
    --   FINDINGS_COUNT : Total number of lineage sources found
    -- ============================================================
    CREATE OR REPLACE TABLE LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES (
        TRACE_ID         VARCHAR(100) NOT NULL,
        TARGET_DATABASE  VARCHAR(200),
        TARGET_SCHEMA    VARCHAR(200),
        TARGET_TABLE     VARCHAR(200),
        TARGET_COLUMN    VARCHAR(200),
        STARTED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        COMPLETED_AT     TIMESTAMP_NTZ,
        TRACE_STATUS     VARCHAR(50) DEFAULT 'RUNNING',
        FINDINGS_COUNT   INTEGER DEFAULT 0
    );
--  Say done when ready for Step 4.
-- > done
-- Step 4
    -- ============================================================
    -- STEP 4: CREATE LINEAGE RESULTS TABLE
    -- ============================================================
    -- Purpose: Stores individual lineage findings (the "detail" rows).
    --          Each row = one discovered data source for a traced column.
    --
    -- Columns:
    --   TRACE_ID       : Links back to COLUMN_LINEAGE_TRACES
    --   SOURCE_TYPE    : What kind of source (PROCEDURE_CALL, VIEW_DEFINITION,
    --                    VIEW_SOURCE, OBJECT_DEPENDENCY, COPY_INTO)
    --   SOURCE_DATABASE: Database where source lives
    --   SOURCE_SCHEMA  : Schema where source lives (critical for cross-schema lineage)
    --   SOURCE_OBJECT  : Table, procedure, or view name
    --   SOURCE_COLUMN  : Specific column (NULL for procedure-level entries)
    --   RELATIONSHIP   : How data flows (DIRECT_WRITE, TRANSFORM, PROCEDURE_WRITE,
    --                    VIEW_READS, VIEW_READS_COLUMN, DEPENDENCY, COPY_LOAD)
    --   CONFIDENCE     : HIGH / MEDIUM / LOW — how certain the finding is
    --   SQL_SNIPPET    : The actual SQL code where the relationship was found
    --   QUERY_ID       : Snowflake query ID (unused — query history was skipped)
    --   DISCOVERED_AT  : When this finding was recorded
    --   ANALYSIS_LAYER : Which layer found it (PROCEDURAL, VIEW_ANALYSIS, DEPENDENCY)
    -- ============================================================
    CREATE OR REPLACE TABLE LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS (
        TRACE_ID        VARCHAR(100) NOT NULL,
        SOURCE_TYPE     VARCHAR(100),
        SOURCE_DATABASE VARCHAR(200),
        SOURCE_SCHEMA   VARCHAR(200),
        SOURCE_OBJECT   VARCHAR(500),
        SOURCE_COLUMN   VARCHAR(200),
        RELATIONSHIP    VARCHAR(100),
        CONFIDENCE      VARCHAR(20),
        SQL_SNIPPET     VARCHAR(16000),
        QUERY_ID        VARCHAR(200),
        DISCOVERED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        ANALYSIS_LAYER  VARCHAR(50)
    );
--  Say done when ready for Step 5.
-- > done
-- Step 5
    -- ============================================================
    -- STEP 5: CREATE ANALYZE_PROCEDURAL_OBJECTS PROCEDURE
    -- ============================================================
    -- Purpose: Layer 1 of 3 — scans ALL stored procedures across
    --          ALL schemas in the target database to find which ones
    --          write data into the target column.
    --
    -- How it works:
    --   1. Builds a temp table by CROSS JOINing all procedures with
    --      all columns in the target database. This gives every
    --      possible (procedure, source_column) combination to check.
    --   2. Filters: procedure DDL must mention the target table name.
    --   3. For each candidate, determines the relationship:
    --      DIRECT_WRITE: Source column name = target column name
    --      TRANSFORM: AGG(source_col) AS target_col detected via
    --                 position-proximity algorithm (within 30 chars,
    --                 no intermediate AS keyword)
    --
    -- EXECUTE AS CALLER: Required so the procedure can access
    --   INFORMATION_SCHEMA of whatever database the caller passes in.
    -- EXECUTE IMMEDIATE: Builds temp table dynamically because
    --   Snowflake SQL scripting can't use variable database names
    --   in FROM clauses directly.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE LINEAGE_FRAMEWORK_DB.PUBLIC.ANALYZE_PROCEDURAL_OBJECTS(
        P_TRACE_ID VARCHAR, P_DB VARCHAR, P_SCHEMA VARCHAR, P_TABLE VARCHAR, P_COLUMN VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    '
    DECLARE
        -- Local variables initialized from parameters
        v_trace_id VARCHAR DEFAULT P_TRACE_ID;  -- Unique trace identifier
        v_db       VARCHAR DEFAULT P_DB;        -- Target database (e.g., RETAIL_DB)
        v_schema   VARCHAR DEFAULT P_SCHEMA;    -- Target schema (e.g., SILVER)
        v_table    VARCHAR DEFAULT P_TABLE;     -- Target table (e.g., SALES)
        v_column   VARCHAR DEFAULT P_COLUMN;    -- Target column (e.g., AMOUNT)
        v_count    INTEGER DEFAULT 0;           -- Counter for findings
        v_sql      VARCHAR;                     -- Dynamic SQL string for temp table
        v_fq_table VARCHAR;                     -- Fully qualified: SCHEMA.TABLE
    BEGIN
        -- Build fully qualified table name for matching (e.g., SILVER.SALES)
        v_fq_table := v_schema || ''.'' || v_table;

        -- --------------------------------------------------------
        -- PHASE 1: Build temp table with all (procedure, column) candidates
        -- --------------------------------------------------------
        -- CROSS JOIN all procedures with all columns in the database.
        -- Filter: procedure DDL must contain the target table name.
        -- Filter: exclude the target table itself as a source
        --         (same table + same schema = self-reference).
        -- This avoids deeply nested IDENTIFIER(?) cursors which
        -- fail silently inside Snowflake SQL scripting loops.
        -- --------------------------------------------------------
        v_sql := ''CREATE OR REPLACE TEMPORARY TABLE LINEAGE_FRAMEWORK_DB.PUBLIC.TMP_PROC_SOURCES AS
        SELECT
            p.PROCEDURE_SCHEMA,
            p.PROCEDURE_NAME,
            p.ARGUMENT_SIGNATURE,
            p.PROCEDURE_DEFINITION AS DDL,
            c.TABLE_SCHEMA AS SRC_SCHEMA,
            c.TABLE_NAME AS SRC_TABLE,
            c.COLUMN_NAME AS SRC_COL,
            c.TABLE_SCHEMA || ''''.'''' || c.TABLE_NAME AS FQ_NAME
        FROM IDENTIFIER('''''' || v_db || ''.INFORMATION_SCHEMA.PROCEDURES'''') p
        CROSS JOIN IDENTIFIER('''''' || v_db || ''.INFORMATION_SCHEMA.COLUMNS'''') c
        WHERE p.PROCEDURE_DEFINITION IS NOT NULL
          AND UPPER(p.PROCEDURE_DEFINITION) LIKE ''''%'''' || '''''' || v_table || '''''' || ''''%''''
          AND c.TABLE_NAME != '''''' || v_table || '''''' OR c.TABLE_SCHEMA != '''''' || v_schema || '''''''';

        EXECUTE IMMEDIATE v_sql;

        -- --------------------------------------------------------
        -- PHASE 2: Iterate temp table and classify each finding
        -- --------------------------------------------------------
        LET cur CURSOR FOR
            SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME, ARGUMENT_SIGNATURE, DDL,
                   SRC_SCHEMA, SRC_TABLE, SRC_COL, FQ_NAME
            FROM LINEAGE_FRAMEWORK_DB.PUBLIC.TMP_PROC_SOURCES;

        FOR r IN cur DO
            LET v_ddl VARCHAR := r.DDL;               -- Full procedure body text
            LET v_ddl_upper VARCHAR := UPPER(v_ddl);   -- Uppercased for case-insensitive matching
            LET v_src_col3 VARCHAR := r.SRC_COL;       -- Current source column being checked
            LET v_col_upper VARCHAR := UPPER(v_column); -- Target column uppercased
            LET v_proc_schema VARCHAR := r.PROCEDURE_SCHEMA;
            LET v_proc_name VARCHAR := r.PROCEDURE_NAME;
            LET v_src_schema VARCHAR := r.SRC_SCHEMA;
            LET v_src_table VARCHAR := r.SRC_TABLE;

            -- Check: does this procedure reference the source table?
            -- Match either SCHEMA.TABLE (fully qualified) or just TABLE name
            IF (POSITION(r.FQ_NAME IN v_ddl_upper) > 0 OR POSITION(r.SRC_TABLE IN v_ddl_upper) > 0) THEN

                -- Check: does the source column name appear in the procedure body?
                IF (POSITION(v_src_col3 IN v_ddl_upper) > 0) THEN

                    -- --------------------------------------------------------
                    -- TRANSFORM DETECTION (aggregate functions)
                    -- --------------------------------------------------------
                    -- Detects: SUM(AMOUNT) AS TOTAL_REVENUE
                    -- Algorithm:
                    --   1. Find "AS <target_column>" position in DDL
                    --   2. For each aggregate (SUM, AVG, MAX, MIN, COUNT):
                    --      a. Find "AGG(<source_column>)" position
                    --      b. AGG must appear BEFORE the AS keyword
                    --      c. Distance must be < 30 chars
                    --      d. No other "AS " keyword in between
                    --         (prevents false positives like matching
                    --          MAX(SALE_DATE) with AS TOTAL_REVENUE)
                    -- --------------------------------------------------------
                    LET v_is_transform INTEGER := 0;
                    LET v_as_pos INTEGER := POSITION(''AS '' || v_col_upper IN v_ddl_upper);

                    IF (v_as_pos > 0) THEN
                        -- Check SUM(source_col) ... AS target_col
                        LET v_agg_pat VARCHAR := ''SUM('' || v_src_col3 || '')'';
                        LET v_agg_pos INTEGER := POSITION(v_agg_pat IN v_ddl_upper);
                        IF (v_agg_pos > 0 AND v_agg_pos < v_as_pos AND (v_as_pos - v_agg_pos - LENGTH(v_agg_pat)) < 30) THEN
                            -- Verify no intermediate AS keyword between AGG() and AS TARGET
                            LET v_between VARCHAR := SUBSTR(v_ddl_upper, v_agg_pos + LENGTH(v_agg_pat), v_as_pos - v_agg_pos - 
  LENGTH(v_agg_pat));
                            IF (POSITION(''AS '' IN v_between) = 0) THEN
                                v_is_transform := 1;  -- Confirmed: SUM(col) AS target
                            END IF;
                        END IF;

                        -- Check AVG(source_col) ... AS target_col
                        IF (v_is_transform = 0) THEN
                            v_agg_pat := ''AVG('' || v_src_col3 || '')'';
                            v_agg_pos := POSITION(v_agg_pat IN v_ddl_upper);
                            IF (v_agg_pos > 0 AND v_agg_pos < v_as_pos AND (v_as_pos - v_agg_pos - LENGTH(v_agg_pat)) < 30) THEN
                                LET v_between2 VARCHAR := SUBSTR(v_ddl_upper, v_agg_pos + LENGTH(v_agg_pat), v_as_pos - v_agg_pos - 
  LENGTH(v_agg_pat));
                                IF (POSITION(''AS '' IN v_between2) = 0) THEN
                                    v_is_transform := 1;  -- Confirmed: AVG(col) AS target
                                END IF;
                            END IF;
                        END IF;

                        -- Check MAX(source_col) ... AS target_col
                        IF (v_is_transform = 0) THEN
                            v_agg_pat := ''MAX('' || v_src_col3 || '')'';
                            v_agg_pos := POSITION(v_agg_pat IN v_ddl_upper);
                            IF (v_agg_pos > 0 AND v_agg_pos < v_as_pos AND (v_as_pos - v_agg_pos - LENGTH(v_agg_pat)) < 30) THEN
                                LET v_between3 VARCHAR := SUBSTR(v_ddl_upper, v_agg_pos + LENGTH(v_agg_pat), v_as_pos - v_agg_pos - 
  LENGTH(v_agg_pat));
                                IF (POSITION(''AS '' IN v_between3) = 0) THEN
                                    v_is_transform := 1;  -- Confirmed: MAX(col) AS target
                                END IF;
                            END IF;
                        END IF;

                        -- Check MIN(source_col) ... AS target_col
                        IF (v_is_transform = 0) THEN
                            v_agg_pat := ''MIN('' || v_src_col3 || '')'';
                            v_agg_pos := POSITION(v_agg_pat IN v_ddl_upper);
                            IF (v_agg_pos > 0 AND v_agg_pos < v_as_pos AND (v_as_pos - v_agg_pos - LENGTH(v_agg_pat)) < 30) THEN
                                LET v_between4 VARCHAR := SUBSTR(v_ddl_upper, v_agg_pos + LENGTH(v_agg_pat), v_as_pos - v_agg_pos - 
  LENGTH(v_agg_pat));
                                IF (POSITION(''AS '' IN v_between4) = 0) THEN
                                    v_is_transform := 1;  -- Confirmed: MIN(col) AS target
                                END IF;
                            END IF;
                        END IF;

                        -- Check COUNT(source_col) ... AS target_col
                        IF (v_is_transform = 0) THEN
                            v_agg_pat := ''COUNT('' || v_src_col3 || '')'';
                            v_agg_pos := POSITION(v_agg_pat IN v_ddl_upper);
                            IF (v_agg_pos > 0 AND v_agg_pos < v_as_pos AND (v_as_pos - v_agg_pos - LENGTH(v_agg_pat)) < 30) THEN
                                LET v_between5 VARCHAR := SUBSTR(v_ddl_upper, v_agg_pos + LENGTH(v_agg_pat), v_as_pos - v_agg_pos - 
  LENGTH(v_agg_pat));
                                IF (POSITION(''AS '' IN v_between5) = 0) THEN
                                    v_is_transform := 1;  -- Confirmed: COUNT(col) AS target
                                END IF;
                            END IF;
                        END IF;
                    END IF;

                    -- --------------------------------------------------------
                    -- INSERT FINDING based on classification
                    -- --------------------------------------------------------
                    IF (v_is_transform = 1) THEN
                        -- TRANSFORM: aggregate function wraps source column
                        -- Example: SUM(AMOUNT) AS TOTAL_REVENUE
                        INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                            (TRACE_ID, SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
                             RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, ANALYSIS_LAYER)
                        VALUES (:v_trace_id, ''PROCEDURE_CALL'', :v_db, :v_src_schema, :v_src_table, :v_src_col3,
                                ''TRANSFORM'', ''MEDIUM'', :v_ddl, ''PROCEDURAL'');
                        v_count := v_count + 1;
                    ELSEIF (UPPER(v_src_col3) = v_col_upper) THEN
                        -- DIRECT_WRITE: source column has same name as target column
                        -- Example: INSERT INTO silver.sales(AMOUNT) SELECT AMOUNT FROM bronze.sales
                        INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                            (TRACE_ID, SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
                             RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, ANALYSIS_LAYER)
                        VALUES (:v_trace_id, ''PROCEDURE_CALL'', :v_db, :v_src_schema, :v_src_table, :v_src_col3,
                                ''DIRECT_WRITE'', ''MEDIUM'', :v_ddl, ''PROCEDURAL'');
                        v_count := v_count + 1;
                    END IF;

                    -- --------------------------------------------------------
                    -- RECORD THE PROCEDURE ITSELF as a writer
                    -- --------------------------------------------------------
                    -- Also record the procedure as a PROCEDURE_WRITE relationship.
                    -- Shows "which procedure writes to this table" in the graph.
                    -- WHERE NOT EXISTS prevents duplicate entries if the same
                    -- procedure matches multiple source columns.
                    -- --------------------------------------------------------
                    INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                        (TRACE_ID, SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
                         RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, ANALYSIS_LAYER)
                    SELECT :v_trace_id, ''PROCEDURE_CALL'', :v_db, :v_proc_schema, :v_proc_name, NULL,
                           ''PROCEDURE_WRITE'', ''MEDIUM'', :v_ddl, ''PROCEDURAL''
                    WHERE NOT EXISTS (
                        SELECT 1 FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                        WHERE TRACE_ID = :v_trace_id AND SOURCE_OBJECT = :v_proc_name
                          AND SOURCE_SCHEMA = :v_proc_schema AND RELATIONSHIP = ''PROCEDURE_WRITE''
                    );
                    v_count := v_count + 1;

                END IF;  -- source column found in DDL
            END IF;  -- source table found in DDL
        END FOR;

        RETURN ''Procedural analysis found '' || :v_count || '' sources'';

    EXCEPTION
        -- Catch all errors so the orchestrator can continue with other layers
        WHEN OTHER THEN
            RETURN ''Procedural error: '' || SQLERRM;
    END;
    ';
--  Say done when ready for Step 6.
-- > done
-- Step 6
    -- ============================================================
    -- STEP 6: CREATE ANALYZE_VIEW_DEFINITIONS PROCEDURE
    -- ============================================================
    -- Purpose: Layer 2 of 3 — scans ALL view definitions across
    --          ALL schemas in the target database to find views
    --          that SELECT from the target table.
    --
    -- How it works:
    --   1. Queries INFORMATION_SCHEMA.VIEWS using IDENTIFIER(?)
    --      to dynamically access the target database's metadata.
    --   2. For each view, checks if its SQL definition mentions
    --      the target table (fully qualified or short name).
    --   3. If found, records:
    --      VIEW_READS: the view reads from this table
    --      VIEW_READS_COLUMN: the view specifically uses this column
    --
    -- EXECUTE AS CALLER: Required for cross-database access.
    -- IDENTIFIER(?): Snowflake's way to use a variable as a table
    --   reference in FROM clause. The USING clause binds the value.
    -- WHERE NOT EXISTS: Prevents duplicate entries.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE LINEAGE_FRAMEWORK_DB.PUBLIC.ANALYZE_VIEW_DEFINITIONS(
        P_TRACE_ID VARCHAR, P_DB VARCHAR, P_SCHEMA VARCHAR, P_TABLE VARCHAR, P_COLUMN VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    '
    DECLARE
        v_trace_id VARCHAR DEFAULT P_TRACE_ID;  -- Unique trace identifier
        v_db       VARCHAR DEFAULT P_DB;        -- Target database
        v_schema   VARCHAR DEFAULT P_SCHEMA;    -- Target schema
        v_table    VARCHAR DEFAULT P_TABLE;     -- Target table
        v_column   VARCHAR DEFAULT P_COLUMN;    -- Target column
        v_count    INTEGER DEFAULT 0;           -- Counter for findings
        v_fq_table VARCHAR;                     -- Fully qualified: SCHEMA.TABLE
    BEGIN
        -- Build fully qualified name for matching (e.g., GOLD.CUSTOMER_REVENUE)
        v_fq_table := v_schema || ''.'' || v_table;

        -- --------------------------------------------------------
        -- Scan all views in the target database
        -- --------------------------------------------------------
        -- IDENTIFIER(?) with USING clause lets us dynamically query
        -- any database''s INFORMATION_SCHEMA without hard-coding.
        -- This is the key pattern that makes the framework reusable.
        -- --------------------------------------------------------
        LET cur CURSOR FOR
            SELECT TABLE_SCHEMA AS VIEW_SCHEMA, TABLE_NAME AS VIEW_NAME, VIEW_DEFINITION
            FROM IDENTIFIER(?)
            WHERE TABLE_TYPE = ''VIEW''
              AND VIEW_DEFINITION IS NOT NULL
            USING (v_db || ''.INFORMATION_SCHEMA.VIEWS'');

        FOR r IN cur DO
            LET v_def VARCHAR := UPPER(r.VIEW_DEFINITION);  -- Uppercase for matching
            LET v_view_schema VARCHAR := r.VIEW_SCHEMA;
            LET v_view_name VARCHAR := r.VIEW_NAME;
            LET v_view_def VARCHAR := r.VIEW_DEFINITION;     -- Original case for SQL snippet

            -- Check: does this view reference the target table?
            -- Match both "SILVER.SALES" (fully qualified) and "SALES" (short name)
            IF (POSITION(UPPER(v_fq_table) IN v_def) > 0 OR POSITION(UPPER(v_table) IN v_def) > 0) THEN

                -- Record: this VIEW reads from the target table
                INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                    (TRACE_ID, SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
                     RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, ANALYSIS_LAYER)
                SELECT :v_trace_id, ''VIEW_DEFINITION'', :v_db, :v_view_schema, :v_view_name, NULL,
                       ''VIEW_READS'', ''MEDIUM'', :v_view_def, ''VIEW_ANALYSIS''
                WHERE NOT EXISTS (
                    SELECT 1 FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                    WHERE TRACE_ID = :v_trace_id AND SOURCE_OBJECT = :v_view_name
                      AND SOURCE_SCHEMA = :v_view_schema AND RELATIONSHIP = ''VIEW_READS''
                );
                v_count := v_count + 1;

                -- Check: does the view also reference the specific target column?
                IF (POSITION(UPPER(v_column) IN v_def) > 0) THEN
                    -- Record: this VIEW reads this specific COLUMN
                    INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                        (TRACE_ID, SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
                         RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, ANALYSIS_LAYER)
                    SELECT :v_trace_id, ''VIEW_SOURCE'', :v_db, :v_view_schema, :v_view_name, :v_column,
                           ''VIEW_READS_COLUMN'', ''MEDIUM'', :v_view_def, ''VIEW_ANALYSIS''
                    WHERE NOT EXISTS (
                        SELECT 1 FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                        WHERE TRACE_ID = :v_trace_id AND SOURCE_OBJECT = :v_view_name
                          AND SOURCE_SCHEMA = :v_view_schema AND SOURCE_COLUMN = :v_column
                          AND RELATIONSHIP = ''VIEW_READS_COLUMN''
                    );
                    v_count := v_count + 1;
                END IF;

            END IF;
        END FOR;

        RETURN ''View analysis found '' || :v_count || '' sources'';

    EXCEPTION
        -- Catch all errors so the orchestrator can continue with other layers
        WHEN OTHER THEN
            RETURN ''View analysis error: '' || SQLERRM;
    END;
    ';
--  Say done when ready for Step 7.
-- > done
-- Step 7
    -- ============================================================
    -- STEP 7: CREATE ANALYZE_OBJECT_DEPENDENCIES PROCEDURE
    -- ============================================================
    -- Purpose: Layer 3 of 3 — uses Snowflake's built-in
    --          INFORMATION_SCHEMA.OBJECT_DEPENDENCIES to find
    --          structural dependencies between objects.
    --
    -- How it works:
    --   1. Queries OBJECT_DEPENDENCIES using IDENTIFIER(?) to
    --      dynamically access the target database's metadata.
    --   2. Finds objects that either:
    --      - REFERENCE the target table (e.g., a view built on it)
    --      - ARE REFERENCED BY the target table (e.g., a source table)
    --   3. Records each as a DEPENDENCY relationship with LOW confidence
    --      because structural dependencies don't prove data flow —
    --      they only show that one object "knows about" another.
    --
    -- EXECUTE AS CALLER: Required for cross-database access.
    -- IDENTIFIER(?) with multiple bind params: The USING clause
    --   binds both the table reference AND the WHERE clause values.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE LINEAGE_FRAMEWORK_DB.PUBLIC.ANALYZE_OBJECT_DEPENDENCIES(
        P_TRACE_ID VARCHAR, P_DB VARCHAR, P_SCHEMA VARCHAR, P_TABLE VARCHAR, P_COLUMN VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    '
    DECLARE
        v_trace_id VARCHAR DEFAULT P_TRACE_ID;  -- Unique trace identifier
        v_db       VARCHAR DEFAULT P_DB;        -- Target database
        v_schema   VARCHAR DEFAULT P_SCHEMA;    -- Target schema
        v_table    VARCHAR DEFAULT P_TABLE;     -- Target table
        v_column   VARCHAR DEFAULT P_COLUMN;    -- Target column (not used in this layer)
        v_count    INTEGER DEFAULT 0;           -- Counter for findings
    BEGIN
        -- --------------------------------------------------------
        -- Query OBJECT_DEPENDENCIES for both directions:
        -- 1. Objects that REFERENCE the target table
        --    (REFERENCED_OBJECT_NAME = target)
        -- 2. Objects that the target table REFERENCES
        --    (REFERENCING_OBJECT_NAME = target)
        -- --------------------------------------------------------
        LET cur CURSOR FOR
            SELECT REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_TYPE,
                   REFERENCING_SCHEMA AS REF_SCHEMA,
                   REFERENCED_OBJECT_NAME, REFERENCED_SCHEMA AS REFD_SCHEMA
            FROM IDENTIFIER(?)
            WHERE (REFERENCED_OBJECT_NAME = ? AND REFERENCED_SCHEMA = ?)
               OR (REFERENCING_OBJECT_NAME = ? AND REFERENCING_SCHEMA = ?)
            USING (v_db || ''.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES'',
                   v_table, v_schema, v_table, v_schema);

        FOR r IN cur DO
            LET v_ref_name VARCHAR := r.REFERENCING_OBJECT_NAME;    -- Name of dependent object
            LET v_ref_type VARCHAR := r.REFERENCING_OBJECT_TYPE;    -- Type (VIEW, TABLE, etc.)
            LET v_ref_schema VARCHAR := r.REF_SCHEMA;               -- Schema of dependent object

            -- Record the dependency (LOW confidence — structural only)
            -- WHERE NOT EXISTS prevents duplicates
            INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                (TRACE_ID, SOURCE_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN,
                 RELATIONSHIP, CONFIDENCE, SQL_SNIPPET, ANALYSIS_LAYER)
            SELECT :v_trace_id, ''OBJECT_DEPENDENCY'', :v_db, :v_ref_schema, :v_ref_name, NULL,
                   ''DEPENDENCY'', ''LOW'', :v_ref_type || '' depends on '' || :v_table, ''DEPENDENCY''
            WHERE NOT EXISTS (
                SELECT 1 FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
                WHERE TRACE_ID = :v_trace_id AND SOURCE_OBJECT = :v_ref_name
                  AND SOURCE_SCHEMA = :v_ref_schema AND RELATIONSHIP = ''DEPENDENCY''
            );
            v_count := v_count + 1;
        END FOR;

        RETURN ''Dependency analysis found '' || :v_count || '' sources'';

    EXCEPTION
        -- Catch all errors so the orchestrator can continue
        WHEN OTHER THEN
            RETURN ''Dependency error: '' || SQLERRM;
    END;
    ';
--  Say done when ready for Step 8.
-- > done
-- Step 8
    -- ============================================================
    -- STEP 8: CREATE TRACE_COLUMN_LINEAGE — MAIN ORCHESTRATOR
    -- ============================================================
    -- Purpose: The entry point for all lineage tracing. This is the
    --          procedure you CALL to trace a column's lineage.
    --
    -- How it works:
    --   1. Creates a unique trace ID from DB.SCHEMA.TABLE.COLUMN.TIMESTAMP
    --   2. Inserts a trace record into COLUMN_LINEAGE_TRACES
    --   3. Calls all 3 analysis layers in sequence:
    --      Layer 1: ANALYZE_PROCEDURAL_OBJECTS (stored procedures)
    --      Layer 2: ANALYZE_VIEW_DEFINITIONS (views)
    --      Layer 3: ANALYZE_OBJECT_DEPENDENCIES (structural deps)
    --   4. Cleans up self-references:
    --      Removes entries where the target table appears as its own source.
    --      CRITICAL: AND SOURCE_SCHEMA=:v_schema ensures we only delete
    --      same-schema self-references. Without this fix, tracing
    --      SILVER.SALES would delete BRONZE.SALES findings because
    --      both have TABLE_NAME='SALES'.
    --   5. Deduplicates: keeps only one row per unique source combination
    --   6. Updates the trace record with final count and COMPLETED status
    --
    -- EXECUTE AS CALLER: Required because sub-procedures need to
    --   access INFORMATION_SCHEMA of the target database.
    -- Error handling: If any error occurs, trace is marked FAILED
    --   with the error message for diagnostics.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE(
        P_DB VARCHAR, P_SCHEMA VARCHAR, P_TABLE VARCHAR, P_COLUMN VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    '
    DECLARE
        v_trace_id VARCHAR;                     -- Will be generated below
        v_db       VARCHAR DEFAULT P_DB;        -- Target database (e.g., RETAIL_DB)
        v_schema   VARCHAR DEFAULT P_SCHEMA;    -- Target schema (e.g., SILVER)
        v_table    VARCHAR DEFAULT P_TABLE;     -- Target table (e.g., SALES)
        v_column   VARCHAR DEFAULT P_COLUMN;    -- Target column (e.g., AMOUNT)
        v_result   VARCHAR;                     -- Temp variable for sub-procedure results
        v_count    INTEGER;                     -- Final findings count
    BEGIN
        -- Generate unique trace ID: RETAIL_DB.SILVER.SALES.AMOUNT.20240124_143022
        v_trace_id := v_db || ''.'' || v_schema || ''.'' || v_table || ''.'' || v_column || ''.'' ||
                      TO_CHAR(CURRENT_TIMESTAMP(), ''YYYYMMDD_HH24MISS'');

        -- --------------------------------------------------------
        -- Create the trace header record (status = RUNNING)
        -- --------------------------------------------------------
        INSERT INTO LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
            (TRACE_ID, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
        VALUES (:v_trace_id, :v_db, :v_schema, :v_table, :v_column);

        -- --------------------------------------------------------
        -- Run all 3 analysis layers
        -- --------------------------------------------------------

        -- Layer 1: Scan stored procedures for INSERT/SELECT patterns
        CALL LINEAGE_FRAMEWORK_DB.PUBLIC.ANALYZE_PROCEDURAL_OBJECTS(:v_trace_id, :v_db, :v_schema, :v_table, :v_column);

        -- Layer 2: Parse view definitions for table/column references
        CALL LINEAGE_FRAMEWORK_DB.PUBLIC.ANALYZE_VIEW_DEFINITIONS(:v_trace_id, :v_db, :v_schema, :v_table, :v_column);

        -- Layer 3: Check INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
        CALL LINEAGE_FRAMEWORK_DB.PUBLIC.ANALYZE_OBJECT_DEPENDENCIES(:v_trace_id, :v_db, :v_schema, :v_table, :v_column);

        -- --------------------------------------------------------
        -- POST-PROCESSING: Clean up self-references
        -- --------------------------------------------------------
        -- Remove entries where the target table appears as its own source.
        -- CRITICAL FIX: AND SOURCE_SCHEMA=:v_schema prevents deleting
        -- legitimate cross-schema sources.
        -- Without this: tracing SILVER.SALES deletes BRONZE.SALES
        --   because both have SOURCE_OBJECT=''SALES''.
        -- With this: only SILVER.SALES self-refs are removed.
        -- --------------------------------------------------------
        DELETE FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
        WHERE TRACE_ID = :v_trace_id
          AND SOURCE_SCHEMA = :v_schema        -- Only match same schema as target
          AND SOURCE_OBJECT = :v_table
          AND SOURCE_COLUMN = :v_column
          AND SOURCE_TYPE NOT IN (''VIEW_DEFINITION'', ''OBJECT_DEPENDENCY'', ''COPY_INTO'');

        -- --------------------------------------------------------
        -- POST-PROCESSING: Deduplicate results
        -- --------------------------------------------------------
        -- Keep only the first row for each unique combination of
        -- SOURCE_TYPE + SOURCE_SCHEMA + SOURCE_OBJECT + SOURCE_COLUMN + RELATIONSHIP.
        -- Handles cases where multiple layers find the same source.
        -- --------------------------------------------------------
        DELETE FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS a
        WHERE TRACE_ID = :v_trace_id
          AND ROWID NOT IN (
              SELECT MIN(ROWID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
              WHERE TRACE_ID = :v_trace_id
              GROUP BY SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP
          );

        -- --------------------------------------------------------
        -- FINALIZE: Update trace record with results
        -- --------------------------------------------------------
        SELECT COUNT(*) INTO :v_count
        FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
        WHERE TRACE_ID = :v_trace_id;

        UPDATE LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
        SET TRACE_STATUS = ''COMPLETED'',
            COMPLETED_AT = CURRENT_TIMESTAMP(),
            FINDINGS_COUNT = :v_count
        WHERE TRACE_ID = :v_trace_id;

        RETURN :v_trace_id;

    EXCEPTION
        -- Mark the trace as FAILED with the error message
        WHEN OTHER THEN
            UPDATE LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
            SET TRACE_STATUS = ''FAILED: '' || SQLERRM,
                COMPLETED_AT = CURRENT_TIMESTAMP()
            WHERE TRACE_ID = :v_trace_id;
            RETURN ''ERROR: '' || SQLERRM;
    END;
    ';
--  Say done when ready for Step 9.
-- > done
-- Step 9
    -- ============================================================
    -- STEP 9: CREATE GENERATE_LINEAGE_REPORT PROCEDURE
    -- ============================================================
    -- Purpose: Generates a human-readable text report for any
    --          completed trace. Useful for quick CLI-based review
    --          without needing the Streamlit app.
    --
    -- Output format example:
    --   === COLUMN LINEAGE REPORT ===
    --   Target: RETAIL_DB.SILVER.SALES.AMOUNT
    --   ==========================
    --   [MEDIUM] BRONZE.SALES.AMOUNT (DIRECT_WRITE via PROCEDURE_CALL)
    --   [MEDIUM] SILVER.CLEAN_SALES_TO_SILVER (PROCEDURE_WRITE via PROCEDURE_CALL)
    --   [LOW] GOLD.VW_TOP_CUSTOMERS (DEPENDENCY via OBJECT_DEPENDENCY)
    --
    -- Shows SCHEMA.TABLE.COLUMN format using SOURCE_SCHEMA for
    -- accurate cross-schema display.
    -- EXECUTE AS CALLER: So it can read from LINEAGE_FRAMEWORK_DB tables.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE LINEAGE_FRAMEWORK_DB.PUBLIC.GENERATE_LINEAGE_REPORT(P_TRACE_ID VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    '
    DECLARE
        v_trace_id VARCHAR DEFAULT P_TRACE_ID;  -- Which trace to report on
        v_report   VARCHAR DEFAULT '''';         -- Accumulator for report text
        v_db       VARCHAR;                      -- Target database from trace
        v_schema   VARCHAR;                      -- Target schema from trace
        v_table    VARCHAR;                      -- Target table from trace
        v_column   VARCHAR;                      -- Target column from trace
    BEGIN
        -- --------------------------------------------------------
        -- Fetch trace metadata for the report header
        -- --------------------------------------------------------
        SELECT TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN
        INTO :v_db, :v_schema, :v_table, :v_column
        FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
        WHERE TRACE_ID = :v_trace_id;

        -- --------------------------------------------------------
        -- Build report header
        -- --------------------------------------------------------
        v_report := ''=== COLUMN LINEAGE REPORT ==='' || CHR(10);
        v_report := v_report || ''Target: '' || v_db || ''.'' || v_schema || ''.'' || v_table || ''.'' || v_column || CHR(10);
        v_report := v_report || ''=========================='' || CHR(10) || CHR(10);

        -- --------------------------------------------------------
        -- Iterate all findings, ordered by confidence level
        -- --------------------------------------------------------
        LET cur CURSOR FOR
            SELECT SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP, CONFIDENCE
            FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
            WHERE TRACE_ID = :v_trace_id
            ORDER BY CONFIDENCE, SOURCE_TYPE;

        FOR r IN cur DO
            -- Build display name: SCHEMA.OBJECT or just OBJECT or just SOURCE_TYPE
            LET v_src_display VARCHAR := '''';
            IF (r.SOURCE_SCHEMA IS NOT NULL AND r.SOURCE_OBJECT IS NOT NULL) THEN
                -- Full display: BRONZE.SALES or GOLD.BUILD_CUSTOMER_REVENUE
                v_src_display := r.SOURCE_SCHEMA || ''.'' || r.SOURCE_OBJECT;
            ELSEIF (r.SOURCE_OBJECT IS NOT NULL) THEN
                -- Object name only (no schema available)
                v_src_display := r.SOURCE_OBJECT;
            ELSE
                -- Fallback to source type label
                v_src_display := r.SOURCE_TYPE;
            END IF;

            -- Append column name if available (e.g., BRONZE.SALES.AMOUNT)
            IF (r.SOURCE_COLUMN IS NOT NULL) THEN
                v_src_display := v_src_display || ''.'' || r.SOURCE_COLUMN;
            END IF;

            -- Add line: [MEDIUM] BRONZE.SALES.AMOUNT (DIRECT_WRITE via PROCEDURE_CALL)
            v_report := v_report || ''['' || r.CONFIDENCE || ''] '' || v_src_display ||
                        '' ('' || r.RELATIONSHIP || '' via '' || r.SOURCE_TYPE || '')'' || CHR(10);
        END FOR;

        RETURN v_report;
    END;
    ';
--  Say done when ready for Step 10.
-- > done
-- Step 10
    -- ============================================================
    -- STEP 10: CREATE RETAIL_DB — DEMO DATABASE
    -- ============================================================
    -- Purpose: Creates the demo retail database that demonstrates
    --          the lineage framework with a Bronze/Silver/Gold
    --          medallion architecture.
    --
    -- Architecture overview:
    --   BRONZE: Raw data landing zone (direct inserts, no cleaning)
    --   SILVER: Cleaned data (populated via CLEAN_SALES_TO_SILVER procedure)
    --   GOLD:   Business-ready aggregations (populated via BUILD_CUSTOMER_REVENUE procedure)
    --
    -- Data flow that the lineage framework will detect:
    --   BRONZE.SALES → procedure → SILVER.SALES → procedure → GOLD.CUSTOMER_REVENUE
    --
    -- This database is separate from LINEAGE_FRAMEWORK_DB so you can
    -- see how the framework traces lineage in ANY external database.
    -- ============================================================
    CREATE DATABASE IF NOT EXISTS RETAIL_DB;

--  Say done when ready for Step 11.
-- > done
-- Step 11
    -- ============================================================
    -- STEP 11: CREATE BRONZE SCHEMA AND RAW TABLES
    -- ============================================================
    -- Purpose: Bronze layer — raw data as it arrives.
    --          No transformations, no cleaning, no enrichment.
    --
    -- SALES table: Core sales transactions
    --   SALE_ID       : Unique sale identifier
    --   CUSTOMER_NAME : Who bought it (Alice, Bob, Charlie)
    --   PRODUCT       : What was bought (Widget, Gadget, Gizmo)
    --   AMOUNT        : How much they paid (this is the key column
    --                   that flows through Silver into Gold as
    --                   SUM(AMOUNT) → TOTAL_REVENUE)
    --   SALE_DATE     : When the sale happened
    --
    -- TRANSACTIONS table: Financial transaction log
    --   TXN_ID        : Unique transaction identifier
    --   TXN_TYPE      : SALE or REFUND
    --   TXN_AMOUNT    : Amount (negative for refunds)
    --   TXN_DATE      : When the transaction happened
    --
    -- These tables are the ultimate upstream data sources.
    -- The lineage framework will trace data FROM here through
    -- Silver and into Gold.
    -- ============================================================
    CREATE SCHEMA IF NOT EXISTS RETAIL_DB.BRONZE;

    -- Raw sales data — each row is one sale
    CREATE OR REPLACE TABLE RETAIL_DB.BRONZE.SALES (
        SALE_ID       INTEGER,        -- Unique sale identifier
        CUSTOMER_NAME VARCHAR(200),   -- Who bought it
        PRODUCT       VARCHAR(200),   -- What was bought
        AMOUNT        DECIMAL(10,2),  -- How much they paid
        SALE_DATE     DATE            -- When the sale happened
    );

    -- Raw transaction log — includes refunds
    CREATE OR REPLACE TABLE RETAIL_DB.BRONZE.TRANSACTIONS (
        TXN_ID     INTEGER,          -- Unique transaction identifier
        TXN_TYPE   VARCHAR(50),      -- SALE or REFUND
        TXN_AMOUNT DECIMAL(10,2),    -- Amount (negative for refunds)
        TXN_DATE   DATE              -- When the transaction happened
    );

-- Say done when ready for Step 12.
-- > done
-- Step 12
    -- ============================================================
    -- STEP 12: INSERT SAMPLE DATA INTO BRONZE TABLES
    -- ============================================================
    -- Purpose: Populates Bronze tables with realistic sample data.
    --
    -- SALES data (5 rows):
    --   Alice   : 2 orders (Widget $100, Widget $150) = $250 total
    --   Bob     : 2 orders (Gadget $250, Widget $200) = $450 total
    --   Charlie : 1 order  (Gizmo $300)               = $300 total
    --   These totals will appear in GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE
    --   after the pipeline runs through Silver → Gold.
    --
    -- TRANSACTIONS data (4 rows):
    --   3 sales + 1 refund (negative amount)
    --   Used by GOLD.TXN_SUMMARY (Step 19) to show a second
    --   lineage path from a different Bronze source table.
    -- ============================================================
    -- 5 sales across 3 customers and 3 products
    INSERT INTO RETAIL_DB.BRONZE.SALES VALUES
        (1, 'Alice',   'Widget', 100.00, '2024-01-15'),
        (2, 'Bob',     'Gadget', 250.00, '2024-01-16'),
        (3, 'Alice',   'Widget', 150.00, '2024-02-01'),
        (4, 'Charlie', 'Gizmo',  300.00, '2024-02-10'),
        (5, 'Bob',     'Widget', 200.00, '2024-03-01');

    -- 4 transactions: 3 sales + 1 refund (-$50)
    INSERT INTO RETAIL_DB.BRONZE.TRANSACTIONS VALUES
        (101, 'SALE',   100.00,  '2024-01-15'),
        (102, 'REFUND', -50.00,  '2024-01-20'),
        (103, 'SALE',   250.00,  '2024-01-16'),
        (104, 'SALE',   300.00,  '2024-02-10');
--  Say done when ready for Step 13.
-- > done
-- Step 13
    -- ============================================================
    -- STEP 13: CREATE SILVER SCHEMA AND CLEANED TABLE
    -- ============================================================
    -- Purpose: Silver layer — cleaned and enriched data.
    --          Same core columns as Bronze.SALES but with two
    --          additional columns added during cleaning:
    --
    --   SALE_ID       : Copied directly from Bronze (DIRECT_WRITE)
    --   CUSTOMER_NAME : Copied directly from Bronze (DIRECT_WRITE)
    --   PRODUCT       : Copied directly from Bronze (DIRECT_WRITE)
    --   AMOUNT        : Copied directly from Bronze (DIRECT_WRITE)
    --                   This is the key column the lineage framework
    --                   traces — it flows unchanged from Bronze.
    --   SALE_DATE     : Copied directly from Bronze (DIRECT_WRITE)
    --   IS_VALID      : NEW — computed flag (TRUE if AMOUNT > 0)
    --                   Derived column, not from Bronze directly.
    --   CLEANED_AT    : NEW — timestamp of when cleaning ran.
    --                   Set to CURRENT_TIMESTAMP() during the procedure.
    --
    -- Data flow: BRONZE.SALES → CLEAN_SALES_TO_SILVER → SILVER.SALES
    -- The lineage framework detects this via the procedure in Step 14.
    -- ============================================================
    CREATE SCHEMA IF NOT EXISTS RETAIL_DB.SILVER;
    CREATE OR REPLACE TABLE RETAIL_DB.SILVER.SALES (
        SALE_ID       INTEGER,            -- Copied from Bronze
        CUSTOMER_NAME VARCHAR(200),       -- Copied from Bronze
        PRODUCT       VARCHAR(200),       -- Copied from Bronze
        AMOUNT        DECIMAL(10,2),      -- Copied from Bronze (DIRECT_WRITE lineage)
        SALE_DATE     DATE,               -- Copied from Bronze
        IS_VALID      BOOLEAN,            -- Derived: AMOUNT > 0
        CLEANED_AT    TIMESTAMP_NTZ       -- When this row was cleaned
    );
--  Say done when ready for Step 14.
-- > done
-- Step 14
    -- ============================================================
    -- STEP 14: CREATE CLEAN_SALES_TO_SILVER PROCEDURE
    -- ============================================================
    -- Purpose: Stored procedure that moves data from Bronze to Silver.
    --          This is a key piece of the lineage chain:
    --          BRONZE.SALES.AMOUNT → this procedure → SILVER.SALES.AMOUNT
    --
    -- What it does:
    --   1. TRUNCATE: Clears existing Silver data (full refresh pattern)
    --   2. INSERT...SELECT: Copies all rows from Bronze with:
    --      - IS_VALID = (AMOUNT > 0) — flags bad data
    --      - CLEANED_AT = CURRENT_TIMESTAMP() — audit trail
    --
    -- How the lineage framework detects this:
    --   1. Layer 1 (ANALYZE_PROCEDURAL_OBJECTS) finds this procedure
    --      via INFORMATION_SCHEMA.PROCEDURES
    --   2. Sees the DDL mentions both BRONZE.SALES and SILVER.SALES
    --   3. Matches column names: AMOUNT in source = AMOUNT in target
    --   4. Records as DIRECT_WRITE with MEDIUM confidence
    --   5. Also records CLEAN_SALES_TO_SILVER as PROCEDURE_WRITE
    --
    -- EXECUTE AS OWNER: This is a business procedure, not a framework
    --   procedure. Runs with the owner's privileges which is fine
    --   because it only accesses RETAIL_DB objects.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE RETAIL_DB.SILVER.CLEAN_SALES_TO_SILVER()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    '
    BEGIN
        -- Clear existing Silver data (full refresh pattern)
        TRUNCATE TABLE RETAIL_DB.SILVER.SALES;

        -- Copy from Bronze with cleaning logic:
        -- All 5 columns copied directly (DIRECT_WRITE lineage)
        -- IS_VALID = TRUE when AMOUNT > 0 (filters out bad data)
        -- CLEANED_AT = current timestamp for audit trail
        INSERT INTO RETAIL_DB.SILVER.SALES
        SELECT SALE_ID, CUSTOMER_NAME, PRODUCT, AMOUNT, SALE_DATE,
               (AMOUNT > 0) AS IS_VALID,
               CURRENT_TIMESTAMP() AS CLEANED_AT
        FROM RETAIL_DB.BRONZE.SALES;

        RETURN ''Silver cleaned: '' || SQLROWCOUNT || '' rows'';
    END;
    ';
--  Say done when ready for Step 15.
-- > done
-- Step 15
    -- ============================================================
    -- STEP 15: EXECUTE BRONZE → SILVER PIPELINE
    -- ============================================================
    -- Purpose: Runs the cleaning procedure to populate Silver.
    --          After this, SILVER.SALES will have 5 rows:
    --
    --   SALE_ID | CUSTOMER_NAME | PRODUCT | AMOUNT | SALE_DATE  | IS_VALID | CLEANED_AT
    --   1       | Alice         | Widget  | 100.00 | 2024-01-15 | TRUE     | <now>
    --   2       | Bob           | Gadget  | 250.00 | 2024-01-16 | TRUE     | <now>
    --   3       | Alice         | Widget  | 150.00 | 2024-02-01 | TRUE     | <now>
    --   4       | Charlie       | Gizmo   | 300.00 | 2024-02-10 | TRUE     | <now>
    --   5       | Bob           | Widget  | 200.00 | 2024-03-01 | TRUE     | <now>
    --
    -- Note: The procedure must EXIST (created in Step 14) for the
    --       lineage framework to scan its DDL. Running it here
    --       populates the actual data.
    -- Expected output: "Silver cleaned: 5 rows"
    -- ============================================================
    CALL RETAIL_DB.SILVER.CLEAN_SALES_TO_SILVER();
--  Say done when ready for Step 16.
-- > done
-- Step 16
    -- ============================================================
    -- STEP 16: CREATE GOLD SCHEMA AND AGGREGATION TABLE
    -- ============================================================
    -- Purpose: Gold layer — business-ready aggregated data.
    --          One row per customer with revenue totals.
    --
    -- Key lineage relationships (detected by the framework):
    --   SILVER.SALES.AMOUNT      → SUM()   → TOTAL_REVENUE   (TRANSFORM)
    --   SILVER.SALES.*           → COUNT() → ORDER_COUNT      (TRANSFORM)
    --   SILVER.SALES.SALE_DATE   → MAX()   → LAST_ORDER_DATE  (TRANSFORM)
    --   SILVER.SALES.CUSTOMER_NAME → GROUP BY → CUSTOMER_NAME (DIRECT_WRITE)
    --
    -- TOTAL_REVENUE is the most interesting column to trace because:
    --   - It doesn't exist in Silver (no column named TOTAL_REVENUE)
    --   - It's created by SUM(AMOUNT) AS TOTAL_REVENUE
    --   - The framework detects this as a TRANSFORM relationship
    --     using the position-proximity algorithm in Layer 1
    -- ============================================================
    CREATE SCHEMA IF NOT EXISTS RETAIL_DB.GOLD;
    CREATE OR REPLACE TABLE RETAIL_DB.GOLD.CUSTOMER_REVENUE (
        CUSTOMER_NAME   VARCHAR(200),     -- GROUP BY key (from Silver)
        TOTAL_REVENUE   DECIMAL(12,2),    -- SUM(AMOUNT) — TRANSFORM from Silver
        ORDER_COUNT     INTEGER,          -- COUNT(*) — aggregate
        LAST_ORDER_DATE DATE              -- MAX(SALE_DATE) — aggregate
    );
--  Say done when ready for Step 17.
-- > done
-- Step 17
    -- ============================================================
    -- STEP 17: CREATE BUILD_CUSTOMER_REVENUE PROCEDURE
    -- ============================================================
    -- Purpose: Wraps the Gold aggregation in a stored procedure so
    --          the lineage framework can detect the data flow.
    --
    -- Why a procedure instead of bare INSERT...SELECT?
    --   Without a procedure, the Gold table would be populated by
    --   a standalone SQL statement which wouldn't appear in
    --   INFORMATION_SCHEMA.PROCEDURES. The framework's Layer 1
    --   scans procedure DDL to find lineage, so wrapping in a
    --   procedure makes the data flow visible and traceable.
    --
    -- Key transform patterns detected by the framework:
    --   SUM(AMOUNT) AS TOTAL_REVENUE       → TRANSFORM (AMOUNT is source)
    --   COUNT(*) AS ORDER_COUNT            → TRANSFORM (* is source)
    --   MAX(SALE_DATE) AS LAST_ORDER_DATE  → TRANSFORM (SALE_DATE is source)
    --
    -- False positive prevention:
    --   When tracing TOTAL_REVENUE, the framework finds:
    --     SUM(AMOUNT) at position ~294 in the DDL
    --     AS TOTAL_REVENUE at position ~309 in the DDL
    --     Distance = 15 chars (< 30 char threshold) → confirmed TRANSFORM
    --   It does NOT falsely match MAX(SALE_DATE) because:
    --     MAX(SALE_DATE) is far from AS TOTAL_REVENUE (> 30 chars)
    --     AND there's another AS keyword in between
    --
    -- EXECUTE AS OWNER: Business procedure, not framework procedure.
    -- ============================================================
    CREATE OR REPLACE PROCEDURE RETAIL_DB.GOLD.BUILD_CUSTOMER_REVENUE()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    '
    BEGIN
        -- Clear existing Gold data (full refresh pattern)
        TRUNCATE TABLE RETAIL_DB.GOLD.CUSTOMER_REVENUE;

        -- Aggregate Silver data into Gold:
        -- SUM(AMOUNT)    → TOTAL_REVENUE    (lineage: TRANSFORM)
        -- COUNT(*)       → ORDER_COUNT      (lineage: TRANSFORM)
        -- MAX(SALE_DATE) → LAST_ORDER_DATE  (lineage: TRANSFORM)
        INSERT INTO RETAIL_DB.GOLD.CUSTOMER_REVENUE
        SELECT CUSTOMER_NAME,
               SUM(AMOUNT) AS TOTAL_REVENUE,
               COUNT(*) AS ORDER_COUNT,
               MAX(SALE_DATE) AS LAST_ORDER_DATE
        FROM RETAIL_DB.SILVER.SALES
        GROUP BY CUSTOMER_NAME;

        RETURN ''Gold built: '' || SQLROWCOUNT || '' rows'';
    END;
    ';
--  Say done when ready for Step 18.
-- > done
-- Step 18
    -- ============================================================
    -- STEP 18: EXECUTE SILVER → GOLD PIPELINE
    -- ============================================================
    -- Purpose: Runs the Gold aggregation procedure.
    --          After this, GOLD.CUSTOMER_REVENUE will have 3 rows:
    --
    --   CUSTOMER_NAME | TOTAL_REVENUE | ORDER_COUNT | LAST_ORDER_DATE
    --   Alice         | 250.00        | 2           | 2024-02-01
    --   Bob           | 450.00        | 2           | 2024-03-01
    --   Charlie       | 300.00        | 1           | 2024-02-10
    --
    -- The procedure DDL is now scannable by the lineage framework.
    -- When you trace GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE, Layer 1
    -- will find BUILD_CUSTOMER_REVENUE and detect:
    --   SUM(AMOUNT) AS TOTAL_REVENUE → TRANSFORM relationship
    --
    -- Expected output: "Gold built: 3 rows"
    -- ============================================================
    CALL RETAIL_DB.GOLD.BUILD_CUSTOMER_REVENUE();
--  Say done when ready for Step 19.
-- > done
-- Step 19
    -- ============================================================
    -- STEP 19: CREATE AND POPULATE TXN_SUMMARY (SECOND GOLD TABLE)
    -- ============================================================
    -- Purpose: A second Gold table that aggregates transactions
    --          by type. Demonstrates lineage from a different
    --          Bronze source table (TRANSACTIONS instead of SALES).
    --
    -- After running, TXN_SUMMARY will have 2 rows:
    --   TXN_TYPE | TOTAL  | TXN_COUNT
    --   SALE     | 650.00 | 3
    --   REFUND   | -50.00 | 1
    --
    -- IMPORTANT NOTE about lineage detection:
    --   This uses a bare INSERT...SELECT (no procedure wrapper).
    --   The lineage framework WON'T detect this via Layer 1
    --   (procedural analysis) because there's no procedure DDL
    --   to scan. It will only find it via Layer 3
    --   (OBJECT_DEPENDENCIES) with LOW confidence.
    --   This intentionally shows the difference in detection
    --   between wrapped (procedure) and unwrapped SQL.
    --   To get MEDIUM confidence, you would wrap this in a
    --   procedure like we did for BUILD_CUSTOMER_REVENUE.
    -- ============================================================
    CREATE OR REPLACE TABLE RETAIL_DB.GOLD.TXN_SUMMARY (
        TXN_TYPE   VARCHAR(50),       -- SALE or REFUND
        TOTAL      DECIMAL(12,2),     -- SUM of transaction amounts
        TXN_COUNT  INTEGER            -- COUNT of transactions
    );

    -- Direct INSERT without procedure — less traceable by the framework
    INSERT INTO RETAIL_DB.GOLD.TXN_SUMMARY
    SELECT TXN_TYPE,
           SUM(TXN_AMOUNT) AS TOTAL,
           COUNT(*) AS TXN_COUNT
    FROM RETAIL_DB.BRONZE.TRANSACTIONS
    GROUP BY TXN_TYPE;
--  Say done when ready for Step 20.
-- > done
-- Step 20
    -- ============================================================
    -- STEP 20: CREATE VW_TOP_CUSTOMERS VIEW
    -- ============================================================
    -- Purpose: A view on top of Gold that filters high-value customers
    --          (TOTAL_REVENUE > 200). Demonstrates how the lineage
    --          framework detects downstream consumers via Layer 2
    --          (ANALYZE_VIEW_DEFINITIONS).
    --
    -- After creation, the view returns 2 rows:
    --   CUSTOMER_NAME | TOTAL_REVENUE | ORDER_COUNT
    --   Alice         | 250.00        | 2
    --   Bob           | 450.00        | 2
    --   (Charlie excluded — $300 > $200 so actually 3 rows)
    --
    -- How the lineage framework detects this:
    --   When tracing GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE:
    --   1. Layer 2 scans all views in RETAIL_DB
    --   2. Finds VW_TOP_CUSTOMERS references CUSTOMER_REVENUE
    --   3. Records VIEW_READS relationship (view reads target table)
    --   4. Finds TOTAL_REVENUE in the view definition
    --   5. Records VIEW_READS_COLUMN relationship (view uses target column)
    --
    -- In the Streamlit graph:
    --   Arrow points AWAY from target ("read by") because this view
    --   is a downstream consumer, not an upstream data source.
    -- ============================================================
    CREATE OR REPLACE VIEW RETAIL_DB.GOLD.VW_TOP_CUSTOMERS AS
    SELECT CUSTOMER_NAME, TOTAL_REVENUE, ORDER_COUNT
    FROM RETAIL_DB.GOLD.CUSTOMER_REVENUE
    WHERE TOTAL_REVENUE > 200;
--  Say done when ready for Step 21.
-- > done
-- Step 21
    -- ============================================================
    -- STEP 21: VERIFY FRAMEWORK PROCEDURES EXIST
    -- ============================================================
    -- Purpose: Confirms all 5 framework procedures were created
    --          successfully in LINEAGE_FRAMEWORK_DB.PUBLIC.
    --
    -- Expected procedures (5 rows):
    --   1. ANALYZE_OBJECT_DEPENDENCIES  — Layer 3 (structural deps)
    --   2. ANALYZE_PROCEDURAL_OBJECTS   — Layer 1 (stored procedures)
    --   3. ANALYZE_VIEW_DEFINITIONS     — Layer 2 (view parsing)
    --   4. GENERATE_LINEAGE_REPORT      — Text report generator
    --   5. TRACE_COLUMN_LINEAGE         — Main orchestrator
    --
    -- If you see fewer than 5, re-run the missing step.
    -- All should show EXECUTE AS CALLER except GENERATE_LINEAGE_REPORT
    -- which is also CALLER.
    -- ============================================================
    SHOW PROCEDURES IN SCHEMA LINEAGE_FRAMEWORK_DB.PUBLIC;
--  Say done when ready for Step 22.
-- > done
-- Step 22
    -- ============================================================
    -- STEP 22: VERIFY ALL RETAIL DB OBJECTS
    -- ============================================================
    -- Purpose: Confirms all demo tables, procedures, and views
    --          were created successfully across all schemas.
    --
    -- Expected tables (5 rows):
    --   BRONZE.SALES              — Raw sales data (5 rows)
    --   BRONZE.TRANSACTIONS       — Raw transaction log (4 rows)
    --   SILVER.SALES              — Cleaned sales data (5 rows)
    --   GOLD.CUSTOMER_REVENUE     — Aggregated revenue per customer (3 rows)
    --   GOLD.TXN_SUMMARY          — Aggregated transactions by type (2 rows)
    --
    -- Expected procedures (2 rows):
    --   SILVER.CLEAN_SALES_TO_SILVER  — Bronze → Silver pipeline
    --   GOLD.BUILD_CUSTOMER_REVENUE   — Silver → Gold pipeline
    --
    -- Expected views (1 row):
    --   GOLD.VW_TOP_CUSTOMERS     — Filters customers with revenue > 200
    --
    -- If any are missing, re-run the corresponding step.
    -- ============================================================
    SHOW TABLES IN DATABASE RETAIL_DB;
    SHOW PROCEDURES IN DATABASE RETAIL_DB;
    SHOW VIEWS IN DATABASE RETAIL_DB;

--  Say done when ready for Step 23.
-- > done
-- Step 23
    -- ============================================================
    -- STEP 23: TEST LINEAGE TRACE — SILVER.SALES.AMOUNT
    -- ============================================================
    -- Purpose: First real test of the framework.
    --          Traces where SILVER.SALES.AMOUNT gets its data from.
    --
    -- What happens when you run this:
    --   1. Orchestrator creates trace ID and header record
    --   2. Layer 1 (Procedural): Scans all procedures in RETAIL_DB
    --      - Finds CLEAN_SALES_TO_SILVER: mentions BRONZE.SALES and SILVER.SALES
    --        - BRONZE.SALES.AMOUNT → SILVER.SALES.AMOUNT = DIRECT_WRITE
    --        - CLEAN_SALES_TO_SILVER = PROCEDURE_WRITE
    --      - Finds BUILD_CUSTOMER_REVENUE: mentions SILVER.SALES
    --        - Records as PROCEDURE_WRITE (downstream consumer)
    --   3. Layer 2 (Views): Scans all views in RETAIL_DB
    --      - No views directly reference SILVER.SALES (VW_TOP_CUSTOMERS
    --        references GOLD.CUSTOMER_REVENUE, not SILVER.SALES)
    --   4. Layer 3 (Dependencies): Checks OBJECT_DEPENDENCIES
    --      - May find structural dependencies (LOW confidence)
    --   5. Post-processing: Removes self-references, deduplicates
    --
    -- Expected findings:
    --   [MEDIUM] BRONZE.SALES.AMOUNT — DIRECT_WRITE via PROCEDURE_CALL
    --   [MEDIUM] SILVER.CLEAN_SALES_TO_SILVER — PROCEDURE_WRITE
    --   [MEDIUM] GOLD.BUILD_CUSTOMER_REVENUE — PROCEDURE_WRITE (downstream)
    --   [LOW]    Various OBJECT_DEPENDENCY entries
    --
    -- Returns: The trace ID string (use in Step 24 to view results)
    -- ============================================================
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE('RETAIL_DB', 'SILVER', 'SALES', 'AMOUNT');
--  Say done when ready for Step 24.
-- > done
-- Step 24
    -- ============================================================
    -- STEP 24: VIEW LINEAGE RESULTS FROM THE TEST TRACE
    -- ============================================================
    -- Purpose: Shows all lineage findings from the most recent trace
    --          (the SILVER.SALES.AMOUNT trace from Step 23).
    --
    -- Displays 6 columns:
    --   SOURCE_TYPE   : What kind of source (PROCEDURE_CALL, OBJECT_DEPENDENCY)
    --   SOURCE_SCHEMA : Which schema the source lives in (BRONZE, SILVER, GOLD)
    --   SOURCE_OBJECT : Table or procedure name
    --   SOURCE_COLUMN : Specific column (NULL for procedure-level entries)
    --   RELATIONSHIP  : How data flows (DIRECT_WRITE, PROCEDURE_WRITE, DEPENDENCY)
    --   CONFIDENCE    : MEDIUM or LOW
    --
    -- The subquery (SELECT MAX(TRACE_ID)...) automatically picks
    -- the latest trace so you don't need to copy-paste the trace ID.
    --
    -- Expected rows (example):
    --   PROCEDURE_CALL | BRONZE | SALES                    | AMOUNT | DIRECT_WRITE    | MEDIUM
    --   PROCEDURE_CALL | SILVER | CLEAN_SALES_TO_SILVER    | NULL   | PROCEDURE_WRITE | MEDIUM
    --   PROCEDURE_CALL | GOLD   | BUILD_CUSTOMER_REVENUE   | NULL   | PROCEDURE_WRITE | MEDIUM
    --   OBJECT_DEPENDENCY | ...  | ...                     | NULL   | DEPENDENCY      | LOW
    --
    -- Ordered by CONFIDENCE (MEDIUM first) then SOURCE_TYPE.
    -- ============================================================
    SELECT SOURCE_TYPE,
           SOURCE_SCHEMA,
           SOURCE_OBJECT,
           SOURCE_COLUMN,
           RELATIONSHIP,
           CONFIDENCE
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = (
        SELECT MAX(TRACE_ID)
        FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
    )
    ORDER BY CONFIDENCE, SOURCE_TYPE;
--  Say done when ready for Step 25.
-- > done
-- Step 25
    -- ============================================================
    -- STEP 25: TEST LINEAGE TRACE — GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE
    -- ============================================================
    -- Purpose: Tests the TRANSFORM detection feature — the most
    --          complex part of the framework.
    --          TOTAL_REVENUE doesn't exist in Silver. It's created
    --          by SUM(AMOUNT) AS TOTAL_REVENUE in BUILD_CUSTOMER_REVENUE.
    --
    -- What happens when you run this:
    --   1. Layer 1 (Procedural): Scans all procedures in RETAIL_DB
    --      - Finds BUILD_CUSTOMER_REVENUE: mentions SILVER.SALES
    --        and CUSTOMER_REVENUE
    --        - Checks each Silver column against the procedure DDL:
    --          - AMOUNT: finds SUM(AMOUNT) at pos ~294, AS TOTAL_REVENUE
    --            at pos ~309, distance=15 < 30, no intermediate AS
    --            → confirmed TRANSFORM
    --          - SALE_DATE: finds MAX(SALE_DATE) but it's far from
    --            AS TOTAL_REVENUE (> 30 chars) → rejected (no false positive)
    --          - CUSTOMER_NAME: same name in source and target but
    --            TRANSFORM check runs first... falls through to DIRECT_WRITE
    --        - Records BUILD_CUSTOMER_REVENUE as PROCEDURE_WRITE
    --   2. Layer 2 (Views): Scans all views
    --      - Finds VW_TOP_CUSTOMERS references CUSTOMER_REVENUE
    --        - Records VIEW_READS (view reads target table)
    --        - Finds TOTAL_REVENUE in view definition
    --        - Records VIEW_READS_COLUMN (view uses target column)
    --   3. Layer 3 (Dependencies): Structural dependencies
    --
    -- Expected findings:
    --   [MEDIUM] SILVER.SALES.AMOUNT — TRANSFORM via PROCEDURE_CALL
    --   [MEDIUM] GOLD.BUILD_CUSTOMER_REVENUE — PROCEDURE_WRITE
    --   [MEDIUM] GOLD.VW_TOP_CUSTOMERS — VIEW_READS
    --   [MEDIUM] GOLD.VW_TOP_CUSTOMERS.TOTAL_REVENUE — VIEW_READS_COLUMN
    --   [LOW]    OBJECT_DEPENDENCY entries
    --
    -- This verifies:
    --   1. Cross-schema detection (Gold target ← Silver source)
    --   2. Transform detection (SUM(AMOUNT) AS TOTAL_REVENUE)
    --   3. No false positives (MAX(SALE_DATE) not matched)
    --   4. View detection (VW_TOP_CUSTOMERS reads target)
    -- ============================================================
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE('RETAIL_DB', 'GOLD', 'CUSTOMER_REVENUE', 'TOTAL_REVENUE');
--  Say done when ready for Step 26.
-- > done
-- Step 26
    -- ============================================================
    -- STEP 26: GENERATE HUMAN-READABLE LINEAGE REPORT
    -- ============================================================
    -- Purpose: Generates a text report for the latest trace
    --          (GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE from Step 25).
    --          Useful for quick review in the Snowflake worksheet
    --          without needing the Streamlit app.
    --
    -- The subquery automatically picks the latest trace ID.
    --
    -- Expected output (example):
    --   === COLUMN LINEAGE REPORT ===
    --   Target: RETAIL_DB.GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE
    --   ==========================
    --
    --   [MEDIUM] SILVER.SALES.AMOUNT (TRANSFORM via PROCEDURE_CALL)
    --   [MEDIUM] GOLD.BUILD_CUSTOMER_REVENUE (PROCEDURE_WRITE via PROCEDURE_CALL)
    --   [MEDIUM] GOLD.VW_TOP_CUSTOMERS (VIEW_READS via VIEW_DEFINITION)
    --   [MEDIUM] GOLD.VW_TOP_CUSTOMERS.TOTAL_REVENUE (VIEW_READS_COLUMN via VIEW_SOURCE)
    --   [LOW] GOLD.VW_TOP_CUSTOMERS (DEPENDENCY via OBJECT_DEPENDENCY)
    --
    -- Reading the report:
    --   [MEDIUM] = Found by parsing procedure/view SQL (good confidence)
    --   [LOW]    = Found by structural dependency only (weaker signal)
    --   TRANSFORM = Aggregate function applied (SUM, AVG, etc.)
    --   DIRECT_WRITE = Same column name copied directly
    --   PROCEDURE_WRITE = A stored procedure writes to this table
    --   VIEW_READS = A view reads from this table
    -- ============================================================
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.GENERATE_LINEAGE_REPORT(
        (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    );

--  Say done when ready for Step 27 (Streamlit deployment).
-- > done
-- Step 27: Deploy Streamlit App
    -- ============================================================
    -- STEP 27: DEPLOY STREAMLIT APP IN SNOWSIGHT
    -- ============================================================
    -- This is a manual step done in the Snowsight web interface.
    -- No SQL to run — follow the instructions below.
    --
    -- INSTRUCTIONS:
    --
    --   1. Log into Snowsight (app.snowflake.com)
    --
    --   2. Navigate to: Projects > Streamlit > + Streamlit App
    --
    --   3. Configure the app with these settings:
    --      - App name  : Column Lineage Explorer
    --      - Database   : LINEAGE_FRAMEWORK_DB
    --      - Schema     : PUBLIC
    --      - Warehouse  : LINEAGE_WH
    --
    --   4. Delete ALL the default template code in the editor
    --
    --   5. Paste the full contents of the Python file:
    --      C:\Users\VIVBANSA\column_lineage_explorer.py
    --      (408 lines — the complete Streamlit app)
    --
    --   6. Click "Run" to start the app
    --
    -- WHAT THE APP PROVIDES:
    --   - Sidebar: Database/Schema/Table/Column picker
    --   - "Trace Lineage" button to run a new trace
    --   - Trace History: last 20 traces, click to revisit
    --   - 4 tabs:
    --     Tab 1 — Lineage Graph:
    --       Graphviz data flow diagram with:
    --       * Color-coded confidence (green=HIGH, yellow=MEDIUM, gray=LOW)
    --       * Shape-coded types (hexagon=procedure, diamond=view, cylinder=stage)
    --       * Smart arrow directions:
    --         - Upstream writers → arrow TOWARD target (source, proc write, transform)
    --         - Downstream consumers → arrow AWAY from target (read by, depends)
    --         - Cross-schema procedures detected as downstream readers
    --     Tab 2 — Findings:
    --       Card-based list of all findings with confidence icons,
    --       relationship descriptions, and explanations
    --     Tab 3 — SQL Inspector:
    --       Expandable panels showing the actual SQL code where
    --       each relationship was detected
    --     Tab 4 — Raw Data:
    --       Table view of all results + trace metadata JSON
    --
    -- VERIFICATION:
    --   After clicking Run, the app should load with:
    --   - Sidebar showing database/schema/table dropdowns
    --   - Previous traces from Steps 23 and 25 in Trace History
    --   - Click any trace to see its lineage graph
    -- ============================================================
--  The Python file is already saved at C:\Users\VIVBANSA\column_lineage_explorer.py. Open it, copy all 408 lines, and paste into the Streamlit editor.
--  Say done when ready for Step 28 (optional cleanup).
-- > done
-- Step 28 (Optional): Cleanup Debug Objects
    -- ============================================================
    -- STEP 28 (OPTIONAL): CLEANUP DEBUG AND TEST OBJECTS
    -- ============================================================
    -- Purpose: Removes debug procedures and test data created
    --          during the development/troubleshooting process.
    --          Safe to skip if you never created these objects.
    --          Safe to run even if the objects don't exist
    --          (IF EXISTS prevents errors).
    --
    -- What gets removed:
    --   1. DEBUG_FULL_FLOW: Debug procedure for full pipeline testing
    --   2. DEBUG_COL_CHECK: Debug procedure for column matching logic
    --   3. DEBUG_PROC_SCAN: Debug procedure for procedure scanning
    --   4. DEBUG_TEMP_SQL: Debug procedure for temp table SQL generation
    --   5. test123 data: Test rows inserted during manual debugging
    --   6. ANALYZE_QUERY_HISTORY: Unused procedure — was designed for
    --      ACCOUNT_USAGE.QUERY_HISTORY but that dependency was removed
    --      (you chose "Skip query history" option)
    -- ============================================================
  ────────────────────────────────────────

  That's all 28 steps complete. Here's a summary of what you now have:

  LINEAGE_FRAMEWORK_DB (Framework)
  • 2 tables: COLUMN_LINEAGE_TRACES, COLUMN_LINEAGE_RESULTS
  • 5 procedures: TRACE_COLUMN_LINEAGE, ANALYZE_PROCEDURAL_OBJECTS, ANALYZE_VIEW_DEFINITIONS, ANALYZE_OBJECT_DEPENDENCIES, GENERATE_LINEAGE_REPORT
  RETAIL_DB (Demo Data)
  • 5 tables across Bronze/Silver/Gold
  • 2 procedures: CLEAN_SALES_TO_SILVER, BUILD_CUSTOMER_REVENUE
  • 1 view: VW_TOP_CUSTOMERS

  Streamlit App: Column Lineage Explorer with graph visualization, findings, SQL inspector, and raw data tabs.
  To trace any column's lineage going forward:
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE('<database>', '<schema>', '<table>', '<column>');
	CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE('RETAIL_DB', 'SILVER', 'SALES', 'AMOUNT');
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.GENERATE_LINEAGE_REPORT('eb881c18-e930-460f-ad06-17d58b7a75a0');
