--Here are all the test cases with commands and expected results.
──────────────────────────────────────────────────────────────────────────────────────────────────────
  Test Case 1: Silver AMOUNT — Direct Write Detection

  What we are testing: Can the framework detect a column copied directly from Bronze to Silver via a stored procedure?

  Data flow: BRONZE.SALES.AMOUNT → CLEAN_SALES_TO_SILVER → SILVER.SALES.AMOUNT

    -- Run the trace
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE(  are TAIL_DB', 'SILVER', 'SALES', 'AMOUNT');

    -- View results
    SELECT SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP, CONFIDENCE
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    ORDER BY CONFIDENCE, SOURCE_TYPE;

  Expected Results:

  ┌───────────────────┬───────────────┬────────────────────────┬───────────────┬─────────────────┬────────────┐
  │ SOURCE_TYPE       │ SOURCE_SCHEMA │ SOURCE_OBJECT          │ SOURCE_COLUMN │ RELATIONSHIP    │ CONFIDENCE │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ BRONZE        │ SALES                  │ AMOUNT        │ DIRECT_WRITE    │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ GOLD          │ BUILD_CUSTOMER_REVENUE │ NULL          │ PROCEDURE_WRITE │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ SILVER        │ CLEAN_SALES_TO_SILVER  │ NULL          │ PROCEDURE_WRITE │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ OBJECT_DEPENDENCY │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ DEPENDENCY      │ LOW        │
  └───────────────────┴───────────────┴────────────────────────┴───────────────┴─────────────────┴────────────┘

  What each row means:

  • Row 1: AMOUNT is copied directly from BRONZE.SALES (same column name = DIRECT_WRITE)
  • Row 2: BUILD_CUSTOMER_REVENUE reads from SILVER.SALES (downstream consumer)
  • Row 3: CLEAN_SALES_TO_SILVER writes to SILVER.SALES (the procedure that populates it)
  • Row 4: VW_TOP_CUSTOMERS has a structural dependency (LOW confidence)

  ────────────────────────────────────────

  Test Case 2: Gold TOTAL_REVENUE — Transform Detection

  What we are testing: Can the framework detect SUM(AMOUNT) AS TOTAL_REVENUE as a TRANSFORM relationship?

  Data flow: SILVER.SALES.AMOUNT → SUM() → GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE

    -- Run the trace
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE(  are TAIL_DB', 'GOLD', 'CUSTOMER_REVENUE', 'TOTAL_REVENUE');

    -- View results
    SELECT SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP, CONFIDENCE
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    ORDER BY CONFIDENCE, SOURCE_TYPE;

  Expected Results:

  ┌───────────────────┬───────────────┬────────────────────────┬───────────────┬───────────────────┬────────────┐
  │ SOURCE_TYPE       │ SOURCE_SCHEMA │ SOURCE_OBJECT          │ SOURCE_COLUMN │ RELATIONSHIP      │ CONFIDENCE │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ PROCEDURE_CALL    │ GOLD          │ BUILD_CUSTOMER_REVENUE │ NULL          │ PROCEDURE_WRITE   │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ PROCEDURE_CALL    │ SILVER        │ SALES                  │ AMOUNT        │ TRANSFORM         │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ VIEW_DEFINITION   │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ VIEW_READS        │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ VIEW_SOURCE       │ GOLD          │ VW_TOP_CUSTOMERS       │ TOTAL_REVENUE │ VIEW_READS_COLUMN │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ OBJECT_DEPENDENCY │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ DEPENDENCY        │ LOW        │
  └───────────────────┴───────────────┴────────────────────────┴───────────────┴───────────────────┴────────────┘

  What each row means:

  • Row 1: BUILD_CUSTOMER_REVENUE is the procedure that writes to this table
  • Row 2: SILVER.SALES.AMOUNT is the source, transformed via SUM() — this is the key finding
  • Row 3: VW_TOP_CUSTOMERS view reads from the target table (downstream)
  • Row 4: VW_TOP_CUSTOMERS view specifically uses the TOTAL_REVENUE column
  • Row 5: Structural dependency on VW_TOP_CUSTOMERS (LOW confidence)

  Key verification:

  • AMOUNT should show as TRANSFORM (not DIRECT_WRITE) — confirms SUM() detection works
  • SALE_DATE should NOT appear — confirms no false positive from MAX(SALE_DATE)

  ────────────────────────────────────────

  Test Case 3: Gold CUSTOMER_NAME — Direct Write Across Schemas

  What we  are  testing: Can the framework detect a column that passes through Silver to Gold with the same name (GROUP BY key)?

  Data flow: SILVER.SALES.CUSTOMER_NAME → GROUP BY → GOLD.CUSTOMER_REVENUE.CUSTOMER_NAME

    -- Run the trace
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE(  are TAIL_DB', 'GOLD', 'CUSTOMER_REVENUE', 'CUSTOMER_NAME');

    -- View results
    SELECT SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP, CONFIDENCE
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    ORDER BY CONFIDENCE, SOURCE_TYPE;

  Expected Results:

  ┌───────────────────┬───────────────┬────────────────────────┬───────────────┬───────────────────┬────────────┐
  │ SOURCE_TYPE       │ SOURCE_SCHEMA │ SOURCE_OBJECT          │ SOURCE_COLUMN │ RELATIONSHIP      │ CONFIDENCE │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ PROCEDURE_CALL    │ GOLD          │ BUILD_CUSTOMER_REVENUE │ NULL          │ PROCEDURE_WRITE   │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ PROCEDURE_CALL    │ SILVER        │ SALES                  │ CUSTOMER_NAME │ DIRECT_WRITE      │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ VIEW_DEFINITION   │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ VIEW_READS        │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ VIEW_SOURCE       │ GOLD          │ VW_TOP_CUSTOMERS       │ CUSTOMER_NAME │ VIEW_READS_COLUMN │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼───────────────────┼────────────┤
  │ OBJECT_DEPENDENCY │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ DEPENDENCY        │ LOW        │
  └───────────────────┴───────────────┴────────────────────────┴───────────────┴───────────────────┴────────────┘

  What each row means:

  • Row 1: BUILD_CUSTOMER_REVENUE writes to the target table
  • Row 2: CUSTOMER_NAME comes directly from SILVER.SALES (same name = DIRECT_WRITE)
  • Row 3-4: VW_TOP_CUSTOMERS reads this table and uses CUSTOMER_NAME
  • Row 5: Structural dependency

  Key verification:

  • CUSTOMER_NAME should show as DIRECT_WRITE (not TRANSFORM) — same column name, no aggregate

  ────────────────────────────────────────

  Test Case 4: Gold LAST_ORDER_DATE — MAX Transform Detection

  What we  are  testing: Can the framework detect MAX(SALE_DATE) AS LAST_ORDER_DATE as a TRANSFORM?

  Data flow: SILVER.SALES.SALE_DATE → MAX() → GOLD.CUSTOMER_REVENUE.LAST_ORDER_DATE

    -- Run the trace
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE(  are TAIL_DB', 'GOLD', 'CUSTOMER_REVENUE', 'LAST_ORDER_DATE');

    -- View results
    SELECT SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP, CONFIDENCE
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    ORDER BY CONFIDENCE, SOURCE_TYPE;

  Expected Results:

  ┌───────────────────┬───────────────┬────────────────────────┬───────────────┬─────────────────┬────────────┐
  │ SOURCE_TYPE       │ SOURCE_SCHEMA │ SOURCE_OBJECT          │ SOURCE_COLUMN │ RELATIONSHIP    │ CONFIDENCE │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ GOLD          │ BUILD_CUSTOMER_REVENUE │ NULL          │ PROCEDURE_WRITE │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ SILVER        │ SALES                  │ SALE_DATE     │ TRANSFORM       │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ VIEW_DEFINITION   │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ VIEW_READS      │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ OBJECT_DEPENDENCY │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ DEPENDENCY      │ LOW        │
  └───────────────────┴───────────────┴────────────────────────┴───────────────┴─────────────────┴────────────┘

  What each row means:

  • Row 1: BUILD_CUSTOMER_REVENUE writes to the target table
  • Row 2: SALE_DATE is transformed via MAX() into LAST_ORDER_DATE
  • Row 3: VW_TOP_CUSTOMERS reads from target table (but doesn't use LAST_ORDER_DATE column — so no VIEW_READS_COLUMN row)
  • Row 4: Structural dependency

  Key verification:

  • SALE_DATE should show as TRANSFORM (MAX detected)
  • No VIEW_READS_COLUMN row because LAST_ORDER_DATE is NOT in VW_TOP_CUSTOMERS

  ────────────────────────────────────────

  Test Case 5: Silver CUSTOMER_NAME — Full Chain Detection

  What we  are  testing: Can the framework trace a column that exists in both Bronze and Silver with the same name?

  Data flow: BRONZE.SALES.CUSTOMER_NAME → CLEAN_SALES_TO_SILVER → SILVER.SALES.CUSTOMER_NAME

    -- Run the trace
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.TRACE_COLUMN_LINEAGE(  are TAIL_DB', 'SILVER', 'SALES', 'CUSTOMER_NAME');

    -- View results
    SELECT SOURCE_TYPE, SOURCE_SCHEMA, SOURCE_OBJECT, SOURCE_COLUMN, RELATIONSHIP, CONFIDENCE
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_RESULTS
    WHERE TRACE_ID = (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    ORDER BY CONFIDENCE, SOURCE_TYPE;

  Expected Results:

  ┌───────────────────┬───────────────┬────────────────────────┬───────────────┬─────────────────┬────────────┐
  │ SOURCE_TYPE       │ SOURCE_SCHEMA │ SOURCE_OBJECT          │ SOURCE_COLUMN │ RELATIONSHIP    │ CONFIDENCE │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ BRONZE        │ SALES                  │ CUSTOMER_NAME │ DIRECT_WRITE    │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ GOLD          │ BUILD_CUSTOMER_REVENUE │ NULL          │ PROCEDURE_WRITE │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ PROCEDURE_CALL    │ SILVER        │ CLEAN_SALES_TO_SILVER  │ NULL          │ PROCEDURE_WRITE │ MEDIUM     │
  ├───────────────────┼───────────────┼────────────────────────┼───────────────┼─────────────────┼────────────┤
  │ OBJECT_DEPENDENCY │ GOLD          │ VW_TOP_CUSTOMERS       │ NULL          │ DEPENDENCY      │ LOW        │
  └───────────────────┴───────────────┴────────────────────────┴───────────────┴─────────────────┴────────────┘

  What each row means:

  • Row 1: CUSTOMER_NAME copied directly from BRONZE.SALES (DIRECT_WRITE)
  • Row 2: BUILD_CUSTOMER_REVENUE reads SILVER.SALES (downstream consumer)
  • Row 3: CLEAN_SALES_TO_SILVER writes to SILVER.SALES (upstream procedure)
  • Row 4: Structural dependency on VW_TOP_CUSTOMERS

  Key verification:

  • BRONZE.SALES should appear (cross-schema detection works)
  • SILVER.SALES should NOT appear as a source (self-reference cleanup works)

  ────────────────────────────────────────

  Test Case 6: Generate Text Report

  What we  are  testing: Does the report procedure produce readable output?

    -- Generate report for the latest trace
    CALL LINEAGE_FRAMEWORK_DB.PUBLIC.GENERATE_LINEAGE_REPORT(
        (SELECT MAX(TRACE_ID) FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES)
    );

  Expected Output (text):

    === COLUMN LINEAGE REPORT ===
    Target: RETAIL_DB.SILVER.SALES.CUSTOMER_NAME
    ==========================

    [MEDIUM] BRONZE.SALES.CUSTOMER_NAME (DIRECT_WRITE via PROCEDURE_CALL)
    [MEDIUM] GOLD.BUILD_CUSTOMER_REVENUE (PROCEDURE_WRITE via PROCEDURE_CALL)
    [MEDIUM] SILVER.CLEAN_SALES_TO_SILVER (PROCEDURE_WRITE via PROCEDURE_CALL)
    [LOW] GOLD.VW_TOP_CUSTOMERS (DEPENDENCY via OBJECT_DEPENDENCY)

  Key verification:

  • All sources show SCHEMA.OBJECT format (not just OBJECT)
  • MEDIUM findings listed before LOW
  • Target line shows full path: DB.SCHEMA.TABLE.COLUMN

  ────────────────────────────────────────

  Test Case 7: Trace History Verification

  What we  are  testing: Are all traces recorded in the traces table?

    -- Check all traces run so far
    SELECT TRACE_ID,
           TARGET_SCHEMA,
           TARGET_TABLE,
           TARGET_COLUMN,
           TRACE_STATUS,
           FINDINGS_COUNT,
           TO_CHAR(STARTED_AT, 'YYYY-MM-DD HH24:MI:SS') AS STARTED
    FROM LINEAGE_FRAMEWORK_DB.PUBLIC.COLUMN_LINEAGE_TRACES
    ORDER BY STARTED_AT DESC;

  Expected Results:

  • Multiple rows, one per trace you've run
  • All should show TRACE_STATUS = COMPLETED
  • FINDINGS_COUNT should be > 0 for each
  • If any show FAILED: <error>, re-run that trace

  ────────────────────────────────────────

  Test Case 8: Streamlit App Verification

  What we  are  testing: Does the Streamlit app render correctly?

  Steps:

  1. Open the Column Lineage Explorer app in Snowsight
  2. In the sidebar, select:
     • Database: RETAIL_DB
     • Schema: GOLD
     • Table: CUSTOMER_REVENUE
     • Column: TOTAL_REVENUE
  3. Click Trace Lineage

  Expected in Lineage Graph tab:

  • Red box: GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE (target)
  • Yellow box: SILVER.SALES.AMOUNT with arrow toward target labeled "transform"
  • Yellow hexagon: GOLD.BUILD_CUSTOMER_REVENUE with arrow toward target labeled "proc write"
  • Yellow diamond: GOLD.VW_TOP_CUSTOMERS with arrow away from target labeled "read by"
  • Gray diamond: GOLD.VW_TOP_CUSTOMERS (dependency) with dashed arrow away

  Expected in Findings tab:

  • Cards showing each finding with confidence icons and explanations

  Expected in SQL Inspector tab:

  • Expandable panels with actual procedure/view SQL code

  Expected in Raw Data tab:

  • Table with all results and trace metadata JSON

  ────────────────────────────────────────

  Test Case Summary

  Row 1
  #: 1
  Trace Target: SILVER.SALES.AMOUNT
  Key Verification: Direct write from Bronze
  Pass Criteria: BRONZE.SALES.AMOUNT = DIRECT_WRITE
  ────────────────────────────────────────────────────────────
  Row 2
  #: 2
  Trace Target: GOLD.CUSTOMER_REVENUE.TOTAL_REVENUE
  Key Verification: SUM transform detection
  Pass Criteria: SILVER.SALES.AMOUNT = TRANSFORM, no SALE_DATE false positive
  ────────────────────────────────────────────────────────────
  Row 3
  #: 3
  Trace Target: GOLD.CUSTOMER_REVENUE.CUSTOMER_NAME
  Key Verification: Same-name cross-schema
  Pass Criteria: SILVER.SALES.CUSTOMER_NAME = DIRECT_WRITE
  ────────────────────────────────────────────────────────────
  Row 4
  #: 4
  Trace Target: GOLD.CUSTOMER_REVENUE.LAST_ORDER_DATE
  Key Verification: MAX transform detection
  Pass Criteria: SILVER.SALES.SALE_DATE = TRANSFORM
  ────────────────────────────────────────────────────────────
  Row 5
  #: 5
  Trace Target: SILVER.SALES.CUSTOMER_NAME
  Key Verification: Cross-schema + self-ref cleanup
  Pass Criteria: BRONZE.SALES found, SILVER.SALES not self-referenced
  ────────────────────────────────────────────────────────────
  Row 6
  #: 6
  Trace Target: Report generation
  Key Verification: Readable text output
  Pass Criteria: SCHEMA.OBJECT format, ordered by confidence
  ────────────────────────────────────────────────────────────
  Row 7
  #: 7
  Trace Target: Trace history
  Key Verification: All traces recorded
  Pass Criteria: All COMPLETED, FINDINGS_COUNT > 0
  ────────────────────────────────────────────────────────────
  Row 8
  #: 8
  Trace Target: Streamlit app
  Key Verification: Visual graph correctness
  Pass Criteria: Correct arrow directions, shapes, colors

  Run Test Cases 1-7 in your Snowflake worksheet. Test Case 8 is done in the Streamlit app. Say done after each or let me know if any
  results don't match.
