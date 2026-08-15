# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "61816ce7-e600-44cb-a900-02fc677fd1e8",
# META       "default_lakehouse_name": "lh_procurement_gold",
# META       "default_lakehouse_workspace_id": "83e05aab-2eed-49cb-a339-674db19d4b92",
# META       "known_lakehouses": [
# META         {
# META           "id": "61816ce7-e600-44cb-a900-02fc677fd1e8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_40 – Validate ML Gold Outputs
# 
# Performs final Fabric-side validation of the Databricks ML data products promoted to Gold, including supplier risk, pricing anomalies, and savings opportunities.
# 
# The notebook validates output grain, surrogate and dimensional keys, score ranges, SCD2 alignment, cross-product consistency, source-to-Gold reconciliation, lineage, promotion monitoring, and data freshness before the ML tables are exposed to the semantic model.

# MARKDOWN ********************

# **Imports and validation configuration**

# CELL ********************

# ============================================================
# NB_40_Validate_ML_Gold_Outputs
#
# Purpose:
# - Validate Databricks ML products promoted into Fabric Gold
# - Validate business grain and deterministic keys
# - Validate Gold referential integrity
# - Validate Supplier SCD2 alignment
# - Reconcile ML products with DB_07 promotion monitoring
# - Validate cross-product consistency
# - Persist final ML Gold data-quality monitoring
#
# Default Lakehouse:
# lh_procurement_gold
# ============================================================

from datetime import date, datetime, timezone

import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ------------------------------------------------------------
# Expected analytical snapshot
# ------------------------------------------------------------

EXPECTED_PREDICTION_DATE = date(
    2026,
    7,
    31
)

EXPECTED_PREDICTION_DATE_KEY = 20260731


# ------------------------------------------------------------
# Freshness
#
# Promotion freshness is measured from DB_07 monitoring
# execution metadata, not from SCD row-version timestamps.
# ------------------------------------------------------------

MAX_PROMOTION_AGE_DAYS = 2


# ------------------------------------------------------------
# Numerical tolerances
# ------------------------------------------------------------

MONETARY_TOLERANCE_EUR = 0.02

SCORE_TOLERANCE = 0.000001


# ------------------------------------------------------------
# Validation execution metadata
# ------------------------------------------------------------

VALIDATION_EXECUTION_ID = str(
    uuid.uuid4()
)

VALIDATION_EXECUTION_TIMESTAMP_UTC = (
    datetime.now(
        timezone.utc
    )
)

VALIDATION_EXECUTION_DATE = (
    VALIDATION_EXECUTION_TIMESTAMP_UTC.date()
)


print(
    "NB_40 configuration loaded."
)

print(
    "Expected prediction date:",
    EXPECTED_PREDICTION_DATE
)

print(
    "Expected DateKey:",
    EXPECTED_PREDICTION_DATE_KEY
)

print(
    "Validation execution ID:",
    VALIDATION_EXECUTION_ID
)

print(
    "Validation timestamp UTC:",
    VALIDATION_EXECUTION_TIMESTAMP_UTC
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Define GOld and ML Paths

# CELL ********************

# ============================================================
# Gold ML and reference objects
# ============================================================

# ------------------------------------------------------------
# Physical Gold ML tables
# ------------------------------------------------------------

ML_SUPPLIER_RISK_TABLE = (
    "ml_supplier_risk_prediction"
)

ML_PRICING_ANOMALY_TABLE = (
    "ml_pricing_anomaly_prediction"
)

ML_SAVINGS_OPPORTUNITY_TABLE = (
    "ml_savings_opportunity"
)


# ------------------------------------------------------------
# DB_07 promotion monitoring
# ------------------------------------------------------------

ML_PROMOTION_MONITORING_TABLE = (
    "monitoring_ml_gold_promotion_results"
)


# ------------------------------------------------------------
# Gold reference model
# ------------------------------------------------------------

DIM_SUPPLIER_TABLE = (
    "dim_supplier"
)

DIM_CATEGORY_TABLE = (
    "dim_category"
)

DIM_DATE_TABLE = (
    "dim_date"
)

FACT_PURCHASE_ORDER_TABLE = (
    "fact_purchase_order"
)


# ------------------------------------------------------------
# Direct Databricks working outputs
#
# These live under the Files area of the same Gold Lakehouse.
# Used only for final source-to-Gold reconciliation.
# ------------------------------------------------------------

SUPPLIER_RISK_SOURCE_PATH = (
    "Files/ml/supplier_risk/"
    "predictions_2026"
)

PRICING_ANOMALY_SOURCE_PATH = (
    "Files/ml/pricing_anomaly/"
    "scoring_predictions"
)

SAVINGS_OPPORTUNITY_SOURCE_PATH = (
    "Files/ml/savings_opportunity/"
    "opportunities_2026"
)


# ------------------------------------------------------------
# NB_40 monitoring output
# ------------------------------------------------------------

VALIDATION_MONITORING_TABLE = (
    "monitoring_ml_gold_validation_results"
)


print(
    "NB_40 Fabric objects configured."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read ML Gold tables and references**

# CELL ********************

# ============================================================
# Read ML Gold products
# ============================================================

ml_supplier_risk_df = (
    spark.table(
        ML_SUPPLIER_RISK_TABLE
    )
)


ml_pricing_anomaly_df = (
    spark.table(
        ML_PRICING_ANOMALY_TABLE
    )
)


ml_savings_opportunity_df = (
    spark.table(
        ML_SAVINGS_OPPORTUNITY_TABLE
    )
)


promotion_monitoring_df = (
    spark.table(
        ML_PROMOTION_MONITORING_TABLE
    )
)


# ============================================================
# Gold reference tables
# ============================================================

dim_supplier_df = (
    spark.table(
        DIM_SUPPLIER_TABLE
    )
)


dim_category_df = (
    spark.table(
        DIM_CATEGORY_TABLE
    )
)


dim_date_df = (
    spark.table(
        DIM_DATE_TABLE
    )
)


fact_po_df = (
    spark.table(
        FACT_PURCHASE_ORDER_TABLE
    )
)


# ============================================================
# Direct Databricks working outputs stored under Gold Files
# ============================================================

supplier_risk_source_df = (
    spark.read
    .format(
        "delta"
    )
    .load(
        SUPPLIER_RISK_SOURCE_PATH
    )
)


pricing_anomaly_source_df = (
    spark.read
    .format(
        "delta"
    )
    .load(
        PRICING_ANOMALY_SOURCE_PATH
    )
)


savings_opportunity_source_df = (
    spark.read
    .format(
        "delta"
    )
    .load(
        SAVINGS_OPPORTUNITY_SOURCE_PATH
    )
)


# ============================================================
# Population diagnostics
# ============================================================

ml_supplier_risk_count = (
    ml_supplier_risk_df.count()
)

ml_pricing_anomaly_count = (
    ml_pricing_anomaly_df.count()
)

ml_savings_opportunity_count = (
    ml_savings_opportunity_df.count()
)

promotion_monitoring_count = (
    promotion_monitoring_df.count()
)


print(
    "Gold Supplier Risk rows:",
    f"{ml_supplier_risk_count:,}"
)

print(
    "Gold Pricing Anomaly rows:",
    f"{ml_pricing_anomaly_count:,}"
)

print(
    "Gold Savings Opportunity rows:",
    f"{ml_savings_opportunity_count:,}"
)

print(
    "DB_07 promotion monitoring rows:",
    f"{promotion_monitoring_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation framework**

# CELL ********************

# ============================================================
# Validation framework
# ============================================================

validation_results = []


def add_validation(
    table_name,
    category,
    rule,
    failed_record_count,
    details
):

    failed_record_count = int(
        failed_record_count
    )


    validation_results.append({
        "ValidationLayer":
            "ML Gold",

        "TableName":
            table_name,

        "ValidationCategory":
            category,

        "ValidationRule":
            rule,

        "ValidationStatus":
            (
                "PASS"
                if failed_record_count == 0
                else "FAIL"
            ),

        "FailedRecordCount":
            failed_record_count,

        "ValidationDetails":
            str(
                details
            )
    })


def require_columns(
    dataframe,
    required_columns,
    dataframe_name
):

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]


    if missing_columns:

        raise ValueError(
            f"{dataframe_name} is missing required columns: "
            +
            ", ".join(
                missing_columns
            )
        )


    print(
        f"{dataframe_name} schema contract PASSED."
    )


print(
    "Validation framework loaded."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate schemas**

# CELL ********************

# ============================================================
# Validate ML Gold schema contracts
# ============================================================

require_columns(
    ml_supplier_risk_df,
    [
        "SupplierRiskPredictionKey",
        "PredictionDate",
        "PredictionDateKey",
        "SupplierKey",
        "SupplierDimensionVersion",
        "SupplierID",
        "SupplierRiskScore",
        "PredictedHighRiskFlag",
        "ModelName",
        "ModelRunID",
        "PromotionBatchID",
        "MLSourceNotebook",
        "GoldMLLoadTimestampUTC",
        "GoldMLLoadDate"
    ],
    "ml_supplier_risk_prediction"
)


require_columns(
    ml_pricing_anomaly_df,
    [
        "PricingAnomalyPredictionKey",
        "PredictionDate",
        "PredictionDateKey",
        "PurchaseOrderFactKey",
        "POItemID",
        "POID",
        "SupplierKey",
        "MaterialKey",
        "CategoryKey",
        "PricingAnomalyScore",
        "PricingAnomalyFlag",
        "RawAnomalyScore",
        "RawAnomalyThreshold",
        "ModelName",
        "ModelRunID",
        "PromotionBatchID",
        "MLSourceNotebook",
        "GoldMLLoadTimestampUTC",
        "GoldMLLoadDate"
    ],
    "ml_pricing_anomaly_prediction"
)


require_columns(
    ml_savings_opportunity_df,
    [
        "SavingsOpportunityKey",
        "PredictionDate",
        "PredictionDateKey",
        "SupplierKey",
        "SupplierDimensionVersion",
        "CategoryKey",
        "SupplierID",
        "CategoryID",
        "AnnualizedEligibleSpendEUR",
        "AnnualizedPricingOpportunityEUR",
        "AnnualizedMaverickOpportunityEUR",
        "PotentialAnnualSavingsEUR",
        "PotentialSavingsPct",
        "SupplierRiskScore",
        "PredictedHighRiskFlag",
        "NegotiationPriorityScore",
        "NegotiationPriority",
        "SavingsOpportunityRank",
        "ActionableOpportunityFlag",
        "PricingModelRunID",
        "SupplierRiskModelRunID",
        "EngineRunID",
        "PromotionBatchID",
        "MLSourceNotebook",
        "GoldMLLoadTimestampUTC",
        "GoldMLLoadDate"
    ],
    "ml_savings_opportunity"
)


require_columns(
    promotion_monitoring_df,
    [
        "TableName",
        "SourceRowCount",
        "PersistedRowCount",
        "DuplicateRecordCount",
        "OrphanRecordCount",
        "InvalidMetricCount",
        "ValidationStatus",
        "PredictionDate",
        "PromotionBatchID",
        "ExecutionTimestampUTC"
    ],
    "monitoring_ml_gold_promotion_results"
)


print(
    "\nAll ML Gold schema contracts PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve and validate current prediction snapshots**

# CELL ********************

# ============================================================
# Resolve current ML prediction snapshots
# ============================================================

risk_latest_date = (
    ml_supplier_risk_df

    .agg(
        F.max(
            "PredictionDate"
        )
        .alias(
            "PredictionDate"
        )
    )

    .first()[
        "PredictionDate"
    ]
)


pricing_latest_date = (
    ml_pricing_anomaly_df

    .agg(
        F.max(
            "PredictionDate"
        )
        .alias(
            "PredictionDate"
        )
    )

    .first()[
        "PredictionDate"
    ]
)


savings_latest_date = (
    ml_savings_opportunity_df

    .agg(
        F.max(
            "PredictionDate"
        )
        .alias(
            "PredictionDate"
        )
    )

    .first()[
        "PredictionDate"
    ]
)


# ------------------------------------------------------------
# Validate latest dates
# ------------------------------------------------------------

add_validation(
    "ml_supplier_risk_prediction",
    "Snapshot",
    "Latest prediction date equals expected snapshot",
    (
        0
        if risk_latest_date
        ==
        EXPECTED_PREDICTION_DATE
        else 1
    ),
    f"Latest PredictionDate: {risk_latest_date}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Snapshot",
    "Latest prediction date equals expected snapshot",
    (
        0
        if pricing_latest_date
        ==
        EXPECTED_PREDICTION_DATE
        else 1
    ),
    f"Latest PredictionDate: {pricing_latest_date}"
)


add_validation(
    "ml_savings_opportunity",
    "Snapshot",
    "Latest prediction date equals expected snapshot",
    (
        0
        if savings_latest_date
        ==
        EXPECTED_PREDICTION_DATE
        else 1
    ),
    f"Latest PredictionDate: {savings_latest_date}"
)


# ------------------------------------------------------------
# Current snapshot DataFrames
# ------------------------------------------------------------

risk_snapshot_df = (
    ml_supplier_risk_df

    .filter(
        F.col(
            "PredictionDate"
        )
        ==
        F.lit(
            EXPECTED_PREDICTION_DATE
        )
    )
)


pricing_snapshot_df = (
    ml_pricing_anomaly_df

    .filter(
        F.col(
            "PredictionDate"
        )
        ==
        F.lit(
            EXPECTED_PREDICTION_DATE
        )
    )
)


savings_snapshot_df = (
    ml_savings_opportunity_df

    .filter(
        F.col(
            "PredictionDate"
        )
        ==
        F.lit(
            EXPECTED_PREDICTION_DATE
        )
    )
)


risk_snapshot_count = (
    risk_snapshot_df.count()
)

pricing_snapshot_count = (
    pricing_snapshot_df.count()
)

savings_snapshot_count = (
    savings_snapshot_df.count()
)


add_validation(
    "ml_supplier_risk_prediction",
    "Population",
    "Current prediction snapshot contains rows",
    int(
        risk_snapshot_count == 0
    ),
    f"Rows: {risk_snapshot_count:,}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Population",
    "Current prediction snapshot contains rows",
    int(
        pricing_snapshot_count == 0
    ),
    f"Rows: {pricing_snapshot_count:,}"
)


add_validation(
    "ml_savings_opportunity",
    "Population",
    "Current prediction snapshot contains rows",
    int(
        savings_snapshot_count == 0
    ),
    f"Rows: {savings_snapshot_count:,}"
)


print(
    "Supplier Risk snapshot:",
    f"{risk_snapshot_count:,}"
)

print(
    "Pricing Anomaly snapshot:",
    f"{pricing_snapshot_count:,}"
)

print(
    "Savings Opportunity snapshot:",
    f"{savings_snapshot_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Validate latest DB_07 promotion batch

# CELL ********************

# ============================================================
# Validate latest DB_07 promotion batch
# ============================================================

latest_promotion_timestamp = (
    promotion_monitoring_df

    .agg(
        F.max(
            "ExecutionTimestampUTC"
        )
        .alias(
            "ExecutionTimestampUTC"
        )
    )

    .first()[
        "ExecutionTimestampUTC"
    ]
)


latest_batch_row = (
    promotion_monitoring_df

    .filter(
        F.col(
            "ExecutionTimestampUTC"
        )
        ==
        F.lit(
            latest_promotion_timestamp
        )
    )

    .select(
        "PromotionBatchID"
    )

    .first()
)


LATEST_PROMOTION_BATCH_ID = (
    latest_batch_row[
        "PromotionBatchID"
    ]
)


latest_promotion_df = (
    promotion_monitoring_df

    .filter(
        F.col(
            "PromotionBatchID"
        )
        ==
        F.lit(
            LATEST_PROMOTION_BATCH_ID
        )
    )
)


latest_promotion_count = (
    latest_promotion_df.count()
)


latest_distinct_table_count = (
    latest_promotion_df

    .select(
        "TableName"
    )

    .distinct()

    .count()
)


latest_failed_status_count = (
    latest_promotion_df

    .filter(
        F.col(
            "ValidationStatus"
        )
        !=
        "PASS"
    )

    .count()
)


latest_count_mismatch_count = (
    latest_promotion_df

    .filter(
        F.col(
            "SourceRowCount"
        )
        !=
        F.col(
            "PersistedRowCount"
        )
    )

    .count()
)


latest_invalid_metric_count = (
    latest_promotion_df

    .filter(
        (
            F.col(
                "DuplicateRecordCount"
            )
            > 0
        )
        |
        (
            F.col(
                "OrphanRecordCount"
            )
            > 0
        )
        |
        (
            F.col(
                "InvalidMetricCount"
            )
            > 0
        )
    )

    .count()
)


latest_prediction_date_mismatch = (
    latest_promotion_df

    .filter(
        F.col(
            "PredictionDate"
        )
        !=
        F.lit(
            EXPECTED_PREDICTION_DATE
        )
    )

    .count()
)


latest_promotion_age_days = (
    latest_promotion_df

    .select(
        F.datediff(
            F.current_date(),

            F.to_date(
                "ExecutionTimestampUTC"
            )
        )
        .alias(
            "PromotionAgeDays"
        )
    )

    .agg(
        F.max(
            "PromotionAgeDays"
        )
        .alias(
            "PromotionAgeDays"
        )
    )

    .first()[
        "PromotionAgeDays"
    ]
)


add_validation(
    "monitoring_ml_gold_promotion_results",
    "Promotion",
    "Latest DB_07 batch contains exactly three ML products",
    abs(
        latest_distinct_table_count
        -
        3
    ),
    (
        f"PromotionBatchID: {LATEST_PROMOTION_BATCH_ID}; "
        f"Tables: {latest_distinct_table_count}"
    )
)


add_validation(
    "monitoring_ml_gold_promotion_results",
    "Promotion",
    "Latest DB_07 validations all passed",
    latest_failed_status_count,
    f"Failed monitoring rows: {latest_failed_status_count}"
)


add_validation(
    "monitoring_ml_gold_promotion_results",
    "Reconciliation",
    "DB_07 source and persisted row counts reconcile",
    latest_count_mismatch_count,
    f"Mismatching tables: {latest_count_mismatch_count}"
)


add_validation(
    "monitoring_ml_gold_promotion_results",
    "Quality",
    "DB_07 reports no duplicate, orphan, or invalid records",
    latest_invalid_metric_count,
    f"Invalid monitoring rows: {latest_invalid_metric_count}"
)


add_validation(
    "monitoring_ml_gold_promotion_results",
    "Snapshot",
    "DB_07 promotion date matches expected snapshot",
    latest_prediction_date_mismatch,
    (
        f"Expected: {EXPECTED_PREDICTION_DATE}"
    )
)


add_validation(
    "monitoring_ml_gold_promotion_results",
    "Freshness",
    f"Latest DB_07 promotion is within {MAX_PROMOTION_AGE_DAYS} days",
    (
        0
        if (
            latest_promotion_age_days
            is not None
            and
            latest_promotion_age_days
            <=
            MAX_PROMOTION_AGE_DAYS
        )
        else 1
    ),
    (
        f"Latest promotion timestamp: "
        f"{latest_promotion_timestamp}; "
        f"Age: {latest_promotion_age_days} day(s)"
    )
)


print(
    "Latest Promotion Batch:",
    LATEST_PROMOTION_BATCH_ID
)

print(
    "Promotion timestamp:",
    latest_promotion_timestamp
)

print(
    "Promotion age:",
    latest_promotion_age_days,
    "day(s)"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier Risk product**

# CELL ********************

# ============================================================
# Validate Supplier Risk prediction product
# ============================================================

risk_duplicate_grain_count = (
    risk_snapshot_df

    .groupBy(
        "SupplierID",
        "PredictionDate"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


risk_duplicate_key_count = (
    risk_snapshot_df

    .groupBy(
        "SupplierRiskPredictionKey"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


risk_missing_business_key_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "SupplierID"
        ).isNull()
        |
        F.col(
            "SupplierKey"
        ).isNull()
        |
        F.col(
            "SupplierRiskPredictionKey"
        ).isNull()
    )

    .count()
)


risk_invalid_score_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "SupplierRiskScore"
        ).isNull()
        |
        (
            F.col(
                "SupplierRiskScore"
            )
            < 0
        )
        |
        (
            F.col(
                "SupplierRiskScore"
            )
            > 100
        )
    )

    .count()
)


risk_invalid_flag_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "PredictedHighRiskFlag"
        ).isNull()
        |
        (
            ~F.col(
                "PredictedHighRiskFlag"
            )
            .isin(
                0,
                1
            )
        )
    )

    .count()
)


risk_prediction_date_key_mismatch_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "PredictionDateKey"
        )
        !=
        F.date_format(
            F.col(
                "PredictionDate"
            ),
            "yyyyMMdd"
        )
        .cast(
            "int"
        )
    )

    .count()
)


risk_lineage_missing_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "ModelName"
        ).isNull()
        |
        F.col(
            "ModelRunID"
        ).isNull()
        |
        F.col(
            "PromotionBatchID"
        ).isNull()
        |
        F.col(
            "GoldMLLoadTimestampUTC"
        ).isNull()
    )

    .count()
)


high_risk_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "PredictedHighRiskFlag"
        )
        == 1
    )

    .count()
)


risk_degenerate_count = (
    1
    if (
        high_risk_count == 0
        or
        high_risk_count
        ==
        risk_snapshot_count
    )
    else 0
)


add_validation(
    "ml_supplier_risk_prediction",
    "Grain",
    "SupplierID × PredictionDate is unique",
    risk_duplicate_grain_count,
    f"Duplicate grains: {risk_duplicate_grain_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Key",
    "SupplierRiskPredictionKey is unique",
    risk_duplicate_key_count,
    f"Duplicate keys: {risk_duplicate_key_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Completeness",
    "Supplier business and Gold keys are complete",
    risk_missing_business_key_count,
    f"Missing-key rows: {risk_missing_business_key_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Metric",
    "SupplierRiskScore is between 0 and 100",
    risk_invalid_score_count,
    f"Invalid score rows: {risk_invalid_score_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Classification",
    "PredictedHighRiskFlag is binary",
    risk_invalid_flag_count,
    f"Invalid flags: {risk_invalid_flag_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Classification",
    "Supplier Risk classification is non-degenerate",
    risk_degenerate_count,
    (
        f"High-risk suppliers: {high_risk_count:,} / "
        f"{risk_snapshot_count:,}"
    )
)


add_validation(
    "ml_supplier_risk_prediction",
    "Date",
    "PredictionDateKey matches PredictionDate",
    risk_prediction_date_key_mismatch_count,
    (
        f"DateKey mismatches: "
        f"{risk_prediction_date_key_mismatch_count}"
    )
)


add_validation(
    "ml_supplier_risk_prediction",
    "Lineage",
    "Model and promotion lineage are complete",
    risk_lineage_missing_count,
    f"Missing-lineage rows: {risk_lineage_missing_count}"
)


print(
    "Supplier Risk validation completed."
)

print(
    "High-risk suppliers:",
    f"{high_risk_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Pricing Anomaly product**

# CELL ********************

# ============================================================
# Validate Pricing Anomaly prediction product
# ============================================================

pricing_duplicate_grain_count = (
    pricing_snapshot_df

    .groupBy(
        "POItemID",
        "PredictionDate"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


pricing_duplicate_key_count = (
    pricing_snapshot_df

    .groupBy(
        "PricingAnomalyPredictionKey"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


pricing_missing_key_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "POItemID"
        ).isNull()
        |
        F.col(
            "PurchaseOrderFactKey"
        ).isNull()
        |
        F.col(
            "SupplierKey"
        ).isNull()
        |
        F.col(
            "MaterialKey"
        ).isNull()
        |
        F.col(
            "CategoryKey"
        ).isNull()
        |
        F.col(
            "PricingAnomalyPredictionKey"
        ).isNull()
    )

    .count()
)


pricing_invalid_score_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "PricingAnomalyScore"
        ).isNull()
        |
        (
            F.col(
                "PricingAnomalyScore"
            )
            < 0
        )
        |
        (
            F.col(
                "PricingAnomalyScore"
            )
            > 100
        )
    )

    .count()
)


pricing_invalid_flag_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "PricingAnomalyFlag"
        ).isNull()
        |
        (
            ~F.col(
                "PricingAnomalyFlag"
            )
            .isin(
                0,
                1
            )
        )
    )

    .count()
)


# ------------------------------------------------------------
# Validate custom business threshold used by DB_05
# ------------------------------------------------------------

pricing_threshold_mismatch_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "RawAnomalyScore"
        ).isNull()
        |
        F.col(
            "RawAnomalyThreshold"
        ).isNull()
        |
        (
            (
                F.col(
                    "PricingAnomalyFlag"
                )
                == 1
            )
            &
            (
                F.col(
                    "RawAnomalyScore"
                )
                <
                F.col(
                    "RawAnomalyThreshold"
                )
            )
        )
        |
        (
            (
                F.col(
                    "PricingAnomalyFlag"
                )
                == 0
            )
            &
            (
                F.col(
                    "RawAnomalyScore"
                )
                >=
                F.col(
                    "RawAnomalyThreshold"
                )
            )
        )
    )

    .count()
)


pricing_prediction_date_key_mismatch_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "PredictionDateKey"
        )
        !=
        F.date_format(
            F.col(
                "PredictionDate"
            ),
            "yyyyMMdd"
        )
        .cast(
            "int"
        )
    )

    .count()
)


pricing_lineage_missing_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "ModelName"
        ).isNull()
        |
        F.col(
            "ModelRunID"
        ).isNull()
        |
        F.col(
            "PromotionBatchID"
        ).isNull()
        |
        F.col(
            "GoldMLLoadTimestampUTC"
        ).isNull()
    )

    .count()
)


pricing_anomaly_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "PricingAnomalyFlag"
        )
        == 1
    )

    .count()
)


pricing_anomaly_rate = (
    pricing_anomaly_count
    /
    pricing_snapshot_count
    if pricing_snapshot_count > 0
    else 0.0
)


pricing_degenerate_count = (
    1
    if (
        pricing_anomaly_rate
        <
        0.005
        or
        pricing_anomaly_rate
        >
        0.20
    )
    else 0
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Grain",
    "POItemID × PredictionDate is unique",
    pricing_duplicate_grain_count,
    f"Duplicate grains: {pricing_duplicate_grain_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Key",
    "PricingAnomalyPredictionKey is unique",
    pricing_duplicate_key_count,
    f"Duplicate keys: {pricing_duplicate_key_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Completeness",
    "PO and dimensional keys are complete",
    pricing_missing_key_count,
    f"Missing-key rows: {pricing_missing_key_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Metric",
    "PricingAnomalyScore is between 0 and 100",
    pricing_invalid_score_count,
    f"Invalid score rows: {pricing_invalid_score_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Classification",
    "PricingAnomalyFlag is binary",
    pricing_invalid_flag_count,
    f"Invalid flags: {pricing_invalid_flag_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Classification",
    "PricingAnomalyFlag matches RawAnomalyThreshold",
    pricing_threshold_mismatch_count,
    (
        f"Threshold mismatches: "
        f"{pricing_threshold_mismatch_count}"
    )
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Classification",
    "Pricing anomaly rate is non-degenerate",
    pricing_degenerate_count,
    (
        f"Anomalies: {pricing_anomaly_count:,} / "
        f"{pricing_snapshot_count:,} "
        f"({pricing_anomaly_rate:.2%})"
    )
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Date",
    "PredictionDateKey matches PredictionDate",
    pricing_prediction_date_key_mismatch_count,
    (
        f"DateKey mismatches: "
        f"{pricing_prediction_date_key_mismatch_count}"
    )
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Lineage",
    "Model and promotion lineage are complete",
    pricing_lineage_missing_count,
    f"Missing-lineage rows: {pricing_lineage_missing_count}"
)


print(
    "Pricing Anomaly validation completed."
)

print(
    "Pricing anomalies:",
    f"{pricing_anomaly_count:,}"
)

print(
    "Pricing anomaly rate:",
    f"{pricing_anomaly_rate:.2%}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Savings Opportunity product**

# CELL ********************

# ============================================================
# Validate Savings Opportunity product
# ============================================================

savings_duplicate_grain_count = (
    savings_snapshot_df

    .groupBy(
        "SupplierID",
        "CategoryID",
        "PredictionDate"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


savings_duplicate_key_count = (
    savings_snapshot_df

    .groupBy(
        "SavingsOpportunityKey"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


savings_missing_key_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "SupplierID"
        ).isNull()
        |
        F.col(
            "CategoryID"
        ).isNull()
        |
        F.col(
            "SupplierKey"
        ).isNull()
        |
        F.col(
            "CategoryKey"
        ).isNull()
        |
        F.col(
            "SavingsOpportunityKey"
        ).isNull()
    )

    .count()
)


savings_negative_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "PotentialAnnualSavingsEUR"
        )
        < 0
    )

    .count()
)


savings_exceeds_spend_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "PotentialAnnualSavingsEUR"
        )
        >
        F.col(
            "AnnualizedEligibleSpendEUR"
        )
    )

    .count()
)


savings_component_mismatch_count = (
    savings_snapshot_df

    .filter(
        F.abs(
            F.col(
                "PotentialAnnualSavingsEUR"
            )
            -
            (
                F.col(
                    "AnnualizedPricingOpportunityEUR"
                )
                +
                F.col(
                    "AnnualizedMaverickOpportunityEUR"
                )
            )
        )
        >
        MONETARY_TOLERANCE_EUR
    )

    .count()
)


savings_invalid_pct_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "PotentialSavingsPct"
        ).isNull()
        |
        (
            F.col(
                "PotentialSavingsPct"
            )
            < 0
        )
        |
        (
            F.col(
                "PotentialSavingsPct"
            )
            > 40
        )
    )

    .count()
)


savings_invalid_priority_score_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "NegotiationPriorityScore"
        ).isNull()
        |
        (
            F.col(
                "NegotiationPriorityScore"
            )
            < 0
        )
        |
        (
            F.col(
                "NegotiationPriorityScore"
            )
            > 100
        )
    )

    .count()
)


savings_invalid_priority_band_count = (
    savings_snapshot_df

    .filter(
        ~F.col(
            "NegotiationPriority"
        )
        .isin(
            "CRITICAL",
            "HIGH",
            "MEDIUM",
            "LOW",
            "NONE"
        )
    )

    .count()
)


savings_invalid_rank_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "SavingsOpportunityRank"
        ).isNull()
        |
        (
            F.col(
                "SavingsOpportunityRank"
            )
            <= 0
        )
    )

    .count()
)


savings_duplicate_rank_count = (
    savings_snapshot_df

    .groupBy(
        "SavingsOpportunityRank"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


savings_prediction_date_key_mismatch_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "PredictionDateKey"
        )
        !=
        F.date_format(
            F.col(
                "PredictionDate"
            ),
            "yyyyMMdd"
        )
        .cast(
            "int"
        )
    )

    .count()
)


savings_lineage_missing_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "EngineRunID"
        ).isNull()
        |
        F.col(
            "PricingModelRunID"
        ).isNull()
        |
        F.col(
            "SupplierRiskModelRunID"
        ).isNull()
        |
        F.col(
            "PromotionBatchID"
        ).isNull()
        |
        F.col(
            "GoldMLLoadTimestampUTC"
        ).isNull()
    )

    .count()
)


positive_savings_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "PotentialAnnualSavingsEUR"
        )
        > 0
    )

    .count()
)


actionable_savings_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "ActionableOpportunityFlag"
        )
        == 1
    )

    .count()
)


total_potential_savings_eur = (
    savings_snapshot_df

    .agg(
        F.sum(
            "PotentialAnnualSavingsEUR"
        )
        .alias(
            "PotentialAnnualSavingsEUR"
        )
    )

    .first()[
        "PotentialAnnualSavingsEUR"
    ]
)


add_validation(
    "ml_savings_opportunity",
    "Grain",
    "SupplierID × CategoryID × PredictionDate is unique",
    savings_duplicate_grain_count,
    f"Duplicate grains: {savings_duplicate_grain_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Key",
    "SavingsOpportunityKey is unique",
    savings_duplicate_key_count,
    f"Duplicate keys: {savings_duplicate_key_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Completeness",
    "Supplier and Category Gold keys are complete",
    savings_missing_key_count,
    f"Missing-key rows: {savings_missing_key_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Metric",
    "Potential Annual Savings is non-negative",
    savings_negative_count,
    f"Negative savings rows: {savings_negative_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Metric",
    "Potential savings does not exceed annualized spend",
    savings_exceeds_spend_count,
    f"Invalid rows: {savings_exceeds_spend_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Reconciliation",
    "Potential savings reconciles to pricing plus maverick components",
    savings_component_mismatch_count,
    f"Mismatch rows: {savings_component_mismatch_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Metric",
    "PotentialSavingsPct is between 0 and 40",
    savings_invalid_pct_count,
    f"Invalid percentage rows: {savings_invalid_pct_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Priority",
    "NegotiationPriorityScore is between 0 and 100",
    savings_invalid_priority_score_count,
    f"Invalid priority scores: {savings_invalid_priority_score_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Priority",
    "NegotiationPriority contains approved bands",
    savings_invalid_priority_band_count,
    f"Invalid priority bands: {savings_invalid_priority_band_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Rank",
    "SavingsOpportunityRank is positive",
    savings_invalid_rank_count,
    f"Invalid ranks: {savings_invalid_rank_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Rank",
    "SavingsOpportunityRank is unique",
    savings_duplicate_rank_count,
    f"Duplicate ranks: {savings_duplicate_rank_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Date",
    "PredictionDateKey matches PredictionDate",
    savings_prediction_date_key_mismatch_count,
    (
        f"DateKey mismatches: "
        f"{savings_prediction_date_key_mismatch_count}"
    )
)


add_validation(
    "ml_savings_opportunity",
    "Lineage",
    "Engine, model and promotion lineage are complete",
    savings_lineage_missing_count,
    f"Missing-lineage rows: {savings_lineage_missing_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Population",
    "Positive and actionable opportunities exist",
    (
        0
        if (
            positive_savings_count > 0
            and
            actionable_savings_count > 0
        )
        else 1
    ),
    (
        f"Positive: {positive_savings_count:,}; "
        f"Actionable: {actionable_savings_count:,}"
    )
)


print(
    "Savings Opportunity validation completed."
)

print(
    "Positive opportunities:",
    f"{positive_savings_count:,}"
)

print(
    "Actionable opportunities:",
    f"{actionable_savings_count:,}"
)

print(
    "Potential Annual Savings EUR:",
    f"{total_potential_savings_eur:,.2f}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Gold referential integrity and Supplier SCD2**

# CELL ********************

# ============================================================
# Validate Gold referential integrity
# ============================================================

prediction_date_literal = (
    F.lit(
        EXPECTED_PREDICTION_DATE
    )
    .cast(
        "date"
    )
)


# ------------------------------------------------------------
# Supplier SCD2 version valid on prediction date
# ------------------------------------------------------------

supplier_as_of_df = (
    dim_supplier_df

    .filter(
        (
            F.to_date(
                "EffectiveFromDate"
            )
            <=
            prediction_date_literal
        )
        &
        (
            F.coalesce(
                F.to_date(
                    "EffectiveToDate"
                ),

                F.lit(
                    "9999-12-31"
                )
                .cast(
                    "date"
                )
            )
            >=
            prediction_date_literal
        )
    )

    .select(
        "SupplierID",
        "SupplierKey",

        F.col(
            "DimensionVersion"
        )
        .cast(
            "int"
        )
        .alias(
            "ExpectedSupplierDimensionVersion"
        )
    )
)


supplier_as_of_duplicate_count = (
    supplier_as_of_df

    .groupBy(
        "SupplierID"
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )

    .count()
)


# ------------------------------------------------------------
# Supplier Risk -> dim_supplier
# ------------------------------------------------------------

risk_supplier_scd_mismatch_count = (
    risk_snapshot_df.alias("ml")

    .join(
        supplier_as_of_df.alias("ds"),

        on="SupplierID",

        how="left"
    )

    .filter(
        F.col(
            "ds.SupplierKey"
        ).isNull()
        |
        (
            F.col(
                "ml.SupplierKey"
            )
            !=
            F.col(
                "ds.SupplierKey"
            )
        )
        |
        (
            F.col(
                "ml.SupplierDimensionVersion"
            )
            !=
            F.col(
                "ds.ExpectedSupplierDimensionVersion"
            )
        )
    )

    .count()
)


# ------------------------------------------------------------
# Savings -> dim_supplier
# ------------------------------------------------------------

savings_supplier_scd_mismatch_count = (
    savings_snapshot_df.alias("ml")

    .join(
        supplier_as_of_df.alias("ds"),

        on="SupplierID",

        how="left"
    )

    .filter(
        F.col(
            "ds.SupplierKey"
        ).isNull()
        |
        (
            F.col(
                "ml.SupplierKey"
            )
            !=
            F.col(
                "ds.SupplierKey"
            )
        )
        |
        (
            F.col(
                "ml.SupplierDimensionVersion"
            )
            !=
            F.col(
                "ds.ExpectedSupplierDimensionVersion"
            )
        )
    )

    .count()
)


# ------------------------------------------------------------
# Savings -> dim_category
# ------------------------------------------------------------

category_map_df = (
    dim_category_df

    .select(
        "CategoryID",
        "CategoryKey"
    )
)


savings_category_mismatch_count = (
    savings_snapshot_df.alias("ml")

    .join(
        category_map_df.alias("dc"),

        on="CategoryID",

        how="left"
    )

    .filter(
        F.col(
            "dc.CategoryKey"
        ).isNull()
        |
        (
            F.col(
                "ml.CategoryKey"
            )
            !=
            F.col(
                "dc.CategoryKey"
            )
        )
    )

    .count()
)


# ------------------------------------------------------------
# Pricing anomaly -> fact_purchase_order
# ------------------------------------------------------------

pricing_fact_mismatch_count = (
    pricing_snapshot_df.alias("ml")

    .join(
        fact_po_df.alias("po"),

        F.col(
            "ml.POItemID"
        )
        ==
        F.col(
            "po.POItemID"
        ),

        "left"
    )

    .filter(
        F.col(
            "po.PurchaseOrderFactKey"
        ).isNull()
        |
        (
            F.col(
                "ml.PurchaseOrderFactKey"
            )
            !=
            F.col(
                "po.PurchaseOrderFactKey"
            )
        )
        |
        (
            F.col(
                "ml.SupplierKey"
            )
            !=
            F.col(
                "po.SupplierKey"
            )
        )
        |
        (
            F.col(
                "ml.MaterialKey"
            )
            !=
            F.col(
                "po.MaterialKey"
            )
        )
        |
        (
            F.col(
                "ml.CategoryKey"
            )
            !=
            F.col(
                "po.CategoryKey"
            )
        )
        |
        (
            ~F.col(
                "ml.ContractKey"
            )
            .eqNullSafe(
                F.col(
                    "po.ContractKey"
                )
            )
        )
    )

    .count()
)


# ------------------------------------------------------------
# Prediction DateKey -> dim_date
# ------------------------------------------------------------

risk_date_orphan_count = (
    risk_snapshot_df

    .select(
        "PredictionDateKey"
    )

    .distinct()

    .join(
        dim_date_df

        .select(
            "DateKey"
        )

        .distinct(),

        F.col(
            "PredictionDateKey"
        )
        ==
        F.col(
            "DateKey"
        ),

        "left_anti"
    )

    .count()
)


pricing_date_orphan_count = (
    pricing_snapshot_df

    .select(
        "PredictionDateKey"
    )

    .distinct()

    .join(
        dim_date_df

        .select(
            "DateKey"
        )

        .distinct(),

        F.col(
            "PredictionDateKey"
        )
        ==
        F.col(
            "DateKey"
        ),

        "left_anti"
    )

    .count()
)


savings_date_orphan_count = (
    savings_snapshot_df

    .select(
        "PredictionDateKey"
    )

    .distinct()

    .join(
        dim_date_df

        .select(
            "DateKey"
        )

        .distinct(),

        F.col(
            "PredictionDateKey"
        )
        ==
        F.col(
            "DateKey"
        ),

        "left_anti"
    )

    .count()
)


add_validation(
    "dim_supplier",
    "SCD2",
    "One Supplier version is valid per SupplierID on prediction date",
    supplier_as_of_duplicate_count,
    f"Duplicate as-of Supplier mappings: {supplier_as_of_duplicate_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Referential Integrity",
    "SupplierKey and SupplierDimensionVersion match dim_supplier as-of snapshot",
    risk_supplier_scd_mismatch_count,
    f"SCD2 mismatches: {risk_supplier_scd_mismatch_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Referential Integrity",
    "SupplierKey and SupplierDimensionVersion match dim_supplier as-of snapshot",
    savings_supplier_scd_mismatch_count,
    f"SCD2 mismatches: {savings_supplier_scd_mismatch_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Referential Integrity",
    "CategoryKey matches dim_category",
    savings_category_mismatch_count,
    f"Category mismatches: {savings_category_mismatch_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Referential Integrity",
    "PO and dimensional keys reconcile to fact_purchase_order",
    pricing_fact_mismatch_count,
    f"Fact mismatches: {pricing_fact_mismatch_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Referential Integrity",
    "PredictionDateKey exists in dim_date",
    risk_date_orphan_count,
    f"Date orphans: {risk_date_orphan_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Referential Integrity",
    "PredictionDateKey exists in dim_date",
    pricing_date_orphan_count,
    f"Date orphans: {pricing_date_orphan_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Referential Integrity",
    "PredictionDateKey exists in dim_date",
    savings_date_orphan_count,
    f"Date orphans: {savings_date_orphan_count}"
)


print(
    "Gold referential-integrity validation completed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate cross-product ML consistency**

# CELL ********************

# ============================================================
# Cross-product ML consistency
# ============================================================

# ------------------------------------------------------------
# Savings opportunity must have Supplier Risk prediction
# ------------------------------------------------------------

savings_risk_reconciliation_df = (
    savings_snapshot_df.alias("so")

    .join(
        risk_snapshot_df

        .select(
            "SupplierID",
            "PredictionDate",
            "SupplierRiskScore",
            "PredictedHighRiskFlag",
            "ModelRunID"
        )

        .alias("sr"),

        on=[
            "SupplierID",
            "PredictionDate"
        ],

        how="left"
    )
)


savings_missing_risk_count = (
    savings_risk_reconciliation_df

    .filter(
        F.col(
            "sr.ModelRunID"
        ).isNull()
    )

    .count()
)


savings_risk_metric_mismatch_count = (
    savings_risk_reconciliation_df

    .filter(
        F.col(
            "sr.ModelRunID"
        ).isNotNull()
        &
        (
            (
                F.abs(
                    F.col(
                        "so.SupplierRiskScore"
                    )
                    -
                    F.col(
                        "sr.SupplierRiskScore"
                    )
                )
                >
                SCORE_TOLERANCE
            )
            |
            (
                F.col(
                    "so.PredictedHighRiskFlag"
                )
                !=
                F.col(
                    "sr.PredictedHighRiskFlag"
                )
            )
            |
            (
                F.col(
                    "so.SupplierRiskModelRunID"
                )
                !=
                F.col(
                    "sr.ModelRunID"
                )
            )
        )
    )

    .count()
)


# ------------------------------------------------------------
# Pricing model lineage consistency
# ------------------------------------------------------------

pricing_model_run_ids = [
    row[
        "ModelRunID"
    ]
    for row in (
        pricing_snapshot_df

        .select(
            "ModelRunID"
        )

        .distinct()

        .collect()
    )
]


pricing_model_run_count = len(
    pricing_model_run_ids
)


if pricing_model_run_count == 1:

    CURRENT_PRICING_MODEL_RUN_ID = (
        pricing_model_run_ids[
            0
        ]
    )

    savings_pricing_run_mismatch_count = (
        savings_snapshot_df

        .filter(
            F.col(
                "PricingModelRunID"
            )
            !=
            F.lit(
                CURRENT_PRICING_MODEL_RUN_ID
            )
        )

        .count()
    )

else:

    CURRENT_PRICING_MODEL_RUN_ID = None

    savings_pricing_run_mismatch_count = (
        savings_snapshot_count
    )


# ------------------------------------------------------------
# Promotion Batch lineage must reconcile across products
# ------------------------------------------------------------

risk_promotion_mismatch_count = (
    risk_snapshot_df

    .filter(
        F.col(
            "PromotionBatchID"
        )
        !=
        F.lit(
            LATEST_PROMOTION_BATCH_ID
        )
    )

    .count()
)


pricing_promotion_mismatch_count = (
    pricing_snapshot_df

    .filter(
        F.col(
            "PromotionBatchID"
        )
        !=
        F.lit(
            LATEST_PROMOTION_BATCH_ID
        )
    )

    .count()
)


savings_promotion_mismatch_count = (
    savings_snapshot_df

    .filter(
        F.col(
            "PromotionBatchID"
        )
        !=
        F.lit(
            LATEST_PROMOTION_BATCH_ID
        )
    )

    .count()
)


add_validation(
    "ml_savings_opportunity",
    "Cross Product",
    "Every Savings supplier has a Supplier Risk prediction",
    savings_missing_risk_count,
    f"Missing Supplier Risk rows: {savings_missing_risk_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Cross Product",
    "Embedded Supplier Risk values reconcile to Supplier Risk product",
    savings_risk_metric_mismatch_count,
    f"Risk mismatches: {savings_risk_metric_mismatch_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Lineage",
    "Current pricing snapshot uses one ModelRunID",
    abs(
        pricing_model_run_count
        -
        1
    ),
    f"Distinct ModelRunIDs: {pricing_model_run_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Cross Product",
    "Savings opportunity references current Pricing ModelRunID",
    savings_pricing_run_mismatch_count,
    f"Pricing ModelRunID mismatches: {savings_pricing_run_mismatch_count}"
)


add_validation(
    "ml_supplier_risk_prediction",
    "Lineage",
    "PromotionBatchID matches latest DB_07 batch",
    risk_promotion_mismatch_count,
    f"Promotion mismatches: {risk_promotion_mismatch_count}"
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Lineage",
    "PromotionBatchID matches latest DB_07 batch",
    pricing_promotion_mismatch_count,
    f"Promotion mismatches: {pricing_promotion_mismatch_count}"
)


add_validation(
    "ml_savings_opportunity",
    "Lineage",
    "PromotionBatchID matches latest DB_07 batch",
    savings_promotion_mismatch_count,
    f"Promotion mismatches: {savings_promotion_mismatch_count}"
)


print(
    "Cross-product ML validation completed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Direct Databricks source-to-Gold reconciliation**

# CELL ********************

# ============================================================
# Direct Databricks source-to-Gold reconciliation
# ============================================================

source_risk_count = (
    supplier_risk_source_df.count()
)

source_pricing_count = (
    pricing_anomaly_source_df.count()
)

source_savings_count = (
    savings_opportunity_source_df.count()
)


source_high_risk_count = (
    supplier_risk_source_df

    .filter(
        F.col(
            "PredictedHighRiskFlag"
        )
        == 1
    )

    .count()
)


source_pricing_anomaly_count = (
    pricing_anomaly_source_df

    .filter(
        F.col(
            "PricingAnomalyFlag"
        )
        == 1
    )

    .count()
)


source_total_savings_eur = (
    savings_opportunity_source_df

    .agg(
        F.sum(
            "PotentialAnnualSavingsEUR"
        )
        .alias(
            "PotentialAnnualSavingsEUR"
        )
    )

    .first()[
        "PotentialAnnualSavingsEUR"
    ]
)


source_gold_savings_difference = (
    abs(
        float(
            source_total_savings_eur
        )
        -
        float(
            total_potential_savings_eur
        )
    )
)


add_validation(
    "ml_supplier_risk_prediction",
    "Source Reconciliation",
    "Gold row count matches DB_03 source output",
    abs(
        source_risk_count
        -
        risk_snapshot_count
    ),
    (
        f"Source: {source_risk_count:,}; "
        f"Gold: {risk_snapshot_count:,}"
    )
)


add_validation(
    "ml_supplier_risk_prediction",
    "Source Reconciliation",
    "High-risk classification matches DB_03 source output",
    abs(
        source_high_risk_count
        -
        high_risk_count
    ),
    (
        f"Source: {source_high_risk_count:,}; "
        f"Gold: {high_risk_count:,}"
    )
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Source Reconciliation",
    "Gold row count matches DB_05 source output",
    abs(
        source_pricing_count
        -
        pricing_snapshot_count
    ),
    (
        f"Source: {source_pricing_count:,}; "
        f"Gold: {pricing_snapshot_count:,}"
    )
)


add_validation(
    "ml_pricing_anomaly_prediction",
    "Source Reconciliation",
    "Pricing anomaly classification matches DB_05 source",
    abs(
        source_pricing_anomaly_count
        -
        pricing_anomaly_count
    ),
    (
        f"Source: {source_pricing_anomaly_count:,}; "
        f"Gold: {pricing_anomaly_count:,}"
    )
)


add_validation(
    "ml_savings_opportunity",
    "Source Reconciliation",
    "Gold row count matches DB_06 source output",
    abs(
        source_savings_count
        -
        savings_snapshot_count
    ),
    (
        f"Source: {source_savings_count:,}; "
        f"Gold: {savings_snapshot_count:,}"
    )
)


add_validation(
    "ml_savings_opportunity",
    "Source Reconciliation",
    "Potential Annual Savings reconciles to DB_06 source",
    (
        0
        if source_gold_savings_difference
        <=
        MONETARY_TOLERANCE_EUR
        else 1
    ),
    (
        f"Source EUR: {source_total_savings_eur:,.2f}; "
        f"Gold EUR: {total_potential_savings_eur:,.2f}; "
        f"Difference EUR: {source_gold_savings_difference:,.2f}"
    )
)


print(
    "Databricks source-to-Gold reconciliation completed."
)

print(
    "Supplier Risk:",
    f"{source_risk_count:,}",
    "->",
    f"{risk_snapshot_count:,}"
)

print(
    "Pricing Anomaly:",
    f"{source_pricing_count:,}",
    "->",
    f"{pricing_snapshot_count:,}"
)

print(
    "Savings Opportunity:",
    f"{source_savings_count:,}",
    "->",
    f"{savings_snapshot_count:,}"
)

print(
    "Savings EUR difference:",
    f"{source_gold_savings_difference:,.2f}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build final validation result dataset**

# CELL ********************

# ============================================================
# Build NB_40 validation result dataset
# ============================================================

validation_schema = T.StructType([

    T.StructField(
        "ValidationLayer",
        T.StringType(),
        False
    ),

    T.StructField(
        "TableName",
        T.StringType(),
        False
    ),

    T.StructField(
        "ValidationCategory",
        T.StringType(),
        False
    ),

    T.StructField(
        "ValidationRule",
        T.StringType(),
        False
    ),

    T.StructField(
        "ValidationStatus",
        T.StringType(),
        False
    ),

    T.StructField(
        "FailedRecordCount",
        T.LongType(),
        False
    ),

    T.StructField(
        "ValidationDetails",
        T.StringType(),
        False
    )
])


validation_rows = [
    (
        result[
            "ValidationLayer"
        ],

        result[
            "TableName"
        ],

        result[
            "ValidationCategory"
        ],

        result[
            "ValidationRule"
        ],

        result[
            "ValidationStatus"
        ],

        int(
            result[
                "FailedRecordCount"
            ]
        ),

        result[
            "ValidationDetails"
        ]
    )

    for result in validation_results
]


ml_gold_validation_results_df = (
    spark.createDataFrame(
        validation_rows,
        validation_schema
    )

    .withColumn(
        "PredictionDate",

        F.lit(
            EXPECTED_PREDICTION_DATE
        )
        .cast(
            "date"
        )
    )

    .withColumn(
        "PromotionBatchID",

        F.lit(
            LATEST_PROMOTION_BATCH_ID
        )
    )

    .withColumn(
        "ValidationExecutionID",

        F.lit(
            VALIDATION_EXECUTION_ID
        )
    )

    .withColumn(
        "ExecutionTimestampUTC",

        F.lit(
            VALIDATION_EXECUTION_TIMESTAMP_UTC
        )
        .cast(
            "timestamp"
        )
    )

    .withColumn(
        "ExecutionDate",

        F.lit(
            VALIDATION_EXECUTION_DATE
        )
        .cast(
            "date"
        )
    )
)


display(
    ml_gold_validation_results_df

    .orderBy(
        F.when(
            F.col(
                "ValidationStatus"
            )
            ==
            "FAIL",
            0
        )
        .otherwise(
            1
        ),

        "TableName",
        "ValidationCategory",
        "ValidationRule"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation summary**

# CELL ********************

# ============================================================
# NB_40 validation summary
# ============================================================

validation_summary_df = (
    ml_gold_validation_results_df

    .groupBy(
        "ValidationStatus"
    )

    .agg(
        F.count("*")
        .alias(
            "ValidationRuleCount"
        ),

        F.sum(
            "FailedRecordCount"
        )
        .alias(
            "FailedRecordCount"
        )
    )

    .orderBy(
        "ValidationStatus"
    )
)


display(
    validation_summary_df
)


failed_validation_count = (
    ml_gold_validation_results_df

    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAIL"
    )

    .count()
)


passed_validation_count = (
    ml_gold_validation_results_df

    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "PASS"
    )

    .count()
)


total_validation_count = (
    ml_gold_validation_results_df.count()
)


print(
    "Total validation rules:",
    total_validation_count
)

print(
    "Passed:",
    passed_validation_count
)

print(
    "Failed:",
    failed_validation_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist NB_40 monitoring results**

# CELL ********************

# ============================================================
# Persist NB_40 ML Gold validation monitoring
# ============================================================

if spark.catalog.tableExists(
    VALIDATION_MONITORING_TABLE
):

    (
        ml_gold_validation_results_df

        .write

        .format(
            "delta"
        )

        .mode(
            "append"
        )

        .option(
            "mergeSchema",
            "true"
        )

        .saveAsTable(
            VALIDATION_MONITORING_TABLE
        )
    )


else:

    (
        ml_gold_validation_results_df

        .write

        .format(
            "delta"
        )

        .mode(
            "overwrite"
        )

        .option(
            "overwriteSchema",
            "true"
        )

        .saveAsTable(
            VALIDATION_MONITORING_TABLE
        )
    )


print(
    "NB_40 validation monitoring persisted:"
)

print(
    VALIDATION_MONITORING_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final ML Gold quality gate**

# CELL ********************

# ============================================================
# FINAL NB_40 ML GOLD QUALITY GATE
# ============================================================

print(
    "============================================================"
)

print(
    "NB_40 FINAL ML GOLD VALIDATION"
)

print(
    "============================================================"
)


print(
    "\nPrediction Date:",
    EXPECTED_PREDICTION_DATE
)

print(
    "Promotion Batch:",
    LATEST_PROMOTION_BATCH_ID
)


print(
    "\nSUPPLIER RISK"
)

print(
    "Rows:",
    f"{risk_snapshot_count:,}"
)

print(
    "High-risk suppliers:",
    f"{high_risk_count:,}"
)


print(
    "\nPRICING ANOMALY"
)

print(
    "Rows:",
    f"{pricing_snapshot_count:,}"
)

print(
    "Pricing anomalies:",
    f"{pricing_anomaly_count:,}"
)

print(
    "Anomaly rate:",
    f"{pricing_anomaly_rate:.2%}"
)


print(
    "\nSAVINGS OPPORTUNITY"
)

print(
    "Rows:",
    f"{savings_snapshot_count:,}"
)

print(
    "Positive opportunities:",
    f"{positive_savings_count:,}"
)

print(
    "Actionable opportunities:",
    f"{actionable_savings_count:,}"
)

print(
    "Potential Annual Savings EUR:",
    f"{total_potential_savings_eur:,.2f}"
)


print(
    "\nVALIDATION"
)

print(
    "Rules:",
    total_validation_count
)

print(
    "Passed:",
    passed_validation_count
)

print(
    "Failed:",
    failed_validation_count
)


if failed_validation_count > 0:

    print(
        "\nFAILED VALIDATION RULES"
    )


    display(
        ml_gold_validation_results_df

        .filter(
            F.col(
                "ValidationStatus"
            )
            ==
            "FAIL"
        )

        .select(
            "TableName",
            "ValidationCategory",
            "ValidationRule",
            "FailedRecordCount",
            "ValidationDetails"
        )
    )


    raise ValueError(
        "NB_40 ML GOLD QUALITY GATE FAILED. "
        f"{failed_validation_count} validation rule(s) failed."
    )


print(
    "\n============================================================"
)

print(
    "NB_40 ML GOLD VALIDATION PASSED."
)

print(
    "ML Gold data products are ready for semantic modeling."
)

print(
    "============================================================"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
