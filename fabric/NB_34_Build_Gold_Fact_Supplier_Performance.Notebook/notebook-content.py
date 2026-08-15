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

# **Pyshical Output:**  _fact_supplier_performance_
# 
# **Grain:**  One row perSupplierID x PerformanceYear
# 
# This is a periodic snapshot fact, not a transaction fact. We are using the end of the performance period to resolve the SCD2 supplier version. For completed years this means December 31st, for 2026 is the current analytical snapshot date, July 31, 2026.

# MARKDOWN ********************

# **Configuration**

# CELL ********************

# ============================================================
# NB_34_Build_Gold_Fact_Supplier_Performance
# Configuration
# ============================================================

from datetime import date


# ============================================================
# Analytical Snapshot
# ============================================================

SNAPSHOT_DATE = date(
    2026,
    7,
    31
)


# ============================================================
# Silver Source
# ============================================================

SILVER_SUPPLIER_PERFORMANCE_TABLE = (
    "silver_supplier_performance"
)

SILVER_SUPPLIER_PERFORMANCE_MONITORING_TABLE = (
    "monitoring_silver_supplier_performance_quality_results"
)


# ============================================================
# Gold Dependencies
# ============================================================

GOLD_DIMENSION_MONITORING_TABLE = (
    "monitoring_gold_dimensions_quality_results"
)

DIM_DATE_TABLE = (
    "dim_date"
)

DIM_SUPPLIER_TABLE = (
    "dim_supplier"
)


# ============================================================
# Gold Output
# ============================================================

FACT_SUPPLIER_PERFORMANCE_TABLE = (
    "fact_supplier_performance"
)

GOLD_MONITORING_TABLE = (
    "monitoring_gold_fact_supplier_performance_quality_results"
)


# ============================================================
# Notebook Information
# ============================================================

print(
    "Notebook: "
    "NB_34_Build_Gold_Fact_Supplier_Performance"
)

print(
    "Default Lakehouse: "
    "lh_procurement_gold"
)

print(
    "Output table: "
    "fact_supplier_performance"
)

print(
    "Grain: "
    "one row per SupplierID x PerformanceYear"
)

print(
    "Snapshot date:",
    SNAPSHOT_DATE
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Imports**

# CELL ********************

import re

from pyspark.sql import functions as F


print(
    "Libraries loaded."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate required tables**

# CELL ********************

required_tables = [
    SILVER_SUPPLIER_PERFORMANCE_TABLE,
    SILVER_SUPPLIER_PERFORMANCE_MONITORING_TABLE,
    GOLD_DIMENSION_MONITORING_TABLE,
    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE
]


missing_tables = [
    table_name
    for table_name in required_tables
    if not spark.catalog.tableExists(
        table_name
    )
]


if missing_tables:

    raise RuntimeError(
        "Missing required Gold tables "
        "or Silver shortcuts: "
        +
        ", ".join(
            missing_tables
        )
    )


print(
    "All required tables exist."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Confirm upstream quality gates**

# CELL ********************

silver_supplier_performance_failure_count = (
    spark.table(
        SILVER_SUPPLIER_PERFORMANCE_MONITORING_TABLE
    )

    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAILED"
    )

    .count()
)


gold_dimension_failure_count = (
    spark.table(
        GOLD_DIMENSION_MONITORING_TABLE
    )

    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAILED"
    )

    .count()
)


print(
    "NB_24 Silver Supplier Performance failures:",
    silver_supplier_performance_failure_count
)

print(
    "NB_30 Gold Dimension failures:",
    gold_dimension_failure_count
)


assert (
    silver_supplier_performance_failure_count
    ==
    0
), (
    "NB_24 Silver Supplier Performance "
    "quality gate has not passed."
)


assert (
    gold_dimension_failure_count
    ==
    0
), (
    "NB_30 Gold Dimensions "
    "quality gate has not passed."
)


print(
    "Required upstream quality gates confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load source and dimensions**

# CELL ********************

silver_supplier_performance_df = (
    spark.table(
        SILVER_SUPPLIER_PERFORMANCE_TABLE
    )
)


dim_supplier_df = (
    spark.table(
        DIM_SUPPLIER_TABLE
    )
)


dim_date_df = (
    spark.table(
        DIM_DATE_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Silver supplier-performance rows:",
    f"{silver_supplier_performance_df.count():,}"
)

print(
    "Gold Supplier dimension rows:",
    f"{dim_supplier_df.count():,}"
)

print(
    "Gold Date dimension rows:",
    f"{dim_date_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect Silver supplier-performance schema**

# CELL ********************

silver_supplier_performance_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Column-resolution helpers**

# CELL ********************

def normalize_column_name(
    column_name
):

    return re.sub(
        r"[^a-z0-9]",
        "",
        column_name.lower()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resolve_column_name(
    dataframe,
    logical_name,
    candidates,
    required=True
):

    normalized_columns = {
        normalize_column_name(
            column_name
        ): column_name

        for column_name
        in dataframe.columns
    }


    for candidate in candidates:

        normalized_candidate = (
            normalize_column_name(
                candidate
            )
        )

        if (
            normalized_candidate
            in normalized_columns
        ):

            return normalized_columns[
                normalized_candidate
            ]


    if required:

        raise RuntimeError(
            f"Required Silver supplier-performance "
            f"column '{logical_name}' was not found. "
            f"Candidates: {candidates}. "
            f"Available columns: "
            f"{dataframe.columns}"
        )


    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resolved_expression(
    source_column,
    alias_name,
    data_type=None
):

    if source_column is None:

        expression = (
            F.lit(None)
        )

    else:

        expression = (
            F.col(
                source_column
            )
        )


    if data_type is not None:

        expression = (
            expression.cast(
                data_type
            )
        )


    return expression.alias(
        alias_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve Silver columns**

# CELL ********************

# ============================================================
# Resolve Silver Supplier Performance Columns
# ============================================================
#
# NB_24 uses operational Silver naming such as:
#
#   DeliveryCompletePOItemCount
#   OnTimeDeliveryPOItemCount
#   SupplierOnTimeDeliveryPct
#
# Gold standardizes those names to:
#
#   FullyReceivedPOItemCount
#   OnTimeFullyReceivedPOItemCount
#   SupplierOTDPct
#
# This keeps the Gold fact terminology consistent while
# preserving the original NB_24 KPI logic.
# ============================================================

SOURCE_COLUMNS = {

    # ========================================================
    # Grain
    # ========================================================

    "SupplierID":
        resolve_column_name(
            silver_supplier_performance_df,
            "SupplierID",
            [
                "SupplierID"
            ]
        ),

    "PerformanceYear":
        resolve_column_name(
            silver_supplier_performance_df,
            "PerformanceYear",
            [
                "PerformanceYear",
                "Year"
            ]
        ),


    # ========================================================
    # Spend / Contract Compliance
    # ========================================================

    "EligibleSpendEUR":
        resolve_column_name(
            silver_supplier_performance_df,
            "EligibleSpendEUR",
            [
                "EligibleSpendEUR",
                "TotalEligibleSpendEUR"
            ],
            required=False
        ),

    "ContractCompliantSpendEUR":
        resolve_column_name(
            silver_supplier_performance_df,
            "ContractCompliantSpendEUR",
            [
                "ContractCompliantSpendEUR",
                "CompliantSpendEUR"
            ],
            required=False
        ),

    "MaverickSpendEUR":
        resolve_column_name(
            silver_supplier_performance_df,
            "MaverickSpendEUR",
            [
                "MaverickSpendEUR"
            ],
            required=False
        ),

    "ContractCompliancePct":
        resolve_column_name(
            silver_supplier_performance_df,
            "ContractCompliancePct",
            [
                "ContractCompliancePct"
            ]
        ),

    "MaverickSpendPct":
        resolve_column_name(
            silver_supplier_performance_df,
            "MaverickSpendPct",
            [
                "MaverickSpendPct"
            ]
        ),


    # ========================================================
    # Delivery / OTD
    # ========================================================
    #
    # Actual NB_24 schema:
    #
    # DeliveryCompletePOItemCount
    # OnTimeDeliveryPOItemCount
    # SupplierOnTimeDeliveryPct
    # OverdueOpenPOItemCount
    #
    # Gold canonical names are intentionally standardized.
    # ========================================================

    "FullyReceivedPOItemCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "FullyReceivedPOItemCount",
            [
                "DeliveryCompletePOItemCount",
                "FullyReceivedPOItemCount",
                "FullyReceivedItemCount",
                "CompletedDeliveryPOItemCount"
            ]
        ),

    "OnTimeFullyReceivedPOItemCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "OnTimeFullyReceivedPOItemCount",
            [
                "OnTimeDeliveryPOItemCount",
                "OnTimeFullyReceivedPOItemCount",
                "OnTimeDeliveredPOItemCount",
                "OnTimePOItemCount"
            ]
        ),

    "SupplierOTDPct":
        resolve_column_name(
            silver_supplier_performance_df,
            "SupplierOTDPct",
            [
                "SupplierOnTimeDeliveryPct",
                "SupplierOTDPct",
                "OTDPct",
                "OnTimeDeliveryPct"
            ]
        ),

    "OverdueOpenDeliveryCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "OverdueOpenDeliveryCount",
            [
                "OverdueOpenPOItemCount",
                "OverdueOpenDeliveryCount"
            ]
        ),


    # ========================================================
    # Supplier Quality Index
    # ========================================================

    "InvoiceCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "InvoiceCount",
            [
                "InvoiceCount",
                "TotalInvoiceCount"
            ],
            required=False
        ),

    "DisputedInvoiceCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "DisputedInvoiceCount",
            [
                "DisputedInvoiceCount"
            ],
            required=False
        ),

    "DisputeFreeInvoiceCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "DisputeFreeInvoiceCount",
            [
                "DisputeFreeInvoiceCount"
            ],
            required=False
        ),

    "SupplierQualityIndexPct":
        resolve_column_name(
            silver_supplier_performance_df,
            "SupplierQualityIndexPct",
            [
                "SupplierQualityIndexPct",
                "SupplierQualityIndex",
                "SQIPct"
            ]
        ),


    # ========================================================
    # Three-Way Match / Invoice Exceptions
    # ========================================================

    "InvoiceItemCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "InvoiceItemCount",
            [
                "InvoiceItemCount",
                "TotalInvoiceItemCount"
            ],
            required=False
        ),

    "ThreeWayMatchedInvoiceItemCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "ThreeWayMatchedInvoiceItemCount",
            [
                "MatchedInvoiceItemCount",
                "ThreeWayMatchedInvoiceItemCount"
            ],
            required=False
        ),

    "InvoiceExceptionItemCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "InvoiceExceptionItemCount",
            [
                "InvoiceExceptionItemCount"
            ],
            required=False
        ),

    "ThreeWayMatchPct":
        resolve_column_name(
            silver_supplier_performance_df,
            "ThreeWayMatchPct",
            [
                "ThreeWayMatchPct"
            ]
        ),

    "InvoiceExceptionPct":
        resolve_column_name(
            silver_supplier_performance_df,
            "InvoiceExceptionPct",
            [
                "InvoiceExceptionPct"
            ]
        ),


    # ========================================================
    # Duplicate Invoices
    # ========================================================

    "DuplicateInvoiceCount":
        resolve_column_name(
            silver_supplier_performance_df,
            "DuplicateInvoiceCount",
            [
                "DuplicateInvoiceCount"
            ],
            required=False
        ),

    "DuplicateInvoicePct":
        resolve_column_name(
            silver_supplier_performance_df,
            "DuplicateInvoicePct",
            [
                "DuplicateInvoicePct"
            ]
        ),


    # ========================================================
    # Lineage
    # ========================================================

    "SilverRecordHash":
        resolve_column_name(
            silver_supplier_performance_df,
            "SilverRecordHash",
            [
                "SilverRecordHash"
            ]
        )
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Review resolved source columns**
# 
# If one of the additive numerator/denominator fields is absent, the result:
# 
# _FullyReceivedPOItemCount: None_
# 
# The notebook can still run, but the annual weighted KPI preview for that metric would be unavailable.

# CELL ********************

for (
    logical_name,
    physical_name
) in SOURCE_COLUMNS.items():

    print(
        f"{logical_name}: "
        f"{physical_name}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build canonical Silver source**

# CELL ********************

supplier_performance_source_df = (
    silver_supplier_performance_df

    .select(
        # ====================================================
        # Grain
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "SupplierID"
            ],
            "SupplierID",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "PerformanceYear"
            ],
            "PerformanceYear",
            "int"
        ),


        # ====================================================
        # Spend / Contract Compliance
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "EligibleSpendEUR"
            ],
            "EligibleSpendEUR",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "ContractCompliantSpendEUR"
            ],
            "ContractCompliantSpendEUR",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "MaverickSpendEUR"
            ],
            "MaverickSpendEUR",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "ContractCompliancePct"
            ],
            "ContractCompliancePct",
            "double"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "MaverickSpendPct"
            ],
            "MaverickSpendPct",
            "double"
        ),


        # ====================================================
        # Delivery / OTD
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "FullyReceivedPOItemCount"
            ],
            "FullyReceivedPOItemCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "OnTimeFullyReceivedPOItemCount"
            ],
            "OnTimeFullyReceivedPOItemCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "SupplierOTDPct"
            ],
            "SupplierOTDPct",
            "double"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "OverdueOpenDeliveryCount"
            ],
            "OverdueOpenDeliveryCount",
            "long"
        ),


        # ====================================================
        # Supplier Quality
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceCount"
            ],
            "InvoiceCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DisputedInvoiceCount"
            ],
            "DisputedInvoiceCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DisputeFreeInvoiceCount"
            ],
            "DisputeFreeInvoiceCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "SupplierQualityIndexPct"
            ],
            "SupplierQualityIndexPct",
            "double"
        ),


        # ====================================================
        # Invoice Matching
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceItemCount"
            ],
            "InvoiceItemCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "ThreeWayMatchedInvoiceItemCount"
            ],
            "ThreeWayMatchedInvoiceItemCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceExceptionItemCount"
            ],
            "InvoiceExceptionItemCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "ThreeWayMatchPct"
            ],
            "ThreeWayMatchPct",
            "double"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceExceptionPct"
            ],
            "InvoiceExceptionPct",
            "double"
        ),


        # ====================================================
        # Duplicate Invoice
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "DuplicateInvoiceCount"
            ],
            "DuplicateInvoiceCount",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DuplicateInvoicePct"
            ],
            "DuplicateInvoicePct",
            "double"
        ),


        # ====================================================
        # Lineage
        # ====================================================

        resolved_expression(
            SOURCE_COLUMNS[
                "SilverRecordHash"
            ],
            "SourceSilverRecordHash",
            "string"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive missing SQI numerator/denominator where possible**
# 
# If NB_24 has _DisputedInvoiceCount_ but not _DisputeFreeInvoiceCount_, derive it, and vice versa.

# CELL ********************

supplier_performance_source_df = (
    supplier_performance_source_df

    .withColumn(
        "DisputeFreeInvoiceCount",

        F.when(
            F.col(
                "DisputeFreeInvoiceCount"
            ).isNotNull(),

            F.col(
                "DisputeFreeInvoiceCount"
            )
        )

        .when(
            F.col(
                "InvoiceCount"
            ).isNotNull()
            &
            F.col(
                "DisputedInvoiceCount"
            ).isNotNull(),

            F.col(
                "InvoiceCount"
            )
            -
            F.col(
                "DisputedInvoiceCount"
            )
        )
    )

    .withColumn(
        "DisputedInvoiceCount",

        F.when(
            F.col(
                "DisputedInvoiceCount"
            ).isNotNull(),

            F.col(
                "DisputedInvoiceCount"
            )
        )

        .when(
            F.col(
                "InvoiceCount"
            ).isNotNull()
            &
            F.col(
                "DisputeFreeInvoiceCount"
            ).isNotNull(),

            F.col(
                "InvoiceCount"
            )
            -
            F.col(
                "DisputeFreeInvoiceCount"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect source grain**

# CELL ********************

source_performance_row_count = (
    supplier_performance_source_df.count()
)


source_supplier_count = (
    supplier_performance_source_df

    .select(
        "SupplierID"
    )

    .distinct()

    .count()
)


performance_years = (
    supplier_performance_source_df

    .select(
        "PerformanceYear"
    )

    .distinct()

    .orderBy(
        "PerformanceYear"
    )
)


print(
    "Supplier-performance rows:",
    f"{source_performance_row_count:,}"
)

print(
    "Distinct suppliers:",
    f"{source_supplier_count:,}"
)


display(
    performance_years
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build performance-period dates**
# 
# For completed years:
# 
# _2024 → 2024-01-01 through 2024-12-31_
# 
# For the current partial year:
# 
# _2026 → 2026-01-01 through 2026-07-31_

# CELL ********************

supplier_performance_source_df = (
    supplier_performance_source_df

    .withColumn(
        "PerformancePeriodStartDate",

        F.to_date(
            F.concat(
                F.col(
                    "PerformanceYear"
                ).cast("string"),

                F.lit(
                    "-01-01"
                )
            )
        )
    )

    .withColumn(
        "_CalendarYearEndDate",

        F.to_date(
            F.concat(
                F.col(
                    "PerformanceYear"
                ).cast("string"),

                F.lit(
                    "-12-31"
                )
            )
        )
    )

    .withColumn(
        "PerformancePeriodEndDate",

        F.when(
            F.col(
                "PerformanceYear"
            )
            ==
            F.lit(
                SNAPSHOT_DATE.year
            ),

            F.lit(
                SNAPSHOT_DATE
            )
        )

        .otherwise(
            F.col(
                "_CalendarYearEndDate"
            )
        )
    )

    .drop(
        "_CalendarYearEndDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add PerformancePeriodStartDateKey**

# CELL ********************

start_date_reference_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimPerformanceStartDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "PerformancePeriodStartDateKey"
        )
    )
)


fact_supplier_performance_df = (
    supplier_performance_source_df

    .join(
        start_date_reference_df,

        supplier_performance_source_df[
            "PerformancePeriodStartDate"
        ]
        ==
        start_date_reference_df[
            "DimPerformanceStartDate"
        ],

        "left"
    )

    .drop(
        "DimPerformanceStartDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add PerformancePeriodEndDateKey**

# CELL ********************

end_date_reference_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimPerformanceEndDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "PerformancePeriodEndDateKey"
        )
    )
)


fact_supplier_performance_df = (
    fact_supplier_performance_df

    .join(
        end_date_reference_df,

        fact_supplier_performance_df[
            "PerformancePeriodEndDate"
        ]
        ==
        end_date_reference_df[
            "DimPerformanceEndDate"
        ],

        "left"
    )

    .drop(
        "DimPerformanceEndDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Supplier SCD2 reference**
# 
# This fact represents supplier performance **as of the end of the reporting period**.

# CELL ********************

supplier_dimension_reference_df = (
    dim_supplier_df

    .select(
        F.col(
            "SupplierID"
        ).alias(
            "DimSupplierID"
        ),

        "SupplierKey",

        "DimensionVersion",

        "EffectiveFromDate",
        "EffectiveToDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve historical SupplierKey**

# CELL ********************

fact_supplier_performance_df = (
    fact_supplier_performance_df.alias(
        "performance"
    )

    .join(
        supplier_dimension_reference_df.alias(
            "supplier"
        ),

        (
            F.col(
                "performance.SupplierID"
            )
            ==
            F.col(
                "supplier.DimSupplierID"
            )
        )
        &
        (
            F.col(
                "performance.PerformancePeriodEndDate"
            )
            >=
            F.col(
                "supplier.EffectiveFromDate"
            )
        )
        &
        (
            F.col(
                "performance.PerformancePeriodEndDate"
            )
            <=
            F.col(
                "supplier.EffectiveToDate"
            )
        ),

        "left"
    )

    .select(
        F.col(
            "performance.*"
        ),

        F.col(
            "supplier.SupplierKey"
        ),

        F.col(
            "supplier.DimensionVersion"
        ).alias(
            "SupplierDimensionVersion"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate SCD2 join did not duplicate fact rows**

# CELL ********************

supplier_scd_duplicate_count = (
    fact_supplier_performance_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
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


print(
    "Rows duplicated by Supplier SCD2 join:",
    supplier_scd_duplicate_count
)


assert (
    supplier_scd_duplicate_count
    ==
    0
), (
    "Supplier SCD Type 2 join created "
    "duplicate SupplierID x PerformanceYear rows."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add fact surrogate key**

# CELL ********************

fact_supplier_performance_df = (
    fact_supplier_performance_df

    .withColumn(
        "SupplierPerformanceFactKey",

        F.xxhash64(
            F.col(
                "SupplierID"
            ),

            F.col(
                "PerformanceYear"
            ).cast("string")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add additive snapshot measures**

# CELL ********************

fact_supplier_performance_df = (
    fact_supplier_performance_df

    .withColumn(
        "SupplierPerformanceRecordCount",
        F.lit(1).cast("long")
    )

    .withColumn(
        "SupplierWithSpendCount",

        F.when(
            F.coalesce(
                F.col(
                    "EligibleSpendEUR"
                ),
                F.lit(0)
            )
            > 0,

            F.lit(1)
        )

        .otherwise(
            F.lit(0)
        )

        .cast("long")
    )

    .withColumn(
        "SupplierWithDeliveryActivityCount",

        F.when(
            F.coalesce(
                F.col(
                    "FullyReceivedPOItemCount"
                ),
                F.lit(0)
            )
            > 0,

            F.lit(1)
        )

        .otherwise(
            F.lit(0)
        )

        .cast("long")
    )

    .withColumn(
        "SupplierWithInvoiceActivityCount",

        F.when(
            F.coalesce(
                F.col(
                    "InvoiceCount"
                ),
                F.lit(0)
            )
            > 0,

            F.lit(1)
        )

        .otherwise(
            F.lit(0)
        )

        .cast("long")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add Gold metadata**

# CELL ********************

GOLD_HASH_EXCLUDED_COLUMNS = {
    "GoldLoadTimestamp",
    "GoldLoadDate",
    "GoldRecordHash"
}


hash_columns = [
    column_name
    for column_name
    in fact_supplier_performance_df.columns
    if column_name
    not in GOLD_HASH_EXCLUDED_COLUMNS
]


hash_components = [
    F.coalesce(
        F.col(
            column_name
        ).cast("string"),
        F.lit("__NULL__")
    )
    for column_name
    in hash_columns
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_supplier_performance_df = (
    fact_supplier_performance_df

    .withColumn(
        "GoldSourceTable",

        F.lit(
            SILVER_SUPPLIER_PERFORMANCE_TABLE
        )
    )

    .withColumn(
        "GoldLoadTimestamp",
        F.current_timestamp()
    )

    .withColumn(
        "GoldLoadDate",
        F.current_date()
    )

    .withColumn(
        "GoldRecordHash",

        F.sha2(
            F.concat_ws(
                "||",
                *hash_components
            ),
            256
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Preview dimension resolution**

# CELL ********************

display(
    fact_supplier_performance_df

    .select(
        "SupplierID",
        "PerformanceYear",

        "PerformancePeriodStartDate",
        "PerformancePeriodStartDateKey",

        "PerformancePeriodEndDate",
        "PerformancePeriodEndDateKey",

        "SupplierKey",
        "SupplierDimensionVersion"
    )

    .orderBy(
        "PerformanceYear",
        "SupplierID"
    )

    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Preview performance KPIs**

# CELL ********************

display(
    fact_supplier_performance_df

    .select(
        "SupplierID",
        "PerformanceYear",

        "EligibleSpendEUR",
        "ContractCompliantSpendEUR",
        "MaverickSpendEUR",

        "ContractCompliancePct",
        "MaverickSpendPct",

        "FullyReceivedPOItemCount",
        "OnTimeFullyReceivedPOItemCount",
        "SupplierOTDPct",

        "OverdueOpenDeliveryCount",

        "InvoiceCount",
        "DisputedInvoiceCount",
        "DisputeFreeInvoiceCount",

        "SupplierQualityIndexPct",

        "InvoiceItemCount",
        "ThreeWayMatchedInvoiceItemCount",
        "InvoiceExceptionItemCount",

        "ThreeWayMatchPct",
        "InvoiceExceptionPct",

        "DuplicateInvoiceCount",
        "DuplicateInvoicePct"
    )

    .orderBy(
        "PerformanceYear",
        "SupplierID"
    )

    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation framework**

# CELL ********************

validation_results = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def register_validation(
    category,
    rule,
    failed_count,
    details=""
):

    failed_count = int(
        failed_count or 0
    )


    validation_results.append(
        {
            "TableName":
                FACT_SUPPLIER_PERFORMANCE_TABLE,

            "ValidationCategory":
                category,

            "ValidationRule":
                rule,

            "FailedRecordCount":
                failed_count,

            "ValidationStatus":
                (
                    "PASSED"
                    if failed_count == 0
                    else "FAILED"
                ),

            "ValidationDetails":
                details
        }
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate row preservation**

# CELL ********************

gold_performance_row_count = (
    fact_supplier_performance_df.count()
)


register_validation(
    "Row Count",

    (
        "One Gold fact row exists per "
        "Silver SupplierID x PerformanceYear"
    ),

    abs(
        source_performance_row_count
        -
        gold_performance_row_count
    ),

    (
        f"Silver: "
        f"{source_performance_row_count:,}; "
        f"Gold: "
        f"{gold_performance_row_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate fact grain**

# CELL ********************

duplicate_supplier_year_count = (
    fact_supplier_performance_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
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


register_validation(
    "Primary Key",

    (
        "SupplierID x PerformanceYear "
        "is unique"
    ),

    duplicate_supplier_year_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate fact surrogate key**

# CELL ********************

duplicate_fact_key_count = (
    fact_supplier_performance_df

    .groupBy(
        "SupplierPerformanceFactKey"
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


register_validation(
    "Surrogate Key",

    (
        "SupplierPerformanceFactKey "
        "is unique"
    ),

    duplicate_fact_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate performance years**

# CELL ********************

invalid_performance_year_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "PerformanceYear"
        ).isNull()
        |
        (
            F.col(
                "PerformanceYear"
            )
            <
            2022
        )
        |
        (
            F.col(
                "PerformanceYear"
            )
            >
            F.lit(
                SNAPSHOT_DATE.year
            )
        )
    )

    .count()
)


register_validation(
    "Performance Period",

    (
        "PerformanceYear is within "
        "the analytical period"
    ),

    invalid_performance_year_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate period dates**

# CELL ********************

invalid_period_date_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "PerformancePeriodStartDate"
        ).isNull()
        |
        F.col(
            "PerformancePeriodEndDate"
        ).isNull()
        |
        (
            F.col(
                "PerformancePeriodEndDate"
            )
            <
            F.col(
                "PerformancePeriodStartDate"
            )
        )
        |
        (
            F.col(
                "PerformancePeriodEndDate"
            )
            >
            F.lit(
                SNAPSHOT_DATE
            )
        )
    )

    .count()
)


register_validation(
    "Performance Period",

    (
        "Supplier performance period "
        "dates are valid"
    ),

    invalid_period_date_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate DateKeys**

# CELL ********************

missing_date_key_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "PerformancePeriodStartDateKey"
        ).isNull()
        |
        F.col(
            "PerformancePeriodEndDateKey"
        ).isNull()
    )

    .count()
)


register_validation(
    "Date Dimension",

    (
        "Performance period start and "
        "end dates resolve to dim_date"
    ),

    missing_date_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier SCD2 resolution**

# CELL ********************

unresolved_supplier_key_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "SupplierKey"
        ).isNull()
    )

    .count()
)


register_validation(
    "SCD Type 2",

    (
        "Every supplier-performance row "
        "resolves to the Supplier version "
        "valid at performance period end"
    ),

    unresolved_supplier_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate percentage domains**

# CELL ********************

percentage_columns = [
    "ContractCompliancePct",
    "MaverickSpendPct",
    "SupplierOTDPct",
    "SupplierQualityIndexPct",
    "ThreeWayMatchPct",
    "InvoiceExceptionPct",
    "DuplicateInvoicePct"
]


for percentage_column in percentage_columns:

    invalid_percentage_count = (
        fact_supplier_performance_df

        .filter(
            F.col(
                percentage_column
            ).isNotNull()
            &
            (
                (
                    F.col(
                        percentage_column
                    )
                    < 0
                )
                |
                (
                    F.col(
                        percentage_column
                    )
                    > 100
                )
            )
        )

        .count()
    )


    register_validation(
        "KPI Domain",

        (
            f"{percentage_column} "
            f"is between 0 and 100"
        ),

        invalid_percentage_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate spend reconciliation**
# 
# Only evaluate rows where the additive measures exist.

# CELL ********************

spend_reconciliation_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "EligibleSpendEUR"
        ).isNotNull()
        &
        F.col(
            "ContractCompliantSpendEUR"
        ).isNotNull()
        &
        F.col(
            "MaverickSpendEUR"
        ).isNotNull()
        &
        (
            F.abs(
                F.col(
                    "EligibleSpendEUR"
                )
                -
                (
                    F.col(
                        "ContractCompliantSpendEUR"
                    )
                    +
                    F.col(
                        "MaverickSpendEUR"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "Spend Reconciliation",

    (
        "Eligible spend equals compliant "
        "spend plus Maverick spend"
    ),

    spend_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Contract Compliance %**

# CELL ********************

contract_compliance_pct_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "EligibleSpendEUR"
        ).isNotNull()
        &
        F.col(
            "ContractCompliantSpendEUR"
        ).isNotNull()
        &
        (
            F.col(
                "EligibleSpendEUR"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "ContractCompliancePct"
                )
                -
                (
                    F.col(
                        "ContractCompliantSpendEUR"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "EligibleSpendEUR"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "KPI Logic",

    (
        "ContractCompliancePct agrees "
        "with additive spend measures"
    ),

    contract_compliance_pct_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Maverick Spend %**

# CELL ********************

maverick_pct_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "EligibleSpendEUR"
        ).isNotNull()
        &
        F.col(
            "MaverickSpendEUR"
        ).isNotNull()
        &
        (
            F.col(
                "EligibleSpendEUR"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "MaverickSpendPct"
                )
                -
                (
                    F.col(
                        "MaverickSpendEUR"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "EligibleSpendEUR"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "KPI Logic",

    (
        "MaverickSpendPct agrees "
        "with additive spend measures"
    ),

    maverick_pct_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate OTD counts**

# CELL ********************

invalid_otd_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "FullyReceivedPOItemCount"
        ).isNotNull()
        &
        F.col(
            "OnTimeFullyReceivedPOItemCount"
        ).isNotNull()
        &
        (
            (
                F.col(
                    "FullyReceivedPOItemCount"
                )
                < 0
            )
            |
            (
                F.col(
                    "OnTimeFullyReceivedPOItemCount"
                )
                < 0
            )
            |
            (
                F.col(
                    "OnTimeFullyReceivedPOItemCount"
                )
                >
                F.col(
                    "FullyReceivedPOItemCount"
                )
            )
        )
    )

    .count()
)


register_validation(
    "OTD",

    (
        "On-time fully received count "
        "does not exceed fully received count"
    ),

    invalid_otd_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate OTD %**

# CELL ********************

otd_pct_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "FullyReceivedPOItemCount"
        ).isNotNull()
        &
        F.col(
            "OnTimeFullyReceivedPOItemCount"
        ).isNotNull()
        &
        (
            F.col(
                "FullyReceivedPOItemCount"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "SupplierOTDPct"
                )
                -
                (
                    F.col(
                        "OnTimeFullyReceivedPOItemCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "FullyReceivedPOItemCount"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "OTD",

    (
        "SupplierOTDPct agrees with "
        "fully received delivery counts"
    ),

    otd_pct_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier Quality Index**

# CELL ********************

sqi_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "InvoiceCount"
        ).isNotNull()
        &
        F.col(
            "DisputeFreeInvoiceCount"
        ).isNotNull()
        &
        (
            F.col(
                "InvoiceCount"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "SupplierQualityIndexPct"
                )
                -
                (
                    F.col(
                        "DisputeFreeInvoiceCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "InvoiceCount"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "Supplier Quality",

    (
        "SupplierQualityIndexPct agrees "
        "with dispute-free invoice rate"
    ),

    sqi_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Three-Way Match %**

# CELL ********************

three_way_match_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "InvoiceItemCount"
        ).isNotNull()
        &
        F.col(
            "ThreeWayMatchedInvoiceItemCount"
        ).isNotNull()
        &
        (
            F.col(
                "InvoiceItemCount"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "ThreeWayMatchPct"
                )
                -
                (
                    F.col(
                        "ThreeWayMatchedInvoiceItemCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "InvoiceItemCount"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "Invoice Matching",

    (
        "ThreeWayMatchPct agrees "
        "with invoice-item counts"
    ),

    three_way_match_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Invoice Exception %**

# CELL ********************

invoice_exception_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "InvoiceItemCount"
        ).isNotNull()
        &
        F.col(
            "InvoiceExceptionItemCount"
        ).isNotNull()
        &
        (
            F.col(
                "InvoiceItemCount"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "InvoiceExceptionPct"
                )
                -
                (
                    F.col(
                        "InvoiceExceptionItemCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "InvoiceItemCount"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "Invoice Matching",

    (
        "InvoiceExceptionPct agrees "
        "with invoice-item counts"
    ),

    invoice_exception_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Duplicate Invoice %**

# CELL ********************

duplicate_invoice_error_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "InvoiceCount"
        ).isNotNull()
        &
        F.col(
            "DuplicateInvoiceCount"
        ).isNotNull()
        &
        (
            F.col(
                "InvoiceCount"
            )
            > 0
        )
        &
        (
            F.abs(
                F.col(
                    "DuplicateInvoicePct"
                )
                -
                (
                    F.col(
                        "DuplicateInvoiceCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                    /
                    F.col(
                        "InvoiceCount"
                    )
                )
            )
            >
            F.lit(
                0.02
            )
        )
    )

    .count()
)


register_validation(
    "Invoice Quality",

    (
        "DuplicateInvoicePct agrees "
        "with invoice-level counts"
    ),

    duplicate_invoice_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate non-negative operational counts**

# CELL ********************

count_columns = [
    "FullyReceivedPOItemCount",
    "OnTimeFullyReceivedPOItemCount",
    "OverdueOpenDeliveryCount",
    "InvoiceCount",
    "DisputedInvoiceCount",
    "DisputeFreeInvoiceCount",
    "InvoiceItemCount",
    "ThreeWayMatchedInvoiceItemCount",
    "InvoiceExceptionItemCount",
    "DuplicateInvoiceCount"
]


for count_column in count_columns:

    negative_count = (
        fact_supplier_performance_df

        .filter(
            F.col(
                count_column
            ).isNotNull()
            &
            (
                F.col(
                    count_column
                )
                < 0
            )
        )

        .count()
    )


    register_validation(
        "Operational Counts",

        (
            f"{count_column} "
            f"is non-negative"
        ),

        negative_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Gold lineage**

# CELL ********************

invalid_lineage_count = (
    fact_supplier_performance_df

    .filter(
        F.col(
            "SourceSilverRecordHash"
        ).isNull()
        |
        F.col(
            "GoldLoadTimestamp"
        ).isNull()
        |
        F.col(
            "GoldLoadDate"
        ).isNull()
        |
        F.col(
            "GoldRecordHash"
        ).isNull()
        |
        (
            F.length(
                "GoldRecordHash"
            )
            != 64
        )
    )

    .count()
)


register_validation(
    "Lineage",

    (
        "Silver lineage and Gold "
        "metadata are complete"
    ),

    invalid_lineage_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build validation results**

# CELL ********************

validation_results_df = (
    spark.createDataFrame(
        validation_results
    )

    .withColumn(
        "ExecutionTimestamp",
        F.current_timestamp()
    )
)


display(
    validation_results_df

    .orderBy(
        "ValidationStatus",
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

# **Count failures**

# CELL ********************

pre_write_failure_count = (
    validation_results_df

    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAILED"
    )

    .count()
)


print(
    "Gold Supplier Performance Fact "
    "pre-write failures:",
    pre_write_failure_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist monitoring table**

# CELL ********************

(
    validation_results_df.write

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
        GOLD_MONITORING_TABLE
    )
)


print(
    "Created Gold monitoring table:",
    GOLD_MONITORING_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Pre-write quality gate**

# CELL ********************

if pre_write_failure_count > 0:

    display(
        validation_results_df

        .filter(
            F.col(
                "ValidationStatus"
            )
            ==
            "FAILED"
        )

        .orderBy(
            F.desc(
                "FailedRecordCount"
            )
        )
    )


    raise AssertionError(
        f"Gold Supplier Performance Fact "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "GOLD SUPPLIER PERFORMANCE FACT "
    "PRE-WRITE QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write physical Gold fact**

# CELL ********************

(
    fact_supplier_performance_df.write

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
        FACT_SUPPLIER_PERFORMANCE_TABLE
    )
)


print(
    "Created physical Gold fact:",
    FACT_SUPPLIER_PERFORMANCE_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted row count**

# CELL ********************

persisted_fact_supplier_performance_df = (
    spark.table(
        FACT_SUPPLIER_PERFORMANCE_TABLE
    )
)


persisted_row_count = (
    persisted_fact_supplier_performance_df.count()
)


print(
    "Expected rows:",
    f"{source_performance_row_count:,}"
)

print(
    "Persisted rows:",
    f"{persisted_row_count:,}"
)


assert (
    source_performance_row_count
    ==
    persisted_row_count
)


print(
    "Persisted row-count validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Enterprise annual KPI preview**
# 
# We calculate enterprise KPIs from the underlying **additive measures**, not by averaging supplier percentages.

# CELL ********************

annual_supplier_performance_df = (
    persisted_fact_supplier_performance_df

    .groupBy(
        "PerformanceYear"
    )

    .agg(
        # ====================================================
        # Supplier population
        # ====================================================

        F.sum(
            "SupplierPerformanceRecordCount"
        ).alias(
            "SupplierPerformanceRecordCount"
        ),

        # ====================================================
        # Spend
        # ====================================================

        F.sum(
            "EligibleSpendEUR"
        ).alias(
            "EligibleSpendEUR"
        ),

        F.sum(
            "ContractCompliantSpendEUR"
        ).alias(
            "ContractCompliantSpendEUR"
        ),

        F.sum(
            "MaverickSpendEUR"
        ).alias(
            "MaverickSpendEUR"
        ),

        # ====================================================
        # Delivery
        # ====================================================

        F.sum(
            "FullyReceivedPOItemCount"
        ).alias(
            "FullyReceivedPOItemCount"
        ),

        F.sum(
            "OnTimeFullyReceivedPOItemCount"
        ).alias(
            "OnTimeFullyReceivedPOItemCount"
        ),

        F.sum(
            "OverdueOpenDeliveryCount"
        ).alias(
            "OverdueOpenDeliveryCount"
        ),

        # ====================================================
        # Invoice quality
        # ====================================================

        F.sum(
            "InvoiceCount"
        ).alias(
            "InvoiceCount"
        ),

        F.sum(
            "DisputeFreeInvoiceCount"
        ).alias(
            "DisputeFreeInvoiceCount"
        ),

        F.sum(
            "DuplicateInvoiceCount"
        ).alias(
            "DuplicateInvoiceCount"
        ),

        # ====================================================
        # Invoice matching
        # ====================================================

        F.sum(
            "InvoiceItemCount"
        ).alias(
            "InvoiceItemCount"
        ),

        F.sum(
            "ThreeWayMatchedInvoiceItemCount"
        ).alias(
            "ThreeWayMatchedInvoiceItemCount"
        ),

        F.sum(
            "InvoiceExceptionItemCount"
        ).alias(
            "InvoiceExceptionItemCount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Calculate enterprise annual percentages**

# CELL ********************

annual_supplier_performance_df = (
    annual_supplier_performance_df

    .withColumn(
        "ContractCompliancePct",

        F.when(
            F.col(
                "EligibleSpendEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "ContractCompliantSpendEUR"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "EligibleSpendEUR"
                ),
                2
            )
        )
    )

    .withColumn(
        "MaverickSpendPct",

        F.when(
            F.col(
                "EligibleSpendEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "MaverickSpendEUR"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "EligibleSpendEUR"
                ),
                2
            )
        )
    )

    .withColumn(
        "SupplierOTDPct",

        F.when(
            F.col(
                "FullyReceivedPOItemCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "OnTimeFullyReceivedPOItemCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "FullyReceivedPOItemCount"
                ),
                2
            )
        )
    )

    .withColumn(
        "SupplierQualityIndexPct",

        F.when(
            F.col(
                "InvoiceCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "DisputeFreeInvoiceCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "InvoiceCount"
                ),
                2
            )
        )
    )

    .withColumn(
        "ThreeWayMatchPct",

        F.when(
            F.col(
                "InvoiceItemCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "ThreeWayMatchedInvoiceItemCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "InvoiceItemCount"
                ),
                2
            )
        )
    )

    .withColumn(
        "InvoiceExceptionPct",

        F.when(
            F.col(
                "InvoiceItemCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "InvoiceExceptionItemCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "InvoiceItemCount"
                ),
                2
            )
        )
    )

    .withColumn(
        "DuplicateInvoicePct",

        F.when(
            F.col(
                "InvoiceCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "DuplicateInvoiceCount"
                    )
                    *
                    F.lit(
                        100.0
                    )
                )
                /
                F.col(
                    "InvoiceCount"
                ),
                2
            )
        )
    )

    .orderBy(
        "PerformanceYear"
    )
)


display(
    annual_supplier_performance_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Supplier performance trend preview**

# CELL ********************

# ============================================================
# Supplier Performance Trend Preview
# ============================================================

supplier_performance_trend_df = (
    persisted_fact_supplier_performance_df.alias(
        "performance"
    )

    .join(
        dim_supplier_df.alias(
            "supplier"
        ),

        F.col(
            "performance.SupplierKey"
        )
        ==
        F.col(
            "supplier.SupplierKey"
        ),

        "left"
    )

    .select(
        F.col(
            "performance.PerformanceYear"
        ).alias(
            "PerformanceYear"
        ),

        F.col(
            "performance.SupplierKey"
        ).alias(
            "SupplierKey"
        ),

        F.col(
            "performance.SupplierID"
        ).alias(
            "SupplierID"
        ),

        F.col(
            "supplier.SupplierName"
        ).alias(
            "SupplierName"
        ),

        F.col(
            "performance.SupplierDimensionVersion"
        ).alias(
            "SupplierDimensionVersion"
        ),

        F.col(
            "performance.EligibleSpendEUR"
        ).alias(
            "EligibleSpendEUR"
        ),

        F.col(
            "performance.ContractCompliancePct"
        ).alias(
            "ContractCompliancePct"
        ),

        F.col(
            "performance.MaverickSpendPct"
        ).alias(
            "MaverickSpendPct"
        ),

        F.col(
            "performance.SupplierOTDPct"
        ).alias(
            "SupplierOTDPct"
        ),

        F.col(
            "performance.SupplierQualityIndexPct"
        ).alias(
            "SupplierQualityIndexPct"
        ),

        F.col(
            "performance.ThreeWayMatchPct"
        ).alias(
            "ThreeWayMatchPct"
        ),

        F.col(
            "performance.InvoiceExceptionPct"
        ).alias(
            "InvoiceExceptionPct"
        ),

        F.col(
            "performance.DuplicateInvoicePct"
        ).alias(
            "DuplicateInvoicePct"
        ),

        F.col(
            "performance.OverdueOpenDeliveryCount"
        ).alias(
            "OverdueOpenDeliveryCount"
        )
    )

    .orderBy(
        "SupplierID",
        "PerformanceYear"
    )
)


display(
    supplier_performance_trend_df

    .limit(
        100
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Highest overdue delivery exposure**

# CELL ********************

# ============================================================
# Highest Overdue Delivery Exposure
# ============================================================

display(
    persisted_fact_supplier_performance_df.alias(
        "performance"
    )

    .join(
        dim_supplier_df.alias(
            "supplier"
        ),

        F.col(
            "performance.SupplierKey"
        )
        ==
        F.col(
            "supplier.SupplierKey"
        ),

        "left"
    )

    .filter(
        F.col(
            "performance.OverdueOpenDeliveryCount"
        )
        > 0
    )

    .select(
        F.col(
            "performance.PerformanceYear"
        ).alias(
            "PerformanceYear"
        ),

        F.col(
            "performance.SupplierID"
        ).alias(
            "SupplierID"
        ),

        F.col(
            "supplier.SupplierName"
        ).alias(
            "SupplierName"
        ),

        F.col(
            "performance.EligibleSpendEUR"
        ).alias(
            "EligibleSpendEUR"
        ),

        F.col(
            "performance.SupplierOTDPct"
        ).alias(
            "SupplierOTDPct"
        ),

        F.col(
            "performance.OverdueOpenDeliveryCount"
        ).alias(
            "OverdueOpenDeliveryCount"
        ),

        F.col(
            "performance.ContractCompliancePct"
        ).alias(
            "ContractCompliancePct"
        ),

        F.col(
            "performance.SupplierQualityIndexPct"
        ).alias(
            "SupplierQualityIndexPct"
        ),

        F.col(
            "performance.InvoiceExceptionPct"
        ).alias(
            "InvoiceExceptionPct"
        )
    )

    .orderBy(
        F.desc(
            "OverdueOpenDeliveryCount"
        ),

        F.desc(
            "EligibleSpendEUR"
        )
    )

    .limit(
        50
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Lowest OTD suppliers**

# CELL ********************

# ============================================================
# Lowest OTD Suppliers
# ============================================================

display(
    persisted_fact_supplier_performance_df.alias(
        "performance"
    )

    .join(
        dim_supplier_df.alias(
            "supplier"
        ),

        F.col(
            "performance.SupplierKey"
        )
        ==
        F.col(
            "supplier.SupplierKey"
        ),

        "left"
    )

    .filter(
        F.col(
            "performance.FullyReceivedPOItemCount"
        )
        > 0
    )

    .select(
        F.col(
            "performance.PerformanceYear"
        ).alias(
            "PerformanceYear"
        ),

        F.col(
            "performance.SupplierID"
        ).alias(
            "SupplierID"
        ),

        F.col(
            "supplier.SupplierName"
        ).alias(
            "SupplierName"
        ),

        F.col(
            "performance.EligibleSpendEUR"
        ).alias(
            "EligibleSpendEUR"
        ),

        F.col(
            "performance.FullyReceivedPOItemCount"
        ).alias(
            "FullyReceivedPOItemCount"
        ),

        F.col(
            "performance.OnTimeFullyReceivedPOItemCount"
        ).alias(
            "OnTimeFullyReceivedPOItemCount"
        ),

        F.col(
            "performance.SupplierOTDPct"
        ).alias(
            "SupplierOTDPct"
        ),

        F.col(
            "performance.OverdueOpenDeliveryCount"
        ).alias(
            "OverdueOpenDeliveryCount"
        )
    )

    .orderBy(
        F.asc(
            "SupplierOTDPct"
        ),

        F.desc(
            "EligibleSpendEUR"
        )
    )

    .limit(
        50
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final fact preview**

# CELL ********************

display(
    persisted_fact_supplier_performance_df

    .select(
        "SupplierPerformanceFactKey",

        "SupplierID",
        "SupplierKey",
        "SupplierDimensionVersion",

        "PerformanceYear",

        "PerformancePeriodStartDate",
        "PerformancePeriodStartDateKey",

        "PerformancePeriodEndDate",
        "PerformancePeriodEndDateKey",

        "EligibleSpendEUR",
        "ContractCompliantSpendEUR",
        "MaverickSpendEUR",

        "ContractCompliancePct",
        "MaverickSpendPct",

        "FullyReceivedPOItemCount",
        "OnTimeFullyReceivedPOItemCount",
        "SupplierOTDPct",

        "OverdueOpenDeliveryCount",

        "InvoiceCount",
        "DisputedInvoiceCount",
        "DisputeFreeInvoiceCount",

        "SupplierQualityIndexPct",

        "InvoiceItemCount",
        "ThreeWayMatchedInvoiceItemCount",
        "InvoiceExceptionItemCount",

        "ThreeWayMatchPct",
        "InvoiceExceptionPct",

        "DuplicateInvoiceCount",
        "DuplicateInvoicePct",

        "SupplierPerformanceRecordCount"
    )

    .orderBy(
        "PerformanceYear",
        "SupplierID"
    )

    .limit(
        100
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final status**

# CELL ********************

print(
    "NB_34_Build_Gold_Fact_Supplier_Performance "
    "completed successfully."
)

print()

print(
    "Physical Gold output:"
)

print(
    "  fact_supplier_performance"
)

print()

print(
    "Grain:"
)

print(
    "  SupplierID x PerformanceYear"
)

print()

print(
    "Period keys:"
)

print(
    "  - PerformancePeriodStartDateKey"
)

print(
    "  - PerformancePeriodEndDateKey"
)

print()

print(
    "Conformed dimension key:"
)

print(
    "  - SupplierKey "
    "(SCD Type 2 at performance period end)"
)

print()

print(
    "Core KPI families:"
)

print(
    "  - Contract Compliance"
)

print(
    "  - Maverick Spend"
)

print(
    "  - Supplier OTD"
)

print(
    "  - Overdue Open Deliveries"
)

print(
    "  - Supplier Quality Index"
)

print(
    "  - Three-Way Match"
)

print(
    "  - Invoice Exceptions"
)

print(
    "  - Duplicate Invoices"
)

print()

print(
    "Gold core fact layer is now complete:"
)

print(
    "  - fact_purchase_order"
)

print(
    "  - fact_invoice"
)

print(
    "  - fact_savings"
)

print(
    "  - fact_supplier_performance"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
