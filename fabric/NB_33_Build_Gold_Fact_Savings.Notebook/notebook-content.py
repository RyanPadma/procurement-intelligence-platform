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

# **Physical Output**: _fact_savings_
# 
# **Grain**: One row per _SavingsProjectID_
# 
# The EUR conversion is not recalculated in Gold. This notebook will carry the validated EUR savings measures from NB_25.

# MARKDOWN ********************

# **Configuration**

# CELL ********************

# ============================================================
# NB_33_Build_Gold_Fact_Savings
# Configuration
# ============================================================

SILVER_SAVINGS_TABLE = (
    "silver_savings_project"
)

SILVER_SAVINGS_MONITORING_TABLE = (
    "monitoring_silver_savings_project_quality_results"
)

GOLD_DIMENSION_MONITORING_TABLE = (
    "monitoring_gold_dimensions_quality_results"
)


DIM_DATE_TABLE = "dim_date"
DIM_SUPPLIER_TABLE = "dim_supplier"
DIM_CATEGORY_TABLE = "dim_category"
DIM_BUYER_TABLE = "dim_buyer"
DIM_BUSINESS_UNIT_TABLE = "dim_business_unit"
DIM_CONTRACT_TABLE = "dim_contract"
DIM_CURRENCY_TABLE = "dim_currency"


FACT_SAVINGS_TABLE = (
    "fact_savings"
)

GOLD_MONITORING_TABLE = (
    "monitoring_gold_fact_savings_quality_results"
)


print(
    "Notebook: NB_33_Build_Gold_Fact_Savings"
)

print(
    "Default Lakehouse: lh_procurement_gold"
)

print(
    "Output table: fact_savings"
)

print(
    "Grain: one row per SavingsProjectID"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Imports**

# CELL ********************

from pyspark.sql import functions as F

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate required tables**

# CELL ********************

required_tables = [
    SILVER_SAVINGS_TABLE,
    SILVER_SAVINGS_MONITORING_TABLE,
    GOLD_DIMENSION_MONITORING_TABLE,

    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE,
    DIM_CATEGORY_TABLE,
    DIM_BUYER_TABLE,
    DIM_BUSINESS_UNIT_TABLE,
    DIM_CONTRACT_TABLE,
    DIM_CURRENCY_TABLE
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

silver_savings_failure_count = (
    spark.table(
        SILVER_SAVINGS_MONITORING_TABLE
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
    "NB_25 Silver Savings failures:",
    silver_savings_failure_count
)

print(
    "NB_30 Gold Dimensions failures:",
    gold_dimension_failure_count
)


assert (
    silver_savings_failure_count == 0
), (
    "NB_25 Silver Savings Project "
    "quality gate has not passed."
)


assert (
    gold_dimension_failure_count == 0
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

silver_savings_df = spark.table(
    SILVER_SAVINGS_TABLE
)

dim_date_df = spark.table(
    DIM_DATE_TABLE
)

dim_supplier_df = spark.table(
    DIM_SUPPLIER_TABLE
)

dim_category_df = spark.table(
    DIM_CATEGORY_TABLE
)

dim_buyer_df = spark.table(
    DIM_BUYER_TABLE
)

dim_business_unit_df = spark.table(
    DIM_BUSINESS_UNIT_TABLE
)

dim_contract_df = spark.table(
    DIM_CONTRACT_TABLE
)

dim_currency_df = spark.table(
    DIM_CURRENCY_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Silver savings projects:",
    f"{silver_savings_df.count():,}"
)

print(
    "Supplier dimension rows:",
    f"{dim_supplier_df.count():,}"
)

print(
    "Category dimension rows:",
    f"{dim_category_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate required Silver source columns**

# CELL ********************

required_source_columns = [
    "SavingsProjectID",
    "SavingsProjectName",

    "SupplierID",
    "CategoryID",
    "BuyerID",
    "BusinessUnitID",
    "ContractID",

    "SavingsType",
    "ProjectStatus",
    "SavingsLevel",
    "ApprovalStatus",

    "ProjectCreatedDate",
    "PlannedStartDate",
    "PlannedCompletionDate",
    "ActualCompletionDate",
    "CancellationDate",

    "Currency",

    "BaselineSpend",
    "ForecastedSavings",
    "WeightedForecastSavings",
    "ApprovedSavings",
    "RealizedSavings",

    "BaselineSpendEUR",
    "ForecastedSavingsEUR",
    "WeightedForecastSavingsEUR",
    "ApprovedSavingsEUR",
    "RealizedSavingsEUR",

    "SavingsConfidenceWeight",

    "ForecastSavingsPctOfBaseline",
    "ApprovedSavingsPctOfBaseline",
    "RealizedSavingsPctOfBaseline",

    "SavingsAchievementPct",
    "SavingsAchievementVarianceEUR",

    "ActivePipelineFlag",
    "IsIdeaFlag",
    "IsValidatedFlag",
    "IsNegotiationFlag",
    "IsImplementedFlag",
    "IsCancelledFlag",

    "OverdueProjectFlag",
    "DaysPastPlannedCompletion",

    "PlannedDurationDays",
    "ActualDurationDays",

    "ActivePipelineForecastEUR",
    "ActivePipelineWeightedForecastEUR",
    "ImplementedSavingsEUR",

    "RecurringSavingsFlag",

    "SilverRecordHash"
]


missing_source_columns = [
    column_name
    for column_name in required_source_columns
    if column_name
    not in silver_savings_df.columns
]


if missing_source_columns:

    raise RuntimeError(
        "silver_savings_project is missing "
        "required columns: "
        +
        ", ".join(
            missing_source_columns
        )
    )


print(
    "silver_savings_project schema confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build canonical Savings source**

# CELL ********************

savings_source_df = (
    silver_savings_df

    .select(
        # ----------------------------------------------------
        # Business keys
        # ----------------------------------------------------

        "SavingsProjectID",
        "SavingsProjectName",

        "SupplierID",
        "CategoryID",
        "BuyerID",
        "BusinessUnitID",
        "ContractID",

        # ----------------------------------------------------
        # Project attributes
        # ----------------------------------------------------

        "SavingsType",
        "ProjectStatus",
        "SavingsLevel",
        "ApprovalStatus",

        # ----------------------------------------------------
        # Date roles
        # ----------------------------------------------------

        "ProjectCreatedDate",
        "PlannedStartDate",
        "PlannedCompletionDate",
        "ActualCompletionDate",
        "CancellationDate",

        # ----------------------------------------------------
        # Original transaction currency
        # ----------------------------------------------------

        F.col(
            "Currency"
        ).alias(
            "CurrencyCode"
        ),

        # ----------------------------------------------------
        # Source-currency measures
        # ----------------------------------------------------

        "BaselineSpend",
        "ForecastedSavings",
        "WeightedForecastSavings",
        "ApprovedSavings",
        "RealizedSavings",

        # ----------------------------------------------------
        # EUR measures
        # ----------------------------------------------------

        "BaselineSpendEUR",
        "ForecastedSavingsEUR",
        "WeightedForecastSavingsEUR",
        "ApprovedSavingsEUR",
        "RealizedSavingsEUR",

        # ----------------------------------------------------
        # Savings maturity
        # ----------------------------------------------------

        "SavingsConfidenceWeight",

        # ----------------------------------------------------
        # Savings-rate analytics
        # ----------------------------------------------------

        "ForecastSavingsPctOfBaseline",
        "ApprovedSavingsPctOfBaseline",
        "RealizedSavingsPctOfBaseline",

        "SavingsAchievementPct",
        "SavingsAchievementVarianceEUR",

        # ----------------------------------------------------
        # Lifecycle
        # ----------------------------------------------------

        "ActivePipelineFlag",

        "IsIdeaFlag",
        "IsValidatedFlag",
        "IsNegotiationFlag",
        "IsImplementedFlag",
        "IsCancelledFlag",

        "OverdueProjectFlag",

        "DaysPastPlannedCompletion",
        "PlannedDurationDays",
        "ActualDurationDays",

        # ----------------------------------------------------
        # Reporting-ready measures
        # ----------------------------------------------------

        "ActivePipelineForecastEUR",
        "ActivePipelineWeightedForecastEUR",
        "ImplementedSavingsEUR",

        "RecurringSavingsFlag",

        # ----------------------------------------------------
        # Silver lineage
        # ----------------------------------------------------

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect Savings population**

# CELL ********************

source_savings_project_count = (
    savings_source_df.count()
)


print(
    "Savings projects:",
    f"{source_savings_project_count:,}"
)


display(
    savings_source_df

    .groupBy(
        "ProjectStatus",
        "SavingsLevel"
    )

    .count()

    .orderBy(
        "ProjectStatus",
        "SavingsLevel"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create reusable _DateKey_ join helper**

# CELL ********************

def add_date_key(
    dataframe,
    source_date_column,
    output_key_column
):

    reference_date_column = (
        f"_DimDate_{output_key_column}"
    )


    date_reference_df = (
        dim_date_df

        .select(
            F.col(
                "Date"
            ).alias(
                reference_date_column
            ),

            F.col(
                "DateKey"
            ).alias(
                output_key_column
            )
        )
    )


    return (
        dataframe

        .join(
            date_reference_df,

            dataframe[
                source_date_column
            ]
            ==
            date_reference_df[
                reference_date_column
            ],

            "left"
        )

        .drop(
            reference_date_column
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve ProjectCreatedDateKey**

# CELL ********************

fact_savings_df = (
    add_date_key(
        dataframe=savings_source_df,
        source_date_column="ProjectCreatedDate",
        output_key_column="ProjectCreatedDateKey"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve PlannedStartDateKey**

# CELL ********************

fact_savings_df = (
    add_date_key(
        dataframe=fact_savings_df,
        source_date_column="PlannedStartDate",
        output_key_column="PlannedStartDateKey"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve PlannedCompletionDateKey**

# CELL ********************

fact_savings_df = (
    add_date_key(
        dataframe=fact_savings_df,
        source_date_column="PlannedCompletionDate",
        output_key_column="PlannedCompletionDateKey"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve ActualCompletionDateKey**
# 
# _ActualCompletionDate_ is null for projects that are not implemented yet.

# CELL ********************

fact_savings_df = (
    add_date_key(
        dataframe=fact_savings_df,
        source_date_column="ActualCompletionDate",
        output_key_column="ActualCompletionDateKey"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve CancellationDateKey**
# 
# Only populated for cancelled projects.

# CELL ********************

fact_savings_df = (
    add_date_key(
        dataframe=fact_savings_df,
        source_date_column="CancellationDate",
        output_key_column="CancellationDateKey"
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
# For a savings project, we attribute supplier history according to:
# 
# SupplierID
# +
# ProjectCreatedDate

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

# **Resolve _SupplierKey_ by _ProjectCreatedDate_**

# CELL ********************

fact_savings_df = (
    fact_savings_df.alias(
        "saving"
    )

    .join(
        supplier_dimension_reference_df.alias(
            "supplier"
        ),

        (
            F.col(
                "saving.SupplierID"
            )
            ==
            F.col(
                "supplier.DimSupplierID"
            )
        )
        &
        (
            F.col(
                "saving.ProjectCreatedDate"
            )
            >=
            F.col(
                "supplier.EffectiveFromDate"
            )
        )
        &
        (
            F.col(
                "saving.ProjectCreatedDate"
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
            "saving.*"
        ),

        F.col(
            "supplier.SupplierKey"
        ).alias(
            "SupplierKey"
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

# **Validate Supplier SCD2 join does not duplicate projects**

# CELL ********************

supplier_join_duplicate_count = (
    fact_savings_df

    .groupBy(
        "SavingsProjectID"
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
    "Savings projects duplicated by "
    "Supplier SCD join:",
    supplier_join_duplicate_count
)


assert (
    supplier_join_duplicate_count == 0
), (
    "Supplier SCD Type 2 join created "
    "duplicate SavingsProjectID rows."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Category dimension**

# CELL ********************

category_reference_df = (
    dim_category_df

    .select(
        "CategoryID",
        "CategoryKey"
    )
)


fact_savings_df = (
    fact_savings_df

    .join(
        category_reference_df,
        "CategoryID",
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Buyer dimension**

# CELL ********************

buyer_reference_df = (
    dim_buyer_df

    .select(
        "BuyerID",
        "BuyerKey"
    )
)


fact_savings_df = (
    fact_savings_df

    .join(
        buyer_reference_df,
        "BuyerID",
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Business Unit dimension**

# CELL ********************

business_unit_reference_df = (
    dim_business_unit_df

    .select(
        "BusinessUnitID",
        "BusinessUnitKey"
    )
)


fact_savings_df = (
    fact_savings_df

    .join(
        business_unit_reference_df,
        "BusinessUnitID",
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Contract dimension**
# 
# Contract is optional for savings projects.

# CELL ********************

contract_reference_df = (
    dim_contract_df

    .select(
        "ContractID",
        "ContractKey"
    )
)


fact_savings_df = (
    fact_savings_df

    .join(
        contract_reference_df,
        "ContractID",
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Currency dimension**

# CELL ********************

currency_reference_df = (
    dim_currency_df

    .select(
        "CurrencyCode",
        "CurrencyKey"
    )
)


fact_savings_df = (
    fact_savings_df

    .join(
        currency_reference_df,
        "CurrencyCode",
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add Savings Fact key**

# CELL ********************

fact_savings_df = (
    fact_savings_df

    .withColumn(
        "SavingsFactKey",

        F.xxhash64(
            F.col(
                "SavingsProjectID"
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

# **Add additive project-count measures**

# CELL ********************

fact_savings_df = (
    fact_savings_df

    .withColumn(
        "ProjectCount",
        F.lit(1).cast("long")
    )

    .withColumn(
        "ActivePipelineProjectCount",

        F.when(
            F.col(
                "ActivePipelineFlag"
            ),
            F.lit(1)
        )

        .otherwise(
            F.lit(0)
        )

        .cast("long")
    )

    .withColumn(
        "ImplementedProjectCount",

        F.when(
            F.col(
                "IsImplementedFlag"
            ),
            F.lit(1)
        )

        .otherwise(
            F.lit(0)
        )

        .cast("long")
    )

    .withColumn(
        "OverdueProjectCount",

        F.when(
            F.col(
                "OverdueProjectFlag"
            ),
            F.lit(1)
        )

        .otherwise(
            F.lit(0)
        )

        .cast("long")
    )

    .withColumn(
        "CancelledProjectCount",

        F.when(
            F.col(
                "IsCancelledFlag"
            ),
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
    in fact_savings_df.columns
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

fact_savings_df = (
    fact_savings_df

    .withColumn(
        "GoldSourceTable",
        F.lit(
            SILVER_SAVINGS_TABLE
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

# **Inspect dimension-key resolution**

# CELL ********************

display(
    fact_savings_df

    .select(
        "SavingsProjectID",

        "ProjectCreatedDate",
        "ProjectCreatedDateKey",

        "PlannedStartDate",
        "PlannedStartDateKey",

        "PlannedCompletionDate",
        "PlannedCompletionDateKey",

        "ActualCompletionDate",
        "ActualCompletionDateKey",

        "CancellationDate",
        "CancellationDateKey",

        "SupplierID",
        "SupplierKey",
        "SupplierDimensionVersion",

        "CategoryID",
        "CategoryKey",

        "BuyerID",
        "BuyerKey",

        "BusinessUnitID",
        "BusinessUnitKey",

        "ContractID",
        "ContractKey",

        "CurrencyCode",
        "CurrencyKey"
    )

    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect savings measures**

# CELL ********************

display(
    fact_savings_df

    .select(
        "SavingsProjectID",
        "SavingsProjectName",

        "ProjectStatus",
        "SavingsLevel",

        "CurrencyCode",

        "BaselineSpend",
        "ForecastedSavings",
        "WeightedForecastSavings",
        "ApprovedSavings",
        "RealizedSavings",

        "BaselineSpendEUR",
        "ForecastedSavingsEUR",
        "WeightedForecastSavingsEUR",
        "ApprovedSavingsEUR",
        "RealizedSavingsEUR",

        "SavingsConfidenceWeight",

        "ActivePipelineWeightedForecastEUR",
        "ImplementedSavingsEUR"
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
                FACT_SAVINGS_TABLE,

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

gold_savings_project_count = (
    fact_savings_df.count()
)


register_validation(
    "Row Count",

    (
        "One Gold fact row exists "
        "per Silver savings project"
    ),

    abs(
        source_savings_project_count
        -
        gold_savings_project_count
    ),

    (
        f"Silver: "
        f"{source_savings_project_count:,}; "
        f"Gold: "
        f"{gold_savings_project_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate natural fact grain**

# CELL ********************

duplicate_savings_project_count = (
    fact_savings_df

    .groupBy(
        "SavingsProjectID"
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

    "SavingsProjectID is unique",

    duplicate_savings_project_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate SavingsFactKey**

# CELL ********************

duplicate_fact_key_count = (
    fact_savings_df

    .groupBy(
        "SavingsFactKey"
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
        "SavingsFactKey is unique"
    ),

    duplicate_fact_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate mandatory dimension keys**

# CELL ********************

# ============================================================
# Validate Mandatory Gold Dimension and Date Keys
# ============================================================

mandatory_key_checks = [
    (
        "ProjectCreatedDateKey",
        "ProjectCreatedDate resolves to dim_date"
    ),

    (
        "PlannedStartDateKey",
        "PlannedStartDate resolves to dim_date"
    ),

    (
        "PlannedCompletionDateKey",
        "PlannedCompletionDate resolves to dim_date"
    ),

    (
        "SupplierKey",
        (
            "Supplier resolves to the "
            "historically valid dim_supplier version"
        )
    ),

    (
        "CategoryKey",
        "Category resolves to dim_category"
    ),

    (
        "BuyerKey",
        "Buyer resolves to dim_buyer"
    ),

    (
        "BusinessUnitKey",
        (
            "Business Unit resolves to "
            "dim_business_unit"
        )
    ),

    (
        "CurrencyKey",
        "Currency resolves to dim_currency"
    )
]


for (
    key_column,
    validation_rule
) in mandatory_key_checks:

    missing_key_count = (
        fact_savings_df

        .filter(
            F.col(
                key_column
            ).isNull()
        )

        .count()
    )


    register_validation(
        "Referential Integrity",
        validation_rule,
        missing_key_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Diagnose Unresolved Mandatory Gold Keys
# ============================================================

unresolved_savings_keys_df = (
    fact_savings_df

    .filter(
        F.col(
            "ProjectCreatedDateKey"
        ).isNull()
        |
        F.col(
            "PlannedStartDateKey"
        ).isNull()
        |
        F.col(
            "PlannedCompletionDateKey"
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
            "BuyerKey"
        ).isNull()
        |
        F.col(
            "BusinessUnitKey"
        ).isNull()
        |
        F.col(
            "CurrencyKey"
        ).isNull()
    )

    .select(
        "SavingsProjectID",
        "SavingsProjectName",

        "ProjectStatus",
        "SavingsLevel",

        # ----------------------------------------------------
        # Date relationships
        # ----------------------------------------------------

        "ProjectCreatedDate",
        "ProjectCreatedDateKey",

        "PlannedStartDate",
        "PlannedStartDateKey",

        "PlannedCompletionDate",
        "PlannedCompletionDateKey",

        # ----------------------------------------------------
        # Supplier
        # ----------------------------------------------------

        "SupplierID",
        "SupplierKey",

        # ----------------------------------------------------
        # Category
        # ----------------------------------------------------

        "CategoryID",
        "CategoryKey",

        # ----------------------------------------------------
        # Buyer
        # ----------------------------------------------------

        "BuyerID",
        "BuyerKey",

        # ----------------------------------------------------
        # Business Unit
        # ----------------------------------------------------

        "BusinessUnitID",
        "BusinessUnitKey",

        # ----------------------------------------------------
        # Currency
        # ----------------------------------------------------

        "CurrencyCode",
        "CurrencyKey"
    )
)


print(
    "Savings projects with unresolved "
    "mandatory Gold keys:",
    unresolved_savings_keys_df.count()
)


display(
    unresolved_savings_keys_df

    .orderBy(
        "SavingsProjectID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Compare Savings Dates with Gold Date Dimension Coverage
# ============================================================

gold_date_range_df = (
    dim_date_df

    .agg(
        F.min(
            "Date"
        ).alias(
            "GoldDateStart"
        ),

        F.max(
            "Date"
        ).alias(
            "GoldDateEnd"
        )
    )
)


savings_date_range_df = (
    fact_savings_df

    .agg(
        F.min(
            "ProjectCreatedDate"
        ).alias(
            "MinProjectCreatedDate"
        ),

        F.max(
            "ProjectCreatedDate"
        ).alias(
            "MaxProjectCreatedDate"
        ),

        F.min(
            "PlannedStartDate"
        ).alias(
            "MinPlannedStartDate"
        ),

        F.max(
            "PlannedStartDate"
        ).alias(
            "MaxPlannedStartDate"
        ),

        F.min(
            "PlannedCompletionDate"
        ).alias(
            "MinPlannedCompletionDate"
        ),

        F.max(
            "PlannedCompletionDate"
        ).alias(
            "MaxPlannedCompletionDate"
        )
    )
)


print(
    "Gold Date dimension coverage:"
)

display(
    gold_date_range_df
)


print(
    "Savings project date coverage:"
)

display(
    savings_date_range_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate optional completion date key**

# CELL ********************

actual_completion_date_key_error_count = (
    fact_savings_df

    .filter(
        F.col(
            "ActualCompletionDate"
        ).isNotNull()
        &
        F.col(
            "ActualCompletionDateKey"
        ).isNull()
    )

    .count()
)


register_validation(
    "Date Dimension",

    (
        "ActualCompletionDate resolves "
        "when populated"
    ),

    actual_completion_date_key_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate optional cancellation date key**

# CELL ********************

cancellation_date_key_error_count = (
    fact_savings_df

    .filter(
        F.col(
            "CancellationDate"
        ).isNotNull()
        &
        F.col(
            "CancellationDateKey"
        ).isNull()
    )

    .count()
)


register_validation(
    "Date Dimension",

    (
        "CancellationDate resolves "
        "when populated"
    ),

    cancellation_date_key_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate optional ContractKey**

# CELL ********************

unresolved_contract_key_count = (
    fact_savings_df

    .filter(
        F.col(
            "ContractID"
        ).isNotNull()
        &
        F.col(
            "ContractKey"
        ).isNull()
    )

    .count()
)


register_validation(
    "Referential Integrity",

    (
        "Referenced ContractID values "
        "resolve to ContractKey"
    ),

    unresolved_contract_key_count
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
    fact_savings_df

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
        "Every savings project resolves "
        "to the Supplier version valid "
        "on ProjectCreatedDate"
    ),

    unresolved_supplier_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate ProjectCreatedDateKey**

# CELL ********************

project_created_date_key_error_count = (
    fact_savings_df

    .filter(
        F.col(
            "ProjectCreatedDateKey"
        )
        !=
        F.date_format(
            "ProjectCreatedDate",
            "yyyyMMdd"
        ).cast("int")
    )

    .count()
)


register_validation(
    "Date Dimension",

    (
        "ProjectCreatedDateKey corresponds "
        "to ProjectCreatedDate"
    ),

    project_created_date_key_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate weighted forecast logic**

# CELL ********************

weighted_forecast_error_count = (
    fact_savings_df

    .filter(
        F.abs(
            F.col(
                "WeightedForecastSavingsEUR"
            )
            -
            F.round(
                F.col(
                    "ForecastedSavingsEUR"
                )
                *
                F.col(
                    "SavingsConfidenceWeight"
                ),
                2
            )
        )
        >
        F.lit(0.02)
    )

    .count()
)


register_validation(
    "Savings Logic",

    (
        "WeightedForecastSavingsEUR "
        "matches forecast times "
        "confidence weight"
    ),

    weighted_forecast_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validae active pipeline amount**

# CELL ********************

active_pipeline_amount_error_count = (
    fact_savings_df

    .filter(
        F.abs(
            F.col(
                "ActivePipelineWeightedForecastEUR"
            )
            -
            F.when(
                F.col(
                    "ActivePipelineFlag"
                ),

                F.col(
                    "WeightedForecastSavingsEUR"
                )
            )

            .otherwise(
                F.lit(0)
            )
        )
        >
        F.lit(0.02)
    )

    .count()
)


register_validation(
    "Savings Logic",

    (
        "Active pipeline weighted amount "
        "agrees with lifecycle status"
    ),

    active_pipeline_amount_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate implemented savings amount**

# CELL ********************

implemented_savings_error_count = (
    fact_savings_df

    .filter(
        F.abs(
            F.col(
                "ImplementedSavingsEUR"
            )
            -
            F.when(
                F.col(
                    "IsImplementedFlag"
                ),

                F.col(
                    "RealizedSavingsEUR"
                )
            )

            .otherwise(
                F.lit(0)
            )
        )
        >
        F.lit(0.02)
    )

    .count()
)


register_validation(
    "Savings Logic",

    (
        "ImplementedSavingsEUR agrees "
        "with implemented project status"
    ),

    implemented_savings_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate project-count measures**

# CELL ********************

project_count_error_count = (
    fact_savings_df

    .filter(
        (
            F.col(
                "ProjectCount"
            )
            != 1
        )
        |
        (
            F.col(
                "ActivePipelineProjectCount"
            )
            !=
            F.col(
                "ActivePipelineFlag"
            ).cast("int")
        )
        |
        (
            F.col(
                "ImplementedProjectCount"
            )
            !=
            F.col(
                "IsImplementedFlag"
            ).cast("int")
        )
        |
        (
            F.col(
                "OverdueProjectCount"
            )
            !=
            F.col(
                "OverdueProjectFlag"
            ).cast("int")
        )
        |
        (
            F.col(
                "CancelledProjectCount"
            )
            !=
            F.col(
                "IsCancelledFlag"
            ).cast("int")
        )
    )

    .count()
)


register_validation(
    "Additive Measures",

    (
        "Gold project-count measures "
        "agree with project flags"
    ),

    project_count_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate savings monetary values**

# CELL ********************

invalid_savings_amount_count = (
    fact_savings_df

    .filter(
        F.col(
            "BaselineSpendEUR"
        ).isNull()
        |
        F.col(
            "ForecastedSavingsEUR"
        ).isNull()
        |
        (
            F.col(
                "BaselineSpendEUR"
            )
            < 0
        )
        |
        (
            F.col(
                "ForecastedSavingsEUR"
            )
            < 0
        )
        |
        (
            F.coalesce(
                F.col(
                    "ApprovedSavingsEUR"
                ),
                F.lit(0)
            )
            < 0
        )
        |
        (
            F.coalesce(
                F.col(
                    "RealizedSavingsEUR"
                ),
                F.lit(0)
            )
            < 0
        )
    )

    .count()
)


register_validation(
    "Monetary Values",

    (
        "Gold EUR savings amounts "
        "are valid and non-negative"
    ),

    invalid_savings_amount_count
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
    fact_savings_df

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
    "Gold Savings Fact "
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

    .format("delta")

    .mode("overwrite")

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
        f"Gold Savings Fact "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "GOLD SAVINGS FACT "
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
    fact_savings_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        FACT_SAVINGS_TABLE
    )
)


print(
    "Created physical Gold fact:",
    FACT_SAVINGS_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Veriy persisted row count**

# CELL ********************

persisted_fact_savings_df = (
    spark.table(
        FACT_SAVINGS_TABLE
    )
)


persisted_savings_count = (
    persisted_fact_savings_df.count()
)


print(
    "Expected rows:",
    f"{source_savings_project_count:,}"
)

print(
    "Persisted rows:",
    f"{persisted_savings_count:,}"
)


assert (
    persisted_savings_count
    ==
    source_savings_project_count
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

# **Savings pipeline by status**

# CELL ********************

display(
    persisted_fact_savings_df

    .groupBy(
        "ProjectStatus",
        "SavingsLevel"
    )

    .agg(
        F.sum(
            "ProjectCount"
        ).alias(
            "ProjectCount"
        ),

        F.round(
            F.sum(
                "ForecastedSavingsEUR"
            ),
            2
        ).alias(
            "ForecastedSavingsEUR"
        ),

        F.round(
            F.sum(
                "WeightedForecastSavingsEUR"
            ),
            2
        ).alias(
            "WeightedForecastSavingsEUR"
        ),

        F.round(
            F.sum(
                "ActivePipelineWeightedForecastEUR"
            ),
            2
        ).alias(
            "ActivePipelineWeightedForecastEUR"
        ),

        F.round(
            F.sum(
                "ImplementedSavingsEUR"
            ),
            2
        ).alias(
            "ImplementedSavingsEUR"
        )
    )

    .orderBy(
        "ProjectStatus",
        "SavingsLevel"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Annual savings pipeline**
# 
# Use _PlannedStartDate_ as the planning-year view.

# CELL ********************

annual_pipeline_df = (
    persisted_fact_savings_df

    .join(
        dim_date_df

        .select(
            F.col(
                "DateKey"
            ).alias(
                "RefPlannedStartDateKey"
            ),

            "Year"
        ),

        F.col(
            "PlannedStartDateKey"
        )
        ==
        F.col(
            "RefPlannedStartDateKey"
        ),

        "left"
    )

    .groupBy(
        "Year"
    )

    .agg(
        F.sum(
            "ProjectCount"
        ).alias(
            "ProjectCount"
        ),

        F.sum(
            "ActivePipelineProjectCount"
        ).alias(
            "ActivePipelineProjectCount"
        ),

        F.sum(
            "ImplementedProjectCount"
        ).alias(
            "ImplementedProjectCount"
        ),

        F.sum(
            "OverdueProjectCount"
        ).alias(
            "OverdueProjectCount"
        ),

        F.round(
            F.sum(
                "ForecastedSavingsEUR"
            ),
            2
        ).alias(
            "ForecastedSavingsEUR"
        ),

        F.round(
            F.sum(
                "WeightedForecastSavingsEUR"
            ),
            2
        ).alias(
            "WeightedForecastSavingsEUR"
        ),

        F.round(
            F.sum(
                "ActivePipelineWeightedForecastEUR"
            ),
            2
        ).alias(
            "ActivePipelineWeightedForecastEUR"
        ),

        F.round(
            F.sum(
                "ImplementedSavingsEUR"
            ),
            2
        ).alias(
            "ImplementedSavingsEUR"
        )
    )

    .orderBy(
        "Year"
    )
)


display(
    annual_pipeline_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Realized savings by completion year**

# CELL ********************

realized_savings_by_year_df = (
    persisted_fact_savings_df

    .filter(
        F.col(
            "ActualCompletionDateKey"
        ).isNotNull()
    )

    .join(
        dim_date_df

        .select(
            F.col(
                "DateKey"
            ).alias(
                "RefActualCompletionDateKey"
            ),

            "Year"
        ),

        F.col(
            "ActualCompletionDateKey"
        )
        ==
        F.col(
            "RefActualCompletionDateKey"
        ),

        "left"
    )

    .groupBy(
        "Year"
    )

    .agg(
        F.sum(
            "ImplementedProjectCount"
        ).alias(
            "ImplementedProjectCount"
        ),

        F.round(
            F.sum(
                "RealizedSavingsEUR"
            ),
            2
        ).alias(
            "RealizedSavingsEUR"
        ),

        F.round(
            F.sum(
                "ImplementedSavingsEUR"
            ),
            2
        ).alias(
            "ImplementedSavingsEUR"
        )
    )

    .orderBy(
        "Year"
    )
)


display(
    realized_savings_by_year_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Category savings preview**

# CELL ********************

category_savings_df = (
    persisted_fact_savings_df

    .groupBy(
        "CategoryKey"
    )

    .agg(
        F.sum(
            "ProjectCount"
        ).alias(
            "ProjectCount"
        ),

        F.sum(
            "ActivePipelineProjectCount"
        ).alias(
            "ActivePipelineProjectCount"
        ),

        F.round(
            F.sum(
                "ActivePipelineWeightedForecastEUR"
            ),
            2
        ).alias(
            "WeightedPipelineEUR"
        ),

        F.round(
            F.sum(
                "ImplementedSavingsEUR"
            ),
            2
        ).alias(
            "ImplementedSavingsEUR"
        )
    )

    .join(
        dim_category_df

        .select(
            "CategoryKey",
            "CategoryID",
            "CategoryName",
            "ProcurementType"
        ),

        "CategoryKey",

        "left"
    )

    .orderBy(
        F.desc(
            "WeightedPipelineEUR"
        )
    )
)


display(
    category_savings_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Supplier savings preview**

# CELL ********************

supplier_savings_df = (
    persisted_fact_savings_df

    .groupBy(
        "SupplierKey"
    )

    .agg(
        F.sum(
            "ProjectCount"
        ).alias(
            "ProjectCount"
        ),

        F.sum(
            "ActivePipelineProjectCount"
        ).alias(
            "ActivePipelineProjectCount"
        ),

        F.sum(
            "OverdueProjectCount"
        ).alias(
            "OverdueProjectCount"
        ),

        F.round(
            F.sum(
                "ActivePipelineWeightedForecastEUR"
            ),
            2
        ).alias(
            "WeightedPipelineEUR"
        ),

        F.round(
            F.sum(
                "ImplementedSavingsEUR"
            ),
            2
        ).alias(
            "ImplementedSavingsEUR"
        )
    )

    .join(
        dim_supplier_df

        .select(
            "SupplierKey",
            "SupplierID",
            "SupplierName",
            "DimensionVersion"
        ),

        "SupplierKey",

        "left"
    )

    .orderBy(
        F.desc(
            "WeightedPipelineEUR"
        )
    )
)


display(
    supplier_savings_df

    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Overdue savings projects**

# CELL ********************

display(
    persisted_fact_savings_df

    .filter(
        F.col(
            "OverdueProjectFlag"
        )
    )

    .select(
        "SavingsProjectID",
        "SavingsProjectName",

        "SupplierKey",
        "CategoryKey",
        "BuyerKey",

        "ProjectStatus",
        "SavingsLevel",

        "PlannedCompletionDate",
        "DaysPastPlannedCompletion",

        "ForecastedSavingsEUR",
        "WeightedForecastSavingsEUR",
        "ActivePipelineWeightedForecastEUR"
    )

    .orderBy(
        F.desc(
            "DaysPastPlannedCompletion"
        )
    )

    .limit(100)
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
    persisted_fact_savings_df

    .select(
        "SavingsFactKey",

        "SavingsProjectID",
        "SavingsProjectName",

        "SupplierKey",
        "SupplierDimensionVersion",

        "CategoryKey",
        "BuyerKey",
        "BusinessUnitKey",
        "ContractKey",
        "CurrencyKey",

        "ProjectCreatedDateKey",
        "PlannedStartDateKey",
        "PlannedCompletionDateKey",
        "ActualCompletionDateKey",
        "CancellationDateKey",

        "SavingsType",
        "ProjectStatus",
        "SavingsLevel",
        "ApprovalStatus",

        "CurrencyCode",

        "BaselineSpendEUR",
        "ForecastedSavingsEUR",
        "WeightedForecastSavingsEUR",
        "ApprovedSavingsEUR",
        "RealizedSavingsEUR",

        "SavingsConfidenceWeight",
        "SavingsAchievementPct",

        "ActivePipelineFlag",
        "OverdueProjectFlag",

        "ActivePipelineWeightedForecastEUR",
        "ImplementedSavingsEUR",

        "ProjectCount",
        "ActivePipelineProjectCount",
        "ImplementedProjectCount",
        "OverdueProjectCount",
        "CancelledProjectCount",

        "RecurringSavingsFlag"
    )

    .orderBy(
        F.desc(
            "WeightedForecastSavingsEUR"
        )
    )

    .limit(100)
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
    "NB_33_Build_Gold_Fact_Savings "
    "completed successfully."
)

print()

print(
    "Physical Gold output:"
)

print(
    "  fact_savings"
)

print()

print(
    "Grain:"
)

print(
    "  One row per SavingsProjectID"
)

print()

print(
    "Date role keys:"
)

print(
    "  - ProjectCreatedDateKey"
)

print(
    "  - PlannedStartDateKey"
)

print(
    "  - PlannedCompletionDateKey"
)

print(
    "  - ActualCompletionDateKey"
)

print(
    "  - CancellationDateKey"
)

print()

print(
    "Conformed dimension keys:"
)

print(
    "  - SupplierKey "
    "(SCD Type 2 by ProjectCreatedDate)"
)

print(
    "  - CategoryKey"
)

print(
    "  - BuyerKey"
)

print(
    "  - BusinessUnitKey"
)

print(
    "  - ContractKey"
)

print(
    "  - CurrencyKey"
)

print()

print(
    "Primary savings measures:"
)

print(
    "  - BaselineSpendEUR"
)

print(
    "  - ForecastedSavingsEUR"
)

print(
    "  - WeightedForecastSavingsEUR"
)

print(
    "  - ApprovedSavingsEUR"
)

print(
    "  - RealizedSavingsEUR"
)

print(
    "  - ActivePipelineWeightedForecastEUR"
)

print(
    "  - ImplementedSavingsEUR"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
