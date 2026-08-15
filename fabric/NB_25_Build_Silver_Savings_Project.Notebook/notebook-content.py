# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0b90ff24-5060-4c76-becc-1337f3f00f28",
# META       "default_lakehouse_name": "lh_procurement_silver",
# META       "default_lakehouse_workspace_id": "83e05aab-2eed-49cb-a339-674db19d4b92",
# META       "known_lakehouses": [
# META         {
# META           "id": "0b90ff24-5060-4c76-becc-1337f3f00f28"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Output: _silver_savings_project_
# 
# Grain: one row per _SavingsProjectID_

# MARKDOWN ********************

# **Configuration**

# CELL ********************

from datetime import date

AS_OF_DATE = date(2026, 7, 31)

BRONZE_SAVINGS_TABLE = (
    "bronze_savings_project"
)

BRONZE_MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)

SILVER_SUPPLIER_TABLE = (
    "silver_supplier"
)

SILVER_CATEGORY_TABLE = (
    "silver_category"
)

SILVER_BUYER_TABLE = (
    "silver_buyer"
)

SILVER_BUSINESS_UNIT_TABLE = (
    "silver_business_unit"
)

SILVER_CONTRACT_TABLE = (
    "silver_contract"
)

SILVER_EXCHANGE_RATE_TABLE = (
    "silver_exchange_rate"
)

SILVER_SAVINGS_TABLE = (
    "silver_savings_project"
)

SILVER_MONITORING_TABLE = (
    "monitoring_silver_savings_project_quality_results"
)

print(
    "Notebook: NB_25_Build_Silver_Savings_Project"
)

print(
    "Default Lakehouse: lh_procurement_silver"
)

print(
    f"As-of date: {AS_OF_DATE}"
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
from pyspark.sql.window import Window

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate prerequisite tables**

# CELL ********************

required_tables = [
    BRONZE_SAVINGS_TABLE,
    BRONZE_MONITORING_TABLE,
    SILVER_SUPPLIER_TABLE,
    SILVER_CATEGORY_TABLE,
    SILVER_BUYER_TABLE,
    SILVER_BUSINESS_UNIT_TABLE,
    SILVER_CONTRACT_TABLE,
    SILVER_EXCHANGE_RATE_TABLE
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
        "Missing required tables: "
        + ", ".join(
            missing_tables
        )
    )

print(
    "All required Bronze shortcuts "
    "and Silver tables exist."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Confirm Bronze quality gate**

# CELL ********************

bronze_failure_count = (
    spark.table(
        BRONZE_MONITORING_TABLE
    )
    .filter(
        (
            F.col("Severity")
            == "ERROR"
        )
        &
        (
            F.col("ValidationStatus")
            == "FAILED"
        )
    )
    .count()
)

print(
    "Bronze critical failures:",
    bronze_failure_count
)

assert (
    bronze_failure_count == 0
), (
    "Bronze quality gate has not passed."
)

print(
    "Bronze quality gate confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load source and reference tables**

# CELL ********************

savings_project_df = spark.table(
    BRONZE_SAVINGS_TABLE
)

silver_supplier_df = spark.table(
    SILVER_SUPPLIER_TABLE
)

silver_category_df = spark.table(
    SILVER_CATEGORY_TABLE
)

silver_buyer_df = spark.table(
    SILVER_BUYER_TABLE
)

silver_business_unit_df = spark.table(
    SILVER_BUSINESS_UNIT_TABLE
)

silver_contract_df = spark.table(
    SILVER_CONTRACT_TABLE
)

silver_exchange_rate_df = spark.table(
    SILVER_EXCHANGE_RATE_TABLE
)

print(
    "Savings projects:",
    f"{savings_project_df.count():,}"
)

print(
    "Suppliers:",
    f"{silver_supplier_df.count():,}"
)

print(
    "Contracts:",
    f"{silver_contract_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Normalize Bronze savings fields**

# CELL ********************

silver_savings_project_df = (
    savings_project_df

    .withColumn(
        "SavingsProjectID",
        F.upper(
            F.trim(
                F.col(
                    "SavingsProjectID"
                )
            )
        )
    )

    .withColumn(
        "SavingsProjectName",
        F.trim(
            F.regexp_replace(
                F.col(
                    "SavingsProjectName"
                ),
                r"\s+",
                " "
            )
        )
    )

    .withColumn(
        "SupplierID",
        F.upper(
            F.trim(
                F.col(
                    "SupplierID"
                )
            )
        )
    )

    .withColumn(
        "CategoryID",
        F.upper(
            F.trim(
                F.col(
                    "CategoryID"
                )
            )
        )
    )

    .withColumn(
        "BuyerID",
        F.upper(
            F.trim(
                F.col(
                    "BuyerID"
                )
            )
        )
    )

    .withColumn(
        "BusinessUnitID",
        F.upper(
            F.trim(
                F.col(
                    "BusinessUnitID"
                )
            )
        )
    )

    .withColumn(
        "ContractID",
        F.when(
            F.trim(
                F.col(
                    "ContractID"
                )
            ) == "",
            F.lit(None)
        ).otherwise(
            F.upper(
                F.trim(
                    F.col(
                        "ContractID"
                    )
                )
            )
        )
    )

    .withColumn(
        "Currency",
        F.upper(
            F.trim(
                F.col(
                    "Currency"
                )
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

# **Cast dates and amounts**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ProjectCreatedDate",
        F.to_date(
            "ProjectCreatedDate"
        )
    )

    .withColumn(
        "PlannedStartDate",
        F.to_date(
            "PlannedStartDate"
        )
    )

    .withColumn(
        "PlannedCompletionDate",
        F.to_date(
            "PlannedCompletionDate"
        )
    )

    .withColumn(
        "ActualCompletionDate",
        F.to_date(
            "ActualCompletionDate"
        )
    )

    .withColumn(
        "CancellationDate",
        F.to_date(
            "CancellationDate"
        )
    )

    .withColumn(
        "BaselineSpend",
        F.col(
            "BaselineSpend"
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "ForecastedSavings",
        F.col(
            "ForecastedSavings"
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "ApprovedSavings",
        F.col(
            "ApprovedSavings"
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "RealizedSavings",
        F.col(
            "RealizedSavings"
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "RecurringSavingsFlag",
        F.col(
            "RecurringSavingsFlag"
        ).cast("boolean")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Standardize status fields**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ProjectStatus",
        F.initcap(
            F.lower(
                F.trim(
                    F.col(
                        "ProjectStatus"
                    )
                )
            )
        )
    )

    .withColumn(
        "SavingsType",
        F.initcap(
            F.lower(
                F.trim(
                    F.col(
                        "SavingsType"
                    )
                )
            )
        )
    )

    .withColumn(
        "ApprovalStatus",
        F.initcap(
            F.lower(
                F.trim(
                    F.col(
                        "ApprovalStatus"
                    )
                )
            )
        )
    )

    .withColumn(
        "SavingsLevel",
        F.upper(
            F.trim(
                F.col(
                    "SavingsLevel"
                )
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

# **Deduplicate SavingsProjectID**

# CELL ********************

dedupe_window = (
    Window
    .partitionBy(
        "SavingsProjectID"
    )
    .orderBy(
        F.col(
            "IngestionTimestamp"
        ).desc_nulls_last(),
        F.col(
            "SourceExtractDate"
        ).desc_nulls_last()
    )
)

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "_RowNumber",
        F.row_number().over(
            dedupe_window
        )
    )

    .filter(
        F.col("_RowNumber") == 1
    )

    .drop(
        "_RowNumber"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Rename Bronze lineage fields**

# CELL ********************

BRONZE_AUDIT_RENAME = {
    "SourceSystem":
        "BronzeSourceSystem",

    "SourceExtractDate":
        "BronzeSourceExtractDate",

    "IngestionTimestamp":
        "BronzeIngestionTimestamp",

    "LoadDate":
        "BronzeLoadDate",

    "SourceRecordHash":
        "BronzeSourceRecordHash"
}

for (
    source_column,
    target_column
) in BRONZE_AUDIT_RENAME.items():

    if (
        source_column
        in silver_savings_project_df.columns
    ):

        silver_savings_project_df = (
            silver_savings_project_df
            .withColumnRenamed(
                source_column,
                target_column
            )
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Enrich supplier**

# CELL ********************

supplier_reference_df = (
    silver_supplier_df
    .select(
        F.col(
            "SupplierID"
        ).alias(
            "RefSupplierID"
        ),

        "SupplierName",
        "SupplierType",
        "Country",
        "Region",
        "PreferredSupplier",
        "StrategicSupplier",
        "SupplierActiveFlag",
        "ESGRating",
        "FinancialRiskScore"
    )
)

silver_savings_project_df = (
    silver_savings_project_df

    .join(
        supplier_reference_df,

        silver_savings_project_df[
            "SupplierID"
        ]
        ==
        supplier_reference_df[
            "RefSupplierID"
        ],

        "left"
    )

    .drop(
        "RefSupplierID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Enrich category**

# CELL ********************

category_reference_df = (
    silver_category_df
    .select(
        F.col(
            "CategoryID"
        ).alias(
            "RefCategoryID"
        ),

        "CategoryName",
        "ProcurementType"
    )
)

silver_savings_project_df = (
    silver_savings_project_df

    .join(
        category_reference_df,

        silver_savings_project_df[
            "CategoryID"
        ]
        ==
        category_reference_df[
            "RefCategoryID"
        ],

        "left"
    )

    .drop(
        "RefCategoryID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Enrich buyer**

# CELL ********************

buyer_reference_df = (
    silver_buyer_df
    .select(
        F.col(
            "BuyerID"
        ).alias(
            "RefBuyerID"
        ),

        "BuyerName",
        "BuyerRole",

        F.col(
            "BusinessUnitID"
        ).alias(
            "BuyerBusinessUnitID"
        )
    )
)

silver_savings_project_df = (
    silver_savings_project_df

    .join(
        buyer_reference_df,

        silver_savings_project_df[
            "BuyerID"
        ]
        ==
        buyer_reference_df[
            "RefBuyerID"
        ],

        "left"
    )

    .drop(
        "RefBuyerID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate business-unit reference**

# CELL ********************

business_unit_reference_df = (
    silver_business_unit_df

    .select(
        F.col(
            "BusinessUnitID"
        ).alias(
            "RefBusinessUnitID"
        )
    )

    .distinct()
)

silver_savings_project_df = (
    silver_savings_project_df

    .join(
        business_unit_reference_df,

        silver_savings_project_df[
            "BusinessUnitID"
        ]
        ==
        business_unit_reference_df[
            "RefBusinessUnitID"
        ],

        "left"
    )

    .withColumn(
        "BusinessUnitResolvedFlag",
        F.col(
            "RefBusinessUnitID"
        ).isNotNull()
    )

    .drop(
        "RefBusinessUnitID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare contract reference**

# CELL ********************

contract_reference_df = (
    silver_contract_df

    .select(
        F.col(
            "ContractID"
        ).alias(
            "RefContractID"
        ),

        F.col(
            "SupplierID"
        ).alias(
            "ContractSupplierID"
        ),

        F.col(
            "CategoryID"
        ).alias(
            "ContractCategoryID"
        ),

        "ContractStartDate",
        "ContractEndDate",
        "ContractType"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join contract**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .join(
        contract_reference_df,

        silver_savings_project_df[
            "ContractID"
        ]
        ==
        contract_reference_df[
            "RefContractID"
        ],

        "left"
    )

    .withColumn(
        "ContractReferenceFlag",
        F.col(
            "ContractID"
        ).isNotNull()
    )

    .withColumn(
        "ContractResolvedFlag",
        F.col(
            "RefContractID"
        ).isNotNull()
    )

    .drop(
        "RefContractID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Contract relationship checks**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ContractSupplierMatchFlag",

        F.when(
            F.col(
                "ContractResolvedFlag"
            ),

            F.col(
                "SupplierID"
            )
            ==
            F.col(
                "ContractSupplierID"
            )
        )
    )

    .withColumn(
        "ContractCategoryMatchFlag",

        F.when(
            F.col(
                "ContractResolvedFlag"
            ),

            F.col(
                "CategoryID"
            )
            ==
            F.col(
                "ContractCategoryID"
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

# **Derive project lifecycle flags**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "IsIdeaFlag",
        (
            F.col(
                "ProjectStatus"
            )
            == "Idea"
        )
    )

    .withColumn(
        "IsValidatedFlag",
        (
            F.col(
                "ProjectStatus"
            )
            == "Validated"
        )
    )

    .withColumn(
        "IsNegotiationFlag",
        (
            F.col(
                "ProjectStatus"
            )
            == "Negotiation"
        )
    )

    .withColumn(
        "IsImplementedFlag",
        (
            F.col(
                "ProjectStatus"
            )
            == "Implemented"
        )
    )

    .withColumn(
        "IsCancelledFlag",
        (
            F.col(
                "ProjectStatus"
            )
            == "Cancelled"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive active pipeline flag**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ActivePipelineFlag",

        F.col(
            "ProjectStatus"
        ).isin(
            "Idea",
            "Validated",
            "Negotiation"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive Overdue Flag and Project Duration Metrics**

# CELL ********************

# ============================================================
# Derive Overdue Flag and Project Duration Metrics
# ============================================================

silver_savings_project_df = (
    silver_savings_project_df

    # --------------------------------------------------------
    # Overdue project
    # --------------------------------------------------------

    .withColumn(
        "OverdueProjectFlag",

        (
            F.col(
                "ActivePipelineFlag"
            )
        )
        &
        (
            F.col(
                "PlannedCompletionDate"
            ).isNotNull()
        )
        &
        (
            F.col(
                "PlannedCompletionDate"
            )
            <
            F.lit(
                AS_OF_DATE
            )
        )
    )

    # --------------------------------------------------------
    # Planned project duration
    # --------------------------------------------------------

    .withColumn(
        "PlannedDurationDays",

        F.when(
            F.col(
                "PlannedCompletionDate"
            ).isNotNull()
            &
            F.col(
                "PlannedStartDate"
            ).isNotNull(),

            F.datediff(
                F.col(
                    "PlannedCompletionDate"
                ),
                F.col(
                    "PlannedStartDate"
                )
            )
            + F.lit(1)
        )
    )

    # --------------------------------------------------------
    # Actual project duration
    # --------------------------------------------------------

    .withColumn(
        "ActualDurationDays",

        F.when(
            F.col(
                "ActualCompletionDate"
            ).isNotNull()
            &
            F.col(
                "PlannedStartDate"
            ).isNotNull(),

            F.datediff(
                F.col(
                    "ActualCompletionDate"
                ),
                F.col(
                    "PlannedStartDate"
                )
            )
            + F.lit(1)
        )
    )

    # --------------------------------------------------------
    # Days overdue
    # --------------------------------------------------------

    .withColumn(
        "DaysPastPlannedCompletion",

        F.when(
            F.col(
                "OverdueProjectFlag"
            ),

            F.datediff(
                F.lit(
                    AS_OF_DATE
                ),
                F.col(
                    "PlannedCompletionDate"
                )
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

# **Derive savings confidence weight**
# 
# Project's maturity logic:
# - L0 = 0%
# - L1 = 25%
# - L2 = 50%
# - L3 = 100%

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "SavingsConfidenceWeight",

        F.when(
            F.col(
                "SavingsLevel"
            )
            == "L0",
            F.lit(0.00)
        )

        .when(
            F.col(
                "SavingsLevel"
            )
            == "L1",
            F.lit(0.25)
        )

        .when(
            F.col(
                "SavingsLevel"
            )
            == "L2",
            F.lit(0.50)
        )

        .when(
            F.col(
                "SavingsLevel"
            )
            == "L3",
            F.lit(1.00)
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive weighted forecast in source currency**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "WeightedForecastSavings",

        F.round(
            F.col(
                "ForecastedSavings"
            )
            *
            F.col(
                "SavingsConfidenceWeight"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare FX data**
# 
# Use the project-created date as the consistent valuation date for all project-level monetary values.

# CELL ********************

fx_base_df = (
    silver_exchange_rate_df

    .select(
        "RateDate",
        "Currency",
        "ExchangeRateToEUR"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Define FX resolution helper**

# CELL ********************

def add_fx_resolution(
    dataframe,
    date_column,
    currency_column,
    prefix
):

    exact_fx_df = (
        fx_base_df
        .select(
            F.col(
                "RateDate"
            ).alias(
                f"{prefix}ExactFXDate"
            ),

            F.col(
                "Currency"
            ).alias(
                f"{prefix}ExactFXCurrency"
            ),

            F.col(
                "ExchangeRateToEUR"
            ).alias(
                f"{prefix}ExactFXRate"
            )
        )
    )


    first_window = (
        Window
        .partitionBy(
            "Currency"
        )
        .orderBy(
            F.col(
                "RateDate"
            ).asc()
        )
    )


    first_fx_df = (
        fx_base_df

        .withColumn(
            "_rn",
            F.row_number().over(
                first_window
            )
        )

        .filter(
            F.col("_rn") == 1
        )

        .select(
            F.col(
                "Currency"
            ).alias(
                f"{prefix}FirstFXCurrency"
            ),

            F.col(
                "RateDate"
            ).alias(
                f"{prefix}FirstFXDate"
            ),

            F.col(
                "ExchangeRateToEUR"
            ).alias(
                f"{prefix}FirstFXRate"
            )
        )
    )


    last_window = (
        Window
        .partitionBy(
            "Currency"
        )
        .orderBy(
            F.col(
                "RateDate"
            ).desc()
        )
    )


    last_fx_df = (
        fx_base_df

        .withColumn(
            "_rn",
            F.row_number().over(
                last_window
            )
        )

        .filter(
            F.col("_rn") == 1
        )

        .select(
            F.col(
                "Currency"
            ).alias(
                f"{prefix}LastFXCurrency"
            ),

            F.col(
                "RateDate"
            ).alias(
                f"{prefix}LastFXDate"
            ),

            F.col(
                "ExchangeRateToEUR"
            ).alias(
                f"{prefix}LastFXRate"
            )
        )
    )


    result_df = (
        dataframe

        .join(
            exact_fx_df,

            (
                F.col(
                    date_column
                )
                ==
                F.col(
                    f"{prefix}ExactFXDate"
                )
            )
            &
            (
                F.col(
                    currency_column
                )
                ==
                F.col(
                    f"{prefix}ExactFXCurrency"
                )
            ),

            "left"
        )

        .join(
            first_fx_df,

            F.col(
                currency_column
            )
            ==
            F.col(
                f"{prefix}FirstFXCurrency"
            ),

            "left"
        )

        .join(
            last_fx_df,

            F.col(
                currency_column
            )
            ==
            F.col(
                f"{prefix}LastFXCurrency"
            ),

            "left"
        )
    )


    result_df = (
        result_df

        .withColumn(
            f"{prefix}FXResolutionMethod",

            F.when(
                F.col(
                    f"{prefix}ExactFXRate"
                ).isNotNull(),

                F.lit(
                    "EXACT_DATE"
                )
            )

            .when(
                F.col(
                    date_column
                )
                <
                F.col(
                    f"{prefix}FirstFXDate"
                ),

                F.lit(
                    "EARLIEST_AVAILABLE_RATE"
                )
            )

            .when(
                F.col(
                    date_column
                )
                >
                F.col(
                    f"{prefix}LastFXDate"
                ),

                F.lit(
                    "LATEST_AVAILABLE_RATE"
                )
            )

            .otherwise(
                F.lit(
                    "UNRESOLVED"
                )
            )
        )

        .withColumn(
            f"{prefix}ExchangeRateToEUR",

            F.when(
                F.col(
                    f"{prefix}ExactFXRate"
                ).isNotNull(),

                F.col(
                    f"{prefix}ExactFXRate"
                )
            )

            .when(
                F.col(
                    date_column
                )
                <
                F.col(
                    f"{prefix}FirstFXDate"
                ),

                F.col(
                    f"{prefix}FirstFXRate"
                )
            )

            .when(
                F.col(
                    date_column
                )
                >
                F.col(
                    f"{prefix}LastFXDate"
                ),

                F.col(
                    f"{prefix}LastFXRate"
                )
            )
        )

        .withColumn(
            f"{prefix}FXRateDate",

            F.when(
                F.col(
                    f"{prefix}ExactFXRate"
                ).isNotNull(),

                F.col(
                    f"{prefix}ExactFXDate"
                )
            )

            .when(
                F.col(
                    date_column
                )
                <
                F.col(
                    f"{prefix}FirstFXDate"
                ),

                F.col(
                    f"{prefix}FirstFXDate"
                )
            )

            .when(
                F.col(
                    date_column
                )
                >
                F.col(
                    f"{prefix}LastFXDate"
                ),

                F.col(
                    f"{prefix}LastFXDate"
                )
            )
        )
    )


    return (
        result_df

        .drop(
            f"{prefix}ExactFXDate",
            f"{prefix}ExactFXCurrency",
            f"{prefix}ExactFXRate",

            f"{prefix}FirstFXCurrency",
            f"{prefix}FirstFXDate",
            f"{prefix}FirstFXRate",

            f"{prefix}LastFXCurrency",
            f"{prefix}LastFXDate",
            f"{prefix}LastFXRate"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve project FX rate**

# CELL ********************

silver_savings_project_df = (
    add_fx_resolution(
        dataframe=(
            silver_savings_project_df
        ),
        date_column=(
            "ProjectCreatedDate"
        ),
        currency_column=(
            "Currency"
        ),
        prefix=(
            "Savings"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **COnvert savings values to EUR**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "BaselineSpendEUR",

        F.round(
            F.col(
                "BaselineSpend"
            )
            *
            F.col(
                "SavingsExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "ForecastedSavingsEUR",

        F.round(
            F.col(
                "ForecastedSavings"
            )
            *
            F.col(
                "SavingsExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "ApprovedSavingsEUR",

        F.round(
            F.col(
                "ApprovedSavings"
            )
            *
            F.col(
                "SavingsExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "RealizedSavingsEUR",

        F.round(
            F.col(
                "RealizedSavings"
            )
            *
            F.col(
                "SavingsExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "WeightedForecastSavingsEUR",

        F.round(
            F.col(
                "WeightedForecastSavings"
            )
            *
            F.col(
                "SavingsExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive savings rates**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ForecastSavingsPctOfBaseline",

        F.when(
            F.col(
                "BaselineSpendEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "ForecastedSavingsEUR"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "BaselineSpendEUR"
                ),
                2
            )
        )
    )

    .withColumn(
        "ApprovedSavingsPctOfBaseline",

        F.when(
            F.col(
                "BaselineSpendEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "ApprovedSavingsEUR"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "BaselineSpendEUR"
                ),
                2
            )
        )
    )

    .withColumn(
        "RealizedSavingsPctOfBaseline",

        F.when(
            F.col(
                "BaselineSpendEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "RealizedSavingsEUR"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "BaselineSpendEUR"
                ),
                2
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

# **Derive realization achievement**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "SavingsAchievementPct",

        F.when(
            F.col(
                "ApprovedSavingsEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "RealizedSavingsEUR"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "ApprovedSavingsEUR"
                ),
                2
            )
        )
    )

    .withColumn(
        "SavingsAchievementVarianceEUR",

        F.round(
            F.col(
                "RealizedSavingsEUR"
            )
            -
            F.col(
                "ApprovedSavingsEUR"
            ),
            2
        ).cast(
            "decimal(20,2)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive pipeline value**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ActivePipelineForecastEUR",

        F.when(
            F.col(
                "ActivePipelineFlag"
            ),

            F.col(
                "ForecastedSavingsEUR"
            )
        ).otherwise(
            F.lit(0)
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "ActivePipelineWeightedForecastEUR",

        F.when(
            F.col(
                "ActivePipelineFlag"
            ),

            F.col(
                "WeightedForecastSavingsEUR"
            )
        ).otherwise(
            F.lit(0)
        ).cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "ImplementedSavingsEUR",

        F.when(
            F.col(
                "IsImplementedFlag"
            ),

            F.col(
                "RealizedSavingsEUR"
            )
        ).otherwise(
            F.lit(0)
        ).cast(
            "decimal(20,2)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add time attributes**

# CELL ********************

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "ProjectCreatedYear",
        F.year(
            "ProjectCreatedDate"
        )
    )

    .withColumn(
        "ProjectCreatedMonth",
        F.month(
            "ProjectCreatedDate"
        )
    )

    .withColumn(
        "ProjectCreatedYearMonth",
        F.date_format(
            "ProjectCreatedDate",
            "yyyy-MM"
        )
    )

    .withColumn(
        "PlannedStartYear",
        F.year(
            "PlannedStartDate"
        )
    )

    .withColumn(
        "PlannedCompletionYear",
        F.year(
            "PlannedCompletionDate"
        )
    )

    .withColumn(
        "ActualCompletionYear",
        F.year(
            "ActualCompletionDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add silver metadata**

# CELL ********************

SILVER_HASH_EXCLUDED_COLUMNS = {
    "BronzeSourceSystem",
    "BronzeSourceExtractDate",
    "BronzeIngestionTimestamp",
    "BronzeLoadDate",
    "BronzeSourceRecordHash",

    "SilverLoadTimestamp",
    "SilverLoadDate",
    "SilverRecordHash"
}

hash_columns = [
    column_name
    for column_name
    in silver_savings_project_df.columns
    if column_name
    not in SILVER_HASH_EXCLUDED_COLUMNS
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

silver_savings_project_df = (
    silver_savings_project_df

    .withColumn(
        "SilverLoadTimestamp",
        F.current_timestamp()
    )

    .withColumn(
        "SilverLoadDate",
        F.current_date()
    )

    .withColumn(
        "SilverRecordHash",
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

# **Inspect project-status distribution**

# CELL ********************

display(
    silver_savings_project_df

    .groupBy(
        "ProjectStatus",
        "SavingsLevel"
    )

    .agg(
        F.count("*").alias(
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
                "RealizedSavingsEUR"
            ),
            2
        ).alias(
            "RealizedSavingsEUR"
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

# **Pipeline preview**

# CELL ********************

display(
    silver_savings_project_df

    .filter(
        F.col(
            "ActivePipelineFlag"
        )
    )

    .groupBy(
        "ProjectStatus"
    )

    .agg(
        F.count("*").alias(
            "ProjectCount"
        ),

        F.round(
            F.sum(
                "ActivePipelineForecastEUR"
            ),
            2
        ).alias(
            "PipelineForecastEUR"
        ),

        F.round(
            F.sum(
                "ActivePipelineWeightedForecastEUR"
            ),
            2
        ).alias(
            "WeightedPipelineForecastEUR"
        )
    )

    .orderBy(
        "ProjectStatus"
    )
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
                SILVER_SAVINGS_TABLE,

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

bronze_count = (
    savings_project_df.count()
)


bronze_unique_project_count = (
    savings_project_df

    .select(
        F.upper(
            F.trim(
                F.col(
                    "SavingsProjectID"
                )
            )
        ).alias(
            "SavingsProjectID"
        )
    )

    .distinct()

    .count()
)


silver_count = (
    silver_savings_project_df.count()
)


register_validation(
    "Row Count",

    (
        "One Silver row exists per "
        "unique Bronze savings project"
    ),

    abs(
        bronze_unique_project_count
        -
        silver_count
    ),

    (
        f"Bronze rows: "
        f"{bronze_count:,}; "
        f"Bronze unique projects: "
        f"{bronze_unique_project_count:,}; "
        f"Silver rows: "
        f"{silver_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate project key**

# CELL ********************

duplicate_project_count = (
    silver_savings_project_df

    .groupBy(
        "SavingsProjectID"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)

register_validation(
    "Primary Key",
    "SavingsProjectID is unique",
    duplicate_project_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_project_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "SavingsProjectID"
        ).isNull()
    )

    .count()
)

register_validation(
    "Primary Key",
    "SavingsProjectID is not null",
    null_project_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate master-data references**

# CELL ********************

unresolved_reference_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "SupplierName"
        ).isNull()
        |
        F.col(
            "CategoryName"
        ).isNull()
        |
        F.col(
            "BuyerName"
        ).isNull()
        |
        (
            ~F.col(
                "BusinessUnitResolvedFlag"
            )
        )
    )

    .count()
)

register_validation(
    "Referential Integrity",

    (
        "Supplier, category, buyer "
        "and business unit resolve"
    ),

    unresolved_reference_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate buyer / business-unit relationship**

# CELL ********************

buyer_business_unit_mismatch_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "BuyerName"
        ).isNotNull()
        &
        (
            ~F.col(
                "BuyerBusinessUnitID"
            ).eqNullSafe(
                F.col(
                    "BusinessUnitID"
                )
            )
        )
    )

    .count()
)


register_validation(
    "Referential Integrity",

    (
        "Buyer belongs to the "
        "project BusinessUnitID"
    ),

    buyer_business_unit_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate contract references**
# 
# Only projects with a ContractID require contract resolution

# CELL ********************

unresolved_contract_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "ContractReferenceFlag"
        )
        &
        (
            ~F.col(
                "ContractResolvedFlag"
            )
        )
    )

    .count()
)

register_validation(
    "Referential Integrity",

    (
        "Referenced contracts resolve"
    ),

    unresolved_contract_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate contract supplier/category consistency**

# CELL ********************

invalid_contract_relationship_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "ContractResolvedFlag"
        )
        &
        (
            (
                ~F.col(
                    "ContractSupplierMatchFlag"
                )
            )
            |
            (
                ~F.col(
                    "ContractCategoryMatchFlag"
                )
            )
        )
    )

    .count()
)

register_validation(
    "Contract Relationship",

    (
        "Referenced contract agrees "
        "with supplier and category"
    ),

    invalid_contract_relationship_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate status domain**

# CELL ********************

VALID_PROJECT_STATUSES = [
    "Idea",
    "Validated",
    "Negotiation",
    "Implemented",
    "Cancelled"
]


invalid_status_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "ProjectStatus"
        ).isNull()
        |
        (
            ~F.col(
                "ProjectStatus"
            ).isin(
                VALID_PROJECT_STATUSES
            )
        )
    )

    .count()
)


register_validation(
    "Domain",

    (
        "ProjectStatus is not null "
        "and belongs to approved domain"
    ),

    invalid_status_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate savings levels**

# CELL ********************

VALID_SAVINGS_LEVELS = [
    "L0",
    "L1",
    "L2",
    "L3"
]


invalid_level_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "SavingsLevel"
        ).isNull()
        |
        (
            ~F.col(
                "SavingsLevel"
            ).isin(
                VALID_SAVINGS_LEVELS
            )
        )
    )

    .count()
)


register_validation(
    "Domain",

    (
        "SavingsLevel is not null "
        "and belongs to L0-L3"
    ),

    invalid_level_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate confidence mapping**

# CELL ********************

confidence_mapping_error_count = (
    silver_savings_project_df

    .filter(
        (
            (F.col("SavingsLevel") == "L0")
            &
            (F.col("SavingsConfidenceWeight") != 0.00)
        )
        |
        (
            (F.col("SavingsLevel") == "L1")
            &
            (F.col("SavingsConfidenceWeight") != 0.25)
        )
        |
        (
            (F.col("SavingsLevel") == "L2")
            &
            (F.col("SavingsConfidenceWeight") != 0.50)
        )
        |
        (
            (F.col("SavingsLevel") == "L3")
            &
            (F.col("SavingsConfidenceWeight") != 1.00)
        )
    )

    .count()
)

register_validation(
    "Savings Logic",

    (
        "Savings confidence weight "
        "matches SavingsLevel"
    ),

    confidence_mapping_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate weighted forecast**

# CELL ********************

weighted_forecast_error_count = (
    silver_savings_project_df

    .filter(
        F.abs(
            F.col(
                "WeightedForecastSavings"
            )
            -
            F.round(
                F.col(
                    "ForecastedSavings"
                )
                *
                F.col(
                    "SavingsConfidenceWeight"
                ),
                2
            )
        )
        > F.lit(0.01)
    )

    .count()
)

register_validation(
    "Savings Logic",

    (
        "Weighted forecast equals "
        "forecast times confidence weight"
    ),

    weighted_forecast_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate project dates**

# CELL ********************

invalid_date_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "ProjectCreatedDate"
        ).isNull()
        |
        F.col(
            "PlannedStartDate"
        ).isNull()
        |
        F.col(
            "PlannedCompletionDate"
        ).isNull()
        |
        (
            F.col(
                "PlannedCompletionDate"
            )
            <
            F.col(
                "PlannedStartDate"
            )
        )
    )

    .count()
)

register_validation(
    "Date Integrity",

    (
        "Project dates are present "
        "and chronologically valid"
    ),

    invalid_date_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate completion/cancellation dates**

# CELL ********************

status_date_error_count = (
    silver_savings_project_df

    .filter(
        (
            F.col(
                "IsImplementedFlag"
            )
            &
            F.col(
                "ActualCompletionDate"
            ).isNull()
        )
        |
        (
            F.col(
                "IsCancelledFlag"
            )
            &
            F.col(
                "CancellationDate"
            ).isNull()
        )
    )

    .count()
)

register_validation(
    "Lifecycle",

    (
        "Implemented projects have "
        "completion dates and cancelled "
        "projects have cancellation dates"
    ),

    status_date_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate amounts**

# CELL ********************

invalid_amount_count = (
    silver_savings_project_df

    .filter(
        (
            F.col(
                "BaselineSpend"
            )
            < 0
        )
        |
        (
            F.col(
                "ForecastedSavings"
            )
            < 0
        )
        |
        (
            F.col(
                "ApprovedSavings"
            )
            < 0
        )
        |
        (
            F.col(
                "RealizedSavings"
            )
            < 0
        )
    )

    .count()
)

register_validation(
    "Monetary Values",

    (
        "Savings monetary values "
        "are non-negative"
    ),

    invalid_amount_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate FX resolution**

# CELL ********************

unresolved_fx_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "SavingsExchangeRateToEUR"
        ).isNull()
        |
        F.col(
            "SavingsFXRateDate"
        ).isNull()
        |
        (
            F.col(
                "SavingsFXResolutionMethod"
            )
            == "UNRESOLVED"
        )
    )

    .count()
)

register_validation(
    "Currency Conversion",

    (
        "Savings project FX rate "
        "is resolved"
    ),

    unresolved_fx_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate EUR arithmetic**

# CELL ********************

eur_conversion_error_count = (
    silver_savings_project_df

    .filter(
        F.abs(
            F.col(
                "ForecastedSavingsEUR"
            )
            -
            F.round(
                F.col(
                    "ForecastedSavings"
                )
                *
                F.col(
                    "SavingsExchangeRateToEUR"
                ),
                2
            )
        )
        > F.lit(0.01)
    )

    .count()
)

register_validation(
    "Currency Conversion",

    (
        "ForecastedSavingsEUR matches "
        "source-currency FX conversion"
    ),

    eur_conversion_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate acive pipeline**

# CELL ********************

pipeline_status_error_count = (
    silver_savings_project_df

    .filter(
        (
            F.col(
                "ActivePipelineFlag"
            )
            &
            (
                ~F.col(
                    "ProjectStatus"
                ).isin(
                    "Idea",
                    "Validated",
                    "Negotiation"
                )
            )
        )
        |
        (
            (
                F.col(
                    "ProjectStatus"
                ).isin(
                    "Implemented",
                    "Cancelled"
                )
            )
            &
            F.col(
                "ActivePipelineFlag"
            )
        )
    )

    .count()
)

register_validation(
    "Lifecycle",

    (
        "Active pipeline contains only "
        "Idea, Validated and Negotiation"
    ),

    pipeline_status_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Silver lineage**

# CELL ********************

invalid_lineage_count = (
    silver_savings_project_df

    .filter(
        F.col(
            "BronzeSourceRecordHash"
        ).isNull()
        |
        F.col(
            "SilverLoadTimestamp"
        ).isNull()
        |
        F.col(
            "SilverLoadDate"
        ).isNull()
        |
        F.col(
            "SilverRecordHash"
        ).isNull()
        |
        (
            F.length(
                "SilverRecordHash"
            )
            != 64
        )
    )

    .count()
)

register_validation(
    "Lineage",

    (
        "Bronze lineage and Silver "
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
    "Silver Savings Project "
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
        SILVER_MONITORING_TABLE
    )
)

print(
    "Monitoring table created:",
    SILVER_MONITORING_TABLE
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
        f"Silver Savings Project "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )

print(
    "SILVER SAVINGS PROJECT "
    "PRE-WRITE QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write physical Silver table**

# CELL ********************

(
    silver_savings_project_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        SILVER_SAVINGS_TABLE
    )
)

print(
    "Created physical Silver table:",
    SILVER_SAVINGS_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persistent row count**

# CELL ********************

persisted_savings_df = (
    spark.table(
        SILVER_SAVINGS_TABLE
    )
)

expected_row_count = (
    silver_savings_project_df.count()
)

persisted_row_count = (
    persisted_savings_df.count()
)

print(
    "Expected rows:",
    f"{expected_row_count:,}"
)

print(
    "Persisted rows:",
    f"{persisted_row_count:,}"
)

assert (
    expected_row_count
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

# **Annual savings preview**

# CELL ********************

annual_savings_df = (
    persisted_savings_df

    .groupBy(
        "PlannedStartYear"
    )

    .agg(
        F.count(
            "SavingsProjectID"
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
                "ApprovedSavingsEUR"
            ),
            2
        ).alias(
            "ApprovedSavingsEUR"
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
                "ActivePipelineWeightedForecastEUR"
            ),
            2
        ).alias(
            "ActivePipelineWeightedForecastEUR"
        )
    )

    .orderBy(
        "PlannedStartYear"
    )
)

display(
    annual_savings_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Savings pipeline by stage**

# CELL ********************

display(
    persisted_savings_df

    .groupBy(
        "ProjectStatus",
        "SavingsLevel"
    )

    .agg(
        F.count(
            "SavingsProjectID"
        ).alias(
            "ProjectCount"
        ),

        F.round(
            F.sum(
                "BaselineSpendEUR"
            ),
            2
        ).alias(
            "BaselineSpendEUR"
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
                "RealizedSavingsEUR"
            ),
            2
        ).alias(
            "RealizedSavingsEUR"
        )
    )

    .orderBy(
        "SavingsLevel",
        "ProjectStatus"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Category savings preview**

# CELL ********************

display(
    persisted_savings_df

    .groupBy(
        "CategoryID",
        "CategoryName"
    )

    .agg(
        F.count(
            "SavingsProjectID"
        ).alias(
            "ProjectCount"
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

    .orderBy(
        F.desc(
            "WeightedPipelineEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Buyer savings preview**

# CELL ********************

display(
    persisted_savings_df

    .groupBy(
        "BuyerID",
        "BuyerName",
        "BuyerRole"
    )

    .agg(
        F.count(
            "SavingsProjectID"
        ).alias(
            "ProjectCount"
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

    .orderBy(
        F.desc(
            "WeightedPipelineEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect overdue savings projects**

# CELL ********************

display(
    persisted_savings_df

    .filter(
        F.col(
            "OverdueProjectFlag"
        )
    )

    .select(
        "SavingsProjectID",
        "SavingsProjectName",
        "SupplierName",
        "CategoryName",
        "BuyerName",
        "ProjectStatus",
        "SavingsLevel",
        "PlannedCompletionDate",
        "DaysPastPlannedCompletion",
        "ForecastedSavingsEUR",
        "WeightedForecastSavingsEUR"
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

# **Final table preview**

# CELL ********************

display(
    persisted_savings_df

    .select(
        "SavingsProjectID",
        "SavingsProjectName",

        "SupplierID",
        "SupplierName",

        "CategoryID",
        "CategoryName",
        "ProcurementType",

        "BuyerID",
        "BuyerName",

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

        "Currency",

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
    "NB_25_Build_Silver_Savings_Project "
    "completed successfully."
)

print()

print(
    "Physical Silver output:"
)

print(
    "  silver_savings_project"
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
    "Core analytical measures:"
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

print(
    "  - SavingsAchievementPct"
)

print(
    "  - OverdueProjectFlag"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
