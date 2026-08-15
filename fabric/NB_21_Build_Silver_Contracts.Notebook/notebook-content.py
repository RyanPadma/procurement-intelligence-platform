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

# **Configuration**

# CELL ********************

from datetime import date

AS_OF_DATE = date(2026, 7, 31)

BRONZE_CONTRACT_TABLE = (
    "bronze_contract"
)

BRONZE_MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)

SILVER_CONTRACT_TABLE = (
    "silver_contract"
)

SILVER_MONITORING_TABLE = (
    "monitoring_silver_contract_quality_results"
)

REQUIRED_SILVER_REFERENCE_TABLES = [
    "silver_supplier",
    "silver_category",
    "silver_buyer",
    "silver_exchange_rate"
]

print(
    "Notebook: "
    "NB_21_Build_Silver_Contracts"
)

print(
    "Default Lakehouse: "
    "lh_procurement_silver"
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

from functools import reduce
from operator import or_

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

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
    BRONZE_CONTRACT_TABLE,
    BRONZE_MONITORING_TABLE
] + REQUIRED_SILVER_REFERENCE_TABLES

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
    "and Silver reference tables exist."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Confirm Bronze quality gate**

# CELL ********************

bronze_validation_df = spark.table(
    BRONZE_MONITORING_TABLE
)

bronze_critical_failure_count = (
    bronze_validation_df
    .filter(
        (
            F.col("Severity")
            == "ERROR"
        )
        &
        (
            F.col(
                "ValidationStatus"
            )
            == "FAILED"
        )
    )
    .count()
)

print(
    f"Bronze critical failures: "
    f"{bronze_critical_failure_count}"
)

assert (
    bronze_critical_failure_count == 0
), (
    "Bronze quality gate has not passed. "
    "Resolve NB_19 before building "
    "Silver contracts."
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

bronze_contract_df = spark.table(
    BRONZE_CONTRACT_TABLE
)

silver_supplier_df = spark.table(
    "silver_supplier"
)

silver_category_df = spark.table(
    "silver_category"
)

silver_buyer_df = spark.table(
    "silver_buyer"
)

silver_exchange_rate_df = spark.table(
    "silver_exchange_rate"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Bronze contracts:",
    bronze_contract_df.count()
)

print(
    "Silver suppliers:",
    silver_supplier_df.count()
)

print(
    "Silver categories:",
    silver_category_df.count()
)

print(
    "Silver buyers:",
    silver_buyer_df.count()
)

print(
    "Silver exchange rates:",
    silver_exchange_rate_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Normalize string columns**

# CELL ********************

def normalize_string_columns(
    dataframe
):
    result_df = dataframe

    for field in dataframe.schema.fields:
        if isinstance(
            field.dataType,
            StringType
        ):
            normalized_value = (
                F.trim(
                    F.regexp_replace(
                        F.col(field.name),
                        r"\s+",
                        " "
                    )
                )
            )

            result_df = (
                result_df
                .withColumn(
                    field.name,
                    F.when(
                        normalized_value == "",
                        F.lit(None)
                    ).otherwise(
                        normalized_value
                    )
                )
            )

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Identifier standardization helper**

# CELL ********************

def uppercase_columns(
    dataframe,
    column_names
):
    result_df = dataframe

    for column_name in column_names:
        if (
            column_name
            in result_df.columns
        ):
            result_df = (
                result_df
                .withColumn(
                    column_name,
                    F.upper(
                        F.col(
                            column_name
                        )
                    )
                )
            )

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplication helper**

# CELL ********************

def deduplicate_latest(
    dataframe,
    key_columns
):
    ordering_columns = []

    if (
        "IngestionTimestamp"
        in dataframe.columns
    ):
        ordering_columns.append(
            F.col(
                "IngestionTimestamp"
            ).desc_nulls_last()
        )

    if (
        "SourceRecordHash"
        in dataframe.columns
    ):
        ordering_columns.append(
            F.col(
                "SourceRecordHash"
            ).desc_nulls_last()
        )

    if not ordering_columns:
        ordering_columns = [
            F.monotonically_increasing_id()
        ]

    window_spec = (
        Window
        .partitionBy(
            *key_columns
        )
        .orderBy(
            *ordering_columns
        )
    )

    return (
        dataframe
        .withColumn(
            "_SilverRowNumber",
            F.row_number().over(
                window_spec
            )
        )
        .filter(
            F.col(
                "_SilverRowNumber"
            ) == 1
        )
        .drop(
            "_SilverRowNumber"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Bronze lineage mapping**

# CELL ********************

BRONZE_AUDIT_RENAME = {
    "SourceSystem":
        "BronzeSourceSystem",

    "IngestionTimestamp":
        "BronzeIngestionTimestamp",

    "LoadDate":
        "BronzeLoadDate",

    "SourceRecordHash":
        "BronzeSourceRecordHash"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def rename_bronze_audit_columns(
    dataframe
):
    result_df = dataframe

    for (
        source_column,
        target_column
    ) in BRONZE_AUDIT_RENAME.items():

        if (
            source_column
            in result_df.columns
        ):
            result_df = (
                result_df
                .withColumnRenamed(
                    source_column,
                    target_column
                )
            )

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Silver Metadata Helper**

# CELL ********************

SILVER_TECHNICAL_COLUMNS = {
    "BronzeSourceTable",
    "BronzeSourceSystem",
    "BronzeIngestionTimestamp",
    "BronzeLoadDate",
    "BronzeSourceRecordHash",
    "SilverLoadTimestamp",
    "SilverLoadDate",
    "SilverRecordHash"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_silver_metadata(
    dataframe,
    bronze_source_table
):
    result_df = (
        dataframe
        .withColumn(
            "BronzeSourceTable",
            F.lit(
                bronze_source_table
            )
        )
        .withColumn(
            "SilverLoadTimestamp",
            F.current_timestamp()
        )
        .withColumn(
            "SilverLoadDate",
            F.current_date()
        )
    )

    business_columns = [
        column_name
        for column_name
        in result_df.columns
        if column_name
        not in SILVER_TECHNICAL_COLUMNS
    ]

    hash_components = [
        F.coalesce(
            F.col(
                column_name
            ).cast("string"),
            F.lit("__NULL__")
        )
        for column_name
        in business_columns
    ]

    return (
        result_df
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

# **Initial COntract Cleansing**

# CELL ********************

silver_contract_df = (
    normalize_string_columns(
        bronze_contract_df
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize identifiers
silver_contract_df = (
    uppercase_columns(
        silver_contract_df,
        [
            "ContractID",
            "SupplierID",
            "CategoryID",
            "ContractOwnerBuyerID",
            "Currency"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Standardize contract datatypes**

# CELL ********************

silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ContractStartDate",
        F.to_date(
            "ContractStartDate"
        )
    )

    .withColumn(
        "ContractEndDate",
        F.to_date(
            "ContractEndDate"
        )
    )

    .withColumn(
        "ContractValue",
        F.col(
            "ContractValue"
        ).cast(
            "decimal(18,2)"
        )
    )

    .withColumn(
        "NegotiatedUnitPrice",
        F.col(
            "NegotiatedUnitPrice"
        ).cast(
            "decimal(18,4)"
        )
    )

    .withColumn(
        "PaymentTermsDays",
        F.col(
            "PaymentTermsDays"
        ).cast("int")
    )

    .withColumn(
        "AutoRenewalFlag",
        F.col(
            "AutoRenewalFlag"
        ).cast("boolean")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Preserve source contract status**
# 
# The Bronze status remains useful for lineage, but Silver will calculate contract lifecycle independently from dates.

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .withColumnRenamed(
        "ContractStatus",
        "SourceContractStatus"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#preserve the source-entered owner name
silver_contract_df = (
    silver_contract_df
    .withColumnRenamed(
        "ContractOwner",
        "SourceContractOwner"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Standardize source status values**

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .withColumn(
        "SourceContractStatus",
        F.when(
            F.lower(
                "SourceContractStatus"
            ) == "active",
            F.lit("Active")
        )
        .when(
            F.lower(
                "SourceContractStatus"
            ) == "expired",
            F.lit("Expired")
        )
        .when(
            F.lower(
                "SourceContractStatus"
            ) == "future",
            F.lit("Future")
        )
        .otherwise(
            F.col(
                "SourceContractStatus"
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

# **Standardize contract type**

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .withColumn(
        "ContractType",
        F.when(
            F.col(
                "ContractType"
            ).isNull(),
            F.lit(None)
        )
        .otherwise(
            F.initcap(
                F.lower(
                    F.col(
                        "ContractType"
                    )
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

# **Deduplicate contracts**

# CELL ********************

silver_contract_df = (
    deduplicate_latest(
        silver_contract_df,
        ["ContractID"]
    )
)

print(
    "Contracts after deduplication:",
    silver_contract_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive contract lifecycle**

# CELL ********************

as_of_date_literal = (
    F.lit(
        AS_OF_DATE.isoformat()
    ).cast("date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ContractLifecycleStatus",
        F.when(
            F.col(
                "ContractStartDate"
            )
            > as_of_date_literal,
            F.lit("Future")
        )
        .when(
            F.col(
                "ContractEndDate"
            )
            < as_of_date_literal,
            F.lit("Expired")
        )
        .otherwise(
            F.lit("Active")
        )
    )

    .withColumn(
        "IsContractActiveAsOfDate",
        (
            (
                F.col(
                    "ContractStartDate"
                )
                <= as_of_date_literal
            )
            &
            (
                F.col(
                    "ContractEndDate"
                )
                >= as_of_date_literal
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

# **Add contract date attributes**

# CELL ********************

silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ContractDurationDays",
        (
            F.datediff(
                F.col(
                    "ContractEndDate"
                ),
                F.col(
                    "ContractStartDate"
                )
            )
            + F.lit(1)
        )
    )

    .withColumn(
        "ContractStartYear",
        F.year(
            "ContractStartDate"
        )
    )

    .withColumn(
        "ContractEndYear",
        F.year(
            "ContractEndDate"
        )
    )

    .withColumn(
        "DaysToExpiryAsOfDate",
        F.datediff(
            F.col(
                "ContractEndDate"
            ),
            as_of_date_literal
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Compare source and derived status**

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .withColumn(
        "ContractStatusConsistencyFlag",
        (
            F.col(
                "SourceContractStatus"
            )
            ==
            F.col(
                "ContractLifecycleStatus"
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

# **Build canonical buyer reference**

# CELL ********************

buyer_reference_df = (
    silver_buyer_df
    .select(
        F.col(
            "BuyerID"
        ).alias(
            "RefBuyerID"
        ),
        F.col(
            "BuyerName"
        ).alias(
            "ContractOwnerName"
        ),
        F.col(
            "BuyerRole"
        ).alias(
            "ContractOwnerRole"
        ),
        F.col(
            "BusinessUnitID"
        ).alias(
            "ContractOwnerBusinessUnitID"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .join(
        buyer_reference_df,
        silver_contract_df[
            "ContractOwnerBuyerID"
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

# **Add canonical supplier attributes**

# CELL ********************

supplier_reference_df = (
    silver_supplier_df
    .select(
        F.col(
            "SupplierID"
        ).alias(
            "RefSupplierID"
        ),
        F.col(
            "SupplierName"
        ).alias(
            "SupplierName"
        ),
        F.col(
            "SupplierType"
        ).alias(
            "SupplierType"
        ),
        F.col(
            "SupplierActiveFlag"
        ).alias(
            "SupplierActiveFlag"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .join(
        supplier_reference_df,
        silver_contract_df[
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

# **Add canonical category attributes**

# CELL ********************

category_reference_df = (
    silver_category_df
    .select(
        F.col(
            "CategoryID"
        ).alias(
            "RefCategoryID"
        ),
        F.col(
            "CategoryName"
        ).alias(
            "CategoryName"
        ),
        F.col(
            "ProcurementType"
        ).alias(
            "ProcurementType"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .join(
        category_reference_df,
        silver_contract_df[
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

# **23 Prepare exchange-rate reference**
# 
# We use the exchange rate on the contract start date.

# CELL ********************

# Prepare FX reference
contract_fx_exact_df = (
    silver_exchange_rate_df
    .select(
        F.col("RateDate").alias(
            "ExactFXRateDate"
        ),
        F.col("Currency").alias(
            "ExactFXCurrency"
        ),
        F.col(
            "ExchangeRateToEUR"
        ).alias(
            "ExactExchangeRateToEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create first and last available FX rate per currency
fx_first_window = (
    Window
    .partitionBy("Currency")
    .orderBy(
        F.col("RateDate").asc()
    )
)

first_fx_rate_df = (
    silver_exchange_rate_df
    .withColumn(
        "_FXRowNumber",
        F.row_number().over(
            fx_first_window
        )
    )
    .filter(
        F.col("_FXRowNumber") == 1
    )
    .select(
        F.col("Currency").alias(
            "BoundaryCurrency"
        ),
        F.col("RateDate").alias(
            "FirstAvailableFXDate"
        ),
        F.col(
            "ExchangeRateToEUR"
        ).alias(
            "FirstAvailableFXRate"
        )
    )
)

fx_last_window = (
    Window
    .partitionBy("Currency")
    .orderBy(
        F.col("RateDate").desc()
    )
)

last_fx_rate_df = (
    silver_exchange_rate_df
    .withColumn(
        "_FXRowNumber",
        F.row_number().over(
            fx_last_window
        )
    )
    .filter(
        F.col("_FXRowNumber") == 1
    )
    .select(
        F.col("Currency").alias(
            "LastBoundaryCurrency"
        ),
        F.col("RateDate").alias(
            "LastAvailableFXDate"
        ),
        F.col(
            "ExchangeRateToEUR"
        ).alias(
            "LastAvailableFXRate"
        )
    )
)

print(
    "FX exact and boundary "
    "references prepared."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve contract FX rate**

# CELL ********************

silver_contract_df = (
    silver_contract_df

    # Exact ContractStartDate rate
    .join(
        contract_fx_exact_df,
        (
            silver_contract_df[
                "ContractStartDate"
            ]
            ==
            contract_fx_exact_df[
                "ExactFXRateDate"
            ]
        )
        &
        (
            silver_contract_df[
                "Currency"
            ]
            ==
            contract_fx_exact_df[
                "ExactFXCurrency"
            ]
        ),
        "left"
    )

    # Earliest available currency rate
    .join(
        first_fx_rate_df,
        silver_contract_df[
            "Currency"
        ]
        ==
        first_fx_rate_df[
            "BoundaryCurrency"
        ],
        "left"
    )

    # Latest available currency rate
    .join(
        last_fx_rate_df,
        silver_contract_df[
            "Currency"
        ]
        ==
        last_fx_rate_df[
            "LastBoundaryCurrency"
        ],
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Determines FX resolution method
silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ContractFXResolutionMethod",

        F.when(
            F.col(
                "ExactExchangeRateToEUR"
            ).isNotNull(),
            F.lit("EXACT_CONTRACT_START_DATE")
        )

        .when(
            F.col("ContractStartDate")
            < F.col(
                "FirstAvailableFXDate"
            ),
            F.lit(
                "EARLIEST_AVAILABLE_RATE"
            )
        )

        .when(
            F.col("ContractStartDate")
            > F.col(
                "LastAvailableFXDate"
            ),
            F.lit(
                "LATEST_AVAILABLE_RATE"
            )
        )

        .otherwise(
            F.lit("UNRESOLVED")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#resolve and record the actual FX date used
silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ExchangeRateToEURAtContractStart",

        F.when(
            F.col(
                "ExactExchangeRateToEUR"
            ).isNotNull(),
            F.col(
                "ExactExchangeRateToEUR"
            )
        )

        .when(
            F.col("ContractStartDate")
            < F.col(
                "FirstAvailableFXDate"
            ),
            F.col(
                "FirstAvailableFXRate"
            )
        )

        .when(
            F.col("ContractStartDate")
            > F.col(
                "LastAvailableFXDate"
            ),
            F.col(
                "LastAvailableFXRate"
            )
        )

        .otherwise(
            F.lit(None)
        )
    )
)

silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ContractFXRateDate",

        F.when(
            F.col(
                "ExactExchangeRateToEUR"
            ).isNotNull(),
            F.col("ExactFXRateDate")
        )

        .when(
            F.col("ContractStartDate")
            < F.col(
                "FirstAvailableFXDate"
            ),
            F.col(
                "FirstAvailableFXDate"
            )
        )

        .when(
            F.col("ContractStartDate")
            > F.col(
                "LastAvailableFXDate"
            ),
            F.col(
                "LastAvailableFXDate"
            )
        )

        .otherwise(
            F.lit(None).cast("date")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Clean up temporary join columns
silver_contract_df = (
    silver_contract_df
    .drop(
        "ExactFXRateDate",
        "ExactFXCurrency",
        "ExactExchangeRateToEUR",
        "BoundaryCurrency",
        "FirstAvailableFXDate",
        "FirstAvailableFXRate",
        "LastBoundaryCurrency",
        "LastAvailableFXDate",
        "LastAvailableFXRate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Clculate EUR Values**

# CELL ********************

silver_contract_df = (
    silver_contract_df

    .withColumn(
        "ContractValueEUR",
        F.round(
            F.col("ContractValue")
            *
            F.col(
                "ExchangeRateToEURAtContractStart"
            ),
            2
        ).cast(
            "decimal(18,2)"
        )
    )

    .withColumn(
        "NegotiatedUnitPriceEUR",
        F.round(
            F.col(
                "NegotiatedUnitPrice"
            )
            *
            F.col(
                "ExchangeRateToEURAtContractStart"
            ),
            4
        ).cast(
            "decimal(18,4)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **FX-resolution inspection cell**

# CELL ********************

display(
    silver_contract_df
    .groupBy(
        "ContractFXResolutionMethod"
    )
    .agg(
        F.count("*").alias(
            "ContractCount"
        )
    )
    .orderBy(
        F.desc("ContractCount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add contract validity-window flag**

# CELL ********************

silver_contract_df = (
    silver_contract_df
    .withColumn(
        "ContractValidityWindowFlag",
        (
            F.col(
                "ContractStartDate"
            ).isNotNull()
            &
            F.col(
                "ContractEndDate"
            ).isNotNull()
            &
            (
                F.col(
                    "ContractEndDate"
                )
                >=
                F.col(
                    "ContractStartDate"
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

# **Add silver lineage metadata**

# CELL ********************

silver_contract_df = (
    rename_bronze_audit_columns(
        silver_contract_df
    )
)

silver_contract_df = (
    add_silver_metadata(
        silver_contract_df,
        BRONZE_CONTRACT_TABLE
    )
)

display(
    silver_contract_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect contract lifecycle distribution**

# CELL ********************

display(
    silver_contract_df
    .groupBy(
        "ContractLifecycleStatus"
    )
    .agg(
        F.count("*").alias(
            "ContractCount"
        ),
        F.round(
            F.sum(
                "ContractValueEUR"
            ),
            2
        ).alias(
            "ContractValueEUR"
        )
    )
    .orderBy(
        "ContractLifecycleStatus"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect contracts by procurement type**

# CELL ********************

display(
    silver_contract_df
    .groupBy(
        "ProcurementType",
        "ContractLifecycleStatus"
    )
    .agg(
        F.count("*").alias(
            "ContractCount"
        ),
        F.round(
            F.sum(
                "ContractValueEUR"
            ),
            2
        ).alias(
            "ContractValueEUR"
        )
    )
    .orderBy(
        "ProcurementType",
        "ContractLifecycleStatus"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation Framework**

# CELL ********************

silver_contract_validation_results = []

def register_validation(
    validation_category,
    validation_rule,
    failed_record_count,
    validation_details=""
):
    failed_record_count = int(
        failed_record_count or 0
    )

    validation_status = (
        "PASSED"
        if failed_record_count == 0
        else "FAILED"
    )

    silver_contract_validation_results.append({
        "TableName":
            SILVER_CONTRACT_TABLE,

        "ValidationCategory":
            validation_category,

        "ValidationRule":
            validation_rule,

        "FailedRecordCount":
            failed_record_count,

        "ValidationStatus":
            validation_status,

        "ValidationDetails":
            validation_details
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate row preservation**

# CELL ********************

bronze_contract_count = (
    bronze_contract_df.count()
)

silver_contract_count = (
    silver_contract_df.count()
)

register_validation(
    validation_category=(
        "Row Count"
    ),
    validation_rule=(
        "Silver contract row count "
        "matches validated Bronze source"
    ),
    failed_record_count=abs(
        bronze_contract_count
        - silver_contract_count
    ),
    validation_details=(
        f"Bronze: "
        f"{bronze_contract_count:,}; "
        f"Silver: "
        f"{silver_contract_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate COntractID**

# CELL ********************

null_contract_id_count = (
    silver_contract_df
    .filter(
        F.col(
            "ContractID"
        ).isNull()
    )
    .count()
)

register_validation(
    "Primary Key",
    "ContractID is not null",
    null_contract_id_count
)

duplicate_contract_id_count = (
    silver_contract_df
    .groupBy(
        "ContractID"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

register_validation(
    "Primary Key",
    "ContractID is unique",
    duplicate_contract_id_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate master-data references**

# CELL ********************

missing_reference_count = (
    silver_contract_df
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
            "ContractOwnerName"
        ).isNull()
    )
    .count()
)

register_validation(
    "Referential Integrity",
    (
        "Supplier, category and buyer "
        "references are resolved"
    ),
    missing_reference_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate contract dates**

# CELL ********************

invalid_contract_date_count = (
    silver_contract_df
    .filter(
        F.col(
            "ContractStartDate"
        ).isNull()
        |
        F.col(
            "ContractEndDate"
        ).isNull()
        |
        (
            F.col(
                "ContractEndDate"
            )
            <
            F.col(
                "ContractStartDate"
            )
        )
        |
        (
            F.col(
                "ContractDurationDays"
            )
            <= 0
        )
    )
    .count()
)

register_validation(
    "Date Integrity",
    (
        "Contract validity dates "
        "are chronologically valid"
    ),
    invalid_contract_date_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate source lifecycle consistency**

# CELL ********************

status_mismatch_count = (
    silver_contract_df
    .filter(
        ~F.col(
            "ContractStatusConsistencyFlag"
        )
    )
    .count()
)

register_validation(
    "Lifecycle",
    (
        "Source contract status agrees "
        "with date-derived lifecycle"
    ),
    status_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate active flag**

# CELL ********************

active_flag_error_count = (
    silver_contract_df
    .filter(
        F.col(
            "IsContractActiveAsOfDate"
        )
        !=
        (
            F.col(
                "ContractLifecycleStatus"
            )
            == "Active"
        )
    )
    .count()
)

register_validation(
    "Lifecycle",
    (
        "Active contract flag agrees "
        "with lifecycle status"
    ),
    active_flag_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate contract monetary values**

# CELL ********************

invalid_contract_value_count = (
    silver_contract_df
    .filter(
        (
            F.col(
                "ContractValue"
            ) <= 0
        )
        |
        (
            F.col(
                "NegotiatedUnitPrice"
            ) <= 0
        )
        |
        (
            F.col(
                "ContractValueEUR"
            ) <= 0
        )
        |
        (
            F.col(
                "NegotiatedUnitPriceEUR"
            ) <= 0
        )
    )
    .count()
)

register_validation(
    "Amount Integrity",
    (
        "Contract values and negotiated "
        "prices are positive"
    ),
    invalid_contract_value_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate FX coverage**

# CELL ********************

unresolved_fx_count = (
    silver_contract_df
    .filter(
        F.col(
            "ExchangeRateToEURAtContractStart"
        ).isNull()
        |
        F.col(
            "ContractFXRateDate"
        ).isNull()
        |
        (
            F.col(
                "ContractFXResolutionMethod"
            )
            == "UNRESOLVED"
        )
    )
    .count()
)

register_validation(
    "Currency Conversion",
    (
        "Contract FX rate is resolved "
        "using exact or boundary rate"
    ),
    unresolved_fx_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#transparency check
fallback_fx_count = (
    silver_contract_df
    .filter(
        F.col(
            "ContractFXResolutionMethod"
        ).isin(
            "EARLIEST_AVAILABLE_RATE",
            "LATEST_AVAILABLE_RATE"
        )
    )
    .count()
)

exact_fx_count = (
    silver_contract_df
    .filter(
        F.col(
            "ContractFXResolutionMethod"
        )
        == "EXACT_CONTRACT_START_DATE"
    )
    .count()
)

print(
    f"Exact-date FX contracts: "
    f"{exact_fx_count:,}"
)

print(
    f"Boundary-rate FX contracts: "
    f"{fallback_fx_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate EUR conversion**

# CELL ********************

fx_conversion_error_count = (
    silver_contract_df
    .filter(
        (
            F.abs(
                F.col(
                    "ContractValueEUR"
                )
                -
                F.round(
                    F.col(
                        "ContractValue"
                    )
                    *
                    F.col(
                        "ExchangeRateToEURAtContractStart"
                    ),
                    2
                )
            )
            > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col(
                    "NegotiatedUnitPriceEUR"
                )
                -
                F.round(
                    F.col(
                        "NegotiatedUnitPrice"
                    )
                    *
                    F.col(
                        "ExchangeRateToEURAtContractStart"
                    ),
                    4
                )
            )
            > F.lit(0.0001)
        )
    )
    .count()
)

register_validation(
    "Currency Conversion",
    (
        "EUR contract values match "
        "exchange-rate conversion"
    ),
    fx_conversion_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate payment terms**

# CELL ********************

invalid_payment_terms_count = (
    silver_contract_df
    .filter(
        F.col(
            "PaymentTermsDays"
        ).isNull()
        |
        (
            F.col(
                "PaymentTermsDays"
            ) < 0
        )
    )
    .count()
)

register_validation(
    "Business Rule",
    (
        "Payment terms contain "
        "valid day values"
    ),
    invalid_payment_terms_count
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
    silver_contract_df
    .filter(
        F.col(
            "BronzeSourceTable"
        ).isNull()
        |
        F.col(
            "BronzeSourceSystem"
        ).isNull()
        |
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
            ) != 64
        )
    )
    .count()
)

register_validation(
    "Lineage",
    (
        "Bronze lineage and Silver "
        "audit metadata are complete"
    ),
    invalid_lineage_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build validation DataFrame**

# CELL ********************

silver_contract_validation_df = (
    spark.createDataFrame(
        silver_contract_validation_results
    )
    .withColumn(
        "ExecutionTimestamp",
        F.current_timestamp()
    )
)

display(
    silver_contract_validation_df
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

# **Count pre-write failures**

# CELL ********************

pre_write_failure_count = (
    silver_contract_validation_df
    .filter(
        F.col(
            "ValidationStatus"
        )
        == "FAILED"
    )
    .count()
)

print(
    f"Silver contract "
    f"pre-write failures: "
    f"{pre_write_failure_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist contract monitoring results**

# CELL ********************

(
    silver_contract_validation_df.write
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
    f"Created monitoring table: "
    f"{SILVER_MONITORING_TABLE}"
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
        silver_contract_validation_df
        .filter(
            F.col(
                "ValidationStatus"
            )
            == "FAILED"
        )
        .orderBy(
            F.desc(
                "FailedRecordCount"
            )
        )
    )

    raise AssertionError(
        f"Silver contract validation "
        f"failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )

print(
    "SILVER CONTRACT PRE-WRITE "
    "QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write physical silver_contract**

# CELL ********************

(
    silver_contract_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        SILVER_CONTRACT_TABLE
    )
)

print(
    "Created physical Silver table: "
    f"{SILVER_CONTRACT_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted row count**

# CELL ********************

persisted_contract_df = spark.table(
    SILVER_CONTRACT_TABLE
)

persisted_contract_count = (
    persisted_contract_df.count()
)

expected_contract_count = (
    silver_contract_df.count()
)

print(
    f"Expected rows: "
    f"{expected_contract_count:,}"
)

print(
    f"Persisted rows: "
    f"{persisted_contract_count:,}"
)

assert (
    persisted_contract_count
    == expected_contract_count
), (
    "Persisted silver_contract "
    "row count does not match."
)

print(
    "Silver contract row-count "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect final contract schema**

# CELL ********************

persisted_contract_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final Contract inventory**

# CELL ********************

display(
    persisted_contract_df
    .select(
        "ContractID",
        "SupplierID",
        "SupplierName",
        "CategoryID",
        "CategoryName",
        "ProcurementType",
        "ContractStartDate",
        "ContractEndDate",
        "SourceContractStatus",
        "ContractLifecycleStatus",
        "IsContractActiveAsOfDate",
        "Currency",
        "ContractValue",
        "ContractValueEUR",
        "NegotiatedUnitPrice",
        "NegotiatedUnitPriceEUR",
        "ContractOwnerBuyerID",
        "ContractOwnerName",
        "ContractOwnerBusinessUnitID"
    )
    .orderBy(
        "ContractID"
    )
    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final Status**

# CELL ********************

print(
    "NB_21_Build_Silver_Contracts "
    "completed successfully."
)

print()
print(
    "Bronze source:"
)
print(
    "  bronze_contract "
    "(OneLake shortcut)"
)

print()
print(
    "Silver references:"
)
print(
    "  silver_supplier"
)
print(
    "  silver_category"
)
print(
    "  silver_buyer"
)
print(
    "  silver_exchange_rate"
)

print()
print(
    "Physical Silver output:"
)
print(
    "  silver_contract"
)

print()
print(
    "Contract Compliance and "
    "Maverick Spend classification "
    "will be performed in NB_22."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
