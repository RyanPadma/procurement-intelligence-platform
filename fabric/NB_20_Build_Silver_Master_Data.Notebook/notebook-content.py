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

# **Check the Bronze Shortcut**

# CELL ********************

required_bronze_shortcuts = [
    "bronze_category",
    "bronze_business_unit",
    "bronze_buyer",
    "bronze_supplier",
    "bronze_material",
    "bronze_exchange_rate",
    "bronze_contract",
    "bronze_purchase_order_header",
    "bronze_purchase_order_item",
    "bronze_goods_receipt",
    "bronze_invoice_header",
    "bronze_invoice_item",
    "bronze_savings_project",
    "monitoring_bronze_data_quality_results"
]

missing_shortcuts = [
    table_name
    for table_name in required_bronze_shortcuts
    if not spark.catalog.tableExists(
        table_name
    )
]

if missing_shortcuts:
    raise RuntimeError(
        "Missing Bronze shortcuts: "
        + ", ".join(missing_shortcuts)
    )

print(
    "All Bronze shortcuts are available "
    "from lh_procurement_silver."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Test the shorcut
for table_name in required_bronze_shortcuts:
    row_count = (
        spark.table(
            table_name
        ).count()
    )

    print(
        f"{table_name}: "
        f"{row_count:,} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Confirm the Bronze quality gate
from pyspark.sql import functions as F

bronze_critical_failures = (
    spark.table(
        "monitoring_bronze_data_quality_results"
    )
    .filter(
        (F.col("Severity") == "ERROR")
        &
        (
            F.col("ValidationStatus")
            == "FAILED"
        )
    )
    .count()
)

print(
    f"Bronze critical failures: "
    f"{bronze_critical_failures}"
)

assert bronze_critical_failures == 0, (
    "Bronze quality gate has not passed."
)

print(
    "Bronze quality gate confirmed "
    "from the Silver Lakehouse."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Configuration**

# CELL ********************

from datetime import date

AS_OF_DATE = date(2026, 7, 31)

BRONZE_SOURCE_TABLES = {
    "category": "bronze_category",
    "business_unit": "bronze_business_unit",
    "buyer": "bronze_buyer",
    "supplier": "bronze_supplier",
    "material": "bronze_material",
    "exchange_rate": "bronze_exchange_rate"
}

SILVER_TARGET_TABLES = {
    "category": "silver_category",
    "business_unit": "silver_business_unit",
    "buyer": "silver_buyer",
    "supplier": "silver_supplier",
    "material": "silver_material",
    "exchange_rate": "silver_exchange_rate"
}

BRONZE_MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)

SILVER_MONITORING_TABLE = (
    "monitoring_silver_master_data_quality_results"
)

print("Notebook: NB_20_Build_Silver_Master_Data")
print("Expected default Lakehouse: lh_procurement_silver")
print(f"As-of date: {AS_OF_DATE}")

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

# **Load Bronze shorcut tables**

# CELL ********************

bronze_category_df = spark.table(
    "bronze_category"
)

bronze_business_unit_df = spark.table(
    "bronze_business_unit"
)

bronze_buyer_df = spark.table(
    "bronze_buyer"
)

bronze_supplier_df = spark.table(
    "bronze_supplier"
)

bronze_material_df = spark.table(
    "bronze_material"
)

bronze_exchange_rate_df = spark.table(
    "bronze_exchange_rate"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check row count
bronze_source_counts = {
    "bronze_category": (
        bronze_category_df.count()
    ),
    "bronze_business_unit": (
        bronze_business_unit_df.count()
    ),
    "bronze_buyer": (
        bronze_buyer_df.count()
    ),
    "bronze_supplier": (
        bronze_supplier_df.count()
    ),
    "bronze_material": (
        bronze_material_df.count()
    ),
    "bronze_exchange_rate": (
        bronze_exchange_rate_df.count()
    )
}

for (
    table_name,
    row_count
) in bronze_source_counts.items():
    print(
        f"{table_name}: "
        f"{row_count:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Define reusable Silver cleansing functions**

# MARKDOWN ********************

# **Normalize strings**
# 
# This:
# 
# - trims leading/trailing spaces
# - collapses repeated spaces
# - converts empty strings to null#

# CELL ********************

#Normalize strings
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

# **Standardize identifiers**

# CELL ********************

def uppercase_columns(
    dataframe,
    column_names
):
    result_df = dataframe

    for column_name in column_names:
        if column_name in result_df.columns:
            result_df = (
                result_df
                .withColumn(
                    column_name,
                    F.upper(
                        F.col(column_name)
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

# **Define deduplication logic**
# 
# Bronze has already passed uniqueness validation, but Silver should still implement deterministic deduplication as an engineering safeguard.

# CELL ********************

def deduplicate_latest(
    dataframe,
    key_columns
):
    if (
        "IngestionTimestamp"
        in dataframe.columns
    ):
        ordering_columns = [
            F.col(
                "IngestionTimestamp"
            ).desc_nulls_last()
        ]

        if (
            "SourceRecordHash"
            in dataframe.columns
        ):
            ordering_columns.append(
                F.col(
                    "SourceRecordHash"
                ).desc_nulls_last()
            )

    else:
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

# **Define Bronze lineage handling**
# 
# Rather than leaving Bronze and Silver audit columns with ambiguous names, Silver will explicitly retain the Bronze lineage.

# CELL ********************

BRONZE_AUDIT_RENAME = {
    "SourceSystem": (
        "BronzeSourceSystem"
    ),
    "IngestionTimestamp": (
        "BronzeIngestionTimestamp"
    ),
    "LoadDate": (
        "BronzeLoadDate"
    ),
    "SourceRecordHash": (
        "BronzeSourceRecordHash"
    )
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

# **Define Silver metadata function**
# 
# Silver receives its own technical lineage.

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

    result_df = (
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

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build silver_category**

# CELL ********************

silver_category_df = (
    normalize_string_columns(
        bronze_category_df
    )
)

silver_category_df = (
    uppercase_columns(
        silver_category_df,
        [
            "CategoryID"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize procurement type
silver_category_df = (
    silver_category_df
    .withColumn(
        "ProcurementType",
        F.when(
            F.lower(
                F.col(
                    "ProcurementType"
                )
            ) == "direct",
            F.lit("Direct")
        )
        .when(
            F.lower(
                F.col(
                    "ProcurementType"
                )
            ) == "indirect",
            F.lit("Indirect")
        )
        .when(
            F.lower(
                F.col(
                    "ProcurementType"
                )
            ) == "capital",
            F.lit("Capital")
        )
        .otherwise(
            F.col(
                "ProcurementType"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Deduplicate and add lineage
silver_category_df = (
    deduplicate_latest(
        silver_category_df,
        ["CategoryID"]
    )
)

silver_category_df = (
    rename_bronze_audit_columns(
        silver_category_df
    )
)

silver_category_df = (
    add_silver_metadata(
        silver_category_df,
        "bronze_category"
    )
)

display(
    silver_category_df.limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build silver_business_unit**

# CELL ********************

silver_business_unit_df = (
    normalize_string_columns(
        bronze_business_unit_df
    )
)

silver_business_unit_df = (
    uppercase_columns(
        silver_business_unit_df,
        [
            "BusinessUnitID"
        ]
    )
)

silver_business_unit_df = (
    deduplicate_latest(
        silver_business_unit_df,
        ["BusinessUnitID"]
    )
)

silver_business_unit_df = (
    rename_bronze_audit_columns(
        silver_business_unit_df
    )
)

silver_business_unit_df = (
    add_silver_metadata(
        silver_business_unit_df,
        "bronze_business_unit"
    )
)

display(
    silver_business_unit_df.limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build silver_buyer**

# CELL ********************

silver_buyer_df = (
    normalize_string_columns(
        bronze_buyer_df
    )
)

silver_buyer_df = (
    uppercase_columns(
        silver_buyer_df,
        [
            "BuyerID",
            "BusinessUnitID"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize buyer roles
silver_buyer_df = (
    silver_buyer_df
    .withColumn(
        "BuyerRole",
        F.when(
            F.lower(
                "BuyerRole"
            )
            == "operational buyer",
            F.lit(
                "Operational Buyer"
            )
        )
        .when(
            F.lower(
                "BuyerRole"
            )
            == "senior buyer",
            F.lit(
                "Senior Buyer"
            )
        )
        .when(
            F.lower(
                "BuyerRole"
            )
            == "strategic buyer",
            F.lit(
                "Strategic Buyer"
            )
        )
        .when(
            F.lower(
                "BuyerRole"
            )
            == "category manager",
            F.lit(
                "Category Manager"
            )
        )
        .when(
            F.lower(
                "BuyerRole"
            )
            == "procurement manager",
            F.lit(
                "Procurement Manager"
            )
        )
        .otherwise(
            F.col("BuyerRole")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_buyer_df = (
    deduplicate_latest(
        silver_buyer_df,
        ["BuyerID"]
    )
)

silver_buyer_df = (
    rename_bronze_audit_columns(
        silver_buyer_df
    )
)

silver_buyer_df = (
    add_silver_metadata(
        silver_buyer_df,
        "bronze_buyer"
    )
)

display(
    silver_buyer_df.limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build silver_supplier**

# CELL ********************

silver_supplier_df = (
    normalize_string_columns(
        bronze_supplier_df
    )
)

silver_supplier_df = (
    uppercase_columns(
        silver_supplier_df,
        [
            "SupplierID"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize supplier status
silver_supplier_df = (
    silver_supplier_df
    .withColumn(
        "Status",
        F.when(
            F.lower(
                "Status"
            ) == "active",
            F.lit("Active")
        )
        .when(
            F.lower(
                "Status"
            ) == "inactive",
            F.lit("Inactive")
        )
        .otherwise(
            F.col("Status")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize supplier type
silver_supplier_df = (
    silver_supplier_df
    .withColumn(
        "SupplierType",
        F.when(
            F.lower(
                "SupplierType"
            ) == "manufacturer",
            F.lit("Manufacturer")
        )
        .when(
            F.lower(
                "SupplierType"
            ) == "distributor",
            F.lit("Distributor")
        )
        .when(
            F.lower(
                "SupplierType"
            ) == "service provider",
            F.lit(
                "Service Provider"
            )
        )
        .when(
            F.lower(
                "SupplierType"
            ) == "logistics provider",
            F.lit(
                "Logistics Provider"
            )
        )
        .when(
            F.lower(
                "SupplierType"
            ) == "utility provider",
            F.lit(
                "Utility Provider"
            )
        )
        .when(
            F.lower(
                "SupplierType"
            ) == "contractor",
            F.lit("Contractor")
        )
        .otherwise(
            F.col("SupplierType")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create a conformed master-data status flag
silver_supplier_df = (
    silver_supplier_df
    .withColumn(
        "SupplierActiveFlag",
        F.col("Status")
        == F.lit("Active")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#deduplicate and add lineage
silver_supplier_df = (
    deduplicate_latest(
        silver_supplier_df,
        ["SupplierID"]
    )
)

silver_supplier_df = (
    rename_bronze_audit_columns(
        silver_supplier_df
    )
)

silver_supplier_df = (
    add_silver_metadata(
        silver_supplier_df,
        "bronze_supplier"
    )
)

display(
    silver_supplier_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build silver_material**

# CELL ********************

silver_material_df = (
    normalize_string_columns(
        bronze_material_df
    )
)

silver_material_df = (
    uppercase_columns(
        silver_material_df,
        [
            "MaterialID",
            "CategoryID",
            "UnitOfMeasure"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize material status
silver_material_df = (
    silver_material_df
    .withColumn(
        "Status",
        F.when(
            F.lower(
                "Status"
            ) == "active",
            F.lit("Active")
        )
        .when(
            F.lower(
                "Status"
            ) == "inactive",
            F.lit("Inactive")
        )
        .otherwise(
            F.col("Status")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize monetary datatype
silver_material_df = (
    silver_material_df
    .withColumn(
        "StandardCost",
        F.col(
            "StandardCost"
        ).cast(
            "decimal(18,2)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#deduplicate and add lineage
silver_material_df = (
    deduplicate_latest(
        silver_material_df,
        ["MaterialID"]
    )
)

silver_material_df = (
    rename_bronze_audit_columns(
        silver_material_df
    )
)

silver_material_df = (
    add_silver_metadata(
        silver_material_df,
        "bronze_material"
    )
)

display(
    silver_material_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build silver_exchange_rate**

# CELL ********************

#Standardization
silver_exchange_rate_df = (
    normalize_string_columns(
        bronze_exchange_rate_df
    )
)

silver_exchange_rate_df = (
    uppercase_columns(
        silver_exchange_rate_df,
        [
            "Currency"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Rename the rate to make its meaning explicit
silver_exchange_rate_df = (
    silver_exchange_rate_df
    .withColumnRenamed(
        "ExchangeRateEUR",
        "ExchangeRateToEUR"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Standardize precision
silver_exchange_rate_df = (
    silver_exchange_rate_df
    .withColumn(
        "ExchangeRateToEUR",
        F.col(
            "ExchangeRateToEUR"
        ).cast(
            "decimal(18,8)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add  conformed date attributes
silver_exchange_rate_df = (
    silver_exchange_rate_df
    .withColumn(
        "RateYear",
        F.year(
            "RateDate"
        )
    )
    .withColumn(
        "RateMonth",
        F.month(
            "RateDate"
        )
    )
    .withColumn(
        "RateYearMonth",
        F.date_format(
            "RateDate",
            "yyyy-MM"
        )
    )
    .withColumn(
        "IsEUR",
        F.col("Currency")
        == F.lit("EUR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Deduplicate
silver_exchange_rate_df = (
    deduplicate_latest(
        silver_exchange_rate_df,
        [
            "RateDate",
            "Currency"
        ]
    )
)

silver_exchange_rate_df = (
    rename_bronze_audit_columns(
        silver_exchange_rate_df
    )
)

silver_exchange_rate_df = (
    add_silver_metadata(
        silver_exchange_rate_df,
        "bronze_exchange_rate"
    )
)

display(
    silver_exchange_rate_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Register Silver DataFrames**

# CELL ********************

silver_dataframes = {
    "silver_category": (
        silver_category_df
    ),
    "silver_business_unit": (
        silver_business_unit_df
    ),
    "silver_buyer": (
        silver_buyer_df
    ),
    "silver_supplier": (
        silver_supplier_df
    ),
    "silver_material": (
        silver_material_df
    ),
    "silver_exchange_rate": (
        silver_exchange_rate_df
    )
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Bronze to Silver source map
bronze_silver_pairs = {
    "silver_category": (
        bronze_category_df
    ),
    "silver_business_unit": (
        bronze_business_unit_df
    ),
    "silver_buyer": (
        bronze_buyer_df
    ),
    "silver_supplier": (
        bronze_supplier_df
    ),
    "silver_material": (
        bronze_material_df
    ),
    "silver_exchange_rate": (
        bronze_exchange_rate_df
    )
}

print(
    f"Prepared "
    f"{len(silver_dataframes)} "
    f"Silver master-data tables."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Silver validation framework**

# CELL ********************

silver_validation_results = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def register_silver_validation(
    table_name,
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

    silver_validation_results.append({
        "TableName": table_name,
        "ValidationCategory": (
            validation_category
        ),
        "ValidationRule": (
            validation_rule
        ),
        "FailedRecordCount": (
            failed_record_count
        ),
        "ValidationStatus": (
            validation_status
        ),
        "ValidationDetails": (
            validation_details
        )
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate row preservation**
# 
# Because NB_19 already confirmed unique Bronze master keys, no rows should disappear during Silver transformation.

# CELL ********************

for (
    silver_table_name,
    bronze_dataframe
) in bronze_silver_pairs.items():

    silver_dataframe = (
        silver_dataframes[
            silver_table_name
        ]
    )

    bronze_count = (
        bronze_dataframe.count()
    )

    silver_count = (
        silver_dataframe.count()
    )

    register_silver_validation(
        table_name=(
            silver_table_name
        ),
        validation_category=(
            "Row Count"
        ),
        validation_rule=(
            "Silver row count matches "
            "validated Bronze source"
        ),
        failed_record_count=abs(
            bronze_count
            - silver_count
        ),
        validation_details=(
            f"Bronze: {bronze_count:,}; "
            f"Silver: {silver_count:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Silver primary keys**

# CELL ********************

silver_primary_keys = {
    "silver_category": [
        "CategoryID"
    ],
    "silver_business_unit": [
        "BusinessUnitID"
    ],
    "silver_buyer": [
        "BuyerID"
    ],
    "silver_supplier": [
        "SupplierID"
    ],
    "silver_material": [
        "MaterialID"
    ],
    "silver_exchange_rate": [
        "RateDate",
        "Currency"
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for (
    table_name,
    key_columns
) in silver_primary_keys.items():

    dataframe = (
        silver_dataframes[
            table_name
        ]
    )

    null_conditions = [
        F.col(
            column_name
        ).isNull()
        for column_name
        in key_columns
    ]

    null_condition = reduce(
        or_,
        null_conditions
    )

    null_key_count = (
        dataframe
        .filter(
            null_condition
        )
        .count()
    )

    duplicate_key_count = (
        dataframe
        .groupBy(
            *key_columns
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    register_silver_validation(
        table_name=table_name,
        validation_category=(
            "Primary Key"
        ),
        validation_rule=(
            "Primary key contains "
            "no null values"
        ),
        failed_record_count=(
            null_key_count
        )
    )

    register_silver_validation(
        table_name=table_name,
        validation_category=(
            "Primary Key"
        ),
        validation_rule=(
            "Primary key is unique"
        ),
        failed_record_count=(
            duplicate_key_count
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate conformed relationships**

# CELL ********************

#Buyer -> Business Unit
invalid_buyer_business_unit_count = (
    silver_buyer_df.alias("buyer")
    .join(
        silver_business_unit_df.alias(
            "business_unit"
        ),
        F.col(
            "buyer.BusinessUnitID"
        )
        == F.col(
            "business_unit.BusinessUnitID"
        ),
        "left_anti"
    )
    .count()
)

register_silver_validation(
    table_name="silver_buyer",
    validation_category=(
        "Referential Integrity"
    ),
    validation_rule=(
        "Buyer business unit exists in "
        "silver_business_unit"
    ),
    failed_record_count=(
        invalid_buyer_business_unit_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Material -> Category
invalid_material_category_count = (
    silver_material_df.alias(
        "material"
    )
    .join(
        silver_category_df.alias(
            "category"
        ),
        F.col(
            "material.CategoryID"
        )
        == F.col(
            "category.CategoryID"
        ),
        "left_anti"
    )
    .count()
)

register_silver_validation(
    table_name="silver_material",
    validation_category=(
        "Referential Integrity"
    ),
    validation_rule=(
        "Material category exists in "
        "silver_category"
    ),
    failed_record_count=(
        invalid_material_category_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate conformed domains**

# CELL ********************

#Procurement type
invalid_procurement_type_count = (
    silver_category_df
    .filter(
        ~F.col(
            "ProcurementType"
        ).isin(
            "Direct",
            "Indirect",
            "Capital"
        )
    )
    .count()
)

register_silver_validation(
    "silver_category",
    "Domain",
    (
        "ProcurementType uses "
        "conformed values"
    ),
    invalid_procurement_type_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Buyer role
invalid_buyer_role_count = (
    silver_buyer_df
    .filter(
        ~F.col(
            "BuyerRole"
        ).isin(
            "Operational Buyer",
            "Senior Buyer",
            "Strategic Buyer",
            "Category Manager",
            "Procurement Manager"
        )
    )
    .count()
)

register_silver_validation(
    "silver_buyer",
    "Domain",
    (
        "BuyerRole uses "
        "conformed values"
    ),
    invalid_buyer_role_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Supplier type
invalid_supplier_type_count = (
    silver_supplier_df
    .filter(
        ~F.col(
            "SupplierType"
        ).isin(
            "Manufacturer",
            "Distributor",
            "Service Provider",
            "Logistics Provider",
            "Utility Provider",
            "Contractor"
        )
    )
    .count()
)

register_silver_validation(
    "silver_supplier",
    "Domain",
    (
        "SupplierType uses "
        "conformed values"
    ),
    invalid_supplier_type_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#material cost
invalid_material_cost_count = (
    silver_material_df
    .filter(
        F.col(
            "StandardCost"
        ) <= F.lit(0)
    )
    .count()
)

register_silver_validation(
    "silver_material",
    "Business Rule",
    (
        "StandardCost is positive"
    ),
    invalid_material_cost_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate supplier active flag**

# CELL ********************

supplier_active_flag_error_count = (
    silver_supplier_df
    .filter(
        F.col(
            "SupplierActiveFlag"
        )
        != (
            F.col("Status")
            == F.lit("Active")
        )
    )
    .count()
)

register_silver_validation(
    "silver_supplier",
    "Business Rule",
    (
        "SupplierActiveFlag matches "
        "supplier status"
    ),
    supplier_active_flag_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate exchange rates**

# CELL ********************

#Positive rates
invalid_exchange_rate_count = (
    silver_exchange_rate_df
    .filter(
        (
            F.col(
                "ExchangeRateToEUR"
            ) <= F.lit(0)
        )
        |
        (
            F.col("RateDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
    )
    .count()
)

register_silver_validation(
    "silver_exchange_rate",
    "Business Rule",
    (
        "Exchange rates are positive "
        "and within reporting period"
    ),
    invalid_exchange_rate_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#EUR always equal 1
invalid_eur_rate_count = (
    silver_exchange_rate_df
    .filter(
        (
            F.col("Currency")
            == "EUR"
        )
        &
        (
            F.abs(
                F.col(
                    "ExchangeRateToEUR"
                )
                - F.lit(1.0)
            )
            > F.lit(0.000001)
        )
    )
    .count()
)

register_silver_validation(
    "silver_exchange_rate",
    "Business Rule",
    (
        "EUR exchange rate to EUR "
        "equals 1"
    ),
    invalid_eur_rate_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Silver lineage metadata**

# CELL ********************

for (
    table_name,
    dataframe
) in silver_dataframes.items():

    invalid_metadata_count = (
        dataframe
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

    register_silver_validation(
        table_name=table_name,
        validation_category=(
            "Lineage"
        ),
        validation_rule=(
            "Bronze lineage and Silver "
            "audit metadata are complete"
        ),
        failed_record_count=(
            invalid_metadata_count
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build pre-write validation results**

# CELL ********************

silver_validation_results_df = (
    spark.createDataFrame(
        silver_validation_results
    )
    .withColumn(
        "ExecutionTimestamp",
        F.current_timestamp()
    )
)

display(
    silver_validation_results_df
    .orderBy(
        "ValidationStatus",
        "TableName",
        "ValidationCategory"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Count failures
pre_write_failure_count = (
    silver_validation_results_df
    .filter(
        F.col(
            "ValidationStatus"
        )
        == "FAILED"
    )
    .count()
)

print(
    f"Silver pre-write failures: "
    f"{pre_write_failure_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist pre-write monitoring results**
# 
# We persist validation evidence even if a transformation fails.

# CELL ********************

(
    silver_validation_results_df.write
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
    f"Monitoring table updated: "
    f"{SILVER_MONITORING_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Silver pre-write quality gate**

# CELL ********************

if pre_write_failure_count > 0:
    display(
        silver_validation_results_df
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
        f"Silver master-data validation "
        f"failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )

print(
    "SILVER PRE-WRITE QUALITY "
    "GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write physical Silver tables**
# 
# These are physical Delta tables in lh_procurement_silver.

# CELL ********************

for (
    table_name,
    dataframe
) in silver_dataframes.items():

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            table_name
        )
    )

    print(
        f"Created physical Silver table: "
        f"{table_name}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify physical Silver tables**

# CELL ********************

persisted_silver_results = []

for (
    table_name,
    source_dataframe
) in silver_dataframes.items():

    if not spark.catalog.tableExists(
        table_name
    ):
        persisted_silver_results.append({
            "TableName": table_name,
            "ExpectedRows": (
                source_dataframe.count()
            ),
            "PersistedRows": 0,
            "Difference": (
                -source_dataframe.count()
            ),
            "TableExists": False
        })

        continue

    expected_count = (
        source_dataframe.count()
    )

    persisted_count = (
        spark.table(
            table_name
        ).count()
    )

    persisted_silver_results.append({
        "TableName": table_name,
        "ExpectedRows": (
            expected_count
        ),
        "PersistedRows": (
            persisted_count
        ),
        "Difference": (
            persisted_count
            - expected_count
        ),
        "TableExists": True
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

persisted_silver_validation_df = (
    spark.createDataFrame(
        persisted_silver_results
    )
)

display(
    persisted_silver_validation_df
    .orderBy("TableName")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Post-write quality gate**

# CELL ********************

post_write_failure_count = (
    persisted_silver_validation_df
    .filter(
        (
            ~F.col("TableExists")
        )
        |
        (
            F.col("Difference") != 0
        )
    )
    .count()
)

print(
    f"Silver post-write failures: "
    f"{post_write_failure_count}"
)

assert (
    post_write_failure_count == 0
), (
    f"Found {post_write_failure_count} "
    "Silver persistence failures."
)

print(
    "SILVER POST-WRITE QUALITY "
    "GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final Silver inventory**

# CELL ********************

silver_inventory_rows = []

for table_name in (
    silver_dataframes.keys()
):
    dataframe = spark.table(
        table_name
    )

    silver_inventory_rows.append({
        "TableName": table_name,
        "RowCount": dataframe.count(),
        "ColumnCount": len(
            dataframe.columns
        )
    })

silver_inventory_df = (
    spark.createDataFrame(
        silver_inventory_rows
    )
)

display(
    silver_inventory_df
    .orderBy("TableName")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final notebook status**

# CELL ********************

print(
    "NB_20_Build_Silver_Master_Data "
    "completed successfully."
)

print()
print(
    "Bronze data source:"
)
print(
    "  lh_procurement_bronze"
)

print()
print(
    "Bronze access method:"
)
print(
    "  OneLake shortcuts"
)

print()
print(
    "Silver physical storage:"
)
print(
    "  lh_procurement_silver"
)

print()
print(
    "Created Silver tables:"
)

for table_name in (
    silver_dataframes.keys()
):
    print(
        f"  - {table_name}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
