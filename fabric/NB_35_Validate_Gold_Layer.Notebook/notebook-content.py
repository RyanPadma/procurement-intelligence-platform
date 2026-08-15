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

# This notebook does not create another business fact or dimension. Its output is the final Gold monitoring table:
# 
# _monitoring_gold_layer_quality_results_
# 
# It validates the complete Gold star schema and reconciles it back to Silver.

# MARKDOWN ********************

# **Configuration**

# CELL ********************

# ============================================================
# NB_35_Validate_Gold_Layer
# Configuration
# ============================================================

MAX_GOLD_STALENESS_DAYS = 2

MONETARY_TOLERANCE = 0.05


# ============================================================
# Silver Source Shortcuts
# ============================================================

SILVER_SUPPLIER_TABLE = "silver_supplier"
SILVER_CATEGORY_TABLE = "silver_category"
SILVER_MATERIAL_TABLE = "silver_material"
SILVER_BUYER_TABLE = "silver_buyer"
SILVER_BUSINESS_UNIT_TABLE = "silver_business_unit"
SILVER_CONTRACT_TABLE = "silver_contract"
SILVER_EXCHANGE_RATE_TABLE = "silver_exchange_rate"

SILVER_PO_TABLE = "silver_po_spend"
SILVER_INVOICE_TABLE = "silver_invoice_matching"
SILVER_SAVINGS_TABLE = "silver_savings_project"
SILVER_SUPPLIER_PERFORMANCE_TABLE = (
    "silver_supplier_performance"
)


# ============================================================
# Gold Dimensions
# ============================================================

DIM_DATE_TABLE = "dim_date"
DIM_SUPPLIER_TABLE = "dim_supplier"
DIM_CATEGORY_TABLE = "dim_category"
DIM_MATERIAL_TABLE = "dim_material"
DIM_BUYER_TABLE = "dim_buyer"
DIM_BUSINESS_UNIT_TABLE = "dim_business_unit"
DIM_CONTRACT_TABLE = "dim_contract"
DIM_CURRENCY_TABLE = "dim_currency"


# ============================================================
# Gold Facts
# ============================================================

FACT_PO_TABLE = "fact_purchase_order"
FACT_INVOICE_TABLE = "fact_invoice"
FACT_SAVINGS_TABLE = "fact_savings"
FACT_SUPPLIER_PERFORMANCE_TABLE = (
    "fact_supplier_performance"
)


# ============================================================
# Upstream Gold Monitoring
# ============================================================

GOLD_DIMENSION_MONITORING_TABLE = (
    "monitoring_gold_dimensions_quality_results"
)

GOLD_PO_MONITORING_TABLE = (
    "monitoring_gold_fact_purchase_order_quality_results"
)

GOLD_INVOICE_MONITORING_TABLE = (
    "monitoring_gold_fact_invoice_quality_results"
)

GOLD_SAVINGS_MONITORING_TABLE = (
    "monitoring_gold_fact_savings_quality_results"
)

GOLD_SUPPLIER_PERFORMANCE_MONITORING_TABLE = (
    "monitoring_gold_fact_supplier_performance_quality_results"
)


# ============================================================
# Final Gold Monitoring Output
# ============================================================

GOLD_LAYER_MONITORING_TABLE = (
    "monitoring_gold_layer_quality_results"
)


print(
    "Notebook: NB_35_Validate_Gold_Layer"
)

print(
    "Default Lakehouse: lh_procurement_gold"
)

print(
    "Output:",
    GOLD_LAYER_MONITORING_TABLE
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
    # Silver
    SILVER_SUPPLIER_TABLE,
    SILVER_CATEGORY_TABLE,
    SILVER_MATERIAL_TABLE,
    SILVER_BUYER_TABLE,
    SILVER_BUSINESS_UNIT_TABLE,
    SILVER_CONTRACT_TABLE,
    SILVER_EXCHANGE_RATE_TABLE,

    SILVER_PO_TABLE,
    SILVER_INVOICE_TABLE,
    SILVER_SAVINGS_TABLE,
    SILVER_SUPPLIER_PERFORMANCE_TABLE,

    # Gold dimensions
    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE,
    DIM_CATEGORY_TABLE,
    DIM_MATERIAL_TABLE,
    DIM_BUYER_TABLE,
    DIM_BUSINESS_UNIT_TABLE,
    DIM_CONTRACT_TABLE,
    DIM_CURRENCY_TABLE,

    # Gold facts
    FACT_PO_TABLE,
    FACT_INVOICE_TABLE,
    FACT_SAVINGS_TABLE,
    FACT_SUPPLIER_PERFORMANCE_TABLE,

    # Gold monitoring
    GOLD_DIMENSION_MONITORING_TABLE,
    GOLD_PO_MONITORING_TABLE,
    GOLD_INVOICE_MONITORING_TABLE,
    GOLD_SAVINGS_MONITORING_TABLE,
    GOLD_SUPPLIER_PERFORMANCE_MONITORING_TABLE
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
        "Missing required Gold/Silver tables: "
        +
        ", ".join(
            missing_tables
        )
    )


print(
    "All required Gold and Silver tables exist."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load Gold dimensions**

# CELL ********************

dim_date_df = spark.table(
    DIM_DATE_TABLE
)

dim_supplier_df = spark.table(
    DIM_SUPPLIER_TABLE
)

dim_category_df = spark.table(
    DIM_CATEGORY_TABLE
)

dim_material_df = spark.table(
    DIM_MATERIAL_TABLE
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

# MARKDOWN ********************

# **Load Gold facts**

# CELL ********************

fact_po_df = spark.table(
    FACT_PO_TABLE
)

fact_invoice_df = spark.table(
    FACT_INVOICE_TABLE
)

fact_savings_df = spark.table(
    FACT_SAVINGS_TABLE
)

fact_supplier_performance_df = (
    spark.table(
        FACT_SUPPLIER_PERFORMANCE_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load Silver reconciliation sources**

# CELL ********************

silver_supplier_df = spark.table(
    SILVER_SUPPLIER_TABLE
)

silver_category_df = spark.table(
    SILVER_CATEGORY_TABLE
)

silver_material_df = spark.table(
    SILVER_MATERIAL_TABLE
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


silver_po_df = spark.table(
    SILVER_PO_TABLE
)

silver_invoice_df = spark.table(
    SILVER_INVOICE_TABLE
)

silver_savings_df = spark.table(
    SILVER_SAVINGS_TABLE
)

silver_supplier_performance_df = (
    spark.table(
        SILVER_SUPPLIER_PERFORMANCE_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Show Gold table populations**

# CELL ********************

gold_population_summary = [
    (
        DIM_DATE_TABLE,
        dim_date_df.count()
    ),
    (
        DIM_SUPPLIER_TABLE,
        dim_supplier_df.count()
    ),
    (
        DIM_CATEGORY_TABLE,
        dim_category_df.count()
    ),
    (
        DIM_MATERIAL_TABLE,
        dim_material_df.count()
    ),
    (
        DIM_BUYER_TABLE,
        dim_buyer_df.count()
    ),
    (
        DIM_BUSINESS_UNIT_TABLE,
        dim_business_unit_df.count()
    ),
    (
        DIM_CONTRACT_TABLE,
        dim_contract_df.count()
    ),
    (
        DIM_CURRENCY_TABLE,
        dim_currency_df.count()
    ),
    (
        FACT_PO_TABLE,
        fact_po_df.count()
    ),
    (
        FACT_INVOICE_TABLE,
        fact_invoice_df.count()
    ),
    (
        FACT_SAVINGS_TABLE,
        fact_savings_df.count()
    ),
    (
        FACT_SUPPLIER_PERFORMANCE_TABLE,
        fact_supplier_performance_df.count()
    )
]


gold_population_df = (
    spark.createDataFrame(
        gold_population_summary,
        [
            "TableName",
            "RowCount"
        ]
    )
)


display(
    gold_population_df
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
    table_name,
    rule,
    failed_count,
    details=""
):

    failed_count = int(
        failed_count or 0
    )

    validation_results.append(
        {
            "ValidationLayer":
                "GOLD",

            "ValidationCategory":
                category,

            "TableName":
                table_name,

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

# **Validate upstream Gold quality gates**

# CELL ********************

upstream_monitoring_tables = [
    (
        "NB_30 Gold Dimensions",
        GOLD_DIMENSION_MONITORING_TABLE
    ),

    (
        "NB_31 Purchase Order Fact",
        GOLD_PO_MONITORING_TABLE
    ),

    (
        "NB_32 Invoice Fact",
        GOLD_INVOICE_MONITORING_TABLE
    ),

    (
        "NB_33 Savings Fact",
        GOLD_SAVINGS_MONITORING_TABLE
    ),

    (
        "NB_34 Supplier Performance Fact",
        GOLD_SUPPLIER_PERFORMANCE_MONITORING_TABLE
    )
]


for (
    notebook_name,
    monitoring_table
) in upstream_monitoring_tables:

    failed_count = (
        spark.table(
            monitoring_table
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


    register_validation(
        category="Upstream Quality Gate",

        table_name=monitoring_table,

        rule=(
            f"{notebook_name} "
            f"completed with zero failed rules"
        ),

        failed_count=failed_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Generic uniqueness helper**

# CELL ********************

def validate_unique_key(
    dataframe,
    table_name,
    key_columns,
    rule
):

    duplicate_count = (
        dataframe

        .groupBy(
            *key_columns
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
        category="Grain / Key Integrity",
        table_name=table_name,
        rule=rule,
        failed_count=duplicate_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate dimension surrogate keys**

# CELL ********************

validate_unique_key(
    dim_date_df,
    DIM_DATE_TABLE,
    [
        "DateKey"
    ],
    "DateKey is unique"
)


validate_unique_key(
    dim_supplier_df,
    DIM_SUPPLIER_TABLE,
    [
        "SupplierKey"
    ],
    "SupplierKey is unique across SCD history"
)


validate_unique_key(
    dim_category_df,
    DIM_CATEGORY_TABLE,
    [
        "CategoryKey"
    ],
    "CategoryKey is unique"
)


validate_unique_key(
    dim_material_df,
    DIM_MATERIAL_TABLE,
    [
        "MaterialKey"
    ],
    "MaterialKey is unique"
)


validate_unique_key(
    dim_buyer_df,
    DIM_BUYER_TABLE,
    [
        "BuyerKey"
    ],
    "BuyerKey is unique"
)


validate_unique_key(
    dim_business_unit_df,
    DIM_BUSINESS_UNIT_TABLE,
    [
        "BusinessUnitKey"
    ],
    "BusinessUnitKey is unique"
)


validate_unique_key(
    dim_contract_df,
    DIM_CONTRACT_TABLE,
    [
        "ContractKey"
    ],
    "ContractKey is unique"
)


validate_unique_key(
    dim_currency_df,
    DIM_CURRENCY_TABLE,
    [
        "CurrencyKey"
    ],
    "CurrencyKey is unique"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate fact grains**

# CELL ********************

validate_unique_key(
    fact_po_df,
    FACT_PO_TABLE,
    [
        "POItemID"
    ],
    "One row exists per POItemID"
)


validate_unique_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    [
        "InvoiceItemID"
    ],
    "One row exists per InvoiceItemID"
)


validate_unique_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    [
        "SavingsProjectID"
    ],
    "One row exists per SavingsProjectID"
)


validate_unique_key(
    fact_supplier_performance_df,
    FACT_SUPPLIER_PERFORMANCE_TABLE,
    [
        "SupplierID",
        "PerformanceYear"
    ],
    (
        "One row exists per "
        "SupplierID x PerformanceYear"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate fact surrogate keys**

# CELL ********************

validate_unique_key(
    fact_po_df,
    FACT_PO_TABLE,
    [
        "PurchaseOrderFactKey"
    ],
    "PurchaseOrderFactKey is unique"
)


validate_unique_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    [
        "InvoiceFactKey"
    ],
    "InvoiceFactKey is unique"
)


validate_unique_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    [
        "SavingsFactKey"
    ],
    "SavingsFactKey is unique"
)


validate_unique_key(
    fact_supplier_performance_df,
    FACT_SUPPLIER_PERFORMANCE_TABLE,
    [
        "SupplierPerformanceFactKey"
    ],
    "SupplierPerformanceFactKey is unique"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier SCD2 current-version coverage**

# CELL ********************

current_supplier_versions_df = (
    dim_supplier_df

    .filter(
        F.col(
            "IsCurrentFlag"
        )
    )

    .groupBy(
        "SupplierID"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "CurrentVersionCount"
        )
    )
)


invalid_current_supplier_count = (
    silver_supplier_df

    .select(
        "SupplierID"
    )

    .distinct()

    .join(
        current_supplier_versions_df,
        "SupplierID",
        "left"
    )

    .filter(
        F.coalesce(
            F.col(
                "CurrentVersionCount"
            ),
            F.lit(0)
        )
        !=
        1
    )

    .count()
)


register_validation(
    "SCD Type 2",
    DIM_SUPPLIER_TABLE,
    (
        "Every current Silver supplier has "
        "exactly one current Gold SCD version"
    ),
    invalid_current_supplier_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier SCD date ranges**

# CELL ********************

invalid_supplier_date_range_count = (
    dim_supplier_df

    .filter(
        F.col(
            "EffectiveFromDate"
        ).isNull()
        |
        F.col(
            "EffectiveToDate"
        ).isNull()
        |
        (
            F.col(
                "EffectiveToDate"
            )
            <
            F.col(
                "EffectiveFromDate"
            )
        )
    )

    .count()
)


register_validation(
    "SCD Type 2",
    DIM_SUPPLIER_TABLE,
    (
        "Supplier SCD effective-date "
        "ranges are valid"
    ),
    invalid_supplier_date_range_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate SUpplier SCD2 ranges do not overlap**

# CELL ********************

supplier_scd_window = (
    Window
    .partitionBy(
        "SupplierID"
    )
    .orderBy(
        "EffectiveFromDate",
        "DimensionVersion"
    )
)


supplier_scd_overlap_df = (
    dim_supplier_df

    .withColumn(
        "PreviousEffectiveToDate",

        F.lag(
            "EffectiveToDate"
        ).over(
            supplier_scd_window
        )
    )

    .filter(
        F.col(
            "PreviousEffectiveToDate"
        ).isNotNull()
        &
        (
            F.col(
                "EffectiveFromDate"
            )
            <=
            F.col(
                "PreviousEffectiveToDate"
            )
        )
    )
)


supplier_scd_overlap_count = (
    supplier_scd_overlap_df.count()
)


register_validation(
    "SCD Type 2",
    DIM_SUPPLIER_TABLE,
    (
        "Supplier SCD versions "
        "do not overlap"
    ),
    supplier_scd_overlap_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Type 1 dimension row preservation**

# CELL ********************

dimension_row_checks = [
    (
        DIM_CATEGORY_TABLE,
        dim_category_df,
        silver_category_df,
        "CategoryID"
    ),

    (
        DIM_MATERIAL_TABLE,
        dim_material_df,
        silver_material_df,
        "MaterialID"
    ),

    (
        DIM_BUYER_TABLE,
        dim_buyer_df,
        silver_buyer_df,
        "BuyerID"
    ),

    (
        DIM_BUSINESS_UNIT_TABLE,
        dim_business_unit_df,
        silver_business_unit_df,
        "BusinessUnitID"
    ),

    (
        DIM_CONTRACT_TABLE,
        dim_contract_df,
        silver_contract_df,
        "ContractID"
    )
]


for (
    table_name,
    gold_df,
    silver_df,
    natural_key
) in dimension_row_checks:

    silver_count = (
        silver_df

        .select(
            natural_key
        )

        .distinct()

        .count()
    )

    gold_count = (
        gold_df

        .select(
            natural_key
        )

        .distinct()

        .count()
    )


    register_validation(
        "Dimension Reconciliation",
        table_name,
        (
            f"Gold {natural_key} population "
            f"matches Silver"
        ),
        abs(
            silver_count
            -
            gold_count
        ),
        (
            f"Silver: {silver_count:,}; "
            f"Gold: {gold_count:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Currency dimension population**

# CELL ********************

silver_currency_count = (
    silver_exchange_rate_df

    .select(
        "Currency"
    )

    .distinct()

    .count()
)


gold_currency_count = (
    dim_currency_df

    .select(
        "CurrencyCode"
    )

    .distinct()

    .count()
)


register_validation(
    "Dimension Reconciliation",
    DIM_CURRENCY_TABLE,
    (
        "Gold Currency population "
        "matches Silver FX currencies"
    ),
    abs(
        silver_currency_count
        -
        gold_currency_count
    ),
    (
        f"Silver: {silver_currency_count:,}; "
        f"Gold: {gold_currency_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Generic orphan-key helper**

# CELL ********************

def validate_orphan_key(
    fact_df,
    fact_table,
    fact_key,
    dimension_df,
    dimension_key
):

    orphan_count = (
        fact_df

        .filter(
            F.col(
                fact_key
            ).isNotNull()
        )

        .select(
            F.col(
                fact_key
            ).alias(
                "_FactKey"
            )
        )

        .distinct()

        .join(
            dimension_df

            .select(
                F.col(
                    dimension_key
                ).alias(
                    "_DimensionKey"
                )
            )

            .distinct(),

            F.col(
                "_FactKey"
            )
            ==
            F.col(
                "_DimensionKey"
            ),

            "left_anti"
        )

        .count()
    )


    register_validation(
        "Referential Integrity",
        fact_table,
        (
            f"{fact_key} contains "
            f"no orphan dimension keys"
        ),
        orphan_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Purchase Order Fact dimension relationships**

# CELL ********************

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "SupplierKey",
    dim_supplier_df,
    "SupplierKey"
)

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "CategoryKey",
    dim_category_df,
    "CategoryKey"
)

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "MaterialKey",
    dim_material_df,
    "MaterialKey"
)

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "BuyerKey",
    dim_buyer_df,
    "BuyerKey"
)

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "BusinessUnitKey",
    dim_business_unit_df,
    "BusinessUnitKey"
)

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "ContractKey",
    dim_contract_df,
    "ContractKey"
)

validate_orphan_key(
    fact_po_df,
    FACT_PO_TABLE,
    "CurrencyKey",
    dim_currency_df,
    "CurrencyKey"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Invoice Fact dimension relationships**

# CELL ********************

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "SupplierKey",
    dim_supplier_df,
    "SupplierKey"
)

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "CategoryKey",
    dim_category_df,
    "CategoryKey"
)

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "MaterialKey",
    dim_material_df,
    "MaterialKey"
)

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "BuyerKey",
    dim_buyer_df,
    "BuyerKey"
)

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "BusinessUnitKey",
    dim_business_unit_df,
    "BusinessUnitKey"
)

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "ContractKey",
    dim_contract_df,
    "ContractKey"
)

validate_orphan_key(
    fact_invoice_df,
    FACT_INVOICE_TABLE,
    "CurrencyKey",
    dim_currency_df,
    "CurrencyKey"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Savings Fact dimension relationships**

# CELL ********************

validate_orphan_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    "SupplierKey",
    dim_supplier_df,
    "SupplierKey"
)

validate_orphan_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    "CategoryKey",
    dim_category_df,
    "CategoryKey"
)

validate_orphan_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    "BuyerKey",
    dim_buyer_df,
    "BuyerKey"
)

validate_orphan_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    "BusinessUnitKey",
    dim_business_unit_df,
    "BusinessUnitKey"
)

validate_orphan_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    "ContractKey",
    dim_contract_df,
    "ContractKey"
)

validate_orphan_key(
    fact_savings_df,
    FACT_SAVINGS_TABLE,
    "CurrencyKey",
    dim_currency_df,
    "CurrencyKey"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier Performance dimension relationship**

# CELL ********************

validate_orphan_key(
    fact_supplier_performance_df,
    FACT_SUPPLIER_PERFORMANCE_TABLE,
    "SupplierKey",
    dim_supplier_df,
    "SupplierKey"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Generic DateKey validation helper**

# CELL ********************

def validate_date_key(
    fact_df,
    fact_table,
    date_key_column
):

    orphan_count = (
        fact_df

        .filter(
            F.col(
                date_key_column
            ).isNotNull()
        )

        .select(
            F.col(
                date_key_column
            ).alias(
                "_FactDateKey"
            )
        )

        .distinct()

        .join(
            dim_date_df

            .select(
                F.col(
                    "DateKey"
                ).alias(
                    "_DimensionDateKey"
                )
            )

            .distinct(),

            F.col(
                "_FactDateKey"
            )
            ==
            F.col(
                "_DimensionDateKey"
            ),

            "left_anti"
        )

        .count()
    )


    register_validation(
        "Date Dimension",
        fact_table,
        (
            f"{date_key_column} "
            f"resolves to dim_date"
        ),
        orphan_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate all Purchase Order date roles**

# CELL ********************

for date_key in [
    "OrderDateKey",
    "RequestedDeliveryDateKey"
]:

    validate_date_key(
        fact_po_df,
        FACT_PO_TABLE,
        date_key
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate all Invoice date roles**

# CELL ********************

for date_key in [
    "InvoiceDateKey",
    "PostingDateKey",
    "DueDateKey",
    "OrderDateKey",
    "RequestedDeliveryDateKey"
]:

    validate_date_key(
        fact_invoice_df,
        FACT_INVOICE_TABLE,
        date_key
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate all Savings date roles**

# CELL ********************

for date_key in [
    "ProjectCreatedDateKey",
    "PlannedStartDateKey",
    "PlannedCompletionDateKey",
    "ActualCompletionDateKey",
    "CancellationDateKey"
]:

    validate_date_key(
        fact_savings_df,
        FACT_SAVINGS_TABLE,
        date_key
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier Performance date roles**

# CELL ********************

for date_key in [
    "PerformancePeriodStartDateKey",
    "PerformancePeriodEndDateKey"
]:

    validate_date_key(
        fact_supplier_performance_df,
        FACT_SUPPLIER_PERFORMANCE_TABLE,
        date_key
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Gold Date dimension covers all Gold dates**

# CELL ********************

gold_date_range = (
    dim_date_df

    .agg(
        F.min(
            "Date"
        ).alias(
            "DateStart"
        ),

        F.max(
            "Date"
        ).alias(
            "DateEnd"
        )
    )

    .collect()[0]
)


gold_date_start = (
    gold_date_range[
        "DateStart"
    ]
)

gold_date_end = (
    gold_date_range[
        "DateEnd"
    ]
)


print(
    "Gold Date coverage:",
    gold_date_start,
    "to",
    gold_date_end
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

maximum_business_date_df = (
    fact_savings_df

    .agg(
        F.max(
            F.greatest(
                "ProjectCreatedDate",
                "PlannedStartDate",
                "PlannedCompletionDate",
                "ActualCompletionDate",
                "CancellationDate"
            )
        ).alias(
            "MaxBusinessDate"
        )
    )
)


maximum_business_date = (
    maximum_business_date_df
    .collect()[0][
        "MaxBusinessDate"
    ]
)


date_coverage_failure = (
    1
    if (
        maximum_business_date is not None
        and
        maximum_business_date > gold_date_end
    )
    else 0
)


register_validation(
    "Date Dimension",
    DIM_DATE_TABLE,
    (
        "Gold Date dimension covers "
        "forward savings planning dates"
    ),
    date_coverage_failure,
    (
        f"Gold Date end: {gold_date_end}; "
        f"Latest savings date: "
        f"{maximum_business_date}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Gold-to-Silver row counts**

# CELL ********************

row_count_reconciliations = [
    (
        FACT_PO_TABLE,
        silver_po_df.count(),
        fact_po_df.count()
    ),

    (
        FACT_INVOICE_TABLE,
        silver_invoice_df.count(),
        fact_invoice_df.count()
    ),

    (
        FACT_SAVINGS_TABLE,
        silver_savings_df.count(),
        fact_savings_df.count()
    ),

    (
        FACT_SUPPLIER_PERFORMANCE_TABLE,
        silver_supplier_performance_df.count(),
        fact_supplier_performance_df.count()
    )
]


for (
    table_name,
    silver_count,
    gold_count
) in row_count_reconciliations:

    register_validation(
        "Silver-to-Gold Reconciliation",
        table_name,
        (
            "Gold fact row count "
            "matches Silver source"
        ),
        abs(
            silver_count
            -
            gold_count
        ),
        (
            f"Silver: {silver_count:,}; "
            f"Gold: {gold_count:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Monetary reconciliation helper**

# CELL ********************

def reconcile_sum(
    silver_df,
    silver_column,
    gold_df,
    gold_column,
    table_name,
    rule
):

    silver_value = (
        silver_df

        .agg(
            F.sum(
                silver_column
            ).alias(
                "Value"
            )
        )

        .collect()[0][
            "Value"
        ]
    )


    gold_value = (
        gold_df

        .agg(
            F.sum(
                gold_column
            ).alias(
                "Value"
            )
        )

        .collect()[0][
            "Value"
        ]
    )


    silver_numeric = (
        float(
            silver_value
        )
        if silver_value is not None
        else 0.0
    )

    gold_numeric = (
        float(
            gold_value
        )
        if gold_value is not None
        else 0.0
    )


    difference = abs(
        silver_numeric
        -
        gold_numeric
    )


    register_validation(
        "Silver-to-Gold Reconciliation",
        table_name,
        rule,
        (
            0
            if difference
            <= MONETARY_TOLERANCE
            else 1
        ),
        (
            f"Silver: {silver_numeric:,.2f}; "
            f"Gold: {gold_numeric:,.2f}; "
            f"Difference: {difference:,.4f}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reconcile Purchase Order measures**

# CELL ********************

for measure in [
    "LineAmountEUR",
    "EligibleSpendEUR",
    "ContractCompliantSpendEUR",
    "MaverickSpendEUR"
]:

    reconcile_sum(
        silver_df=silver_po_df,
        silver_column=measure,

        gold_df=fact_po_df,
        gold_column=measure,

        table_name=FACT_PO_TABLE,

        rule=(
            f"{measure} total "
            f"matches silver_po_spend"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reconcile Invoice measures**

# CELL ********************

for measure in [
    "NetAmountEUR",
    "TaxAmountEUR",
    "GrossAmountEUR",
    "InvoiceExceptionAmountEUR"
]:

    reconcile_sum(
        silver_df=silver_invoice_df,
        silver_column=measure,

        gold_df=fact_invoice_df,
        gold_column=measure,

        table_name=FACT_INVOICE_TABLE,

        rule=(
            f"{measure} total "
            f"matches silver_invoice_matching"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reconcile Savings measures**

# CELL ********************

for measure in [
    "BaselineSpendEUR",
    "ForecastedSavingsEUR",
    "WeightedForecastSavingsEUR",
    "ApprovedSavingsEUR",
    "RealizedSavingsEUR",
    "ActivePipelineWeightedForecastEUR",
    "ImplementedSavingsEUR"
]:

    reconcile_sum(
        silver_df=silver_savings_df,
        silver_column=measure,

        gold_df=fact_savings_df,
        gold_column=measure,

        table_name=FACT_SAVINGS_TABLE,

        rule=(
            f"{measure} total "
            f"matches silver_savings_project"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reconcile Supplier Performance spend measures**

# CELL ********************

for measure in [
    "EligibleSpendEUR",
    "ContractCompliantSpendEUR",
    "MaverickSpendEUR"
]:

    reconcile_sum(
        silver_df=(
            silver_supplier_performance_df
        ),
        silver_column=measure,

        gold_df=(
            fact_supplier_performance_df
        ),
        gold_column=measure,

        table_name=(
            FACT_SUPPLIER_PERFORMANCE_TABLE
        ),

        rule=(
            f"{measure} total matches "
            f"silver_supplier_performance"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reconcile Supplier Performance operational counts**
# 
# The Silver names differ slightly from the standardized Gold names.

# CELL ********************

supplier_performance_count_mapping = [
    (
        "DeliveryCompletePOItemCount",
        "FullyReceivedPOItemCount"
    ),

    (
        "OnTimeDeliveryPOItemCount",
        "OnTimeFullyReceivedPOItemCount"
    ),

    (
        "OverdueOpenPOItemCount",
        "OverdueOpenDeliveryCount"
    ),

    (
        "InvoiceCount",
        "InvoiceCount"
    ),

    (
        "DisputeFreeInvoiceCount",
        "DisputeFreeInvoiceCount"
    ),

    (
        "DuplicateInvoiceCount",
        "DuplicateInvoiceCount"
    ),

    (
        "InvoiceItemCount",
        "InvoiceItemCount"
    ),

    (
        "MatchedInvoiceItemCount",
        "ThreeWayMatchedInvoiceItemCount"
    ),

    (
        "InvoiceExceptionItemCount",
        "InvoiceExceptionItemCount"
    )
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for (
    silver_column,
    gold_column
) in supplier_performance_count_mapping:

    silver_value = (
        silver_supplier_performance_df

        .agg(
            F.sum(
                silver_column
            ).alias(
                "Value"
            )
        )

        .collect()[0][
            "Value"
        ]
    )


    gold_value = (
        fact_supplier_performance_df

        .agg(
            F.sum(
                gold_column
            ).alias(
                "Value"
            )
        )

        .collect()[0][
            "Value"
        ]
    )


    silver_numeric = int(
        silver_value or 0
    )

    gold_numeric = int(
        gold_value or 0
    )


    register_validation(
        "Silver-to-Gold Reconciliation",
        FACT_SUPPLIER_PERFORMANCE_TABLE,
        (
            f"{gold_column} matches "
            f"Silver {silver_column}"
        ),
        abs(
            silver_numeric
            -
            gold_numeric
        ),
        (
            f"Silver: {silver_numeric:,}; "
            f"Gold: {gold_numeric:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cross-fact Invoice -> Purchase Order relationship**

# CELL ********************

orphan_purchase_order_fact_key_count = (
    fact_invoice_df

    .filter(
        F.col(
            "PurchaseOrderFactKey"
        ).isNotNull()
    )

    .select(
        "PurchaseOrderFactKey"
    )

    .distinct()

    .join(
        fact_po_df

        .select(
            "PurchaseOrderFactKey"
        )

        .distinct(),

        "PurchaseOrderFactKey",

        "left_anti"
    )

    .count()
)


register_validation(
    "Cross-Fact Integrity",
    FACT_INVOICE_TABLE,
    (
        "Invoice PurchaseOrderFactKey "
        "resolves to fact_purchase_order"
    ),
    orphan_purchase_order_fact_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cross-fact PO business-key consistency**

# CELL ********************

invoice_po_consistency_error_count = (
    fact_invoice_df.alias(
        "invoice"
    )

    .filter(
        F.col(
            "invoice.PurchaseOrderFactKey"
        ).isNotNull()
    )

    .join(
        fact_po_df.alias(
            "po"
        ),

        F.col(
            "invoice.PurchaseOrderFactKey"
        )
        ==
        F.col(
            "po.PurchaseOrderFactKey"
        ),

        "inner"
    )

    .filter(
        (
            F.col(
                "invoice.POID"
            )
            !=
            F.col(
                "po.POID"
            )
        )
        |
        (
            F.col(
                "invoice.POItemID"
            )
            !=
            F.col(
                "po.POItemID"
            )
        )
    )

    .count()
)


register_validation(
    "Cross-Fact Integrity",
    FACT_INVOICE_TABLE,
    (
        "Invoice POID and POItemID agree "
        "with fact_purchase_order"
    ),
    invoice_po_consistency_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate inherited PO dimension keys in Invoice Fact**

# CELL ********************

invoice_po_dimension_mismatch_count = (
    fact_invoice_df.alias(
        "invoice"
    )

    .filter(
        F.col(
            "invoice.PurchaseOrderFactKey"
        ).isNotNull()
    )

    .join(
        fact_po_df.alias(
            "po"
        ),

        F.col(
            "invoice.PurchaseOrderFactKey"
        )
        ==
        F.col(
            "po.PurchaseOrderFactKey"
        ),

        "inner"
    )

    .filter(
        (
            F.col(
                "invoice.CategoryKey"
            )
            !=
            F.col(
                "po.CategoryKey"
            )
        )
        |
        (
            F.col(
                "invoice.MaterialKey"
            )
            !=
            F.col(
                "po.MaterialKey"
            )
        )
        |
        (
            F.col(
                "invoice.BuyerKey"
            )
            !=
            F.col(
                "po.BuyerKey"
            )
        )
        |
        (
            F.col(
                "invoice.BusinessUnitKey"
            )
            !=
            F.col(
                "po.BusinessUnitKey"
            )
        )
    )

    .count()
)


register_validation(
    "Cross-Fact Integrity",
    FACT_INVOICE_TABLE,
    (
        "Invoice PO-derived dimension keys "
        "match fact_purchase_order"
    ),
    invoice_po_dimension_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cross-check Supplier Performance spend against PO fact**
# 
# This validates that the annual supplier-performance snapshot reconciles to the transactional Purchase Order fact.

# CELL ********************

po_annual_spend_df = (
    fact_po_df

    .join(
        dim_date_df

        .select(
            F.col(
                "DateKey"
            ).alias(
                "_OrderDateKey"
            ),

            F.col(
                "Year"
            ).alias(
                "_PerformanceYear"
            )
        ),

        F.col(
            "OrderDateKey"
        )
        ==
        F.col(
            "_OrderDateKey"
        ),

        "left"
    )

    .groupBy(
        "_PerformanceYear"
    )

    .agg(
        F.sum(
            "EligibleSpendEUR"
        ).alias(
            "POEligibleSpendEUR"
        ),

        F.sum(
            "ContractCompliantSpendEUR"
        ).alias(
            "POContractCompliantSpendEUR"
        ),

        F.sum(
            "MaverickSpendEUR"
        ).alias(
            "POMaverickSpendEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_performance_annual_spend_df = (
    fact_supplier_performance_df

    .groupBy(
        "PerformanceYear"
    )

    .agg(
        F.sum(
            "EligibleSpendEUR"
        ).alias(
            "PerformanceEligibleSpendEUR"
        ),

        F.sum(
            "ContractCompliantSpendEUR"
        ).alias(
            "PerformanceContractCompliantSpendEUR"
        ),

        F.sum(
            "MaverickSpendEUR"
        ).alias(
            "PerformanceMaverickSpendEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_performance_spend_reconciliation_df = (
    po_annual_spend_df

    .join(
        supplier_performance_annual_spend_df,

        F.col(
            "_PerformanceYear"
        )
        ==
        F.col(
            "PerformanceYear"
        ),

        "full"
    )

    .withColumn(
        "EligibleSpendDifference",

        F.abs(
            F.coalesce(
                F.col(
                    "POEligibleSpendEUR"
                ),
                F.lit(0)
            )
            -
            F.coalesce(
                F.col(
                    "PerformanceEligibleSpendEUR"
                ),
                F.lit(0)
            )
        )
    )

    .withColumn(
        "ContractSpendDifference",

        F.abs(
            F.coalesce(
                F.col(
                    "POContractCompliantSpendEUR"
                ),
                F.lit(0)
            )
            -
            F.coalesce(
                F.col(
                    "PerformanceContractCompliantSpendEUR"
                ),
                F.lit(0)
            )
        )
    )

    .withColumn(
        "MaverickSpendDifference",

        F.abs(
            F.coalesce(
                F.col(
                    "POMaverickSpendEUR"
                ),
                F.lit(0)
            )
            -
            F.coalesce(
                F.col(
                    "PerformanceMaverickSpendEUR"
                ),
                F.lit(0)
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

supplier_performance_spend_failure_count = (
    supplier_performance_spend_reconciliation_df

    .filter(
        (
            F.col(
                "EligibleSpendDifference"
            )
            >
            MONETARY_TOLERANCE
        )
        |
        (
            F.col(
                "ContractSpendDifference"
            )
            >
            MONETARY_TOLERANCE
        )
        |
        (
            F.col(
                "MaverickSpendDifference"
            )
            >
            MONETARY_TOLERANCE
        )
    )

    .count()
)


register_validation(
    "Cross-Fact Reconciliation",
    FACT_SUPPLIER_PERFORMANCE_TABLE,
    (
        "Annual spend KPIs reconcile "
        "to fact_purchase_order"
    ),
    supplier_performance_spend_failure_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Display annual PO vs Supplier Performance reconciliation**

# CELL ********************

display(
    supplier_performance_spend_reconciliation_df

    .orderBy(
        F.coalesce(
            F.col(
                "_PerformanceYear"
            ),
            F.col(
                "PerformanceYear"
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

# **Validate Gold metadata helper**

# CELL ********************

def validate_gold_metadata(
    dataframe,
    table_name
):

    invalid_metadata_count = (
        dataframe

        .filter(
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
        "Gold Metadata",
        table_name,
        (
            "Gold load metadata and "
            "record hash are complete"
        ),
        invalid_metadata_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Gold metadata across all tables**

# CELL ********************

gold_tables_for_metadata = [
    (
        DIM_DATE_TABLE,
        dim_date_df
    ),
    (
        DIM_SUPPLIER_TABLE,
        dim_supplier_df
    ),
    (
        DIM_CATEGORY_TABLE,
        dim_category_df
    ),
    (
        DIM_MATERIAL_TABLE,
        dim_material_df
    ),
    (
        DIM_BUYER_TABLE,
        dim_buyer_df
    ),
    (
        DIM_BUSINESS_UNIT_TABLE,
        dim_business_unit_df
    ),
    (
        DIM_CONTRACT_TABLE,
        dim_contract_df
    ),
    (
        DIM_CURRENCY_TABLE,
        dim_currency_df
    ),
    (
        FACT_PO_TABLE,
        fact_po_df
    ),
    (
        FACT_INVOICE_TABLE,
        fact_invoice_df
    ),
    (
        FACT_SAVINGS_TABLE,
        fact_savings_df
    ),
    (
        FACT_SUPPLIER_PERFORMANCE_TABLE,
        fact_supplier_performance_df
    )
]


for (
    table_name,
    dataframe
) in gold_tables_for_metadata:

    validate_gold_metadata(
        dataframe,
        table_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate GOld freshness**

# CELL ********************

for (
    table_name,
    dataframe
) in gold_tables_for_metadata:

    latest_load_date = (
        dataframe

        .agg(
            F.max(
                "GoldLoadDate"
            ).alias(
                "LatestLoadDate"
            )
        )

        .collect()[0][
            "LatestLoadDate"
        ]
    )


    if latest_load_date is None:

        stale_failure_count = 1
        freshness_details = (
            "No GoldLoadDate found."
        )

    else:

        days_old = (
            spark

            .range(
                1
            )

            .select(
                F.datediff(
                    F.current_date(),
                    F.lit(
                        latest_load_date
                    )
                ).alias(
                    "DaysOld"
                )
            )

            .collect()[0][
                "DaysOld"
            ]
        )


        stale_failure_count = (
            1
            if days_old
            >
            MAX_GOLD_STALENESS_DAYS
            else 0
        )


        freshness_details = (
            f"Latest GoldLoadDate: "
            f"{latest_load_date}; "
            f"Age: {days_old} day(s)"
        )


    register_validation(
        "Freshness",
        table_name,
        (
            f"Gold table refreshed within "
            f"{MAX_GOLD_STALENESS_DAYS} days"
        ),
        stale_failure_count,
        freshness_details
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build final validation results**

# CELL ********************

gold_validation_results_df = (
    spark.createDataFrame(
        validation_results
    )

    .withColumn(
        "ExecutionTimestamp",
        F.current_timestamp()
    )

    .withColumn(
        "ExecutionDate",
        F.current_date()
    )
)


display(
    gold_validation_results_df

    .orderBy(
        "ValidationStatus",
        "ValidationCategory",
        "TableName",
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

gold_validation_summary_df = (
    gold_validation_results_df

    .groupBy(
        "ValidationStatus"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "ValidationRuleCount"
        ),

        F.sum(
            "FailedRecordCount"
        ).alias(
            "TotalFailedRecords"
        )
    )

    .orderBy(
        "ValidationStatus"
    )
)


display(
    gold_validation_summary_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****Summary by validation category****

# CELL ********************

display(
    gold_validation_results_df

    .groupBy(
        "ValidationCategory"
    )

    .agg(
        F.count(
            "*"
        ).alias(
            "RuleCount"
        ),

        F.sum(
            F.when(
                F.col(
                    "ValidationStatus"
                )
                ==
                "FAILED",

                1
            )

            .otherwise(
                0
            )
        ).alias(
            "FailedRuleCount"
        ),

        F.sum(
            "FailedRecordCount"
        ).alias(
            "FailedRecordCount"
        )
    )

    .orderBy(
        "ValidationCategory"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Count final failures**

# CELL ********************

gold_layer_failure_count = (
    gold_validation_results_df

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
    "Gold Layer failed validation rules:",
    gold_layer_failure_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist final GOld monitoring table**

# CELL ********************

(
    gold_validation_results_df.write

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
        GOLD_LAYER_MONITORING_TABLE
    )
)


print(
    "Created final Gold monitoring table:",
    GOLD_LAYER_MONITORING_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Diagnose Gold dimension refresh metadata
# ============================================================

dimension_monitor_df = (
    spark.table(
        "monitoring_gold_dimensions_quality_results"
    )
)


print(
    "monitoring_gold_dimensions_quality_results columns:"
)

print(
    dimension_monitor_df.columns
)


display(
    dimension_monitor_df
    .orderBy(
        F.desc(
            "ExecutionTimestamp"
        )
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final Gold quality gate**

# CELL ********************

# ============================================================
# Correct SCD2 Supplier Freshness Semantics
# + Final Gold Quality Gate
#
# dim_supplier is SCD Type 2.
#
# GoldLoadDate represents when an SCD row version was created.
# For freshness, use the latest successful NB_30 execution
# timestamp from dimension monitoring instead.
#
# This implementation dynamically mirrors the existing
# gold_validation_results_df schema.
# ============================================================


# ------------------------------------------------------------
# 1. Read NB_30 dimension monitoring results
# ------------------------------------------------------------

dimension_monitor_df = (
    spark.table(
        "monitoring_gold_dimensions_quality_results"
    )
)


# ------------------------------------------------------------
# 2. Obtain latest successful NB_30 execution
# ------------------------------------------------------------

latest_dimension_execution_timestamp = (
    dimension_monitor_df

    .filter(
        F.col("ValidationStatus") == "PASSED"
    )

    .agg(
        F.max(
            "ExecutionTimestamp"
        ).alias(
            "LatestExecutionTimestamp"
        )
    )

    .first()[
        "LatestExecutionTimestamp"
    ]
)


if latest_dimension_execution_timestamp is None:

    raise RuntimeError(
        "No successful NB_30 execution timestamp "
        "was found in "
        "monitoring_gold_dimensions_quality_results."
    )


latest_dimension_execution_date = (
    latest_dimension_execution_timestamp.date()
)


# ------------------------------------------------------------
# 3. Current NB_35 execution date
# ------------------------------------------------------------

current_gold_validation_date = (
    spark.sql(
        """
        SELECT current_date() AS ExecutionDate
        """
    )
    .first()[
        "ExecutionDate"
    ]
)


supplier_dimension_refresh_age_days = (
    current_gold_validation_date
    -
    latest_dimension_execution_date
).days


supplier_dimension_freshness_passed = (
    supplier_dimension_refresh_age_days <= 2
)


print(
    "Latest successful NB_30 execution:",
    latest_dimension_execution_timestamp
)

print(
    "NB_35 execution date:",
    current_gold_validation_date
)

print(
    "dim_supplier processing age:",
    supplier_dimension_refresh_age_days,
    "day(s)"
)


# ------------------------------------------------------------
# 4. Remove existing dim_supplier freshness rule
#
# This removes both:
# - the original incorrect GoldLoadDate-based result
# - any corrected version from an earlier rerun
#
# Therefore this cell is safe to rerun.
# ------------------------------------------------------------

gold_validation_results_df = (
    gold_validation_results_df

    .filter(
        ~(
            (
                F.col("TableName")
                == "dim_supplier"
            )
            &
            (
                F.col("ValidationCategory")
                == "Freshness"
            )
            &
            (
                F.col("ValidationRule")
                == "Gold table refreshed within 2 days"
            )
        )
    )
)


# ------------------------------------------------------------
# 5. Values for corrected dim_supplier validation
# ------------------------------------------------------------

corrected_validation_values = {

    "ValidationLayer":
        "Gold",

    "FailedRecordCount":
        (
            0
            if supplier_dimension_freshness_passed
            else 1
        ),

    "TableName":
        "dim_supplier",

    "ValidationCategory":
        "Freshness",

    "ValidationDetails":
        (
            "Latest successful NB_30 execution: "
            f"{latest_dimension_execution_timestamp}; "
            "NB_35 execution date: "
            f"{current_gold_validation_date}; "
            "Processing age: "
            f"{supplier_dimension_refresh_age_days} day(s); "
            "SCD2 GoldLoadDate intentionally remains unchanged "
            "when supplier attributes do not change."
        ),

    "ValidationRule":
        "Gold table refreshed within 2 days",

    "ValidationStatus":
        (
            "PASSED"
            if supplier_dimension_freshness_passed
            else "FAILED"
        ),

    "ExecutionDate":
        current_gold_validation_date,

    "ExecutionTimestamp":
        latest_dimension_execution_timestamp
}


# ------------------------------------------------------------
# 6. Dynamically construct row using EXACT NB_35 schema
#
# This prevents further union errors if NB_35 contains
# additional monitoring columns.
# ------------------------------------------------------------

supplier_freshness_expressions = []


for schema_field in gold_validation_results_df.schema.fields:

    column_name = (
        schema_field.name
    )


    if column_name in corrected_validation_values:

        expression = (
            F.lit(
                corrected_validation_values[
                    column_name
                ]
            )

            .cast(
                schema_field.dataType
            )

            .alias(
                column_name
            )
        )

    else:

        # Preserve schema compatibility for any metadata
        # columns that are not relevant to this correction.
        expression = (
            F.lit(None)

            .cast(
                schema_field.dataType
            )

            .alias(
                column_name
            )
        )


    supplier_freshness_expressions.append(
        expression
    )


supplier_freshness_validation_df = (
    spark.range(1)

    .select(
        *supplier_freshness_expressions
    )
)


# ------------------------------------------------------------
# 7. Confirm schemas are now identical
# ------------------------------------------------------------

print(
    "\nExisting NB_35 validation columns:"
)

print(
    gold_validation_results_df.columns
)


print(
    "\nCorrected freshness-row columns:"
)

print(
    supplier_freshness_validation_df.columns
)


assert (
    gold_validation_results_df.columns
    ==
    supplier_freshness_validation_df.columns
), (
    "Corrected freshness validation schema "
    "does not exactly match NB_35 schema."
)


print(
    "\nCorrected dim_supplier freshness validation:"
)


display(
    supplier_freshness_validation_df
)


# ------------------------------------------------------------
# 8. Add corrected validation
# ------------------------------------------------------------

gold_validation_results_df = (
    gold_validation_results_df

    .unionByName(
        supplier_freshness_validation_df
    )
)


# ------------------------------------------------------------
# 9. Recalculate Gold quality-gate totals
# ------------------------------------------------------------

gold_layer_validation_count = (
    gold_validation_results_df.count()
)


gold_layer_passed_count = (
    gold_validation_results_df

    .filter(
        F.col("ValidationStatus") == "PASSED"
    )

    .count()
)


gold_layer_failure_count = (
    gold_validation_results_df

    .filter(
        F.col("ValidationStatus") == "FAILED"
    )

    .count()
)


print(
    "\nFINAL GOLD QUALITY SUMMARY"
)

print(
    "Total validation rules:",
    gold_layer_validation_count
)

print(
    "Passed:",
    gold_layer_passed_count
)

print(
    "Failed:",
    gold_layer_failure_count
)


# ------------------------------------------------------------
# 10. Final Gold quality gate
# ------------------------------------------------------------

if gold_layer_failure_count > 0:

    print(
        "\nFAILED GOLD VALIDATIONS:"
    )


    display(
        gold_validation_results_df

        .filter(
            F.col(
                "ValidationStatus"
            )
            == "FAILED"
        )

        .orderBy(
            F.desc(
                "FailedRecordCount"
            ),
            "ValidationCategory",
            "TableName"
        )
    )


    raise AssertionError(
        f"GOLD LAYER QUALITY GATE FAILED "
        f"with {gold_layer_failure_count} "
        f"failed validation rule(s)."
    )


print(
    "\nGOLD LAYER QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final GOld architecture inventory**

# CELL ********************

final_gold_inventory = [
    (
        "Dimension",
        DIM_DATE_TABLE,
        "One row per Date"
    ),

    (
        "Dimension",
        DIM_SUPPLIER_TABLE,
        "Supplier SCD Type 2"
    ),

    (
        "Dimension",
        DIM_CATEGORY_TABLE,
        "One row per Category"
    ),

    (
        "Dimension",
        DIM_MATERIAL_TABLE,
        "One row per Material"
    ),

    (
        "Dimension",
        DIM_BUYER_TABLE,
        "One row per Buyer"
    ),

    (
        "Dimension",
        DIM_BUSINESS_UNIT_TABLE,
        "One row per Business Unit"
    ),

    (
        "Dimension",
        DIM_CONTRACT_TABLE,
        "One row per Contract"
    ),

    (
        "Dimension",
        DIM_CURRENCY_TABLE,
        "One row per Currency"
    ),

    (
        "Fact",
        FACT_PO_TABLE,
        "One row per POItemID"
    ),

    (
        "Fact",
        FACT_INVOICE_TABLE,
        "One row per InvoiceItemID"
    ),

    (
        "Fact",
        FACT_SAVINGS_TABLE,
        "One row per SavingsProjectID"
    ),

    (
        "Fact",
        FACT_SUPPLIER_PERFORMANCE_TABLE,
        "Supplier x PerformanceYear"
    )
]


final_gold_inventory_df = (
    spark.createDataFrame(
        final_gold_inventory,
        [
            "TableType",
            "TableName",
            "Grain"
        ]
    )
)


display(
    final_gold_inventory_df
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
    "=" * 70
)

print(
    "ENTERPRISE PROCUREMENT INTELLIGENCE PLATFORM"
)

print(
    "GOLD DATA ENGINEERING LAYER VALIDATED"
)

print(
    "=" * 70
)

print()

print(
    "Validated dimensions:"
)

print(
    "  - dim_date"
)

print(
    "  - dim_supplier (SCD Type 2)"
)

print(
    "  - dim_category"
)

print(
    "  - dim_material"
)

print(
    "  - dim_buyer"
)

print(
    "  - dim_business_unit"
)

print(
    "  - dim_contract"
)

print(
    "  - dim_currency"
)

print()

print(
    "Validated facts:"
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

print()

print(
    "Validated:"
)

print(
    "  - Dimension key integrity"
)

print(
    "  - Fact grain integrity"
)

print(
    "  - Supplier SCD Type 2 integrity"
)

print(
    "  - Fact-to-dimension referential integrity"
)

print(
    "  - Role-playing date relationships"
)

print(
    "  - Silver-to-Gold row reconciliation"
)

print(
    "  - Silver-to-Gold monetary reconciliation"
)

print(
    "  - Invoice-to-PO cross-fact integrity"
)

print(
    "  - Supplier-performance-to-PO reconciliation"
)

print(
    "  - Gold lineage"
)

print(
    "  - Gold table freshness"
)

print()

print(
    "Final monitoring table:"
)

print(
    "  monitoring_gold_layer_quality_results"
)

print()

print(
    "NB_35_Validate_Gold_Layer "
    "completed successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
