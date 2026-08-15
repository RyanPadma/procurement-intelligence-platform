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

# **Configuration**

# CELL ********************


# ============================================================

SILVER_PO_SPEND_TABLE = (
    "silver_po_spend"
)

SILVER_PO_MONITORING_TABLE = (
    "monitoring_silver_po_spend_quality_results"
)

GOLD_DIMENSION_MONITORING_TABLE = (
    "monitoring_gold_dimensions_quality_results"
)


DIM_DATE_TABLE = "dim_date"
DIM_SUPPLIER_TABLE = "dim_supplier"
DIM_CATEGORY_TABLE = "dim_category"
DIM_MATERIAL_TABLE = "dim_material"
DIM_BUYER_TABLE = "dim_buyer"
DIM_BUSINESS_UNIT_TABLE = "dim_business_unit"
DIM_CONTRACT_TABLE = "dim_contract"
DIM_CURRENCY_TABLE = "dim_currency"


FACT_PO_TABLE = (
    "fact_purchase_order"
)

GOLD_MONITORING_TABLE = (
    "monitoring_gold_fact_purchase_order_quality_results"
)


print(
    "Notebook: NB_31_Build_Gold_Fact_Purchase_Orders"
)

print(
    "Default Lakehouse: lh_procurement_gold"
)

print(
    "Output table: fact_purchase_order"
)

print(
    "Grain: one row per POItemID"
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
    SILVER_PO_SPEND_TABLE,
    SILVER_PO_MONITORING_TABLE,
    GOLD_DIMENSION_MONITORING_TABLE,

    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE,
    DIM_CATEGORY_TABLE,
    DIM_MATERIAL_TABLE,
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

silver_po_failure_count = (
    spark.table(
        SILVER_PO_MONITORING_TABLE
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
    "NB_22 Silver PO failures:",
    silver_po_failure_count
)

print(
    "NB_30 Gold dimension failures:",
    gold_dimension_failure_count
)


assert (
    silver_po_failure_count == 0
), (
    "NB_22 Silver PO Spend "
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

silver_po_spend_df = spark.table(
    SILVER_PO_SPEND_TABLE
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

# **Validate required Silver columns**

# CELL ********************

required_source_columns = [
    "POID",
    "POItemID",
    "POLineNumber",

    "POSupplierID",
    "BuyerID",
    "BusinessUnitID",

    "OrderDate",
    "RequestedDeliveryDate",

    "POCurrency",
    "POStatus",
    "POItemStatus",

    "MaterialID",
    "CategoryID",

    "Quantity",
    "OrderUnit",

    "UnitPrice",
    "UnitPriceEUR",

    "LineAmount",
    "LineAmountEUR",

    "ContractID",

    "SpendEligibilityFlag",

    "EligibleSpendEUR",
    "ContractCompliantSpendEUR",
    "MaverickSpendEUR",

    "ContractComplianceFlag",
    "MaverickSpendFlag",

    "SpendComplianceStatus",
    "ContractValidityStatusAtPO",

    "ValidContractAtPOFlag",

    "ContractPriceWithinToleranceFlag",
    "ContractPriceVariancePercentage",

    "SilverRecordHash"
]


missing_source_columns = [
    column_name
    for column_name in required_source_columns
    if column_name
    not in silver_po_spend_df.columns
]


if missing_source_columns:

    raise RuntimeError(
        "silver_po_spend is missing "
        "required columns: "
        +
        ", ".join(
            missing_source_columns
        )
    )


print(
    "silver_po_spend schema confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect source population**

# CELL ********************

source_po_item_count = (
    silver_po_spend_df.count()
)

source_po_count = (
    silver_po_spend_df

    .select(
        "POID"
    )

    .distinct()

    .count()
)


print(
    "Silver PO items:",
    f"{source_po_item_count:,}"
)

print(
    "Purchase orders:",
    f"{source_po_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare PO source**
# 
# We intentionally retain both:
# 
# LineAmount
# 
# POCurrency
# 
# and:
# 
# LineAmountEUR
# 
# The first represents the original transaction. The second is the standardized analytical measure.

# CELL ********************

po_source_df = (
    silver_po_spend_df

    .select(
        "POID",
        "POItemID",
        "POLineNumber",

        F.col(
            "POSupplierID"
        ).alias(
            "SupplierID"
        ),

        "BuyerID",
        "BusinessUnitID",

        "OrderDate",
        "RequestedDeliveryDate",

        F.col(
            "POCurrency"
        ).alias(
            "CurrencyCode"
        ),

        "POStatus",
        "POItemStatus",

        "MaterialID",
        "CategoryID",

        "Quantity",
        "OrderUnit",

        "UnitPrice",
        "UnitPriceEUR",

        "LineAmount",
        "LineAmountEUR",

        "ContractID",

        "SpendEligibilityFlag",

        "EligibleSpendEUR",
        "ContractCompliantSpendEUR",
        "MaverickSpendEUR",

        "ContractComplianceFlag",
        "MaverickSpendFlag",

        "SpendComplianceStatus",
        "ContractValidityStatusAtPO",

        "ValidContractAtPOFlag",

        "ContractPriceWithinToleranceFlag",
        "ContractPriceVariancePercentage",

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

# **Prepare Order Date dimension**

# CELL ********************

order_date_dimension_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimOrderDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "OrderDateKey"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Order Date dimension**

# CELL ********************

fact_po_df = (
    po_source_df

    .join(
        order_date_dimension_df,

        po_source_df[
            "OrderDate"
        ]
        ==
        order_date_dimension_df[
            "DimOrderDate"
        ],

        "left"
    )

    .drop(
        "DimOrderDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Requested Delivery Date dimension**

# CELL ********************

requested_date_dimension_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimRequestedDeliveryDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "RequestedDeliveryDateKey"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Requested Delivery Date dimension**

# CELL ********************

fact_po_df = (
    fact_po_df

    .join(
        requested_date_dimension_df,

        fact_po_df[
            "RequestedDeliveryDate"
        ]
        ==
        requested_date_dimension_df[
            "DimRequestedDeliveryDate"
        ],

        "left"
    )

    .drop(
        "DimRequestedDeliveryDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Supplier SCD2 dimension**
# 
# We need to join supplier by:
# 
# SupplierID
# 
# +
# 
# OrderDate between EffectiveFromDate and EffectiveToDate

# CELL ********************

supplier_dimension_reference_df = (
    dim_supplier_df

    .select(
        "SupplierKey",

        F.col(
            "SupplierID"
        ).alias(
            "DimSupplierID"
        ),

        "EffectiveFromDate",
        "EffectiveToDate",
        "DimensionVersion"
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

fact_po_df = (
    fact_po_df.alias("po")

    .join(
        supplier_dimension_reference_df.alias(
            "supplier"
        ),

        (
            F.col(
                "po.SupplierID"
            )
            ==
            F.col(
                "supplier.DimSupplierID"
            )
        )
        &
        (
            F.col(
                "po.OrderDate"
            )
            >=
            F.col(
                "supplier.EffectiveFromDate"
            )
        )
        &
        (
            F.col(
                "po.OrderDate"
            )
            <=
            F.col(
                "supplier.EffectiveToDate"
            )
        ),

        "left"
    )

    .select(
        F.col("po.*"),

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

# **Validate Supplier SCD join did not duplicate PO items**

# CELL ********************

supplier_join_duplicate_count = (
    fact_po_df

    .groupBy(
        "POItemID"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


print(
    "PO items duplicated by Supplier SCD join:",
    supplier_join_duplicate_count
)


assert (
    supplier_join_duplicate_count == 0
), (
    "Supplier SCD2 join created duplicate "
    "POItemID rows. Check overlapping "
    "Supplier effective-date ranges."
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


fact_po_df = (
    fact_po_df

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

# **Join Material dimension**

# CELL ********************

material_reference_df = (
    dim_material_df

    .select(
        "MaterialID",
        "MaterialKey"
    )
)


fact_po_df = (
    fact_po_df

    .join(
        material_reference_df,
        "MaterialID",
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


fact_po_df = (
    fact_po_df

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


fact_po_df = (
    fact_po_df

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

# **Join Currency dimension**

# CELL ********************

currency_reference_df = (
    dim_currency_df

    .select(
        "CurrencyCode",
        "CurrencyKey"
    )
)


fact_po_df = (
    fact_po_df

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

# **Join Contract dimension**

# CELL ********************

contract_reference_df = (
    dim_contract_df

    .select(
        "ContractID",
        "ContractKey"
    )
)


fact_po_df = (
    fact_po_df

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

# **Add Gold fact key**

# CELL ********************

fact_po_df = (
    fact_po_df

    .withColumn(
        "PurchaseOrderFactKey",

        F.xxhash64(
            F.col(
                "POItemID"
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

# **Add useful Gold analytical flags**

# CELL ********************

fact_po_df = (
    fact_po_df

    .withColumn(
        "HasContractReferenceFlag",
        F.col(
            "ContractID"
        ).isNotNull()
    )

    .withColumn(
        "PriceComplianceExceptionFlag",

        F.when(
            F.col(
                "SpendEligibilityFlag"
            )
            &
            F.col(
                "ValidContractAtPOFlag"
            ),

            ~F.coalesce(
                F.col(
                    "ContractPriceWithinToleranceFlag"
                ),
                F.lit(False)
            )
        )

        .otherwise(
            F.lit(False)
        )
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
    in fact_po_df.columns
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

fact_po_df = (
    fact_po_df

    .withColumn(
        "GoldSourceTable",
        F.lit(
            SILVER_PO_SPEND_TABLE
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

# **Inspect dimensional key resolution**

# CELL ********************

display(
    fact_po_df

    .select(
        "POItemID",

        "OrderDate",
        "OrderDateKey",

        "RequestedDeliveryDate",
        "RequestedDeliveryDateKey",

        "SupplierID",
        "SupplierKey",
        "SupplierDimensionVersion",

        "CategoryID",
        "CategoryKey",

        "MaterialID",
        "MaterialKey",

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

# **Inspect spend measures**

# CELL ********************

display(
    fact_po_df

    .select(
        "POID",
        "POItemID",
        "POLineNumber",

        "CurrencyCode",

        "Quantity",
        "UnitPrice",
        "LineAmount",

        "UnitPriceEUR",
        "LineAmountEUR",

        "SpendEligibilityFlag",

        "EligibleSpendEUR",
        "ContractCompliantSpendEUR",
        "MaverickSpendEUR",

        "SpendComplianceStatus"
    )

    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation Framework**

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
                FACT_PO_TABLE,

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

gold_po_item_count = (
    fact_po_df.count()
)


register_validation(
    "Row Count",

    (
        "One Gold fact row exists "
        "per Silver PO item"
    ),

    abs(
        source_po_item_count
        -
        gold_po_item_count
    ),

    (
        f"Silver: "
        f"{source_po_item_count:,}; "
        f"Gold: "
        f"{gold_po_item_count:,}"
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

duplicate_po_item_count = (
    fact_po_df

    .groupBy(
        "POItemID"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


register_validation(
    "Primary Key",
    "POItemID is unique",
    duplicate_po_item_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_fact_key_count = (
    fact_po_df

    .groupBy(
        "PurchaseOrderFactKey"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


register_validation(
    "Surrogate Key",

    (
        "PurchaseOrderFactKey "
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

# **Validate mandatory dimension keys**

# CELL ********************

missing_dimension_key_count = (
    fact_po_df

    .filter(
        F.col(
            "OrderDateKey"
        ).isNull()
        |
        F.col(
            "RequestedDeliveryDateKey"
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
            "MaterialKey"
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

    .count()
)


register_validation(
    "Referential Integrity",

    (
        "All mandatory Gold dimension "
        "keys are resolved"
    ),

    missing_dimension_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate ContractKey**
# 
# Only records carrying _ContractID_ require a _ContractKey_

# CELL ********************

unresolved_contract_key_count = (
    fact_po_df

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

# **Validate Supplier SCD resolution**

# CELL ********************

unresolved_supplier_key_count = (
    fact_po_df

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
        "Every PO item resolves to "
        "the Supplier version valid "
        "on OrderDate"
    ),

    unresolved_supplier_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate spend reconcilitation**
# 
# Established in Silver:
# 
# EligibleSpendEUR =
# ContractCompliantSpendEUR
# +
# MaverickSpendEUR

# CELL ********************

spend_reconciliation_error_count = (
    fact_po_df

    .filter(
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
        > F.lit(0.01)
    )

    .count()
)


register_validation(
    "Spend Reconciliation",

    (
        "Eligible spend equals "
        "contract-compliant plus "
        "Maverick spend"
    ),

    spend_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Contract Compliance & Maverick flags**

# CELL ********************

compliance_flag_error_count = (
    fact_po_df

    .filter(
        F.col(
            "SpendEligibilityFlag"
        )
        &
        (
            F.col(
                "ContractComplianceFlag"
            )
            ==
            F.col(
                "MaverickSpendFlag"
            )
        )
    )

    .count()
)


register_validation(
    "KPI Logic",

    (
        "Eligible spend is either "
        "contract compliant or Maverick"
    ),

    compliance_flag_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate ineligible spend amounts**

# CELL ********************

ineligible_spend_error_count = (
    fact_po_df

    .filter(
        ~F.col(
            "SpendEligibilityFlag"
        )
        &
        (
            (
                F.col(
                    "EligibleSpendEUR"
                )
                != 0
            )
            |
            (
                F.col(
                    "ContractCompliantSpendEUR"
                )
                != 0
            )
            |
            (
                F.col(
                    "MaverickSpendEUR"
                )
                != 0
            )
        )
    )

    .count()
)


register_validation(
    "KPI Logic",

    (
        "Ineligible PO items contribute "
        "zero eligible/compliant/Maverick spend"
    ),

    ineligible_spend_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate transaction/EUR amounts**

# CELL ********************

invalid_amount_count = (
    fact_po_df

    .filter(
        F.col(
            "LineAmount"
        ).isNull()
        |
        F.col(
            "LineAmountEUR"
        ).isNull()
        |
        (
            F.col(
                "LineAmount"
            )
            < 0
        )
        |
        (
            F.col(
                "LineAmountEUR"
            )
            < 0
        )
    )

    .count()
)


register_validation(
    "Monetary Values",

    (
        "PO line amounts in source "
        "currency and EUR are valid"
    ),

    invalid_amount_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate natural DateKey logic**

# CELL ********************

order_date_key_error_count = (
    fact_po_df

    .filter(
        F.col(
            "OrderDateKey"
        )
        !=
        F.date_format(
            "OrderDate",
            "yyyyMMdd"
        ).cast("int")
    )

    .count()
)


register_validation(
    "Date Dimension",

    (
        "OrderDateKey corresponds "
        "to OrderDate"
    ),

    order_date_key_error_count
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
    fact_po_df

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
    "Gold Purchase Order Fact "
    "pre-write failures:",
    pre_write_failure_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist monitoring**

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
        f"Gold Purchase Order Fact "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "GOLD PURCHASE ORDER FACT "
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
    fact_po_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        FACT_PO_TABLE
    )
)


print(
    "Created physical Gold fact:",
    FACT_PO_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted row count**

# CELL ********************

persisted_fact_po_df = (
    spark.table(
        FACT_PO_TABLE
    )
)


persisted_row_count = (
    persisted_fact_po_df.count()
)


print(
    "Expected rows:",
    f"{source_po_item_count:,}"
)

print(
    "Persisted rows:",
    f"{persisted_row_count:,}"
)


assert (
    source_po_item_count
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

# **Annual procurement KPI preview**

# CELL ********************

annual_procurement_kpi_df = (
    persisted_fact_po_df

    .groupBy(
        "OrderDateKey"
    )

    .agg(
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
        )
    )

    .join(
        dim_date_df
        .select(
            "DateKey",
            "Year"
        ),

        F.col(
            "OrderDateKey"
        )
        ==
        F.col(
            "DateKey"
        ),

        "left"
    )

    .groupBy(
        "Year"
    )

    .agg(
        F.round(
            F.sum(
                "EligibleSpendEUR"
            ),
            2
        ).alias(
            "EligibleSpendEUR"
        ),

        F.round(
            F.sum(
                "ContractCompliantSpendEUR"
            ),
            2
        ).alias(
            "ContractCompliantSpendEUR"
        ),

        F.round(
            F.sum(
                "MaverickSpendEUR"
            ),
            2
        ).alias(
            "MaverickSpendEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Calculate annual KPI percentages**

# CELL ********************

annual_procurement_kpi_df = (
    annual_procurement_kpi_df

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
                    * F.lit(100.0)
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
                    * F.lit(100.0)
                )
                /
                F.col(
                    "EligibleSpendEUR"
                ),
                2
            )
        )
    )

    .orderBy(
        "Year"
    )
)


display(
    annual_procurement_kpi_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Spend by currency**

# CELL ********************

display(
    persisted_fact_po_df

    .groupBy(
        "CurrencyCode"
    )

    .agg(
        F.count(
            "POItemID"
        ).alias(
            "POItemCount"
        ),

        F.round(
            F.sum(
                "LineAmountEUR"
            ),
            2
        ).alias(
            "SpendEUR"
        )
    )

    .orderBy(
        F.desc(
            "SpendEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Spend by Supplier using _SupplierKey_**

# CELL ********************

supplier_spend_preview_df = (
    persisted_fact_po_df

    .groupBy(
        "SupplierKey"
    )

    .agg(
        F.countDistinct(
            "POID"
        ).alias(
            "POCount"
        ),

        F.round(
            F.sum(
                "EligibleSpendEUR"
            ),
            2
        ).alias(
            "EligibleSpendEUR"
        ),

        F.round(
            F.sum(
                "MaverickSpendEUR"
            ),
            2
        ).alias(
            "MaverickSpendEUR"
        )
    )

    .join(
        dim_supplier_df

        .select(
            "SupplierKey",
            "SupplierID",
            "SupplierName",
            "DimensionVersion",
            "IsCurrentFlag"
        ),

        "SupplierKey",

        "left"
    )

    .orderBy(
        F.desc(
            "EligibleSpendEUR"
        )
    )
)


display(
    supplier_spend_preview_df

    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Category spend preview**

# CELL ********************

category_spend_preview_df = (
    persisted_fact_po_df

    .groupBy(
        "CategoryKey"
    )

    .agg(
        F.count(
            "POItemID"
        ).alias(
            "POItemCount"
        ),

        F.round(
            F.sum(
                "EligibleSpendEUR"
            ),
            2
        ).alias(
            "EligibleSpendEUR"
        ),

        F.round(
            F.sum(
                "ContractCompliantSpendEUR"
            ),
            2
        ).alias(
            "ContractCompliantSpendEUR"
        ),

        F.round(
            F.sum(
                "MaverickSpendEUR"
            ),
            2
        ).alias(
            "MaverickSpendEUR"
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
            "EligibleSpendEUR"
        )
    )
)


display(
    category_spend_preview_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Price-compliance preview**

# CELL ********************

display(
    persisted_fact_po_df

    .filter(
        F.col(
            "ValidContractAtPOFlag"
        )
    )

    .groupBy(
        "ContractPriceWithinToleranceFlag"
    )

    .agg(
        F.count(
            "POItemID"
        ).alias(
            "POItemCount"
        ),

        F.round(
            F.sum(
                "EligibleSpendEUR"
            ),
            2
        ).alias(
            "EligibleSpendEUR"
        )
    )

    .orderBy(
        F.desc(
            "POItemCount"
        )
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
    persisted_fact_po_df

    .select(
        "PurchaseOrderFactKey",

        "POID",
        "POItemID",
        "POLineNumber",

        "OrderDate",
        "OrderDateKey",

        "RequestedDeliveryDate",
        "RequestedDeliveryDateKey",

        "SupplierKey",
        "CategoryKey",
        "MaterialKey",
        "BuyerKey",
        "BusinessUnitKey",
        "ContractKey",
        "CurrencyKey",

        "Quantity",
        "OrderUnit",

        "CurrencyCode",

        "UnitPrice",
        "LineAmount",

        "UnitPriceEUR",
        "LineAmountEUR",

        "SpendEligibilityFlag",

        "EligibleSpendEUR",
        "ContractCompliantSpendEUR",
        "MaverickSpendEUR",

        "ContractComplianceFlag",
        "MaverickSpendFlag",

        "SpendComplianceStatus",
        "ContractValidityStatusAtPO",

        "ContractPriceWithinToleranceFlag",
        "ContractPriceVariancePercentage"
    )

    .orderBy(
        "OrderDate",
        "POID",
        "POLineNumber"
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
    "NB_31_Build_Gold_Fact_Purchase_Orders "
    "completed successfully."
)

print()

print(
    "Physical Gold output:"
)

print(
    "  fact_purchase_order"
)

print()

print(
    "Grain:"
)

print(
    "  One row per POItemID"
)

print()

print(
    "Conformed dimension keys:"
)

print(
    "  - OrderDateKey"
)

print(
    "  - RequestedDeliveryDateKey"
)

print(
    "  - SupplierKey (SCD Type 2)"
)

print(
    "  - CategoryKey"
)

print(
    "  - MaterialKey"
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
    "Primary measures:"
)

print(
    "  - LineAmount"
)

print(
    "  - LineAmountEUR"
)

print(
    "  - EligibleSpendEUR"
)

print(
    "  - ContractCompliantSpendEUR"
)

print(
    "  - MaverickSpendEUR"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
