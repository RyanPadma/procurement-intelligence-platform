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

CONTRACT_PRICE_TOLERANCE_PERCENT = 3.0

BRONZE_PO_HEADER_TABLE = (
    "bronze_purchase_order_header"
)

BRONZE_PO_ITEM_TABLE = (
    "bronze_purchase_order_item"
)

BRONZE_MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)

SILVER_CONTRACT_TABLE = (
    "silver_contract"
)

SILVER_PO_SPEND_TABLE = (
    "silver_po_spend"
)

SILVER_MONITORING_TABLE = (
    "monitoring_silver_po_spend_quality_results"
)

print(
    "Notebook: NB_22_Build_Silver_PO_Spend"
)

print(
    "Default Lakehouse: "
    "lh_procurement_silver"
)

print(
    f"As-of date: {AS_OF_DATE}"
)

print(
    f"Contract price tolerance: "
    f"{CONTRACT_PRICE_TOLERANCE_PERCENT:.1f}%"
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
from pyspark.sql.window import Window

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate prerequisites**

# CELL ********************

required_tables = [
    "bronze_purchase_order_header",
    "bronze_purchase_order_item",
    "monitoring_bronze_data_quality_results",
    "silver_supplier",
    "silver_category",
    "silver_material",
    "silver_buyer",
    "silver_business_unit",
    "silver_exchange_rate",
    "silver_contract"
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

bronze_validation_df = spark.table(
    BRONZE_MONITORING_TABLE
)

bronze_critical_failure_count = (
    bronze_validation_df
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
    "Bronze critical failures:",
    bronze_critical_failure_count
)

assert (
    bronze_critical_failure_count == 0
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

# **Confirm NB_21 contract quality gate**

# CELL ********************

CONTRACT_MONITORING_TABLE = (
    "monitoring_silver_contract_quality_results"
)

if not spark.catalog.tableExists(
    CONTRACT_MONITORING_TABLE
):
    raise RuntimeError(
        "NB_21 monitoring table does not exist. "
        "Run NB_21 successfully first."
    )

contract_quality_failure_count = (
    spark.table(
        CONTRACT_MONITORING_TABLE
    )
    .filter(
        F.col("ValidationStatus")
        == "FAILED"
    )
    .count()
)

print(
    "Silver contract failures:",
    contract_quality_failure_count
)

assert (
    contract_quality_failure_count == 0
), (
    "NB_21 Silver contract quality "
    "gate has not passed."
)

print(
    "NB_21 contract quality gate confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load source tables**

# CELL ********************

po_header_df = spark.table(
    BRONZE_PO_HEADER_TABLE
)

po_item_df = spark.table(
    BRONZE_PO_ITEM_TABLE
)

silver_supplier_df = spark.table(
    "silver_supplier"
)

silver_category_df = spark.table(
    "silver_category"
)

silver_material_df = spark.table(
    "silver_material"
)

silver_buyer_df = spark.table(
    "silver_buyer"
)

silver_business_unit_df = spark.table(
    "silver_business_unit"
)

silver_exchange_rate_df = spark.table(
    "silver_exchange_rate"
)

silver_contract_df = spark.table(
    SILVER_CONTRACT_TABLE
)

print(
    "PO headers:",
    po_header_df.count()
)

print(
    "PO items:",
    po_item_df.count()
)

print(
    "Silver contracts:",
    silver_contract_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare PO header projection**

# CELL ********************

po_header_projection_df = (
    po_header_df
    .select(
        "POID",

        F.col(
            "SupplierID"
        ).alias(
            "POSupplierID"
        ),

        "BuyerID",
        "BusinessUnitID",
        "OrderDate",

        F.col(
            "Currency"
        ).alias(
            "POCurrency"
        ),

        "POStatus",
        "TotalAmount",
        "AmountReconciliationStatus",

        F.col(
            "SourceSystem"
        ).alias(
            "POHeaderSourceSystem"
        ),

        F.col(
            "IngestionTimestamp"
        ).alias(
            "POHeaderIngestionTimestamp"
        ),

        F.col(
            "LoadDate"
        ).alias(
            "POHeaderLoadDate"
        ),

        F.col(
            "SourceRecordHash"
        ).alias(
            "POHeaderSourceRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare PO item projection**

# CELL ********************

po_item_projection_df = (
    po_item_df
    .select(
        "POItemID",
        "POID",
        "POLineNumber",
        "MaterialID",
        "CategoryID",
        "ContractID",

        F.col(
            "Quantity"
        ).cast(
            "decimal(18,3)"
        ).alias(
            "Quantity"
        ),

        "OrderUnit",

        F.col(
            "UnitPrice"
        ).cast(
            "decimal(18,4)"
        ).alias(
            "UnitPrice"
        ),

        F.col(
            "LineAmount"
        ).cast(
            "decimal(18,2)"
        ).alias(
            "LineAmount"
        ),

        F.col(
            "Currency"
        ).alias(
            "POItemCurrency"
        ),

        "RequestedDeliveryDate",
        "POItemStatus",

        "SimulationContractScenario",
        "SimulationPriceAnomalyFlag",

        F.col(
            "SourceSystem"
        ).alias(
            "POItemSourceSystem"
        ),

        F.col(
            "IngestionTimestamp"
        ).alias(
            "POItemIngestionTimestamp"
        ),

        F.col(
            "LoadDate"
        ).alias(
            "POItemLoadDate"
        ),

        F.col(
            "SourceRecordHash"
        ).alias(
            "POItemSourceRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join PO header and item**

# CELL ********************

silver_po_spend_df = (
    po_item_projection_df.alias("item")
    .join(
        po_header_projection_df.alias(
            "header"
        ),
        F.col("item.POID")
        == F.col("header.POID"),
        "inner"
    )
    .select(
        F.col("item.*"),

        F.col(
            "header.POSupplierID"
        ),

        F.col(
            "header.BuyerID"
        ),

        F.col(
            "header.BusinessUnitID"
        ),

        F.col(
            "header.OrderDate"
        ),

        F.col(
            "header.POCurrency"
        ),

        F.col(
            "header.POStatus"
        ),

        F.col(
            "header.TotalAmount"
        ).alias(
            "POHeaderTotalAmount"
        ),

        F.col(
            "header.AmountReconciliationStatus"
        ).alias(
            "POAmountReconciliationStatus"
        ),

        F.col(
            "header.POHeaderSourceSystem"
        ),

        F.col(
            "header.POHeaderIngestionTimestamp"
        ),

        F.col(
            "header.POHeaderLoadDate"
        ),

        F.col(
            "header.POHeaderSourceRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate header/item currency consistency**

# CELL ********************

currency_mismatch_count = (
    silver_po_spend_df
    .filter(
        F.col("POCurrency")
        != F.col("POItemCurrency")
    )
    .count()
)

print(
    "PO header/item currency mismatches:",
    currency_mismatch_count
)

assert currency_mismatch_count == 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare supplier reference**

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
        "SupplierActiveFlag",
        "PreferredSupplier",
        "StrategicSupplier",
        "ESGRating",
        "FinancialRiskScore"
    )
)

silver_po_spend_df = (
    silver_po_spend_df
    .join(
        supplier_reference_df,
        silver_po_spend_df[
            "POSupplierID"
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

# **Prepare buyer reference**

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

silver_po_spend_df = (
    silver_po_spend_df
    .join(
        buyer_reference_df,
        silver_po_spend_df[
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

# **Prepare material reference**

# CELL ********************

material_reference_df = (
    silver_material_df
    .select(
        F.col(
            "MaterialID"
        ).alias(
            "RefMaterialID"
        ),

        "MaterialDescription",

        F.col(
            "CategoryID"
        ).alias(
            "MaterialCategoryID"
        ),

        "StandardCost",

        F.col(
            "UnitOfMeasure"
        ).alias(
            "MaterialUnitOfMeasure"
        ),

        "Manufacturer",

        F.col(
            "Status"
        ).alias(
            "MaterialStatus"
        )
    )
)

silver_po_spend_df = (
    silver_po_spend_df
    .join(
        material_reference_df,
        silver_po_spend_df[
            "MaterialID"
        ]
        ==
        material_reference_df[
            "RefMaterialID"
        ],
        "left"
    )
    .drop(
        "RefMaterialID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare category reference**

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

silver_po_spend_df = (
    silver_po_spend_df
    .join(
        category_reference_df,
        silver_po_spend_df[
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

# **Validate business-unit reference**
# 
# We do not need to duplicate all business-unit attributes into the transaction table yet, but we need to verify the key exists

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

silver_po_spend_df = (
    silver_po_spend_df
    .join(
        business_unit_reference_df,
        silver_po_spend_df[
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

# **Prepare Silver contract reference**
# 
# We deliberately rename contract fields so PO and contract attributes cannot become ambiguous.

# CELL ********************

contract_reference_df = (
    silver_contract_df
    .select(
        F.col(
            "ContractID"
        ).alias(
            "ResolvedContractID"
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

        F.col(
            "Currency"
        ).alias(
            "ContractCurrency"
        ),

        F.col(
            "NegotiatedUnitPrice"
        ).alias(
            "ContractNegotiatedUnitPrice"
        ),

        "ContractType",
        "ContractLifecycleStatus",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SilverContractRecordHash"
        )
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

silver_po_spend_df = (
    silver_po_spend_df
    .join(
        contract_reference_df,
        silver_po_spend_df[
            "ContractID"
        ]
        ==
        contract_reference_df[
            "ResolvedContractID"
        ],
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive spend eligibility**
# 
# Cancelled POs should not enter procurement spend KPIs

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "SpendEligibilityFlag",
        (
            F.col("POStatus")
            != F.lit("Cancelled")
        )
        &
        (
            F.col("POItemStatus")
            != F.lit("Cancelled")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive contract reference flags**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "ContractReferenceFlag",
        F.col(
            "ContractID"
        ).isNotNull()
    )

    .withColumn(
        "ContractFoundFlag",
        F.col(
            "ResolvedContractID"
        ).isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Supplier and category contract matching**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "ContractSupplierMatchFlag",
        F.when(
            F.col(
                "ContractFoundFlag"
            ),
            F.col(
                "POSupplierID"
            )
            ==
            F.col(
                "ContractSupplierID"
            )
        ).otherwise(
            F.lit(False)
        )
    )

    .withColumn(
        "ContractCategoryMatchFlag",
        F.when(
            F.col(
                "ContractFoundFlag"
            ),
            F.col(
                "CategoryID"
            )
            ==
            F.col(
                "ContractCategoryID"
            )
        ).otherwise(
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

# **Contract validity at PO OrderDate**
# 
# This is the important distinction from NB_21.
# 
# It's not whether the contract is active on July 31, 2026. t's about whether it was valid when the PO was created.

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "ContractDateValidAtPOFlag",

        F.when(
            F.col(
                "ContractFoundFlag"
            ),
            (
                F.col("OrderDate")
                >=
                F.col(
                    "ContractStartDate"
                )
            )
            &
            (
                F.col("OrderDate")
                <=
                F.col(
                    "ContractEndDate"
                )
            )
        ).otherwise(
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

# **Valid contract at PO level**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "ValidContractAtPOFlag",
        (
            F.col(
                "ContractFoundFlag"
            )
            &
            F.col(
                "ContractSupplierMatchFlag"
            )
            &
            F.col(
                "ContractCategoryMatchFlag"
            )
            &
            F.col(
                "ContractDateValidAtPOFlag"
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

# **Contract validity status**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "ContractValidityStatusAtPO",

        F.when(
            ~F.col(
                "ContractReferenceFlag"
            ),
            F.lit(
                "NO_CONTRACT_REFERENCE"
            )
        )

        .when(
            ~F.col(
                "ContractFoundFlag"
            ),
            F.lit(
                "CONTRACT_NOT_FOUND"
            )
        )

        .when(
            ~F.col(
                "ContractSupplierMatchFlag"
            ),
            F.lit(
                "SUPPLIER_MISMATCH"
            )
        )

        .when(
            ~F.col(
                "ContractCategoryMatchFlag"
            ),
            F.lit(
                "CATEGORY_MISMATCH"
            )
        )

        .when(
            F.col("OrderDate")
            <
            F.col(
                "ContractStartDate"
            ),
            F.lit(
                "CONTRACT_NOT_YET_ACTIVE"
            )
        )

        .when(
            F.col("OrderDate")
            >
            F.col(
                "ContractEndDate"
            ),
            F.lit(
                "CONTRACT_EXPIRED"
            )
        )

        .when(
            F.col(
                "ValidContractAtPOFlag"
            ),
            F.lit(
                "VALID_CONTRACT"
            )
        )

        .otherwise(
            F.lit("UNKNOWN")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive Contract COmpliance**
# 
# The project KPI definition:
# 
# Contract Compliance
# 
# =
# 
# Spend aligned to a valid negotiated contract
# 
# /
# 
# Eligible procurement spend

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "ContractComplianceFlag",
        (
            F.col(
                "SpendEligibilityFlag"
            )
            &
            F.col(
                "ValidContractAtPOFlag"
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

# **Derive Maverick Spend**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "MaverickSpendFlag",
        (
            F.col(
                "SpendEligibilityFlag"
            )
            &
            (
                ~F.col(
                    "ValidContractAtPOFlag"
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

# **Analytical compliance classification**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "SpendComplianceStatus",

        F.when(
            ~F.col(
                "SpendEligibilityFlag"
            ),
            F.lit(
                "EXCLUDED_CANCELLED"
            )
        )

        .when(
            F.col(
                "ContractComplianceFlag"
            ),
            F.lit("COMPLIANT")
        )

        .when(
            ~F.col(
                "ContractReferenceFlag"
            ),
            F.lit(
                "MAVERICK_NO_CONTRACT"
            )
        )

        .when(
            ~F.col(
                "ContractFoundFlag"
            ),
            F.lit(
                "MAVERICK_CONTRACT_NOT_FOUND"
            )
        )

        .when(
            ~F.col(
                "ContractSupplierMatchFlag"
            ),
            F.lit(
                "MAVERICK_SUPPLIER_MISMATCH"
            )
        )

        .when(
            ~F.col(
                "ContractCategoryMatchFlag"
            ),
            F.lit(
                "MAVERICK_CATEGORY_MISMATCH"
            )
        )

        .when(
            ~F.col(
                "ContractDateValidAtPOFlag"
            ),
            F.lit(
                "MAVERICK_INVALID_CONTRACT_DATE"
            )
        )

        .otherwise(
            F.lit(
                "MAVERICK_OTHER"
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

# **Prepare FX base reference**
# 
# PO spend will be valued at the PO OrderDate

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

# **FX resolution helper**
# 
# This handles exact rates and boundary fallback consistently.

# CELL ********************

def add_fx_resolution(
    dataframe,
    date_column,
    currency_column,
    prefix
):
    exact_date_col = (
        f"{prefix}ExactFXDate"
    )

    exact_currency_col = (
        f"{prefix}ExactFXCurrency"
    )

    exact_rate_col = (
        f"{prefix}ExactFXRate"
    )

    first_date_col = (
        f"{prefix}FirstFXDate"
    )

    first_rate_col = (
        f"{prefix}FirstFXRate"
    )

    first_currency_col = (
        f"{prefix}FirstFXCurrency"
    )

    last_date_col = (
        f"{prefix}LastFXDate"
    )

    last_rate_col = (
        f"{prefix}LastFXRate"
    )

    last_currency_col = (
        f"{prefix}LastFXCurrency"
    )

    exact_fx_df = (
        fx_base_df
        .select(
            F.col(
                "RateDate"
            ).alias(
                exact_date_col
            ),
            F.col(
                "Currency"
            ).alias(
                exact_currency_col
            ),
            F.col(
                "ExchangeRateToEUR"
            ).alias(
                exact_rate_col
            )
        )
    )

    first_window = (
        Window
        .partitionBy("Currency")
        .orderBy(
            F.col(
                "RateDate"
            ).asc()
        )
    )

    first_fx_df = (
        fx_base_df
        .withColumn(
            "_RowNumber",
            F.row_number().over(
                first_window
            )
        )
        .filter(
            F.col("_RowNumber") == 1
        )
        .select(
            F.col(
                "Currency"
            ).alias(
                first_currency_col
            ),
            F.col(
                "RateDate"
            ).alias(
                first_date_col
            ),
            F.col(
                "ExchangeRateToEUR"
            ).alias(
                first_rate_col
            )
        )
    )

    last_window = (
        Window
        .partitionBy("Currency")
        .orderBy(
            F.col(
                "RateDate"
            ).desc()
        )
    )

    last_fx_df = (
        fx_base_df
        .withColumn(
            "_RowNumber",
            F.row_number().over(
                last_window
            )
        )
        .filter(
            F.col("_RowNumber") == 1
        )
        .select(
            F.col(
                "Currency"
            ).alias(
                last_currency_col
            ),
            F.col(
                "RateDate"
            ).alias(
                last_date_col
            ),
            F.col(
                "ExchangeRateToEUR"
            ).alias(
                last_rate_col
            )
        )
    )

    result_df = (
        dataframe

        .join(
            exact_fx_df,
            (
                F.col(date_column)
                ==
                F.col(exact_date_col)
            )
            &
            (
                F.col(currency_column)
                ==
                F.col(
                    exact_currency_col
                )
            ),
            "left"
        )

        .join(
            first_fx_df,
            F.col(currency_column)
            ==
            F.col(
                first_currency_col
            ),
            "left"
        )

        .join(
            last_fx_df,
            F.col(currency_column)
            ==
            F.col(
                last_currency_col
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
                    currency_column
                ).isNull(),
                F.lit(
                    "NOT_APPLICABLE"
                )
            )

            .when(
                F.col(
                    exact_rate_col
                ).isNotNull(),
                F.lit("EXACT_DATE")
            )

            .when(
                F.col(date_column)
                <
                F.col(
                    first_date_col
                ),
                F.lit(
                    "EARLIEST_AVAILABLE_RATE"
                )
            )

            .when(
                F.col(date_column)
                >
                F.col(
                    last_date_col
                ),
                F.lit(
                    "LATEST_AVAILABLE_RATE"
                )
            )

            .otherwise(
                F.lit("UNRESOLVED")
            )
        )

        .withColumn(
            f"{prefix}ExchangeRateToEUR",

            F.when(
                F.col(
                    exact_rate_col
                ).isNotNull(),
                F.col(
                    exact_rate_col
                )
            )

            .when(
                F.col(date_column)
                <
                F.col(
                    first_date_col
                ),
                F.col(
                    first_rate_col
                )
            )

            .when(
                F.col(date_column)
                >
                F.col(
                    last_date_col
                ),
                F.col(
                    last_rate_col
                )
            )

            .otherwise(
                F.lit(None)
            )
        )

        .withColumn(
            f"{prefix}FXRateDate",

            F.when(
                F.col(
                    exact_rate_col
                ).isNotNull(),
                F.col(
                    exact_date_col
                )
            )

            .when(
                F.col(date_column)
                <
                F.col(
                    first_date_col
                ),
                F.col(
                    first_date_col
                )
            )

            .when(
                F.col(date_column)
                >
                F.col(
                    last_date_col
                ),
                F.col(
                    last_date_col
                )
            )

            .otherwise(
                F.lit(None).cast(
                    "date"
                )
            )
        )
    )

    return (
        result_df
        .drop(
            exact_date_col,
            exact_currency_col,
            exact_rate_col,
            first_date_col,
            first_rate_col,
            first_currency_col,
            last_date_col,
            last_rate_col,
            last_currency_col
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve PO currency FX**

# CELL ********************

silver_po_spend_df = (
    add_fx_resolution(
        dataframe=(
            silver_po_spend_df
        ),
        date_column="OrderDate",
        currency_column="POCurrency",
        prefix="PO"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Resolve contract currency at PO date**
# 
# Comparing contract and PO prices at the same economic date, the PO OrderDate.

# CELL ********************

silver_po_spend_df = (
    add_fx_resolution(
        dataframe=(
            silver_po_spend_df
        ),
        date_column="OrderDate",
        currency_column="ContractCurrency",
        prefix="ContractAtPO"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Convert PO Spend to EUR**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "UnitPriceEUR",
        F.round(
            F.col("UnitPrice")
            *
            F.col(
                "POExchangeRateToEUR"
            ),
            4
        ).cast(
            "decimal(18,4)"
        )
    )

    .withColumn(
        "LineAmountEUR",
        F.round(
            F.col("LineAmount")
            *
            F.col(
                "POExchangeRateToEUR"
            ),
            2
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

# MARKDOWN ********************

# **Convert negotiated contract price at PO date**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "ContractNegotiatedUnitPriceEURAtPODate",

        F.when(
            F.col(
                "ContractFoundFlag"
            )
            &
            F.col(
                "ContractAtPOExchangeRateToEUR"
            ).isNotNull(),

            F.round(
                F.col(
                    "ContractNegotiatedUnitPrice"
                )
                *
                F.col(
                    "ContractAtPOExchangeRateToEUR"
                ),
                4
            ).cast(
                "decimal(18,4)"
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

# **Contract price comparison eligibility**
# 
# Price compliance is separate from contract validity.

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "ContractPriceComparisonEligibleFlag",
        (
            F.col(
                "ValidContractAtPOFlag"
            )
            &
            F.col(
                "UnitPriceEUR"
            ).isNotNull()
            &
            F.col(
                "ContractNegotiatedUnitPriceEURAtPODate"
            ).isNotNull()
            &
            (
                F.col(
                    "ContractNegotiatedUnitPriceEURAtPODate"
                ) > 0
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

# **Calculate price variance**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "ContractPriceVarianceEUR",

        F.when(
            F.col(
                "ContractPriceComparisonEligibleFlag"
            ),

            F.round(
                F.col(
                    "UnitPriceEUR"
                )
                -
                F.col(
                    "ContractNegotiatedUnitPriceEURAtPODate"
                ),
                4
            )
        )
    )

    .withColumn(
        "ContractPriceVariancePercentage",

        F.when(
            F.col(
                "ContractPriceComparisonEligibleFlag"
            ),

            F.round(
                (
                    (
                        F.col(
                            "UnitPriceEUR"
                        )
                        /
                        F.col(
                            "ContractNegotiatedUnitPriceEURAtPODate"
                        )
                    )
                    - F.lit(1)
                )
                * 100,
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

# **Contract price tolerance flag**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df
    .withColumn(
        "ContractPriceWithinToleranceFlag",

        F.when(
            F.col(
                "ContractPriceComparisonEligibleFlag"
            ),

            F.abs(
                F.col(
                    "ContractPriceVariancePercentage"
                )
            )
            <=
            F.lit(
                CONTRACT_PRICE_TOLERANCE_PERCENT
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

# **Create KPI amount columns**

# CELL ********************

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "EligibleSpendEUR",

        F.when(
            F.col(
                "SpendEligibilityFlag"
            ),
            F.col(
                "LineAmountEUR"
            )
        ).otherwise(
            F.lit(0)
        )
    )

    .withColumn(
        "ContractCompliantSpendEUR",

        F.when(
            F.col(
                "ContractComplianceFlag"
            ),
            F.col(
                "LineAmountEUR"
            )
        ).otherwise(
            F.lit(0)
        )
    )

    .withColumn(
        "MaverickSpendEUR",

        F.when(
            F.col(
                "MaverickSpendFlag"
            ),
            F.col(
                "LineAmountEUR"
            )
        ).otherwise(
            F.lit(0)
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

silver_po_spend_df = (
    silver_po_spend_df

    .withColumn(
        "POYear",
        F.year(
            "OrderDate"
        )
    )

    .withColumn(
        "POMonth",
        F.month(
            "OrderDate"
        )
    )

    .withColumn(
        "POYearMonth",
        F.date_format(
            "OrderDate",
            "yyyy-MM"
        )
    )

    .withColumn(
        "POQuarter",
        F.concat(
            F.lit("Q"),
            F.quarter(
                "OrderDate"
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

# **Add Silver metadata**

# CELL ********************

SILVER_HASH_EXCLUDED_COLUMNS = {
    "POHeaderSourceSystem",
    "POHeaderIngestionTimestamp",
    "POHeaderLoadDate",
    "POHeaderSourceRecordHash",

    "POItemSourceSystem",
    "POItemIngestionTimestamp",
    "POItemLoadDate",
    "POItemSourceRecordHash",

    "SilverContractRecordHash",

    "SilverLoadTimestamp",
    "SilverLoadDate",
    "SilverRecordHash"
}

business_hash_columns = [
    column_name
    for column_name
    in silver_po_spend_df.columns
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
    in business_hash_columns
]

silver_po_spend_df = (
    silver_po_spend_df

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

# **Inspect compliance distribution**

# CELL ********************

display(
    silver_po_spend_df
    .groupBy(
        "SpendComplianceStatus"
    )
    .agg(
        F.count("*").alias(
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
            "EligibleSpendEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Preview KPI results by year**
# 
# Silver validation preview. Final KPI model can be found in Gold version.

# CELL ********************

annual_spend_preview_df = (
    silver_po_spend_df

    .groupBy(
        "POYear"
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

    .withColumn(
        "ContractCompliancePct",

        F.when(
            F.col(
                "EligibleSpendEUR"
            ) > 0,

            F.round(
                (
                    F.col(
                        "ContractCompliantSpendEUR"
                    )
                    /
                    F.col(
                        "EligibleSpendEUR"
                    )
                )
                * 100,
                2
            )
        )
    )

    .withColumn(
        "MaverickSpendPct",

        F.when(
            F.col(
                "EligibleSpendEUR"
            ) > 0,

            F.round(
                (
                    F.col(
                        "MaverickSpendEUR"
                    )
                    /
                    F.col(
                        "EligibleSpendEUR"
                    )
                )
                * 100,
                2
            )
        )
    )

    .orderBy(
        "POYear"
    )
)

display(
    annual_spend_preview_df
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

    validation_results.append({
        "TableName":
            SILVER_PO_SPEND_TABLE,

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
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate row preservation**

# CELL ********************

bronze_po_item_count = (
    po_item_df.count()
)

silver_po_spend_count = (
    silver_po_spend_df.count()
)

register_validation(
    "Row Count",
    (
        "One Silver spend row exists "
        "per Bronze PO item"
    ),
    abs(
        bronze_po_item_count
        - silver_po_spend_count
    ),
    (
        f"Bronze PO items: "
        f"{bronze_po_item_count:,}; "
        f"Silver spend rows: "
        f"{silver_po_spend_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate POItemID uniqueness**

# CELL ********************

duplicate_po_item_count = (
    silver_po_spend_df
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

null_po_item_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "POItemID"
        ).isNull()
    )
    .count()
)

register_validation(
    "Primary Key",
    "POItemID is not null",
    null_po_item_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate master references**

# CELL ********************

missing_master_reference_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "SupplierName"
        ).isNull()
        |
        F.col(
            "BuyerName"
        ).isNull()
        |
        F.col(
            "MaterialDescription"
        ).isNull()
        |
        F.col(
            "CategoryName"
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
        "Supplier, buyer, material, "
        "category and business unit resolve"
    ),
    missing_master_reference_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate line arithmetic**

# CELL ********************

po_line_arithmetic_error_count = (
    silver_po_spend_df
    .filter(
        F.abs(
            F.col("LineAmount")
            -
            F.round(
                F.col("Quantity")
                *
                F.col("UnitPrice"),
                2
            )
        )
        > F.lit(0.01)
    )
    .count()
)

register_validation(
    "Arithmetic",
    (
        "PO LineAmount equals "
        "Quantity × UnitPrice"
    ),
    po_line_arithmetic_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate PO FX resolution**

# CELL ********************

unresolved_po_fx_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "POExchangeRateToEUR"
        ).isNull()
        |
        F.col(
            "POFXRateDate"
        ).isNull()
        |
        (
            F.col(
                "POFXResolutionMethod"
            )
            == "UNRESOLVED"
        )
    )
    .count()
)

register_validation(
    "Currency Conversion",
    (
        "PO currency exchange rate "
        "is resolved"
    ),
    unresolved_po_fx_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate contract FX resolution**
# 
# Only lines with a resolved contract require contract FX.

# CELL ********************

unresolved_contract_fx_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "ContractFoundFlag"
        )
        &
        (
            F.col(
                "ContractAtPOExchangeRateToEUR"
            ).isNull()
            |
            F.col(
                "ContractAtPOFXRateDate"
            ).isNull()
            |
            (
                F.col(
                    "ContractAtPOFXResolutionMethod"
                )
                == "UNRESOLVED"
            )
        )
    )
    .count()
)

register_validation(
    "Currency Conversion",
    (
        "Resolved contracts have "
        "contract-currency FX at PO date"
    ),
    unresolved_contract_fx_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate EUR conversion**

# CELL ********************

po_eur_conversion_error_count = (
    silver_po_spend_df
    .filter(
        F.abs(
            F.col(
                "LineAmountEUR"
            )
            -
            F.round(
                F.col(
                    "LineAmount"
                )
                *
                F.col(
                    "POExchangeRateToEUR"
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
        "PO LineAmountEUR matches "
        "PO-date exchange-rate conversion"
    ),
    po_eur_conversion_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate compliance classification**
# 
# Eligible items must be either compliant or Maverick, never both.

# CELL ********************

classification_error_count = (
    silver_po_spend_df
    .filter(
        (
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
        |
        (
            ~F.col(
                "SpendEligibilityFlag"
            )
            &
            (
                F.col(
                    "ContractComplianceFlag"
                )
                |
                F.col(
                    "MaverickSpendFlag"
                )
            )
        )
    )
    .count()
)

register_validation(
    "Compliance Classification",
    (
        "Eligible spend is exactly one of "
        "compliant or Maverick"
    ),
    classification_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate compliant lines**

# CELL ********************

invalid_compliant_line_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "ContractComplianceFlag"
        )
        &
        (
            ~F.col(
                "ValidContractAtPOFlag"
            )
        )
    )
    .count()
)

register_validation(
    "Compliance Classification",
    (
        "Contract-compliant lines "
        "have a valid contract at PO date"
    ),
    invalid_compliant_line_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate KPI amount reconciliation**

# CELL ********************

spend_reconciliation = (
    silver_po_spend_df
    .agg(
        F.sum(
            "EligibleSpendEUR"
        ).alias(
            "Eligible"
        ),

        F.sum(
            "ContractCompliantSpendEUR"
        ).alias(
            "Compliant"
        ),

        F.sum(
            "MaverickSpendEUR"
        ).alias(
            "Maverick"
        )
    )
    .first()
)

eligible_total = (
    spend_reconciliation[
        "Eligible"
    ]
)

classified_total = (
    spend_reconciliation[
        "Compliant"
    ]
    +
    spend_reconciliation[
        "Maverick"
    ]
)

spend_difference = abs(
    float(
        eligible_total
        - classified_total
    )
)

register_validation(
    "KPI Reconciliation",
    (
        "Eligible spend equals compliant "
        "spend plus Maverick spend"
    ),
    (
        0
        if spend_difference <= 0.01
        else 1
    ),
    (
        f"Difference EUR: "
        f"{spend_difference:.2f}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate price variance calculations**

# CELL ********************

price_variance_error_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "ContractPriceComparisonEligibleFlag"
        )
        &
        (
            F.abs(
                F.col(
                    "ContractPriceVarianceEUR"
                )
                -
                F.round(
                    F.col(
                        "UnitPriceEUR"
                    )
                    -
                    F.col(
                        "ContractNegotiatedUnitPriceEURAtPODate"
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
    "Price Compliance",
    (
        "Contract price variance "
        "is calculated correctly"
    ),
    price_variance_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Silver metadata**

# CELL ********************

invalid_silver_metadata_count = (
    silver_po_spend_df
    .filter(
        F.col(
            "POHeaderSourceRecordHash"
        ).isNull()
        |
        F.col(
            "POItemSourceRecordHash"
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
        "metadata are complete"
    ),
    invalid_silver_metadata_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build validation DataFrame**

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

# **Count Failures**

# CELL ********************

pre_write_failure_count = (
    validation_results_df
    .filter(
        F.col(
            "ValidationStatus"
        )
        == "FAILED"
    )
    .count()
)

print(
    "Silver PO spend "
    "pre-write failures:",
    pre_write_failure_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist monitoring results**

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
            == "FAILED"
        )
        .orderBy(
            F.desc(
                "FailedRecordCount"
            )
        )
    )

    raise AssertionError(
        f"Silver PO spend validation "
        f"failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )

print(
    "SILVER PO SPEND PRE-WRITE "
    "QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write physical silver_po_spend**

# CELL ********************

(
    silver_po_spend_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        SILVER_PO_SPEND_TABLE
    )
)

print(
    "Created physical Silver table:",
    SILVER_PO_SPEND_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted row count**

# CELL ********************

persisted_po_spend_df = (
    spark.table(
        SILVER_PO_SPEND_TABLE
    )
)

expected_row_count = (
    silver_po_spend_df.count()
)

persisted_row_count = (
    persisted_po_spend_df.count()
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
    persisted_row_count
    == expected_row_count
)

print(
    "Silver PO spend row-count "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect FX resolution methods**

# CELL ********************

display(
    persisted_po_spend_df
    .groupBy(
        "POFXResolutionMethod"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    persisted_po_spend_df
    .filter(
        F.col(
            "ContractFoundFlag"
        )
    )
    .groupBy(
        "ContractAtPOFXResolutionMethod"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final annual KPI preview**

# CELL ********************

annual_kpi_preview_df = (
    persisted_po_spend_df

    .groupBy(
        "POYear"
    )

    .agg(
        F.countDistinct(
            "POID"
        ).alias(
            "POCount"
        ),

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

    .withColumn(
        "ContractCompliancePct",

        F.round(
            (
                F.col(
                    "ContractCompliantSpendEUR"
                )
                /
                F.col(
                    "EligibleSpendEUR"
                )
            )
            * 100,
            2
        )
    )

    .withColumn(
        "MaverickSpendPct",

        F.round(
            (
                F.col(
                    "MaverickSpendEUR"
                )
                /
                F.col(
                    "EligibleSpendEUR"
                )
            )
            * 100,
            2
        )
    )

    .orderBy(
        "POYear"
    )
)

display(
    annual_kpi_preview_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect Maverick reasons**

# CELL ********************

display(
    persisted_po_spend_df

    .filter(
        F.col(
            "MaverickSpendFlag"
        )
    )

    .groupBy(
        "SpendComplianceStatus"
    )

    .agg(
        F.count(
            "POItemID"
        ).alias(
            "POItemCount"
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

    .orderBy(
        F.desc(
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

# **Inspect price compliance**

# CELL ********************

display(
    persisted_po_spend_df

    .filter(
        F.col(
            "ContractPriceComparisonEligibleFlag"
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
                "LineAmountEUR"
            ),
            2
        ).alias(
            "SpendEUR"
        ),

        F.round(
            F.avg(
                "ContractPriceVariancePercentage"
            ),
            2
        ).alias(
            "AveragePriceVariancePct"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect final table**

# CELL ********************

display(
    persisted_po_spend_df
    .select(
        "POID",
        "POItemID",
        "POLineNumber",
        "OrderDate",
        "POYear",
        "SupplierName",
        "BuyerName",
        "BusinessUnitID",
        "MaterialDescription",
        "CategoryName",
        "ProcurementType",
        "Quantity",
        "POCurrency",
        "UnitPrice",
        "UnitPriceEUR",
        "LineAmount",
        "LineAmountEUR",
        "ContractID",
        "ContractValidityStatusAtPO",
        "ContractComplianceFlag",
        "MaverickSpendFlag",
        "SpendComplianceStatus",
        "ContractNegotiatedUnitPrice",
        "ContractCurrency",
        "ContractNegotiatedUnitPriceEURAtPODate",
        "ContractPriceVariancePercentage",
        "ContractPriceWithinToleranceFlag"
    )
    .orderBy(
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
    "NB_22_Build_Silver_PO_Spend "
    "completed successfully."
)

print()

print(
    "Physical Silver output:"
)

print(
    "  silver_po_spend"
)

print()

print(
    "Grain:"
)

print(
    "  One row per PO item"
)

print()

print(
    "Core analytical fields:"
)

print(
    "  - EligibleSpendEUR"
)

print(
    "  - ContractComplianceFlag"
)

print(
    "  - ContractCompliantSpendEUR"
)

print(
    "  - MaverickSpendFlag"
)

print(
    "  - MaverickSpendEUR"
)

print(
    "  - ContractValidityStatusAtPO"
)

print(
    "  - ContractPriceVariancePercentage"
)

print(
    "  - ContractPriceWithinToleranceFlag"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    persisted_po_spend_df
    .groupBy(
        "POYear",
        "SimulationContractScenario"
    )
    .agg(
        F.count("*").alias(
            "POItemCount"
        ),
        F.round(
            F.sum("EligibleSpendEUR"),
            2
        ).alias(
            "EligibleSpendEUR"
        )
    )
    .orderBy(
        "POYear",
        "SimulationContractScenario"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scenario_summary_df = (
    persisted_po_spend_df
    .filter(
        F.col("SpendEligibilityFlag")
    )
    .groupBy(
        "SimulationContractScenario"
    )
    .agg(
        F.count("*").alias(
            "POItemCount"
        ),
        F.round(
            F.sum("EligibleSpendEUR"),
            2
        ).alias(
            "SpendEUR"
        )
    )
)

total_eligible_spend = (
    scenario_summary_df
    .agg(
        F.sum("SpendEUR")
        .alias("TotalSpendEUR")
    )
    .first()["TotalSpendEUR"]
)

scenario_summary_df = (
    scenario_summary_df
    .withColumn(
        "SpendPct",
        F.round(
            (
                F.col("SpendEUR")
                / F.lit(total_eligible_spend)
            ) * 100,
            2
        )
    )
)

display(
    scenario_summary_df
    .orderBy(
        F.desc("SpendEUR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    persisted_po_spend_df
    .filter(
        F.col("SpendEligibilityFlag")
    )
    .groupBy(
        "SimulationContractScenario",
        "SpendComplianceStatus"
    )
    .agg(
        F.count("*").alias(
            "POItemCount"
        ),
        F.round(
            F.sum("EligibleSpendEUR"),
            2
        ).alias(
            "SpendEUR"
        )
    )
    .orderBy(
        "SimulationContractScenario",
        F.desc("SpendEUR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

compliant_scenario_diagnostic_df = (
    persisted_po_spend_df
    .filter(
        (
            F.col(
                "SimulationContractScenario"
            )
            == "COMPLIANT_CONTRACT"
        )
        &
        F.col(
            "SpendEligibilityFlag"
        )
    )
    .groupBy(
        "ContractValidityStatusAtPO",
        "SpendComplianceStatus"
    )
    .agg(
        F.count("*").alias(
            "POItemCount"
        ),
        F.round(
            F.sum("EligibleSpendEUR"),
            2
        ).alias(
            "SpendEUR"
        )
    )
    .orderBy(
        F.desc("SpendEUR")
    )
)

display(
    compliant_scenario_diagnostic_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

maverick_reason_df = (
    persisted_po_spend_df
    .filter(
        F.col("MaverickSpendFlag")
    )
    .groupBy(
        "SpendComplianceStatus"
    )
    .agg(
        F.count("*").alias(
            "POItemCount"
        ),
        F.round(
            F.sum("MaverickSpendEUR"),
            2
        ).alias(
            "MaverickSpendEUR"
        )
    )
)

total_maverick_spend = (
    maverick_reason_df
    .agg(
        F.sum("MaverickSpendEUR")
        .alias("TotalMaverick")
    )
    .first()["TotalMaverick"]
)

maverick_reason_df = (
    maverick_reason_df
    .withColumn(
        "MaverickSpendPct",
        F.round(
            (
                F.col("MaverickSpendEUR")
                / F.lit(total_maverick_spend)
            ) * 100,
            2
        )
    )
)

display(
    maverick_reason_df
    .orderBy(
        F.desc("MaverickSpendEUR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
