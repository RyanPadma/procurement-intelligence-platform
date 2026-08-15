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

# This Notebook will create _silver_supplier_performance_
# 
# Grain: One row per SupplierID x PerformanceYear

# MARKDOWN ********************

# **Configuration**

# CELL ********************

from datetime import date

AS_OF_DATE = date(2026, 7, 31)

BRONZE_GOODS_RECEIPT_TABLE = (
    "bronze_goods_receipt"
)

BRONZE_MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)

SILVER_SUPPLIER_TABLE = (
    "silver_supplier"
)

SILVER_PO_SPEND_TABLE = (
    "silver_po_spend"
)

SILVER_INVOICE_MATCHING_TABLE = (
    "silver_invoice_matching"
)

SILVER_PO_MONITORING_TABLE = (
    "monitoring_silver_po_spend_quality_results"
)

SILVER_INVOICE_MONITORING_TABLE = (
    "monitoring_silver_invoice_matching_quality_results"
)

SILVER_SUPPLIER_PERFORMANCE_TABLE = (
    "silver_supplier_performance"
)

SILVER_MONITORING_TABLE = (
    "monitoring_silver_supplier_performance_quality_results"
)

print(
    "Notebook: "
    "NB_24_Build_Silver_Supplier_Performance"
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

from pyspark.sql import functions as F

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
    BRONZE_GOODS_RECEIPT_TABLE,
    BRONZE_MONITORING_TABLE,
    SILVER_SUPPLIER_TABLE,
    SILVER_PO_SPEND_TABLE,
    SILVER_INVOICE_MATCHING_TABLE,
    SILVER_PO_MONITORING_TABLE,
    SILVER_INVOICE_MONITORING_TABLE
]


missing_tables = [
    table_name
    for table_name
    in required_tables
    if not spark.catalog.tableExists(
        table_name
    )
]


if missing_tables:

    raise RuntimeError(
        "Missing required tables: "
        +
        ", ".join(
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
    "Bronze quality gate "
    "has not passed."
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

# **Confirm NB_22 and NB_23 quality gates**

# CELL ********************

po_spend_failure_count = (
    spark.table(
        SILVER_PO_MONITORING_TABLE
    )

    .filter(
        F.col("ValidationStatus")
        == "FAILED"
    )

    .count()
)


invoice_matching_failure_count = (
    spark.table(
        SILVER_INVOICE_MONITORING_TABLE
    )

    .filter(
        F.col("ValidationStatus")
        == "FAILED"
    )

    .count()
)


print(
    "NB_22 failures:",
    po_spend_failure_count
)

print(
    "NB_23 failures:",
    invoice_matching_failure_count
)


assert (
    po_spend_failure_count == 0
), (
    "NB_22 Silver PO Spend "
    "quality gate has not passed."
)


assert (
    invoice_matching_failure_count == 0
), (
    "NB_23 Silver Invoice Matching "
    "quality gate has not passed."
)


print(
    "NB_22 and NB_23 "
    "quality gates confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load input tables**

# CELL ********************

silver_supplier_df = spark.table(
    SILVER_SUPPLIER_TABLE
)

silver_po_spend_df = spark.table(
    SILVER_PO_SPEND_TABLE
)

goods_receipt_df = spark.table(
    BRONZE_GOODS_RECEIPT_TABLE
)

silver_invoice_matching_df = spark.table(
    SILVER_INVOICE_MATCHING_TABLE
)

print(
    "Suppliers:",
    f"{silver_supplier_df.count():,}"
)

print(
    "PO items:",
    f"{silver_po_spend_df.count():,}"
)

print(
    "Goods receipts:",
    f"{goods_receipt_df.count():,}"
)

print(
    "Invoice items:",
    f"{silver_invoice_matching_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare spplier master reference**

# CELL ********************

supplier_reference_df = (
    silver_supplier_df

    .select(
        "SupplierID",
        "SupplierName",
        "SupplierType",
        "Country",
        "Region",
        "PreferredSupplier",
        "StrategicSupplier",
        "ESGRating",
        "FinancialRiskScore",
        "Status",
        "SupplierActiveFlag",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SupplierSilverRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare spend population**
# 
# Cancelled transactions were already excluded by NB_22's _SpendEligibilityFlag_

# CELL ********************

supplier_spend_base_df = (
    silver_po_spend_df

    .filter(
        F.col(
            "SpendEligibilityFlag"
        )
    )

    .select(
        F.col(
            "POSupplierID"
        ).alias(
            "SupplierID"
        ),

        F.col(
            "POYear"
        ).cast("int").alias(
            "PerformanceYear"
        ),

        "POID",
        "POItemID",

        "EligibleSpendEUR",
        "ContractCompliantSpendEUR",
        "MaverickSpendEUR",

        "ContractComplianceFlag",
        "MaverickSpendFlag"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Aggregate spend performance**

# CELL ********************

supplier_spend_year_df = (
    supplier_spend_base_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
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
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# **Prepare goods receipt aggregation**
# 
# Goods receipts can have multiple records per PO item.

# CELL ********************

goods_receipt_item_df = (
    goods_receipt_df

    .groupBy(
        "POItemID"
    )

    .agg(
        F.countDistinct(
            "GoodsReceiptID"
        ).alias(
            "GoodsReceiptCount"
        ),

        F.sum(
            F.col(
                "QuantityReceived"
            ).cast(
                "decimal(18,3)"
            )
        ).alias(
            "TotalQuantityReceived"
        ),

        F.min(
            "ReceiptDate"
        ).alias(
            "FirstReceiptDate"
        ),

        F.max(
            "ReceiptDate"
        ).alias(
            "FinalReceiptDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare PO delivery population**
# 
# For supplier delivery performance we only evaluate PO items whose requested delivery date has already occurred as of the dataset date

# CELL ********************

delivery_base_df = (
    silver_po_spend_df

    .filter(
        F.col(
            "SpendEligibilityFlag"
        )
        &
        (
            F.col(
                "RequestedDeliveryDate"
            )
            <=
            F.lit(
                AS_OF_DATE
            )
        )
    )

    .select(
        F.col(
            "POSupplierID"
        ).alias(
            "SupplierID"
        ),

        "POID",
        "POItemID",

        F.col(
            "Quantity"
        ).cast(
            "decimal(18,3)"
        ).alias(
            "OrderedQuantity"
        ),

        "RequestedDeliveryDate"
    )

    .withColumn(
        "PerformanceYear",
        F.year(
            "RequestedDeliveryDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join goods receipts to PO items**

# CELL ********************

delivery_performance_df = (
    delivery_base_df

    .join(
        goods_receipt_item_df,
        "POItemID",
        "left"
    )

    .withColumn(
        "GoodsReceiptCount",
        F.coalesce(
            F.col(
                "GoodsReceiptCount"
            ),
            F.lit(0)
        ).cast("long")
    )

    .withColumn(
        "TotalQuantityReceived",
        F.coalesce(
            F.col(
                "TotalQuantityReceived"
            ),
            F.lit(0)
        ).cast(
            "decimal(18,3)"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Derive receipt and completion flags**

# CELL ********************

delivery_performance_df = (
    delivery_performance_df

    .withColumn(
        "ReceivedPOItemFlag",
        (
            F.col(
                "GoodsReceiptCount"
            )
            > 0
        )
    )

    .withColumn(
        "CalculatedDeliveryCompleteFlag",
        (
            F.col(
                "TotalQuantityReceived"
            )
            >=
            F.col(
                "OrderedQuantity"
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

# **Derive OTD status**
# 
# A PO item counts in OTD only once it is fully received.

# CELL ********************

delivery_performance_df = (
    delivery_performance_df

    .withColumn(
        "DeliveryPerformanceEligibleFlag",
        F.col(
            "CalculatedDeliveryCompleteFlag"
        )
    )

    .withColumn(
        "OnTimeDeliveryFlag",
        (
            F.col(
                "CalculatedDeliveryCompleteFlag"
            )
            &
            (
                F.col(
                    "FinalReceiptDate"
                )
                <=
                F.col(
                    "RequestedDeliveryDate"
                )
            )
        )
    )

    .withColumn(
        "LateDeliveryFlag",
        (
            F.col(
                "CalculatedDeliveryCompleteFlag"
            )
            &
            (
                F.col(
                    "FinalReceiptDate"
                )
                >
                F.col(
                    "RequestedDeliveryDate"
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

# **Derive overdue incomplete deliveries**
# 
# This prevents incomplete overdue items from disappearing from supplier-performance analysis.

# CELL ********************

delivery_performance_df = (
    delivery_performance_df

    .withColumn(
        "OverdueOpenDeliveryFlag",
        (
            ~F.col(
                "CalculatedDeliveryCompleteFlag"
            )
        )
        &
        (
            F.col(
                "RequestedDeliveryDate"
            )
            <=
            F.lit(
                AS_OF_DATE
            )
        )
    )

    .withColumn(
        "DaysLate",

        F.when(
            F.col(
                "LateDeliveryFlag"
            ),

            F.datediff(
                F.col(
                    "FinalReceiptDate"
                ),
                F.col(
                    "RequestedDeliveryDate"
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

# **Aggregate delivery performance**

# CELL ********************

supplier_delivery_year_df = (
    delivery_performance_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
    )

    .agg(
        F.count(
            "POItemID"
        ).alias(
            "DuePOItemCount"
        ),

        F.sum(
            F.col(
                "ReceivedPOItemFlag"
            ).cast("int")
        ).alias(
            "ReceivedPOItemCount"
        ),

        F.sum(
            F.col(
                "CalculatedDeliveryCompleteFlag"
            ).cast("int")
        ).alias(
            "DeliveryCompletePOItemCount"
        ),

        F.sum(
            F.col(
                "OnTimeDeliveryFlag"
            ).cast("int")
        ).alias(
            "OnTimeDeliveryPOItemCount"
        ),

        F.sum(
            F.col(
                "LateDeliveryFlag"
            ).cast("int")
        ).alias(
            "LateDeliveryPOItemCount"
        ),

        F.sum(
            F.col(
                "OverdueOpenDeliveryFlag"
            ).cast("int")
        ).alias(
            "OverdueOpenPOItemCount"
        ),

        F.round(
            F.avg(
                "DaysLate"
            ),
            2
        ).alias(
            "AverageDaysLate"
        ),

        F.max(
            "DaysLate"
        ).alias(
            "MaximumDaysLate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare invoice item performance**
# 
# NB_23 already derived the canonical three-way-match results.

# CELL ********************

invoice_item_performance_df = (
    silver_invoice_matching_df

    .select(
        F.col(
            "POSupplierID"
        ).alias(
            "SupplierID"
        ),

        F.col(
            "InvoiceYear"
        ).cast("int").alias(
            "PerformanceYear"
        ),

        "InvoiceID",
        "InvoiceItemID",

        "ThreeWayMatchFlag",
        "InvoiceExceptionFlag",

        "DerivedDuplicateInvoiceFlag",

        "GrossAmountEUR",
        "InvoiceExceptionAmountEUR"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Aggregate invoice item performance**

# CELL ********************

supplier_invoice_item_year_df = (
    invoice_item_performance_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
    )

    .agg(
        F.count(
            "InvoiceItemID"
        ).alias(
            "InvoiceItemCount"
        ),

        F.sum(
            F.col(
                "ThreeWayMatchFlag"
            ).cast("int")
        ).alias(
            "MatchedInvoiceItemCount"
        ),

        F.sum(
            F.col(
                "InvoiceExceptionFlag"
            ).cast("int")
        ).alias(
            "InvoiceExceptionItemCount"
        ),

        F.round(
            F.sum(
                "GrossAmountEUR"
            ),
            2
        ).alias(
            "GrossInvoiceAmountEUR"
        ),

        F.round(
            F.sum(
                "InvoiceExceptionAmountEUR"
            ),
            2
        ).alias(
            "InvoiceExceptionAmountEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build one row per invoice**
# 
# Supplier Quality Index will count invoices, not invoice lines.

# CELL ********************

invoice_level_quality_df = (
    silver_invoice_matching_df

    .select(
        "InvoiceID",

        F.col(
            "POSupplierID"
        ).alias(
            "SupplierID"
        ),

        F.col(
            "InvoiceYear"
        ).cast("int").alias(
            "PerformanceYear"
        ),

        "DisputeFlag",
        "DerivedDuplicateInvoiceFlag"
    )

    .groupBy(
        "InvoiceID",
        "SupplierID",
        "PerformanceYear"
    )

    .agg(
        F.max(
            F.coalesce(
                F.col(
                    "DisputeFlag"
                ).cast("int"),
                F.lit(0)
            )
        ).alias(
            "InvoiceDisputeFlag"
        ),

        F.max(
            F.coalesce(
                F.col(
                    "DerivedDuplicateInvoiceFlag"
                ).cast("int"),
                F.lit(0)
            )
        ).alias(
            "InvoiceDuplicateFlag"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Aggregate invoice quality**

# CELL ********************

supplier_invoice_quality_year_df = (
    invoice_level_quality_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
    )

    .agg(
        F.count(
            "InvoiceID"
        ).alias(
            "InvoiceCount"
        ),

        F.sum(
            "InvoiceDisputeFlag"
        ).alias(
            "DisputedInvoiceCount"
        ),

        F.sum(
            F.when(
                F.col(
                    "InvoiceDisputeFlag"
                )
                == 0,
                1
            ).otherwise(0)
        ).alias(
            "DisputeFreeInvoiceCount"
        ),

        F.sum(
            "InvoiceDuplicateFlag"
        ).alias(
            "DuplicateInvoiceCount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build Supplier × Year spine**
# 
# Only supplier-years with activity are included.

# CELL ********************

supplier_year_spine_df = (
    supplier_spend_year_df

    .select(
        "SupplierID",
        "PerformanceYear"
    )

    .unionByName(
        supplier_delivery_year_df
        .select(
            "SupplierID",
            "PerformanceYear"
        )
    )

    .unionByName(
        supplier_invoice_item_year_df
        .select(
            "SupplierID",
            "PerformanceYear"
        )
    )

    .unionByName(
        supplier_invoice_quality_year_df
        .select(
            "SupplierID",
            "PerformanceYear"
        )
    )

    .distinct()
)

print(
    "Supplier-year combinations:",
    f"{supplier_year_spine_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join supplier master**

# CELL ********************

silver_supplier_performance_df = (
    supplier_year_spine_df

    .join(
        supplier_reference_df,
        "SupplierID",
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join spend performance**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .join(
        supplier_spend_year_df,
        [
            "SupplierID",
            "PerformanceYear"
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

# **Join delivery performance**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .join(
        supplier_delivery_year_df,
        [
            "SupplierID",
            "PerformanceYear"
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

# **Join invoice item performance**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .join(
        supplier_invoice_item_year_df,
        [
            "SupplierID",
            "PerformanceYear"
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

# **Join invoice quality performance**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .join(
        supplier_invoice_quality_year_df,
        [
            "SupplierID",
            "PerformanceYear"
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

# **Fill missing count metrics**
# 
# A supplier may have spend in a year without invoice activity.

# CELL ********************

COUNT_COLUMNS = [
    "POCount",
    "POItemCount",

    "DuePOItemCount",
    "ReceivedPOItemCount",
    "DeliveryCompletePOItemCount",
    "OnTimeDeliveryPOItemCount",
    "LateDeliveryPOItemCount",
    "OverdueOpenPOItemCount",

    "InvoiceItemCount",
    "MatchedInvoiceItemCount",
    "InvoiceExceptionItemCount",

    "InvoiceCount",
    "DisputedInvoiceCount",
    "DisputeFreeInvoiceCount",
    "DuplicateInvoiceCount"
]


for column_name in COUNT_COLUMNS:

    silver_supplier_performance_df = (
        silver_supplier_performance_df

        .withColumn(
            column_name,

            F.coalesce(
                F.col(
                    column_name
                ),
                F.lit(0)
            ).cast("long")
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fill missing monetary metrics**

# CELL ********************

AMOUNT_COLUMNS = [
    "EligibleSpendEUR",
    "ContractCompliantSpendEUR",
    "MaverickSpendEUR",

    "GrossInvoiceAmountEUR",
    "InvoiceExceptionAmountEUR"
]


for column_name in AMOUNT_COLUMNS:

    silver_supplier_performance_df = (
        silver_supplier_performance_df

        .withColumn(
            column_name,

            F.coalesce(
                F.col(
                    column_name
                ),
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

# **Fill delivery statistics**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .withColumn(
        "AverageDaysLate",
        F.coalesce(
            F.col(
                "AverageDaysLate"
            ),
            F.lit(0.0)
        )
    )

    .withColumn(
        "MaximumDaysLate",
        F.coalesce(
            F.col(
                "MaximumDaysLate"
            ),
            F.lit(0)
        ).cast("int")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Calculate Contract Compliance %**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

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
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Calculate Supplier OTD %**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .withColumn(
        "SupplierOnTimeDeliveryPct",

        F.when(
            F.col(
                "DeliveryCompletePOItemCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "OnTimeDeliveryPOItemCount"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "DeliveryCompletePOItemCount"
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

# **Calculate overdue-open rate**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .withColumn(
        "OverdueOpenDeliveryPct",

        F.when(
            F.col(
                "DuePOItemCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "OverdueOpenPOItemCount"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "DuePOItemCount"
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

# **Calculate Supplier Quality Index**
# 
# Supplier Quality Index:
# 
# Dispute-free invoices
# ÷
# Total invoices

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

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
                    * F.lit(100.0)
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
        "InvoiceDisputePct",

        F.when(
            F.col(
                "InvoiceCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "DisputedInvoiceCount"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "InvoiceCount"
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

# **Duplicate invoice rate**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

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
                    * F.lit(100.0)
                )
                /
                F.col(
                    "InvoiceCount"
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

# **Three-way-match KPIs**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

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
                        "MatchedInvoiceItemCount"
                    )
                    * F.lit(100.0)
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
                    * F.lit(100.0)
                )
                /
                F.col(
                    "InvoiceItemCount"
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

# **Exception-value rate**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .withColumn(
        "InvoiceExceptionAmountPct",

        F.when(
            F.col(
                "GrossInvoiceAmountEUR"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "InvoiceExceptionAmountEUR"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "GrossInvoiceAmountEUR"
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

# **Add activity flags**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .withColumn(
        "HasSpendActivityFlag",
        (
            F.col(
                "POItemCount"
            )
            > 0
        )
    )

    .withColumn(
        "HasDeliveryActivityFlag",
        (
            F.col(
                "DuePOItemCount"
            )
            > 0
        )
    )

    .withColumn(
        "HasInvoiceActivityFlag",
        (
            F.col(
                "InvoiceCount"
            )
            > 0
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add source lineage description**

# CELL ********************

silver_supplier_performance_df = (
    silver_supplier_performance_df

    .withColumn(
        "PerformanceSourceTables",
        F.lit(
            "silver_supplier|"
            "silver_po_spend|"
            "bronze_goods_receipt|"
            "silver_invoice_matching"
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
    "SupplierSilverRecordHash",
    "SilverLoadTimestamp",
    "SilverLoadDate",
    "SilverRecordHash"
}

hash_columns = [
    column_name
    for column_name
    in silver_supplier_performance_df.columns
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

silver_supplier_performance_df = (
    silver_supplier_performance_df

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

# **Inspect Supplier OTD distribution**

# CELL ********************

display(
    silver_supplier_performance_df

    .filter(
        F.col(
            "SupplierOnTimeDeliveryPct"
        ).isNotNull()
    )

    .select(
        "SupplierID",
        "SupplierName",
        "PerformanceYear",
        "DeliveryCompletePOItemCount",
        "OnTimeDeliveryPOItemCount",
        "LateDeliveryPOItemCount",
        "OverdueOpenPOItemCount",
        "SupplierOnTimeDeliveryPct",
        "AverageDaysLate"
    )

    .orderBy(
        F.desc(
            "DeliveryCompletePOItemCount"
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

# **Inspect Supplier Quality Index**

# CELL ********************

display(
    silver_supplier_performance_df

    .filter(
        F.col(
            "SupplierQualityIndexPct"
        ).isNotNull()
    )

    .select(
        "SupplierID",
        "SupplierName",
        "PerformanceYear",
        "InvoiceCount",
        "DisputedInvoiceCount",
        "DisputeFreeInvoiceCount",
        "SupplierQualityIndexPct",
        "DuplicateInvoicePct"
    )

    .orderBy(
        F.asc(
            "SupplierQualityIndexPct"
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

# **Annual portfolio KPI preview**
# 
# This calculates weighted portfolio KPIs correctly from the underlying amounts and counts.

# CELL ********************

annual_supplier_performance_df = (
    silver_supplier_performance_df

    .groupBy(
        "PerformanceYear"
    )

    .agg(
        F.countDistinct(
            "SupplierID"
        ).alias(
            "SupplierCount"
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
        ),

        F.sum(
            "DeliveryCompletePOItemCount"
        ).alias(
            "DeliveryCompletePOItemCount"
        ),

        F.sum(
            "OnTimeDeliveryPOItemCount"
        ).alias(
            "OnTimeDeliveryPOItemCount"
        ),

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
            "InvoiceItemCount"
        ).alias(
            "InvoiceItemCount"
        ),

        F.sum(
            "MatchedInvoiceItemCount"
        ).alias(
            "MatchedInvoiceItemCount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Calculate annual portfolio KPIs**

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

    .withColumn(
        "SupplierOnTimeDeliveryPct",

        F.when(
            F.col(
                "DeliveryCompletePOItemCount"
            )
            > 0,

            F.round(
                (
                    F.col(
                        "OnTimeDeliveryPOItemCount"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "DeliveryCompletePOItemCount"
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
                    * F.lit(100.0)
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
                        "MatchedInvoiceItemCount"
                    )
                    * F.lit(100.0)
                )
                /
                F.col(
                    "InvoiceItemCount"
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
                SILVER_SUPPLIER_PERFORMANCE_TABLE,

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

# **Validate grain uniqueness**

# CELL ********************

duplicate_grain_count = (
    silver_supplier_performance_df

    .groupBy(
        "SupplierID",
        "PerformanceYear"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


register_validation(
    "Primary Key",

    (
        "SupplierID + PerformanceYear "
        "is unique"
    ),

    duplicate_grain_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate supplier resolution**

# CELL ********************

unresolved_supplier_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "SupplierName"
        ).isNull()
    )

    .count()
)


register_validation(
    "Referential Integrity",

    (
        "All performance records "
        "resolve to Silver supplier"
    ),

    unresolved_supplier_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate performance year**

# CELL ********************

invalid_year_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "PerformanceYear"
        ).isNull()
        |
        (
            F.col(
                "PerformanceYear"
            )
            < 2022
        )
        |
        (
            F.col(
                "PerformanceYear"
            )
            > 2026
        )
    )

    .count()
)


register_validation(
    "Time Dimension",

    (
        "PerformanceYear is between "
        "2022 and 2026"
    ),

    invalid_year_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate spend reconciliation**

# CELL ********************

spend_reconciliation_error_count = (
    silver_supplier_performance_df

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
        "Eligible spend equals compliant "
        "plus Maverick spend"
    ),

    spend_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate delivery classification**

# CELL ********************

delivery_classification_error_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "DeliveryCompletePOItemCount"
        )
        !=
        (
            F.col(
                "OnTimeDeliveryPOItemCount"
            )
            +
            F.col(
                "LateDeliveryPOItemCount"
            )
        )
    )

    .count()
)


register_validation(
    "Delivery Performance",

    (
        "Completed deliveries equal "
        "on-time plus late deliveries"
    ),

    delivery_classification_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate due-delivery population**
# 
# Because the delivery dataset only contains PO items already due as of the as-of date

# CELL ********************

due_delivery_reconciliation_error_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "DuePOItemCount"
        )
        !=
        (
            F.col(
                "DeliveryCompletePOItemCount"
            )
            +
            F.col(
                "OverdueOpenPOItemCount"
            )
        )
    )

    .count()
)


register_validation(
    "Delivery Performance",

    (
        "Due PO items equal completed "
        "plus overdue open items"
    ),

    due_delivery_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate invoice quality counts**

# CELL ********************

invoice_quality_error_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "InvoiceCount"
        )
        !=
        (
            F.col(
                "DisputedInvoiceCount"
            )
            +
            F.col(
                "DisputeFreeInvoiceCount"
            )
        )
    )

    .count()
)


register_validation(
    "Supplier Quality",

    (
        "Invoice count equals disputed "
        "plus dispute-free invoices"
    ),

    invoice_quality_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate three-way-match classification**

# CELL ********************

invoice_match_reconciliation_error_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "InvoiceItemCount"
        )
        !=
        (
            F.col(
                "MatchedInvoiceItemCount"
            )
            +
            F.col(
                "InvoiceExceptionItemCount"
            )
        )
    )

    .count()
)


register_validation(
    "Invoice Matching",

    (
        "Invoice items equal matched "
        "plus exception items"
    ),

    invoice_match_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate duplicate invoices**

# CELL ********************

invalid_duplicate_count = (
    silver_supplier_performance_df

    .filter(
        F.col(
            "DuplicateInvoiceCount"
        )
        >
        F.col(
            "InvoiceCount"
        )
    )

    .count()
)


register_validation(
    "Invoice Quality",

    (
        "Duplicate invoice count cannot "
        "exceed total invoice count"
    ),

    invalid_duplicate_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate percentage domains**

# CELL ********************

PERCENTAGE_COLUMNS = [
    "ContractCompliancePct",
    "MaverickSpendPct",
    "SupplierOnTimeDeliveryPct",
    "OverdueOpenDeliveryPct",
    "SupplierQualityIndexPct",
    "InvoiceDisputePct",
    "DuplicateInvoicePct",
    "ThreeWayMatchPct",
    "InvoiceExceptionPct",
    "InvoiceExceptionAmountPct"
]


invalid_percentage_conditions = []

for column_name in PERCENTAGE_COLUMNS:

    invalid_percentage_conditions.append(
        (
            F.col(
                column_name
            ).isNotNull()
        )
        &
        (
            (
                F.col(
                    column_name
                )
                < 0
            )
            |
            (
                F.col(
                    column_name
                )
                > 100
            )
        )
    )


combined_invalid_percentage_condition = (
    invalid_percentage_conditions[0]
)

for condition in (
    invalid_percentage_conditions[1:]
):

    combined_invalid_percentage_condition = (
        combined_invalid_percentage_condition
        |
        condition
    )


invalid_percentage_count = (
    silver_supplier_performance_df

    .filter(
        combined_invalid_percentage_condition
    )

    .count()
)


register_validation(
    "KPI Domain",

    (
        "All percentage KPIs are "
        "between 0 and 100"
    ),

    invalid_percentage_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate compliance percentages**

# CELL ********************

compliance_percentage_error_count = (
    silver_supplier_performance_df

    .filter(
        (
            F.col(
                "EligibleSpendEUR"
            )
            > 0
        )
        &
        (
            F.abs(
                (
                    F.col(
                        "ContractCompliancePct"
                    )
                    +
                    F.col(
                        "MaverickSpendPct"
                    )
                )
                -
                F.lit(100.0)
            )
            > F.lit(0.05)
        )
    )

    .count()
)


register_validation(
    "KPI Reconciliation",

    (
        "Contract Compliance % plus "
        "Maverick Spend % equals 100%"
    ),

    compliance_percentage_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate supplier quality percentages**

# CELL ********************

quality_percentage_error_count = (
    silver_supplier_performance_df

    .filter(
        (
            F.col(
                "InvoiceCount"
            )
            > 0
        )
        &
        (
            F.abs(
                (
                    F.col(
                        "SupplierQualityIndexPct"
                    )
                    +
                    F.col(
                        "InvoiceDisputePct"
                    )
                )
                -
                F.lit(100.0)
            )
            > F.lit(0.05)
        )
    )

    .count()
)


register_validation(
    "KPI Reconciliation",

    (
        "Supplier Quality Index % plus "
        "Invoice Dispute % equals 100%"
    ),

    quality_percentage_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate invoice match percentages**

# CELL ********************

invoice_match_percentage_error_count = (
    silver_supplier_performance_df

    .filter(
        (
            F.col(
                "InvoiceItemCount"
            )
            > 0
        )
        &
        (
            F.abs(
                (
                    F.col(
                        "ThreeWayMatchPct"
                    )
                    +
                    F.col(
                        "InvoiceExceptionPct"
                    )
                )
                -
                F.lit(100.0)
            )
            > F.lit(0.05)
        )
    )

    .count()
)


register_validation(
    "KPI Reconciliation",

    (
        "Three-Way Match % plus "
        "Invoice Exception % equals 100%"
    ),

    invoice_match_percentage_error_count
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
    silver_supplier_performance_df

    .filter(
        F.col(
            "SupplierSilverRecordHash"
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
        "Supplier lineage and Silver "
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
    "Silver Supplier Performance "
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
        f"Silver Supplier Performance "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "SILVER SUPPLIER PERFORMANCE "
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
    silver_supplier_performance_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        SILVER_SUPPLIER_PERFORMANCE_TABLE
    )
)


print(
    "Created physical Silver table:",
    SILVER_SUPPLIER_PERFORMANCE_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted table**

# CELL ********************

persisted_supplier_performance_df = (
    spark.table(
        SILVER_SUPPLIER_PERFORMANCE_TABLE
    )
)


expected_row_count = (
    silver_supplier_performance_df.count()
)

persisted_row_count = (
    persisted_supplier_performance_df.count()
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

# **Final annual KPI preview**

# CELL ********************

display(
    annual_supplier_performance_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Supplier performance preview**

# CELL ********************

display(
    persisted_supplier_performance_df

    .select(
        "SupplierID",
        "SupplierName",
        "SupplierType",
        "Country",
        "Region",

        "PreferredSupplier",
        "StrategicSupplier",

        "PerformanceYear",

        "POCount",
        "EligibleSpendEUR",

        "ContractCompliancePct",
        "MaverickSpendPct",

        "DuePOItemCount",
        "DeliveryCompletePOItemCount",
        "OnTimeDeliveryPOItemCount",
        "LateDeliveryPOItemCount",
        "OverdueOpenPOItemCount",
        "SupplierOnTimeDeliveryPct",
        "AverageDaysLate",

        "InvoiceCount",
        "SupplierQualityIndexPct",
        "InvoiceDisputePct",

        "ThreeWayMatchPct",
        "InvoiceExceptionPct",
        "InvoiceExceptionAmountEUR",

        "DuplicateInvoicePct",

        "ESGRating",
        "FinancialRiskScore"
    )

    .orderBy(
        "PerformanceYear",
        F.desc(
            "EligibleSpendEUR"
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

# **Identify high-spend poor-performing suppliers**
# 
# This is useful as a portfolio diagnostic, not a new stored score.

# CELL ********************

display(
    persisted_supplier_performance_df

    .filter(
        (
            F.col(
                "EligibleSpendEUR"
            )
            > 0
        )
        &
        (
            (
                F.col(
                    "SupplierOnTimeDeliveryPct"
                )
                < 90
            )
            |
            (
                F.col(
                    "SupplierQualityIndexPct"
                )
                < 95
            )
            |
            (
                F.col(
                    "MaverickSpendPct"
                )
                > 25
            )
        )
    )

    .select(
        "SupplierID",
        "SupplierName",
        "PerformanceYear",

        "EligibleSpendEUR",
        "SupplierOnTimeDeliveryPct",
        "SupplierQualityIndexPct",
        "MaverickSpendPct",
        "ThreeWayMatchPct",
        "InvoiceExceptionAmountEUR"
    )

    .orderBy(
        F.desc(
            "EligibleSpendEUR"
        )
    )

    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **FInal status**

# CELL ********************

print(
    "NB_24_Build_Silver_Supplier_Performance "
    "completed successfully."
)

print()

print(
    "Physical Silver output:"
)

print(
    "  silver_supplier_performance"
)

print()

print(
    "Grain:"
)

print(
    "  One row per SupplierID "
    "x PerformanceYear"
)

print()

print(
    "Core supplier KPIs:"
)

print(
    "  - ContractCompliancePct"
)

print(
    "  - MaverickSpendPct"
)

print(
    "  - SupplierOnTimeDeliveryPct"
)

print(
    "  - SupplierQualityIndexPct"
)

print(
    "  - ThreeWayMatchPct"
)

print(
    "  - InvoiceExceptionPct"
)

print(
    "  - DuplicateInvoicePct"
)

print(
    "  - OverdueOpenDeliveryPct"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
