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

# This Notebook will create : silver_invoice_matching
# 
# with Grain: one row per invoice item.
# 
# We will retain the Bronze _ThreeWayMatchStatus_ as a source comparison field.

# MARKDOWN ********************

# **Configuration**

# CELL ********************

from datetime import date

AS_OF_DATE = date(2026, 7, 31)

PRICE_TOLERANCE_PERCENT = 3.0
QUANTITY_TOLERANCE_PERCENT = 2.0


BRONZE_INVOICE_HEADER_TABLE = (
    "bronze_invoice_header"
)

BRONZE_INVOICE_ITEM_TABLE = (
    "bronze_invoice_item"
)

BRONZE_GOODS_RECEIPT_TABLE = (
    "bronze_goods_receipt"
)

BRONZE_MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)


SILVER_PO_SPEND_TABLE = (
    "silver_po_spend"
)

SILVER_EXCHANGE_RATE_TABLE = (
    "silver_exchange_rate"
)

SILVER_PO_MONITORING_TABLE = (
    "monitoring_silver_po_spend_quality_results"
)


SILVER_INVOICE_MATCHING_TABLE = (
    "silver_invoice_matching"
)

SILVER_MONITORING_TABLE = (
    "monitoring_silver_invoice_matching_quality_results"
)


print(
    "Notebook: NB_23_Build_Silver_Invoice_Matching"
)

print(
    "Default Lakehouse: lh_procurement_silver"
)

print(
    f"Price tolerance: "
    f"{PRICE_TOLERANCE_PERCENT:.1f}%"
)

print(
    f"Quantity tolerance: "
    f"{QUANTITY_TOLERANCE_PERCENT:.1f}%"
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

# **Validate required tables**

# CELL ********************

required_tables = [
    BRONZE_INVOICE_HEADER_TABLE,
    BRONZE_INVOICE_ITEM_TABLE,
    BRONZE_GOODS_RECEIPT_TABLE,
    BRONZE_MONITORING_TABLE,
    SILVER_PO_SPEND_TABLE,
    SILVER_EXCHANGE_RATE_TABLE,
    SILVER_PO_MONITORING_TABLE
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

bronze_validation_df = (
    spark.table(
        BRONZE_MONITORING_TABLE
    )
)


bronze_failure_count = (
    bronze_validation_df
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

# **Confirm NB_22 quality gate**

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


print(
    "Silver PO-spend failures:",
    po_spend_failure_count
)


assert (
    po_spend_failure_count == 0
), (
    "NB_22 Silver PO Spend "
    "quality gate has not passed."
)


print(
    "NB_22 quality gate confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load tables**

# CELL ********************

invoice_header_df = spark.table(
    BRONZE_INVOICE_HEADER_TABLE
)

invoice_item_df = spark.table(
    BRONZE_INVOICE_ITEM_TABLE
)

goods_receipt_df = spark.table(
    BRONZE_GOODS_RECEIPT_TABLE
)

silver_po_spend_df = spark.table(
    SILVER_PO_SPEND_TABLE
)

silver_exchange_rate_df = spark.table(
    SILVER_EXCHANGE_RATE_TABLE
)

print(
    "Invoice headers:",
    f"{invoice_header_df.count():,}"
)

print(
    "Invoice items:",
    f"{invoice_item_df.count():,}"
)

print(
    "Goods receipts:",
    f"{goods_receipt_df.count():,}"
)

print(
    "Silver PO items:",
    f"{silver_po_spend_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare invoice header**

# CELL ********************

invoice_header_projection_df = (
    invoice_header_df
    .select(
        "InvoiceID",
        "InvoiceNumber",

        F.col("POID").alias(
            "InvoiceHeaderPOID"
        ),

        F.col("SupplierID").alias(
            "InvoiceSupplierID"
        ),

        F.col("BusinessUnitID").alias(
            "InvoiceBusinessUnitID"
        ),

        "InvoiceDate",
        "PostingDate",
        "DueDate",

        F.col("Currency").alias(
            "InvoiceHeaderCurrency"
        ),

        F.col("TotalInvoiceAmount").cast(
            "decimal(18,2)"
        ).alias(
            "TotalInvoiceAmount"
        ),

        "InvoiceStatus",
        "PaymentStatus",

        F.col("PaymentTermsDays").cast(
            "int"
        ).alias(
            "InvoicePaymentTermsDays"
        ),

        "DisputeFlag",
        "DisputeReason",

        F.col("DuplicateInvoiceFlag").alias(
            "HeaderDuplicateInvoiceFlag"
        ),

        F.col("OriginalInvoiceID").alias(
            "OriginalInvoiceID"
        ),

        "AmountReconciliationStatus",

        F.col("SourceSystem").alias(
            "InvoiceHeaderSourceSystem"
        ),

        F.col("IngestionTimestamp").alias(
            "InvoiceHeaderIngestionTimestamp"
        ),

        F.col("LoadDate").alias(
            "InvoiceHeaderLoadDate"
        ),

        F.col("SourceRecordHash").alias(
            "InvoiceHeaderSourceRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare invoice items**

# CELL ********************

invoice_item_projection_df = (
    invoice_item_df
    .select(
        "InvoiceItemID",
        "InvoiceID",
        "InvoiceLineNumber",
        "POID",
        "POItemID",
        "MaterialID",
        "CategoryID",
        "ContractID",

        F.col("InvoicedQuantity").cast(
            "decimal(18,3)"
        ).alias(
            "InvoicedQuantity"
        ),

        "UnitOfMeasure",

        F.col("POUnitPrice").cast(
            "decimal(18,4)"
        ).alias(
            "SourcePOUnitPrice"
        ),

        F.col("InvoiceUnitPrice").cast(
            "decimal(18,4)"
        ).alias(
            "InvoiceUnitPrice"
        ),

        F.col("NetAmount").cast(
            "decimal(18,2)"
        ).alias(
            "NetAmount"
        ),

        F.col("TaxRate").cast(
            "decimal(8,4)"
        ).alias(
            "TaxRate"
        ),

        F.col("TaxAmount").cast(
            "decimal(18,2)"
        ).alias(
            "TaxAmount"
        ),

        F.col("GrossAmount").cast(
            "decimal(18,2)"
        ).alias(
            "GrossAmount"
        ),

        F.col("Currency").alias(
            "InvoiceItemCurrency"
        ),

        F.col(
            "ReceivedQuantityAtInvoiceDate"
        ).cast(
            "decimal(18,3)"
        ).alias(
            "SourceReceivedQuantityAtInvoiceDate"
        ),

        F.col("PriceVarianceAmount").alias(
            "SourcePriceVarianceAmount"
        ),

        F.col("PriceVariancePercentage").alias(
            "SourcePriceVariancePercentage"
        ),

        F.col("QuantityVariance").alias(
            "SourceQuantityVariance"
        ),

        F.col(
            "QuantityVariancePercentage"
        ).alias(
            "SourceQuantityVariancePercentage"
        ),

        F.col("ThreeWayMatchStatus").alias(
            "SourceThreeWayMatchStatus"
        ),

        "SimulationMatchScenario",

        F.col(
            "DuplicateInvoiceLineFlag"
        ).alias(
            "DuplicateInvoiceLineFlag"
        ),

        "OriginalInvoiceItemID",

        F.col("SourceSystem").alias(
            "InvoiceItemSourceSystem"
        ),

        F.col("IngestionTimestamp").alias(
            "InvoiceItemIngestionTimestamp"
        ),

        F.col("LoadDate").alias(
            "InvoiceItemLoadDate"
        ),

        F.col("SourceRecordHash").alias(
            "InvoiceItemSourceRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join invoice header and invoice item**

# CELL ********************

invoice_base_df = (
    invoice_item_projection_df.alias("item")

    .join(
        invoice_header_projection_df.alias(
            "header"
        ),
        F.col("item.InvoiceID")
        ==
        F.col("header.InvoiceID"),
        "inner"
    )

    .select(
        F.col("item.*"),

        F.col(
            "header.InvoiceNumber"
        ),

        F.col(
            "header.InvoiceHeaderPOID"
        ),

        F.col(
            "header.InvoiceSupplierID"
        ),

        F.col(
            "header.InvoiceBusinessUnitID"
        ),

        F.col(
            "header.InvoiceDate"
        ),

        F.col(
            "header.PostingDate"
        ),

        F.col(
            "header.DueDate"
        ),

        F.col(
            "header.InvoiceHeaderCurrency"
        ),

        F.col(
            "header.TotalInvoiceAmount"
        ),

        F.col(
            "header.InvoiceStatus"
        ),

        F.col(
            "header.PaymentStatus"
        ),

        F.col(
            "header.InvoicePaymentTermsDays"
        ),

        F.col(
            "header.DisputeFlag"
        ),

        F.col(
            "header.DisputeReason"
        ),

        F.col(
            "header.HeaderDuplicateInvoiceFlag"
        ),

        F.col(
            "header.OriginalInvoiceID"
        ),

        F.col(
            "header.AmountReconciliationStatus"
        ),

        F.col(
            "header.InvoiceHeaderSourceSystem"
        ),

        F.col(
            "header.InvoiceHeaderIngestionTimestamp"
        ),

        F.col(
            "header.InvoiceHeaderLoadDate"
        ),

        F.col(
            "header.InvoiceHeaderSourceRecordHash"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate invoice header/item consistency**

# CELL ********************

invoice_header_item_mismatch_count = (
    invoice_base_df
    .filter(
        (
            F.col("POID")
            !=
            F.col("InvoiceHeaderPOID")
        )
        |
        (
            F.col("InvoiceItemCurrency")
            !=
            F.col("InvoiceHeaderCurrency")
        )
    )
    .count()
)


print(
    "Invoice header/item inconsistencies:",
    invoice_header_item_mismatch_count
)


assert (
    invoice_header_item_mismatch_count == 0
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Set canonical invoice currency**

# CELL ********************

invoice_base_df = (
    invoice_base_df
    .withColumn(
        "InvoiceCurrency",
        F.col(
            "InvoiceHeaderCurrency"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Silver PO reference**

# CELL ********************

po_reference_df = (
    silver_po_spend_df
    .select(
        F.col("POItemID").alias(
            "RefPOItemID"
        ),

        F.col("POID").alias(
            "RefPOID"
        ),

        F.col("POSupplierID").alias(
            "POSupplierID"
        ),

        F.col("BusinessUnitID").alias(
            "POBusinessUnitID"
        ),

        "BuyerID",
        "BuyerName",
        "OrderDate",

        F.col("POCurrency").alias(
            "POCurrency"
        ),

        "POStatus",
        "POItemStatus",

        F.col("MaterialID").alias(
            "POMaterialID"
        ),

        F.col("CategoryID").alias(
            "POCategoryID"
        ),

        "MaterialDescription",
        "CategoryName",
        "ProcurementType",

        F.col("Quantity").alias(
            "OrderedQuantity"
        ),

        F.col("OrderUnit").alias(
            "POOrderUnit"
        ),

        F.col("UnitPrice").alias(
            "POUnitPrice"
        ),

        F.col("UnitPriceEUR").alias(
            "POUnitPriceEUR"
        ),

        F.col("LineAmount").alias(
            "POLineAmount"
        ),

        F.col("LineAmountEUR").alias(
            "POLineAmountEUR"
        ),

        F.col("ContractID").alias(
            "POContractID"
        ),

        "ContractComplianceFlag",
        "MaverickSpendFlag",
        "SpendComplianceStatus",

        "ContractPriceWithinToleranceFlag",
        "ContractPriceVariancePercentage"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join PO reference**

# CELL ********************

silver_invoice_matching_df = (
    invoice_base_df

    .join(
        po_reference_df,
        invoice_base_df[
            "POItemID"
        ]
        ==
        po_reference_df[
            "RefPOItemID"
        ],
        "left"
    )

    .withColumn(
        "POReferenceResolvedFlag",
        F.col(
            "RefPOItemID"
        ).isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate PO reference consistency**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "InvoicePOSupplierMatchFlag",
        F.when(
            F.col(
                "POReferenceResolvedFlag"
            ),
            F.col(
                "InvoiceSupplierID"
            )
            ==
            F.col(
                "POSupplierID"
            )
        ).otherwise(
            F.lit(False)
        )
    )

    .withColumn(
        "InvoicePOBusinessUnitMatchFlag",
        F.when(
            F.col(
                "POReferenceResolvedFlag"
            ),
            F.col(
                "InvoiceBusinessUnitID"
            )
            ==
            F.col(
                "POBusinessUnitID"
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

# **Prepare goods receipts**
# 
# To calculate what had actually been received by the invoice date.

# CELL ********************

goods_receipt_projection_df = (
    goods_receipt_df
    .select(
        "GoodsReceiptID",
        "POItemID",
        "ReceiptDate",

        F.col("QuantityReceived").cast(
            "decimal(18,3)"
        ).alias(
            "QuantityReceived"
        ),

        "ReceiptStatus",
        "DeliveryCompleteFlag"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reconstruct receipt quantity at invoice date**

# CELL ********************

invoice_receipt_join_df = (
    silver_invoice_matching_df
    .select(
        "InvoiceItemID",
        "POItemID",
        "InvoiceDate"
    )
    .alias("invoice")

    .join(
        goods_receipt_projection_df.alias(
            "receipt"
        ),
        (
            F.col(
                "invoice.POItemID"
            )
            ==
            F.col(
                "receipt.POItemID"
            )
        )
        &
        (
            F.col(
                "receipt.ReceiptDate"
            )
            <=
            F.col(
                "invoice.InvoiceDate"
            )
        ),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Aggregate receipts at invoice date**

# CELL ********************

invoice_receipt_aggregation_df = (
    invoice_receipt_join_df

    .groupBy(
        "InvoiceItemID"
    )

    .agg(
        F.coalesce(
            F.sum(
                "QuantityReceived"
            ),
            F.lit(0)
        ).cast(
            "decimal(18,3)"
        ).alias(
            "CalculatedReceivedQuantityAtInvoiceDate"
        ),

        F.countDistinct(
            "GoodsReceiptID"
        ).alias(
            "GoodsReceiptCountAtInvoiceDate"
        ),

        F.max(
            "ReceiptDate"
        ).alias(
            "LatestReceiptDateAtInvoiceDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join receipt calculation**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .join(
        invoice_receipt_aggregation_df,
        "InvoiceItemID",
        "left"
    )

    .fillna(
        {
            "GoodsReceiptCountAtInvoiceDate": 0
        }
    )

    .withColumn(
        "CalculatedReceivedQuantityAtInvoiceDate",
        F.coalesce(
            F.col(
                "CalculatedReceivedQuantityAtInvoiceDate"
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

# **Good receipt availability**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "GoodsReceiptAvailableAtInvoiceDateFlag",
        (
            F.col(
                "CalculatedReceivedQuantityAtInvoiceDate"
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

# **Compare source vs reconstructed receipt quantity**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "ReceiptQuantitySourceConsistencyFlag",
        (
            F.abs(
                F.col(
                    "SourceReceivedQuantityAtInvoiceDate"
                )
                -
                F.col(
                    "CalculatedReceivedQuantityAtInvoiceDate"
                )
            )
            <= F.lit(0.001)
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Recalculate PO price variance**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "CalculatedPriceVarianceAmount",

        F.when(
            F.col("POUnitPrice") > 0,

            F.round(
                F.col(
                    "InvoiceUnitPrice"
                )
                -
                F.col(
                    "POUnitPrice"
                ),
                4
            )
        )
    )

    .withColumn(
        "CalculatedPriceVariancePercentage",

        F.when(
            F.col("POUnitPrice") > 0,

            F.round(
                (
                    (
                        F.col(
                            "InvoiceUnitPrice"
                        )
                        /
                        F.col(
                            "POUnitPrice"
                        )
                    )
                    -
                    F.lit(1)
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

# **Derive price tolerance**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "InvoicePriceWithinToleranceFlag",

        F.when(
            F.col(
                "CalculatedPriceVariancePercentage"
            ).isNotNull(),

            F.abs(
                F.col(
                    "CalculatedPriceVariancePercentage"
                )
            )
            <=
            F.lit(
                PRICE_TOLERANCE_PERCENT
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

# **Recalculate quantity variance**
# 
# Three-way matching compares:
# 
# Invoice quantity
# vs
# quantity actually received by invoice date

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "CalculatedQuantityVariance",

        F.round(
            F.col(
                "InvoicedQuantity"
            )
            -
            F.col(
                "CalculatedReceivedQuantityAtInvoiceDate"
            ),
            3
        )
    )

    .withColumn(
        "CalculatedQuantityVariancePercentage",

        F.when(
            F.col(
                "CalculatedReceivedQuantityAtInvoiceDate"
            )
            > 0,

            F.round(
                (
                    (
                        F.col(
                            "InvoicedQuantity"
                        )
                        -
                        F.col(
                            "CalculatedReceivedQuantityAtInvoiceDate"
                        )
                    )
                    /
                    F.col(
                        "CalculatedReceivedQuantityAtInvoiceDate"
                    )
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

# **Derive quantity tolerance**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "InvoiceQuantityWithinToleranceFlag",

        F.when(
            F.col(
                "GoodsReceiptAvailableAtInvoiceDateFlag"
            ),

            F.abs(
                F.col(
                    "CalculatedQuantityVariancePercentage"
                )
            )
            <=
            F.lit(
                QUANTITY_TOLERANCE_PERCENT
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

# **Derive duplicate flag**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "DerivedDuplicateInvoiceFlag",
        (
            F.coalesce(
                F.col(
                    "HeaderDuplicateInvoiceFlag"
                ),
                F.lit(False)
            )
            |
            F.coalesce(
                F.col(
                    "DuplicateInvoiceLineFlag"
                ),
                F.lit(False)
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

# **Derive three-way match status**
# 
# This becomes the canonical Silver status.

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "DerivedThreeWayMatchStatus",

        F.when(
            F.col(
                "DerivedDuplicateInvoiceFlag"
            ),
            F.lit(
                "DUPLICATE_INVOICE"
            )
        )

        .when(
            ~F.col(
                "POReferenceResolvedFlag"
            ),
            F.lit(
                "PO_REFERENCE_NOT_FOUND"
            )
        )

        .when(
            ~F.col(
                "GoodsReceiptAvailableAtInvoiceDateFlag"
            ),
            F.lit(
                "MISSING_GOODS_RECEIPT"
            )
        )

        .when(
            (
                ~F.col(
                    "InvoicePriceWithinToleranceFlag"
                )
            )
            &
            (
                ~F.col(
                    "InvoiceQuantityWithinToleranceFlag"
                )
            ),
            F.lit(
                "PRICE_AND_QUANTITY_VARIANCE"
            )
        )

        .when(
            ~F.col(
                "InvoicePriceWithinToleranceFlag"
            ),
            F.lit(
                "PRICE_VARIANCE"
            )
        )

        .when(
            ~F.col(
                "InvoiceQuantityWithinToleranceFlag"
            ),
            F.lit(
                "QUANTITY_VARIANCE"
            )
        )

        .otherwise(
            F.lit(
                "MATCHED"
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

# **Derived match flags**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "ThreeWayMatchFlag",
        (
            F.col(
                "DerivedThreeWayMatchStatus"
            )
            ==
            "MATCHED"
        )
    )

    .withColumn(
        "InvoiceExceptionFlag",
        (
            F.col(
                "DerivedThreeWayMatchStatus"
            )
            !=
            "MATCHED"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Compare source and Silver match status**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "ThreeWayMatchSourceConsistencyFlag",
        (
            F.col(
                "SourceThreeWayMatchStatus"
            )
            ==
            F.col(
                "DerivedThreeWayMatchStatus"
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

# **Prepare FX reference**

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
            F.col("RateDate").alias(
                f"{prefix}ExactFXDate"
            ),

            F.col("Currency").alias(
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
        .partitionBy("Currency")
        .orderBy(
            F.col("RateDate").asc()
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
            F.col("Currency").alias(
                f"{prefix}FirstFXCurrency"
            ),

            F.col("RateDate").alias(
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
        .partitionBy("Currency")
        .orderBy(
            F.col("RateDate").desc()
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
            F.col("Currency").alias(
                f"{prefix}LastFXCurrency"
            ),

            F.col("RateDate").alias(
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
                F.col(date_column)
                ==
                F.col(
                    f"{prefix}ExactFXDate"
                )
            )
            &
            (
                F.col(currency_column)
                ==
                F.col(
                    f"{prefix}ExactFXCurrency"
                )
            ),
            "left"
        )

        .join(
            first_fx_df,
            F.col(currency_column)
            ==
            F.col(
                f"{prefix}FirstFXCurrency"
            ),
            "left"
        )

        .join(
            last_fx_df,
            F.col(currency_column)
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
                F.lit("EXACT_DATE")
            )

            .when(
                F.col(date_column)
                <
                F.col(
                    f"{prefix}FirstFXDate"
                ),
                F.lit(
                    "EARLIEST_AVAILABLE_RATE"
                )
            )

            .when(
                F.col(date_column)
                >
                F.col(
                    f"{prefix}LastFXDate"
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
                    f"{prefix}ExactFXRate"
                ).isNotNull(),

                F.col(
                    f"{prefix}ExactFXRate"
                )
            )

            .when(
                F.col(date_column)
                <
                F.col(
                    f"{prefix}FirstFXDate"
                ),

                F.col(
                    f"{prefix}FirstFXRate"
                )
            )

            .when(
                F.col(date_column)
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
                F.col(date_column)
                <
                F.col(
                    f"{prefix}FirstFXDate"
                ),

                F.col(
                    f"{prefix}FirstFXDate"
                )
            )

            .when(
                F.col(date_column)
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

# **Resolve incoive-date FX**

# CELL ********************

silver_invoice_matching_df = (
    add_fx_resolution(
        dataframe=(
            silver_invoice_matching_df
        ),
        date_column="InvoiceDate",
        currency_column="InvoiceCurrency",
        prefix="Invoice"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Convert invoice values to EUR**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "InvoiceUnitPriceEUR",

        F.round(
            F.col(
                "InvoiceUnitPrice"
            )
            *
            F.col(
                "InvoiceExchangeRateToEUR"
            ),
            4
        ).cast(
            "decimal(18,4)"
        )
    )

    .withColumn(
        "NetAmountEUR",

        F.round(
            F.col(
                "NetAmount"
            )
            *
            F.col(
                "InvoiceExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(18,2)"
        )
    )

    .withColumn(
        "TaxAmountEUR",

        F.round(
            F.col(
                "TaxAmount"
            )
            *
            F.col(
                "InvoiceExchangeRateToEUR"
            ),
            2
        ).cast(
            "decimal(18,2)"
        )
    )

    .withColumn(
        "GrossAmountEUR",

        F.round(
            F.col(
                "GrossAmount"
            )
            *
            F.col(
                "InvoiceExchangeRateToEUR"
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

# **Exception amount**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "InvoiceExceptionAmountEUR",

        F.when(
            F.col(
                "InvoiceExceptionFlag"
            ),
            F.col(
                "GrossAmountEUR"
            )
        ).otherwise(
            F.lit(0)
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

# **Add time attributes**

# CELL ********************

silver_invoice_matching_df = (
    silver_invoice_matching_df

    .withColumn(
        "InvoiceYear",
        F.year(
            "InvoiceDate"
        )
    )

    .withColumn(
        "InvoiceMonth",
        F.month(
            "InvoiceDate"
        )
    )

    .withColumn(
        "InvoiceYearMonth",
        F.date_format(
            "InvoiceDate",
            "yyyy-MM"
        )
    )

    .withColumn(
        "InvoiceQuarter",
        F.concat(
            F.lit("Q"),
            F.quarter(
                "InvoiceDate"
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
    "InvoiceHeaderSourceSystem",
    "InvoiceHeaderIngestionTimestamp",
    "InvoiceHeaderLoadDate",
    "InvoiceHeaderSourceRecordHash",

    "InvoiceItemSourceSystem",
    "InvoiceItemIngestionTimestamp",
    "InvoiceItemLoadDate",
    "InvoiceItemSourceRecordHash",

    "SilverLoadTimestamp",
    "SilverLoadDate",
    "SilverRecordHash"
}

hash_columns = [
    column_name
    for column_name
    in silver_invoice_matching_df.columns
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

silver_invoice_matching_df = (
    silver_invoice_matching_df

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

# **Inspect derived match distribution**

# CELL ********************

display(
    silver_invoice_matching_df

    .groupBy(
        "DerivedThreeWayMatchStatus"
    )

    .agg(
        F.count("*").alias(
            "InvoiceItemCount"
        ),

        F.round(
            F.sum(
                "GrossAmountEUR"
            ),
            2
        ).alias(
            "GrossInvoiceAmountEUR"
        )
    )

    .orderBy(
        F.desc(
            "InvoiceItemCount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Compare source vs derived match status**

# CELL ********************

display(
    silver_invoice_matching_df

    .groupBy(
        "SourceThreeWayMatchStatus",
        "DerivedThreeWayMatchStatus"
    )

    .count()

    .orderBy(
        "SourceThreeWayMatchStatus",
        "DerivedThreeWayMatchStatus"
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
                SILVER_INVOICE_MATCHING_TABLE,

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

# **Row preservation**

# CELL ********************

bronze_invoice_item_count = (
    invoice_item_df.count()
)

silver_invoice_item_count = (
    silver_invoice_matching_df.count()
)


register_validation(
    "Row Count",

    (
        "One Silver row exists per "
        "Bronze invoice item"
    ),

    abs(
        bronze_invoice_item_count
        -
        silver_invoice_item_count
    ),

    (
        f"Bronze: "
        f"{bronze_invoice_item_count:,}; "
        f"Silver: "
        f"{silver_invoice_item_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **InvoiceItemID integrity**

# CELL ********************

duplicate_invoice_item_count = (
    silver_invoice_matching_df

    .groupBy(
        "InvoiceItemID"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


register_validation(
    "Primary Key",
    "InvoiceItemID is unique",
    duplicate_invoice_item_count
)

null_invoice_item_count = (
    silver_invoice_matching_df
    .filter(
        F.col(
            "InvoiceItemID"
        ).isNull()
    )
    .count()
)


register_validation(
    "Primary Key",
    "InvoiceItemID is not null",
    null_invoice_item_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate PO references**

# CELL ********************

unresolved_po_count = (
    silver_invoice_matching_df

    .filter(
        ~F.col(
            "POReferenceResolvedFlag"
        )
    )

    .count()
)


register_validation(
    "Referential Integrity",
    (
        "Invoice items resolve to "
        "Silver PO items"
    ),
    unresolved_po_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate supplier and business unit**

# CELL ********************

invoice_po_reference_mismatch_count = (
    silver_invoice_matching_df

    .filter(
        ~F.col(
            "InvoicePOSupplierMatchFlag"
        )
        |
        ~F.col(
            "InvoicePOBusinessUnitMatchFlag"
        )
    )

    .count()
)


register_validation(
    "Referential Integrity",
    (
        "Invoice supplier and business unit "
        "agree with PO"
    ),
    invoice_po_reference_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate invoice PO unit price**

# CELL ********************

po_price_mismatch_count = (
    silver_invoice_matching_df

    .filter(
        F.abs(
            F.col(
                "SourcePOUnitPrice"
            )
            -
            F.col(
                "POUnitPrice"
            )
        )
        > F.lit(0.0001)
    )

    .count()
)


register_validation(
    "PO Consistency",
    (
        "Invoice source PO price "
        "matches Silver PO price"
    ),
    po_price_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate reconstructed receipt quantity**

# CELL ********************

receipt_quantity_mismatch_count = (
    silver_invoice_matching_df

    .filter(
        ~F.col(
            "ReceiptQuantitySourceConsistencyFlag"
        )
    )

    .count()
)


register_validation(
    "Goods Receipt",
    (
        "Source received quantity matches "
        "reconstructed receipt quantity "
        "at invoice date"
    ),
    receipt_quantity_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate invoice arithmetic**

# CELL ********************

invoice_net_amount_error_count = (
    silver_invoice_matching_df

    .filter(
        F.abs(
            F.col("NetAmount")
            -
            F.round(
                F.col(
                    "InvoicedQuantity"
                )
                *
                F.col(
                    "InvoiceUnitPrice"
                ),
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
        "Invoice NetAmount equals "
        "quantity × invoice unit price"
    ),
    invoice_net_amount_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate gross arithmetic**

# CELL ********************

invoice_gross_error_count = (
    silver_invoice_matching_df

    .filter(
        F.abs(
            F.col(
                "GrossAmount"
            )
            -
            (
                F.col(
                    "NetAmount"
                )
                +
                F.col(
                    "TaxAmount"
                )
            )
        )
        > F.lit(0.01)
    )

    .count()
)


register_validation(
    "Arithmetic",
    (
        "GrossAmount equals NetAmount "
        "plus TaxAmount"
    ),
    invoice_gross_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate FX resolution**

# CELL ********************

unresolved_invoice_fx_count = (
    silver_invoice_matching_df

    .filter(
        F.col(
            "InvoiceExchangeRateToEUR"
        ).isNull()
        |
        F.col(
            "InvoiceFXRateDate"
        ).isNull()
        |
        (
            F.col(
                "InvoiceFXResolutionMethod"
            )
            ==
            "UNRESOLVED"
        )
    )

    .count()
)


register_validation(
    "Currency Conversion",
    (
        "Invoice FX rate is resolved"
    ),
    unresolved_invoice_fx_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Silver three-way match**
# 
# Every derived status must belong to the approved domain.

# CELL ********************

VALID_MATCH_STATUSES = [
    "MATCHED",
    "PRICE_VARIANCE",
    "QUANTITY_VARIANCE",
    "PRICE_AND_QUANTITY_VARIANCE",
    "MISSING_GOODS_RECEIPT",
    "DUPLICATE_INVOICE",
    "PO_REFERENCE_NOT_FOUND"
]

invalid_match_status_count = (
    silver_invoice_matching_df

    .filter(
        ~F.col(
            "DerivedThreeWayMatchStatus"
        ).isin(
            VALID_MATCH_STATUSES
        )
    )

    .count()
)


register_validation(
    "Three-Way Match",
    (
        "Derived match status belongs "
        "to approved domain"
    ),
    invalid_match_status_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Compare source and derived match logic**

# CELL ********************

source_derived_match_mismatch_count = (
    silver_invoice_matching_df

    .filter(
        ~F.col(
            "ThreeWayMatchSourceConsistencyFlag"
        )
    )

    .count()
)


register_validation(
    "Three-Way Match",
    (
        "Source simulated match status "
        "agrees with independently derived "
        "Silver match status"
    ),
    source_derived_match_mismatch_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate duplicate classification**

# CELL ********************

duplicate_classification_error_count = (
    silver_invoice_matching_df

    .filter(
        F.col(
            "DerivedDuplicateInvoiceFlag"
        )
        &
        (
            F.col(
                "DerivedThreeWayMatchStatus"
            )
            != "DUPLICATE_INVOICE"
        )
    )

    .count()
)


register_validation(
    "Duplicate Invoice",
    (
        "Duplicate invoices are classified "
        "as DUPLICATE_INVOICE"
    ),
    duplicate_classification_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate matched invoices**

# CELL ********************

invalid_matched_invoice_count = (
    silver_invoice_matching_df

    .filter(
        F.col(
            "ThreeWayMatchFlag"
        )
        &
        (
            ~F.col(
                "GoodsReceiptAvailableAtInvoiceDateFlag"
            )
            |
            ~F.col(
                "InvoicePriceWithinToleranceFlag"
            )
            |
            ~F.col(
                "InvoiceQuantityWithinToleranceFlag"
            )
        )
    )

    .count()
)


register_validation(
    "Three-Way Match",
    (
        "Matched invoice items satisfy "
        "receipt, price and quantity rules"
    ),
    invalid_matched_invoice_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate lineage**

# CELL ********************

invalid_lineage_count = (
    silver_invoice_matching_df

    .filter(
        F.col(
            "InvoiceHeaderSourceRecordHash"
        ).isNull()
        |
        F.col(
            "InvoiceItemSourceRecordHash"
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
    "Silver Invoice Matching "
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
        f"Silver invoice matching "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "SILVER INVOICE MATCHING "
    "PRE-WRITE QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write silver_invoice_matching**

# CELL ********************

(
    silver_invoice_matching_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        SILVER_INVOICE_MATCHING_TABLE
    )
)


print(
    "Created physical Silver table:",
    SILVER_INVOICE_MATCHING_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted row count**

# CELL ********************

persisted_invoice_matching_df = (
    spark.table(
        SILVER_INVOICE_MATCHING_TABLE
    )
)


expected_row_count = (
    silver_invoice_matching_df.count()
)

persisted_row_count = (
    persisted_invoice_matching_df.count()
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

# **Three-way-match summary**

# CELL ********************

display(
    persisted_invoice_matching_df

    .groupBy(
        "DerivedThreeWayMatchStatus"
    )

    .agg(
        F.count("*").alias(
            "InvoiceItemCount"
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
            "ExceptionAmountEUR"
        )
    )

    .orderBy(
        F.desc(
            "GrossInvoiceAmountEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Annual invoice-match KPI preview**

# CELL ********************

annual_invoice_match_df = (
    persisted_invoice_matching_df

    .groupBy(
        "InvoiceYear"
    )

    .agg(
        F.count(
            "InvoiceItemID"
        ).alias(
            "InvoiceItemCount"
        ),

        F.sum(
            F.when(
                F.col(
                    "ThreeWayMatchFlag"
                ),
                1
            ).otherwise(0)
        ).alias(
            "MatchedInvoiceItemCount"
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
            "ExceptionAmountEUR"
        )
    )

    .withColumn(
        "ThreeWayMatchPct",

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

    .orderBy(
        "InvoiceYear"
    )
)


display(
    annual_invoice_match_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Exception analysis**

# CELL ********************

display(
    persisted_invoice_matching_df

    .filter(
        F.col(
            "InvoiceExceptionFlag"
        )
    )

    .groupBy(
        "DerivedThreeWayMatchStatus"
    )

    .agg(
        F.count(
            "InvoiceItemID"
        ).alias(
            "InvoiceItemCount"
        ),

        F.round(
            F.sum(
                "InvoiceExceptionAmountEUR"
            ),
            2
        ).alias(
            "ExceptionAmountEUR"
        )
    )

    .orderBy(
        F.desc(
            "ExceptionAmountEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Supplier exception analysis**

# CELL ********************

display(
    persisted_invoice_matching_df

    .filter(
        F.col(
            "InvoiceExceptionFlag"
        )
    )

    .groupBy(
        "POSupplierID"
    )

    .agg(
        F.count(
            "InvoiceItemID"
        ).alias(
            "ExceptionItemCount"
        ),

        F.round(
            F.sum(
                "InvoiceExceptionAmountEUR"
            ),
            2
        ).alias(
            "ExceptionAmountEUR"
        )
    )

    .orderBy(
        F.desc(
            "ExceptionAmountEUR"
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

# **Inspect final table**

# CELL ********************

display(
    persisted_invoice_matching_df

    .select(
        "InvoiceID",
        "InvoiceNumber",
        "InvoiceItemID",
        "InvoiceLineNumber",

        "InvoiceDate",

        "POID",
        "POItemID",

        "POSupplierID",
        "BuyerName",

        "MaterialDescription",
        "CategoryName",

        "InvoiceCurrency",

        "OrderedQuantity",
        "InvoicedQuantity",

        "CalculatedReceivedQuantityAtInvoiceDate",

        "POUnitPrice",
        "InvoiceUnitPrice",

        "CalculatedPriceVariancePercentage",
        "CalculatedQuantityVariancePercentage",

        "GoodsReceiptAvailableAtInvoiceDateFlag",
        "InvoicePriceWithinToleranceFlag",
        "InvoiceQuantityWithinToleranceFlag",

        "SourceThreeWayMatchStatus",
        "DerivedThreeWayMatchStatus",

        "ThreeWayMatchFlag",
        "InvoiceExceptionFlag",

        "GrossAmountEUR",
        "InvoiceExceptionAmountEUR",

        "DisputeFlag",
        "DerivedDuplicateInvoiceFlag",

        "ContractComplianceFlag",
        "MaverickSpendFlag"
    )

    .orderBy(
        "InvoiceID",
        "InvoiceLineNumber"
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
    "NB_23_Build_Silver_Invoice_Matching "
    "completed successfully."
)

print()

print(
    "Physical Silver output:"
)

print(
    "  silver_invoice_matching"
)

print()

print(
    "Grain:"
)

print(
    "  One row per invoice item"
)

print()

print(
    "Core analytical fields:"
)

print(
    "  - DerivedThreeWayMatchStatus"
)

print(
    "  - ThreeWayMatchFlag"
)

print(
    "  - InvoiceExceptionFlag"
)

print(
    "  - CalculatedPriceVariancePercentage"
)

print(
    "  - CalculatedQuantityVariancePercentage"
)

print(
    "  - CalculatedReceivedQuantityAtInvoiceDate"
)

print(
    "  - GrossAmountEUR"
)

print(
    "  - InvoiceExceptionAmountEUR"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
