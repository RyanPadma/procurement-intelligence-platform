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

# **Output** = _fact_invoice_
# 
# **Grain** = one row per _InvoiceItemID_
# 
# This notebook will use _fact_purchase_order_ to inherit PO-derived conformed dimension keys, while resolving the invoice's own _SupplierKey_ by _InvoiceDate_ and _CurrencyKey_ by invoice currency. Defensive source-column resolution is also being used  because the earlier invoice-generation work already uses alias resolution to accomodate alternate ERP-style column names.

# MARKDOWN ********************

# **Configuration**

# CELL ********************

# ============================================================
# NB_32_Build_Gold_Fact_Invoices
# Configuration
# ============================================================

SILVER_INVOICE_MATCHING_TABLE = (
    "silver_invoice_matching"
)

SILVER_INVOICE_MONITORING_TABLE = (
    "monitoring_silver_invoice_matching_quality_results"
)

GOLD_DIMENSION_MONITORING_TABLE = (
    "monitoring_gold_dimensions_quality_results"
)

GOLD_PO_MONITORING_TABLE = (
    "monitoring_gold_fact_purchase_order_quality_results"
)


FACT_PO_TABLE = (
    "fact_purchase_order"
)

DIM_DATE_TABLE = (
    "dim_date"
)

DIM_SUPPLIER_TABLE = (
    "dim_supplier"
)

DIM_CURRENCY_TABLE = (
    "dim_currency"
)


FACT_INVOICE_TABLE = (
    "fact_invoice"
)

GOLD_MONITORING_TABLE = (
    "monitoring_gold_fact_invoice_quality_results"
)


print(
    "Notebook: NB_32_Build_Gold_Fact_Invoices"
)

print(
    "Default Lakehouse: lh_procurement_gold"
)

print(
    "Output table: fact_invoice"
)

print(
    "Grain: one row per InvoiceItemID"
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
    SILVER_INVOICE_MATCHING_TABLE,
    SILVER_INVOICE_MONITORING_TABLE,
    GOLD_DIMENSION_MONITORING_TABLE,
    GOLD_PO_MONITORING_TABLE,

    FACT_PO_TABLE,

    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE,
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

silver_invoice_failure_count = (
    spark.table(
        SILVER_INVOICE_MONITORING_TABLE
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


gold_po_failure_count = (
    spark.table(
        GOLD_PO_MONITORING_TABLE
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
    "NB_23 Silver Invoice Matching failures:",
    silver_invoice_failure_count
)

print(
    "NB_30 Gold Dimensions failures:",
    gold_dimension_failure_count
)

print(
    "NB_31 Gold Purchase Order Fact failures:",
    gold_po_failure_count
)


assert (
    silver_invoice_failure_count == 0
), (
    "NB_23 Silver Invoice Matching "
    "quality gate has not passed."
)


assert (
    gold_dimension_failure_count == 0
), (
    "NB_30 Gold Dimensions "
    "quality gate has not passed."
)


assert (
    gold_po_failure_count == 0
), (
    "NB_31 Gold Purchase Order Fact "
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

# **Load source and Gold references**

# CELL ********************

silver_invoice_matching_df = (
    spark.table(
        SILVER_INVOICE_MATCHING_TABLE
    )
)

fact_po_df = (
    spark.table(
        FACT_PO_TABLE
    )
)

dim_date_df = (
    spark.table(
        DIM_DATE_TABLE
    )
)

dim_supplier_df = (
    spark.table(
        DIM_SUPPLIER_TABLE
    )
)

dim_currency_df = (
    spark.table(
        DIM_CURRENCY_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Silver invoice items:",
    f"{silver_invoice_matching_df.count():,}"
)

print(
    "Gold PO fact rows:",
    f"{fact_po_df.count():,}"
)

print(
    "Supplier dimension rows:",
    f"{dim_supplier_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Source-column resolution helper**

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
            f"Required Silver invoice column "
            f"'{logical_name}' was not found. "
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

        expression = F.lit(None)

    else:

        expression = F.col(
            source_column
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

# **Resolve Silver invoice columns**

# CELL ********************

SOURCE_COLUMNS = {
    # --------------------------------------------------------
    # Business keys
    # --------------------------------------------------------

    "InvoiceID":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceID",
            [
                "InvoiceID"
            ]
        ),

    "InvoiceNumber":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceNumber",
            [
                "InvoiceNumber",
                "SupplierInvoiceNumber"
            ],
            required=False
        ),

    "InvoiceItemID":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceItemID",
            [
                "InvoiceItemID"
            ]
        ),

    "InvoiceLineNumber":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceLineNumber",
            [
                "InvoiceLineNumber"
            ]
        ),

    "POID":
        resolve_column_name(
            silver_invoice_matching_df,
            "POID",
            [
                "POID"
            ]
        ),

    "POItemID":
        resolve_column_name(
            silver_invoice_matching_df,
            "POItemID",
            [
                "POItemID"
            ]
        ),

    "SupplierID":
        resolve_column_name(
            silver_invoice_matching_df,
            "SupplierID",
            [
                "POSupplierID",
                "SupplierID"
            ]
        ),

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    "InvoiceDate":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceDate",
            [
                "InvoiceDate"
            ]
        ),

    "PostingDate":
        resolve_column_name(
            silver_invoice_matching_df,
            "PostingDate",
            [
                "PostingDate"
            ]
        ),

    "DueDate":
        resolve_column_name(
            silver_invoice_matching_df,
            "DueDate",
            [
                "DueDate"
            ]
        ),

    # --------------------------------------------------------
    # Currency
    # --------------------------------------------------------

    "CurrencyCode":
        resolve_column_name(
            silver_invoice_matching_df,
            "Invoice Currency",
            [
                "InvoiceCurrency",
                "Currency"
            ]
        ),

    # --------------------------------------------------------
    # Header attributes
    # --------------------------------------------------------

    "InvoiceStatus":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceStatus",
            [
                "InvoiceStatus"
            ],
            required=False
        ),

    "PaymentStatus":
        resolve_column_name(
            silver_invoice_matching_df,
            "PaymentStatus",
            [
                "PaymentStatus"
            ],
            required=False
        ),

    "DisputeFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "DisputeFlag",
            [
                "DisputeFlag"
            ]
        ),

    "DisputeReason":
        resolve_column_name(
            silver_invoice_matching_df,
            "DisputeReason",
            [
                "DisputeReason"
            ],
            required=False
        ),

    # --------------------------------------------------------
    # Quantities and values
    # --------------------------------------------------------

    "InvoicedQuantity":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoicedQuantity",
            [
                "InvoicedQuantity"
            ]
        ),

    "UnitOfMeasure":
        resolve_column_name(
            silver_invoice_matching_df,
            "UnitOfMeasure",
            [
                "UnitOfMeasure"
            ],
            required=False
        ),

    "POUnitPrice":
        resolve_column_name(
            silver_invoice_matching_df,
            "POUnitPrice",
            [
                "POUnitPrice"
            ]
        ),

    "InvoiceUnitPrice":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceUnitPrice",
            [
                "InvoiceUnitPrice"
            ]
        ),

    "NetAmount":
        resolve_column_name(
            silver_invoice_matching_df,
            "NetAmount",
            [
                "NetAmount"
            ]
        ),

    "TaxAmount":
        resolve_column_name(
            silver_invoice_matching_df,
            "TaxAmount",
            [
                "TaxAmount"
            ]
        ),

    "GrossAmount":
        resolve_column_name(
            silver_invoice_matching_df,
            "GrossAmount",
            [
                "GrossAmount"
            ]
        ),

    "InvoiceUnitPriceEUR":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceUnitPriceEUR",
            [
                "InvoiceUnitPriceEUR"
            ]
        ),

    "NetAmountEUR":
        resolve_column_name(
            silver_invoice_matching_df,
            "NetAmountEUR",
            [
                "NetAmountEUR"
            ]
        ),

    "TaxAmountEUR":
        resolve_column_name(
            silver_invoice_matching_df,
            "TaxAmountEUR",
            [
                "TaxAmountEUR"
            ]
        ),

    "GrossAmountEUR":
        resolve_column_name(
            silver_invoice_matching_df,
            "GrossAmountEUR",
            [
                "GrossAmountEUR"
            ]
        ),

    # --------------------------------------------------------
    # Goods receipt / matching
    # --------------------------------------------------------

    "CalculatedReceivedQuantityAtInvoiceDate":
        resolve_column_name(
            silver_invoice_matching_df,
            "CalculatedReceivedQuantityAtInvoiceDate",
            [
                "CalculatedReceivedQuantityAtInvoiceDate"
            ]
        ),

    "GoodsReceiptCountAtInvoiceDate":
        resolve_column_name(
            silver_invoice_matching_df,
            "GoodsReceiptCountAtInvoiceDate",
            [
                "GoodsReceiptCountAtInvoiceDate"
            ],
            required=False
        ),

    "GoodsReceiptAvailableAtInvoiceDateFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "GoodsReceiptAvailableAtInvoiceDateFlag",
            [
                "GoodsReceiptAvailableAtInvoiceDateFlag"
            ]
        ),

    # --------------------------------------------------------
    # Variance analytics
    # --------------------------------------------------------

    "CalculatedPriceVarianceAmount":
        resolve_column_name(
            silver_invoice_matching_df,
            "CalculatedPriceVarianceAmount",
            [
                "CalculatedPriceVarianceAmount"
            ]
        ),

    "CalculatedPriceVariancePercentage":
        resolve_column_name(
            silver_invoice_matching_df,
            "CalculatedPriceVariancePercentage",
            [
                "CalculatedPriceVariancePercentage"
            ]
        ),

    "InvoicePriceWithinToleranceFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoicePriceWithinToleranceFlag",
            [
                "InvoicePriceWithinToleranceFlag"
            ]
        ),

    "CalculatedQuantityVariance":
        resolve_column_name(
            silver_invoice_matching_df,
            "CalculatedQuantityVariance",
            [
                "CalculatedQuantityVariance"
            ]
        ),

    "CalculatedQuantityVariancePercentage":
        resolve_column_name(
            silver_invoice_matching_df,
            "CalculatedQuantityVariancePercentage",
            [
                "CalculatedQuantityVariancePercentage"
            ]
        ),

    "InvoiceQuantityWithinToleranceFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceQuantityWithinToleranceFlag",
            [
                "InvoiceQuantityWithinToleranceFlag"
            ]
        ),

    # --------------------------------------------------------
    # Exception logic
    # --------------------------------------------------------

    "POReferenceResolvedFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "POReferenceResolvedFlag",
            [
                "POReferenceResolvedFlag"
            ]
        ),

    "InvoicePOSupplierMatchFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoicePOSupplierMatchFlag",
            [
                "InvoicePOSupplierMatchFlag"
            ],
            required=False
        ),

    "DerivedDuplicateInvoiceFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "DerivedDuplicateInvoiceFlag",
            [
                "DerivedDuplicateInvoiceFlag"
            ]
        ),

    "DerivedThreeWayMatchStatus":
        resolve_column_name(
            silver_invoice_matching_df,
            "DerivedThreeWayMatchStatus",
            [
                "DerivedThreeWayMatchStatus"
            ]
        ),

    "ThreeWayMatchFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "ThreeWayMatchFlag",
            [
                "ThreeWayMatchFlag"
            ]
        ),

    "InvoiceExceptionFlag":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceExceptionFlag",
            [
                "InvoiceExceptionFlag"
            ]
        ),

    "InvoiceExceptionAmountEUR":
        resolve_column_name(
            silver_invoice_matching_df,
            "InvoiceExceptionAmountEUR",
            [
                "InvoiceExceptionAmountEUR"
            ]
        ),

    # --------------------------------------------------------
    # Lineage
    # --------------------------------------------------------

    "SilverRecordHash":
        resolve_column_name(
            silver_invoice_matching_df,
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

# **Review resolved source mapping**

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

# **Build canonical Invoice source**

# CELL ********************

invoice_source_df = (
    silver_invoice_matching_df

    .select(
        # ----------------------------------------------------
        # Business keys
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceID"
            ],
            "InvoiceID",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceNumber"
            ],
            "InvoiceNumber",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceItemID"
            ],
            "InvoiceItemID",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceLineNumber"
            ],
            "InvoiceLineNumber",
            "int"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "POID"
            ],
            "POID",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "POItemID"
            ],
            "POItemID",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "SupplierID"
            ],
            "SupplierID",
            "string"
        ),

        # ----------------------------------------------------
        # Dates
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceDate"
            ],
            "InvoiceDate",
            "date"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "PostingDate"
            ],
            "PostingDate",
            "date"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DueDate"
            ],
            "DueDate",
            "date"
        ),

        # ----------------------------------------------------
        # Currency
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "CurrencyCode"
            ],
            "CurrencyCode",
            "string"
        ),

        # ----------------------------------------------------
        # Invoice header attributes
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceStatus"
            ],
            "InvoiceStatus",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "PaymentStatus"
            ],
            "PaymentStatus",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DisputeFlag"
            ],
            "DisputeFlag",
            "boolean"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DisputeReason"
            ],
            "DisputeReason",
            "string"
        ),

        # ----------------------------------------------------
        # Quantity / source-currency values
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoicedQuantity"
            ],
            "InvoicedQuantity",
            "decimal(18,3)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "UnitOfMeasure"
            ],
            "UnitOfMeasure",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "POUnitPrice"
            ],
            "POUnitPrice",
            "decimal(20,6)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceUnitPrice"
            ],
            "InvoiceUnitPrice",
            "decimal(20,6)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "NetAmount"
            ],
            "NetAmount",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "TaxAmount"
            ],
            "TaxAmount",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "GrossAmount"
            ],
            "GrossAmount",
            "decimal(20,2)"
        ),

        # ----------------------------------------------------
        # EUR analytical values
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceUnitPriceEUR"
            ],
            "InvoiceUnitPriceEUR",
            "decimal(20,6)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "NetAmountEUR"
            ],
            "NetAmountEUR",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "TaxAmountEUR"
            ],
            "TaxAmountEUR",
            "decimal(20,2)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "GrossAmountEUR"
            ],
            "GrossAmountEUR",
            "decimal(20,2)"
        ),

        # ----------------------------------------------------
        # Goods receipt matching
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "CalculatedReceivedQuantityAtInvoiceDate"
            ],
            "CalculatedReceivedQuantityAtInvoiceDate",
            "decimal(18,3)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "GoodsReceiptCountAtInvoiceDate"
            ],
            "GoodsReceiptCountAtInvoiceDate",
            "long"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "GoodsReceiptAvailableAtInvoiceDateFlag"
            ],
            "GoodsReceiptAvailableAtInvoiceDateFlag",
            "boolean"
        ),

        # ----------------------------------------------------
        # Price variance
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "CalculatedPriceVarianceAmount"
            ],
            "CalculatedPriceVarianceAmount",
            "decimal(20,6)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "CalculatedPriceVariancePercentage"
            ],
            "CalculatedPriceVariancePercentage",
            "double"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoicePriceWithinToleranceFlag"
            ],
            "InvoicePriceWithinToleranceFlag",
            "boolean"
        ),

        # ----------------------------------------------------
        # Quantity variance
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "CalculatedQuantityVariance"
            ],
            "CalculatedQuantityVariance",
            "decimal(18,3)"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "CalculatedQuantityVariancePercentage"
            ],
            "CalculatedQuantityVariancePercentage",
            "double"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceQuantityWithinToleranceFlag"
            ],
            "InvoiceQuantityWithinToleranceFlag",
            "boolean"
        ),

        # ----------------------------------------------------
        # Exception logic
        # ----------------------------------------------------

        resolved_expression(
            SOURCE_COLUMNS[
                "POReferenceResolvedFlag"
            ],
            "POReferenceResolvedFlag",
            "boolean"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoicePOSupplierMatchFlag"
            ],
            "InvoicePOSupplierMatchFlag",
            "boolean"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DerivedDuplicateInvoiceFlag"
            ],
            "DerivedDuplicateInvoiceFlag",
            "boolean"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "DerivedThreeWayMatchStatus"
            ],
            "DerivedThreeWayMatchStatus",
            "string"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "ThreeWayMatchFlag"
            ],
            "ThreeWayMatchFlag",
            "boolean"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceExceptionFlag"
            ],
            "InvoiceExceptionFlag",
            "boolean"
        ),

        resolved_expression(
            SOURCE_COLUMNS[
                "InvoiceExceptionAmountEUR"
            ],
            "InvoiceExceptionAmountEUR",
            "decimal(20,2)"
        ),

        # ----------------------------------------------------
        # Lineage
        # ----------------------------------------------------

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

# **Inspect invoice source population**

# CELL ********************

source_invoice_item_count = (
    invoice_source_df.count()
)


source_invoice_count = (
    invoice_source_df

    .select(
        "InvoiceID"
    )

    .distinct()

    .count()
)


print(
    "Invoice items:",
    f"{source_invoice_item_count:,}"
)

print(
    "Invoices:",
    f"{source_invoice_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Purchase Order Fact reference**
# 
# Rather than redoing Category, Material, Buyer and Business Unit lookups, inherit the already validated keys from NB_31.

# CELL ********************

po_fact_reference_df = (
    fact_po_df

    .select(
        F.col(
            "POItemID"
        ).alias(
            "RefPOItemID"
        ),

        F.col(
            "POID"
        ).alias(
            "RefPOID"
        ),

        "PurchaseOrderFactKey",

        "OrderDateKey",
        "RequestedDeliveryDateKey",

        F.col(
            "SupplierKey"
        ).alias(
            "POSupplierKey"
        ),

        F.col(
            "SupplierID"
        ).alias(
            "POSupplierIDFromFact"
        ),

        "CategoryKey",
        "MaterialKey",
        "BuyerKey",
        "BusinessUnitKey",
        "ContractKey",

        F.col(
            "CurrencyKey"
        ).alias(
            "POCurrencyKey"
        ),

        F.col(
            "CurrencyCode"
        ).alias(
            "POCurrencyCode"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Purchase Order Fact**

# CELL ********************

fact_invoice_df = (
    invoice_source_df.alias(
        "invoice"
    )

    .join(
        po_fact_reference_df.alias(
            "po"
        ),

        F.col(
            "invoice.POItemID"
        )
        ==
        F.col(
            "po.RefPOItemID"
        ),

        "left"
    )

    .select(
        F.col(
            "invoice.*"
        ),

        F.col(
            "po.RefPOID"
        ),

        F.col(
            "po.PurchaseOrderFactKey"
        ),

        F.col(
            "po.OrderDateKey"
        ),

        F.col(
            "po.RequestedDeliveryDateKey"
        ),

        F.col(
            "po.POSupplierKey"
        ),

        F.col(
            "po.POSupplierIDFromFact"
        ),

        F.col(
            "po.CategoryKey"
        ),

        F.col(
            "po.MaterialKey"
        ),

        F.col(
            "po.BuyerKey"
        ),

        F.col(
            "po.BusinessUnitKey"
        ),

        F.col(
            "po.ContractKey"
        ),

        F.col(
            "po.POCurrencyKey"
        ),

        F.col(
            "po.POCurrencyCode"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add PO Fact resolution checks**

# CELL ********************

fact_invoice_df = (
    fact_invoice_df

    .withColumn(
        "PurchaseOrderFactResolvedFlag",

        F.col(
            "PurchaseOrderFactKey"
        ).isNotNull()
    )

    .withColumn(
        "POIDFactConsistencyFlag",

        F.when(
            F.col(
                "PurchaseOrderFactResolvedFlag"
            ),

            F.col(
                "POID"
            )
            ==
            F.col(
                "RefPOID"
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

# **Validate PO Fact join did nit duplicate invoice items**

# CELL ********************

po_join_duplicate_count = (
    fact_invoice_df

    .groupBy(
        "InvoiceItemID"
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
    "Invoice items duplicated by PO Fact join:",
    po_join_duplicate_count
)


assert (
    po_join_duplicate_count == 0
), (
    "Purchase Order Fact join created "
    "duplicate InvoiceItemID rows."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Invoice Date dimension**

# CELL ********************

invoice_date_reference_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimInvoiceDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "InvoiceDateKey"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Invoice Date dimension**

# CELL ********************

fact_invoice_df = (
    fact_invoice_df

    .join(
        invoice_date_reference_df,

        fact_invoice_df[
            "InvoiceDate"
        ]
        ==
        invoice_date_reference_df[
            "DimInvoiceDate"
        ],

        "left"
    )

    .drop(
        "DimInvoiceDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **JOin Posting Date dimension**

# CELL ********************

posting_date_reference_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimPostingDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "PostingDateKey"
        )
    )
)


fact_invoice_df = (
    fact_invoice_df

    .join(
        posting_date_reference_df,

        fact_invoice_df[
            "PostingDate"
        ]
        ==
        posting_date_reference_df[
            "DimPostingDate"
        ],

        "left"
    )

    .drop(
        "DimPostingDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join Due Date dimension**

# CELL ********************

due_date_reference_df = (
    dim_date_df

    .select(
        F.col(
            "Date"
        ).alias(
            "DimDueDate"
        ),

        F.col(
            "DateKey"
        ).alias(
            "DueDateKey"
        )
    )
)


fact_invoice_df = (
    fact_invoice_df

    .join(
        due_date_reference_df,

        fact_invoice_df[
            "DueDate"
        ]
        ==
        due_date_reference_df[
            "DimDueDate"
        ],

        "left"
    )

    .drop(
        "DimDueDate"
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
# 
# For the invoice fact, supplier history is resolved based on:
# 
# SupplierID
# +
# InvoiceDate
# 
# not PO OrderDate.

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

# **Resolve SupplierKey by InvoiceDate**

# CELL ********************

fact_invoice_df = (
    fact_invoice_df.alias(
        "invoice"
    )

    .join(
        supplier_dimension_reference_df.alias(
            "supplier"
        ),

        (
            F.col(
                "invoice.SupplierID"
            )
            ==
            F.col(
                "supplier.DimSupplierID"
            )
        )
        &
        (
            F.col(
                "invoice.InvoiceDate"
            )
            >=
            F.col(
                "supplier.EffectiveFromDate"
            )
        )
        &
        (
            F.col(
                "invoice.InvoiceDate"
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
            "invoice.*"
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

# **Validate Supplier SCD join did not duplicate rows**

# CELL ********************

supplier_join_duplicate_count = (
    fact_invoice_df

    .groupBy(
        "InvoiceItemID"
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
    "Invoice items duplicated by Supplier SCD join:",
    supplier_join_duplicate_count
)


assert (
    supplier_join_duplicate_count == 0
), (
    "Supplier SCD Type 2 join "
    "created duplicate InvoiceItemID rows."
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


fact_invoice_df = (
    fact_invoice_df

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

# **Add Invoice Fact key**

# CELL ********************

fact_invoice_df = (
    fact_invoice_df

    .withColumn(
        "InvoiceFactKey",

        F.xxhash64(
            F.col(
                "InvoiceItemID"
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

# **Add Gold analytical measures**
# 
# These make Power BI measures simpler without changing the underlying Silver logic.

# CELL ********************

fact_invoice_df = (
    fact_invoice_df

    .withColumn(
        "MatchedInvoiceAmountEUR",

        F.when(
            F.col(
                "ThreeWayMatchFlag"
            ),

            F.col(
                "GrossAmountEUR"
            )
        )

        .otherwise(
            F.lit(0)
        )

        .cast(
            "decimal(20,2)"
        )
    )

    .withColumn(
        "DisputedInvoiceItemFlag",

        F.coalesce(
            F.col(
                "DisputeFlag"
            ),
            F.lit(False)
        )
    )

    .withColumn(
        "DuplicateInvoiceItemFlag",

        F.coalesce(
            F.col(
                "DerivedDuplicateInvoiceFlag"
            ),
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
    in fact_invoice_df.columns
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

fact_invoice_df = (
    fact_invoice_df

    .withColumn(
        "GoldSourceTable",
        F.lit(
            SILVER_INVOICE_MATCHING_TABLE
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

# Inspect dimensional key resolution

# CELL ********************

display(
    fact_invoice_df

    .select(
        "InvoiceItemID",
        "InvoiceID",

        "InvoiceDate",
        "InvoiceDateKey",

        "PostingDate",
        "PostingDateKey",

        "DueDate",
        "DueDateKey",

        "SupplierID",
        "SupplierKey",
        "SupplierDimensionVersion",

        "POItemID",
        "PurchaseOrderFactKey",

        "CategoryKey",
        "MaterialKey",
        "BuyerKey",
        "BusinessUnitKey",
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
                FACT_INVOICE_TABLE,

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

gold_invoice_item_count = (
    fact_invoice_df.count()
)


register_validation(
    "Row Count",

    (
        "One Gold fact row exists "
        "per Silver invoice item"
    ),

    abs(
        source_invoice_item_count
        -
        gold_invoice_item_count
    ),

    (
        f"Silver: "
        f"{source_invoice_item_count:,}; "
        f"Gold: "
        f"{gold_invoice_item_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate InvoiceItemID grain**

# CELL ********************

duplicate_invoice_item_count = (
    fact_invoice_df

    .groupBy(
        "InvoiceItemID"
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
    "InvoiceItemID is unique",
    duplicate_invoice_item_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_fact_key_count = (
    fact_invoice_df

    .groupBy(
        "InvoiceFactKey"
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
        "InvoiceFactKey is unique"
    ),

    duplicate_fact_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate mandatory invoice dimension keys**

# CELL ********************

missing_invoice_dimension_key_count = (
    fact_invoice_df

    .filter(
        F.col(
            "InvoiceDateKey"
        ).isNull()
        |
        F.col(
            "PostingDateKey"
        ).isNull()
        |
        F.col(
            "DueDateKey"
        ).isNull()
        |
        F.col(
            "SupplierKey"
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
        "Invoice Date, Posting Date, "
        "Due Date, Supplier and Currency "
        "keys are resolved"
    ),

    missing_invoice_dimension_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate PO Fact resolution**
# 
# An invoice with a Silver-resolved PO reference must resolve to NB_31.

# CELL ********************

missing_po_fact_count = (
    fact_invoice_df

    .filter(
        F.col(
            "POReferenceResolvedFlag"
        )
        &
        (
            ~F.col(
                "PurchaseOrderFactResolvedFlag"
            )
        )
    )

    .count()
)


register_validation(
    "Referential Integrity",

    (
        "Silver-resolved PO references "
        "resolve to fact_purchase_order"
    ),

    missing_po_fact_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate PO-derived Gold dimensions**
# 
# Only require these when the PO reference itself is valid._ContractKey_ is deliberately excluded because a PO item may legitimately have no contract.

# CELL ********************

missing_po_dimension_key_count = (
    fact_invoice_df

    .filter(
        F.col(
            "POReferenceResolvedFlag"
        )
        &
        (
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
        )
    )

    .count()
)


register_validation(
    "Referential Integrity",

    (
        "Resolved PO references carry "
        "Category, Material, Buyer and "
        "Business Unit keys"
    ),

    missing_po_dimension_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate POID consistency**

# CELL ********************

po_id_consistency_error_count = (
    fact_invoice_df

    .filter(
        F.col(
            "PurchaseOrderFactResolvedFlag"
        )
        &
        (
            ~F.coalesce(
                F.col(
                    "POIDFactConsistencyFlag"
                ),
                F.lit(False)
            )
        )
    )

    .count()
)


register_validation(
    "PO Relationship",

    (
        "Invoice POID matches the "
        "Purchase Order Fact POID"
    ),

    po_id_consistency_error_count
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
    fact_invoice_df

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
        "Every invoice item resolves to "
        "the Supplier version valid "
        "on InvoiceDate"
    ),

    unresolved_supplier_key_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate three-way-match classification**

# CELL ********************

match_exception_reconciliation_error_count = (
    fact_invoice_df

    .filter(
        F.col(
            "ThreeWayMatchFlag"
        ).isNull()
        |
        F.col(
            "InvoiceExceptionFlag"
        ).isNull()
        |
        (
            F.col(
                "ThreeWayMatchFlag"
            )
            ==
            F.col(
                "InvoiceExceptionFlag"
            )
        )
    )

    .count()
)


register_validation(
    "Three-Way Match",

    (
        "Each invoice item is either "
        "three-way matched or an exception"
    ),

    match_exception_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate exception amount logic**

# CELL ********************

exception_amount_error_count = (
    fact_invoice_df

    .filter(
        (
            ~F.col(
                "InvoiceExceptionFlag"
            )
        )
        &
        (
            F.abs(
                F.coalesce(
                    F.col(
                        "InvoiceExceptionAmountEUR"
                    ),
                    F.lit(0)
                )
            )
            >
            F.lit(0.01)
        )
    )

    .count()
)


register_validation(
    "Three-Way Match",

    (
        "Matched invoice items contribute "
        "zero InvoiceExceptionAmountEUR"
    ),

    exception_amount_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate matched amount logic**

# CELL ********************

matched_amount_error_count = (
    fact_invoice_df

    .filter(
        F.abs(
            F.col(
                "MatchedInvoiceAmountEUR"
            )
            -
            F.when(
                F.col(
                    "ThreeWayMatchFlag"
                ),

                F.col(
                    "GrossAmountEUR"
                )
            )
            .otherwise(
                F.lit(0)
            )
        )
        >
        F.lit(0.01)
    )

    .count()
)


register_validation(
    "Three-Way Match",

    (
        "MatchedInvoiceAmountEUR agrees "
        "with ThreeWayMatchFlag"
    ),

    matched_amount_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate EUR invoice arithmetic**

# CELL ********************

invoice_amount_reconciliation_error_count = (
    fact_invoice_df

    .filter(
        F.abs(
            F.col(
                "GrossAmountEUR"
            )
            -
            (
                F.col(
                    "NetAmountEUR"
                )
                +
                F.col(
                    "TaxAmountEUR"
                )
            )
        )
        >
        F.lit(0.02)
    )

    .count()
)


register_validation(
    "Monetary Values",

    (
        "GrossAmountEUR equals "
        "NetAmountEUR plus TaxAmountEUR"
    ),

    invoice_amount_reconciliation_error_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate invoice amounts**

# CELL ********************

invalid_invoice_amount_count = (
    fact_invoice_df

    .filter(
        F.col(
            "GrossAmount"
        ).isNull()
        |
        F.col(
            "GrossAmountEUR"
        ).isNull()
        |
        (
            F.col(
                "GrossAmount"
            )
            < 0
        )
        |
        (
            F.col(
                "GrossAmountEUR"
            )
            < 0
        )
    )

    .count()
)


register_validation(
    "Monetary Values",

    (
        "Invoice gross amounts in "
        "source currency and EUR are valid"
    ),

    invalid_invoice_amount_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate _InvoiceDateKey_**

# CELL ********************

invoice_date_key_error_count = (
    fact_invoice_df

    .filter(
        F.col(
            "InvoiceDateKey"
        )
        !=
        F.date_format(
            "InvoiceDate",
            "yyyyMMdd"
        ).cast("int")
    )

    .count()
)


register_validation(
    "Date Dimension",

    (
        "InvoiceDateKey corresponds "
        "to InvoiceDate"
    ),

    invoice_date_key_error_count
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
    fact_invoice_df

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
    "Gold Invoice Fact "
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
        f"Gold Invoice Fact "
        f"validation failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "GOLD INVOICE FACT "
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
    fact_invoice_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        FACT_INVOICE_TABLE
    )
)


print(
    "Created physical Gold fact:",
    FACT_INVOICE_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted row count**

# CELL ********************

persisted_fact_invoice_df = (
    spark.table(
        FACT_INVOICE_TABLE
    )
)


persisted_invoice_item_count = (
    persisted_fact_invoice_df.count()
)


print(
    "Expected rows:",
    f"{source_invoice_item_count:,}"
)

print(
    "Persisted rows:",
    f"{persisted_invoice_item_count:,}"
)


assert (
    persisted_invoice_item_count
    ==
    source_invoice_item_count
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

# **Three-way-match overview**

# CELL ********************

display(
    persisted_fact_invoice_df

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

    .orderBy(
        F.desc(
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

# **Annual invoice KPIs**

# CELL ********************

#Calculate item-level metrics
annual_invoice_item_kpi_df = (
    persisted_fact_invoice_df

    .join(
        dim_date_df

        .select(
            F.col(
                "DateKey"
            ).alias(
                "RefInvoiceDateKey"
            ),

            "Year"
        ),

        F.col(
            "InvoiceDateKey"
        )
        ==
        F.col(
            "RefInvoiceDateKey"
        ),

        "left"
    )

    .groupBy(
        "Year"
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

# **Annual invoice-level dispute KPIs**
# 
# DisputeFlag originates at invoice-header level, so we it should not be summed accross invoice lines

# CELL ********************

invoice_level_df = (
    persisted_fact_invoice_df

    .select(
        "InvoiceID",
        "InvoiceDateKey",
        "DisputeFlag",
        "DerivedDuplicateInvoiceFlag"
    )

    .groupBy(
        "InvoiceID",
        "InvoiceDateKey"
    )

    .agg(
        F.max(
            F.col(
                "DisputeFlag"
            ).cast("int")
        ).alias(
            "InvoiceDisputeFlag"
        ),

        F.max(
            F.col(
                "DerivedDuplicateInvoiceFlag"
            ).cast("int")
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

# CELL ********************

annual_invoice_header_kpi_df = (
    invoice_level_df

    .join(
        dim_date_df

        .select(
            F.col(
                "DateKey"
            ).alias(
                "RefInvoiceDateKey"
            ),

            "Year"
        ),

        F.col(
            "InvoiceDateKey"
        )
        ==
        F.col(
            "RefInvoiceDateKey"
        ),

        "left"
    )

    .groupBy(
        "Year"
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

# **Combine annual Invoice KPIs**

# CELL ********************

annual_invoice_kpi_df = (
    annual_invoice_item_kpi_df

    .join(
        annual_invoice_header_kpi_df,
        "Year",
        "left"
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
                    *
                    F.lit(100.0)
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
                    F.lit(100.0)
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
                    *
                    F.lit(100.0)
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
                    F.lit(100.0)
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
        "Year"
    )
)


display(
    annual_invoice_kpi_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Supplier invoice exception preview**

# CELL ********************

supplier_invoice_exception_df = (
    persisted_fact_invoice_df

    .groupBy(
        "SupplierKey"
    )

    .agg(
        F.count(
            "InvoiceItemID"
        ).alias(
            "InvoiceItemCount"
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
                    F.lit(100.0)
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
        F.desc(
            "InvoiceExceptionAmountEUR"
        )
    )
)


display(
    supplier_invoice_exception_df

    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Price and quantity exception preview**

# CELL ********************

display(
    persisted_fact_invoice_df

    .filter(
        F.col(
            "InvoiceExceptionFlag"
        )
    )

    .select(
        "InvoiceID",
        "InvoiceItemID",

        "POID",
        "POItemID",

        "SupplierKey",

        "InvoiceDate",

        "DerivedThreeWayMatchStatus",

        "InvoicedQuantity",
        "CalculatedReceivedQuantityAtInvoiceDate",

        "POUnitPrice",
        "InvoiceUnitPrice",

        "CalculatedPriceVariancePercentage",
        "CalculatedQuantityVariancePercentage",

        "GrossAmountEUR",
        "InvoiceExceptionAmountEUR",

        "DisputeFlag",
        "DerivedDuplicateInvoiceFlag"
    )

    .orderBy(
        F.desc(
            "InvoiceExceptionAmountEUR"
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
    persisted_fact_invoice_df

    .select(
        "InvoiceFactKey",

        "InvoiceID",
        "InvoiceNumber",
        "InvoiceItemID",
        "InvoiceLineNumber",

        "POID",
        "POItemID",
        "PurchaseOrderFactKey",

        "InvoiceDate",
        "InvoiceDateKey",

        "PostingDate",
        "PostingDateKey",

        "DueDate",
        "DueDateKey",

        "SupplierKey",
        "SupplierDimensionVersion",

        "CategoryKey",
        "MaterialKey",
        "BuyerKey",
        "BusinessUnitKey",
        "ContractKey",

        "CurrencyCode",
        "CurrencyKey",

        "InvoicedQuantity",
        "UnitOfMeasure",

        "POUnitPrice",
        "InvoiceUnitPrice",

        "NetAmount",
        "TaxAmount",
        "GrossAmount",

        "InvoiceUnitPriceEUR",
        "NetAmountEUR",
        "TaxAmountEUR",
        "GrossAmountEUR",

        "CalculatedReceivedQuantityAtInvoiceDate",

        "CalculatedPriceVariancePercentage",
        "CalculatedQuantityVariancePercentage",

        "DerivedThreeWayMatchStatus",
        "ThreeWayMatchFlag",

        "InvoiceExceptionFlag",
        "InvoiceExceptionAmountEUR",

        "DisputeFlag",
        "DisputeReason",

        "DerivedDuplicateInvoiceFlag"
    )

    .orderBy(
        "InvoiceDate",
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
    "NB_32_Build_Gold_Fact_Invoices "
    "completed successfully."
)

print()

print(
    "Physical Gold output:"
)

print(
    "  fact_invoice"
)

print()

print(
    "Grain:"
)

print(
    "  One row per InvoiceItemID"
)

print()

print(
    "Invoice-specific dimension keys:"
)

print(
    "  - InvoiceDateKey"
)

print(
    "  - PostingDateKey"
)

print(
    "  - DueDateKey"
)

print(
    "  - SupplierKey "
    "(SCD Type 2 by InvoiceDate)"
)

print(
    "  - CurrencyKey"
)

print()

print(
    "PO-derived conformed keys:"
)

print(
    "  - PurchaseOrderFactKey"
)

print(
    "  - OrderDateKey"
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

print()

print(
    "Primary invoice measures:"
)

print(
    "  - GrossAmountEUR"
)

print(
    "  - MatchedInvoiceAmountEUR"
)

print(
    "  - InvoiceExceptionAmountEUR"
)

print(
    "  - ThreeWayMatchFlag"
)

print(
    "  - InvoiceExceptionFlag"
)

print(
    "  - DisputeFlag"
)

print(
    "  - DerivedDuplicateInvoiceFlag"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
