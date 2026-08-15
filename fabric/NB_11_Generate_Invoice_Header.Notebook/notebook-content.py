# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6900e8cc-2b9a-400f-9c08-f940b37aed8e",
# META       "default_lakehouse_name": "lh_procurement_bronze",
# META       "default_lakehouse_workspace_id": "83e05aab-2eed-49cb-a339-674db19d4b92",
# META       "known_lakehouses": [
# META         {
# META           "id": "6900e8cc-2b9a-400f-9c08-f940b37aed8e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# The notebook will create:

#One primary invoice for most eligible POs
#Approximately 1% duplicate invoices
#Approximately 4% total disputed invoices
#Realistic invoice, posting, and due dates
#Placeholder invoice totals until bronze_invoice_item is generated


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Configuration
# ============================================================

from datetime import date

PROFILE = "development"

AS_OF_DATE = date(2026, 7, 31)

RANDOM_SEED = 20260802


# ------------------------------------------------------------
# Duplicate invoices
# ------------------------------------------------------------

DUPLICATE_INVOICE_RATE = 0.01


# ------------------------------------------------------------
# Supplier-risk-driven primary invoice disputes
#
# Approximate primary dispute behavior:
#
# Low risk      ~ 1-2%
# Moderate risk ~ 2-4%
# High risk     ~ 5-9%
#
# Duplicates remain disputes separately.
# ------------------------------------------------------------

DISPUTE_BASE_RATE = 0.006

DISPUTE_RISK_SLOPE = 0.085

MAX_PRIMARY_DISPUTE_RATE = 0.10


# ------------------------------------------------------------
# Must match NB_10 supplier-risk evolution
# ------------------------------------------------------------

RISK_START_YEAR = 2022

DETERIORATING_SUPPLIER_RATE = 0.10

IMPROVING_SUPPLIER_RATE = 0.05

DETERIORATING_ANNUAL_SLOPE = 0.035

IMPROVING_ANNUAL_SLOPE = -0.025


print(
    f"Profile: {PROFILE}"
)

print(
    f"As-of date: {AS_OF_DATE}"
)

print(
    f"Duplicate invoice target: "
    f"{DUPLICATE_INVOICE_RATE:.1%}"
)

print(
    "Primary invoice disputes: "
    "supplier-risk driven"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Imports
# ============================================================

import hashlib
import random

from collections import defaultdict

from datetime import timedelta

from decimal import Decimal


from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType,
    BooleanType,
    IntegerType
)


random.seed(
    RANDOM_SEED
)


print(
    "Libraries loaded."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Read Parent Tables
# ============================================================

po_header_reference_df = (
    spark.table(
        "bronze_purchase_order_header"
    )

    .select(
        "POID",
        "SupplierID",
        "BusinessUnitID",
        "OrderDate",
        "Currency",
        "POStatus",
        "TotalAmount"
    )
)


goods_receipt_reference_df = (
    spark.table(
        "bronze_goods_receipt"
    )

    .select(
        "GoodsReceiptID",
        "POID",
        "ReceiptDate"
    )
)


supplier_reference_df = (
    spark.table(
        "bronze_supplier"
    )

    .select(
        "SupplierID",
        "ESGRating",
        "FinancialRiskScore",
        "Status"
    )
)


print(
    "PO headers:",
    po_header_reference_df.count()
)

print(
    "Goods receipts:",
    goods_receipt_reference_df.count()
)

print(
    "Suppliers:",
    supplier_reference_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Aggregate receipt dates by PO
po_receipt_summary_df = (
    goods_receipt_reference_df
    .groupBy("POID")
    .agg(
        F.min("ReceiptDate").alias(
            "FirstReceiptDate"
        ),
        F.max("ReceiptDate").alias(
            "LatestReceiptDate"
        ),
        F.countDistinct(
            "GoodsReceiptID"
        ).alias(
            "GoodsReceiptCount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Build Invoice Source Population
# ============================================================

invoice_source_df = (
    po_header_reference_df.alias(
        "po"
    )

    .join(
        po_receipt_summary_df.alias(
            "receipt"
        ),

        F.col("po.POID")
        == F.col("receipt.POID"),

        "left"
    )

    .join(
        supplier_reference_df.alias(
            "supplier"
        ),

        F.col("po.SupplierID")
        == F.col("supplier.SupplierID"),

        "left"
    )

    .select(
        F.col(
            "po.POID"
        ),

        F.col(
            "po.SupplierID"
        ),

        F.col(
            "po.BusinessUnitID"
        ),

        F.col(
            "po.OrderDate"
        ),

        F.col(
            "po.Currency"
        ),

        F.col(
            "po.POStatus"
        ),

        F.col(
            "po.TotalAmount"
        ).alias(
            "POAmount"
        ),

        F.col(
            "receipt.FirstReceiptDate"
        ),

        F.col(
            "receipt.LatestReceiptDate"
        ),

        F.coalesce(
            F.col(
                "receipt.GoodsReceiptCount"
            ),
            F.lit(0)
        ).alias(
            "GoodsReceiptCount"
        ),

        F.col(
            "supplier.ESGRating"
        ),

        F.col(
            "supplier.FinancialRiskScore"
        ),

        F.col(
            "supplier.Status"
        ).alias(
            "SupplierStatus"
        )
    )
)


source_po_count = (
    invoice_source_df.count()
)


assert (
    source_po_count
    == 20_000
), (
    f"Expected 20,000 POs "
    f"but found "
    f"{source_po_count:,}."
)


missing_supplier_risk_count = (
    invoice_source_df

    .filter(
        F.col(
            "FinancialRiskScore"
        ).isNull()
    )

    .count()
)


assert (
    missing_supplier_risk_count
    == 0
), (
    f"{missing_supplier_risk_count:,} "
    f"invoice-source POs have no "
    f"supplier risk score."
)


print(
    f"Invoice source prepared: "
    f"{source_po_count:,} POs."
)

print(
    "Supplier risk attributes joined."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate SOurce Totals
unreconciled_po_count = (
    invoice_source_df
    .filter(
        F.col("POAmount") <= 0
    )
    .count()
)

assert unreconciled_po_count == 0, (
    f"Found {unreconciled_po_count} POs "
    "without valid totals."
)

print("PO totals are ready for invoicing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Collect Source Records
invoice_source_records = [
    row.asDict()
    for row in (
        invoice_source_df
        .orderBy("POID")
        .collect()
    )
]

print(
    f"Collected "
    f"{len(invoice_source_records):,} "
    f"PO records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Determine Invoice Eligibility
def should_generate_invoice(
    po_status,
    goods_receipt_count
):
    if po_status == "Cancelled":
        return False

    if po_status == "Closed":
        probability = 0.98

    elif po_status == "Fully Received":
        probability = 0.95

    elif po_status == "Partially Received":
        probability = 0.75

    elif po_status == "Open":
        if goods_receipt_count > 0:
            probability = 0.45
        else:
            probability = 0.15

    else:
        probability = 0.50

    return random.random() < probability

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate Invoice Dates
def generate_invoice_date(
    order_date,
    latest_receipt_date
):
    if latest_receipt_date is not None:
        # Most invoices arrive after goods receipt.
        if random.random() < 0.90:
            invoice_date = (
                latest_receipt_date
                + timedelta(
                    days=random.randint(0, 20)
                )
            )

        else:
            # Small early-invoice scenario.
            available_days = max(
                (
                    latest_receipt_date
                    - order_date
                ).days,
                0
            )

            invoice_date = (
                order_date
                + timedelta(
                    days=random.randint(
                        0,
                        available_days
                    )
                )
            )

    else:
        invoice_date = (
            order_date
            + timedelta(
                days=random.randint(3, 45)
            )
        )

    return min(
        invoice_date,
        AS_OF_DATE
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Posting and Due dates
def generate_posting_date(
    invoice_date
):
    posting_date = (
        invoice_date
        + timedelta(
            days=random.randint(0, 5)
        )
    )

    return min(
        posting_date,
        AS_OF_DATE
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select Payment Terms days
def select_payment_terms_days():
    return random.choices(
        population=[
            30,
            45,
            60,
            90
        ],
        weights=[
            55,
            20,
            20,
            5
        ],
        k=1
    )[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Supplier Risk and Dispute Logic
# ============================================================

DISPUTE_REASONS = [
    "Price variance",
    "Quantity variance",
    "Missing goods receipt",
    "Incorrect tax treatment",
    "Incorrect supplier reference",
    "Missing purchase order",
    "Payment terms disagreement"
]


ESG_RISK_MAPPING = {
    "A": 0.05,
    "B": 0.20,
    "C": 0.40,
    "D": 0.65,
    "E": 0.85
}


def stable_uniform_value(
    *parts
):
    text_value = "||".join(
        ""
        if part is None
        else str(part)
        for part in parts
    )


    digest = hashlib.sha256(
        text_value.encode(
            "utf-8"
        )
    ).digest()


    integer_value = int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False
    )


    return (
        integer_value
        / float(
            (1 << 64) - 1
        )
    )


def derive_supplier_risk_propensity(
    supplier_id,
    financial_risk_score,
    esg_rating,
    supplier_status,
    event_year
):

    financial_risk = (
        float(
            financial_risk_score
        )
        / 100.0
        if financial_risk_score
        is not None
        else 0.50
    )


    financial_risk = min(
        max(
            financial_risk,
            0.0
        ),
        1.0
    )


    normalized_esg = (
        str(
            esg_rating
        )
        .strip()
        .upper()
        if esg_rating
        is not None
        else None
    )


    esg_risk = (
        ESG_RISK_MAPPING.get(
            normalized_esg,
            0.45
        )
    )


    normalized_status = (
        str(
            supplier_status
        ).strip()
        if supplier_status
        is not None
        else ""
    )


    status_adjustment = {
        "Active": 0.00,
        "Inactive": 0.04,
        "Blocked": 0.12
    }.get(
        normalized_status,
        0.06
    )


    supplier_component = (
        stable_uniform_value(
            supplier_id,
            "supplier_base_risk",
            RANDOM_SEED
        )
    )


    trajectory_selector = (
        stable_uniform_value(
            supplier_id,
            "risk_trajectory",
            RANDOM_SEED
        )
    )


    if (
        trajectory_selector
        < DETERIORATING_SUPPLIER_RATE
    ):
        annual_slope = (
            DETERIORATING_ANNUAL_SLOPE
        )

    elif (
        trajectory_selector
        <
        DETERIORATING_SUPPLIER_RATE
        + IMPROVING_SUPPLIER_RATE
    ):
        annual_slope = (
            IMPROVING_ANNUAL_SLOPE
        )

    else:
        annual_slope = 0.0


    year_offset = max(
        int(event_year)
        - RISK_START_YEAR,
        0
    )


    annual_shock = (
        (
            stable_uniform_value(
                supplier_id,
                event_year,
                "annual_risk_shock",
                RANDOM_SEED
            )
            - 0.50
        )
        * 0.08
    )


    risk_propensity = (
        0.50
        * financial_risk

        + 0.28
        * esg_risk

        + 0.10
        * supplier_component

        + status_adjustment

        + (
            annual_slope
            * year_offset
        )

        + annual_shock
    )


    return min(
        max(
            risk_propensity,
            0.0
        ),
        1.0
    )


def derive_invoice_dispute_probability(
    supplier_risk_propensity
):

    probability = (
        DISPUTE_BASE_RATE
        +
        DISPUTE_RISK_SLOPE
        * supplier_risk_propensity
    )


    return min(
        max(
            probability,
            DISPUTE_BASE_RATE
        ),
        MAX_PRIMARY_DISPUTE_RATE
    )


def classify_supplier_risk_band(
    supplier_risk_propensity
):
    if supplier_risk_propensity < 0.25:
        return "LOW"

    if supplier_risk_propensity < 0.50:
        return "MODERATE"

    return "HIGH"


print(
    "Supplier-risk-driven "
    "invoice dispute logic configured."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def derive_invoice_status(
    dispute_flag
):
    if dispute_flag:
        return "Blocked"

    return random.choices(
        population=[
            "Posted",
            "Parked"
        ],
        weights=[
            97,
            3
        ],
        k=1
    )[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def derive_payment_status(
    invoice_status,
    due_date
):
    if invoice_status == "Blocked":
        return "Blocked"

    if invoice_status == "Parked":
        return "Pending"

    if due_date < AS_OF_DATE:
        return random.choices(
            population=[
                "Paid",
                "Overdue"
            ],
            weights=[
                90,
                10
            ],
            k=1
        )[0]

    return random.choices(
        population=[
            "Paid",
            "Pending"
        ],
        weights=[
            20,
            80
        ],
        k=1
    )[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Generate Primary Invoice Headers
# ============================================================

primary_invoice_rows = []

invoice_risk_diagnostic_rows = []

supplier_invoice_sequence = (
    defaultdict(int)
)


for source_record in invoice_source_records:

    if not should_generate_invoice(
        po_status=(
            source_record[
                "POStatus"
            ]
        ),

        goods_receipt_count=(
            source_record[
                "GoodsReceiptCount"
            ]
        )
    ):
        continue


    supplier_id = (
        source_record[
            "SupplierID"
        ]
    )


    supplier_invoice_sequence[
        supplier_id
    ] += 1


    invoice_date = (
        generate_invoice_date(
            order_date=(
                source_record[
                    "OrderDate"
                ]
            ),

            latest_receipt_date=(
                source_record[
                    "LatestReceiptDate"
                ]
            )
        )
    )


    posting_date = (
        generate_posting_date(
            invoice_date
        )
    )


    payment_terms_days = (
        select_payment_terms_days()
    )


    due_date = (
        posting_date
        + timedelta(
            days=payment_terms_days
        )
    )


    # ========================================================
    # Supplier-risk-driven invoice dispute
    # ========================================================

    event_year = (
        invoice_date.year
    )


    supplier_risk_propensity = (
        derive_supplier_risk_propensity(
            supplier_id=(
                supplier_id
            ),

            financial_risk_score=(
                source_record[
                    "FinancialRiskScore"
                ]
            ),

            esg_rating=(
                source_record[
                    "ESGRating"
                ]
            ),

            supplier_status=(
                source_record[
                    "SupplierStatus"
                ]
            ),

            event_year=(
                event_year
            )
        )
    )


    dispute_probability = (
        derive_invoice_dispute_probability(
            supplier_risk_propensity
        )
    )


    # Preserve stochastic behavior.
    dispute_flag = (
        random.random()
        < dispute_probability
    )


    invoice_risk_diagnostic_rows.append(
        {
            "POID":
                source_record[
                    "POID"
                ],

            "SupplierID":
                supplier_id,

            "EventYear":
                event_year,

            "SupplierRiskPropensity":
                float(
                    supplier_risk_propensity
                ),

            "SupplierRiskBand":
                classify_supplier_risk_band(
                    supplier_risk_propensity
                ),

            "DisputeProbability":
                float(
                    dispute_probability
                ),

            "DisputeFlag":
                bool(
                    dispute_flag
                )
        }
    )


    # ========================================================
    # Existing invoice logic
    # ========================================================

    invoice_status = (
        derive_invoice_status(
            dispute_flag
        )
    )


    payment_status = (
        derive_payment_status(
            invoice_status,
            due_date
        )
    )


    invoice_sequence = (
        supplier_invoice_sequence[
            supplier_id
        ]
    )


    invoice_id = (
        f"INV"
        f"{len(primary_invoice_rows) + 1:010d}"
    )


    invoice_number = (
        f"{supplier_id}-"
        f"{invoice_date.year}-"
        f"{invoice_sequence:06d}"
    )


    primary_invoice_rows.append(
        {
            "InvoiceID":
                invoice_id,

            "InvoiceNumber":
                invoice_number,

            "POID":
                source_record[
                    "POID"
                ],

            "SupplierID":
                supplier_id,

            "BusinessUnitID":
                source_record[
                    "BusinessUnitID"
                ],

            "InvoiceDate":
                invoice_date,

            "PostingDate":
                posting_date,

            "DueDate":
                due_date,

            "Currency":
                source_record[
                    "Currency"
                ],

            "TotalInvoiceAmount":
                Decimal(
                    "0.00"
                ),

            "InvoiceStatus":
                invoice_status,

            "PaymentStatus":
                payment_status,

            "PaymentTermsDays":
                payment_terms_days,

            "DisputeFlag":
                dispute_flag,

            "DisputeReason":
                (
                    random.choice(
                        DISPUTE_REASONS
                    )
                    if dispute_flag
                    else None
                ),

            "DuplicateInvoiceFlag":
                False,

            "OriginalInvoiceID":
                None,

            "AmountReconciliationStatus":
                (
                    "PENDING_"
                    "INVOICE_ITEM_GENERATION"
                )
        }
    )


print(
    f"Prepared "
    f"{len(primary_invoice_rows):,} "
    f"primary invoice headers."
)

print(
    f"Prepared "
    f"{len(invoice_risk_diagnostic_rows):,} "
    f"invoice-risk diagnostic records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate Duplicate Invoices
#Approximately 1% of primary invoices will be duplicated.
duplicate_invoice_count = round(
    len(primary_invoice_rows)
    * DUPLICATE_INVOICE_RATE
)

duplicate_source_rows = random.sample(
    primary_invoice_rows,
    duplicate_invoice_count
)

duplicate_invoice_rows = []

for source_invoice in duplicate_source_rows:
    duplicate_invoice_id = (
        f"INV"
        f"{len(primary_invoice_rows) + len(duplicate_invoice_rows) + 1:010d}"
    )

    duplicate_posting_date = min(
        source_invoice["PostingDate"]
        + timedelta(
            days=random.randint(1, 7)
        ),
        AS_OF_DATE
    )

    duplicate_invoice_rows.append(
        {
            "InvoiceID": duplicate_invoice_id,
            "InvoiceNumber": (
                source_invoice["InvoiceNumber"]
            ),
            "POID": source_invoice["POID"],
            "SupplierID": (
                source_invoice["SupplierID"]
            ),
            "BusinessUnitID": (
                source_invoice["BusinessUnitID"]
            ),
            "InvoiceDate": (
                source_invoice["InvoiceDate"]
            ),
            "PostingDate": (
                duplicate_posting_date
            ),
            "DueDate": (
                duplicate_posting_date
                + timedelta(
                    days=source_invoice[
                        "PaymentTermsDays"
                    ]
                )
            ),
            "Currency": (
                source_invoice["Currency"]
            ),
            "TotalInvoiceAmount": (
                Decimal("0.00")
            ),
            "InvoiceStatus": "Blocked",
            "PaymentStatus": "Blocked",
            "PaymentTermsDays": (
                source_invoice[
                    "PaymentTermsDays"
                ]
            ),
            "DisputeFlag": True,
            "DisputeReason": (
                "Potential duplicate invoice"
            ),
            "DuplicateInvoiceFlag": True,
            "OriginalInvoiceID": (
                source_invoice["InvoiceID"]
            ),
            "AmountReconciliationStatus": (
                "PENDING_INVOICE_ITEM_GENERATION"
            )
        }
    )

print(
    f"Prepared "
    f"{len(duplicate_invoice_rows):,} "
    f"duplicate invoices."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Combine Invoice Rows
invoice_header_rows = (
    primary_invoice_rows
    + duplicate_invoice_rows
)

print(
    f"Total invoice headers prepared: "
    f"{len(invoice_header_rows):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create a Spark DataFrame
invoice_header_schema = StructType([
    StructField(
        "InvoiceID",
        StringType(),
        False
    ),
    StructField(
        "InvoiceNumber",
        StringType(),
        False
    ),
    StructField(
        "POID",
        StringType(),
        False
    ),
    StructField(
        "SupplierID",
        StringType(),
        False
    ),
    StructField(
        "BusinessUnitID",
        StringType(),
        False
    ),
    StructField(
        "InvoiceDate",
        DateType(),
        False
    ),
    StructField(
        "PostingDate",
        DateType(),
        False
    ),
    StructField(
        "DueDate",
        DateType(),
        False
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "TotalInvoiceAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "InvoiceStatus",
        StringType(),
        False
    ),
    StructField(
        "PaymentStatus",
        StringType(),
        False
    ),
    StructField(
        "PaymentTermsDays",
        IntegerType(),
        False
    ),
    StructField(
        "DisputeFlag",
        BooleanType(),
        False
    ),
    StructField(
        "DisputeReason",
        StringType(),
        True
    ),
    StructField(
        "DuplicateInvoiceFlag",
        BooleanType(),
        False
    ),
    StructField(
        "OriginalInvoiceID",
        StringType(),
        True
    ),
    StructField(
        "AmountReconciliationStatus",
        StringType(),
        False
    )
])

invoice_header_df = spark.createDataFrame(
    invoice_header_rows,
    schema=invoice_header_schema
)

display(
    invoice_header_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add audit COlumns and Record Hash
invoice_header_df = (
    invoice_header_df
    .withColumn(
        "SourceSystem",
        F.lit("SYNTHETIC_SAP")
    )
    .withColumn(
        "IngestionTimestamp",
        F.current_timestamp()
    )
    .withColumn(
        "LoadDate",
        F.current_date()
    )
    .withColumn(
        "SourceRecordHash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("InvoiceID"),
                F.col("InvoiceNumber"),
                F.col("POID"),
                F.col("SupplierID"),
                F.col("InvoiceDate").cast(
                    "string"
                ),
                F.col("PostingDate").cast(
                    "string"
                ),
                F.col("Currency"),
                F.col("InvoiceStatus"),
                F.col(
                    "DuplicateInvoiceFlag"
                ).cast("string")
            ),
            256
        )
    )
)

display(
    invoice_header_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Structural Validation
actual_invoice_count = (
    invoice_header_df.count()
)

duplicate_invoice_id_count = (
    invoice_header_df
    .groupBy("InvoiceID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    invoice_header_df
    .filter(
        F.col("InvoiceID").isNull()
        | F.col("InvoiceNumber").isNull()
        | F.col("POID").isNull()
        | F.col("SupplierID").isNull()
        | F.col("BusinessUnitID").isNull()
        | F.col("InvoiceDate").isNull()
        | F.col("PostingDate").isNull()
        | F.col("DueDate").isNull()
        | F.col("Currency").isNull()
        | F.col("InvoiceStatus").isNull()
        | F.col("PaymentStatus").isNull()
    )
    .count()
)

invalid_date_count = (
    invoice_header_df
    .filter(
        (F.col("InvoiceDate")
         > F.col("PostingDate"))
        |
        (F.col("PostingDate")
         > F.lit(
             AS_OF_DATE.isoformat()
         ).cast("date"))
        |
        (F.col("DueDate")
         < F.col("PostingDate"))
    )
    .count()
)

assert actual_invoice_count > 0
assert duplicate_invoice_id_count == 0
assert mandatory_null_count == 0
assert invalid_date_count == 0

print("Invoice-header structural validation passed.")
print(f"Rows: {actual_invoice_count:,}")
print(
    f"Duplicate InvoiceIDs: "
    f"{duplicate_invoice_id_count}"
)
print(
    f"Mandatory nulls: "
    f"{mandatory_null_count}"
)
print(
    f"Invalid dates: "
    f"{invalid_date_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Foreign Key Validation
invalid_po_count = (
    invoice_header_df.alias("invoice")
    .join(
        po_header_reference_df.alias("po"),
        F.col("invoice.POID")
        == F.col("po.POID"),
        "left_anti"
    )
    .count()
)

invalid_supplier_count = (
    invoice_header_df.alias("invoice")
    .join(
        supplier_reference_df.alias(
            "supplier"
        ),
        F.col("invoice.SupplierID")
        == F.col("supplier.SupplierID"),
        "left_anti"
    )
    .count()
)

assert invalid_po_count == 0
assert invalid_supplier_count == 0

print("Invoice-header foreign-key validation passed.")
print(f"Invalid POs: {invalid_po_count}")
print(
    f"Invalid suppliers: "
    f"{invalid_supplier_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate PO Consistency
po_consistency_error_count = (
    invoice_header_df.alias("invoice")
    .join(
        po_header_reference_df.alias("po"),
        F.col("invoice.POID")
        == F.col("po.POID"),
        "inner"
    )
    .filter(
        (F.col("invoice.SupplierID")
         != F.col("po.SupplierID"))
        |
        (F.col("invoice.BusinessUnitID")
         != F.col("po.BusinessUnitID"))
        |
        (F.col("invoice.Currency")
         != F.col("po.Currency"))
    )
    .count()
)

assert po_consistency_error_count == 0

print(
    "Invoice-to-PO consistency "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Dispute Fields
dispute_reason_error_count = (
    invoice_header_df
    .filter(
        (
            F.col("DisputeFlag")
            & F.col(
                "DisputeReason"
            ).isNull()
        )
        |
        (
            ~F.col("DisputeFlag")
            & F.col(
                "DisputeReason"
            ).isNotNull()
        )
    )
    .count()
)

assert dispute_reason_error_count == 0

print(
    "Invoice dispute-field "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Duplicate Invoices
duplicate_validation_df = (
    invoice_header_df.alias("duplicate")
    .filter(
        F.col(
            "duplicate.DuplicateInvoiceFlag"
        )
    )
    .join(
        invoice_header_df.alias("original"),
        F.col(
            "duplicate.OriginalInvoiceID"
        )
        == F.col("original.InvoiceID"),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_duplicate_count = (
    duplicate_validation_df
    .filter(
        F.col("original.InvoiceID").isNull()
        |
        (
            F.col(
                "duplicate.InvoiceNumber"
            )
            != F.col(
                "original.InvoiceNumber"
            )
        )
        |
        (
            F.col("duplicate.SupplierID")
            != F.col("original.SupplierID")
        )
        |
        (
            F.col("duplicate.POID")
            != F.col("original.POID")
        )
    )
    .count()
)

assert invalid_duplicate_count == 0

print("Duplicate-invoice validation passed.")
print(
    f"Duplicate invoices: "
    f"{duplicate_invoice_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Inspect Invoice Scenarios
# ============================================================

display(
    invoice_header_df

    .agg(
        F.count("*")
        .alias(
            "InvoiceCount"
        ),

        F.sum(
            F.col(
                "DisputeFlag"
            ).cast("int")
        )
        .alias(
            "DisputedInvoices"
        ),

        F.round(
            F.avg(
                F.col(
                    "DisputeFlag"
                ).cast("double")
            )
            * 100,
            2
        )
        .alias(
            "DisputePercentage"
        ),

        F.sum(
            F.col(
                "DuplicateInvoiceFlag"
            ).cast("int")
        )
        .alias(
            "DuplicateInvoices"
        ),

        F.round(
            F.avg(
                F.col(
                    "DuplicateInvoiceFlag"
                ).cast("double")
            )
            * 100,
            2
        )
        .alias(
            "DuplicatePercentage"
        )
    )
)


# ============================================================
# Validate Supplier Risk -> Invoice Dispute Relationship
# Primary invoices only
# ============================================================

invoice_risk_diagnostic_df = (
    spark.createDataFrame(
        invoice_risk_diagnostic_rows
    )
)


invoice_risk_band_summary_df = (
    invoice_risk_diagnostic_df

    .groupBy(
        "SupplierRiskBand"
    )

    .agg(
        F.count("*")
        .alias(
            "InvoiceCount"
        ),

        F.round(
            F.avg(
                "SupplierRiskPropensity"
            ),
            4
        )
        .alias(
            "AverageRiskPropensity"
        ),

        F.round(
            F.avg(
                "DisputeProbability"
            )
            * 100,
            2
        )
        .alias(
            "AveragePlannedDisputePct"
        ),

        F.round(
            F.avg(
                F.col(
                    "DisputeFlag"
                )
                .cast("double")
            )
            * 100,
            2
        )
        .alias(
            "ActualDisputePct"
        )
    )

    .orderBy(
        "AverageRiskPropensity"
    )
)


display(
    invoice_risk_band_summary_df
)


print(
    "Expected relationship:"
)

print(
    "LOW dispute rate "
    "< MODERATE dispute rate "
    "< HIGH dispute rate"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    invoice_header_df
    .groupBy(
        "InvoiceStatus",
        "PaymentStatus"
    )
    .agg(
        F.count("*").alias(
            "InvoiceCount"
        )
    )
    .orderBy(
        "InvoiceStatus",
        "PaymentStatus"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to the lakehouse
TARGET_INVOICE_HEADER_TABLE = (
    "bronze_invoice_header"
)

(
    invoice_header_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_INVOICE_HEADER_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_INVOICE_HEADER_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read back & validate
saved_invoice_header_df = (
    spark.table(
        "bronze_invoice_header"
    )
)

persisted_invoice_count = (
    saved_invoice_header_df.count()
)

persisted_distinct_invoice_count = (
    saved_invoice_header_df
    .select("InvoiceID")
    .distinct()
    .count()
)

assert (
    persisted_invoice_count
    == actual_invoice_count
)

assert (
    persisted_distinct_invoice_count
    == actual_invoice_count
)

display(
    saved_invoice_header_df.limit(50)
)

print(
    f"Persisted invoice-header rows: "
    f"{persisted_invoice_count:,}"
)

print(
    "Persisted invoice-header "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
