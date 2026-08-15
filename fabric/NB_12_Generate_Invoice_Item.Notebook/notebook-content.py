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

# Configuration
PROFILE = "development"

RANDOM_SEED = 20260802

PRICE_TOLERANCE_PERCENT = 3.0
QUANTITY_TOLERANCE_PERCENT = 2.0

print(f"Profile: {PROFILE}")
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

# CELL ********************

#Imports
import random
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    BooleanType
)

random.seed(RANDOM_SEED)

MONEY_PRECISION = Decimal("0.01")
QUANTITY_PRECISION = Decimal("0.001")
PERCENT_PRECISION = Decimal("0.0001")

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read Parent tables
invoice_header_reference_df = (
    spark.table("bronze_invoice_header")
    .select(
        "InvoiceID",
        "InvoiceNumber",
        "POID",
        "SupplierID",
        "InvoiceDate",
        "Currency",
        "DisputeFlag",
        "DisputeReason",
        "DuplicateInvoiceFlag",
        "OriginalInvoiceID"
    )
)

po_item_reference_df = (
    spark.table("bronze_purchase_order_item")
    .select(
        "POItemID",
        "POID",
        "POLineNumber",
        "MaterialID",
        "CategoryID",
        "ContractID",
        "Quantity",
        "OrderUnit",
        "UnitPrice",
        "LineAmount",
        "Currency"
    )
)

goods_receipt_reference_df = (
    spark.table("bronze_goods_receipt")
    .select(
        "GoodsReceiptID",
        "POItemID",
        "ReceiptDate",
        "QuantityReceived"
    )
)

print(
    "Invoice headers:",
    invoice_header_reference_df.count()
)

print(
    "PO items:",
    po_item_reference_df.count()
)

print(
    "Goods receipts:",
    goods_receipt_reference_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate the parent tables

invoice_header_count = (
    invoice_header_reference_df.count()
)

po_item_count = (
    po_item_reference_df.count()
)

goods_receipt_count = (
    goods_receipt_reference_df.count()
)

assert invoice_header_count > 0
assert po_item_count > 0
assert goods_receipt_count > 0

unreconciled_invoice_count = (
    spark.table("bronze_invoice_header")
    .filter(
        F.col("AmountReconciliationStatus")
        != "PENDING_INVOICE_ITEM_GENERATION"
    )
    .count()
)

assert unreconciled_invoice_count == 0, (
    f"Found {unreconciled_invoice_count} invoice "
    "headers with an unexpected reconciliation status."
)

print("Parent-table validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Collect Reference Records
invoice_header_records = [
    row.asDict()
    for row in (
        invoice_header_reference_df
        .orderBy("InvoiceID")
        .collect()
    )
]

po_item_records = [
    row.asDict()
    for row in (
        po_item_reference_df
        .orderBy(
            "POID",
            "POLineNumber"
        )
        .collect()
    )
]

goods_receipt_records = [
    row.asDict()
    for row in goods_receipt_reference_df.collect()
]

print(
    f"Collected {len(invoice_header_records):,} "
    "invoice headers."
)

print(
    f"Collected {len(po_item_records):,} "
    "PO items."
)

print(
    f"Collected {len(goods_receipt_records):,} "
    "goods receipts."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Build PO Item Lookups
po_items_by_po = defaultdict(list)

for po_item in po_item_records:
    po_items_by_po[
        po_item["POID"]
    ].append(po_item)

print(
    f"POs with items: "
    f"{len(po_items_by_po):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

receipts_by_po_item = defaultdict(list)

for receipt in goods_receipt_records:
    receipts_by_po_item[
        receipt["POItemID"]
    ].append(
        {
            "ReceiptDate": (
                receipt["ReceiptDate"]
            ),
            "QuantityReceived": (
                receipt["QuantityReceived"]
            )
        }
    )

for po_item_id in receipts_by_po_item:
    receipts_by_po_item[
        po_item_id
    ].sort(
        key=lambda record: record[
            "ReceiptDate"
        ]
    )

print(
    f"PO items with receipt events: "
    f"{len(receipts_by_po_item):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calculate Received quantity by invoice date
def get_received_quantity_as_of(
    po_item_id,
    invoice_date
):
    receipt_events = (
        receipts_by_po_item.get(
            po_item_id,
            []
        )
    )

    total_received = sum(
        (
            receipt["QuantityReceived"]
            for receipt in receipt_events
            if (
                receipt["ReceiptDate"]
                <= invoice_date
            )
        ),
        Decimal("0.000")
    )

    return total_received.quantize(
        QUANTITY_PRECISION,
        rounding=ROUND_HALF_UP
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Helper function
def quantize_money(value):
    return Decimal(value).quantize(
        MONEY_PRECISION,
        rounding=ROUND_HALF_UP
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def quantize_money(value):
    return Decimal(value).quantize(
        MONEY_PRECISION,
        rounding=ROUND_HALF_UP
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def quantize_percentage(value):
    return Decimal(value).quantize(
        PERCENT_PRECISION,
        rounding=ROUND_HALF_UP
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select Invoice Tax Rate
def select_tax_rate():
    selected_rate = random.choices(
        population=[
            Decimal("0.0000"),
            Decimal("0.0500"),
            Decimal("0.1000"),
            Decimal("0.2000"),
            Decimal("0.2100")
        ],
        weights=[
            8,
            4,
            8,
            30,
            50
        ],
        k=1
    )[0]

    return selected_rate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Map dispute reasons to line scenarios
def get_forced_dispute_scenario(
    dispute_reason
):
    dispute_mapping = {
        "Price variance": (
            "PRICE_VARIANCE"
        ),
        "Quantity variance": (
            "QUANTITY_VARIANCE"
        ),
        "Missing goods receipt": (
            "MISSING_GOODS_RECEIPT"
        ),
        "Incorrect tax treatment": (
            "PRICE_VARIANCE"
        ),
        "Incorrect supplier reference": (
            "PRICE_VARIANCE"
        ),
        "Missing purchase order": (
            "MISSING_GOODS_RECEIPT"
        ),
        "Payment terms disagreement": (
            "QUANTITY_VARIANCE"
        )
    }

    return dispute_mapping.get(
        dispute_reason,
        random.choice([
            "PRICE_VARIANCE",
            "QUANTITY_VARIANCE",
            "PRICE_AND_QUANTITY_VARIANCE"
        ])
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select the Issue Line
#For disputed invoices, one suitable line is deliberately assigned a material mismatch.
def select_forced_issue_line(
    line_contexts,
    forced_scenario
):
    if (
        forced_scenario
        == "MISSING_GOODS_RECEIPT"
    ):
        candidate_indexes = [
            index
            for index, context
            in enumerate(line_contexts)
            if (
                context[
                    "ReceivedQuantity"
                ]
                == Decimal("0.000")
            )
        ]

    else:
        candidate_indexes = [
            index
            for index, context
            in enumerate(line_contexts)
            if (
                context[
                    "ReceivedQuantity"
                ]
                > Decimal("0.000")
            )
        ]

    if candidate_indexes:
        return random.choice(
            candidate_indexes
        )

    return 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select Normal Line Scenarios
#Non-disputed lines can contain small differences that remain within tolerance.
def select_normal_scenario(
    received_quantity
):
    if (
        received_quantity
        <= Decimal("0.000")
    ):
        return "MISSING_GOODS_RECEIPT"

    return random.choices(
        population=[
            "MATCHED",
            "PRICE_WITHIN_TOLERANCE",
            "QUANTITY_WITHIN_TOLERANCE"
        ],
        weights=[
            90,
            5,
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

#Generate Invoice Unit Price
def generate_invoice_unit_price(
    po_unit_price,
    scenario
):
    po_unit_price = Decimal(
        po_unit_price
    )

    if scenario == "PRICE_WITHIN_TOLERANCE":
        factor = Decimal(
            str(
                random.choice([
                    random.uniform(
                        1.005,
                        1.025
                    ),
                    random.uniform(
                        0.975,
                        0.995
                    )
                ])
            )
        )

    elif scenario in [
        "PRICE_VARIANCE",
        "PRICE_AND_QUANTITY_VARIANCE"
    ]:
        factor = Decimal(
            str(
                random.choice([
                    random.uniform(
                        1.08,
                        1.30
                    ),
                    random.uniform(
                        0.70,
                        0.92
                    )
                ])
            )
        )

    else:
        factor = Decimal(
            str(
                random.uniform(
                    0.995,
                    1.005
                )
            )
        )

    generated_price = (
        po_unit_price
        * factor
    )

    return max(
        quantize_money(generated_price),
        MONEY_PRECISION
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate Invoice Quantity
def generate_invoiced_quantity(
    ordered_quantity,
    received_quantity,
    scenario
):
    ordered_quantity = Decimal(
        ordered_quantity
    )

    received_quantity = Decimal(
        received_quantity
    )

    if scenario == "MISSING_GOODS_RECEIPT":
        base_quantity = (
            ordered_quantity
            * Decimal(
                str(
                    random.uniform(
                        0.25,
                        1.00
                    )
                )
            )
        )

    elif scenario == "QUANTITY_WITHIN_TOLERANCE":
        base_quantity = (
            received_quantity
            * Decimal(
                str(
                    random.choice([
                        random.uniform(
                            1.005,
                            1.018
                        ),
                        random.uniform(
                            0.982,
                            0.995
                        )
                    ])
                )
            )
        )

    elif scenario in [
        "QUANTITY_VARIANCE",
        "PRICE_AND_QUANTITY_VARIANCE"
    ]:
        base_quantity = (
            received_quantity
            * Decimal(
                str(
                    random.choice([
                        random.uniform(
                            1.06,
                            1.25
                        ),
                        random.uniform(
                            0.70,
                            0.94
                        )
                    ])
                )
            )
        )

    else:
        base_quantity = received_quantity

    if base_quantity <= 0:
        base_quantity = min(
            ordered_quantity,
            Decimal("1.000")
        )

    return quantize_quantity(
        base_quantity
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calculate Variances
def calculate_price_variance(
    invoice_unit_price,
    po_unit_price
):
    price_variance_amount = (
        invoice_unit_price
        - po_unit_price
    )

    price_variance_percentage = (
        price_variance_amount
        / po_unit_price
        * Decimal("100")
    )

    return (
        quantize_money(
            price_variance_amount
        ),
        quantize_percentage(
            price_variance_percentage
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculate_quantity_variance(
    invoiced_quantity,
    received_quantity
):
    quantity_variance = (
        invoiced_quantity
        - received_quantity
    )

    if received_quantity > 0:
        quantity_variance_percentage = (
            quantity_variance
            / received_quantity
            * Decimal("100")
        )
    else:
        quantity_variance_percentage = (
            Decimal("100.0000")
        )

    return (
        quantize_quantity(
            quantity_variance
        ),
        quantize_percentage(
            quantity_variance_percentage
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Derive the 3-way-match result
def derive_three_way_match_status(
    duplicate_invoice_flag,
    received_quantity,
    price_variance_percentage,
    quantity_variance_percentage
):
    if duplicate_invoice_flag:
        return "DUPLICATE_INVOICE"

    if received_quantity <= 0:
        return "MISSING_GOODS_RECEIPT"

    price_outside_tolerance = (
        abs(price_variance_percentage)
        > Decimal(
            str(
                PRICE_TOLERANCE_PERCENT
            )
        )
    )

    quantity_outside_tolerance = (
        abs(quantity_variance_percentage)
        > Decimal(
            str(
                QUANTITY_TOLERANCE_PERCENT
            )
        )
    )

    if (
        price_outside_tolerance
        and quantity_outside_tolerance
    ):
        return (
            "PRICE_AND_QUANTITY_VARIANCE"
        )

    if price_outside_tolerance:
        return "PRICE_VARIANCE"

    if quantity_outside_tolerance:
        return "QUANTITY_VARIANCE"

    return "MATCHED"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Separate Primary and Duplicate Invoices
primary_invoice_headers = [
    invoice
    for invoice in invoice_header_records
    if not invoice[
        "DuplicateInvoiceFlag"
    ]
]

duplicate_invoice_headers = [
    invoice
    for invoice in invoice_header_records
    if invoice[
        "DuplicateInvoiceFlag"
    ]
]

print(
    f"Primary invoices: "
    f"{len(primary_invoice_headers):,}"
)

print(
    f"Duplicate invoices: "
    f"{len(duplicate_invoice_headers):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Repair
from decimal import Decimal, ROUND_HALF_UP

QUANTITY_PRECISION = Decimal("0.001")

def quantize_quantity(value):
    return Decimal(value).quantize(
        QUANTITY_PRECISION,
        rounding=ROUND_HALF_UP
    )

print("quantize_quantity function is available.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate Primary invoice-item rows
primary_invoice_item_rows = []

primary_items_by_invoice = defaultdict(
    list
)

invoice_item_counter = 0

for invoice_header in primary_invoice_headers:
    invoice_id = (
        invoice_header["InvoiceID"]
    )

    po_id = invoice_header["POID"]

    po_lines = po_items_by_po.get(
        po_id,
        []
    )

    if not po_lines:
        raise ValueError(
            f"No PO items found for {po_id}."
        )

    tax_rate = select_tax_rate()

    line_contexts = []

    for po_line in po_lines:
        received_quantity = (
            get_received_quantity_as_of(
                po_item_id=(
                    po_line["POItemID"]
                ),
                invoice_date=(
                    invoice_header[
                        "InvoiceDate"
                    ]
                )
            )
        )

        line_contexts.append(
            {
                "POLine": po_line,
                "ReceivedQuantity": (
                    received_quantity
                )
            }
        )

    forced_scenario = None
    forced_issue_line_index = None

    if invoice_header["DisputeFlag"]:
        forced_scenario = (
            get_forced_dispute_scenario(
                invoice_header[
                    "DisputeReason"
                ]
            )
        )

        forced_issue_line_index = (
            select_forced_issue_line(
                line_contexts,
                forced_scenario
            )
        )

    for line_index, context in enumerate(
        line_contexts
    ):
        po_line = context["POLine"]

        received_quantity = (
            context["ReceivedQuantity"]
        )

        if (
            forced_issue_line_index
            is not None
            and line_index
            == forced_issue_line_index
        ):
            scenario = forced_scenario

            if (
                scenario
                == "MISSING_GOODS_RECEIPT"
                and received_quantity > 0
            ):
                scenario = (
                    "PRICE_AND_QUANTITY_VARIANCE"
                )

        else:
            scenario = select_normal_scenario(
                received_quantity
            )

        po_unit_price = Decimal(
            po_line["UnitPrice"]
        )

        invoice_unit_price = (
            generate_invoice_unit_price(
                po_unit_price,
                scenario
            )
        )

        invoiced_quantity = (
            generate_invoiced_quantity(
                ordered_quantity=(
                    po_line["Quantity"]
                ),
                received_quantity=(
                    received_quantity
                ),
                scenario=scenario
            )
        )

        net_amount = quantize_money(
            invoiced_quantity
            * invoice_unit_price
        )

        tax_amount = quantize_money(
            net_amount
            * tax_rate
        )

        gross_amount = quantize_money(
            net_amount
            + tax_amount
        )

        (
            price_variance_amount,
            price_variance_percentage
        ) = calculate_price_variance(
            invoice_unit_price,
            po_unit_price
        )

        (
            quantity_variance,
            quantity_variance_percentage
        ) = calculate_quantity_variance(
            invoiced_quantity,
            received_quantity
        )

        three_way_match_status = (
            derive_three_way_match_status(
                duplicate_invoice_flag=False,
                received_quantity=(
                    received_quantity
                ),
                price_variance_percentage=(
                    price_variance_percentage
                ),
                quantity_variance_percentage=(
                    quantity_variance_percentage
                )
            )
        )

        invoice_item_counter += 1

        invoice_line_number = (
            line_index + 1
        ) * 10

        invoice_item_row = {
            "InvoiceItemID": (
                f"INI{invoice_item_counter:012d}"
            ),
            "InvoiceID": invoice_id,
            "InvoiceLineNumber": (
                invoice_line_number
            ),
            "POID": po_id,
            "POItemID": (
                po_line["POItemID"]
            ),
            "MaterialID": (
                po_line["MaterialID"]
            ),
            "CategoryID": (
                po_line["CategoryID"]
            ),
            "ContractID": (
                po_line["ContractID"]
            ),
            "InvoicedQuantity": (
                invoiced_quantity
            ),
            "UnitOfMeasure": (
                po_line["OrderUnit"]
            ),
            "POUnitPrice": (
                po_unit_price
            ),
            "InvoiceUnitPrice": (
                invoice_unit_price
            ),
            "NetAmount": net_amount,
            "TaxRate": tax_rate,
            "TaxAmount": tax_amount,
            "GrossAmount": gross_amount,
            "Currency": (
                invoice_header["Currency"]
            ),
            "ReceivedQuantityAtInvoiceDate": (
                received_quantity
            ),
            "PriceVarianceAmount": (
                price_variance_amount
            ),
            "PriceVariancePercentage": (
                price_variance_percentage
            ),
            "QuantityVariance": (
                quantity_variance
            ),
            "QuantityVariancePercentage": (
                quantity_variance_percentage
            ),
            "ThreeWayMatchStatus": (
                three_way_match_status
            ),
            "SimulationMatchScenario": (
                scenario
            ),
            "DuplicateInvoiceLineFlag": (
                False
            ),
            "OriginalInvoiceItemID": None
        }

        primary_invoice_item_rows.append(
            invoice_item_row
        )

        primary_items_by_invoice[
            invoice_id
        ].append(
            invoice_item_row
        )

print(
    f"Prepared "
    f"{len(primary_invoice_item_rows):,} "
    f"primary invoice items."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate Duplicate Invoice Lines
duplicate_invoice_item_rows = []

for duplicate_header in (
    duplicate_invoice_headers
):
    original_invoice_id = (
        duplicate_header[
            "OriginalInvoiceID"
        ]
    )

    original_items = (
        primary_items_by_invoice.get(
            original_invoice_id,
            []
        )
    )

    if not original_items:
        raise ValueError(
            f"No original invoice items found "
            f"for {original_invoice_id}."
        )

    for original_item in original_items:
        invoice_item_counter += 1

        duplicate_item = (
            original_item.copy()
        )

        duplicate_item.update({
            "InvoiceItemID": (
                f"INI{invoice_item_counter:012d}"
            ),
            "InvoiceID": (
                duplicate_header[
                    "InvoiceID"
                ]
            ),
            "ThreeWayMatchStatus": (
                "DUPLICATE_INVOICE"
            ),
            "SimulationMatchScenario": (
                "DUPLICATE_COPY"
            ),
            "DuplicateInvoiceLineFlag": (
                True
            ),
            "OriginalInvoiceItemID": (
                original_item[
                    "InvoiceItemID"
                ]
            )
        })

        duplicate_invoice_item_rows.append(
            duplicate_item
        )

print(
    f"Prepared "
    f"{len(duplicate_invoice_item_rows):,} "
    f"duplicate invoice items."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Combine Invoice Items
invoice_item_rows = (
    primary_invoice_item_rows
    + duplicate_invoice_item_rows
)

print(
    f"Total invoice items prepared: "
    f"{len(invoice_item_rows):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark DataFrame
invoice_item_schema = StructType([
    StructField(
        "InvoiceItemID",
        StringType(),
        False
    ),
    StructField(
        "InvoiceID",
        StringType(),
        False
    ),
    StructField(
        "InvoiceLineNumber",
        IntegerType(),
        False
    ),
    StructField(
        "POID",
        StringType(),
        False
    ),
    StructField(
        "POItemID",
        StringType(),
        False
    ),
    StructField(
        "MaterialID",
        StringType(),
        False
    ),
    StructField(
        "CategoryID",
        StringType(),
        False
    ),
    StructField(
        "ContractID",
        StringType(),
        True
    ),
    StructField(
        "InvoicedQuantity",
        DecimalType(18, 3),
        False
    ),
    StructField(
        "UnitOfMeasure",
        StringType(),
        False
    ),
    StructField(
        "POUnitPrice",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "InvoiceUnitPrice",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "NetAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "TaxRate",
        DecimalType(5, 4),
        False
    ),
    StructField(
        "TaxAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "GrossAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "ReceivedQuantityAtInvoiceDate",
        DecimalType(18, 3),
        False
    ),
    StructField(
        "PriceVarianceAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "PriceVariancePercentage",
        DecimalType(9, 4),
        False
    ),
    StructField(
        "QuantityVariance",
        DecimalType(18, 3),
        False
    ),
    StructField(
        "QuantityVariancePercentage",
        DecimalType(9, 4),
        False
    ),
    StructField(
        "ThreeWayMatchStatus",
        StringType(),
        False
    ),
    StructField(
        "SimulationMatchScenario",
        StringType(),
        False
    ),
    StructField(
        "DuplicateInvoiceLineFlag",
        BooleanType(),
        False
    ),
    StructField(
        "OriginalInvoiceItemID",
        StringType(),
        True
    )
])

invoice_item_df = spark.createDataFrame(
    invoice_item_rows,
    schema=invoice_item_schema
)

display(
    invoice_item_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add Audit Metadata
invoice_item_df = (
    invoice_item_df
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
                F.col("InvoiceItemID"),
                F.col("InvoiceID"),
                F.col("POItemID"),
                F.col(
                    "InvoicedQuantity"
                ).cast("string"),
                F.col(
                    "InvoiceUnitPrice"
                ).cast("string"),
                F.col(
                    "GrossAmount"
                ).cast("string"),
                F.col(
                    "ThreeWayMatchStatus"
                )
            ),
            256
        )
    )
)

display(
    invoice_item_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Structural Validation
actual_invoice_item_count = (
    invoice_item_df.count()
)

distinct_invoice_count = (
    invoice_item_df
    .select("InvoiceID")
    .distinct()
    .count()
)

duplicate_invoice_item_id_count = (
    invoice_item_df
    .groupBy("InvoiceItemID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_invoice_line_count = (
    invoice_item_df
    .groupBy(
        "InvoiceID",
        "InvoiceLineNumber"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    invoice_item_df
    .filter(
        F.col("InvoiceItemID").isNull()
        | F.col("InvoiceID").isNull()
        | F.col("POID").isNull()
        | F.col("POItemID").isNull()
        | F.col("MaterialID").isNull()
        | F.col(
            "InvoicedQuantity"
        ).isNull()
        | F.col(
            "InvoiceUnitPrice"
        ).isNull()
        | F.col("NetAmount").isNull()
        | F.col("GrossAmount").isNull()
    )
    .count()
)

invalid_amount_count = (
    invoice_item_df
    .filter(
        (F.col("InvoicedQuantity") <= 0)
        | (F.col("InvoiceUnitPrice") <= 0)
        | (F.col("NetAmount") <= 0)
        | (F.col("GrossAmount") <= 0)
    )
    .count()
)

assert actual_invoice_item_count > 0
assert distinct_invoice_count == invoice_header_count
assert duplicate_invoice_item_id_count == 0
assert duplicate_invoice_line_count == 0
assert mandatory_null_count == 0
assert invalid_amount_count == 0

print("Invoice-item structural validation passed.")
print(
    f"Invoice-item rows: "
    f"{actual_invoice_item_count:,}"
)
print(
    f"Distinct invoices: "
    f"{distinct_invoice_count:,}"
)
print(
    f"Duplicate InvoiceItemIDs: "
    f"{duplicate_invoice_item_id_count}"
)
print(
    f"Duplicate invoice lines: "
    f"{duplicate_invoice_line_count}"
)
print(
    f"Mandatory nulls: "
    f"{mandatory_null_count}"
)
print(
    f"Invalid amounts: "
    f"{invalid_amount_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Foreign Key Validation
invalid_invoice_count = (
    invoice_item_df.alias("item")
    .join(
        invoice_header_reference_df.alias(
            "invoice"
        ),
        F.col("item.InvoiceID")
        == F.col("invoice.InvoiceID"),
        "left_anti"
    )
    .count()
)

invalid_po_item_count = (
    invoice_item_df.alias("item")
    .join(
        po_item_reference_df.alias(
            "po_item"
        ),
        F.col("item.POItemID")
        == F.col("po_item.POItemID"),
        "left_anti"
    )
    .count()
)

assert invalid_invoice_count == 0
assert invalid_po_item_count == 0

print("Invoice-item foreign-key validation passed.")
print(
    f"Invalid invoices: "
    f"{invalid_invoice_count}"
)
print(
    f"Invalid PO items: "
    f"{invalid_po_item_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Invoice and PO Consistency
invoice_consistency_df = (
    invoice_item_df.alias("item")
    .join(
        invoice_header_reference_df.alias(
            "invoice"
        ),
        F.col("item.InvoiceID")
        == F.col("invoice.InvoiceID"),
        "inner"
    )
    .select(
        F.col("item.InvoiceItemID"),
        F.col("item.InvoiceID"),
        F.col("item.POID").alias(
            "ItemPOID"
        ),
        F.col("invoice.POID").alias(
            "HeaderPOID"
        ),
        F.col("item.Currency").alias(
            "ItemCurrency"
        ),
        F.col("invoice.Currency").alias(
            "HeaderCurrency"
        )
    )
)

invoice_consistency_error_count = (
    invoice_consistency_df
    .filter(
        (F.col("ItemPOID")
         != F.col("HeaderPOID"))
        |
        (F.col("ItemCurrency")
         != F.col("HeaderCurrency"))
    )
    .count()
)

assert invoice_consistency_error_count == 0

print(
    "Invoice header-to-item consistency "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Monetary Calculations
monetary_calculation_error_count = (
    invoice_item_df
    .filter(
        (
            F.abs(
                F.col("NetAmount")
                - F.round(
                    F.col("InvoicedQuantity")
                    * F.col("InvoiceUnitPrice"),
                    2
                )
            ) > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col("TaxAmount")
                - F.round(
                    F.col("NetAmount")
                    * F.col("TaxRate"),
                    2
                )
            ) > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col("GrossAmount")
                - (
                    F.col("NetAmount")
                    + F.col("TaxAmount")
                )
            ) > F.lit(0.01)
        )
    )
    .count()
)

assert monetary_calculation_error_count == 0, (
    f"Found {monetary_calculation_error_count} "
    "invoice items with calculation errors."
)

print(
    "Invoice-item monetary calculation "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Duplicate Invoice Lines
duplicate_line_validation_df = (
    invoice_item_df.alias("duplicate")
    .filter(
        F.col(
            "duplicate."
            "DuplicateInvoiceLineFlag"
        )
    )
    .join(
        invoice_item_df.alias("original"),
        F.col(
            "duplicate."
            "OriginalInvoiceItemID"
        )
        == F.col(
            "original.InvoiceItemID"
        ),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_duplicate_line_count = (
    duplicate_line_validation_df
    .filter(
        F.col(
            "original.InvoiceItemID"
        ).isNull()
        |
        (
            F.col(
                "duplicate.POItemID"
            )
            != F.col(
                "original.POItemID"
            )
        )
        |
        (
            F.col(
                "duplicate.InvoicedQuantity"
            )
            != F.col(
                "original.InvoicedQuantity"
            )
        )
        |
        (
            F.col(
                "duplicate.InvoiceUnitPrice"
            )
            != F.col(
                "original.InvoiceUnitPrice"
            )
        )
        |
        (
            F.col(
                "duplicate.GrossAmount"
            )
            != F.col(
                "original.GrossAmount"
            )
        )
    )
    .count()
)

assert invalid_duplicate_line_count == 0

print(
    "Duplicate invoice-line "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect 3-way-natch results
display(
    invoice_item_df
    .groupBy(
        "ThreeWayMatchStatus"
    )
    .agg(
        F.count("*").alias(
            "InvoiceItemCount"
        ),
        F.round(
            F.sum("GrossAmount"),
            2
        ).alias(
            "GrossInvoiceAmount"
        )
    )
    .orderBy(
        F.desc("InvoiceItemCount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    invoice_item_df
    .agg(
        F.count("*").alias(
            "InvoiceItemCount"
        ),
        F.sum(
            F.when(
                F.col(
                    "ThreeWayMatchStatus"
                )
                == "MATCHED",
                1
            ).otherwise(0)
        ).alias(
            "MatchedItems"
        ),
        F.round(
            F.avg(
                F.when(
                    F.col(
                        "ThreeWayMatchStatus"
                    )
                    == "MATCHED",
                    1.0
                ).otherwise(0.0)
            ) * 100,
            2
        ).alias(
            "ThreeWayMatchPercentage"
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
# Validate Dispute -> Invoice Exception Propagation
# ============================================================

invoice_exception_by_header_df = (
    invoice_item_df

    .groupBy(
        "InvoiceID"
    )

    .agg(
        F.count("*")
        .alias(
            "InvoiceItemCount"
        ),

        F.sum(
            F.when(
                F.col(
                    "ThreeWayMatchStatus"
                )
                != "MATCHED",
                1
            )
            .otherwise(0)
        )
        .alias(
            "InvoiceExceptionItemCount"
        )
    )

    .join(
        invoice_header_reference_df

        .select(
            "InvoiceID",
            "SupplierID",
            "DisputeFlag",
            "DuplicateInvoiceFlag"
        ),

        on="InvoiceID",

        how="left"
    )

    .withColumn(
        "HasInvoiceExceptionFlag",

        F.when(
            F.col(
                "InvoiceExceptionItemCount"
            ) > 0,
            1
        )
        .otherwise(0)
    )
)


# ------------------------------------------------------------
# Validation 1
# Every disputed primary invoice should create at least
# one invoice-item exception.
# ------------------------------------------------------------

disputed_without_exception_count = (
    invoice_exception_by_header_df

    .filter(
        (
            F.col(
                "DisputeFlag"
            )
            == True
        )
        &
        (
            F.col(
                "DuplicateInvoiceFlag"
            )
            == False
        )
        &
        (
            F.col(
                "InvoiceExceptionItemCount"
            )
            == 0
        )
    )

    .count()
)


assert (
    disputed_without_exception_count
    == 0
), (
    f"{disputed_without_exception_count:,} "
    f"disputed primary invoices contain "
    f"no invoice-item exception."
)


# ------------------------------------------------------------
# Validation 2
# Compare exception behavior between disputed and
# non-disputed primary invoices.
# ------------------------------------------------------------

invoice_exception_summary_df = (
    invoice_exception_by_header_df

    .filter(
        F.col(
            "DuplicateInvoiceFlag"
        )
        == False
    )

    .groupBy(
        "DisputeFlag"
    )

    .agg(
        F.count("*")
        .alias(
            "InvoiceCount"
        ),

        F.sum(
            "InvoiceItemCount"
        )
        .alias(
            "InvoiceItemCount"
        ),

        F.sum(
            "InvoiceExceptionItemCount"
        )
        .alias(
            "InvoiceExceptionItemCount"
        ),

        F.round(
            F.avg(
                F.col(
                    "HasInvoiceExceptionFlag"
                )
                .cast("double")
            )
            * 100,
            2
        )
        .alias(
            "InvoicesWithExceptionPct"
        ),

        F.round(
            (
                F.sum(
                    "InvoiceExceptionItemCount"
                )
                /
                F.sum(
                    "InvoiceItemCount"
                )
            )
            * 100,
            2
        )
        .alias(
            "InvoiceItemExceptionPct"
        )
    )

    .orderBy(
        "DisputeFlag"
    )
)


display(
    invoice_exception_summary_df
)


print(
    "Disputed primary invoices "
    "without an invoice-item exception:",
    disputed_without_exception_count
)


print(
    "Dispute -> invoice exception "
    "propagation validation PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write bronze_invoice_item
TARGET_INVOICE_ITEM_TABLE = (
    "bronze_invoice_item"
)

(
    invoice_item_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_INVOICE_ITEM_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_INVOICE_ITEM_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read back the table
saved_invoice_item_df = (
    spark.table(
        "bronze_invoice_item"
    )
)

persisted_invoice_item_count = (
    saved_invoice_item_df.count()
)

assert (
    persisted_invoice_item_count
    == actual_invoice_item_count
)

print(
    f"Persisted invoice-item rows: "
    f"{persisted_invoice_item_count:,}"
)

print(
    "Persisted invoice-item "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calculate Invoice Totals
invoice_total_df = (
    saved_invoice_item_df
    .groupBy("InvoiceID")
    .agg(
        F.sum("GrossAmount").alias(
            "CalculatedInvoiceAmount"
        )
    )
    .withColumn(
        "CalculatedInvoiceAmount",
        F.col(
            "CalculatedInvoiceAmount"
        ).cast("decimal(18,2)")
    )
)

calculated_invoice_count = (
    invoice_total_df.count()
)

assert (
    calculated_invoice_count
    == invoice_header_count
)

display(
    invoice_total_df.limit(50)
)

print(
    f"Calculated totals for "
    f"{calculated_invoice_count:,} invoices."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Update Invoice-header totals
invoice_header_delta_table = (
    DeltaTable.forName(
        spark,
        "bronze_invoice_header"
    )
)

(
    invoice_header_delta_table.alias(
        "target"
    )
    .merge(
        invoice_total_df.alias("source"),
        (
            "target.InvoiceID "
            "= source.InvoiceID"
        )
    )
    .whenMatchedUpdate(
        set={
            "TotalInvoiceAmount": (
                "source."
                "CalculatedInvoiceAmount"
            ),
            "AmountReconciliationStatus": (
                "'RECONCILED'"
            )
        }
    )
    .execute()
)

print(
    "Invoice-header totals "
    "updated successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate header reconciliation
reconciled_invoice_header_df = (
    spark.table(
        "bronze_invoice_header"
    )
)

invoice_reconciliation_error_count = (
    reconciled_invoice_header_df.alias(
        "header"
    )
    .join(
        invoice_total_df.alias("total"),
        F.col("header.InvoiceID")
        == F.col("total.InvoiceID"),
        "inner"
    )
    .filter(
        F.abs(
            F.col(
                "header.TotalInvoiceAmount"
            )
            - F.col(
                "total.CalculatedInvoiceAmount"
            )
        ) > F.lit(0.01)
    )
    .count()
)

pending_invoice_count = (
    reconciled_invoice_header_df
    .filter(
        F.col(
            "AmountReconciliationStatus"
        )
        != "RECONCILED"
    )
    .count()
)

zero_invoice_total_count = (
    reconciled_invoice_header_df
    .filter(
        F.col(
            "TotalInvoiceAmount"
        ) <= 0
    )
    .count()
)

assert (
    invoice_reconciliation_error_count
    == 0
)

assert pending_invoice_count == 0
assert zero_invoice_total_count == 0

print(
    "Invoice-header reconciliation passed."
)

print(
    f"Reconciliation differences: "
    f"{invoice_reconciliation_error_count}"
)

print(
    f"Pending invoices: "
    f"{pending_invoice_count}"
)

print(
    f"Zero invoice totals: "
    f"{zero_invoice_total_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Final Summary
final_invoice_summary_df = (
    saved_invoice_item_df
    .agg(
        F.count("*").alias(
            "InvoiceItemCount"
        ),
        F.countDistinct(
            "InvoiceID"
        ).alias(
            "InvoiceCount"
        ),
        F.round(
            F.sum("NetAmount"),
            2
        ).alias(
            "TotalNetInvoiceAmount"
        ),
        F.round(
            F.sum("TaxAmount"),
            2
        ).alias(
            "TotalTaxAmount"
        ),
        F.round(
            F.sum("GrossAmount"),
            2
        ).alias(
            "TotalGrossInvoiceAmount"
        ),
        F.sum(
            F.when(
                F.col(
                    "ThreeWayMatchStatus"
                )
                != "MATCHED",
                1
            ).otherwise(0)
        ).alias(
            "MatchExceptionItems"
        ),
        F.sum(
            F.col(
                "DuplicateInvoiceLineFlag"
            ).cast("int")
        ).alias(
            "DuplicateInvoiceItems"
        )
    )
)

display(final_invoice_summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
