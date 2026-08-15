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

# ============================================================
# Configuration
# ============================================================

from datetime import date

PROFILE = "development"

AS_OF_DATE = date(2026, 7, 31)

RANDOM_SEED = 20260802


# ------------------------------------------------------------
# Supplier-risk-driven delivery behavior
#
# Late-delivery probability:
#
# P(late) =
#     base rate
#     + risk propensity * risk slope
#
# Approximate behavior:
#
# Low-risk supplier    ~ 5-9%
# Medium-risk supplier ~ 10-16%
# High-risk supplier   ~ 18-30%
#
# We intentionally keep randomness. Risk changes probability,
# it does not deterministically decide the outcome.
# ------------------------------------------------------------

LATE_DELIVERY_BASE_RATE = 0.04

LATE_DELIVERY_RISK_SLOPE = 0.28

MAX_LATE_DELIVERY_RATE = 0.34


# ------------------------------------------------------------
# Supplier-risk time evolution
# ------------------------------------------------------------

RISK_START_YEAR = 2022

DETERIORATING_SUPPLIER_RATE = 0.10

IMPROVING_SUPPLIER_RATE = 0.05

DETERIORATING_ANNUAL_SLOPE = 0.035

IMPROVING_ANNUAL_SLOPE = -0.025


# ------------------------------------------------------------
# Existing delivery simulation
# ------------------------------------------------------------

OVER_DELIVERY_RATE = 0.02


print(
    f"Profile: {PROFILE}"
)

print(
    f"As-of date: {AS_OF_DATE}"
)

print(
    "Late-delivery behavior: "
    "supplier-risk driven"
)

print(
    f"Late probability base: "
    f"{LATE_DELIVERY_BASE_RATE:.1%}"
)

print(
    f"Late probability maximum: "
    f"{MAX_LATE_DELIVERY_RATE:.1%}"
)

print(
    f"Over-delivery target: "
    f"{OVER_DELIVERY_RATE:.1%}"
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

from datetime import timedelta

from decimal import (
    Decimal,
    ROUND_HALF_UP
)


from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    BooleanType
)


random.seed(
    RANDOM_SEED
)


QUANTITY_PRECISION = (
    Decimal("0.001")
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
# Read PO and Supplier Tables
# ============================================================

po_header_reference_df = (
    spark.table(
        "bronze_purchase_order_header"
    )

    .select(
        "POID",
        "OrderDate",
        "SupplierID",
        "BusinessUnitID",
        "POStatus"
    )
)


po_item_reference_df = (
    spark.table(
        "bronze_purchase_order_item"
    )

    .select(
        "POItemID",
        "POID",
        "MaterialID",
        "Quantity",
        "OrderUnit",
        "RequestedDeliveryDate",
        "POItemStatus"
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
    "PO items:",
    po_item_reference_df.count()
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

# ============================================================
# Join Header, Item and Supplier Data
# ============================================================

receipt_source_df = (
    po_item_reference_df.alias(
        "item"
    )

    .join(
        po_header_reference_df.alias(
            "header"
        ),

        F.col("item.POID")
        == F.col("header.POID"),

        "inner"
    )

    .join(
        supplier_reference_df.alias(
            "supplier"
        ),

        F.col("header.SupplierID")
        == F.col("supplier.SupplierID"),

        "left"
    )

    .select(
        F.col(
            "item.POItemID"
        ),

        F.col(
            "item.POID"
        ),

        F.col(
            "item.MaterialID"
        ),

        F.col(
            "item.Quantity"
        ),

        F.col(
            "item.OrderUnit"
        ),

        F.col(
            "item.RequestedDeliveryDate"
        ),

        F.col(
            "item.POItemStatus"
        ),

        F.col(
            "header.OrderDate"
        ),

        F.col(
            "header.SupplierID"
        ),

        F.col(
            "header.BusinessUnitID"
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


source_item_count = (
    receipt_source_df.count()
)


expected_item_count = (
    po_item_reference_df.count()
)


assert (
    source_item_count
    == expected_item_count
), (
    f"Expected {expected_item_count:,} "
    f"joined rows, but found "
    f"{source_item_count:,}."
)


missing_supplier_risk_count = (
    receipt_source_df

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
    f"PO items have no supplier "
    f"financial risk score."
)


print(
    f"Receipt source prepared: "
    f"{source_item_count:,} PO items."
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

#Collect Receipt Source Records
receipt_source_records = [
    row.asDict()
    for row in (
        receipt_source_df
        .orderBy(
            "POID",
            "POItemID"
        )
        .collect()
    )
]

print(
    f"Collected "
    f"{len(receipt_source_records):,} "
    f"PO items."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define the receipt plan
def determine_receipt_plan(
    po_item_status,
    ordered_quantity
):
    if po_item_status == "Cancelled":
        return {
            "receipt_count": 0,
            "total_received": Decimal("0.000"),
            "over_delivery": False
        }

    if po_item_status == "Open":
        # Most open items have not yet been received.
        if random.random() < 0.70:
            return {
                "receipt_count": 0,
                "total_received": Decimal("0.000"),
                "over_delivery": False
            }

        receipt_ratio = random.uniform(
            0.10,
            0.50
        )

        receipt_count = 1
        over_delivery = False

    elif po_item_status == "Partially Received":
        receipt_ratio = random.uniform(
            0.35,
            0.85
        )

        receipt_count = random.choices(
            population=[1, 2],
            weights=[80, 20],
            k=1
        )[0]

        over_delivery = False

    elif po_item_status in [
        "Fully Received",
        "Closed"
    ]:
        over_delivery = (
            random.random()
            < OVER_DELIVERY_RATE
        )

        if over_delivery:
            receipt_ratio = random.uniform(
                1.01,
                1.08
            )
        else:
            receipt_ratio = 1.00

        receipt_count = random.choices(
            population=[1, 2, 3],
            weights=[65, 30, 5],
            k=1
        )[0]

    else:
        raise ValueError(
            f"Unexpected PO item status: "
            f"{po_item_status}"
        )

    total_received = (
        ordered_quantity
        * Decimal(str(receipt_ratio))
    ).quantize(
        QUANTITY_PRECISION,
        rounding=ROUND_HALF_UP
    )

    if total_received <= 0:
        total_received = QUANTITY_PRECISION

    return {
        "receipt_count": receipt_count,
        "total_received": total_received,
        "over_delivery": over_delivery
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Split Quantities across receipts
#This ensures the receipt quantities add up exactly to the planned total.
def split_quantity(
    total_quantity,
    number_of_receipts
):
    if number_of_receipts == 0:
        return []

    if number_of_receipts == 1:
        return [total_quantity]

    total_units = int(
        (
            total_quantity
            * Decimal("1000")
        ).to_integral_value(
            rounding=ROUND_HALF_UP
        )
    )

    number_of_receipts = min(
        number_of_receipts,
        total_units
    )

    cut_points = sorted(
        random.sample(
            range(1, total_units),
            number_of_receipts - 1
        )
    )

    boundaries = (
        [0]
        + cut_points
        + [total_units]
    )

    quantities = []

    for index in range(
        number_of_receipts
    ):
        quantity_units = (
            boundaries[index + 1]
            - boundaries[index]
        )

        quantity = (
            Decimal(quantity_units)
            / Decimal("1000")
        ).quantize(
            QUANTITY_PRECISION
        )

        quantities.append(quantity)

    assert sum(quantities) == total_quantity

    return quantities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate receipt dates
def generate_final_receipt_date(
    order_date,
    requested_delivery_date,
    late_delivery
):
    if (
        late_delivery
        and requested_delivery_date < AS_OF_DATE
    ):
        maximum_late_days = min(
            30,
            (
                AS_OF_DATE
                - requested_delivery_date
            ).days
        )

        if maximum_late_days >= 1:
            late_days = random.randint(
                1,
                maximum_late_days
            )

            return (
                requested_delivery_date
                + timedelta(days=late_days)
            )

    latest_on_time_date = min(
        requested_delivery_date,
        AS_OF_DATE
    )

    latest_on_time_date = max(
        latest_on_time_date,
        order_date
    )

    available_days = (
        latest_on_time_date
        - order_date
    ).days

    days_before_target = random.randint(
        0,
        min(14, available_days)
    )

    return (
        latest_on_time_date
        - timedelta(
            days=days_before_target
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_receipt_dates(
    order_date,
    requested_delivery_date,
    number_of_receipts,
    late_delivery
):
    if number_of_receipts == 0:
        return []

    final_receipt_date = (
        generate_final_receipt_date(
            order_date,
            requested_delivery_date,
            late_delivery
        )
    )

    if number_of_receipts == 1:
        return [final_receipt_date]

    available_days = max(
        (
            final_receipt_date
            - order_date
        ).days,
        0
    )

    earlier_dates = []

    for _ in range(
        number_of_receipts - 1
    ):
        random_offset = random.randint(
            0,
            available_days
        )

        earlier_dates.append(
            order_date
            + timedelta(
                days=random_offset
            )
        )

    receipt_dates = sorted(
        earlier_dates
        + [final_receipt_date]
    )

    return receipt_dates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Supplier Risk Propensity and Receipt Scenario Logic
# ============================================================

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
    """
    Deterministic pseudo-random number in [0, 1].

    Used to create persistent supplier characteristics
    without exposing a synthetic latent-risk field in
    the Bronze source tables.
    """

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
    """
    Synthetic hidden supplier-risk propensity.

    Inputs visible to the analytical model:
    - Financial risk
    - ESG rating

    Additional hidden effects:
    - persistent supplier-specific component
    - gradual deterioration / improvement
    - small annual shock

    The hidden components make historical operational
    performance useful without creating a trivially
    deterministic ML problem.
    """

    # --------------------------------------------------------
    # Financial risk: 0-100 -> 0-1
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # ESG risk
    # Missing ESG is treated as moderate uncertainty.
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Supplier status
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Persistent hidden supplier characteristic
    # --------------------------------------------------------

    supplier_component = (
        stable_uniform_value(
            supplier_id,
            "supplier_base_risk",
            RANDOM_SEED
        )
    )


    # --------------------------------------------------------
    # Persistent supplier trajectory
    #
    # 10% gradually deteriorate.
    # 5% gradually improve.
    # Remaining suppliers are broadly stable.
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Small year-specific shock
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # Combined propensity
    # --------------------------------------------------------

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


def derive_late_delivery_probability(
    supplier_risk_propensity
):
    """
    Convert supplier risk propensity into a
    probabilistic late-delivery rate.
    """

    probability = (
        LATE_DELIVERY_BASE_RATE
        +
        LATE_DELIVERY_RISK_SLOPE
        * supplier_risk_propensity
    )


    return min(
        max(
            probability,
            LATE_DELIVERY_BASE_RATE
        ),
        MAX_LATE_DELIVERY_RATE
    )


def classify_supplier_risk_band(
    supplier_risk_propensity
):
    if supplier_risk_propensity < 0.25:
        return "LOW"

    if supplier_risk_propensity < 0.50:
        return "MODERATE"

    return "HIGH"


def derive_receipt_scenario(
    po_item_status,
    late_delivery,
    over_delivery
):
    if over_delivery:
        return "OVER_DELIVERY"

    if po_item_status in [
        "Fully Received",
        "Closed"
    ]:
        if late_delivery:
            return "LATE_COMPLETE"

        return "ON_TIME_COMPLETE"

    if late_delivery:
        return "LATE_PARTIAL"

    return "PARTIAL_RECEIPT"


print(
    "Supplier-risk-driven "
    "delivery logic configured."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Generate Goods-Receipt Rows
# ============================================================

goods_receipt_rows = []

delivery_risk_diagnostic_rows = []

goods_receipt_number = 0


for source_record in receipt_source_records:

    ordered_quantity = (
        source_record[
            "Quantity"
        ]
    )


    receipt_plan = (
        determine_receipt_plan(
            po_item_status=(
                source_record[
                    "POItemStatus"
                ]
            ),
            ordered_quantity=(
                ordered_quantity
            )
        )
    )


    receipt_count = (
        receipt_plan[
            "receipt_count"
        ]
    )


    if receipt_count == 0:
        continue


    # ========================================================
    # Supplier-risk-driven late-delivery probability
    # ========================================================

    event_year = (
        source_record[
            "RequestedDeliveryDate"
        ].year
    )


    supplier_risk_propensity = (
        derive_supplier_risk_propensity(
            supplier_id=(
                source_record[
                    "SupplierID"
                ]
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


    late_delivery_probability = (
        derive_late_delivery_probability(
            supplier_risk_propensity
        )
    )


    # Keep the process stochastic.
    # High risk means higher probability, not certainty.
    late_delivery = (
        random.random()
        < late_delivery_probability
    )


    # A receipt cannot be late if its requested
    # delivery date has not yet passed.
    if (
        source_record[
            "RequestedDeliveryDate"
        ]
        >= AS_OF_DATE
    ):
        late_delivery = False


    delivery_risk_diagnostic_rows.append(
        {
            "POItemID":
                source_record[
                    "POItemID"
                ],

            "SupplierID":
                source_record[
                    "SupplierID"
                ],

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

            "LateDeliveryProbability":
                float(
                    late_delivery_probability
                ),

            "LateDeliveryFlag":
                bool(
                    late_delivery
                ),

            "LateEvaluationEligibleFlag":
                bool(
                    source_record[
                        "RequestedDeliveryDate"
                    ]
                    < AS_OF_DATE
                )
        }
    )


    # ========================================================
    # Existing receipt generation
    # ========================================================

    receipt_quantities = (
        split_quantity(
            total_quantity=(
                receipt_plan[
                    "total_received"
                ]
            ),

            number_of_receipts=(
                receipt_count
            )
        )
    )


    receipt_dates = (
        generate_receipt_dates(
            order_date=(
                source_record[
                    "OrderDate"
                ]
            ),

            requested_delivery_date=(
                source_record[
                    "RequestedDeliveryDate"
                ]
            ),

            number_of_receipts=(
                receipt_count
            ),

            late_delivery=(
                late_delivery
            )
        )
    )


    receipt_scenario = (
        derive_receipt_scenario(
            po_item_status=(
                source_record[
                    "POItemStatus"
                ]
            ),

            late_delivery=(
                late_delivery
            ),

            over_delivery=(
                receipt_plan[
                    "over_delivery"
                ]
            )
        )
    )


    cumulative_quantity = (
        Decimal("0.000")
    )


    for receipt_index in range(
        receipt_count
    ):

        goods_receipt_number += 1


        quantity_received = (
            receipt_quantities[
                receipt_index
            ]
        )


        receipt_date = (
            receipt_dates[
                receipt_index
            ]
        )


        cumulative_quantity += (
            quantity_received
        )


        is_final_receipt = (
            receipt_index
            == receipt_count - 1
        )


        delivery_complete = (
            is_final_receipt
            and cumulative_quantity
            >= ordered_quantity
        )


        if (
            is_final_receipt
            and receipt_plan[
                "over_delivery"
            ]
        ):
            receipt_status = (
                "OVER_DELIVERY"
            )

        elif delivery_complete:
            receipt_status = (
                "FINAL"
            )

        else:
            receipt_status = (
                "PARTIAL"
            )


        days_late = max(
            (
                receipt_date
                -
                source_record[
                    "RequestedDeliveryDate"
                ]
            ).days,
            0
        )


        goods_receipt_rows.append(
            {
                "GoodsReceiptID":
                    (
                        f"GR"
                        f"{goods_receipt_number:010d}"
                    ),

                "POID":
                    source_record[
                        "POID"
                    ],

                "POItemID":
                    source_record[
                        "POItemID"
                    ],

                "ReceiptSequence":
                    receipt_index + 1,

                "MaterialID":
                    source_record[
                        "MaterialID"
                    ],

                "SupplierID":
                    source_record[
                        "SupplierID"
                    ],

                "BusinessUnitID":
                    source_record[
                        "BusinessUnitID"
                    ],

                "ReceiptDate":
                    receipt_date,

                "QuantityReceived":
                    quantity_received,

                "UnitOfMeasure":
                    source_record[
                        "OrderUnit"
                    ],

                "ReceiptStatus":
                    receipt_status,

                "DeliveryCompleteFlag":
                    delivery_complete,

                "IsLateReceipt":
                    (
                        days_late > 0
                    ),

                "DaysLate":
                    days_late,

                "SimulationReceiptScenario":
                    receipt_scenario
            }
        )


print(
    f"Prepared "
    f"{len(goods_receipt_rows):,} "
    f"goods-receipt records."
)

print(
    f"Prepared "
    f"{len(delivery_risk_diagnostic_rows):,} "
    f"delivery-risk diagnostic records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark DataFrame
goods_receipt_schema = StructType([
    StructField(
        "GoodsReceiptID",
        StringType(),
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
        "ReceiptSequence",
        IntegerType(),
        False
    ),
    StructField(
        "MaterialID",
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
        "ReceiptDate",
        DateType(),
        False
    ),
    StructField(
        "QuantityReceived",
        DecimalType(18, 3),
        False
    ),
    StructField(
        "UnitOfMeasure",
        StringType(),
        False
    ),
    StructField(
        "ReceiptStatus",
        StringType(),
        False
    ),
    StructField(
        "DeliveryCompleteFlag",
        BooleanType(),
        False
    ),
    StructField(
        "IsLateReceipt",
        BooleanType(),
        False
    ),
    StructField(
        "DaysLate",
        IntegerType(),
        False
    ),
    StructField(
        "SimulationReceiptScenario",
        StringType(),
        False
    )
])

goods_receipt_df = (
    spark.createDataFrame(
        goods_receipt_rows,
        schema=goods_receipt_schema
    )
)

display(
    goods_receipt_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add audit columns
goods_receipt_df = (
    goods_receipt_df
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
                F.col("GoodsReceiptID"),
                F.col("POItemID"),
                F.col("ReceiptSequence").cast(
                    "string"
                ),
                F.col("ReceiptDate").cast(
                    "string"
                ),
                F.col(
                    "QuantityReceived"
                ).cast("string"),
                F.col("ReceiptStatus")
            ),
            256
        )
    )
)

display(
    goods_receipt_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Structural Validation
actual_receipt_count = (
    goods_receipt_df.count()
)

duplicate_receipt_id_count = (
    goods_receipt_df
    .groupBy("GoodsReceiptID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_receipt_sequence_count = (
    goods_receipt_df
    .groupBy(
        "POItemID",
        "ReceiptSequence"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    goods_receipt_df
    .filter(
        F.col("GoodsReceiptID").isNull()
        | F.col("POID").isNull()
        | F.col("POItemID").isNull()
        | F.col("ReceiptDate").isNull()
        | F.col(
            "QuantityReceived"
        ).isNull()
        | F.col("ReceiptStatus").isNull()
    )
    .count()
)

invalid_quantity_count = (
    goods_receipt_df
    .filter(
        F.col("QuantityReceived") <= 0
    )
    .count()
)

assert actual_receipt_count > 0
assert duplicate_receipt_id_count == 0
assert duplicate_receipt_sequence_count == 0
assert mandatory_null_count == 0
assert invalid_quantity_count == 0

print("Goods-receipt structural validation passed.")
print(f"Rows: {actual_receipt_count:,}")
print(
    f"Duplicate receipt IDs: "
    f"{duplicate_receipt_id_count}"
)
print(
    f"Duplicate receipt sequences: "
    f"{duplicate_receipt_sequence_count}"
)
print(
    f"Mandatory nulls: "
    f"{mandatory_null_count}"
)
print(
    f"Invalid quantities: "
    f"{invalid_quantity_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Foreign-key validation
invalid_po_item_count = (
    goods_receipt_df.alias("receipt")
    .join(
        po_item_reference_df.alias("item"),
        F.col("receipt.POItemID")
        == F.col("item.POItemID"),
        "left_anti"
    )
    .count()
)

invalid_po_count = (
    goods_receipt_df.alias("receipt")
    .join(
        po_header_reference_df.alias(
            "header"
        ),
        F.col("receipt.POID")
        == F.col("header.POID"),
        "left_anti"
    )
    .count()
)

assert invalid_po_item_count == 0
assert invalid_po_count == 0

print("Goods-receipt foreign-key validation passed.")
print(
    f"Invalid PO items: "
    f"{invalid_po_item_count}"
)
print(
    f"Invalid POs: "
    f"{invalid_po_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Receipt Dates
receipt_date_validation_df = (
    goods_receipt_df.alias("receipt")
    .join(
        receipt_source_df.alias("source"),
        F.col("receipt.POItemID")
        == F.col("source.POItemID"),
        "inner"
    )
)

receipt_before_order_count = (
    receipt_date_validation_df
    .filter(
        F.col("receipt.ReceiptDate")
        < F.col("source.OrderDate")
    )
    .count()
)

receipt_after_as_of_count = (
    goods_receipt_df
    .filter(
        F.col("ReceiptDate")
        > F.lit(
            AS_OF_DATE.isoformat()
        ).cast("date")
    )
    .count()
)

late_flag_mismatch_count = (
    receipt_date_validation_df
    .filter(
        F.col("receipt.IsLateReceipt")
        != (
            F.col("receipt.ReceiptDate")
            > F.col(
                "source.RequestedDeliveryDate"
            )
        )
    )
    .count()
)

assert receipt_before_order_count == 0
assert receipt_after_as_of_count == 0
assert late_flag_mismatch_count == 0

print("Goods-receipt date validation passed.")
print(
    f"Receipts before order: "
    f"{receipt_before_order_count}"
)
print(
    f"Receipts after as-of date: "
    f"{receipt_after_as_of_count}"
)
print(
    f"Late-flag mismatches: "
    f"{late_flag_mismatch_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Reconcile Received Quantities
received_quantity_df = (
    goods_receipt_df
    .groupBy("POItemID")
    .agg(
        F.sum(
            "QuantityReceived"
        ).alias(
            "TotalQuantityReceived"
        ),
        F.max(
            "ReceiptDate"
        ).alias(
            "LatestReceiptDate"
        ),
        F.max(
            F.col(
                "DeliveryCompleteFlag"
            ).cast("int")
        ).alias(
            "DeliveryCompleteFlag"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quantity_reconciliation_df = (
    po_item_reference_df.alias("item")
    .join(
        received_quantity_df.alias(
            "receipt"
        ),
        F.col("item.POItemID")
        == F.col("receipt.POItemID"),
        "left"
    )
    .select(
        F.col("item.POItemID"),
        F.col("item.POItemStatus"),
        F.col("item.Quantity").alias(
            "OrderedQuantity"
        ),
        F.coalesce(
            F.col(
                "receipt.TotalQuantityReceived"
            ),
            F.lit(0)
        ).cast(
            "decimal(18,3)"
        ).alias(
            "TotalQuantityReceived"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Valiate Status Consistency
quantity_reconciliation_df = (
    po_item_reference_df.alias("item")
    .join(
        received_quantity_df.alias(
            "receipt"
        ),
        F.col("item.POItemID")
        == F.col("receipt.POItemID"),
        "left"
    )
    .select(
        F.col("item.POItemID"),
        F.col("item.POItemStatus"),
        F.col("item.Quantity").alias(
            "OrderedQuantity"
        ),
        F.coalesce(
            F.col(
                "receipt.TotalQuantityReceived"
            ),
            F.lit(0)
        ).cast(
            "decimal(18,3)"
        ).alias(
            "TotalQuantityReceived"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Status COnsistency
cancelled_receipt_count = (
    quantity_reconciliation_df
    .filter(
        (F.col("POItemStatus") == "Cancelled")
        & (
            F.col("TotalQuantityReceived")
            > 0
        )
    )
    .count()
)

completed_under_receipt_count = (
    quantity_reconciliation_df
    .filter(
        F.col("POItemStatus").isin(
            "Fully Received",
            "Closed"
        )
        & (
            F.col("TotalQuantityReceived")
            < F.col("OrderedQuantity")
        )
    )
    .count()
)

partial_over_receipt_count = (
    quantity_reconciliation_df
    .filter(
        F.col("POItemStatus").isin(
            "Open",
            "Partially Received"
        )
        & (
            F.col("TotalQuantityReceived")
            >= F.col("OrderedQuantity")
        )
    )
    .count()
)

assert cancelled_receipt_count == 0
assert completed_under_receipt_count == 0
assert partial_over_receipt_count == 0

print("Receipt quantity reconciliation passed.")
print(
    f"Cancelled items with receipts: "
    f"{cancelled_receipt_count}"
)
print(
    f"Completed items under-received: "
    f"{completed_under_receipt_count}"
)
print(
    f"Partial items fully received: "
    f"{partial_over_receipt_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Delivery Scenario
display(
    goods_receipt_df
    .groupBy(
        "SimulationReceiptScenario"
    )
    .agg(
        F.count("*").alias(
            "ReceiptEventCount"
        ),
        F.countDistinct(
            "POItemID"
        ).alias(
            "POItemCount"
        ),
        F.round(
            F.sum(
                "QuantityReceived"
            ),
            3
        ).alias(
            "QuantityReceived"
        )
    )
    .orderBy(
        F.desc("POItemCount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Late Delivery Rate
line_delivery_performance_df = (
    received_quantity_df.alias("receipt")
    .join(
        po_item_reference_df.alias("item"),
        F.col("receipt.POItemID")
        == F.col("item.POItemID"),
        "inner"
    )
    .select(
        F.col("receipt.POItemID").alias("POItemID"),
        F.col("receipt.LatestReceiptDate"),
        F.col("item.RequestedDeliveryDate"),
        (
            F.col("receipt.LatestReceiptDate")
            > F.col("item.RequestedDeliveryDate")
        ).alias("LineLateDeliveryFlag")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    line_delivery_performance_df
    .agg(
        F.countDistinct(
            "POItemID"
        ).alias(
            "ReceivedPOItems"
        ),
        F.sum(
            F.col(
                "LineLateDeliveryFlag"
            ).cast("int")
        ).alias(
            "LatePOItems"
        ),
        F.round(
            F.avg(
                F.col(
                    "LineLateDeliveryFlag"
                ).cast("double")
            ) * 100,
            2
        ).alias(
            "LateDeliveryPercentage"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Over Deliveries
over_delivery_summary_df = (
    quantity_reconciliation_df
    .filter(
        F.col("TotalQuantityReceived")
        > F.col("OrderedQuantity")
    )
)

display(
    over_delivery_summary_df
    .agg(
        F.count("*").alias(
            "OverDeliveredPOItems"
        ),
        F.round(
            F.avg(
                (
                    F.col(
                        "TotalQuantityReceived"
                    )
                    / F.col(
                        "OrderedQuantity"
                    )
                    - 1
                ) * 100
            ),
            2
        ).alias(
            "AverageOverDeliveryPercentage"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to the Lakehouse
TARGET_GOODS_RECEIPT_TABLE = (
    "bronze_goods_receipt"
)

(
    goods_receipt_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_GOODS_RECEIPT_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_GOODS_RECEIPT_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read Back and Validate
saved_goods_receipt_df = (
    spark.table(
        "bronze_goods_receipt"
    )
)

persisted_receipt_count = (
    saved_goods_receipt_df.count()
)

persisted_distinct_receipt_count = (
    saved_goods_receipt_df
    .select("GoodsReceiptID")
    .distinct()
    .count()
)

assert (
    persisted_receipt_count
    == actual_receipt_count
)

assert (
    persisted_distinct_receipt_count
    == actual_receipt_count
)

display(
    saved_goods_receipt_df.limit(50)
)

print(
    f"Persisted goods-receipt rows: "
    f"{persisted_receipt_count:,}"
)

print(
    "Persisted goods-receipt "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Validate Supplier Risk -> Delivery Relationship
# ============================================================

delivery_risk_diagnostic_df = (
    spark.createDataFrame(
        delivery_risk_diagnostic_rows
    )
)


delivery_risk_band_summary_df = (
    delivery_risk_diagnostic_df

    .filter(
        F.col(
            "LateEvaluationEligibleFlag"
        )
    )

    .groupBy(
        "SupplierRiskBand"
    )

    .agg(
        F.count("*")
        .alias(
            "POItemCount"
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
                "LateDeliveryProbability"
            )
            * 100,
            2
        )
        .alias(
            "AveragePlannedLatePct"
        ),

        F.round(
            F.avg(
                F.col(
                    "LateDeliveryFlag"
                )
                .cast("double")
            )
            * 100,
            2
        )
        .alias(
            "ActualLatePct"
        )
    )

    .orderBy(
        "AverageRiskPropensity"
    )
)


display(
    delivery_risk_band_summary_df
)


print(
    "Expected relationship:"
)

print(
    "LOW late rate "
    "< MODERATE late rate "
    "< HIGH late rate"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
