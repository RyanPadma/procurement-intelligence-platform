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

PROFILE_VOLUMES = {
    "development": {
        "purchase_orders": 20_000
    },
    "portfolio": {
        "purchase_orders": 100_000
    }
}

if PROFILE not in PROFILE_VOLUMES:
    raise ValueError(
        f"Invalid profile: {PROFILE}"
    )

EXPECTED_PO_COUNT = (
    PROFILE_VOLUMES[
        PROFILE
    ]["purchase_orders"]
)

RANDOM_SEED = 20260802

PRICE_ANOMALY_RATE = 0.02


# ------------------------------------------------------------------
# Contract-scenario targets
# ------------------------------------------------------------------

TARGET_COMPLIANT_CONTRACT_RATE = 0.70

TARGET_INVALID_DATE_CONTRACT_RATE = 0.10

TARGET_NO_CONTRACT_RATE = 0.20


# When generating a NO_CONTRACT_REFERENCE line,
# prefer a category in which the supplier already
# has contract activity.
#
# This prevents no-contract spend from being
# disproportionately concentrated in unrelated
# high-value categories.
NO_CONTRACT_CONTRACT_CATEGORY_PREFERENCE = 0.85


assert abs(
    (
        TARGET_COMPLIANT_CONTRACT_RATE
        + TARGET_INVALID_DATE_CONTRACT_RATE
        + TARGET_NO_CONTRACT_RATE
    )
    - 1.0
) < 0.000001


print(
    f"Profile: {PROFILE}"
)

print(
    f"Expected PO headers: "
    f"{EXPECTED_PO_COUNT:,}"
)

print(
    f"Price anomaly rate: "
    f"{PRICE_ANOMALY_RATE:.1%}"
)

print(
    "Requested scenario distribution:"
)

print(
    f"  Compliant: "
    f"{TARGET_COMPLIANT_CONTRACT_RATE:.0%}"
)

print(
    f"  Invalid date: "
    f"{TARGET_INVALID_DATE_CONTRACT_RATE:.0%}"
)

print(
    f"  No contract: "
    f"{TARGET_NO_CONTRACT_RATE:.0%}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import Libraries
import random
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP

from delta.tables import DeltaTable

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

random.seed(RANDOM_SEED)

MONEY_PRECISION = Decimal("0.01")
QUANTITY_PRECISION = Decimal("0.001")

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read Parent Tables
po_header_reference_df = (
    spark.table("bronze_purchase_order_header")
    .select(
        "POID",
        "SupplierID",
        "BuyerID",
        "BusinessUnitID",
        "OrderDate",
        "Currency",
        "POStatus"
    )
)

supplier_reference_df = (
    spark.table("bronze_supplier")
    .select(
        "SupplierID",
        "SupplierType",
        "Status"
    )
)

material_reference_df = (
    spark.table("bronze_material")
    .select(
        "MaterialID",
        "CategoryID",
        "StandardCost",
        "UnitOfMeasure",
        "Status"
    )
)

category_reference_df = (
    spark.table("bronze_category")
    .select(
        "CategoryID",
        "ProcurementType"
    )
)

contract_reference_df = (
    spark.table("bronze_contract")
    .select(
        "ContractID",
        "SupplierID",
        "CategoryID",
        "ContractStartDate",
        "ContractEndDate",
        "Currency",
        "NegotiatedUnitPrice"
    )
)

exchange_rate_reference_df = (
    spark.table("bronze_exchange_rate")
    .select(
        "RateDate",
        "Currency",
        "ExchangeRateEUR"
    )
)

print(
    "PO headers:",
    po_header_reference_df.count()
)

print(
    "Suppliers:",
    supplier_reference_df.count()
)

print(
    "Materials:",
    material_reference_df.count()
)

print(
    "Categories:",
    category_reference_df.count()
)

print(
    "Contracts:",
    contract_reference_df.count()
)

print(
    "Exchange rates:",
    exchange_rate_reference_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate parent data
actual_po_count = (
    po_header_reference_df.count()
)

assert actual_po_count == EXPECTED_PO_COUNT, (
    f"Expected {EXPECTED_PO_COUNT:,} PO headers, "
    f"but found {actual_po_count:,}."
)

required_tables = {
    "supplier": supplier_reference_df.count(),
    "material": material_reference_df.count(),
    "category": category_reference_df.count(),
    "contract": contract_reference_df.count(),
    "exchange rate": exchange_rate_reference_df.count()
}

for table_name, row_count in required_tables.items():
    assert row_count > 0, (
        f"The {table_name} reference is empty."
    )

print("Parent-table validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Python reference lookups
po_header_records = [
    row.asDict()
    for row in (
        po_header_reference_df
        .orderBy("POID")
        .collect()
    )
]

supplier_lookup = {
    row["SupplierID"]: row.asDict()
    for row in supplier_reference_df.collect()
}

category_type_lookup = {
    row["CategoryID"]: row["ProcurementType"]
    for row in category_reference_df.collect()
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Active Material Lookups
materials_by_category = defaultdict(list)

active_material_records = (
    material_reference_df
    .filter(
        F.col("Status") == "Active"
    )
    .collect()
)

for row in active_material_records:
    material_record = row.asDict()

    materials_by_category[
        material_record["CategoryID"]
    ].append(material_record)

print(
    f"Active materials available: "
    f"{len(active_material_records):,}"
)

print(
    f"Categories with active materials: "
    f"{len(materials_by_category):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Contract Lookups
contracts_by_supplier = defaultdict(list)

for row in contract_reference_df.collect():
    contract_record = row.asDict()

    contracts_by_supplier[
        contract_record["SupplierID"]
    ].append(contract_record)

print(
    f"Suppliers with contracts: "
    f"{len(contracts_by_supplier):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Exchange Rates Lookups
exchange_rate_lookup = {
    (
        row["RateDate"],
        row["Currency"]
    ): float(row["ExchangeRateEUR"])
    for row in exchange_rate_reference_df.collect()
}

print(
    f"Exchange-rate lookup entries: "
    f"{len(exchange_rate_lookup):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define supplier-category eligibility
SUPPLIER_CATEGORY_ELIGIBILITY = {
    "Manufacturer": [
        "CAT001",
        "CAT002",
        "CAT003",
        "CAT004",
        "CAT005",
        "CAT006",
        "CAT007",
        "CAT008",
        "CAT009",
        "CAT010"
    ],
    "Distributor": [
        "CAT003",
        "CAT004",
        "CAT005",
        "CAT006",
        "CAT008",
        "CAT013",
        "CAT020"
    ],
    "Service Provider": [
        "CAT012",
        "CAT013",
        "CAT014",
        "CAT015",
        "CAT016",
        "CAT018",
        "CAT019"
    ],
    "Logistics Provider": [
        "CAT011",
        "CAT012"
    ],
    "Utility Provider": [
        "CAT017"
    ],
    "Contractor": [
        "CAT008",
        "CAT014",
        "CAT015",
        "CAT016"
    ]
}

supplier_types = set(
    supplier["SupplierType"]
    for supplier in supplier_lookup.values()
)

missing_supplier_types = (
    supplier_types
    - set(SUPPLIER_CATEGORY_ELIGIBILITY)
)

assert not missing_supplier_types, (
    f"Missing category rules for supplier types: "
    f"{missing_supplier_types}"
)

print("Supplier-category rules validated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define line-count logic
def generate_line_count():
    return random.choices(
        population=[
            1,
            2,
            3,
            4,
            5,
            6,
            7
        ],
        weights=[
            5,
            15,
            25,
            25,
            15,
            10,
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

# Define Quantity Logic
#
# Quantity is driven by both:
# 1. unit of measure
# 2. material value
#
# This prevents expensive materials from
# receiving unrealistically large quantities.


def generate_target_line_value_eur(
    procurement_type
):
    """
    Generate a realistic target PO-line value
    in EUR.

    The target is only used to size quantity.
    Actual PO value is still calculated from
    Quantity × UnitPrice.
    """

    if procurement_type == "Capital":

        value_band = random.choices(
            population=[
                "NORMAL",
                "LARGE",
                "MAJOR"
            ],
            weights=[
                0.65,
                0.30,
                0.05
            ],
            k=1
        )[0]

        if value_band == "NORMAL":
            return random.uniform(
                10_000,
                250_000
            )

        elif value_band == "LARGE":
            return random.uniform(
                250_000,
                1_500_000
            )

        else:
            return random.uniform(
                1_500_000,
                5_000_000
            )


    elif procurement_type == "Direct":

        value_band = random.choices(
            population=[
                "NORMAL",
                "LARGE",
                "MAJOR"
            ],
            weights=[
                0.85,
                0.13,
                0.02
            ],
            k=1
        )[0]

        if value_band == "NORMAL":
            return random.uniform(
                500,
                50_000
            )

        elif value_band == "LARGE":
            return random.uniform(
                50_000,
                250_000
            )

        else:
            return random.uniform(
                250_000,
                1_000_000
            )


    else:
        # Indirect procurement

        value_band = random.choices(
            population=[
                "NORMAL",
                "LARGE",
                "MAJOR"
            ],
            weights=[
                0.90,
                0.09,
                0.01
            ],
            k=1
        )[0]

        if value_band == "NORMAL":
            return random.uniform(
                200,
                25_000
            )

        elif value_band == "LARGE":
            return random.uniform(
                25_000,
                100_000
            )

        else:
            return random.uniform(
                100_000,
                500_000
            )

def generate_quantity(
    material,
    procurement_type
):
    """
    Generate quantity based on the material's
    StandardCost and a realistic target line value.

    High-value materials therefore naturally
    receive smaller quantities.
    """

    unit_of_measure = (
        material[
            "UnitOfMeasure"
        ]
    )

    standard_cost_eur = max(
        float(
            material[
                "StandardCost"
            ]
        ),
        0.01
    )


    target_line_value_eur = (
        generate_target_line_value_eur(
            procurement_type
        )
    )


    raw_quantity = (
        target_line_value_eur
        /
        standard_cost_eur
    )


    # ----------------------------------------------------------
    # Reasonable quantity limits by UOM
    # ----------------------------------------------------------

    quantity_limits = {

        "EA": {
            "min": 1,
            "max": 100,
            "integer": True
        },

        "KG": {
            "min": 1,
            "max": 5_000,
            "integer": False
        },

        "TON": {
            "min": 0.1,
            "max": 100,
            "integer": False
        },

        "L": {
            "min": 1,
            "max": 10_000,
            "integer": False
        },

        "PALLET": {
            "min": 1,
            "max": 50,
            "integer": True
        },

        "SERVICE": {
            "min": 1,
            "max": 12,
            "integer": True
        },

        "HOUR": {
            "min": 1,
            "max": 1_000,
            "integer": False
        },

        "DAY": {
            "min": 1,
            "max": 120,
            "integer": False
        },

        "BOX": {
            "min": 1,
            "max": 100,
            "integer": True
        }
    }


    limits = quantity_limits.get(
        unit_of_measure,
        {
            "min": 1,
            "max": 100,
            "integer": False
        }
    )


    quantity = max(
        raw_quantity,
        limits["min"]
    )

    quantity = min(
        quantity,
        limits["max"]
    )


    # Add small natural purchasing variation
    quantity *= random.uniform(
        0.85,
        1.15
    )


    quantity = max(
        quantity,
        limits["min"]
    )

    quantity = min(
        quantity,
        limits["max"]
    )


    if limits["integer"]:

        quantity = max(
            int(
                round(
                    quantity
                )
            ),
            int(
                limits["min"]
            )
        )


    return Decimal(
        str(quantity)
    ).quantize(
        QUANTITY_PRECISION,
        rounding=ROUND_HALF_UP
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Currency-Conversion Logic
def get_exchange_rate(
    rate_date,
    currency
):
    lookup_key = (
        rate_date,
        currency
    )

    if lookup_key not in exchange_rate_lookup:
        raise KeyError(
            f"Missing exchange rate for "
            f"{currency} on {rate_date}."
        )

    return exchange_rate_lookup[
        lookup_key
    ]

def convert_currency(
    amount,
    source_currency,
    target_currency,
    rate_date
):
    source_rate_to_eur = get_exchange_rate(
        rate_date,
        source_currency
    )

    target_rate_to_eur = get_exchange_rate(
        rate_date,
        target_currency
    )

    amount_in_eur = (
        float(amount)
        * source_rate_to_eur
    )

    converted_amount = (
        amount_in_eur
        / target_rate_to_eur
    )

    return converted_amount

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Contract matching
def get_valid_contracts(
    supplier_id,
    order_date
):
    supplier_contracts = (
        contracts_by_supplier.get(
            supplier_id,
            []
        )
    )

    return [
        contract
        for contract in supplier_contracts
        if (
            contract["ContractStartDate"]
            <= order_date
            <= contract["ContractEndDate"]
        )
        and (
            contract["CategoryID"]
            in materials_by_category
        )
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_invalid_date_contracts(
    supplier_id,
    order_date
):
    supplier_contracts = (
        contracts_by_supplier.get(
            supplier_id,
            []
        )
    )

    return [
        contract
        for contract in supplier_contracts
        if not (
            contract["ContractStartDate"]
            <= order_date
            <= contract["ContractEndDate"]
        )
        and (
            contract["CategoryID"]
            in materials_by_category
        )
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Diagnose contract availability by PO header

contract_availability_rows = []

for po_header in po_header_records:

    supplier_id = (
        po_header["SupplierID"]
    )

    order_date = (
        po_header["OrderDate"]
    )

    valid_contracts = (
        get_valid_contracts(
            supplier_id,
            order_date
        )
    )

    invalid_contracts = (
        get_invalid_date_contracts(
            supplier_id,
            order_date
        )
    )

    contract_availability_rows.append(
        {
            "POID": (
                po_header["POID"]
            ),
            "HasValidContract": (
                len(valid_contracts) > 0
            ),
            "HasInvalidDateContract": (
                len(invalid_contracts) > 0
            )
        }
    )


contract_availability_df = (
    spark.createDataFrame(
        contract_availability_rows
    )
)


display(
    contract_availability_df
    .groupBy(
        "HasValidContract",
        "HasInvalidDateContract"
    )
    .count()
    .orderBy(
        "HasValidContract",
        "HasInvalidDateContract"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

po_headers_with_valid_contract = (
    contract_availability_df
    .filter(
        F.col(
            "HasValidContract"
        )
    )
    .count()
)

valid_contract_header_pct = (
    po_headers_with_valid_contract
    / EXPECTED_PO_COUNT
    * 100
)

print(
    "PO headers with at least one "
    "valid contract:",
    f"{po_headers_with_valid_contract:,}"
)

print(
    "Valid-contract coverage:",
    f"{valid_contract_header_pct:.2f}%"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Contract-Scenario Selection


def select_contract_scenario(
    valid_contracts,
    invalid_date_contracts
):
    """
    Select the intended contract scenario.

    Logic:

    1. 20% of lines intentionally have
       no contract reference.

    2. For the remaining 80%:
       - Prefer compliant vs invalid-date
         using the intended 70:10 ratio
         when both are feasible.
       - If only one contract scenario
         is feasible, use that scenario.
       - Fall back to no contract only
         when no supplier contract is
         available for either scenario.

    Returns:
        scenario
        selected_contract
        selection_reason
    """

    intentional_no_contract = (
        random.random()
        <
        TARGET_NO_CONTRACT_RATE
    )

    if intentional_no_contract:
        return (
            "NO_CONTRACT_REFERENCE",
            None,
            "INTENTIONAL_NO_CONTRACT"
        )


    # --------------------------------------------------------------
    # Both valid and invalid-date contracts available
    # --------------------------------------------------------------

    if (
        valid_contracts
        and invalid_date_contracts
    ):

        selected_scenario = (
            random.choices(
                population=[
                    "COMPLIANT_CONTRACT",
                    "INVALID_DATE_CONTRACT"
                ],
                weights=[
                    TARGET_COMPLIANT_CONTRACT_RATE,
                    TARGET_INVALID_DATE_CONTRACT_RATE
                ],
                k=1
            )[0]
        )

        if (
            selected_scenario
            == "COMPLIANT_CONTRACT"
        ):
            return (
                "COMPLIANT_CONTRACT",
                random.choice(
                    valid_contracts
                ),
                "REQUESTED_COMPLIANT"
            )

        return (
            "INVALID_DATE_CONTRACT",
            random.choice(
                invalid_date_contracts
            ),
            "REQUESTED_INVALID_DATE"
        )


    # --------------------------------------------------------------
    # Only valid contracts available
    # --------------------------------------------------------------

    if valid_contracts:
        return (
            "COMPLIANT_CONTRACT",
            random.choice(
                valid_contracts
            ),
            "VALID_ONLY_AVAILABLE"
        )


    # --------------------------------------------------------------
    # Only invalid-date contracts available
    # --------------------------------------------------------------

    if invalid_date_contracts:
        return (
            "INVALID_DATE_CONTRACT",
            random.choice(
                invalid_date_contracts
            ),
            "INVALID_ONLY_AVAILABLE"
        )


    # --------------------------------------------------------------
    # No supplier contract available
    # --------------------------------------------------------------

    return (
        "NO_CONTRACT_REFERENCE",
        None,
        "NO_CONTRACT_AVAILABLE"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define material selection for no-contract scenario


def select_material_without_contract(
    supplier_id,
    supplier_type,
    valid_contracts,
    invalid_date_contracts
):

    eligible_categories = (
        SUPPLIER_CATEGORY_ELIGIBILITY[
            supplier_type
        ]
    )


    general_available_categories = [
        category_id
        for category_id
        in eligible_categories
        if materials_by_category.get(
            category_id
        )
    ]


    if not general_available_categories:
        raise ValueError(
            f"No materials available for "
            f"supplier type "
            f"{supplier_type}."
        )


    # --------------------------------------------------------------
    # Find categories where this supplier
    # already has contract activity.
    #
    # The PO line will still have ContractID=None.
    #
    # This simply keeps the category/value mix
    # of Maverick transactions more comparable
    # with contracted transactions.
    # --------------------------------------------------------------

    supplier_contract_categories = list(
        {
            contract["CategoryID"]
            for contract in (
                valid_contracts
                + invalid_date_contracts
            )
            if (
                contract["CategoryID"]
                in general_available_categories
            )
        }
    )


    # --------------------------------------------------------------
    # Prefer contract-related categories when
    # available, but occasionally allow broader
    # supplier-category purchasing.
    # --------------------------------------------------------------

    if (
        supplier_contract_categories
        and
        random.random()
        <
        NO_CONTRACT_CONTRACT_CATEGORY_PREFERENCE
    ):
        selected_category = (
            random.choice(
                supplier_contract_categories
            )
        )

    else:
        selected_category = (
            random.choice(
                general_available_categories
            )
        )


    selected_material = (
        random.choice(
            materials_by_category[
                selected_category
            ]
        )
    )

    return selected_material

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def select_material_for_contract(
    selected_contract
):
    category_id = (
        selected_contract["CategoryID"]
    )

    available_materials = (
        materials_by_category.get(
            category_id,
            []
        )
    )

    if not available_materials:
        raise ValueError(
            f"No active materials available "
            f"for category {category_id}."
        )

    return random.choice(
        available_materials
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Denife unit Price Logic
def calculate_unit_price(
    material,
    selected_contract,
    scenario,
    po_currency,
    order_date
):
    if selected_contract is not None:
        base_price = convert_currency(
            amount=selected_contract[
                "NegotiatedUnitPrice"
            ],
            source_currency=selected_contract[
                "Currency"
            ],
            target_currency=po_currency,
            rate_date=order_date
        )

        if scenario == "COMPLIANT_CONTRACT":
            normal_factor = random.uniform(
                0.98,
                1.03
            )
        else:
            normal_factor = random.uniform(
                0.95,
                1.15
            )

    else:
        base_price = convert_currency(
            amount=material["StandardCost"],
            source_currency="EUR",
            target_currency=po_currency,
            rate_date=order_date
        )

        normal_factor = random.uniform(
            0.95,
            1.25
        )

    unit_price = (
        base_price
        * normal_factor
    )

    anomaly_injected = (
        random.random()
        < PRICE_ANOMALY_RATE
    )

    if anomaly_injected:
        if random.random() < 0.80:
            anomaly_factor = random.uniform(
                1.50,
                3.00
            )
        else:
            anomaly_factor = random.uniform(
                0.35,
                0.65
            )

        unit_price *= anomaly_factor

    unit_price = max(
        unit_price,
        0.01
    )

    unit_price_decimal = Decimal(
        str(unit_price)
    ).quantize(
        MONEY_PRECISION,
        rounding=ROUND_HALF_UP
    )

    return (
        unit_price_decimal,
        anomaly_injected
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Delivery Date Logic
def generate_requested_delivery_date(
    order_date,
    procurement_type
):
    if procurement_type == "Direct":
        lead_time_days = random.randint(
            14,
            90
        )

    elif procurement_type == "Indirect":
        lead_time_days = random.randint(
            3,
            60
        )

    elif procurement_type == "Capital":
        lead_time_days = random.randint(
            60,
            240
        )

    else:
        lead_time_days = random.randint(
            7,
            90
        )

    return (
        order_date
        + timedelta(days=lead_time_days)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define PO-Item Status
def derive_po_item_status(
    po_status
):
    status_mapping = {
        "Open": "Open",
        "Partially Received": (
            "Partially Received"
        ),
        "Fully Received": (
            "Fully Received"
        ),
        "Closed": "Closed",
        "Cancelled": "Cancelled"
    }

    return status_mapping.get(
        po_status,
        "Open"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate PO-Item Records

purchase_order_item_rows = []

scenario_selection_reason_counts = (
    defaultdict(int)
)


for po_header in po_header_records:

    po_id = (
        po_header["POID"]
    )

    supplier_id = (
        po_header["SupplierID"]
    )

    order_date = (
        po_header["OrderDate"]
    )

    po_currency = (
        po_header["Currency"]
    )

    po_status = (
        po_header["POStatus"]
    )


    supplier = (
        supplier_lookup.get(
            supplier_id
        )
    )


    if supplier is None:
        raise KeyError(
            f"Supplier {supplier_id} "
            f"was not found."
        )


    valid_contracts = (
        get_valid_contracts(
            supplier_id,
            order_date
        )
    )


    invalid_date_contracts = (
        get_invalid_date_contracts(
            supplier_id,
            order_date
        )
    )


    line_count = (
        generate_line_count()
    )


    used_material_ids = set()


    for line_sequence in range(
        1,
        line_count + 1
    ):

        (
            contract_scenario,
            selected_contract,
            scenario_selection_reason
        ) = select_contract_scenario(
            valid_contracts,
            invalid_date_contracts
        )


        scenario_selection_reason_counts[
            scenario_selection_reason
        ] += 1


        # ----------------------------------------------------------
        # Material selection
        # ----------------------------------------------------------

        if selected_contract is not None:

            selected_material = (
                select_material_for_contract(
                    selected_contract
                )
            )

        else:

            selected_material = (
                select_material_without_contract(
                    supplier_id=(
                        supplier_id
                    ),
                    supplier_type=(
                        supplier[
                            "SupplierType"
                        ]
                    ),
                    valid_contracts=(
                        valid_contracts
                    ),
                    invalid_date_contracts=(
                        invalid_date_contracts
                    )
                )
            )


        # ----------------------------------------------------------
        # Reduce duplicate materials within same PO
        # ----------------------------------------------------------

        retry_count = 0

        while (
            selected_material[
                "MaterialID"
            ]
            in used_material_ids
            and retry_count < 5
        ):

            if (
                selected_contract
                is not None
            ):

                selected_material = (
                    select_material_for_contract(
                        selected_contract
                    )
                )

            else:

                selected_material = (
                    select_material_without_contract(
                        supplier_id=(
                            supplier_id
                        ),
                        supplier_type=(
                            supplier[
                                "SupplierType"
                            ]
                        ),
                        valid_contracts=(
                            valid_contracts
                        ),
                        invalid_date_contracts=(
                            invalid_date_contracts
                        )
                    )
                )

            retry_count += 1


        used_material_ids.add(
            selected_material[
                "MaterialID"
            ]
        )


        material_id = (
            selected_material[
                "MaterialID"
            ]
        )

        category_id = (
            selected_material[
                "CategoryID"
            ]
        )

        unit_of_measure = (
            selected_material[
                "UnitOfMeasure"
            ]
        )

        procurement_type = (
            category_type_lookup[
                category_id
            ]
        )


        # ----------------------------------------------------------
        # Quantity
        # ----------------------------------------------------------

        quantity = (generate_quantity(
    material=selected_material,
    procurement_type=procurement_type
)
        )

        # ----------------------------------------------------------
        # Price
        # ----------------------------------------------------------

        (
            unit_price,
            anomaly_injected
        ) = calculate_unit_price(
            material=selected_material,
            selected_contract=(
                selected_contract
            ),
            scenario=(
                contract_scenario
            ),
            po_currency=(
                po_currency
            ),
            order_date=(
                order_date
            )
        )


        line_amount = (
            quantity
            * unit_price
        ).quantize(
            MONEY_PRECISION,
            rounding=ROUND_HALF_UP
        )


        po_line_number = (
            line_sequence * 10
        )


        purchase_order_item_rows.append(
            {
                "POItemID": (
                    f"{po_id}-"
                    f"{po_line_number:05d}"
                ),

                "POID": (
                    po_id
                ),

                "POLineNumber": (
                    po_line_number
                ),

                "MaterialID": (
                    material_id
                ),

                "CategoryID": (
                    category_id
                ),

                "ContractID": (
                    selected_contract[
                        "ContractID"
                    ]
                    if selected_contract
                    else None
                ),

                "Quantity": (
                    quantity
                ),

                "OrderUnit": (
                    unit_of_measure
                ),

                "UnitPrice": (
                    unit_price
                ),

                "LineAmount": (
                    line_amount
                ),

                "Currency": (
                    po_currency
                ),

                "RequestedDeliveryDate": (
                    generate_requested_delivery_date(
                        order_date,
                        procurement_type
                    )
                ),

                "POItemStatus": (
                    derive_po_item_status(
                        po_status
                    )
                ),

                "SimulationContractScenario": (
                    contract_scenario
                ),

                "SimulationPriceAnomalyFlag": (
                    anomaly_injected
                )
            }
        )


print(
    f"Prepared "
    f"{len(purchase_order_item_rows):,} "
    f"purchase-order items."
)


print()
print(
    "Scenario selection reasons:"
)

for (
    reason,
    count
) in sorted(
    scenario_selection_reason_counts.items()
):
    print(
        f"  {reason}: "
        f"{count:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark DataFrame
purchase_order_item_schema = StructType([
    StructField(
        "POItemID",
        StringType(),
        False
    ),
    StructField(
        "POID",
        StringType(),
        False
    ),
    StructField(
        "POLineNumber",
        IntegerType(),
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
        "Quantity",
        DecimalType(18, 3),
        False
    ),
    StructField(
        "OrderUnit",
        StringType(),
        False
    ),
    StructField(
        "UnitPrice",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "LineAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "RequestedDeliveryDate",
        DateType(),
        False
    ),
    StructField(
        "POItemStatus",
        StringType(),
        False
    ),
    StructField(
        "SimulationContractScenario",
        StringType(),
        False
    ),
    StructField(
        "SimulationPriceAnomalyFlag",
        BooleanType(),
        False
    )
])

purchase_order_item_df = (
    spark.createDataFrame(
        purchase_order_item_rows,
        schema=purchase_order_item_schema
    )
)

display(
    purchase_order_item_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add Audit Columns and Record Hash
purchase_order_item_df = (
    purchase_order_item_df
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
                F.col("POItemID"),
                F.col("POID"),
                F.col("MaterialID"),
                F.col("CategoryID"),
                F.coalesce(
                    F.col("ContractID"),
                    F.lit("NO_CONTRACT")
                ),
                F.col("Quantity").cast(
                    "string"
                ),
                F.col("UnitPrice").cast(
                    "string"
                ),
                F.col("LineAmount").cast(
                    "string"
                )
            ),
            256
        )
    )
)

display(
    purchase_order_item_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Row Structure
actual_item_count = (
    purchase_order_item_df.count()
)

distinct_po_count = (
    purchase_order_item_df
    .select("POID")
    .distinct()
    .count()
)

duplicate_item_id_count = (
    purchase_order_item_df
    .groupBy("POItemID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

duplicate_po_line_count = (
    purchase_order_item_df
    .groupBy(
        "POID",
        "POLineNumber"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    purchase_order_item_df
    .filter(
        F.col("POItemID").isNull()
        | F.col("POID").isNull()
        | F.col("MaterialID").isNull()
        | F.col("CategoryID").isNull()
        | F.col("Quantity").isNull()
        | F.col("UnitPrice").isNull()
        | F.col("LineAmount").isNull()
        | F.col("Currency").isNull()
    )
    .count()
)

invalid_amount_count = (
    purchase_order_item_df
    .filter(
        (F.col("Quantity") <= 0)
        | (F.col("UnitPrice") <= 0)
        | (F.col("LineAmount") <= 0)
    )
    .count()
)

assert actual_item_count > EXPECTED_PO_COUNT
assert distinct_po_count == EXPECTED_PO_COUNT
assert duplicate_item_id_count == 0
assert duplicate_po_line_count == 0
assert mandatory_null_count == 0
assert invalid_amount_count == 0

average_lines_per_po = (
    actual_item_count
    / distinct_po_count
)

print("PO-item structural validation passed.")
print(f"PO-item rows: {actual_item_count:,}")
print(f"Distinct POs: {distinct_po_count:,}")
print(
    f"Average lines per PO: "
    f"{average_lines_per_po:.2f}"
)
print(
    f"Duplicate POItemIDs: "
    f"{duplicate_item_id_count}"
)
print(
    f"Duplicate PO-line keys: "
    f"{duplicate_po_line_count}"
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

#Validate Foreign Keys
invalid_po_count = (
    purchase_order_item_df.alias("item")
    .join(
        po_header_reference_df.alias("header"),
        F.col("item.POID")
        == F.col("header.POID"),
        "left_anti"
    )
    .count()
)

invalid_material_count = (
    purchase_order_item_df.alias("item")
    .join(
        material_reference_df.alias("material"),
        F.col("item.MaterialID")
        == F.col("material.MaterialID"),
        "left_anti"
    )
    .count()
)

invalid_category_count = (
    purchase_order_item_df.alias("item")
    .join(
        category_reference_df.alias("category"),
        F.col("item.CategoryID")
        == F.col("category.CategoryID"),
        "left_anti"
    )
    .count()
)

invalid_contract_count = (
    purchase_order_item_df
    .filter(
        F.col("ContractID").isNotNull()
    )
    .alias("item")
    .join(
        contract_reference_df.alias("contract"),
        F.col("item.ContractID")
        == F.col("contract.ContractID"),
        "left_anti"
    )
    .count()
)

assert invalid_po_count == 0
assert invalid_material_count == 0
assert invalid_category_count == 0
assert invalid_contract_count == 0

print("PO-item foreign-key validation passed.")
print(f"Invalid POs: {invalid_po_count}")
print(
    f"Invalid materials: "
    f"{invalid_material_count}"
)
print(
    f"Invalid categories: "
    f"{invalid_category_count}"
)
print(
    f"Invalid contracts: "
    f"{invalid_contract_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Material-Category Consistency
material_category_mismatch_count = (
    purchase_order_item_df.alias("item")
    .join(
        material_reference_df.alias(
            "material"
        ),
        F.col("item.MaterialID")
        == F.col("material.MaterialID"),
        "inner"
    )
    .filter(
        F.col("item.CategoryID")
        != F.col("material.CategoryID")
    )
    .count()
)

assert material_category_mismatch_count == 0

print(
    "Material-category consistency "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate PO Currency
po_currency_mismatch_count = (
    purchase_order_item_df.alias("item")
    .join(
        po_header_reference_df.alias(
            "header"
        ),
        F.col("item.POID")
        == F.col("header.POID"),
        "inner"
    )
    .filter(
        F.col("item.Currency")
        != F.col("header.Currency")
    )
    .count()
)

assert po_currency_mismatch_count == 0

print(
    "PO header-to-item currency "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Simulation Scenarios
invalid_no_contract_scenario_count = (
    purchase_order_item_df
    .filter(
        (
            F.col(
                "SimulationContractScenario"
            )
            == "NO_CONTRACT_REFERENCE"
        )
        & F.col("ContractID").isNotNull()
    )
    .count()
)

assert (
    invalid_no_contract_scenario_count
    == 0
)

print(
    "No-contract scenario "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Compliant COntract
compliant_contract_validation_df = (
    purchase_order_item_df.alias("item")
    .join(
        po_header_reference_df.alias(
            "header"
        ),
        F.col("item.POID")
        == F.col("header.POID"),
        "inner"
    )
    .join(
        contract_reference_df.alias(
            "contract"
        ),
        F.col("item.ContractID")
        == F.col("contract.ContractID"),
        "inner"
    )
)

invalid_compliant_contract_count = (
    compliant_contract_validation_df
    .filter(
        F.col(
            "item.SimulationContractScenario"
        )
        == "COMPLIANT_CONTRACT"
    )
    .filter(
        (F.col("header.SupplierID")
         != F.col("contract.SupplierID"))
        |
        (F.col("item.CategoryID")
         != F.col("contract.CategoryID"))
        |
        (
            F.col("header.OrderDate")
            < F.col(
                "contract.ContractStartDate"
            )
        )
        |
        (
            F.col("header.OrderDate")
            > F.col(
                "contract.ContractEndDate"
            )
        )
    )
    .count()
)

assert invalid_compliant_contract_count == 0

print(
    "Compliant-contract validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Invalid-date contracts
invalid_date_scenario_error_count = (
    compliant_contract_validation_df
    .filter(
        F.col(
            "item.SimulationContractScenario"
        )
        == "INVALID_DATE_CONTRACT"
    )
    .filter(
        (F.col("header.SupplierID")
         != F.col("contract.SupplierID"))
        |
        (F.col("item.CategoryID")
         != F.col("contract.CategoryID"))
        |
        (
            (
                F.col("header.OrderDate")
                >= F.col(
                    "contract.ContractStartDate"
                )
            )
            &
            (
                F.col("header.OrderDate")
                <= F.col(
                    "contract.ContractEndDate"
                )
            )
        )
    )
    .count()
)

assert (
    invalid_date_scenario_error_count
    == 0
)

print(
    "Invalid-date contract scenario "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Prepare EUR-Normalized Diagnostic Dataset
# ============================================================

po_header_date_df = (
    po_header_reference_df
    .select(
        "POID",
        "OrderDate"
    )
    .distinct()
)


diagnostic_fx_df = (
    exchange_rate_reference_df
    .select(
        F.col(
            "RateDate"
        ).alias(
            "DiagnosticFXDate"
        ),

        F.col(
            "Currency"
        ).alias(
            "DiagnosticFXCurrency"
        ),

        F.col(
            "ExchangeRateEUR"
        ).alias(
            "DiagnosticFXRateToEUR"
        )
    )
)


print(
    "PO-header and FX diagnostic "
    "references prepared."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Convert PO-Item Spend to EUR for Diagnostics
# ============================================================

po_item_eur_diagnostic_df = (
    purchase_order_item_df
    .alias("item")

    .join(
        po_header_date_df
        .alias("header"),
        F.col("item.POID")
        ==
        F.col("header.POID"),
        "left"
    )

    .join(
        diagnostic_fx_df
        .alias("fx"),
        (
            F.col(
                "header.OrderDate"
            )
            ==
            F.col(
                "fx.DiagnosticFXDate"
            )
        )
        &
        (
            F.col(
                "item.Currency"
            )
            ==
            F.col(
                "fx.DiagnosticFXCurrency"
            )
        ),
        "left"
    )

    .select(
        F.col("item.*"),

        F.col(
            "header.OrderDate"
        ).alias(
            "DiagnosticOrderDate"
        ),

        F.col(
            "fx.DiagnosticFXRateToEUR"
        )
    )

    .withColumn(
        "DiagnosticLineAmountEUR",
        F.round(
            F.col(
                "LineAmount"
            )
            *
            F.col(
                "DiagnosticFXRateToEUR"
            ),
            2
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Diagnostic FX Coverage

missing_diagnostic_fx_count = (
    po_item_eur_diagnostic_df
    .filter(
        F.col(
            "DiagnosticFXRateToEUR"
        ).isNull()
    )
    .count()
)


assert (
    missing_diagnostic_fx_count
    == 0
), (
    f"Missing FX rate for "
    f"{missing_diagnostic_fx_count:,} "
    f"PO items."
)


null_eur_amount_count = (
    po_item_eur_diagnostic_df
    .filter(
        F.col(
            "DiagnosticLineAmountEUR"
        ).isNull()
    )
    .count()
)


assert (
    null_eur_amount_count == 0
), (
    f"Found "
    f"{null_eur_amount_count:,} "
    f"null EUR line amounts."
)


print(
    "EUR diagnostic conversion passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scenario_summary_df = (
    po_item_eur_diagnostic_df

    .groupBy(
        "SimulationContractScenario"
    )

    .agg(
        F.count("*").alias(
            "POItemCount"
        ),

        F.round(
            F.sum(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "SpendEUR"
        ),

        F.round(
            F.avg(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "AverageLineAmountEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total_po_item_count = (
    po_item_eur_diagnostic_df
    .count()
)


total_spend_eur = (
    po_item_eur_diagnostic_df
    .agg(
        F.sum(
            "DiagnosticLineAmountEUR"
        ).alias(
            "TotalSpendEUR"
        )
    )
    .first()[
        "TotalSpendEUR"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scenario_summary_df = (
    scenario_summary_df

    .withColumn(
        "POItemPct",
        F.round(
            (
                F.col("POItemCount")
                * F.lit(100.0)
            )
            /
            F.lit(
                float(total_po_item_count)
            ),
            2
        )
    )

    .withColumn(
        "SpendPct",
        F.round(
            (
                F.col("SpendEUR")
                * F.lit(100.0)
            )
            /
            F.lit(
                float(total_spend_eur)
            ),
            2
        )
    )

    .orderBy(
        F.desc("SpendEUR")
    )
)

display(
    scenario_summary_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scenario_spend_statistics_df = (
    po_item_eur_diagnostic_df

    .withColumn(
        "_LineAmountEURDouble",
        F.col(
            "DiagnosticLineAmountEUR"
        ).cast("double")
    )

    .groupBy(
        "SimulationContractScenario"
    )

    .agg(
        F.count("*").alias(
            "POItemCount"
        ),

        F.round(
            F.avg(
                "_LineAmountEURDouble"
            ),
            2
        ).alias(
            "AverageLineAmountEUR"
        ),

        F.round(
            F.percentile_approx(
                "_LineAmountEURDouble",
                0.50,
                10000
            ),
            2
        ).alias(
            "MedianLineAmountEUR"
        ),

        F.round(
            F.percentile_approx(
                "_LineAmountEURDouble",
                0.90,
                10000
            ),
            2
        ).alias(
            "P90LineAmountEUR"
        ),

        F.round(
            F.percentile_approx(
                "_LineAmountEURDouble",
                0.99,
                10000
            ),
            2
        ).alias(
            "P99LineAmountEUR"
        ),

        F.round(
            F.max(
                "_LineAmountEURDouble"
            ),
            2
        ).alias(
            "MaximumLineAmountEUR"
        )
    )

    .orderBy(
        "SimulationContractScenario"
    )
)

display(
    scenario_spend_statistics_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Scenario / Category Spend Diagnostic in EUR
# ============================================================

display(
    po_item_eur_diagnostic_df

    .groupBy(
        "SimulationContractScenario",
        "CategoryID"
    )

    .agg(
        F.count("*").alias(
            "POItemCount"
        ),

        F.round(
            F.sum(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "SpendEUR"
        ),

        F.round(
            F.avg(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "AverageLineAmountEUR"
        ),

        F.round(
            F.max(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "MaximumLineAmountEUR"
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

# CELL ********************

# ============================================================
# Validate and Inspect Price Anomalies
# ============================================================

null_anomaly_flag_count = (
    po_item_eur_diagnostic_df
    .filter(
        F.col(
            "SimulationPriceAnomalyFlag"
        ).isNull()
    )
    .count()
)

print(
    "Null price anomaly flags:",
    null_anomaly_flag_count
)

assert (
    null_anomaly_flag_count == 0
), (
    f"Found "
    f"{null_anomaly_flag_count:,} "
    f"null price-anomaly flags."
)


price_anomaly_summary_df = (
    po_item_eur_diagnostic_df

    .groupBy(
        "SimulationPriceAnomalyFlag"
    )

    .agg(
        F.count("*").alias(
            "POItemCount"
        ),

        F.round(
            F.sum(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "SpendEUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

price_anomaly_summary_df = (
    price_anomaly_summary_df

    .withColumn(
        "POItemPct",
        F.round(
            (
                F.col("POItemCount")
                * F.lit(100.0)
            )
            /
            F.lit(
                float(total_po_item_count)
            ),
            2
        )
    )

    .orderBy(
        F.desc(
            "SimulationPriceAnomalyFlag"
        )
    )
)

display(
    price_anomaly_summary_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actual_anomaly_count = (
    po_item_eur_diagnostic_df
    .filter(
        F.col(
            "SimulationPriceAnomalyFlag"
        )
    )
    .count()
)


actual_anomaly_rate = (
    actual_anomaly_count
    /
    total_po_item_count
)


print(
    f"Injected price anomalies: "
    f"{actual_anomaly_count:,}"
)

print(
    f"Actual anomaly rate: "
    f"{actual_anomaly_rate:.2%}"
)


assert (
    0.015
    <= actual_anomaly_rate
    <= 0.025
), (
    f"Unexpected anomaly rate: "
    f"{actual_anomaly_rate:.2%}"
)


print(
    "Price-anomaly distribution "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Final Generated-Distribution Sanity Check
# ============================================================

scenario_check_rows = {
    row[
        "SimulationContractScenario"
    ]: row.asDict()
    for row
    in scenario_summary_df.collect()
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for (
    scenario,
    result
) in scenario_check_rows.items():

    print(
        scenario
    )

    print(
        "  Item share:",
        f"{result['POItemPct']:.2f}%"
    )

    print(
        "  Spend share:",
        f"{result['SpendPct']:.2f}%"
    )

    print(
        "  Spend EUR:",
        f"{result['SpendEUR']:,.2f}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

compliant_item_pct = (
    scenario_check_rows[
        "COMPLIANT_CONTRACT"
    ][
        "POItemPct"
    ]
)

invalid_item_pct = (
    scenario_check_rows[
        "INVALID_DATE_CONTRACT"
    ][
        "POItemPct"
    ]
)

no_contract_item_pct = (
    scenario_check_rows[
        "NO_CONTRACT_REFERENCE"
    ][
        "POItemPct"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Validate Scenario Distribution
# ============================================================

print(
    "Generated scenario distribution:"
)

print(
    f"  Compliant: "
    f"{compliant_item_pct:.2f}%"
)

print(
    f"  Invalid date: "
    f"{invalid_item_pct:.2f}%"
)

print(
    f"  No contract: "
    f"{no_contract_item_pct:.2f}%"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert (
    55.0
    <= compliant_item_pct
    <= 80.0
), (
    f"Unexpected compliant item share: "
    f"{compliant_item_pct:.2f}%"
)


assert (
    5.0
    <= invalid_item_pct
    <= 25.0
), (
    f"Unexpected invalid-date item share: "
    f"{invalid_item_pct:.2f}%"
)


assert (
    10.0
    <= no_contract_item_pct
    <= 30.0
), (
    f"Unexpected no-contract item share: "
    f"{no_contract_item_pct:.2f}%"
)


scenario_percentage_total = (
    compliant_item_pct
    +
    invalid_item_pct
    +
    no_contract_item_pct
)


assert (
    abs(
        scenario_percentage_total
        - 100.0
    )
    <= 0.05
), (
    f"Scenario percentages total "
    f"{scenario_percentage_total:.2f}% "
    f"instead of 100%."
)


print(
    "Scenario-distribution quality "
    "gate passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write Bronze_purchase_order_item
TARGET_PO_ITEM_TABLE = (
    "bronze_purchase_order_item"
)

(
    purchase_order_item_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_PO_ITEM_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_PO_ITEM_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read back the table
saved_po_item_df = spark.table(
    "bronze_purchase_order_item"
)

persisted_item_count = (
    saved_po_item_df.count()
)

assert (
    persisted_item_count
    == actual_item_count
)

print(
    f"Persisted PO-item rows: "
    f"{persisted_item_count:,}"
)

print(
    "Persisted PO-item validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calculate PO Totals
po_total_df = (
    saved_po_item_df
    .groupBy("POID")
    .agg(
        F.sum("LineAmount").alias(
            "CalculatedTotalAmount"
        )
    )
    .withColumn(
        "CalculatedTotalAmount",
        F.col(
            "CalculatedTotalAmount"
        ).cast("decimal(18,2)")
    )
)

calculated_po_count = (
    po_total_df.count()
)

assert (
    calculated_po_count
    == EXPECTED_PO_COUNT
)

display(
    po_total_df.limit(50)
)

print(
    f"Calculated totals for "
    f"{calculated_po_count:,} POs."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Update PO-header totals with Delta MERGE
po_header_delta_table = (
    DeltaTable.forName(
        spark,
        "bronze_purchase_order_header"
    )
)

(
    po_header_delta_table.alias("target")
    .merge(
        po_total_df.alias("source"),
        (
            "target.POID "
            "= source.POID"
        )
    )
    .whenMatchedUpdate(
        set={
            "TotalAmount": (
                "source."
                "CalculatedTotalAmount"
            ),
            "AmountReconciliationStatus": (
                "'RECONCILED'"
            )
        }
    )
    .execute()
)

print(
    "PO-header totals updated successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Header Reconciliation

reconciled_po_header_df = (
    spark.table(
        "bronze_purchase_order_header"
    )
    .select(
        "POID",
        "TotalAmount",
        "AmountReconciliationStatus"
    )
    .alias("header")
    .join(
        po_total_df.alias("calculated"),
        F.col(
            "header.POID"
        )
        ==
        F.col(
            "calculated.POID"
        ),
        "inner"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

po_reconciliation_error_count = (
    reconciled_po_header_df
    .filter(
        (
            F.abs(
                F.col(
                    "header.TotalAmount"
                )
                -
                F.col(
                    "calculated.CalculatedTotalAmount"
                )
            )
            > F.lit(0.01)
        )
        |
        (
            F.col(
                "header.AmountReconciliationStatus"
            )
            != "RECONCILED"
        )
    )
    .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert (
    po_reconciliation_error_count
    == 0
), (
    f"Found "
    f"{po_reconciliation_error_count} "
    f"PO-header reconciliation errors."
)

print(
    "PO-header reconciliation "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Final Summary
# ============================================================

final_summary_df = (
    po_item_eur_diagnostic_df

    .agg(
        F.count("*").alias(
            "PurchaseOrderItemCount"
        ),

        F.countDistinct(
            "POID"
        ).alias(
            "PurchaseOrderCount"
        ),

        F.round(
            F.avg(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "AverageLineAmountEUR"
        ),

        F.round(
            F.sum(
                "DiagnosticLineAmountEUR"
            ),
            2
        ).alias(
            "TotalPurchaseOrderSpendEUR"
        ),

        F.sum(
            F.when(
                F.col(
                    "ContractID"
                ).isNull(),
                1
            ).otherwise(
                0
            )
        ).alias(
            "ItemsWithoutContract"
        ),

        F.sum(
            F.col(
                "SimulationPriceAnomalyFlag"
            ).cast("int")
        ).alias(
            "InjectedPriceAnomalies"
        )
    )
)


display(
    final_summary_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
