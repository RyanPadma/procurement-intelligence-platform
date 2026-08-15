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

from datetime import date

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
        f"Invalid profile: {PROFILE}. "
        f"Choose from {list(PROFILE_VOLUMES.keys())}"
    )

PURCHASE_ORDER_COUNT = (
    PROFILE_VOLUMES[
        PROFILE
    ]["purchase_orders"]
)

START_DATE = date(2022, 1, 1)
END_DATE = date(2026, 7, 31)
AS_OF_DATE = date(2026, 7, 31)

RANDOM_SEED = 20260802


# ------------------------------------------------------------
# PO-header contract-coverage design
# ------------------------------------------------------------

TARGET_CONTRACT_COVERED_HEADER_RATE = 0.80

TARGET_CONTRACT_GAP_HEADER_RATE = 0.10

TARGET_GENERAL_HEADER_RATE = 0.10


assert abs(
    TARGET_CONTRACT_COVERED_HEADER_RATE
    + TARGET_CONTRACT_GAP_HEADER_RATE
    + TARGET_GENERAL_HEADER_RATE
    - 1.0
) < 0.000001


print(
    f"Profile: {PROFILE}"
)

print(
    f"Purchase orders to generate: "
    f"{PURCHASE_ORDER_COUNT:,}"
)

print(
    f"Transaction period: "
    f"{START_DATE} to {END_DATE}"
)

print()

print(
    "PO-header generation targets:"
)

print(
    f"  Contract covered: "
    f"{TARGET_CONTRACT_COVERED_HEADER_RATE:.0%}"
)

print(
    f"  Contract gap: "
    f"{TARGET_CONTRACT_GAP_HEADER_RATE:.0%}"
)

print(
    f"  General: "
    f"{TARGET_GENERAL_HEADER_RATE:.0%}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import libraries
import random
from datetime import timedelta
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType
)

random.seed(RANDOM_SEED)

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Parent Tables

supplier_reference_df = (
    spark.table(
        "bronze_supplier"
    )
    .select(
        "SupplierID",
        "Country",
        "Region",
        "PreferredSupplier",
        "StrategicSupplier",
        "Status"
    )
)

buyer_reference_df = (
    spark.table(
        "bronze_buyer"
    )
    .select(
        "BuyerID",
        "BuyerRole",
        "BusinessUnitID"
    )
)

business_unit_reference_df = (
    spark.table(
        "bronze_business_unit"
    )
    .select(
        "BusinessUnitID"
    )
)

currency_reference_df = (
    spark.table(
        "bronze_exchange_rate"
    )
    .select(
        "Currency"
    )
    .distinct()
)


# ------------------------------------------------------------
# Contract reference used only to create realistic
# supplier/date combinations for PO headers.
#
# ContractID itself remains an item-level relationship.
# ------------------------------------------------------------

contract_reference_df = (
    spark.table(
        "bronze_contract"
    )
    .select(
        "ContractID",
        "SupplierID",
        "CategoryID",
        "ContractStartDate",
        "ContractEndDate",
        "Currency"
    )
)


print(
    "Suppliers:",
    supplier_reference_df.count()
)

print(
    "Buyers:",
    buyer_reference_df.count()
)

print(
    "Business units:",
    business_unit_reference_df.count()
)

print(
    "Currencies:",
    currency_reference_df.count()
)

print(
    "Contracts:",
    contract_reference_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare Reference Records

supplier_records = [
    row.asDict()
    for row in (
        supplier_reference_df
        .filter(
            F.col("Status").isin(
                "Active",
                "Inactive",
                "Blocked"
            )
        )
        .collect()
    )
]

buyer_records = [
    row.asDict()
    for row in (
        buyer_reference_df
        .collect()
    )
]

currency_values = [
    row["Currency"]
    for row in (
        currency_reference_df
        .collect()
    )
]

contract_records = [
    row.asDict()
    for row in (
        contract_reference_df
        .collect()
    )
]


assert supplier_records, (
    "No eligible suppliers are available."
)

assert buyer_records, (
    "No buyers are available."
)

assert currency_values, (
    "No currencies are available."
)

assert contract_records, (
    "No contracts are available."
)


supplier_lookup = {
    supplier["SupplierID"]: supplier
    for supplier
    in supplier_records
}


print(
    f"Eligible suppliers: "
    f"{len(supplier_records):,}"
)

print(
    f"Available buyers: "
    f"{len(buyer_records):,}"
)

print(
    f"Contracts: "
    f"{len(contract_records):,}"
)

print(
    f"Currencies: "
    f"{currency_values}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare Supplier Weights

supplier_selection_weights = []

supplier_weight_by_id = {}


for supplier in supplier_records:

    weight = 1.0

    if supplier[
        "PreferredSupplier"
    ]:
        weight += 2.0

    if supplier[
        "StrategicSupplier"
    ]:
        weight += 4.0

    if (
        supplier["Status"]
        == "Inactive"
    ):
        weight *= 0.20

    elif (
        supplier["Status"]
        == "Blocked"
    ):
        weight *= 0.05


    supplier_selection_weights.append(
        weight
    )

    supplier_weight_by_id[
        supplier["SupplierID"]
    ] = weight


print(
    "Supplier selection weights "
    "prepared."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare Order Dates and Contract-Coverage Lookups


MONTH_WEIGHTS = {
    1: 0.90,
    2: 0.90,
    3: 1.00,
    4: 1.00,
    5: 1.05,
    6: 1.05,
    7: 0.90,
    8: 0.85,
    9: 1.05,
    10: 1.10,
    11: 1.15,
    12: 0.80
}


# ------------------------------------------------------------
# All possible transaction dates
# ------------------------------------------------------------

available_dates = []
available_date_weights = []

current_date = START_DATE

while current_date <= END_DATE:

    date_weight = (
        MONTH_WEIGHTS[
            current_date.month
        ]
    )

    if (
        current_date.weekday()
        >= 5
    ):
        date_weight *= 0.15

    available_dates.append(
        current_date
    )

    available_date_weights.append(
        date_weight
    )

    current_date += timedelta(
        days=1
    )


date_weight_lookup = {
    date_value: date_weight
    for (
        date_value,
        date_weight
    )
    in zip(
        available_dates,
        available_date_weights
    )
}


# ------------------------------------------------------------
# Contract lookup by supplier
# ------------------------------------------------------------

contracts_by_supplier = {}

for contract in contract_records:

    supplier_id = (
        contract["SupplierID"]
    )

    contracts_by_supplier.setdefault(
        supplier_id,
        []
    ).append(
        contract
    )


# ------------------------------------------------------------
# Contracts that overlap the PO generation period
# ------------------------------------------------------------

overlapping_contracts_by_supplier = {}


for (
    supplier_id,
    supplier_contracts
) in contracts_by_supplier.items():

    overlapping_contracts = [
        contract
        for contract
        in supplier_contracts

        if (
            contract[
                "ContractEndDate"
            ]
            >= START_DATE
        )
        and
        (
            contract[
                "ContractStartDate"
            ]
            <= END_DATE
        )
    ]

    if overlapping_contracts:

        overlapping_contracts_by_supplier[
            supplier_id
        ] = overlapping_contracts


# ------------------------------------------------------------
# Suppliers capable of generating guaranteed
# contract-covered PO headers
# ------------------------------------------------------------

covered_supplier_records = [
    supplier_lookup[
        supplier_id
    ]
    for supplier_id
    in overlapping_contracts_by_supplier

    if supplier_id
    in supplier_lookup
]


covered_supplier_weights = [
    supplier_weight_by_id[
        supplier["SupplierID"]
    ]
    for supplier
    in covered_supplier_records
]


assert covered_supplier_records, (
    "No suppliers have contracts "
    "overlapping the PO transaction period."
)


# ------------------------------------------------------------
# Select a weighted date within a contract window
# ------------------------------------------------------------

def select_weighted_date_between(
    start_date,
    end_date
):

    bounded_start = max(
        start_date,
        START_DATE
    )

    bounded_end = min(
        end_date,
        END_DATE
    )

    if (
        bounded_start
        > bounded_end
    ):
        raise ValueError(
            "Contract does not overlap "
            "transaction period."
        )


    start_index = (
        bounded_start
        - START_DATE
    ).days

    end_index = (
        bounded_end
        - START_DATE
    ).days


    candidate_dates = (
        available_dates[
            start_index:
            end_index + 1
        ]
    )

    candidate_weights = (
        available_date_weights[
            start_index:
            end_index + 1
        ]
    )


    return random.choices(
        population=(
            candidate_dates
        ),
        weights=(
            candidate_weights
        ),
        k=1
    )[0]


# ------------------------------------------------------------
# Find dates where a supplier has NO valid contract.
#
# Used to deliberately create historical contract gaps.
# ------------------------------------------------------------

uncovered_dates_by_supplier = {}

uncovered_date_weights_by_supplier = {}


for (
    supplier_id,
    supplier_contracts
) in contracts_by_supplier.items():

    supplier_uncovered_dates = []
    supplier_uncovered_weights = []


    for (
        candidate_date,
        candidate_weight
    ) in zip(
        available_dates,
        available_date_weights
    ):

        has_valid_contract = any(
            (
                contract[
                    "ContractStartDate"
                ]
                <= candidate_date
            )
            and
            (
                contract[
                    "ContractEndDate"
                ]
                >= candidate_date
            )
            for contract
            in supplier_contracts
        )


        if not has_valid_contract:

            supplier_uncovered_dates.append(
                candidate_date
            )

            supplier_uncovered_weights.append(
                candidate_weight
            )


    if supplier_uncovered_dates:

        uncovered_dates_by_supplier[
            supplier_id
        ] = supplier_uncovered_dates

        uncovered_date_weights_by_supplier[
            supplier_id
        ] = supplier_uncovered_weights


gap_supplier_records = [
    supplier_lookup[
        supplier_id
    ]
    for supplier_id
    in uncovered_dates_by_supplier

    if supplier_id
    in supplier_lookup
]


gap_supplier_weights = [
    supplier_weight_by_id[
        supplier["SupplierID"]
    ]
    for supplier
    in gap_supplier_records
]


assert gap_supplier_records, (
    "No suppliers can generate "
    "contract-gap PO headers."
)


print(
    f"Available transaction dates: "
    f"{len(available_dates):,}"
)

print(
    f"Suppliers with overlapping contracts: "
    f"{len(covered_supplier_records):,}"
)

print(
    f"Suppliers with contract-gap dates: "
    f"{len(gap_supplier_records):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Currency & Status Logic
def select_po_currency(
    supplier_country,
    supplier_region
):
    if supplier_country == "United Kingdom":
        options = ["GBP", "EUR"]

    elif supplier_country == "Switzerland":
        options = ["CHF", "EUR"]

    elif supplier_country == "Poland":
        options = ["PLN", "EUR"]

    elif supplier_country == "Sweden":
        options = ["SEK", "EUR"]

    elif supplier_country == "Japan":
        options = ["JPY", "USD", "EUR"]

    elif supplier_country == "China":
        options = ["CNY", "USD", "EUR"]

    elif supplier_region == "Americas":
        options = ["USD", "EUR"]

    elif supplier_region == "APAC":
        options = ["USD", "EUR"]

    else:
        options = ["EUR", "USD"]

    valid_options = [
        currency
        for currency in options
        if currency in currency_values
    ]

    return random.choice(valid_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#PO Status based on order Age
def select_po_status(order_date):
    age_days = (
        AS_OF_DATE - order_date
    ).days

    if age_days <= 30:
        return random.choices(
            population=[
                "Open",
                "Partially Received",
                "Fully Received",
                "Cancelled"
            ],
            weights=[
                55,
                25,
                15,
                5
            ],
            k=1
        )[0]

    elif age_days <= 120:
        return random.choices(
            population=[
                "Open",
                "Partially Received",
                "Fully Received",
                "Closed",
                "Cancelled"
            ],
            weights=[
                15,
                20,
                35,
                25,
                5
            ],
            k=1
        )[0]

    else:
        return random.choices(
            population=[
                "Open",
                "Partially Received",
                "Fully Received",
                "Closed",
                "Cancelled"
            ],
            weights=[
                2,
                3,
                10,
                80,
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

# Prepare Buyers and PO-Header Generation Scenarios


selected_buyers = random.choices(
    population=buyer_records,
    k=PURCHASE_ORDER_COUNT
)


contract_covered_count = int(
    round(
        PURCHASE_ORDER_COUNT
        *
        TARGET_CONTRACT_COVERED_HEADER_RATE
    )
)

contract_gap_count = int(
    round(
        PURCHASE_ORDER_COUNT
        *
        TARGET_CONTRACT_GAP_HEADER_RATE
    )
)

general_count = (
    PURCHASE_ORDER_COUNT
    - contract_covered_count
    - contract_gap_count
)


po_generation_scenarios = (
    [
        "CONTRACT_COVERED"
    ]
    * contract_covered_count

    +

    [
        "CONTRACT_GAP"
    ]
    * contract_gap_count

    +

    [
        "GENERAL"
    ]
    * general_count
)


random.shuffle(
    po_generation_scenarios
)


print(
    "PO-header generation plan:"
)

print(
    f"  CONTRACT_COVERED: "
    f"{contract_covered_count:,}"
)

print(
    f"  CONTRACT_GAP: "
    f"{contract_gap_count:,}"
)

print(
    f"  GENERAL: "
    f"{general_count:,}"
)

print(
    f"  TOTAL: "
    f"{len(po_generation_scenarios):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate PO Headers


purchase_order_header_rows = []

generation_scenario_counts = {
    "CONTRACT_COVERED": 0,
    "CONTRACT_GAP": 0,
    "GENERAL": 0
}


for index in range(
    PURCHASE_ORDER_COUNT
):

    po_number = (
        index + 1
    )

    generation_scenario = (
        po_generation_scenarios[
            index
        ]
    )

    buyer = (
        selected_buyers[
            index
        ]
    )


    # ==========================================================
    # Scenario A:
    # Guaranteed supplier/date combination with
    # at least one valid contract
    # ==========================================================

    if (
        generation_scenario
        == "CONTRACT_COVERED"
    ):

        supplier = random.choices(
            population=(
                covered_supplier_records
            ),
            weights=(
                covered_supplier_weights
            ),
            k=1
        )[0]


        supplier_id = (
            supplier["SupplierID"]
        )


        selected_contract = (
            random.choice(
                overlapping_contracts_by_supplier[
                    supplier_id
                ]
            )
        )


        order_date = (
            select_weighted_date_between(
                start_date=(
                    selected_contract[
                        "ContractStartDate"
                    ]
                ),
                end_date=(
                    selected_contract[
                        "ContractEndDate"
                    ]
                )
            )
        )


    # ==========================================================
    # Scenario B:
    # Supplier has contract history, but no contract
    # is valid on this PO date
    # ==========================================================

    elif (
        generation_scenario
        == "CONTRACT_GAP"
    ):

        supplier = random.choices(
            population=(
                gap_supplier_records
            ),
            weights=(
                gap_supplier_weights
            ),
            k=1
        )[0]


        supplier_id = (
            supplier["SupplierID"]
        )


        order_date = random.choices(
            population=(
                uncovered_dates_by_supplier[
                    supplier_id
                ]
            ),
            weights=(
                uncovered_date_weights_by_supplier[
                    supplier_id
                ]
            ),
            k=1
        )[0]


    # ==========================================================
    # Scenario C:
    # General procurement behavior.
    #
    # This can naturally land either inside or outside
    # contract coverage.
    # ==========================================================

    else:

        supplier = random.choices(
            population=(
                supplier_records
            ),
            weights=(
                supplier_selection_weights
            ),
            k=1
        )[0]


        supplier_id = (
            supplier["SupplierID"]
        )


        order_date = random.choices(
            population=(
                available_dates
            ),
            weights=(
                available_date_weights
            ),
            k=1
        )[0]


    # ----------------------------------------------------------
    # PO status
    # ----------------------------------------------------------

    po_status = (
        select_po_status(
            order_date
        )
    )


    # ----------------------------------------------------------
    # PO currency
    # ----------------------------------------------------------

    po_currency = (
        select_po_currency(
            supplier_country=(
                supplier["Country"]
            ),
            supplier_region=(
                supplier["Region"]
            )
        )
    )


    purchase_order_header_rows.append(
        {
            "POID": (
                f"{4500000000 + po_number}"
            ),

            "SupplierID": (
                supplier_id
            ),

            "BuyerID": (
                buyer["BuyerID"]
            ),

            "BusinessUnitID": (
                buyer[
                    "BusinessUnitID"
                ]
            ),

            "OrderDate": (
                order_date
            ),

            "Currency": (
                po_currency
            ),

            "POStatus": (
                po_status
            ),

            "TotalAmount": (
                Decimal("0.00")
            ),

            "AmountReconciliationStatus": (
                "PENDING_PO_ITEM_GENERATION"
            )
        }
    )


    generation_scenario_counts[
        generation_scenario
    ] += 1


print(
    f"Prepared "
    f"{len(purchase_order_header_rows):,} "
    f"purchase-order headers."
)

print()

print(
    "Generation scenario counts:"
)

for (
    scenario,
    count
) in (
    generation_scenario_counts.items()
):

    print(
        f"  {scenario}: "
        f"{count:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Generated Contract Coverage


def has_valid_contract_on_date(
    supplier_id,
    order_date
):

    supplier_contracts = (
        contracts_by_supplier.get(
            supplier_id,
            []
        )
    )

    return any(
        (
            contract[
                "ContractStartDate"
            ]
            <= order_date
        )
        and
        (
            contract[
                "ContractEndDate"
            ]
            >= order_date
        )
        for contract
        in supplier_contracts
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

covered_po_header_count = sum(
    1
    for po_header
    in purchase_order_header_rows

    if has_valid_contract_on_date(
        supplier_id=(
            po_header[
                "SupplierID"
            ]
        ),
        order_date=(
            po_header[
                "OrderDate"
            ]
        )
    )
)


covered_po_header_pct = (
    covered_po_header_count
    /
    PURCHASE_ORDER_COUNT
    * 100
)


print(
    "PO headers with at least one "
    "valid contract:",
    f"{covered_po_header_count:,}"
)

print(
    "Actual valid-contract coverage:",
    f"{covered_po_header_pct:.2f}%"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PO-Header Contract-Coverage Quality Gate


MIN_ACCEPTABLE_CONTRACT_COVERAGE = 0.78


actual_contract_coverage_rate = (
    covered_po_header_count
    /
    PURCHASE_ORDER_COUNT
)


assert (
    actual_contract_coverage_rate
    >=
    MIN_ACCEPTABLE_CONTRACT_COVERAGE
), (
    f"PO-header contract coverage "
    f"is too low: "
    f"{actual_contract_coverage_rate:.2%}. "
    f"Expected at least "
    f"{MIN_ACCEPTABLE_CONTRACT_COVERAGE:.0%}."
)


print(
    "PO-header contract-coverage "
    "quality gate passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark DataFrame
purchase_order_header_schema = StructType([
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
        "BuyerID",
        StringType(),
        False
    ),
    StructField(
        "BusinessUnitID",
        StringType(),
        False
    ),
    StructField(
        "OrderDate",
        DateType(),
        False
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "POStatus",
        StringType(),
        False
    ),
    StructField(
        "TotalAmount",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "AmountReconciliationStatus",
        StringType(),
        False
    )
])

purchase_order_header_df = (
    spark.createDataFrame(
        purchase_order_header_rows,
        schema=purchase_order_header_schema
    )
)

display(
    purchase_order_header_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add Audit Metadata & Hash
purchase_order_header_df = (
    purchase_order_header_df
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
                F.col("POID"),
                F.col("SupplierID"),
                F.col("BuyerID"),
                F.col("BusinessUnitID"),
                F.col("OrderDate").cast("string"),
                F.col("Currency"),
                F.col("POStatus")
            ),
            256
        )
    )
)

display(
    purchase_order_header_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate basic Structure
actual_po_count = (
    purchase_order_header_df.count()
)

duplicate_po_count = (
    purchase_order_header_df
    .groupBy("POID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    purchase_order_header_df
    .filter(
        F.col("POID").isNull()
        | F.col("SupplierID").isNull()
        | F.col("BuyerID").isNull()
        | F.col("BusinessUnitID").isNull()
        | F.col("OrderDate").isNull()
        | F.col("Currency").isNull()
        | F.col("POStatus").isNull()
    )
    .count()
)

invalid_order_date_count = (
    purchase_order_header_df
    .filter(
        (F.col("OrderDate")
         < F.lit(START_DATE.isoformat()).cast("date"))
        |
        (F.col("OrderDate")
         > F.lit(END_DATE.isoformat()).cast("date"))
    )
    .count()
)

assert actual_po_count == PURCHASE_ORDER_COUNT
assert duplicate_po_count == 0
assert mandatory_null_count == 0
assert invalid_order_date_count == 0

print("Basic PO-header validation passed.")
print(f"Rows: {actual_po_count:,}")
print(f"Duplicate POIDs: {duplicate_po_count}")
print(f"Mandatory nulls: {mandatory_null_count}")
print(
    f"Invalid order dates: "
    f"{invalid_order_date_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Foreign Keys
invalid_supplier_count = (
    purchase_order_header_df.alias("po")
    .join(
        supplier_reference_df.alias(
            "supplier"
        ),
        F.col("po.SupplierID")
        == F.col("supplier.SupplierID"),
        "left_anti"
    )
    .count()
)

invalid_buyer_count = (
    purchase_order_header_df.alias("po")
    .join(
        buyer_reference_df.alias("buyer"),
        F.col("po.BuyerID")
        == F.col("buyer.BuyerID"),
        "left_anti"
    )
    .count()
)

invalid_business_unit_count = (
    purchase_order_header_df.alias("po")
    .join(
        business_unit_reference_df.alias(
            "business_unit"
        ),
        F.col("po.BusinessUnitID")
        == F.col(
            "business_unit.BusinessUnitID"
        ),
        "left_anti"
    )
    .count()
)

invalid_currency_count = (
    purchase_order_header_df.alias("po")
    .join(
        currency_reference_df.alias(
            "currency"
        ),
        F.col("po.Currency")
        == F.col("currency.Currency"),
        "left_anti"
    )
    .count()
)

assert invalid_supplier_count == 0
assert invalid_buyer_count == 0
assert invalid_business_unit_count == 0
assert invalid_currency_count == 0

print("PO-header foreign-key validation passed.")
print(
    f"Invalid suppliers: "
    f"{invalid_supplier_count}"
)
print(
    f"Invalid buyers: "
    f"{invalid_buyer_count}"
)
print(
    f"Invalid business units: "
    f"{invalid_business_unit_count}"
)
print(
    f"Invalid currencies: "
    f"{invalid_currency_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate buyer and Business-unit consistency
buyer_business_unit_mismatch_count = (
    purchase_order_header_df.alias("po")
    .join(
        buyer_reference_df.alias("buyer"),
        F.col("po.BuyerID")
        == F.col("buyer.BuyerID"),
        "inner"
    )
    .filter(
        F.col("po.BusinessUnitID")
        != F.col("buyer.BusinessUnitID")
    )
    .count()
)

assert buyer_business_unit_mismatch_count == 0

print(
    "Buyer-to-business-unit "
    "consistency validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Distributions
#PO Status
display(
    purchase_order_header_df
    .groupBy("POStatus")
    .agg(
        F.count("*").alias(
            "PurchaseOrderCount"
        )
    )
    .orderBy(
        F.desc("PurchaseOrderCount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Currency
display(
    purchase_order_header_df
    .groupBy("Currency")
    .agg(
        F.count("*").alias(
            "PurchaseOrderCount"
        )
    )
    .orderBy(
        F.desc("PurchaseOrderCount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Year
display(
    purchase_order_header_df
    .withColumn(
        "OrderYear",
        F.year("OrderDate")
    )
    .groupBy("OrderYear")
    .agg(
        F.count("*").alias(
            "PurchaseOrderCount"
        )
    )
    .orderBy("OrderYear")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to the lakehouse
TARGET_PO_HEADER_TABLE = (
    "bronze_purchase_order_header"
)

(
    purchase_order_header_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_PO_HEADER_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_PO_HEADER_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read it back
saved_po_header_df = spark.table(
    "bronze_purchase_order_header"
)

persisted_po_count = (
    saved_po_header_df.count()
)

persisted_distinct_po_count = (
    saved_po_header_df
    .select("POID")
    .distinct()
    .count()
)

assert (
    persisted_po_count
    == PURCHASE_ORDER_COUNT
)

assert (
    persisted_distinct_po_count
    == PURCHASE_ORDER_COUNT
)

display(
    saved_po_header_df.limit(50)
)

print(
    f"Persisted PO-header rows: "
    f"{persisted_po_count:,}"
)

print(
    "Persisted PO-header validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
