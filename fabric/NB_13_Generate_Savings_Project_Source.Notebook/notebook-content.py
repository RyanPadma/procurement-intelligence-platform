# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6900e8cc-2b9a-400f-9c08-f940b37aed8e",
# META       "default_lakehouse_name": "lh_procurement_analytics",
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
from datetime import date, timedelta

PROFILE = "development"

PROFILE_VOLUMES = {
    "development": {
        "savings_projects": 3_000
    },
    "portfolio": {
        "savings_projects": 15_000
    }
}

if PROFILE not in PROFILE_VOLUMES:
    raise ValueError(
        f"Invalid profile: {PROFILE}"
    )

SAVINGS_PROJECT_COUNT = (
    PROFILE_VOLUMES[PROFILE][
        "savings_projects"
    ]
)

START_DATE = date(2022, 1, 1)
AS_OF_DATE = date(2026, 7, 31)

RANDOM_SEED = 20260802

print(f"Profile: {PROFILE}")
print(
    f"Savings projects: "
    f"{SAVINGS_PROJECT_COUNT:,}"
)
print(
    f"Source period: "
    f"{START_DATE} to {AS_OF_DATE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import 
import math
import random
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType,
    BooleanType
)

random.seed(RANDOM_SEED)

MONEY_PRECISION = Decimal("0.01")

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read reference table
supplier_reference_df = (
    spark.table("bronze_supplier")
    .select(
        "SupplierID",
        "SupplierName",
        "SupplierType",
        "Status"
    )
)

category_reference_df = (
    spark.table("bronze_category")
    .select(
        "CategoryID",
        "CategoryName",
        "ProcurementType"
    )
)

buyer_reference_df = (
    spark.table("bronze_buyer")
    .select(
        "BuyerID",
        "BuyerName",
        "BuyerRole",
        "BusinessUnitID"
    )
)

business_unit_reference_df = (
    spark.table("bronze_business_unit")
    .select(
        "BusinessUnitID"
    )
)

contract_reference_df = (
    spark.table("bronze_contract")
    .select(
        "ContractID",
        "SupplierID",
        "CategoryID",
        "Currency",
        "ContractStatus"
    )
)

currency_reference_df = (
    spark.table("bronze_exchange_rate")
    .select("Currency")
    .distinct()
)

print(
    "Suppliers:",
    supplier_reference_df.count()
)

print(
    "Categories:",
    category_reference_df.count()
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
    "Contracts:",
    contract_reference_df.count()
)

print(
    "Currencies:",
    currency_reference_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#BUild source-system reference collections
active_supplier_records = [
    row.asDict()
    for row in (
        supplier_reference_df
        .filter(
            F.col("Status") == "Active"
        )
        .collect()
    )
]

category_records = [
    row.asDict()
    for row in category_reference_df.collect()
]

buyer_records = [
    row.asDict()
    for row in buyer_reference_df.collect()
]

contract_records = [
    row.asDict()
    for row in contract_reference_df.collect()
]

currency_values = [
    row["Currency"]
    for row in currency_reference_df.collect()
]

assert active_supplier_records
assert category_records
assert buyer_records
assert contract_records
assert currency_values

supplier_lookup = {
    supplier["SupplierID"]: supplier
    for supplier in active_supplier_records
}

category_lookup = {
    category["CategoryID"]: category
    for category in category_records
}

contracts_by_supplier = defaultdict(list)

for contract in contract_records:
    if (
        contract["SupplierID"]
        in supplier_lookup
    ):
        contracts_by_supplier[
            contract["SupplierID"]
        ].append(contract)

print(
    f"Active suppliers: "
    f"{len(active_supplier_records):,}"
)

print(
    f"Categories: "
    f"{len(category_records):,}"
)

print(
    f"Buyers: "
    f"{len(buyer_records):,}"
)

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

supplier_types = {
    supplier["SupplierType"]
    for supplier in active_supplier_records
}

missing_supplier_types = (
    supplier_types
    - set(
        SUPPLIER_CATEGORY_ELIGIBILITY
    )
)

assert not missing_supplier_types, (
    f"Missing category rules for: "
    f"{missing_supplier_types}"
)

print(
    "Supplier-category rules validated."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Source Application Values
PROJECT_STATUS_CONFIG = {
    "Idea": {
        "SavingsLevel": "L0",
        "ApprovalStatus": "Draft"
    },
    "Validated": {
        "SavingsLevel": "L1",
        "ApprovalStatus": "Under Review"
    },
    "Negotiation": {
        "SavingsLevel": "L2",
        "ApprovalStatus": "Approved"
    },
    "Implemented": {
        "SavingsLevel": "L3",
        "ApprovalStatus": "Approved"
    },
    "Cancelled": {
        "SavingsLevel": "L0",
        "ApprovalStatus": "Rejected"
    }
}

SAVINGS_TYPES = [
    "Price Reduction",
    "Supplier Consolidation",
    "Demand Management",
    "Specification Change",
    "Process Improvement",
    "Payment Terms Improvement",
    "Logistics Optimization",
    "Make-or-Buy Optimization"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Helper functions
def quantize_money(value):
    return Decimal(
        str(value)
    ).quantize(
        MONEY_PRECISION,
        rounding=ROUND_HALF_UP
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_random_date(
    start_date,
    end_date
):
    available_days = (
        end_date - start_date
    ).days

    return (
        start_date
        + timedelta(
            days=random.randint(
                0,
                available_days
            )
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_baseline_spend(
    procurement_type
):
    ranges = {
        "Direct": (
            100_000,
            5_000_000
        ),
        "Indirect": (
            25_000,
            2_000_000
        ),
        "Capital": (
            250_000,
            10_000_000
        )
    }

    minimum_value, maximum_value = (
        ranges[procurement_type]
    )

    generated_value = math.exp(
        random.uniform(
            math.log(minimum_value),
            math.log(maximum_value)
        )
    )

    return quantize_money(
        generated_value
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_forecasted_savings(
    baseline_spend,
    procurement_type
):
    savings_rate_ranges = {
        "Direct": (
            0.015,
            0.120
        ),
        "Indirect": (
            0.025,
            0.180
        ),
        "Capital": (
            0.010,
            0.100
        )
    }

    minimum_rate, maximum_rate = (
        savings_rate_ranges[
            procurement_type
        ]
    )

    savings_rate = random.uniform(
        minimum_rate,
        maximum_rate
    )

    return quantize_money(
        baseline_spend
        * Decimal(
            str(savings_rate)
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select Project Relationships
def select_reporting_currency():
    available_weights = {
        "EUR": 60,
        "USD": 20,
        "GBP": 5,
        "CHF": 3,
        "PLN": 4,
        "JPY": 2,
        "CNY": 4,
        "SEK": 2
    }

    currencies = [
        currency
        for currency in currency_values
        if currency in available_weights
    ]

    weights = [
        available_weights[currency]
        for currency in currencies
    ]

    return random.choices(
        population=currencies,
        weights=weights,
        k=1
    )[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def select_project_relationships():
    supplier = random.choice(
        active_supplier_records
    )

    supplier_contracts = (
        contracts_by_supplier.get(
            supplier["SupplierID"],
            []
        )
    )

    use_contract = (
        bool(supplier_contracts)
        and random.random() < 0.60
    )

    if use_contract:
        contract = random.choice(
            supplier_contracts
        )

        category = category_lookup[
            contract["CategoryID"]
        ]

        return {
            "Supplier": supplier,
            "Category": category,
            "Contract": contract,
            "Currency": contract["Currency"]
        }

    eligible_categories = [
        category_id
        for category_id in (
            SUPPLIER_CATEGORY_ELIGIBILITY[
                supplier["SupplierType"]
            ]
        )
        if category_id in category_lookup
    ]

    category_id = random.choice(
        eligible_categories
    )

    return {
        "Supplier": supplier,
        "Category": (
            category_lookup[category_id]
        ),
        "Contract": None,
        "Currency": (
            select_reporting_currency()
        )
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate the savings project source records
savings_project_rows = []

for project_number in range(
    1,
    SAVINGS_PROJECT_COUNT + 1
):
    relationships = (
        select_project_relationships()
    )

    supplier = relationships[
        "Supplier"
    ]

    category = relationships[
        "Category"
    ]

    contract = relationships[
        "Contract"
    ]

    buyer = random.choice(
        buyer_records
    )

    project_status = random.choices(
        population=[
            "Idea",
            "Validated",
            "Negotiation",
            "Implemented",
            "Cancelled"
        ],
        weights=[
            15,
            20,
            25,
            35,
            5
        ],
        k=1
    )[0]

    if project_status == "Implemented":
        latest_created_date = (
            AS_OF_DATE
            - timedelta(days=120)
        )
    else:
        latest_created_date = (
            AS_OF_DATE
        )

    project_created_date = (
        generate_random_date(
            START_DATE,
            latest_created_date
        )
    )

    planned_start_date = (
        project_created_date
        + timedelta(
            days=random.randint(
                0,
                60
            )
        )
    )

    planned_completion_date = (
        planned_start_date
        + timedelta(
            days=random.randint(
                90,
                540
            )
        )
    )

    actual_completion_date = None
    cancellation_date = None

    if project_status == "Implemented":
        latest_completion_date = min(
            AS_OF_DATE,
            planned_completion_date
            + timedelta(days=90)
        )

        latest_completion_date = max(
            planned_start_date,
            latest_completion_date
        )

        actual_completion_date = (
            generate_random_date(
                planned_start_date,
                latest_completion_date
            )
        )

    elif project_status == "Cancelled":
        cancellation_date = (
            generate_random_date(
                project_created_date,
                AS_OF_DATE
            )
        )

    baseline_spend = (
        generate_baseline_spend(
            category["ProcurementType"]
        )
    )

    forecasted_savings = (
        generate_forecasted_savings(
            baseline_spend,
            category["ProcurementType"]
        )
    )

    if project_status in [
        "Idea",
        "Validated",
        "Cancelled"
    ]:
        approved_savings = Decimal(
            "0.00"
        )

        realized_savings = Decimal(
            "0.00"
        )

    elif project_status == "Negotiation":
        approved_savings = quantize_money(
            forecasted_savings
            * Decimal(
                str(
                    random.uniform(
                        0.80,
                        1.00
                    )
                )
            )
        )

        realized_savings = Decimal(
            "0.00"
        )

    elif project_status == "Implemented":
        approved_savings = quantize_money(
            forecasted_savings
            * Decimal(
                str(
                    random.uniform(
                        0.90,
                        1.05
                    )
                )
            )
        )

        realized_savings = quantize_money(
            approved_savings
            * Decimal(
                str(
                    random.uniform(
                        0.85,
                        1.15
                    )
                )
            )
        )

    status_config = (
        PROJECT_STATUS_CONFIG[
            project_status
        ]
    )

    savings_type = random.choice(
        SAVINGS_TYPES
    )

    savings_project_rows.append({
        "SavingsProjectID": (
            f"SAV{project_number:08d}"
        ),
        "SavingsProjectName": (
            f"{savings_type} - "
            f"{category['CategoryName']} - "
            f"{supplier['SupplierName'][:30]}"
        ),
        "SupplierID": (
            supplier["SupplierID"]
        ),
        "CategoryID": (
            category["CategoryID"]
        ),
        "BuyerID": (
            buyer["BuyerID"]
        ),
        "BusinessUnitID": (
            buyer["BusinessUnitID"]
        ),
        "ContractID": (
            contract["ContractID"]
            if contract
            else None
        ),
        "SavingsType": savings_type,
        "ProjectStatus": (
            project_status
        ),
        "SavingsLevel": (
            status_config[
                "SavingsLevel"
            ]
        ),
        "ApprovalStatus": (
            status_config[
                "ApprovalStatus"
            ]
        ),
        "ProjectCreatedDate": (
            project_created_date
        ),
        "PlannedStartDate": (
            planned_start_date
        ),
        "PlannedCompletionDate": (
            planned_completion_date
        ),
        "ActualCompletionDate": (
            actual_completion_date
        ),
        "CancellationDate": (
            cancellation_date
        ),
        "Currency": (
            relationships["Currency"]
        ),
        "BaselineSpend": (
            baseline_spend
        ),
        "ForecastedSavings": (
            forecasted_savings
        ),
        "ApprovedSavings": (
            approved_savings
        ),
        "RealizedSavings": (
            realized_savings
        ),
        "RecurringSavingsFlag": (
            random.random() < 0.70
        )
    })

print(
    f"Prepared "
    f"{len(savings_project_rows):,} "
    f"savings-project records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark DataFrame
savings_project_schema = StructType([
    StructField(
        "SavingsProjectID",
        StringType(),
        False
    ),
    StructField(
        "SavingsProjectName",
        StringType(),
        False
    ),
    StructField(
        "SupplierID",
        StringType(),
        False
    ),
    StructField(
        "CategoryID",
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
        "ContractID",
        StringType(),
        True
    ),
    StructField(
        "SavingsType",
        StringType(),
        False
    ),
    StructField(
        "ProjectStatus",
        StringType(),
        False
    ),
    StructField(
        "SavingsLevel",
        StringType(),
        False
    ),
    StructField(
        "ApprovalStatus",
        StringType(),
        False
    ),
    StructField(
        "ProjectCreatedDate",
        DateType(),
        False
    ),
    StructField(
        "PlannedStartDate",
        DateType(),
        False
    ),
    StructField(
        "PlannedCompletionDate",
        DateType(),
        False
    ),
    StructField(
        "ActualCompletionDate",
        DateType(),
        True
    ),
    StructField(
        "CancellationDate",
        DateType(),
        True
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "BaselineSpend",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "ForecastedSavings",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "ApprovedSavings",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "RealizedSavings",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "RecurringSavingsFlag",
        BooleanType(),
        False
    )
])

savings_project_df = (
    spark.createDataFrame(
        savings_project_rows,
        schema=savings_project_schema
    )
)

display(
    savings_project_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add bronze audit metadata
savings_project_df = (
    savings_project_df
    .withColumn(
        "SourceSystem",
        F.lit(
            "SYNTHETIC_SAVINGS_APP"
        )
    )
    .withColumn(
        "SourceExtractDate",
        F.lit(
            AS_OF_DATE.isoformat()
        ).cast("date")
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
                F.col(
                    "SavingsProjectID"
                ),
                F.col("SupplierID"),
                F.col("CategoryID"),
                F.col("BuyerID"),
                F.coalesce(
                    F.col("ContractID"),
                    F.lit("NO_CONTRACT")
                ),
                F.col("ProjectStatus"),
                F.col(
                    "ForecastedSavings"
                ).cast("string"),
                F.col(
                    "ApprovedSavings"
                ).cast("string"),
                F.col(
                    "RealizedSavings"
                ).cast("string")
            ),
            256
        )
    )
)

display(
    savings_project_df.limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Structural Validation
actual_project_count = (
    savings_project_df.count()
)

duplicate_project_id_count = (
    savings_project_df
    .groupBy("SavingsProjectID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    savings_project_df
    .filter(
        F.col(
            "SavingsProjectID"
        ).isNull()
        |
        F.col("SupplierID").isNull()
        |
        F.col("CategoryID").isNull()
        |
        F.col("BuyerID").isNull()
        |
        F.col(
            "BusinessUnitID"
        ).isNull()
        |
        F.col(
            "ProjectStatus"
        ).isNull()
        |
        F.col(
            "ProjectCreatedDate"
        ).isNull()
        |
        F.col(
            "ForecastedSavings"
        ).isNull()
    )
    .count()
)

invalid_amount_count = (
    savings_project_df
    .filter(
        (
            F.col("BaselineSpend")
            <= F.lit(0)
        )
        |
        (
            F.col("ForecastedSavings")
            < F.lit(0)
        )
        |
        (
            F.col("ApprovedSavings")
            < F.lit(0)
        )
        |
        (
            F.col("RealizedSavings")
            < F.lit(0)
        )
        |
        (
            F.col("ForecastedSavings")
            > F.col("BaselineSpend")
        )
    )
    .count()
)

assert (
    actual_project_count
    == SAVINGS_PROJECT_COUNT
)

assert duplicate_project_id_count == 0
assert mandatory_null_count == 0
assert invalid_amount_count == 0

print(
    "Savings-project structural "
    "validation passed."
)

print(
    f"Rows: {actual_project_count:,}"
)

print(
    f"Duplicate project IDs: "
    f"{duplicate_project_id_count}"
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

#Validate dates and source status fields
invalid_date_count = (
    savings_project_df
    .filter(
        (
            F.col("PlannedStartDate")
            < F.col(
                "ProjectCreatedDate"
            )
        )
        |
        (
            F.col(
                "PlannedCompletionDate"
            )
            < F.col(
                "PlannedStartDate"
            )
        )
        |
        (
            F.col(
                "ActualCompletionDate"
            ).isNotNull()
            &
            (
                F.col(
                    "ActualCompletionDate"
                )
                < F.col(
                    "PlannedStartDate"
                )
            )
        )
        |
        (
            F.col(
                "CancellationDate"
            ).isNotNull()
            &
            (
                F.col(
                    "CancellationDate"
                )
                < F.col(
                    "ProjectCreatedDate"
                )
            )
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

invalid_status_count = (
    savings_project_df
    .filter(
        (
            (
                F.col("ProjectStatus")
                == "Implemented"
            )
            &
            (
                F.col(
                    "ActualCompletionDate"
                ).isNull()
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    <= F.lit(0)
                )
                |
                (
                    F.col("SavingsLevel")
                    != "L3"
                )
            )
        )
        |
        (
            (
                F.col("ProjectStatus")
                == "Cancelled"
            )
            &
            (
                F.col(
                    "CancellationDate"
                ).isNull()
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    != F.lit(0)
                )
            )
        )
        |
        (
            (
                ~F.col(
                    "ProjectStatus"
                ).isin(
                    "Implemented",
                    "Cancelled"
                )
            )
            &
            (
                F.col(
                    "ActualCompletionDate"
                ).isNotNull()
                |
                F.col(
                    "CancellationDate"
                ).isNotNull()
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    != F.lit(0)
                )
            )
        )
    )
    .count()
)

assert invalid_date_count == 0
assert invalid_status_count == 0

print(
    "Savings-project dates and source "
    "status validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Foreign Key Validation
invalid_supplier_count = (
    savings_project_df.alias(
        "project"
    )
    .join(
        supplier_reference_df.alias(
            "supplier"
        ),
        F.col("project.SupplierID")
        == F.col("supplier.SupplierID"),
        "left_anti"
    )
    .count()
)

invalid_category_count = (
    savings_project_df.alias(
        "project"
    )
    .join(
        category_reference_df.alias(
            "category"
        ),
        F.col("project.CategoryID")
        == F.col("category.CategoryID"),
        "left_anti"
    )
    .count()
)

invalid_buyer_count = (
    savings_project_df.alias(
        "project"
    )
    .join(
        buyer_reference_df.alias(
            "buyer"
        ),
        F.col("project.BuyerID")
        == F.col("buyer.BuyerID"),
        "left_anti"
    )
    .count()
)

invalid_business_unit_count = (
    savings_project_df.alias(
        "project"
    )
    .join(
        business_unit_reference_df.alias(
            "business_unit"
        ),
        F.col(
            "project.BusinessUnitID"
        )
        == F.col(
            "business_unit.BusinessUnitID"
        ),
        "left_anti"
    )
    .count()
)

invalid_contract_count = (
    savings_project_df
    .filter(
        F.col("ContractID").isNotNull()
    )
    .alias("project")
    .join(
        contract_reference_df.alias(
            "contract"
        ),
        F.col("project.ContractID")
        == F.col("contract.ContractID"),
        "left_anti"
    )
    .count()
)

assert invalid_supplier_count == 0
assert invalid_category_count == 0
assert invalid_buyer_count == 0
assert invalid_business_unit_count == 0
assert invalid_contract_count == 0

print(
    "Savings-project foreign-key "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Ownership and Contract Consistency
buyer_business_unit_error_count = (
    savings_project_df.alias(
        "project"
    )
    .join(
        buyer_reference_df.alias(
            "buyer"
        ),
        F.col("project.BuyerID")
        == F.col("buyer.BuyerID"),
        "inner"
    )
    .filter(
        F.col(
            "project.BusinessUnitID"
        )
        != F.col(
            "buyer.BusinessUnitID"
        )
    )
    .count()
)

contract_consistency_error_count = (
    savings_project_df
    .filter(
        F.col("ContractID").isNotNull()
    )
    .alias("project")
    .join(
        contract_reference_df.alias(
            "contract"
        ),
        F.col("project.ContractID")
        == F.col("contract.ContractID"),
        "inner"
    )
    .filter(
        (
            F.col("project.SupplierID")
            != F.col("contract.SupplierID")
        )
        |
        (
            F.col("project.CategoryID")
            != F.col("contract.CategoryID")
        )
        |
        (
            F.col("project.Currency")
            != F.col("contract.Currency")
        )
    )
    .count()
)

assert (
    buyer_business_unit_error_count
    == 0
)

assert (
    contract_consistency_error_count
    == 0
)

print(
    "Savings-project relationship "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Source Distributions
display(
    savings_project_df
    .groupBy(
        "ProjectStatus",
        "SavingsLevel",
        "ApprovalStatus"
    )
    .agg(
        F.count("*").alias(
            "ProjectCount"
        ),
        F.round(
            F.sum(
                "ForecastedSavings"
            ),
            2
        ).alias(
            "ForecastedSavings"
        ),
        F.round(
            F.sum(
                "ApprovedSavings"
            ),
            2
        ).alias(
            "ApprovedSavings"
        ),
        F.round(
            F.sum(
                "RealizedSavings"
            ),
            2
        ).alias(
            "RealizedSavings"
        )
    )
    .orderBy(
        "SavingsLevel",
        "ProjectStatus"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write the Bronze Source Table
TARGET_SAVINGS_PROJECT_TABLE = (
    "bronze_savings_project"
)

(
    savings_project_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_SAVINGS_PROJECT_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_SAVINGS_PROJECT_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read back and validate
saved_savings_project_df = (
    spark.table(
        "bronze_savings_project"
    )
)

persisted_project_count = (
    saved_savings_project_df.count()
)

persisted_distinct_project_count = (
    saved_savings_project_df
    .select(
        "SavingsProjectID"
    )
    .distinct()
    .count()
)

assert (
    persisted_project_count
    == SAVINGS_PROJECT_COUNT
)

assert (
    persisted_distinct_project_count
    == SAVINGS_PROJECT_COUNT
)

display(
    saved_savings_project_df.limit(50)
)

print(
    f"Persisted savings projects: "
    f"{persisted_project_count:,}"
)

print(
    "Persisted savings-project "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
