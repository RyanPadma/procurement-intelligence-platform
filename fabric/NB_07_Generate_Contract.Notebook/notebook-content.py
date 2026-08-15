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
        "contracts": 800
    },
    "portfolio": {
        "contracts": 8_000
    }
}

if PROFILE not in PROFILE_VOLUMES:
    raise ValueError(
        f"Invalid profile: {PROFILE}. "
        f"Choose from {list(PROFILE_VOLUMES.keys())}"
    )

CONTRACT_COUNT = PROFILE_VOLUMES[PROFILE]["contracts"]

RANDOM_SEED = 20260802

# Align this with the end of the synthetic transaction period.
AS_OF_DATE = date(2026, 7, 31)

print(f"Profile: {PROFILE}")
print(f"Contracts to generate: {CONTRACT_COUNT:,}")
print(f"Contract status date: {AS_OF_DATE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import Libraries
import math
import random
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

random.seed(RANDOM_SEED)

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read the parent tables
supplier_reference_df = (
    spark.table("bronze_supplier")
    .select(
        "SupplierID",
        "SupplierName",
        "SupplierType",
        "Country",
        "Region",
        "PreferredSupplier",
        "StrategicSupplier",
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
        "BuyerRole"
    )
)

material_reference_df = (
    spark.table("bronze_material")
    .select(
        "MaterialID",
        "CategoryID",
        "StandardCost"
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
    "Materials:",
    material_reference_df.count()
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

#Contract Reference Lists
active_supplier_records = [
    row.asDict()
    for row in (
        supplier_reference_df
        .filter(F.col("Status") == "Active")
        .collect()
    )
]

category_records = [
    row.asDict()
    for row in category_reference_df.collect()
]

contract_owner_records = [
    row.asDict()
    for row in (
        buyer_reference_df
        .filter(
            F.col("BuyerRole").isin(
                "Senior Buyer",
                "Strategic Buyer",
                "Category Manager",
                "Procurement Manager"
            )
        )
        .collect()
    )
]

currency_values = [
    row["Currency"]
    for row in currency_reference_df.collect()
]

assert active_supplier_records, (
    "No active suppliers are available."
)

assert category_records, (
    "No categories are available."
)

assert contract_owner_records, (
    "No eligible contract owners are available."
)

assert currency_values, (
    "No currencies are available."
)

print(
    f"Active suppliers: "
    f"{len(active_supplier_records):,}"
)

print(
    f"Eligible contract owners: "
    f"{len(contract_owner_records):,}"
)

print(
    f"Available currencies: "
    f"{currency_values}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calculate Category Price Benchmarks
category_benchmark_df = (
    material_reference_df
    .groupBy("CategoryID")
    .agg(
        F.percentile_approx(
            "StandardCost",
            0.5
        ).alias("CategoryMedianCost")
    )
)

category_detail_df = (
    category_reference_df
    .join(
        category_benchmark_df,
        on="CategoryID",
        how="left"
    )
)

category_detail_records = {
    row["CategoryID"]: row.asDict()
    for row in category_detail_df.collect()
}

display(category_detail_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate category benchmark
missing_benchmark_count = (
    category_detail_df
    .filter(
        F.col("CategoryMedianCost").isNull()
    )
    .count()
)

assert missing_benchmark_count == 0, (
    f"{missing_benchmark_count} categories "
    "have no material price benchmark."
)

print("Category benchmarks validated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define realistic Mapping
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Contract Types
CONTRACT_TYPES_BY_PROCUREMENT_TYPE = {
    "Direct": [
        "Framework Agreement",
        "Supply Agreement",
        "Blanket Purchase Agreement"
    ],
    "Indirect": [
        "Service Agreement",
        "Rate Card Agreement",
        "Framework Agreement"
    ],
    "Capital": [
        "Fixed Price Contract",
        "Equipment Purchase Agreement"
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Currency Preferences
def select_contract_currency(
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

    if not valid_options:
        raise ValueError(
            f"No valid currency for "
            f"{supplier_country}."
        )

    return random.choice(valid_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define Helper Functions
##Contract Status Distribution
def create_weighted_values(
    distribution,
    total_count
):
    values = []
    allocated_count = 0
    items = list(distribution.items())

    for index, (
        value,
        percentage
    ) in enumerate(items):

        if index == len(items) - 1:
            value_count = (
                total_count
                - allocated_count
            )
        else:
            value_count = round(
                total_count
                * percentage
            )

            allocated_count += (
                value_count
            )

        values.extend(
            [value] * value_count
        )

    random.shuffle(values)

    return values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_status_values = (
    create_weighted_values(
        {
            "Active": 0.60,
            "Expired": 0.35,
            "Future": 0.05
        },
        CONTRACT_COUNT
    )
)

print(
    f"Prepared {len(contract_status_values)} "
    "contract status assignments."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Contract Dates
def generate_contract_dates(
    contract_status
):
    if contract_status == "Active":
        start_date = (
            AS_OF_DATE
            - timedelta(
                days=random.randint(
                    30,
                    3 * 365
                )
            )
        )

        end_date = (
            AS_OF_DATE
            + timedelta(
                days=random.randint(
                    30,
                    2 * 365
                )
            )
        )

    elif contract_status == "Expired":
        end_date = (
            AS_OF_DATE
            - timedelta(
                days=random.randint(
                    1,
                    4 * 365
                )
            )
        )

        start_date = (
            end_date
            - timedelta(
                days=random.randint(
                    180,
                    4 * 365
                )
            )
        )

    elif contract_status == "Future":
        start_date = (
            AS_OF_DATE
            + timedelta(
                days=random.randint(
                    1,
                    365
                )
            )
        )

        end_date = (
            start_date
            + timedelta(
                days=random.randint(
                    180,
                    3 * 365
                )
            )
        )

    else:
        raise ValueError(
            f"Unknown contract status: "
            f"{contract_status}"
        )

    return start_date, end_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Contract Value
def generate_log_value(
    minimum_value,
    maximum_value
):
    generated_value = math.exp(
        random.uniform(
            math.log(minimum_value),
            math.log(maximum_value)
        )
    )

    return generated_value

def generate_contract_value(
    procurement_type,
    preferred_supplier,
    strategic_supplier
):
    value_ranges = {
        "Direct": (
            250_000,
            8_000_000
        ),
        "Indirect": (
            50_000,
            3_000_000
        ),
        "Capital": (
            500_000,
            20_000_000
        )
    }

    minimum_value, maximum_value = (
        value_ranges[procurement_type]
    )

    generated_value = generate_log_value(
        minimum_value,
        maximum_value
    )

    if strategic_supplier:
        generated_value *= random.uniform(
            1.50,
            2.50
        )

    elif preferred_supplier:
        generated_value *= random.uniform(
            1.10,
            1.50
        )

    return Decimal(
        f"{generated_value:.2f}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Negotiated Unit Price
def generate_negotiated_price(
    category_median_cost,
    preferred_supplier,
    strategic_supplier
):
    benchmark = float(
        category_median_cost
    )

    if (
        preferred_supplier
        and strategic_supplier
    ):
        price_factor = random.uniform(
            0.82,
            0.95
        )

    elif strategic_supplier:
        price_factor = random.uniform(
            0.85,
            0.98
        )

    elif preferred_supplier:
        price_factor = random.uniform(
            0.88,
            1.00
        )

    else:
        price_factor = random.uniform(
            0.95,
            1.10
        )

    negotiated_price = max(
        benchmark * price_factor,
        0.01
    )

    return Decimal(
        f"{negotiated_price:.2f}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Supplier Selection Weight
supplier_selection_weights = []

for supplier in active_supplier_records:
    weight = 1

    if supplier["PreferredSupplier"]:
        weight += 2

    if supplier["StrategicSupplier"]:
        weight += 4

    supplier_selection_weights.append(
        weight
    )

print(
    "Supplier selection weights prepared."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generate Contract Records
contract_rows = []

for index in range(
    1,
    CONTRACT_COUNT + 1
):
    supplier = random.choices(
        population=active_supplier_records,
        weights=supplier_selection_weights,
        k=1
    )[0]

    eligible_categories = (
        SUPPLIER_CATEGORY_ELIGIBILITY[
            supplier["SupplierType"]
        ]
    )

    category_id = random.choice(
        eligible_categories
    )

    category_detail = (
        category_detail_records[
            category_id
        ]
    )

    procurement_type = (
        category_detail[
            "ProcurementType"
        ]
    )

    contract_status = (
        contract_status_values[
            index - 1
        ]
    )

    (
        contract_start_date,
        contract_end_date
    ) = generate_contract_dates(
        contract_status
    )

    contract_owner = random.choice(
        contract_owner_records
    )

    currency = select_contract_currency(
        supplier_country=(
            supplier["Country"]
        ),
        supplier_region=(
            supplier["Region"]
        )
    )

    contract_rows.append(
        {
            "ContractID": (
                f"CTR{index:08d}"
            ),
            "SupplierID": (
                supplier["SupplierID"]
            ),
            "CategoryID": (
                category_id
            ),
            "ContractStartDate": (
                contract_start_date
            ),
            "ContractEndDate": (
                contract_end_date
            ),
            "Currency": currency,
            "ContractValue": (
                generate_contract_value(
                    procurement_type,
                    supplier[
                        "PreferredSupplier"
                    ],
                    supplier[
                        "StrategicSupplier"
                    ]
                )
            ),
            "NegotiatedUnitPrice": (
                generate_negotiated_price(
                    category_detail[
                        "CategoryMedianCost"
                    ],
                    supplier[
                        "PreferredSupplier"
                    ],
                    supplier[
                        "StrategicSupplier"
                    ]
                )
            ),
            "ContractOwnerBuyerID": (
                contract_owner["BuyerID"]
            ),
            "ContractOwner": (
                contract_owner["BuyerName"]
            ),
            "ContractType": random.choice(
                CONTRACT_TYPES_BY_PROCUREMENT_TYPE[
                    procurement_type
                ]
            ),
            "ContractStatus": (
                contract_status
            ),
            "PaymentTermsDays": random.choice(
                [30, 30, 30, 45, 60, 90]
            ),
            "AutoRenewalFlag": random.choices(
                [True, False],
                weights=[25, 75],
                k=1
            )[0]
        }
    )

print(
    f"Prepared "
    f"{len(contract_rows):,} "
    f"contract records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark Dataframe
contract_schema = StructType([
    StructField(
        "ContractID",
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
        "ContractStartDate",
        DateType(),
        False
    ),
    StructField(
        "ContractEndDate",
        DateType(),
        False
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "ContractValue",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "NegotiatedUnitPrice",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "ContractOwnerBuyerID",
        StringType(),
        False
    ),
    StructField(
        "ContractOwner",
        StringType(),
        False
    ),
    StructField(
        "ContractType",
        StringType(),
        False
    ),
    StructField(
        "ContractStatus",
        StringType(),
        False
    ),
    StructField(
        "PaymentTermsDays",
        IntegerType(),
        False
    ),
    StructField(
        "AutoRenewalFlag",
        BooleanType(),
        False
    )
])

contract_df = spark.createDataFrame(
    contract_rows,
    schema=contract_schema
)

display(contract_df.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add audit columns
contract_df = (
    contract_df
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
)

display(contract_df.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate the contracts
#Basic Validation
actual_contract_count = (
    contract_df.count()
)

duplicate_contract_id_count = (
    contract_df
    .groupBy("ContractID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    contract_df
    .filter(
        F.col("ContractID").isNull()
        | F.col("SupplierID").isNull()
        | F.col("CategoryID").isNull()
        | F.col(
            "ContractStartDate"
        ).isNull()
        | F.col(
            "ContractEndDate"
        ).isNull()
        | F.col("Currency").isNull()
        | F.col(
            "ContractValue"
        ).isNull()
        | F.col(
            "NegotiatedUnitPrice"
        ).isNull()
        | F.col(
            "ContractOwnerBuyerID"
        ).isNull()
    )
    .count()
)

invalid_date_count = (
    contract_df
    .filter(
        F.col("ContractStartDate")
        >= F.col("ContractEndDate")
    )
    .count()
)

invalid_value_count = (
    contract_df
    .filter(
        (F.col("ContractValue") <= 0)
        | (
            F.col("NegotiatedUnitPrice")
            <= 0
        )
    )
    .count()
)

assert actual_contract_count == CONTRACT_COUNT
assert duplicate_contract_id_count == 0
assert mandatory_null_count == 0
assert invalid_date_count == 0
assert invalid_value_count == 0

print("Basic contract validation passed.")
print(f"Rows: {actual_contract_count:,}")
print(
    f"Duplicate ContractIDs: "
    f"{duplicate_contract_id_count}"
)
print(
    f"Mandatory nulls: "
    f"{mandatory_null_count}"
)
print(
    f"Invalid dates: "
    f"{invalid_date_count}"
)
print(
    f"Invalid monetary values: "
    f"{invalid_value_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Foreign Key Validation
invalid_supplier_count = (
    contract_df.alias("contract")
    .join(
        supplier_reference_df.alias(
            "supplier"
        ),
        F.col("contract.SupplierID")
        == F.col("supplier.SupplierID"),
        "left_anti"
    )
    .count()
)

invalid_category_count = (
    contract_df.alias("contract")
    .join(
        category_reference_df.alias(
            "category"
        ),
        F.col("contract.CategoryID")
        == F.col("category.CategoryID"),
        "left_anti"
    )
    .count()
)

invalid_owner_count = (
    contract_df.alias("contract")
    .join(
        buyer_reference_df.alias("buyer"),
        F.col(
            "contract.ContractOwnerBuyerID"
        )
        == F.col("buyer.BuyerID"),
        "left_anti"
    )
    .count()
)

invalid_currency_count = (
    contract_df.alias("contract")
    .join(
        currency_reference_df.alias(
            "currency"
        ),
        F.col("contract.Currency")
        == F.col("currency.Currency"),
        "left_anti"
    )
    .count()
)

assert invalid_supplier_count == 0
assert invalid_category_count == 0
assert invalid_owner_count == 0
assert invalid_currency_count == 0

print(
    "Contract foreign-key validation passed."
)

print(
    f"Invalid suppliers: "
    f"{invalid_supplier_count}"
)

print(
    f"Invalid categories: "
    f"{invalid_category_count}"
)

print(
    f"Invalid owners: "
    f"{invalid_owner_count}"
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

#Contract Status Validation
as_of_date_column = F.lit(
    AS_OF_DATE.isoformat()
).cast("date")

invalid_status_count = (
    contract_df
    .filter(
        (
            (F.col("ContractStatus") == "Active")
            & ~(
                (F.col("ContractStartDate")
                 <= as_of_date_column)
                & (F.col("ContractEndDate")
                   >= as_of_date_column)
            )
        )
        |
        (
            (F.col("ContractStatus") == "Expired")
            & ~(
                F.col("ContractEndDate")
                < as_of_date_column
            )
        )
        |
        (
            (F.col("ContractStatus") == "Future")
            & ~(
                F.col("ContractStartDate")
                > as_of_date_column
            )
        )
    )
    .count()
)

assert invalid_status_count == 0, (
    f"Found {invalid_status_count} contracts "
    "with inconsistent status and dates."
)

print(
    "Contract status validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Distributions
display(
    contract_df
    .groupBy("ContractStatus")
    .agg(
        F.count("*").alias(
            "ContractCount"
        ),
        F.round(
            F.sum("ContractValue"),
            2
        ).alias("ContractValue")
    )
    .orderBy("ContractStatus")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect Contracts by Category
display(
    contract_df.alias("contract")
    .join(
        category_reference_df.alias(
            "category"
        ),
        F.col("contract.CategoryID")
        == F.col("category.CategoryID"),
        "inner"
    )
    .groupBy(
        "category.CategoryName",
        "category.ProcurementType"
    )
    .agg(
        F.count("*").alias(
            "ContractCount"
        ),
        F.round(
            F.sum("ContractValue"),
            2
        ).alias(
            "TotalContractValue"
        )
    )
    .orderBy(
        F.desc("TotalContractValue")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write Bronze Contract
TARGET_CONTRACT_TABLE = (
    "bronze_contract"
)

(
    contract_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_CONTRACT_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_CONTRACT_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read it back
saved_contract_df = spark.table(
    "bronze_contract"
)

persisted_contract_count = (
    saved_contract_df.count()
)

persisted_distinct_contract_count = (
    saved_contract_df
    .select("ContractID")
    .distinct()
    .count()
)

assert (
    persisted_contract_count
    == CONTRACT_COUNT
)

assert (
    persisted_distinct_contract_count
    == CONTRACT_COUNT
)

display(
    saved_contract_df.limit(50)
)

print(
    f"Persisted contract rows: "
    f"{persisted_contract_count:,}"
)

print(
    "Persisted contract validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
