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

# Welcome to your new notebook
# Type here in the cell editor to add code!
PROFILE = "development"

PROFILE_VOLUMES = {
    "development": {
        "suppliers": 500
    },
    "portfolio": {
        "suppliers": 3500
    }
}

if PROFILE not in PROFILE_VOLUMES:
    raise ValueError(
        f"Invalid profile: {PROFILE}. "
        f"Choose from {list(PROFILE_VOLUMES.keys())}"
    )

SUPPLIER_COUNT = PROFILE_VOLUMES[PROFILE]["suppliers"]

RANDOM_SEED = 20260802

print(f"Profile: {PROFILE}")
print(f"Suppliers to generate: {SUPPLIER_COUNT}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
from datetime import date
from itertools import product

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DateType
)

random.seed(RANDOM_SEED)

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

countries_by_region = {
    "EMEA": [
        "Netherlands",
        "Germany",
        "France",
        "United Kingdom",
        "Spain",
        "Italy",
        "Poland",
        "Czech Republic",
        "Sweden",
        "Switzerland",
        "Belgium",
        "Austria",
        "Ireland",
        "Denmark",
        "Norway",
        "Finland"
    ],
    "APAC": [
        "China",
        "Japan",
        "Singapore",
        "India",
        "Australia",
        "South Korea",
        "Malaysia",
        "Thailand",
        "Vietnam",
        "Indonesia"
    ],
    "Americas": [
        "United States",
        "Canada",
        "Mexico",
        "Brazil",
        "Argentina",
        "Chile"
    ]
}

legal_suffix_by_country = {
    "Netherlands": ["B.V.", "N.V."],
    "Germany": ["GmbH", "AG"],
    "France": ["S.A.S.", "S.A."],
    "United Kingdom": ["Ltd.", "PLC"],
    "United States": ["Inc.", "LLC"],
    "Switzerland": ["AG", "S.A."],
    "China": ["Co., Ltd."],
    "Japan": ["Co., Ltd."],
    "India": ["Pvt. Ltd."]
}

supplier_name_prefixes = [
    "Apex",
    "Atlas",
    "Aurora",
    "BluePeak",
    "Cobalt",
    "Delta",
    "Evergreen",
    "Falcon",
    "Frontier",
    "Helios",
    "Ironwood",
    "Keystone",
    "Lumina",
    "Meridian",
    "Nordic",
    "Nova",
    "Orion",
    "Pioneer",
    "Quantum",
    "Redwood",
    "Summit",
    "Terra",
    "Titan",
    "Vertex",
    "Westbridge"
]

supplier_name_industries = [
    "Industrial",
    "Manufacturing",
    "Components",
    "Logistics",
    "Engineering",
    "Technology",
    "Materials",
    "Systems",
    "Solutions",
    "Services",
    "Energy",
    "Packaging",
    "Chemicals",
    "Equipment",
    "Distribution",
    "Automation",
    "Electronics",
    "Mechanical",
    "Resources",
    "Supply"
]

supplier_name_descriptors = [
    "Global",
    "Advanced",
    "Integrated",
    "Precision",
    "Sustainable",
    "Dynamic",
    "International"
]

supplier_types = [
    "Manufacturer",
    "Distributor",
    "Service Provider",
    "Logistics Provider",
    "Utility Provider",
    "Contractor"
]

print("Supplier reference values prepared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_weighted_values(distribution, total_count):
    """
    Create a shuffled list whose values approximately follow
    the supplied percentage distribution.
    """

    values = []
    allocated_count = 0
    distribution_items = list(distribution.items())

    for index, (value, percentage) in enumerate(
        distribution_items
    ):
        if index == len(distribution_items) - 1:
            value_count = total_count - allocated_count
        else:
            value_count = round(total_count * percentage)
            allocated_count += value_count

        values.extend([value] * value_count)

    random.shuffle(values)

    return values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

region_values = create_weighted_values(
    {
        "EMEA": 0.60,
        "APAC": 0.23,
        "Americas": 0.17
    },
    SUPPLIER_COUNT
)

supplier_type_values = create_weighted_values(
    {
        "Manufacturer": 0.45,
        "Distributor": 0.20,
        "Service Provider": 0.20,
        "Logistics Provider": 0.08,
        "Utility Provider": 0.04,
        "Contractor": 0.03
    },
    SUPPLIER_COUNT
)

supplier_status_values = create_weighted_values(
    {
        "Active": 0.935,
        "Blocked": 0.03,
        "Inactive": 0.03,
        "Pending Review": 0.005
    },
    SUPPLIER_COUNT
)

esg_values = create_weighted_values(
    {
        "A": 0.19,
        "B": 0.30,
        "C": 0.27,
        "D": 0.14,
        "E": 0.05,
        None: 0.05
    },
    SUPPLIER_COUNT
)

supplier_classification_values = create_weighted_values(
    {
        "PreferredAndStrategic": 0.05,
        "PreferredOnly": 0.20,
        "StrategicOnly": 0.03,
        "Standard": 0.72
    },
    SUPPLIER_COUNT
)

risk_band_values = create_weighted_values(
    {
        "Low": 0.55,
        "Moderate": 0.30,
        "High": 0.10,
        "Critical": 0.05
    },
    SUPPLIER_COUNT
)

print("Controlled supplier distributions created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_name_combinations = list(
    product(
        supplier_name_prefixes,
        supplier_name_industries,
        supplier_name_descriptors
    )
)

random.shuffle(supplier_name_combinations)

if SUPPLIER_COUNT > len(supplier_name_combinations):
    raise ValueError(
        "There are not enough unique supplier name combinations."
    )

selected_name_combinations = (
    supplier_name_combinations[:SUPPLIER_COUNT]
)

print(
    f"Unique supplier name combinations available: "
    f"{len(selected_name_combinations)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_financial_risk_score(risk_band):
    risk_ranges = {
        "Low": (0, 30),
        "Moderate": (31, 60),
        "High": (61, 80),
        "Critical": (81, 100)
    }

    minimum_score, maximum_score = risk_ranges[risk_band]

    return random.randint(
        minimum_score,
        maximum_score
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_rows = []

for index in range(SUPPLIER_COUNT):
    supplier_number = index + 1

    region = region_values[index]

    country = random.choice(
        countries_by_region[region]
    )

    legal_suffix = random.choice(
        legal_suffix_by_country.get(
            country,
            ["Ltd.", "Group", "Corp."]
        )
    )

    name_prefix, name_industry, name_descriptor = (
        selected_name_combinations[index]
    )

    supplier_name = (
        f"{name_prefix} "
        f"{name_descriptor} "
        f"{name_industry} "
        f"{legal_suffix}"
    )

    classification = supplier_classification_values[index]

    preferred_supplier = classification in [
        "PreferredOnly",
        "PreferredAndStrategic"
    ]

    strategic_supplier = classification in [
        "StrategicOnly",
        "PreferredAndStrategic"
    ]

    created_year = random.randint(1998, 2025)
    created_month = random.randint(1, 12)
    created_day = random.randint(1, 28)

    risk_band = risk_band_values[index]

    supplier_rows.append(
        {
            "SupplierID": f"SUP{supplier_number:06d}",
            "SupplierName": supplier_name,
            "SupplierType": supplier_type_values[index],
            "Country": country,
            "Region": region,
            "PreferredSupplier": preferred_supplier,
            "StrategicSupplier": strategic_supplier,
            "ESGRating": esg_values[index],
            "FinancialRiskScore": (
                generate_financial_risk_score(risk_band)
            ),
            "CreatedDate": date(
                created_year,
                created_month,
                created_day
            ),
            "Status": supplier_status_values[index]
        }
    )

print(f"Prepared {len(supplier_rows)} supplier records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_schema = StructType([
    StructField(
        "SupplierID",
        StringType(),
        False
    ),
    StructField(
        "SupplierName",
        StringType(),
        False
    ),
    StructField(
        "SupplierType",
        StringType(),
        False
    ),
    StructField(
        "Country",
        StringType(),
        False
    ),
    StructField(
        "Region",
        StringType(),
        False
    ),
    StructField(
        "PreferredSupplier",
        BooleanType(),
        False
    ),
    StructField(
        "StrategicSupplier",
        BooleanType(),
        False
    ),
    StructField(
        "ESGRating",
        StringType(),
        True
    ),
    StructField(
        "FinancialRiskScore",
        IntegerType(),
        True
    ),
    StructField(
        "CreatedDate",
        DateType(),
        False
    ),
    StructField(
        "Status",
        StringType(),
        False
    )
])

supplier_df = spark.createDataFrame(
    supplier_rows,
    schema=supplier_schema
)

display(supplier_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_df = (
    supplier_df
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

display(supplier_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actual_supplier_count = supplier_df.count()

duplicate_supplier_id_count = (
    supplier_df
    .groupBy("SupplierID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_supplier_name_count = (
    supplier_df
    .groupBy("SupplierName")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

mandatory_supplier_null_count = (
    supplier_df
    .filter(
        F.col("SupplierID").isNull()
        | F.col("SupplierName").isNull()
        | F.col("SupplierType").isNull()
        | F.col("Country").isNull()
        | F.col("Region").isNull()
        | F.col("CreatedDate").isNull()
        | F.col("Status").isNull()
    )
    .count()
)

invalid_region_count = (
    supplier_df
    .filter(
        ~F.col("Region").isin(
            "EMEA",
            "APAC",
            "Americas"
        )
    )
    .count()
)

invalid_risk_score_count = (
    supplier_df
    .filter(
        (F.col("FinancialRiskScore") < 0)
        | (F.col("FinancialRiskScore") > 100)
    )
    .count()
)

assert actual_supplier_count == SUPPLIER_COUNT, (
    f"Expected {SUPPLIER_COUNT} suppliers, "
    f"but found {actual_supplier_count}."
)

assert duplicate_supplier_id_count == 0, (
    f"Found {duplicate_supplier_id_count} "
    "duplicate SupplierIDs."
)

assert duplicate_supplier_name_count == 0, (
    f"Found {duplicate_supplier_name_count} "
    "duplicate SupplierNames."
)

assert mandatory_supplier_null_count == 0, (
    f"Found {mandatory_supplier_null_count} "
    "mandatory null values."
)

assert invalid_region_count == 0, (
    f"Found {invalid_region_count} invalid regions."
)

assert invalid_risk_score_count == 0, (
    f"Found {invalid_risk_score_count} "
    "invalid financial risk scores."
)

print("Supplier validation passed.")
print(f"Rows: {actual_supplier_count}")
print(f"Duplicate SupplierIDs: {duplicate_supplier_id_count}")
print(f"Duplicate names: {duplicate_supplier_name_count}")
print(f"Mandatory nulls: {mandatory_supplier_null_count}")
print(f"Invalid regions: {invalid_region_count}")
print(f"Invalid risk scores: {invalid_risk_score_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect the supplier distributions**

# CELL ********************

display(
    supplier_df
    .groupBy("Region")
    .agg(
        F.count("*").alias("SupplierCount")
    )
    .orderBy(F.desc("SupplierCount"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    supplier_df
    .groupBy("SupplierType")
    .agg(
        F.count("*").alias("SupplierCount")
    )
    .orderBy(F.desc("SupplierCount"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    supplier_df
    .groupBy("Status")
    .agg(
        F.count("*").alias("SupplierCount")
    )
    .orderBy(F.desc("SupplierCount"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    supplier_df
    .agg(
        F.count("*").alias("TotalSuppliers"),
        F.sum(
            F.col("PreferredSupplier").cast("int")
        ).alias("PreferredSuppliers"),
        F.sum(
            F.col("StrategicSupplier").cast("int")
        ).alias("StrategicSuppliers"),
        F.sum(
            F.when(
                F.col("ESGRating").isNull(),
                1
            ).otherwise(0)
        ).alias("MissingESGRatings"),
        F.round(
            F.avg("FinancialRiskScore"),
            2
        ).alias("AverageRiskScore")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Save_bronze_supplier**

# CELL ********************

TARGET_SUPPLIER_TABLE = "bronze_supplier"

(
    supplier_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_SUPPLIER_TABLE)
)

print(
    f"Successfully created table: "
    f"{TARGET_SUPPLIER_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

saved_supplier_df = spark.table(
    "bronze_supplier"
)

persisted_supplier_count = (
    saved_supplier_df.count()
)

assert persisted_supplier_count == SUPPLIER_COUNT

display(saved_supplier_df)

print(
    f"Persisted supplier count: "
    f"{persisted_supplier_count}"
)

print("Persisted supplier validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
