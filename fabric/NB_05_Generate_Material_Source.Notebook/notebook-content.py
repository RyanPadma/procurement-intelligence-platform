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
from datetime import date

PROFILE = "development"

PROFILE_VOLUMES = {
    "development": {
        "materials": 2_000
    },
    "portfolio": {
        "materials": 15_000
    }
}

if PROFILE not in PROFILE_VOLUMES:
    raise ValueError(
        f"Invalid profile: {PROFILE}. "
        f"Choose from {list(PROFILE_VOLUMES.keys())}"
    )

MATERIAL_COUNT = PROFILE_VOLUMES[PROFILE]["materials"]
RANDOM_SEED = 20260802
LOAD_DATE = date.today().isoformat()

LANDING_PATH = (
    f"Files/landing/sap/material/"
    f"load_date={LOAD_DATE}"
)

print(f"Profile: {PROFILE}")
print(f"Materials to generate: {MATERIAL_COUNT:,}")
print(f"Load date: {LOAD_DATE}")
print(f"Landing path: {LANDING_PATH}")

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

#Read the category Reference
category_reference_df = (
    spark.table("bronze_category")
    .select(
        "CategoryID",
        "CategoryName",
        "ProcurementType"
    )
    .orderBy("CategoryID")
)

display(category_reference_df)

category_reference_count = category_reference_df.count()

assert category_reference_count == 20, (
    f"Expected 20 categories but found "
    f"{category_reference_count}."
)

print(
    f"Category reference loaded: "
    f"{category_reference_count} categories."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

category_records = [
    row.asDict()
    for row in category_reference_df.collect()
]

category_ids_by_type = {
    "Direct": [],
    "Indirect": [],
    "Capital": []
}

for category in category_records:
    procurement_type = category["ProcurementType"]

    if procurement_type not in category_ids_by_type:
        raise ValueError(
            f"Unexpected procurement type: "
            f"{procurement_type}"
        )

    category_ids_by_type[procurement_type].append(
        category["CategoryID"]
    )

for procurement_type, category_ids in (
    category_ids_by_type.items()
):
    print(
        f"{procurement_type}: "
        f"{len(category_ids)} categories"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Category-specific Material rules
CATEGORY_SPECS = {
    "CAT001": {
        "description": "Steel and Alloy Material",
        "uoms": ["KG", "TON"],
        "minimum_cost": 0.50,
        "maximum_cost": 12_000
    },
    "CAT002": {
        "description": "Polymer Resin",
        "uoms": ["KG", "TON"],
        "minimum_cost": 0.50,
        "maximum_cost": 8_000
    },
    "CAT003": {
        "description": "Mechanical Component",
        "uoms": ["EA"],
        "minimum_cost": 5,
        "maximum_cost": 25_000
    },
    "CAT004": {
        "description": "Electrical Component",
        "uoms": ["EA"],
        "minimum_cost": 2,
        "maximum_cost": 15_000
    },
    "CAT005": {
        "description": "Electronic Component",
        "uoms": ["EA"],
        "minimum_cost": 0.50,
        "maximum_cost": 10_000
    },
    "CAT006": {
        "description": "Packaging Material",
        "uoms": ["EA", "KG", "PALLET"],
        "minimum_cost": 0.10,
        "maximum_cost": 200
    },
    "CAT007": {
        "description": "Industrial Chemical",
        "uoms": ["L", "KG"],
        "minimum_cost": 1,
        "maximum_cost": 5_000
    },
    "CAT008": {
        "description": "MRO Item",
        "uoms": ["EA", "SERVICE"],
        "minimum_cost": 1,
        "maximum_cost": 10_000
    },
    "CAT009": {
        "description": "Industrial Equipment",
        "uoms": ["EA"],
        "minimum_cost": 5_000,
        "maximum_cost": 1_000_000
    },
    "CAT010": {
        "description": "Production Tooling",
        "uoms": ["EA"],
        "minimum_cost": 500,
        "maximum_cost": 250_000
    },
    "CAT011": {
        "description": "Freight Movement",
        "uoms": ["SERVICE", "PALLET"],
        "minimum_cost": 100,
        "maximum_cost": 20_000
    },
    "CAT012": {
        "description": "Warehousing Service",
        "uoms": ["SERVICE", "PALLET"],
        "minimum_cost": 50,
        "maximum_cost": 25_000
    },
    "CAT013": {
        "description": "IT Product or Service",
        "uoms": ["EA", "SERVICE", "HOUR"],
        "minimum_cost": 20,
        "maximum_cost": 200_000
     },
    "CAT014": {
        "description": "Professional Service",
        "uoms": ["HOUR", "DAY", "SERVICE"],
        "minimum_cost": 50,
        "maximum_cost": 3_500
    },
    "CAT015": {
        "description": "Temporary Labor",
        "uoms": ["HOUR", "DAY"],
        "minimum_cost": 20,
        "maximum_cost": 1_500
    },
    "CAT016": {
        "description": "Facility Service",
        "uoms": ["SERVICE", "HOUR"],
        "minimum_cost": 50,
        "maximum_cost": 100_000
    },
    "CAT017": {
        "description": "Energy or Utility",
        "uoms": ["SERVICE", "L"],
        "minimum_cost": 10,
        "maximum_cost": 500_000
    },
    "CAT018": {
        "description": "Marketing Service",
        "uoms": ["SERVICE", "HOUR"],
        "minimum_cost": 100,
        "maximum_cost": 250_000
    },
    "CAT019": {
        "description": "Travel Service",
        "uoms": ["SERVICE", "EA"],
        "minimum_cost": 20,
        "maximum_cost": 10_000
    },
    "CAT020": {
        "description": "Office Supply",
        "uoms": ["EA", "BOX"],
        "minimum_cost": 1,
        "maximum_cost": 500
    }
}

missing_specifications = (
    set(category_reference_df
        .select("CategoryID")
        .rdd
        .flatMap(lambda row: row)
        .collect())
    - set(CATEGORY_SPECS.keys())
)

assert not missing_specifications, (
    f"Missing specifications for: "
    f"{missing_specifications}"
)

print("All category specifications are available.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Helper Function
def generate_standard_cost(
    minimum_cost,
    maximum_cost
):
    log_minimum = math.log(minimum_cost)
    log_maximum = math.log(maximum_cost)

    generated_value = math.exp(
        random.uniform(
            log_minimum,
            log_maximum
        )
    )

    generated_value = max(
        minimum_cost,
        min(
            generated_value,
            maximum_cost
        )
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

MANUFACTURERS = [
    "Northwind Industrial",
    "Contoso Components",
    "Fabrikam Systems",
    "Adventure Works Manufacturing",
    "Tailspin Equipment",
    "Wide World Materials",
    "Proseware Technologies",
    "Litware Engineering",
    "Lucerne Engineering",
    "Alpine Industrial Group",
    "Blue Yonder Components",
    "Fourth Coffee Equipment"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

procurement_types = random.choices(
    population=[
        "Direct",
        "Indirect",
        "Capital"
    ],
    weights=[
        65,
        27,
        8
    ],
    k=MATERIAL_COUNT
)

category_sequence_numbers = {
    category_id: 0
    for category_id in CATEGORY_SPECS
}

material_rows = []

for material_number in range(
    1,
    MATERIAL_COUNT + 1
):
    procurement_type = (
        procurement_types[
            material_number - 1
        ]
    )

    category_id = random.choice(
        category_ids_by_type[
            procurement_type
        ]
    )

    category_sequence_numbers[
        category_id
    ] += 1

    category_sequence = (
        category_sequence_numbers[
            category_id
        ]
    )

    specification = (
        CATEGORY_SPECS[
            category_id
        ]
    )

    material_rows.append(
        {
            "MaterialID": (
                f"MAT{material_number:06d}"
            ),
            "MaterialDescription": (
                f"{specification['description']} "
                f"{category_sequence:05d}"
            ),
            "CategoryID": category_id,
            "StandardCost": (
                generate_standard_cost(
                    specification[
                        "minimum_cost"
                    ],
                    specification[
                        "maximum_cost"
                    ]
                )
            ),
            "UnitOfMeasure": random.choice(
                specification["uoms"]
            ),
            "Manufacturer": random.choice(
                MANUFACTURERS
            ),
            "Status": random.choices(
                population=[
                    "Active",
                    "Inactive"
                ],
                weights=[
                    97,
                    3
                ],
                k=1
            )[0]
        }
    )

print(
    f"Prepared "
    f"{len(material_rows):,} "
    f"material records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark Dataframe
material_schema = StructType([
    StructField(
        "MaterialID",
        StringType(),
        False
    ),
    StructField(
        "MaterialDescription",
        StringType(),
        False
    ),
    StructField(
        "CategoryID",
        StringType(),
        False
    ),
    StructField(
        "StandardCost",
        DecimalType(18, 2),
        False
    ),
    StructField(
        "UnitOfMeasure",
        StringType(),
        False
    ),
    StructField(
        "Manufacturer",
        StringType(),
        False
    ),
    StructField(
        "Status",
        StringType(),
        False
    )
])

material_source_df = spark.createDataFrame(
    material_rows,
    schema=material_schema
)

display(material_source_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# add source metadata and record hash
material_source_df = (
    material_source_df
    .withColumn(
        "SourceSystem",
        F.lit("SYNTHETIC_SAP")
    )
    .withColumn(
        "ExtractDate",
        F.lit(LOAD_DATE).cast("date")
    )
    .withColumn(
        "ExtractTimestamp",
        F.current_timestamp()
    )
    .withColumn(
        "SourceRecordHash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("MaterialID"),
                F.col("MaterialDescription"),
                F.col("CategoryID"),
                F.col("StandardCost").cast(
                    "string"
                ),
                F.col("UnitOfMeasure"),
                F.col("Manufacturer"),
                F.col("Status")
            ),
            256
        )
    )
)

display(material_source_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate before landing
actual_material_count = (
    material_source_df.count()
)

duplicate_material_id_count = (
    material_source_df
    .groupBy("MaterialID")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    material_source_df
    .filter(
        F.col("MaterialID").isNull()
        | F.col(
            "MaterialDescription"
        ).isNull()
        | F.col("CategoryID").isNull()
        | F.col("StandardCost").isNull()
        | F.col(
            "UnitOfMeasure"
        ).isNull()
        | F.col("Status").isNull()
    )
    .count()
)

invalid_cost_count = (
    material_source_df
    .filter(
        F.col("StandardCost") <= 0
    )
    .count()
)

invalid_category_reference_count = (
    material_source_df.alias("material")
    .join(
        category_reference_df.alias(
            "category"
        ),
        F.col("material.CategoryID")
        == F.col("category.CategoryID"),
        "left_anti"
    )
    .count()
)

assert actual_material_count == MATERIAL_COUNT, (
    f"Expected {MATERIAL_COUNT} records, "
    f"but found {actual_material_count}."
)

assert duplicate_material_id_count == 0, (
    f"Found {duplicate_material_id_count} "
    "duplicate MaterialIDs."
)

assert mandatory_null_count == 0, (
    f"Found {mandatory_null_count} "
    "mandatory null values."
)

assert invalid_cost_count == 0, (
    f"Found {invalid_cost_count} "
    "invalid standard costs."
)

assert invalid_category_reference_count == 0, (
    f"Found {invalid_category_reference_count} "
    "invalid category references."
)

print("Material source validation passed.")
print(f"Rows: {actual_material_count:,}")
print(
    f"Duplicate MaterialIDs: "
    f"{duplicate_material_id_count}"
)
print(
    f"Mandatory nulls: "
    f"{mandatory_null_count}"
)
print(
    f"Invalid costs: "
    f"{invalid_cost_count}"
)
print(
    f"Invalid category references: "
    f"{invalid_category_reference_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect the generated distribution
material_distribution_df = (
    material_source_df.alias("material")
    .join(
        category_reference_df.alias(
            "category"
        ),
        F.col("material.CategoryID")
        == F.col("category.CategoryID"),
        "inner"
    )
    .groupBy(
        F.col(
            "category.ProcurementType"
        )
    )
    .agg(
        F.count("*").alias(
            "MaterialCount"
        )
    )
    .withColumn(
        "MaterialPercentage",
        F.round(
            F.col("MaterialCount")
            / F.lit(MATERIAL_COUNT)
            * 100,
            2
        )
    )
    .orderBy(
        F.desc("MaterialCount")
    )
)

display(material_distribution_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze Audit
from pyspark.sql import functions as F

material_df = (
    material_source_df
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

display(material_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load Into the Lakehouse
TARGET_MATERIAL_TABLE = "bronze_material"

(
    material_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_MATERIAL_TABLE)
)

print(
    f"Successfully created table: "
    f"{TARGET_MATERIAL_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read the table
saved_material_df = spark.table(
    "bronze_material"
)

persisted_material_count = (
    saved_material_df.count()
)

display(saved_material_df)

print(
    f"Persisted material count: "
    f"{persisted_material_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate the tble
persisted_distinct_material_count = (
    saved_material_df
    .select("MaterialID")
    .distinct()
    .count()
)

persisted_invalid_category_count = (
    saved_material_df.alias("material")
    .join(
        category_reference_df.alias("category"),
        F.col("material.CategoryID")
        == F.col("category.CategoryID"),
        "left_anti"
    )
    .count()
)

persisted_invalid_cost_count = (
    saved_material_df
    .filter(
        F.col("StandardCost") <= 0
    )
    .count()
)

assert persisted_material_count == MATERIAL_COUNT, (
    f"Expected {MATERIAL_COUNT:,} rows, "
    f"but found {persisted_material_count:,}."
)

assert persisted_distinct_material_count == MATERIAL_COUNT, (
    "MaterialID uniqueness validation failed."
)

assert persisted_invalid_category_count == 0, (
    f"Found {persisted_invalid_category_count} "
    "invalid category references."
)

assert persisted_invalid_cost_count == 0, (
    f"Found {persisted_invalid_cost_count} "
    "invalid material costs."
)

print("Persisted material validation passed.")
print(f"Rows: {persisted_material_count:,}")
print(
    f"Distinct MaterialIDs: "
    f"{persisted_distinct_material_count:,}"
)
print(
    f"Invalid category references: "
    f"{persisted_invalid_category_count}"
)
print(
    f"Invalid costs: "
    f"{persisted_invalid_cost_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
