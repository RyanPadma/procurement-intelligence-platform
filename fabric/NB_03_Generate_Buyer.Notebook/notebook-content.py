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

PROFILE = "development"

PROFILE_VOLUMES = {
    "development": {
        "business_units": 10
    },
    "portfolio": {
        "business_units": 25
    }
}

if PROFILE not in PROFILE_VOLUMES:
    raise ValueError(
        f"Invalid profile: {PROFILE}. "
        f"Choose from {list(PROFILE_VOLUMES.keys())}"
    )

BUSINESS_UNIT_COUNT = PROFILE_VOLUMES[PROFILE]["business_units"]

print(f"Profile: {PROFILE}")
print(f"Business units to generate: {BUSINESS_UNIT_COUNT}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
BUYER_PROFILE_VOLUMES = {
    "development": 30,
    "portfolio": 120
}

BUYER_COUNT = BUYER_PROFILE_VOLUMES[PROFILE]

print(f"Profile: {PROFILE}")
print(f"Buyers to generate: {BUYER_COUNT}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

business_unit_reference_df = (
    spark.table("bronze_business_unit")
    .select(
        "BusinessUnitID",
        "BusinessUnitName",
        "Region"
    )
    .orderBy("BusinessUnitID")
)

display(business_unit_reference_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

business_unit_records = [
    row.asDict()
    for row in business_unit_reference_df.collect()
]

print(f"Available business units: {len(business_unit_records)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
import re

RANDOM_SEED = 20260802
random.seed(RANDOM_SEED)

first_names = [
    "Emma",
    "Liam",
    "Sophie",
    "Noah",
    "Olivia",
    "Lucas",
    "Mila",
    "Daniel",
    "Eva",
    "Thomas",
    "Anna",
    "Victor"
]

last_names = [
    "Jansen",
    "De Vries",
    "Bakker",
    "Schmidt",
    "Muller",
    "Dubois",
    "Martin",
    "Rossi",
    "Kowalski",
    "Andersson"
]

# 12 first names × 10 last names = 120 unique combinations.
buyer_names = [
    f"{first_name} {last_name}"
    for first_name in first_names
    for last_name in last_names
]

random.shuffle(buyer_names)

selected_buyer_names = buyer_names[:BUYER_COUNT]

print(f"Unique buyer names prepared: {len(selected_buyer_names)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def assign_buyer_role(index, total_buyers):
    position = index / total_buyers

    if position <= 0.70:
        return "Operational Buyer"
    elif position <= 0.80:
        return "Senior Buyer"
    elif position <= 0.90:
        return "Strategic Buyer"
    elif position <= 0.97:
        return "Category Manager"
    else:
        return "Procurement Manager"


def assign_department(buyer_role):
    department_mapping = {
        "Operational Buyer": "Procurement Operations",
        "Senior Buyer": "Direct Procurement",
        "Strategic Buyer": "Strategic Sourcing",
        "Category Manager": "Category Management",
        "Procurement Manager": "Procurement Leadership"
    }

    return department_mapping[buyer_role]


def create_email_address(buyer_name):
    cleaned_name = re.sub(
        r"[^a-zA-Z ]",
        "",
        buyer_name
    ).lower()

    email_name = ".".join(cleaned_name.split())

    return f"{email_name}@northwind-mfg.example"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

weighted_business_units = []

for business_unit in business_unit_records:
    business_unit_name = business_unit["BusinessUnitName"]

    # Manufacturing sites receive three entries in the assignment pool.
    # Other units receive one entry.
    assignment_weight = (
        3
        if "Manufacturing" in business_unit_name
        or "Plant" in business_unit_name
        else 1
    )

    weighted_business_units.extend(
        [business_unit] * assignment_weight
    )

buyer_rows = []

for index, buyer_name in enumerate(
    selected_buyer_names,
    start=1
):
    buyer_role = assign_buyer_role(
        index=index,
        total_buyers=BUYER_COUNT
    )

    assigned_business_unit = weighted_business_units[
        (index - 1) % len(weighted_business_units)
    ]

    buyer_rows.append(
        {
            "BuyerID": f"BUY{index:04d}",
            "BuyerName": buyer_name,
            "Email": create_email_address(buyer_name),
            "Department": assign_department(buyer_role),
            "BuyerRole": buyer_role,
            "BusinessUnitID": assigned_business_unit[
                "BusinessUnitID"
            ]
        }
    )

print(f"Prepared {len(buyer_rows)} buyer records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

buyer_schema = StructType([
    StructField("BuyerID", StringType(), False),
    StructField("BuyerName", StringType(), False),
    StructField("Email", StringType(), False),
    StructField("Department", StringType(), False),
    StructField("BuyerRole", StringType(), False),
    StructField("BusinessUnitID", StringType(), False)
])

buyer_df = spark.createDataFrame(
    buyer_rows,
    schema=buyer_schema
)

display(buyer_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

buyer_df = (
    buyer_df
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

display(buyer_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actual_buyer_count = buyer_df.count()

duplicate_buyer_id_count = (
    buyer_df
    .groupBy("BuyerID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_email_count = (
    buyer_df
    .groupBy("Email")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

mandatory_buyer_null_count = (
    buyer_df
    .filter(
        F.col("BuyerID").isNull()
        | F.col("BuyerName").isNull()
        | F.col("Email").isNull()
        | F.col("Department").isNull()
        | F.col("BuyerRole").isNull()
        | F.col("BusinessUnitID").isNull()
    )
    .count()
)

assert actual_buyer_count == BUYER_COUNT, (
    f"Expected {BUYER_COUNT} buyers, "
    f"but found {actual_buyer_count}."
)

assert duplicate_buyer_id_count == 0, (
    f"Found {duplicate_buyer_id_count} duplicate BuyerIDs."
)

assert duplicate_email_count == 0, (
    f"Found {duplicate_email_count} duplicate email addresses."
)

assert mandatory_buyer_null_count == 0, (
    f"Found {mandatory_buyer_null_count} rows "
    "with mandatory null values."
)

print("Basic buyer validation passed.")
print(f"Rows: {actual_buyer_count}")
print(f"Duplicate BuyerIDs: {duplicate_buyer_id_count}")
print(f"Duplicate emails: {duplicate_email_count}")
print(f"Mandatory nulls: {mandatory_buyer_null_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_business_unit_references_df = (
    buyer_df.alias("buyer")
    .join(
        business_unit_reference_df.alias("business_unit"),
        F.col("buyer.BusinessUnitID")
        == F.col("business_unit.BusinessUnitID"),
        "left_anti"
    )
)

invalid_business_unit_reference_count = (
    invalid_business_unit_references_df.count()
)

assert invalid_business_unit_reference_count == 0, (
    f"Found {invalid_business_unit_reference_count} "
    "buyers with invalid BusinessUnitIDs."
)

print("Buyer-to-business-unit foreign-key validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

allowed_buyer_roles = [
    "Operational Buyer",
    "Senior Buyer",
    "Strategic Buyer",
    "Category Manager",
    "Procurement Manager"
]

invalid_buyer_role_count = (
    buyer_df
    .filter(
        ~F.col("BuyerRole").isin(allowed_buyer_roles)
    )
    .count()
)

assert invalid_buyer_role_count == 0, (
    f"Found {invalid_buyer_role_count} invalid buyer roles."
)

print("Buyer-role validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    buyer_df
    .groupBy("BuyerRole")
    .agg(
        F.count("*").alias("BuyerCount")
    )
    .orderBy(F.desc("BuyerCount"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TARGET_BUYER_TABLE = "bronze_buyer"

(
    buyer_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_BUYER_TABLE)
)

print(
    f"Successfully created table: "
    f"{TARGET_BUYER_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TARGET_BUYER_TABLE = "bronze_buyer"

(
    buyer_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_BUYER_TABLE)
)

print(
    f"Successfully created table: "
    f"{TARGET_BUYER_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
