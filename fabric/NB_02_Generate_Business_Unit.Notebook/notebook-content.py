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

business_unit_source = [
    ("Netherlands Headquarters", "Netherlands", "EMEA"),
    ("Netherlands Distribution Center", "Netherlands", "EMEA"),
    ("Germany Manufacturing North", "Germany", "EMEA"),
    ("Germany Manufacturing South", "Germany", "EMEA"),
    ("France Manufacturing", "France", "EMEA"),
    ("United Kingdom Distribution", "United Kingdom", "EMEA"),
    ("Spain Assembly Plant", "Spain", "EMEA"),
    ("Italy Manufacturing", "Italy", "EMEA"),
    ("Poland Components Plant", "Poland", "EMEA"),
    ("Czech Republic Manufacturing", "Czech Republic", "EMEA"),
    ("Sweden Engineering Center", "Sweden", "EMEA"),
    ("Switzerland Regional Office", "Switzerland", "EMEA"),
    ("Belgium Distribution Center", "Belgium", "EMEA"),
    ("Austria Service Center", "Austria", "EMEA"),
    ("Ireland Shared Services", "Ireland", "EMEA"),
    ("United States Manufacturing East", "United States", "Americas"),
    ("United States Manufacturing West", "United States", "Americas"),
    ("Mexico Assembly Plant", "Mexico", "Americas"),
    ("Brazil Manufacturing", "Brazil", "Americas"),
    ("Canada Distribution Center", "Canada", "Americas"),
    ("China Manufacturing East", "China", "APAC"),
    ("China Manufacturing South", "China", "APAC"),
    ("Japan Engineering Center", "Japan", "APAC"),
    ("Singapore Regional Office", "Singapore", "APAC"),
    ("India Shared Services", "India", "APAC")
]

selected_business_units = business_unit_source[:BUSINESS_UNIT_COUNT]

business_unit_rows = [
    {
        "BusinessUnitID": f"BU{index:03d}",
        "BusinessUnitName": name,
        "Country": country,
        "Region": region
    }
    for index, (name, country, region)
    in enumerate(selected_business_units, start=1)
]

print(f"Prepared {len(business_unit_rows)} business units.")

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

business_unit_schema = StructType([
    StructField("BusinessUnitID", StringType(), False),
    StructField("BusinessUnitName", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("Region", StringType(), False)
])

business_unit_df = spark.createDataFrame(
    business_unit_rows,
    schema=business_unit_schema
)

display(business_unit_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

business_unit_df = (
    business_unit_df
    .withColumn("SourceSystem", F.lit("SYNTHETIC_SAP"))
    .withColumn("IngestionTimestamp", F.current_timestamp())
    .withColumn("LoadDate", F.current_date())
)

display(business_unit_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actual_row_count = business_unit_df.count()

duplicate_key_count = (
    business_unit_df
    .groupBy("BusinessUnitID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

mandatory_null_count = (
    business_unit_df
    .filter(
        F.col("BusinessUnitID").isNull()
        | F.col("BusinessUnitName").isNull()
        | F.col("Country").isNull()
        | F.col("Region").isNull()
    )
    .count()
)

invalid_region_count = (
    business_unit_df
    .filter(
        ~F.col("Region").isin("EMEA", "Americas", "APAC")
    )
    .count()
)

assert actual_row_count == BUSINESS_UNIT_COUNT
assert duplicate_key_count == 0
assert mandatory_null_count == 0
assert invalid_region_count == 0

print("Business-unit validation passed.")
print(f"Rows: {actual_row_count}")
print(f"Duplicate keys: {duplicate_key_count}")
print(f"Mandatory nulls: {mandatory_null_count}")
print(f"Invalid regions: {invalid_region_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TARGET_TABLE = "bronze_business_unit"

(
    business_unit_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully created: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

saved_business_unit_df = spark.table("bronze_business_unit")

display(saved_business_unit_df)

print(
    "Persisted row count:",
    saved_business_unit_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
