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
%pip install Faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
import random

import numpy as np
import pandas as pd
from faker import Faker

SEED = 20260802

random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)

fake = Faker()

CONFIG = {
    "profile": "development",
    "start_date": date(2022, 1, 1),
    "end_date": date(2026, 7, 31),
    "reporting_currency": "EUR",
    "volumes": {
        "development": {
            "business_units": 10,
            "buyers": 30,
            "suppliers": 500,
            "materials": 2_000
        },
        "portfolio": {
            "business_units": 25,
            "buyers": 120,
            "suppliers": 3_500,
            "materials": 15_000
        }
    }
}

PROFILE = CONFIG["profile"]
VOLUMES = CONFIG["volumes"][PROFILE]

print(f"Generating profile: {PROFILE}")
print(VOLUMES)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

categories = [
    ("CAT001", "Raw Materials - Metals", "Raw Materials", "Direct"),
    ("CAT002", "Raw Materials - Polymers", "Raw Materials", "Direct"),
    ("CAT003", "Mechanical Components", "Components", "Direct"),
    ("CAT004", "Electrical Components", "Components", "Direct"),
    ("CAT005", "Electronic Components", "Components", "Direct"),
    ("CAT006", "Packaging Materials", "Packaging", "Direct"),
    ("CAT007", "Chemicals and Coatings", "Chemicals", "Direct"),
    ("CAT008", "Maintenance, Repair and Operations", "MRO", "Indirect"),
    ("CAT009", "Industrial Equipment", "Capital Equipment", "Capital"),
    ("CAT010", "Tooling", "Capital Equipment", "Capital"),
    ("CAT011", "Logistics and Freight", "Logistics", "Indirect"),
    ("CAT012", "Warehousing", "Logistics", "Indirect"),
    ("CAT013", "Information Technology", "Technology", "Indirect"),
    ("CAT014", "Professional Services", "Services", "Indirect"),
    ("CAT015", "Temporary Labor", "Workforce", "Indirect"),
    ("CAT016", "Facilities Management", "Facilities", "Indirect"),
    ("CAT017", "Energy and Utilities", "Utilities", "Indirect"),
    ("CAT018", "Marketing and Communications", "Marketing", "Indirect"),
    ("CAT019", "Travel and Mobility", "Travel", "Indirect"),
    ("CAT020", "Office Supplies", "Office", "Indirect")
]

category_rows = [
    {
        "CategoryID": category_id,
        "CategoryName": category_name,
        "CommodityGroup": commodity_group,
        "ProcurementType": procurement_type,
        "CategoryManager": f"Category Manager {index:02d}"
    }
    for index, (
        category_id,
        category_name,
        commodity_group,
        procurement_type
    ) in enumerate(categories, start=1)
]

category_df = spark.createDataFrame(category_rows)

display(category_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    category_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze_category")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert category_df.count() == 20
assert category_df.select("CategoryID").distinct().count() == 20
assert category_df.filter("CategoryID IS NULL").count() == 0

print("Category validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
