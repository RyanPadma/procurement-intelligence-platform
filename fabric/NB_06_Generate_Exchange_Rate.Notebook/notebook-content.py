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

#Configuration
from datetime import date

PROFILE = "development"

START_DATE = date(2022, 1, 1)
END_DATE = date(2026, 7, 31)

RANDOM_SEED = 20260802

CURRENCIES = [
    "EUR",
    "USD",
    "GBP",
    "CHF",
    "PLN",
    "JPY",
    "CNY",
    "SEK"
]

print(f"Profile: {PROFILE}")
print(f"Start date: {START_DATE}")
print(f"End date: {END_DATE}")
print(f"Currencies: {len(CURRENCIES)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import library
import math
import random
from decimal import Decimal

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
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

# Define Starting Exchange rates
BASE_RATES_TO_EUR = {
    "EUR": 1.000000,
    "USD": 0.920000,
    "GBP": 1.170000,
    "CHF": 1.040000,
    "PLN": 0.230000,
    "JPY": 0.006200,
    "CNY": 0.127000,
    "SEK": 0.087000
}

assert set(CURRENCIES) == set(BASE_RATES_TO_EUR.keys())

print("Base exchange rates configured.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generaate daily Exchange rates
date_range = pd.date_range(
    start=START_DATE,
    end=END_DATE,
    freq="D"
)

exchange_rate_rows = []

for currency in CURRENCIES:
    base_rate = BASE_RATES_TO_EUR[currency]
    current_rate = base_rate
    previous_business_rate = base_rate

    for rate_date in date_range:
        if currency == "EUR":
            current_rate = 1.0

        elif rate_date.weekday() >= 5:
            # Saturday and Sunday use the prior business-day rate.
            current_rate = previous_business_rate

        else:
            daily_change = random.gauss(
                mu=0,
                sigma=0.0025
            )

            current_rate = (
                current_rate
                * math.exp(daily_change)
            )

            minimum_allowed_rate = (
                base_rate * 0.70
            )

            maximum_allowed_rate = (
                base_rate * 1.30
            )

            current_rate = max(
                minimum_allowed_rate,
                min(
                    current_rate,
                    maximum_allowed_rate
                )
            )

            previous_business_rate = current_rate

        exchange_rate_rows.append(
            {
                "RateDate": rate_date.date(),
                "Currency": currency,
                "ExchangeRateEUR": Decimal(
                    f"{current_rate:.6f}"
                )
            }
        )

print(
    f"Prepared "
    f"{len(exchange_rate_rows):,} "
    f"exchange-rate records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the Spark Dataframe
exchange_rate_schema = StructType([
    StructField(
        "RateDate",
        DateType(),
        False
    ),
    StructField(
        "Currency",
        StringType(),
        False
    ),
    StructField(
        "ExchangeRateEUR",
        DecimalType(12, 6),
        False
    )
])

exchange_rate_df = spark.createDataFrame(
    exchange_rate_rows,
    schema=exchange_rate_schema
)

display(exchange_rate_df.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add audit columns
exchange_rate_df = (
    exchange_rate_df
    .withColumn(
        "SourceSystem",
        F.lit("SYNTHETIC_FX_REFERENCE")
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

display(exchange_rate_df.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate the generated Data
expected_date_count = len(date_range)

expected_row_count = (
    expected_date_count
    * len(CURRENCIES)
)

actual_row_count = exchange_rate_df.count()

duplicate_key_count = (
    exchange_rate_df
    .groupBy(
        "RateDate",
        "Currency"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

mandatory_null_count = (
    exchange_rate_df
    .filter(
        F.col("RateDate").isNull()
        | F.col("Currency").isNull()
        | F.col("ExchangeRateEUR").isNull()
    )
    .count()
)

invalid_rate_count = (
    exchange_rate_df
    .filter(
        F.col("ExchangeRateEUR") <= 0
    )
    .count()
)

invalid_currency_count = (
    exchange_rate_df
    .filter(
        ~F.col("Currency").isin(
            CURRENCIES
        )
    )
    .count()
)

invalid_eur_rate_count = (
    exchange_rate_df
    .filter(
        (F.col("Currency") == "EUR")
        & (
            F.col("ExchangeRateEUR")
            != F.lit(1.000000)
        )
    )
    .count()
)

assert actual_row_count == expected_row_count, (
    f"Expected {expected_row_count:,} rows, "
    f"but found {actual_row_count:,}."
)

assert duplicate_key_count == 0, (
    f"Found {duplicate_key_count} duplicate "
    "RateDate-Currency combinations."
)

assert mandatory_null_count == 0, (
    f"Found {mandatory_null_count} mandatory nulls."
)

assert invalid_rate_count == 0, (
    f"Found {invalid_rate_count} invalid rates."
)

assert invalid_currency_count == 0, (
    f"Found {invalid_currency_count} invalid currencies."
)

assert invalid_eur_rate_count == 0, (
    f"Found {invalid_eur_rate_count} invalid EUR rates."
)

print("Exchange-rate validation passed.")
print(f"Rows: {actual_row_count:,}")
print(f"Duplicate keys: {duplicate_key_count}")
print(f"Mandatory nulls: {mandatory_null_count}")
print(f"Invalid rates: {invalid_rate_count}")
print(f"Invalid currencies: {invalid_currency_count}")
print(f"Invalid EUR rates: {invalid_eur_rate_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Inspect the rates
display(
    exchange_rate_df
    .groupBy("Currency")
    .agg(
        F.count("*").alias("DateCount"),
        F.min("ExchangeRateEUR").alias(
            "MinimumRate"
        ),
        F.max("ExchangeRateEUR").alias(
            "MaximumRate"
        ),
        F.round(
            F.avg("ExchangeRateEUR"),
            6
        ).alias("AverageRate")
    )
    .orderBy("Currency")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Display one currency over time
display(
    exchange_rate_df
    .filter(
        F.col("Currency") == "USD"
    )
    .select(
        "RateDate",
        "ExchangeRateEUR"
    )
    .orderBy("RateDate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to the lakehouse
TARGET_EXCHANGE_RATE_TABLE = (
    "bronze_exchange_rate"
)

(
    exchange_rate_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_EXCHANGE_RATE_TABLE
    )
)

print(
    f"Successfully created table: "
    f"{TARGET_EXCHANGE_RATE_TABLE}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read back and validate
saved_exchange_rate_df = spark.table(
    "bronze_exchange_rate"
)

persisted_row_count = (
    saved_exchange_rate_df.count()
)

persisted_distinct_key_count = (
    saved_exchange_rate_df
    .select(
        "RateDate",
        "Currency"
    )
    .distinct()
    .count()
)

assert persisted_row_count == expected_row_count

assert (
    persisted_distinct_key_count
    == expected_row_count
)

display(
    saved_exchange_rate_df.limit(50)
)

print(
    f"Persisted exchange-rate rows: "
    f"{persisted_row_count:,}"
)

print(
    "Persisted exchange-rate "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
