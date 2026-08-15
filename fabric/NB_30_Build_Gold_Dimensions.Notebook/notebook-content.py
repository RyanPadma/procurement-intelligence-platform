# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "61816ce7-e600-44cb-a900-02fc677fd1e8",
# META       "default_lakehouse_name": "lh_procurement_gold",
# META       "default_lakehouse_workspace_id": "83e05aab-2eed-49cb-a339-674db19d4b92",
# META       "known_lakehouses": [
# META         {
# META           "id": "61816ce7-e600-44cb-a900-02fc677fd1e8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# This notebook will create these physical Gold dimensions:
# 
# **dim_date**
# 
# **dim_supplier**          ← SCD Type 2
# 
# **dim_category**
# 
# **dim_material**
# 
# **dim_buyer**
# 
# **dim_business_unit**
# 
# **dim_contract**
# 
# **dim_currency**

# MARKDOWN ********************

# **Configuration**

# CELL ********************

# ============================================================
# NB_30_Build_Gold_Dimensions
# Configuration
# ============================================================

from datetime import date


# ============================================================
# Analytical Snapshot
# ============================================================

SNAPSHOT_DATE = date(
    2026,
    7,
    31
)


# ============================================================
# Gold Date Dimension
# ============================================================
#
# Historical analytical data begins in 2022.
#
# Gold must also support forward-looking procurement planning,
# especially Savings Projects whose planned completion dates
# can extend beyond the current snapshot year.
#
# Maintain a rolling five-year future planning horizon.
# With the current 2026 snapshot this produces:
#
# DATE_END = 2031-12-31
#
# When SNAPSHOT_DATE is advanced in a future pipeline run,
# the Gold date dimension horizon advances automatically.
# ============================================================

DATE_START = date(
    2022,
    1,
    1
)

DATE_FUTURE_YEARS = 5

DATE_END = date(
    SNAPSHOT_DATE.year
    +
    DATE_FUTURE_YEARS,
    12,
    31
)


# ============================================================
# Silver Source Shortcuts
# ============================================================

SILVER_SUPPLIER_TABLE = (
    "silver_supplier"
)

SILVER_CATEGORY_TABLE = (
    "silver_category"
)

SILVER_MATERIAL_TABLE = (
    "silver_material"
)

SILVER_BUYER_TABLE = (
    "silver_buyer"
)

SILVER_BUSINESS_UNIT_TABLE = (
    "silver_business_unit"
)

SILVER_CONTRACT_TABLE = (
    "silver_contract"
)

SILVER_EXCHANGE_RATE_TABLE = (
    "silver_exchange_rate"
)

SILVER_MASTER_MONITORING_TABLE = (
    "monitoring_silver_master_data_quality_results"
)

SILVER_CONTRACT_MONITORING_TABLE = (
    "monitoring_silver_contract_quality_results"
)


# ============================================================
# Physical Gold Output Tables
# ============================================================

DIM_DATE_TABLE = (
    "dim_date"
)

DIM_SUPPLIER_TABLE = (
    "dim_supplier"
)

DIM_CATEGORY_TABLE = (
    "dim_category"
)

DIM_MATERIAL_TABLE = (
    "dim_material"
)

DIM_BUYER_TABLE = (
    "dim_buyer"
)

DIM_BUSINESS_UNIT_TABLE = (
    "dim_business_unit"
)

DIM_CONTRACT_TABLE = (
    "dim_contract"
)

DIM_CURRENCY_TABLE = (
    "dim_currency"
)


GOLD_MONITORING_TABLE = (
    "monitoring_gold_dimensions_quality_results"
)


# ============================================================
# Notebook Information
# ============================================================

print(
    "Notebook: "
    "NB_30_Build_Gold_Dimensions"
)

print(
    "Default Lakehouse: "
    "lh_procurement_gold"
)

print(
    "Dimension snapshot date:",
    SNAPSHOT_DATE
)

print(
    "Date dimension start:",
    DATE_START
)

print(
    "Date dimension end:",
    DATE_END
)

print(
    "Future planning horizon:",
    DATE_FUTURE_YEARS,
    "years"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Imports**

# CELL ********************

from pyspark.sql import functions as F

print("Libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate required Silver shortcusts**

# CELL ********************

required_tables = [
    SILVER_SUPPLIER_TABLE,
    SILVER_CATEGORY_TABLE,
    SILVER_MATERIAL_TABLE,
    SILVER_BUYER_TABLE,
    SILVER_BUSINESS_UNIT_TABLE,
    SILVER_CONTRACT_TABLE,
    SILVER_EXCHANGE_RATE_TABLE,
    SILVER_MASTER_MONITORING_TABLE,
    SILVER_CONTRACT_MONITORING_TABLE
]


missing_tables = [
    table_name
    for table_name in required_tables
    if not spark.catalog.tableExists(
        table_name
    )
]


if missing_tables:

    raise RuntimeError(
        "Missing required Gold Lakehouse "
        "Silver shortcuts: "
        +
        ", ".join(
            missing_tables
        )
    )


print(
    "All required Silver shortcuts exist."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Confirm upstream SIlver quality gates**

# CELL ********************

master_failure_count = (
    spark.table(
        SILVER_MASTER_MONITORING_TABLE
    )
    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAILED"
    )
    .count()
)


contract_failure_count = (
    spark.table(
        SILVER_CONTRACT_MONITORING_TABLE
    )
    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAILED"
    )
    .count()
)


print(
    "Silver master-data failures:",
    master_failure_count
)

print(
    "Silver contract failures:",
    contract_failure_count
)


assert (
    master_failure_count == 0
), (
    "Silver master-data quality gate "
    "has not passed."
)


assert (
    contract_failure_count == 0
), (
    "Silver contract quality gate "
    "has not passed."
)


print(
    "Required Silver quality gates confirmed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Load Silver sources**

# CELL ********************

silver_supplier_df = spark.table(
    SILVER_SUPPLIER_TABLE
)

silver_category_df = spark.table(
    SILVER_CATEGORY_TABLE
)

silver_material_df = spark.table(
    SILVER_MATERIAL_TABLE
)

silver_buyer_df = spark.table(
    SILVER_BUYER_TABLE
)

silver_business_unit_df = spark.table(
    SILVER_BUSINESS_UNIT_TABLE
)

silver_contract_df = spark.table(
    SILVER_CONTRACT_TABLE
)

silver_exchange_rate_df = spark.table(
    SILVER_EXCHANGE_RATE_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Suppliers:",
    f"{silver_supplier_df.count():,}"
)

print(
    "Categories:",
    f"{silver_category_df.count():,}"
)

print(
    "Materials:",
    f"{silver_material_df.count():,}"
)

print(
    "Buyers:",
    f"{silver_buyer_df.count():,}"
)

print(
    "Business units:",
    f"{silver_business_unit_df.count():,}"
)

print(
    "Contracts:",
    f"{silver_contract_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold metadata helper**

# CELL ********************

def add_gold_metadata(
    dataframe,
    source_table
):

    excluded_columns = {
        "GoldLoadTimestamp",
        "GoldLoadDate",
        "GoldRecordHash"
    }


    hash_columns = [
        column_name
        for column_name
        in dataframe.columns
        if column_name
        not in excluded_columns
    ]


    hash_components = [
        F.coalesce(
            F.col(
                column_name
            ).cast("string"),
            F.lit("__NULL__")
        )
        for column_name
        in hash_columns
    ]


    return (
        dataframe

        .withColumn(
            "GoldSourceTable",
            F.lit(
                source_table
            )
        )

        .withColumn(
            "GoldLoadTimestamp",
            F.current_timestamp()
        )

        .withColumn(
            "GoldLoadDate",
            F.current_date()
        )

        .withColumn(
            "GoldRecordHash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *hash_components
                ),
                256
            )
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_date_**

# CELL ********************

date_start_string = (
    DATE_START.isoformat()
)

date_end_string = (
    DATE_END.isoformat()
)


dim_date_df = (
    spark.sql(
        f"""
        SELECT
            explode(
                sequence(
                    to_date('{date_start_string}'),
                    to_date('{date_end_string}'),
                    interval 1 day
                )
            ) AS Date
        """
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add date attributes**

# CELL ********************

dim_date_df = (
    dim_date_df

    .withColumn(
        "DateKey",
        F.date_format(
            "Date",
            "yyyyMMdd"
        ).cast("int")
    )

    .withColumn(
        "DayOfMonth",
        F.dayofmonth(
            "Date"
        )
    )

    .withColumn(
        "DayOfYear",
        F.dayofyear(
            "Date"
        )
    )

    .withColumn(
        "ISOWeekdayNumber",
        (
            (
                F.dayofweek(
                    "Date"
                )
                + F.lit(5)
            )
            % F.lit(7)
        )
        + F.lit(1)
    )

    .withColumn(
        "DayName",
        F.date_format(
            "Date",
            "EEEE"
        )
    )

    .withColumn(
        "WeekOfYear",
        F.weekofyear(
            "Date"
        )
    )

    .withColumn(
        "MonthNumber",
        F.month(
            "Date"
        )
    )

    .withColumn(
        "MonthName",
        F.date_format(
            "Date",
            "MMMM"
        )
    )

    .withColumn(
        "MonthShortName",
        F.date_format(
            "Date",
            "MMM"
        )
    )

    .withColumn(
        "YearMonth",
        F.date_format(
            "Date",
            "yyyy-MM"
        )
    )

    .withColumn(
        "YearMonthSort",
        (
            F.year(
                "Date"
            )
            * F.lit(100)
        )
        +
        F.month(
            "Date"
        )
    )

    .withColumn(
        "QuarterNumber",
        F.quarter(
            "Date"
        )
    )

    .withColumn(
        "Quarter",
        F.concat(
            F.lit("Q"),
            F.quarter(
                "Date"
            )
        )
    )

    .withColumn(
        "YearQuarter",
        F.concat(
            F.year(
                "Date"
            ).cast("string"),
            F.lit("-Q"),
            F.quarter(
                "Date"
            ).cast("string")
        )
    )

    .withColumn(
        "Year",
        F.year(
            "Date"
        )
    )

    .withColumn(
        "IsWeekendFlag",
        F.col(
            "ISOWeekdayNumber"
        )
        >= 6
    )

    .withColumn(
        "MonthStartDate",
        F.trunc(
            "Date",
            "month"
        )
    )

    .withColumn(
        "YearStartDate",
        F.trunc(
            "Date",
            "year"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add metadata to _dim_date_**

# CELL ********************

dim_date_df = (
    add_gold_metadata(
        dataframe=dim_date_df,
        source_table="GENERATED_DATE_DIMENSION"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Prepare Supplier SCD2 source**
# 
# _dim_supplier_ is the one dimension where SCD Type 2 will be implemented.
# 
# Tracked attributes:
# 
# - SupplierName
# - SupplierType
# - Country
# - Region
# - PreferredSupplier
# - StrategicSupplier
# - ESGRating
# - FinancialRiskScore
# - Status
# - SupplierActiveFlag

# CELL ********************

SUPPLIER_TRACKED_COLUMNS = [
    "SupplierName",
    "SupplierType",
    "Country",
    "Region",
    "PreferredSupplier",
    "StrategicSupplier",
    "ESGRating",
    "FinancialRiskScore",
    "Status",
    "SupplierActiveFlag"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_hash_components = [
    F.coalesce(
        F.col(
            column_name
        ).cast("string"),
        F.lit("__NULL__")
    )
    for column_name
    in SUPPLIER_TRACKED_COLUMNS
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_source_df = (
    silver_supplier_df

    .select(
        "SupplierID",
        "SupplierName",
        "SupplierType",
        "Country",
        "Region",
        "PreferredSupplier",
        "StrategicSupplier",
        "ESGRating",
        "FinancialRiskScore",
        "CreatedDate",
        "Status",
        "SupplierActiveFlag",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )

    .withColumn(
        "SupplierAttributeHash",
        F.sha2(
            F.concat_ws(
                "||",
                *supplier_hash_components
            ),
            256
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Check wether Supplier dimension already exists**

# CELL ********************

supplier_dimension_exists = (
    spark.catalog.tableExists(
        DIM_SUPPLIER_TABLE
    )
)


print(
    "Existing dim_supplier:",
    supplier_dimension_exists
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build first Supplier SCD2 load**

# CELL ********************

# ============================================================
# Build First Supplier SCD Type 2 Load
# ============================================================

if not supplier_dimension_exists:

    dim_supplier_candidate_df = (
        supplier_source_df

        # ----------------------------------------------------
        # Initial SCD version
        # ----------------------------------------------------

        .withColumn(
            "DimensionVersion",
            F.lit(1).cast("int")
        )

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # This is the initial SCD bootstrap.
        #
        # Silver contains the current known supplier state,
        # but we do not have historical supplier snapshots
        # before the first Gold load.
        #
        # Therefore the initial Supplier version must cover
        # the full analytical history beginning DATE_START.
        #
        # CreatedDate remains a descriptive supplier attribute
        # and is NOT used as the initial SCD effective date.
        # ----------------------------------------------------

        .withColumn(
            "EffectiveFromDate",
            F.lit(
                DATE_START
            )
        )

        # ----------------------------------------------------
        # Open-ended current record
        # ----------------------------------------------------

        .withColumn(
            "EffectiveToDate",
            F.to_date(
                F.lit(
                    "9999-12-31"
                )
            )
        )

        .withColumn(
            "IsCurrentFlag",
            F.lit(True)
        )

        # ----------------------------------------------------
        # Deterministic Supplier surrogate key
        # ----------------------------------------------------

        .withColumn(
            "SupplierKey",

            F.xxhash64(
                F.col(
                    "SupplierID"
                ),

                F.col(
                    "EffectiveFromDate"
                ).cast("string"),

                F.col(
                    "DimensionVersion"
                ).cast("string")
            )
        )
    )


    dim_supplier_candidate_df = (
        add_gold_metadata(
            dataframe=(
                dim_supplier_candidate_df
            ),
            source_table=(
                SILVER_SUPPLIER_TABLE
            )
        )
    )


    print(
        "Prepared initial Supplier "
        "SCD Type 2 dimension."
    )

    print(
        "Initial Supplier EffectiveFromDate:",
        DATE_START
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build Supplier SCD2 changes on later runs**

# CELL ********************

if supplier_dimension_exists:

    existing_supplier_dim_df = (
        spark.table(
            DIM_SUPPLIER_TABLE
        )
    )


    current_supplier_ref_df = (
        existing_supplier_dim_df

        .filter(
            F.col(
                "IsCurrentFlag"
            )
        )

        .select(
            "SupplierID",

            F.col(
                "SupplierKey"
            ).alias(
                "CurrentSupplierKey"
            ),

            F.col(
                "SupplierAttributeHash"
            ).alias(
                "CurrentSupplierAttributeHash"
            ),

            F.col(
                "DimensionVersion"
            ).alias(
                "CurrentDimensionVersion"
            ),

            F.col(
                "EffectiveFromDate"
            ).alias(
                "CurrentEffectiveFromDate"
            )
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if supplier_dimension_exists:

    supplier_comparison_df = (
        supplier_source_df

        .join(
            current_supplier_ref_df,
            "SupplierID",
            "left"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build new Supplier versions**

# CELL ********************

# ============================================================
# Build New Supplier SCD Type 2 Versions
# ============================================================

if supplier_dimension_exists:

    new_supplier_versions_df = (
        supplier_comparison_df

        # ----------------------------------------------------
        # Keep:
        # 1. Completely new SupplierID
        # 2. Existing SupplierID whose tracked attributes changed
        # ----------------------------------------------------

        .filter(
            F.col(
                "CurrentSupplierKey"
            ).isNull()
            |
            (
                F.col(
                    "SupplierAttributeHash"
                )
                !=
                F.col(
                    "CurrentSupplierAttributeHash"
                )
            )
        )

        # ----------------------------------------------------
        # Version number
        # ----------------------------------------------------

        .withColumn(
            "DimensionVersion",

            F.when(
                F.col(
                    "CurrentSupplierKey"
                ).isNull(),

                F.lit(1)
            )

            .otherwise(
                F.col(
                    "CurrentDimensionVersion"
                )
                +
                F.lit(1)
            )

            .cast("int")
        )

        # ----------------------------------------------------
        # EffectiveFromDate
        #
        # IMPORTANT:
        #
        # After the initial bootstrap, any newly observed
        # supplier or changed supplier version becomes
        # effective from the current Gold snapshot date.
        #
        # We do NOT backdate a newly discovered SCD version
        # using Supplier CreatedDate.
        # ----------------------------------------------------

        .withColumn(
            "EffectiveFromDate",

            F.lit(
                SNAPSHOT_DATE
            )
        )

        # ----------------------------------------------------
        # Open-ended current version
        # ----------------------------------------------------

        .withColumn(
            "EffectiveToDate",

            F.to_date(
                F.lit(
                    "9999-12-31"
                )
            )
        )

        .withColumn(
            "IsCurrentFlag",
            F.lit(True)
        )

        # ----------------------------------------------------
        # Deterministic surrogate key
        # ----------------------------------------------------

        .withColumn(
            "SupplierKey",

            F.xxhash64(
                F.col(
                    "SupplierID"
                ),

                F.col(
                    "EffectiveFromDate"
                ).cast("string"),

                F.col(
                    "DimensionVersion"
                ).cast("string")
            )
        )

        # ----------------------------------------------------
        # Remove comparison-only fields
        # ----------------------------------------------------

        .drop(
            "CurrentSupplierKey",
            "CurrentSupplierAttributeHash",
            "CurrentDimensionVersion",
            "CurrentEffectiveFromDate"
        )
    )


    new_supplier_versions_df = (
        add_gold_metadata(
            dataframe=(
                new_supplier_versions_df
            ),
            source_table=(
                SILVER_SUPPLIER_TABLE
            )
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Close changed or removed Supplier versions**

# CELL ********************

if supplier_dimension_exists:

    source_supplier_hash_df = (
        supplier_source_df

        .select(
            "SupplierID",

            F.col(
                "SupplierAttributeHash"
            ).alias(
                "NewSupplierAttributeHash"
            )
        )
    )


    current_supplier_with_source_df = (
        existing_supplier_dim_df

        .filter(
            F.col(
                "IsCurrentFlag"
            )
        )

        .join(
            source_supplier_hash_df,
            "SupplierID",
            "left"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if supplier_dimension_exists:

    supplier_versions_to_close_df = (
        current_supplier_with_source_df

        .filter(
            F.col(
                "NewSupplierAttributeHash"
            ).isNull()
            |
            (
                F.col(
                    "SupplierAttributeHash"
                )
                !=
                F.col(
                    "NewSupplierAttributeHash"
                )
            )
        )

        .select(
            *existing_supplier_dim_df.columns
        )

        .withColumn(
            "EffectiveToDate",
            F.date_sub(
                F.lit(
                    SNAPSHOT_DATE
                ),
                1
            )
        )

        .withColumn(
            "IsCurrentFlag",
            F.lit(False)
        )
    )


    supplier_versions_to_close_df = (
        add_gold_metadata(
            dataframe=(
                supplier_versions_to_close_df
            ),
            source_table=(
                SILVER_SUPPLIER_TABLE
            )
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Keep unchanged Supplier versions and history**

# CELL ********************

if supplier_dimension_exists:

    supplier_keys_to_close_df = (
        supplier_versions_to_close_df

        .select(
            "SupplierKey"
        )
    )


    supplier_existing_keep_df = (
        existing_supplier_dim_df

        .join(
            supplier_keys_to_close_df,
            "SupplierKey",
            "left_anti"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Assemble Supplier SCD2 candidate**

# CELL ********************

if supplier_dimension_exists:

    dim_supplier_candidate_df = (
        supplier_existing_keep_df

        .unionByName(
            supplier_versions_to_close_df
        )

        .unionByName(
            new_supplier_versions_df
        )
    )


print(
    "Supplier dimension candidate rows:",
    f"{dim_supplier_candidate_df.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_category_**

# CELL ********************

dim_category_df = (
    silver_category_df

    .select(
        "CategoryID",
        "CategoryName",
        "CategoryManager",
        "CommodityGroup",
        "ProcurementType",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )

    .withColumn(
        "CategoryKey",
        F.xxhash64(
            F.col(
                "CategoryID"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_category_df = (
    add_gold_metadata(
        dataframe=dim_category_df,
        source_table=SILVER_CATEGORY_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_material_**

# CELL ********************

dim_material_df = (
    silver_material_df

    .select(
        "MaterialID",
        "MaterialDescription",
        "CategoryID",
        "StandardCost",
        "UnitOfMeasure",
        "Manufacturer",
        "Status",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )

    .withColumn(
        "MaterialKey",
        F.xxhash64(
            F.col(
                "MaterialID"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_material_df = (
    add_gold_metadata(
        dataframe=dim_material_df,
        source_table=SILVER_MATERIAL_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_buyer_**

# CELL ********************

dim_buyer_df = (
    silver_buyer_df

    .select(
        "BuyerID",
        "BuyerName",
        "Email",
        "Department",
        "BuyerRole",
        "BusinessUnitID",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )

    .withColumn(
        "BuyerKey",
        F.xxhash64(
            F.col(
                "BuyerID"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_buyer_df = (
    add_gold_metadata(
        dataframe=dim_buyer_df,
        source_table=SILVER_BUYER_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_business_unit_**

# CELL ********************

dim_business_unit_df = (
    silver_business_unit_df

    .select(
        "BusinessUnitID",
        "BusinessUnitName",
        "Country",
        "Region",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )

    .withColumn(
        "BusinessUnitKey",
        F.xxhash64(
            F.col(
                "BusinessUnitID"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_business_unit_df = (
    add_gold_metadata(
        dataframe=dim_business_unit_df,
        source_table=SILVER_BUSINESS_UNIT_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_contract_**

# CELL ********************

dim_contract_df = (
    silver_contract_df

    .select(
        "ContractID",
        "SupplierID",
        "CategoryID",

        "ContractStartDate",
        "ContractEndDate",

        "Currency",
        "ContractValue",
        "NegotiatedUnitPrice",

        "ContractOwnerBuyerID",
        "SourceContractOwner",

        "ContractType",
        "SourceContractStatus",

        "PaymentTermsDays",
        "AutoRenewalFlag",

        "ContractLifecycleStatus",
        "IsContractActiveAsOfDate",

        "ContractDurationDays",
        "ContractStartYear",
        "ContractEndYear",
        "DaysToExpiryAsOfDate",

        F.col(
            "SilverRecordHash"
        ).alias(
            "SourceSilverRecordHash"
        )
    )

    .withColumn(
        "ContractKey",
        F.xxhash64(
            F.col(
                "ContractID"
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_contract_df = (
    add_gold_metadata(
        dataframe=dim_contract_df,
        source_table=SILVER_CONTRACT_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build _dim_currency_**
# 
# Exchange rates stay in Silver. Gold _dim_currency_ contains only the conformed currency entity and data-coverage attributes.

# CELL ********************

dim_currency_df = (
    silver_exchange_rate_df

    .groupBy(
        "Currency"
    )

    .agg(
        F.min(
            "RateDate"
        ).alias(
            "FirstAvailableRateDate"
        ),

        F.max(
            "RateDate"
        ).alias(
            "LastAvailableRateDate"
        ),

        F.countDistinct(
            "RateDate"
        ).alias(
            "AvailableRateDateCount"
        )
    )

    .withColumnRenamed(
        "Currency",
        "CurrencyCode"
    )

    .withColumn(
        "CurrencyKey",
        F.xxhash64(
            F.col(
                "CurrencyCode"
            )
        )
    )

    .withColumn(
        "IsBaseCurrencyFlag",
        (
            F.col(
                "CurrencyCode"
            )
            == "EUR"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_currency_df = (
    add_gold_metadata(
        dataframe=dim_currency_df,
        source_table=SILVER_EXCHANGE_RATE_TABLE
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation framework**

# CELL ********************

validation_results = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def register_validation(
    table_name,
    category,
    rule,
    failed_count,
    details=""
):

    failed_count = int(
        failed_count or 0
    )

    validation_results.append(
        {
            "TableName":
                table_name,

            "ValidationCategory":
                category,

            "ValidationRule":
                rule,

            "FailedRecordCount":
                failed_count,

            "ValidationStatus":
                (
                    "PASSED"
                    if failed_count == 0
                    else "FAILED"
                ),

            "ValidationDetails":
                details
        }
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Generic dimension-key validation helper**

# CELL ********************

def validate_dimension_keys(
    dataframe,
    table_name,
    natural_key,
    surrogate_key
):

    null_natural_key_count = (
        dataframe

        .filter(
            F.col(
                natural_key
            ).isNull()
        )

        .count()
    )


    duplicate_natural_key_count = (
        dataframe

        .groupBy(
            natural_key
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()
    )


    null_surrogate_key_count = (
        dataframe

        .filter(
            F.col(
                surrogate_key
            ).isNull()
        )

        .count()
    )


    duplicate_surrogate_key_count = (
        dataframe

        .groupBy(
            surrogate_key
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()
    )


    register_validation(
        table_name,
        "Natural Key",
        f"{natural_key} is not null",
        null_natural_key_count
    )


    register_validation(
        table_name,
        "Natural Key",
        f"{natural_key} is unique",
        duplicate_natural_key_count
    )


    register_validation(
        table_name,
        "Surrogate Key",
        f"{surrogate_key} is not null",
        null_surrogate_key_count
    )


    register_validation(
        table_name,
        "Surrogate Key",
        f"{surrogate_key} is unique",
        duplicate_surrogate_key_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Date dimension**

# CELL ********************

expected_date_count = (
    DATE_END
    -
    DATE_START
).days + 1


actual_date_count = (
    dim_date_df.count()
)


register_validation(
    DIM_DATE_TABLE,
    "Row Count",
    "Date dimension contains every configured date",
    abs(
        expected_date_count
        -
        actual_date_count
    ),
    (
        f"Expected: "
        f"{expected_date_count:,}; "
        f"Actual: "
        f"{actual_date_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_dimension_keys(
    dataframe=dim_date_df,
    table_name=DIM_DATE_TABLE,
    natural_key="Date",
    surrogate_key="DateKey"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier SCD Type 2**
# 
# Unlike Type 1 dimensions, Suppl__ierID is intentionally not unique across all history.

# CELL ********************

supplier_key_duplicate_count = (
    dim_supplier_candidate_df

    .groupBy(
        "SupplierKey"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


register_validation(
    DIM_SUPPLIER_TABLE,
    "Surrogate Key",
    "SupplierKey is unique across SCD history",
    supplier_key_duplicate_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_current_duplicate_count = (
    dim_supplier_candidate_df

    .filter(
        F.col(
            "IsCurrentFlag"
        )
    )

    .groupBy(
        "SupplierID"
    )

    .count()

    .filter(
        F.col("count") != 1
    )

    .count()
)


register_validation(
    DIM_SUPPLIER_TABLE,
    "SCD Type 2",
    (
        "Each current SupplierID has "
        "exactly one current version"
    ),
    supplier_current_duplicate_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate current Supplier coverage**

# CELL ********************

source_supplier_count = (
    supplier_source_df

    .select(
        "SupplierID"
    )

    .distinct()

    .count()
)


current_dimension_supplier_count = (
    dim_supplier_candidate_df

    .filter(
        F.col(
            "IsCurrentFlag"
        )
    )

    .select(
        "SupplierID"
    )

    .distinct()

    .count()
)


register_validation(
    DIM_SUPPLIER_TABLE,
    "Row Count",

    (
        "Current Supplier dimension "
        "matches Silver Supplier population"
    ),

    abs(
        source_supplier_count
        -
        current_dimension_supplier_count
    ),

    (
        f"Silver suppliers: "
        f"{source_supplier_count:,}; "
        f"Current Gold suppliers: "
        f"{current_dimension_supplier_count:,}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Supplier SCD dates**

# CELL ********************

invalid_supplier_scd_date_count = (
    dim_supplier_candidate_df

    .filter(
        F.col(
            "EffectiveFromDate"
        ).isNull()
        |
        F.col(
            "EffectiveToDate"
        ).isNull()
        |
        (
            F.col(
                "EffectiveToDate"
            )
            <
            F.col(
                "EffectiveFromDate"
            )
        )
    )

    .count()
)


register_validation(
    DIM_SUPPLIER_TABLE,
    "SCD Type 2",

    (
        "Supplier effective-date "
        "ranges are valid"
    ),

    invalid_supplier_scd_date_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate remaining dimension keys**

# CELL ********************

validate_dimension_keys(
    dataframe=dim_category_df,
    table_name=DIM_CATEGORY_TABLE,
    natural_key="CategoryID",
    surrogate_key="CategoryKey"
)


validate_dimension_keys(
    dataframe=dim_material_df,
    table_name=DIM_MATERIAL_TABLE,
    natural_key="MaterialID",
    surrogate_key="MaterialKey"
)


validate_dimension_keys(
    dataframe=dim_buyer_df,
    table_name=DIM_BUYER_TABLE,
    natural_key="BuyerID",
    surrogate_key="BuyerKey"
)


validate_dimension_keys(
    dataframe=dim_business_unit_df,
    table_name=DIM_BUSINESS_UNIT_TABLE,
    natural_key="BusinessUnitID",
    surrogate_key="BusinessUnitKey"
)


validate_dimension_keys(
    dataframe=dim_contract_df,
    table_name=DIM_CONTRACT_TABLE,
    natural_key="ContractID",
    surrogate_key="ContractKey"
)


validate_dimension_keys(
    dataframe=dim_currency_df,
    table_name=DIM_CURRENCY_TABLE,
    natural_key="CurrencyCode",
    surrogate_key="CurrencyKey"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Type 1 row preservation**

# CELL ********************

dimension_row_checks = [
    (
        DIM_CATEGORY_TABLE,
        silver_category_df,
        dim_category_df,
        "CategoryID"
    ),

    (
        DIM_MATERIAL_TABLE,
        silver_material_df,
        dim_material_df,
        "MaterialID"
    ),

    (
        DIM_BUYER_TABLE,
        silver_buyer_df,
        dim_buyer_df,
        "BuyerID"
    ),

    (
        DIM_BUSINESS_UNIT_TABLE,
        silver_business_unit_df,
        dim_business_unit_df,
        "BusinessUnitID"
    ),

    (
        DIM_CONTRACT_TABLE,
        silver_contract_df,
        dim_contract_df,
        "ContractID"
    )
]


for (
    table_name,
    source_df,
    dimension_df,
    natural_key
) in dimension_row_checks:

    source_count = (
        source_df
        .select(
            natural_key
        )
        .distinct()
        .count()
    )

    dimension_count = (
        dimension_df.count()
    )

    register_validation(
        table_name,
        "Row Count",

        (
            "One Gold dimension row exists "
            "per Silver natural key"
        ),

        abs(
            source_count
            -
            dimension_count
        ),

        (
            f"Silver: "
            f"{source_count:,}; "
            f"Gold: "
            f"{dimension_count:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Material -> Category references**

# CELL ********************

unresolved_material_category_count = (
    dim_material_df

    .join(
        dim_category_df
        .select(
            "CategoryID"
        ),

        "CategoryID",

        "left_anti"
    )

    .count()
)


register_validation(
    DIM_MATERIAL_TABLE,
    "Referential Integrity",

    (
        "All Material CategoryID values "
        "resolve to dim_category"
    ),

    unresolved_material_category_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Buyer -> Business Unit references**

# CELL ********************

unresolved_buyer_bu_count = (
    dim_buyer_df

    .join(
        dim_business_unit_df
        .select(
            "BusinessUnitID"
        ),

        "BusinessUnitID",

        "left_anti"
    )

    .count()
)


register_validation(
    DIM_BUYER_TABLE,
    "Referential Integrity",

    (
        "All Buyer BusinessUnitID values "
        "resolve to dim_business_unit"
    ),

    unresolved_buyer_bu_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Contract references**

# CELL ********************

unresolved_contract_supplier_count = (
    dim_contract_df

    .join(
        dim_supplier_candidate_df

        .filter(
            F.col(
                "IsCurrentFlag"
            )
        )

        .select(
            "SupplierID"
        ),

        "SupplierID",

        "left_anti"
    )

    .count()
)


unresolved_contract_category_count = (
    dim_contract_df

    .join(
        dim_category_df
        .select(
            "CategoryID"
        ),

        "CategoryID",

        "left_anti"
    )

    .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

register_validation(
    DIM_CONTRACT_TABLE,
    "Referential Integrity",

    (
        "All Contract SupplierID values "
        "resolve to current dim_supplier"
    ),

    unresolved_contract_supplier_count
)


register_validation(
    DIM_CONTRACT_TABLE,
    "Referential Integrity",

    (
        "All Contract CategoryID values "
        "resolve to dim_category"
    ),

    unresolved_contract_category_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Gold lineage**

# CELL ********************

gold_dimension_frames = [
    (
        DIM_DATE_TABLE,
        dim_date_df
    ),

    (
        DIM_SUPPLIER_TABLE,
        dim_supplier_candidate_df
    ),

    (
        DIM_CATEGORY_TABLE,
        dim_category_df
    ),

    (
        DIM_MATERIAL_TABLE,
        dim_material_df
    ),

    (
        DIM_BUYER_TABLE,
        dim_buyer_df
    ),

    (
        DIM_BUSINESS_UNIT_TABLE,
        dim_business_unit_df
    ),

    (
        DIM_CONTRACT_TABLE,
        dim_contract_df
    ),

    (
        DIM_CURRENCY_TABLE,
        dim_currency_df
    )
]


for (
    table_name,
    dataframe
) in gold_dimension_frames:

    invalid_metadata_count = (
        dataframe

        .filter(
            F.col(
                "GoldLoadTimestamp"
            ).isNull()
            |
            F.col(
                "GoldLoadDate"
            ).isNull()
            |
            F.col(
                "GoldRecordHash"
            ).isNull()
            |
            (
                F.length(
                    "GoldRecordHash"
                )
                != 64
            )
        )

        .count()
    )


    register_validation(
        table_name,
        "Lineage",

        (
            "Gold load metadata and "
            "record hash are complete"
        ),

        invalid_metadata_count
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Build validation results**

# CELL ********************

validation_results_df = (
    spark.createDataFrame(
        validation_results
    )

    .withColumn(
        "ExecutionTimestamp",
        F.current_timestamp()
    )
)


display(
    validation_results_df

    .orderBy(
        "ValidationStatus",
        "TableName",
        "ValidationCategory",
        "ValidationRule"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Count failures**

# CELL ********************

pre_write_failure_count = (
    validation_results_df

    .filter(
        F.col(
            "ValidationStatus"
        )
        ==
        "FAILED"
    )

    .count()
)


print(
    "Gold dimension pre-write failures:",
    pre_write_failure_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Persist monitoring table**

# CELL ********************

(
    validation_results_df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        GOLD_MONITORING_TABLE
    )
)


print(
    "Created Gold monitoring table:",
    GOLD_MONITORING_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Pre-write quality gate**

# CELL ********************

if pre_write_failure_count > 0:

    display(
        validation_results_df

        .filter(
            F.col(
                "ValidationStatus"
            )
            ==
            "FAILED"
        )

        .orderBy(
            F.desc(
                "FailedRecordCount"
            )
        )
    )

    raise AssertionError(
        f"Gold dimension validation "
        f"failed with "
        f"{pre_write_failure_count} "
        f"failed rules."
    )


print(
    "GOLD DIMENSION PRE-WRITE "
    "QUALITY GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold write helper**

# CELL ********************

def write_gold_dimension(
    dataframe,
    table_name
):

    (
        dataframe.write

        .format("delta")

        .mode("overwrite")

        .option(
            "overwriteSchema",
            "true"
        )

        .saveAsTable(
            table_name
        )
    )


    print(
        "Created Gold dimension:",
        table_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Gold dimensions**

# CELL ********************

write_gold_dimension(
    dim_date_df,
    DIM_DATE_TABLE
)


write_gold_dimension(
    dim_supplier_candidate_df,
    DIM_SUPPLIER_TABLE
)


write_gold_dimension(
    dim_category_df,
    DIM_CATEGORY_TABLE
)


write_gold_dimension(
    dim_material_df,
    DIM_MATERIAL_TABLE
)


write_gold_dimension(
    dim_buyer_df,
    DIM_BUYER_TABLE
)


write_gold_dimension(
    dim_business_unit_df,
    DIM_BUSINESS_UNIT_TABLE
)


write_gold_dimension(
    dim_contract_df,
    DIM_CONTRACT_TABLE
)


write_gold_dimension(
    dim_currency_df,
    DIM_CURRENCY_TABLE
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted dimensions**

# CELL ********************

persisted_dimensions = [
    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE,
    DIM_CATEGORY_TABLE,
    DIM_MATERIAL_TABLE,
    DIM_BUYER_TABLE,
    DIM_BUSINESS_UNIT_TABLE,
    DIM_CONTRACT_TABLE,
    DIM_CURRENCY_TABLE
]


for table_name in persisted_dimensions:

    persisted_count = (
        spark.table(
            table_name
        ).count()
    )

    print(
        f"{table_name}: "
        f"{persisted_count:,} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Verify persisted dimensions**

# CELL ********************

persisted_dimensions = [
    DIM_DATE_TABLE,
    DIM_SUPPLIER_TABLE,
    DIM_CATEGORY_TABLE,
    DIM_MATERIAL_TABLE,
    DIM_BUYER_TABLE,
    DIM_BUSINESS_UNIT_TABLE,
    DIM_CONTRACT_TABLE,
    DIM_CURRENCY_TABLE
]


for table_name in persisted_dimensions:

    persisted_count = (
        spark.table(
            table_name
        ).count()
    )

    print(
        f"{table_name}: "
        f"{persisted_count:,} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Preview Supplier SCD2**

# CELL ********************

display(
    spark.table(
        DIM_SUPPLIER_TABLE
    )

    .select(
        "SupplierKey",
        "SupplierID",
        "SupplierName",

        "SupplierType",
        "Country",
        "Region",

        "PreferredSupplier",
        "StrategicSupplier",

        "ESGRating",
        "FinancialRiskScore",

        "Status",
        "SupplierActiveFlag",

        "DimensionVersion",
        "EffectiveFromDate",
        "EffectiveToDate",
        "IsCurrentFlag"
    )

    .orderBy(
        "SupplierID",
        "DimensionVersion"
    )

    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Preview dimensions**

# CELL ********************

display(
    spark.table(
        DIM_CATEGORY_TABLE
    )
    .orderBy(
        "CategoryID"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table(
        DIM_CURRENCY_TABLE
    )
    .orderBy(
        "CurrencyCode"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Final Gold dimension status**

# CELL ********************

print(
    "NB_30_Build_Gold_Dimensions "
    "completed successfully."
)

print()

print(
    "Physical Gold Lakehouse:"
)

print(
    "  lh_procurement_gold"
)

print()

print(
    "Physical Gold dimensions:"
)

print(
    "  - dim_date"
)

print(
    "  - dim_supplier "
    "(SCD Type 2)"
)

print(
    "  - dim_category"
)

print(
    "  - dim_material"
)

print(
    "  - dim_buyer"
)

print(
    "  - dim_business_unit"
)

print(
    "  - dim_contract"
)

print(
    "  - dim_currency"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
