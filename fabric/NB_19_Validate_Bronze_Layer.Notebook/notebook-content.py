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

#Configuration

from datetime import date

PROFILE = "development"

START_DATE = date(2022, 1, 1)
AS_OF_DATE = date(2026, 7, 31)

EXPECTED_EXCHANGE_RATE_DAYS = (
    AS_OF_DATE - START_DATE
).days + 1

EXPECTED_CURRENCY_COUNT = 8

EXPECTED_EXCHANGE_RATE_ROWS = (
    EXPECTED_EXCHANGE_RATE_DAYS
    * EXPECTED_CURRENCY_COUNT
)

PO_AMOUNT_TOLERANCE = 0.01
INVOICE_AMOUNT_TOLERANCE = 0.01
QUANTITY_TOLERANCE = 0.001

MONITORING_TABLE = (
    "monitoring_bronze_data_quality_results"
)

REQUIRED_TABLES = [
    "bronze_category",
    "bronze_business_unit",
    "bronze_buyer",
    "bronze_supplier",
    "bronze_material",
    "bronze_exchange_rate",
    "bronze_contract",
    "bronze_purchase_order_header",
    "bronze_purchase_order_item",
    "bronze_goods_receipt",
    "bronze_invoice_header",
    "bronze_invoice_item",
    "bronze_savings_project"
]

print(f"Profile: {PROFILE}")
print(f"Validation as-of date: {AS_OF_DATE}")
print(
    f"Required Bronze tables: "
    f"{len(REQUIRED_TABLES)}"
)
print(
    f"Expected exchange-rate rows: "
    f"{EXPECTED_EXCHANGE_RATE_ROWS:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Imports
from functools import reduce
from operator import and_, or_

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

print("Validation libraries loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_results = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validation Framework
def register_validation(
    table_name,
    validation_category,
    validation_rule,
    failed_record_count,
    severity="ERROR",
    validation_details=""
):
    failed_record_count = int(
        failed_record_count or 0
    )

    if failed_record_count == 0:
        validation_status = "PASSED"

    elif severity == "WARNING":
        validation_status = "WARNING"

    else:
        validation_status = "FAILED"

    validation_results.append({
        "TableName": table_name,
        "ValidationCategory": (
            validation_category
        ),
        "ValidationRule": validation_rule,
        "Severity": severity,
        "FailedRecordCount": (
            failed_record_count
        ),
        "ValidationStatus": (
            validation_status
        ),
        "ValidationDetails": (
            validation_details
        )
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create Monitoring dataframe
VALIDATION_RESULT_SCHEMA = StructType([
    StructField(
        "ValidationID",
        StringType(),
        False
    ),
    StructField(
        "TableName",
        StringType(),
        False
    ),
    StructField(
        "ValidationCategory",
        StringType(),
        False
    ),
    StructField(
        "ValidationRule",
        StringType(),
        False
    ),
    StructField(
        "Severity",
        StringType(),
        False
    ),
    StructField(
        "FailedRecordCount",
        LongType(),
        False
    ),
    StructField(
        "ValidationStatus",
        StringType(),
        False
    ),
    StructField(
        "ValidationDetails",
        StringType(),
        True
    )
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_validation_results_df():
    result_rows = []

    for index, result in enumerate(
        validation_results,
        start=1
    ):
        result_rows.append({
            "ValidationID": (
                f"VAL{index:05d}"
            ),
            "TableName": (
                result["TableName"]
            ),
            "ValidationCategory": (
                result[
                    "ValidationCategory"
                ]
            ),
            "ValidationRule": (
                result["ValidationRule"]
            ),
            "Severity": (
                result["Severity"]
            ),
            "FailedRecordCount": (
                result[
                    "FailedRecordCount"
                ]
            ),
            "ValidationStatus": (
                result[
                    "ValidationStatus"
                ]
            ),
            "ValidationDetails": (
                result[
                    "ValidationDetails"
                ]
            )
        })

    return (
        spark.createDataFrame(
            result_rows,
            schema=VALIDATION_RESULT_SCHEMA
        )
        .withColumn(
            "ExecutionTimestamp",
            F.current_timestamp()
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def persist_validation_results():
    validation_results_df = (
        build_validation_results_df()
    )

    (
        validation_results_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            MONITORING_TABLE
        )
    )

    return validation_results_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Build boolean conditions
def combine_with_or(conditions):
    if not conditions:
        return F.lit(False)

    return reduce(
        or_,
        conditions
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def combine_with_and(conditions):
    if not conditions:
        return F.lit(True)

    return reduce(
        and_,
        conditions
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def combine_with_and(conditions):
    if not conditions:
        return F.lit(True)

    return reduce(
        and_,
        conditions
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def null_key_failure_count(
    dataframe,
    key_columns
):
    null_condition = combine_with_or([
        F.col(column_name).isNull()
        for column_name in key_columns
    ])

    return (
        dataframe
        .filter(null_condition)
        .count()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def duplicate_key_failure_count(
    dataframe,
    key_columns
):
    duplicate_summary_df = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(
            F.col("count") > 1
        )
    )

    duplicate_count_row = (
        duplicate_summary_df
        .agg(
            F.coalesce(
                F.sum(
                    F.col("count")
                    - F.lit(1)
                ),
                F.lit(0)
            ).alias(
                "DuplicateRecordCount"
            )
        )
        .first()
    )

    return int(
        duplicate_count_row[
            "DuplicateRecordCount"
        ]
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def foreign_key_failure_count(
    child_df,
    child_columns,
    parent_df,
    parent_columns,
    ignore_nulls=False
):
    source_df = child_df

    if ignore_nulls:
        non_null_condition = (
            combine_with_and([
                F.col(column_name)
                .isNotNull()
                for column_name
                in child_columns
            ])
        )

        source_df = source_df.filter(
            non_null_condition
        )

    source_df = source_df.alias("child")

    parent_key_df = (
        parent_df
        .select(*parent_columns)
        .dropDuplicates()
        .alias("parent")
    )

    join_conditions = [
        F.col(
            f"child.{child_column}"
        )
        == F.col(
            f"parent.{parent_column}"
        )
        for child_column, parent_column
        in zip(
            child_columns,
            parent_columns
        )
    ]

    join_condition = combine_with_and(
        join_conditions
    )

    return (
        source_df
        .join(
            parent_key_df,
            join_condition,
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

#Table Availability
table_availability = {
    table_name: (
        spark.catalog.tableExists(
            table_name
        )
    )
    for table_name in REQUIRED_TABLES
}

for (
    table_name,
    table_exists
) in table_availability.items():
    register_validation(
        table_name=table_name,
        validation_category=(
            "Table Availability"
        ),
        validation_rule=(
            "Required table exists"
        ),
        failed_record_count=(
            0 if table_exists else 1
        ),
        validation_details=(
            f"Table exists: {table_exists}"
        )
    )

missing_tables = [
    table_name
    for table_name, table_exists
    in table_availability.items()
    if not table_exists
]

if missing_tables:
    preliminary_results_df = (
        persist_validation_results()
    )

    display(preliminary_results_df)

    raise RuntimeError(
        "Missing required Bronze tables: "
        + ", ".join(missing_tables)
    )

print(
    "All required Bronze tables exist."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#patching missing bronze audit metadata
from delta.tables import DeltaTable
from pyspark.sql import functions as F


AUDIT_PATCH_TABLES = [
    "bronze_category",
    "bronze_business_unit",
    "bronze_buyer",
    "bronze_supplier",
    "bronze_exchange_rate",
    "bronze_contract"
]


TECHNICAL_COLUMNS = {
    "SourceSystem",
    "IngestionTimestamp",
    "LoadDate",
    "SourceRecordHash"
}


def build_hash_expression(
    table_name
):
    dataframe = spark.table(
        table_name
    )

    business_columns = [
        column_name
        for column_name
        in dataframe.columns
        if column_name
        not in TECHNICAL_COLUMNS
    ]

    hash_components = [
        (
            "coalesce("
            f"cast(`{column_name}` as string), "
            "'__NULL__'"
            ")"
        )
        for column_name
        in business_columns
    ]

    return (
        "sha2("
        "concat_ws("
        "'||', "
        + ", ".join(hash_components)
        + "), "
        "256"
        ")"
    )


for table_name in AUDIT_PATCH_TABLES:
    current_columns = set(
        spark.table(
            table_name
        ).columns
    )

    missing_columns = []

    if "SourceSystem" not in current_columns:
        missing_columns.append(
            "`SourceSystem` STRING"
        )

    if "IngestionTimestamp" not in current_columns:
        missing_columns.append(
            "`IngestionTimestamp` TIMESTAMP"
        )

    if "LoadDate" not in current_columns:
        missing_columns.append(
            "`LoadDate` DATE"
        )

    if "SourceRecordHash" not in current_columns:
        missing_columns.append(
            "`SourceRecordHash` STRING"
        )

    if missing_columns:
        spark.sql(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMNS (
                {", ".join(missing_columns)}
            )
            """
        )

    delta_table = DeltaTable.forName(
        spark,
        table_name
    )

    update_expressions = {}

    refreshed_columns = set(
        spark.table(
            table_name
        ).columns
    )

    if "SourceSystem" in refreshed_columns:
        update_expressions[
            "SourceSystem"
        ] = (
            "coalesce("
            "SourceSystem, "
            "'SYNTHETIC_SAP'"
            ")"
        )

    if "IngestionTimestamp" in refreshed_columns:
        update_expressions[
            "IngestionTimestamp"
        ] = (
            "coalesce("
            "IngestionTimestamp, "
            "current_timestamp()"
            ")"
        )

    if "LoadDate" in refreshed_columns:
        update_expressions[
            "LoadDate"
        ] = (
            "coalesce("
            "LoadDate, "
            "current_date()"
            ")"
        )

    update_expressions[
        "SourceRecordHash"
    ] = build_hash_expression(
        table_name
    )

    delta_table.update(
        set=update_expressions
    )

    print(
        f"Audit metadata patched: "
        f"{table_name}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Verify the patch
for table_name in AUDIT_PATCH_TABLES:
    dataframe = spark.table(
        table_name
    )

    missing_columns = [
        column_name
        for column_name in [
            "SourceSystem",
            "IngestionTimestamp",
            "LoadDate",
            "SourceRecordHash"
        ]
        if column_name
        not in dataframe.columns
    ]

    null_audit_count = (
        dataframe
        .filter(
            F.col("SourceSystem").isNull()
            |
            F.col(
                "IngestionTimestamp"
            ).isNull()
            |
            F.col("LoadDate").isNull()
            |
            F.col(
                "SourceRecordHash"
            ).isNull()
        )
        .count()
    )

    invalid_hash_count = (
        dataframe
        .filter(
            F.col(
                "SourceRecordHash"
            ).isNotNull()
            &
            (
                F.length(
                    "SourceRecordHash"
                ) != 64
            )
        )
        .count()
    )

    print(
        f"{table_name} | "
        f"missing columns: {missing_columns} | "
        f"null audit rows: {null_audit_count} | "
        f"invalid hashes: {invalid_hash_count}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load Bronze tables
bronze_tables = {
    table_name: spark.table(table_name)
    for table_name in REQUIRED_TABLES
}

category_df = bronze_tables[
    "bronze_category"
]

business_unit_df = bronze_tables[
    "bronze_business_unit"
]

buyer_df = bronze_tables[
    "bronze_buyer"
]

supplier_df = bronze_tables[
    "bronze_supplier"
]

material_df = bronze_tables[
    "bronze_material"
]

exchange_rate_df = bronze_tables[
    "bronze_exchange_rate"
]

contract_df = bronze_tables[
    "bronze_contract"
]

po_header_df = bronze_tables[
    "bronze_purchase_order_header"
]

po_item_df = bronze_tables[
    "bronze_purchase_order_item"
]

goods_receipt_df = bronze_tables[
    "bronze_goods_receipt"
]

invoice_header_df = bronze_tables[
    "bronze_invoice_header"
]

invoice_item_df = bronze_tables[
    "bronze_invoice_item"
]

savings_project_df = bronze_tables[
    "bronze_savings_project"
]

print("Bronze tables loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate row counts
table_row_counts = {
    table_name: dataframe.count()
    for table_name, dataframe
    in bronze_tables.items()
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EXPECTED_EXACT_ROW_COUNTS = {
    "bronze_category": 20,
    "bronze_business_unit": 10,
    "bronze_buyer": 30,
    "bronze_supplier": 500,
    "bronze_material": 2_000,
    "bronze_exchange_rate": (
        EXPECTED_EXCHANGE_RATE_ROWS
    ),
    "bronze_contract": 800,
    "bronze_purchase_order_header": (
        20_000
    ),
    "bronze_savings_project": 3_000
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for (
    table_name,
    expected_count
) in EXPECTED_EXACT_ROW_COUNTS.items():
    actual_count = table_row_counts[
        table_name
    ]

    failed_count = abs(
        actual_count - expected_count
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Row Count"
        ),
        validation_rule=(
            f"Row count equals "
            f"{expected_count:,}"
        ),
        failed_record_count=(
            failed_count
        ),
        validation_details=(
            f"Expected: {expected_count:,}; "
            f"actual: {actual_count:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VARIABLE_VOLUME_TABLES = [
    "bronze_purchase_order_item",
    "bronze_goods_receipt",
    "bronze_invoice_header",
    "bronze_invoice_item"
]

for table_name in VARIABLE_VOLUME_TABLES:
    actual_count = table_row_counts[
        table_name
    ]

    register_validation(
        table_name=table_name,
        validation_category=(
            "Row Count"
        ),
        validation_rule=(
            "Table contains at least one row"
        ),
        failed_record_count=(
            0 if actual_count > 0 else 1
        ),
        validation_details=(
            f"Actual rows: {actual_count:,}"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate required columns
REQUIRED_CORE_COLUMNS = {
    "bronze_category": [
        "CategoryID",
        "CategoryName",
        "ProcurementType"
    ],
    "bronze_business_unit": [
        "BusinessUnitID"
    ],
    "bronze_buyer": [
        "BuyerID",
        "BuyerName",
        "BuyerRole",
        "BusinessUnitID"
    ],
    "bronze_supplier": [
        "SupplierID",
        "SupplierName",
        "SupplierType",
        "Country",
        "Region",
        "PreferredSupplier",
        "StrategicSupplier",
        "ESGRating",
        "FinancialRiskScore",
        "Status"
    ],
    "bronze_material": [
        "MaterialID",
        "MaterialDescription",
        "CategoryID",
        "StandardCost",
        "UnitOfMeasure",
        "Status"
    ],
    "bronze_exchange_rate": [
        "RateDate",
        "Currency",
        "ExchangeRateEUR"
    ],
    "bronze_contract": [
        "ContractID",
        "SupplierID",
        "CategoryID",
        "ContractStartDate",
        "ContractEndDate",
        "Currency",
        "ContractValue",
        "NegotiatedUnitPrice",
        "ContractOwnerBuyerID",
        "ContractStatus"
    ],
    "bronze_purchase_order_header": [
        "POID",
        "SupplierID",
        "BuyerID",
        "BusinessUnitID",
        "OrderDate",
        "Currency",
        "POStatus",
        "TotalAmount",
        "AmountReconciliationStatus"
    ],
    "bronze_purchase_order_item": [
        "POItemID",
        "POID",
        "POLineNumber",
        "MaterialID",
        "CategoryID",
        "ContractID",
        "Quantity",
        "OrderUnit",
        "UnitPrice",
        "LineAmount",
        "Currency",
        "RequestedDeliveryDate",
        "POItemStatus",
        "SimulationContractScenario"
    ],
    "bronze_goods_receipt": [
        "GoodsReceiptID",
        "POID",
        "POItemID",
        "ReceiptSequence",
        "MaterialID",
        "SupplierID",
        "BusinessUnitID",
        "ReceiptDate",
        "QuantityReceived",
        "UnitOfMeasure",
        "ReceiptStatus",
        "DeliveryCompleteFlag",
        "IsLateReceipt",
        "DaysLate"
    ],
    "bronze_invoice_header": [
        "InvoiceID",
        "InvoiceNumber",
        "POID",
        "SupplierID",
        "BusinessUnitID",
        "InvoiceDate",
        "PostingDate",
        "DueDate",
        "Currency",
        "TotalInvoiceAmount",
        "InvoiceStatus",
        "PaymentStatus",
        "DisputeFlag",
        "DisputeReason",
        "DuplicateInvoiceFlag",
        "OriginalInvoiceID",
        "AmountReconciliationStatus"
    ],
    "bronze_invoice_item": [
        "InvoiceItemID",
        "InvoiceID",
        "InvoiceLineNumber",
        "POID",
        "POItemID",
        "MaterialID",
        "CategoryID",
        "ContractID",
        "InvoicedQuantity",
        "UnitOfMeasure",
        "POUnitPrice",
        "InvoiceUnitPrice",
        "NetAmount",
        "TaxRate",
        "TaxAmount",
        "GrossAmount",
        "Currency",
        "ReceivedQuantityAtInvoiceDate",
        "PriceVarianceAmount",
        "PriceVariancePercentage",
        "QuantityVariance",
        "QuantityVariancePercentage",
        "ThreeWayMatchStatus",
        "DuplicateInvoiceLineFlag",
        "OriginalInvoiceItemID"
    ],
    "bronze_savings_project": [
        "SavingsProjectID",
        "SavingsProjectName",
        "SupplierID",
        "CategoryID",
        "BuyerID",
        "BusinessUnitID",
        "ContractID",
        "SavingsType",
        "ProjectStatus",
        "SavingsLevel",
        "ApprovalStatus",
        "ProjectCreatedDate",
        "PlannedStartDate",
        "PlannedCompletionDate",
        "ActualCompletionDate",
        "CancellationDate",
        "Currency",
        "BaselineSpend",
        "ForecastedSavings",
        "ApprovedSavings",
        "RealizedSavings",
        "SourceExtractDate"
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

COMMON_AUDIT_COLUMNS = [
    "SourceSystem",
    "IngestionTimestamp",
    "LoadDate",
    "SourceRecordHash"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_with_missing_core_columns = []

for (
    table_name,
    required_columns
) in REQUIRED_CORE_COLUMNS.items():
    actual_columns = set(
        bronze_tables[
            table_name
        ].columns
    )

    missing_core_columns = [
        column_name
        for column_name
        in required_columns
        if column_name
        not in actual_columns
    ]

    missing_audit_columns = [
        column_name
        for column_name
        in COMMON_AUDIT_COLUMNS
        if column_name
        not in actual_columns
    ]

    register_validation(
        table_name=table_name,
        validation_category=(
            "Schema"
        ),
        validation_rule=(
            "Required business columns exist"
        ),
        failed_record_count=len(
            missing_core_columns
        ),
        validation_details=(
            "Missing columns: "
            + (
                ", ".join(
                    missing_core_columns
                )
                if missing_core_columns
                else "None"
            )
        )
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Schema"
        ),
        validation_rule=(
            "Common Bronze audit columns exist"
        ),
        failed_record_count=len(
            missing_audit_columns
        ),
        validation_details=(
            "Missing audit columns: "
            + (
                ", ".join(
                    missing_audit_columns
                )
                if missing_audit_columns
                else "None"
            )
        )
    )

    if missing_core_columns:
        tables_with_missing_core_columns.append(
            table_name
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if tables_with_missing_core_columns:
    preliminary_results_df = (
        persist_validation_results()
    )

    display(
        preliminary_results_df
        .filter(
            F.col(
                "ValidationStatus"
            )
            == "FAILED"
        )
    )

    raise RuntimeError(
        "Core columns are missing from: "
        + ", ".join(
            tables_with_missing_core_columns
        )
    )

print("Required-column validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Primary Keys

PRIMARY_KEYS = {
    "bronze_category": [
        "CategoryID"
    ],
    "bronze_business_unit": [
        "BusinessUnitID"
    ],
    "bronze_buyer": [
        "BuyerID"
    ],
    "bronze_supplier": [
        "SupplierID"
    ],
    "bronze_material": [
        "MaterialID"
    ],
    "bronze_exchange_rate": [
        "RateDate",
        "Currency"
    ],
    "bronze_contract": [
        "ContractID"
    ],
    "bronze_purchase_order_header": [
        "POID"
    ],
    "bronze_purchase_order_item": [
        "POItemID"
    ],
    "bronze_goods_receipt": [
        "GoodsReceiptID"
    ],
    "bronze_invoice_header": [
        "InvoiceID"
    ],
    "bronze_invoice_item": [
        "InvoiceItemID"
    ],
    "bronze_savings_project": [
        "SavingsProjectID"
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for (
    table_name,
    key_columns
) in PRIMARY_KEYS.items():
    dataframe = bronze_tables[
        table_name
    ]

    null_key_count = (
        null_key_failure_count(
            dataframe,
            key_columns
        )
    )

    duplicate_key_count = (
        duplicate_key_failure_count(
            dataframe,
            key_columns
        )
    )

    key_description = ", ".join(
        key_columns
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Primary Key"
        ),
        validation_rule=(
            f"Primary key is not null: "
            f"{key_description}"
        ),
        failed_record_count=(
            null_key_count
        )
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Primary Key"
        ),
        validation_rule=(
            f"Primary key is unique: "
            f"{key_description}"
        ),
        failed_record_count=(
            duplicate_key_count
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate transaction line-number uniqueness
ADDITIONAL_UNIQUE_KEYS = {
    "bronze_purchase_order_item": [
        "POID",
        "POLineNumber"
    ],
    "bronze_goods_receipt": [
        "POItemID",
        "ReceiptSequence"
    ],
    "bronze_invoice_item": [
        "InvoiceID",
        "InvoiceLineNumber"
    ]
}

for (
    table_name,
    key_columns
) in ADDITIONAL_UNIQUE_KEYS.items():
    duplicate_count = (
        duplicate_key_failure_count(
            bronze_tables[
                table_name
            ],
            key_columns
        )
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Business Key"
        ),
        validation_rule=(
            "Composite business key is unique: "
            + ", ".join(key_columns)
        ),
        failed_record_count=(
            duplicate_count
        )
    )

print("Primary-key validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#validate audit metadata
for (
    table_name,
    dataframe
) in bronze_tables.items():
    audit_columns_available = all(
        column_name in dataframe.columns
        for column_name
        in COMMON_AUDIT_COLUMNS
    )

    if not audit_columns_available:
        continue

    audit_null_condition = (
        combine_with_or([
            F.col(column_name).isNull()
            for column_name
            in COMMON_AUDIT_COLUMNS
        ])
    )

    audit_null_count = (
        dataframe
        .filter(
            audit_null_condition
        )
        .count()
    )

    invalid_hash_count = (
        dataframe
        .filter(
            F.col(
                "SourceRecordHash"
            ).isNotNull()
            &
            (
                F.length(
                    F.col(
                        "SourceRecordHash"
                    )
                )
                != 64
            )
        )
        .count()
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Audit Metadata"
        ),
        validation_rule=(
            "Audit metadata is not null"
        ),
        failed_record_count=(
            audit_null_count
        )
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Audit Metadata"
        ),
        validation_rule=(
            "SHA-256 record hash has "
            "64 characters"
        ),
        failed_record_count=(
            invalid_hash_count
        )
    )

print("Audit-metadata validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate foreign keys
def register_foreign_key_validation(
    child_table_name,
    child_columns,
    parent_table_name,
    parent_columns,
    ignore_nulls=False
):
    failed_count = (
        foreign_key_failure_count(
            child_df=bronze_tables[
                child_table_name
            ],
            child_columns=child_columns,
            parent_df=bronze_tables[
                parent_table_name
            ],
            parent_columns=parent_columns,
            ignore_nulls=ignore_nulls
        )
    )

    register_validation(
        table_name=child_table_name,
        validation_category=(
            "Foreign Key"
        ),
        validation_rule=(
            f"{', '.join(child_columns)} "
            f"references "
            f"{parent_table_name}."
            f"{', '.join(parent_columns)}"
        ),
        failed_record_count=(
            failed_count
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#master data relationship
register_foreign_key_validation(
    "bronze_buyer",
    ["BusinessUnitID"],
    "bronze_business_unit",
    ["BusinessUnitID"]
)

register_foreign_key_validation(
    "bronze_material",
    ["CategoryID"],
    "bronze_category",
    ["CategoryID"]
)

register_foreign_key_validation(
    "bronze_contract",
    ["SupplierID"],
    "bronze_supplier",
    ["SupplierID"]
)

register_foreign_key_validation(
    "bronze_contract",
    ["CategoryID"],
    "bronze_category",
    ["CategoryID"]
)

register_foreign_key_validation(
    "bronze_contract",
    ["ContractOwnerBuyerID"],
    "bronze_buyer",
    ["BuyerID"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Purchase-Order relationship
register_foreign_key_validation(
    "bronze_purchase_order_header",
    ["SupplierID"],
    "bronze_supplier",
    ["SupplierID"]
)

register_foreign_key_validation(
    "bronze_purchase_order_header",
    ["BuyerID"],
    "bronze_buyer",
    ["BuyerID"]
)

register_foreign_key_validation(
    "bronze_purchase_order_header",
    ["BusinessUnitID"],
    "bronze_business_unit",
    ["BusinessUnitID"]
)

register_foreign_key_validation(
    "bronze_purchase_order_item",
    ["POID"],
    "bronze_purchase_order_header",
    ["POID"]
)

register_foreign_key_validation(
    "bronze_purchase_order_item",
    ["MaterialID"],
    "bronze_material",
    ["MaterialID"]
)

register_foreign_key_validation(
    "bronze_purchase_order_item",
    ["CategoryID"],
    "bronze_category",
    ["CategoryID"]
)

register_foreign_key_validation(
    "bronze_purchase_order_item",
    ["ContractID"],
    "bronze_contract",
    ["ContractID"],
    ignore_nulls=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#good receipt relationships
register_foreign_key_validation(
    "bronze_goods_receipt",
    ["POID"],
    "bronze_purchase_order_header",
    ["POID"]
)

register_foreign_key_validation(
    "bronze_goods_receipt",
    ["POItemID"],
    "bronze_purchase_order_item",
    ["POItemID"]
)

register_foreign_key_validation(
    "bronze_goods_receipt",
    ["MaterialID"],
    "bronze_material",
    ["MaterialID"]
)

register_foreign_key_validation(
    "bronze_goods_receipt",
    ["SupplierID"],
    "bronze_supplier",
    ["SupplierID"]
)

register_foreign_key_validation(
    "bronze_goods_receipt",
    ["BusinessUnitID"],
    "bronze_business_unit",
    ["BusinessUnitID"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Invoice Relationships
register_foreign_key_validation(
    "bronze_invoice_header",
    ["POID"],
    "bronze_purchase_order_header",
    ["POID"]
)

register_foreign_key_validation(
    "bronze_invoice_header",
    ["SupplierID"],
    "bronze_supplier",
    ["SupplierID"]
)

register_foreign_key_validation(
    "bronze_invoice_header",
    ["BusinessUnitID"],
    "bronze_business_unit",
    ["BusinessUnitID"]
)

register_foreign_key_validation(
    "bronze_invoice_item",
    ["InvoiceID"],
    "bronze_invoice_header",
    ["InvoiceID"]
)

register_foreign_key_validation(
    "bronze_invoice_item",
    ["POID"],
    "bronze_purchase_order_header",
    ["POID"]
)

register_foreign_key_validation(
    "bronze_invoice_item",
    ["POItemID"],
    "bronze_purchase_order_item",
    ["POItemID"]
)

register_foreign_key_validation(
    "bronze_invoice_item",
    ["MaterialID"],
    "bronze_material",
    ["MaterialID"]
)

register_foreign_key_validation(
    "bronze_invoice_item",
    ["CategoryID"],
    "bronze_category",
    ["CategoryID"]
)

register_foreign_key_validation(
    "bronze_invoice_item",
    ["ContractID"],
    "bronze_contract",
    ["ContractID"],
    ignore_nulls=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Savings-project relationships
register_foreign_key_validation(
    "bronze_savings_project",
    ["SupplierID"],
    "bronze_supplier",
    ["SupplierID"]
)

register_foreign_key_validation(
    "bronze_savings_project",
    ["CategoryID"],
    "bronze_category",
    ["CategoryID"]
)

register_foreign_key_validation(
    "bronze_savings_project",
    ["BuyerID"],
    "bronze_buyer",
    ["BuyerID"]
)

register_foreign_key_validation(
    "bronze_savings_project",
    ["BusinessUnitID"],
    "bronze_business_unit",
    ["BusinessUnitID"]
)

register_foreign_key_validation(
    "bronze_savings_project",
    ["ContractID"],
    "bronze_contract",
    ["ContractID"],
    ignore_nulls=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Currency references
currency_reference_df = (
    exchange_rate_df
    .select("Currency")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CURRENCY_RELATIONSHIPS = [
    (
        "bronze_contract",
        "Currency"
    ),
    (
        "bronze_purchase_order_header",
        "Currency"
    ),
    (
        "bronze_purchase_order_item",
        "Currency"
    ),
    (
        "bronze_invoice_header",
        "Currency"
    ),
    (
        "bronze_invoice_item",
        "Currency"
    ),
    (
        "bronze_savings_project",
        "Currency"
    )
]

for (
    table_name,
    currency_column
) in CURRENCY_RELATIONSHIPS:
    invalid_currency_count = (
        foreign_key_failure_count(
            child_df=bronze_tables[
                table_name
            ],
            child_columns=[
                currency_column
            ],
            parent_df=currency_reference_df,
            parent_columns=[
                "Currency"
            ],
            ignore_nulls=False
        )
    )

    register_validation(
        table_name=table_name,
        validation_category=(
            "Foreign Key"
        ),
        validation_rule=(
            f"{currency_column} exists in "
            "exchange-rate currency reference"
        ),
        failed_record_count=(
            invalid_currency_count
        )
    )

print("Foreign-key validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate header to item reconciliation
#PO totals
po_item_total_df = (
    po_item_df
    .groupBy("POID")
    .agg(
        F.round(
            F.sum("LineAmount"),
            2
        ).alias(
            "CalculatedPOAmount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

po_reconciliation_df = (
    po_header_df.alias("header")
    .join(
        po_item_total_df.alias("item"),
        F.col("header.POID")
        == F.col("item.POID"),
        "left"
    )
    .select(
        F.col("header.POID"),
        F.col(
            "header.TotalAmount"
        ),
        F.col(
            "header.AmountReconciliationStatus"
        ),
        F.col(
            "item.CalculatedPOAmount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

po_reconciliation_error_count = (
    po_reconciliation_df
    .filter(
        F.col(
            "CalculatedPOAmount"
        ).isNull()
        |
        (
            F.abs(
                F.col("TotalAmount")
                - F.col(
                    "CalculatedPOAmount"
                )
            )
            > F.lit(
                PO_AMOUNT_TOLERANCE
            )
        )
        |
        (
            F.col(
                "AmountReconciliationStatus"
            )
            != "RECONCILED"
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_purchase_order_header"
    ),
    validation_category=(
        "Reconciliation"
    ),
    validation_rule=(
        "PO header total equals the sum "
        "of PO-item line amounts"
    ),
    failed_record_count=(
        po_reconciliation_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_item_total_df = (
    invoice_item_df
    .groupBy("InvoiceID")
    .agg(
        F.round(
            F.sum("GrossAmount"),
            2
        ).alias(
            "CalculatedInvoiceAmount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_reconciliation_df = (
    invoice_header_df.alias("header")
    .join(
        invoice_item_total_df.alias("item"),
        F.col("header.InvoiceID")
        == F.col("item.InvoiceID"),
        "left"
    )
    .select(
        F.col("header.InvoiceID"),
        F.col(
            "header.TotalInvoiceAmount"
        ),
        F.col(
            "header.AmountReconciliationStatus"
        ),
        F.col(
            "item.CalculatedInvoiceAmount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_reconciliation_error_count = (
    invoice_reconciliation_df
    .filter(
        F.col(
            "CalculatedInvoiceAmount"
        ).isNull()
        |
        (
            F.abs(
                F.col(
                    "TotalInvoiceAmount"
                )
                - F.col(
                    "CalculatedInvoiceAmount"
                )
            )
            > F.lit(
                INVOICE_AMOUNT_TOLERANCE
            )
        )
        |
        (
            F.col(
                "AmountReconciliationStatus"
            )
            != "RECONCILED"
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Reconciliation"
    ),
    validation_rule=(
        "Invoice header total equals the "
        "sum of invoice-item gross amounts"
    ),
    failed_record_count=(
        invoice_reconciliation_error_count
    )
)

print("Header-to-item reconciliation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate PO item consistency
#PO item vs PO Header concistency
po_item_header_consistency_df = (
    po_item_df.alias("item")
    .join(
        po_header_df.alias("header"),
        F.col("item.POID")
        == F.col("header.POID"),
        "inner"
    )
    .select(
        F.col("item.POItemID"),
        F.col("item.Currency").alias(
            "ItemCurrency"
        ),
        F.col("header.Currency").alias(
            "HeaderCurrency"
        )
    )
)

po_item_header_error_count = (
    po_item_header_consistency_df
    .filter(
        F.col("ItemCurrency")
        != F.col("HeaderCurrency")
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_purchase_order_item"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "PO-item currency matches "
        "PO-header currency"
    ),
    failed_record_count=(
        po_item_header_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#PO item versus material
po_item_material_consistency_df = (
    po_item_df.alias("item")
    .join(
        material_df.alias("material"),
        F.col("item.MaterialID")
        == F.col(
            "material.MaterialID"
        ),
        "inner"
    )
    .select(
        F.col("item.POItemID"),
        F.col("item.CategoryID").alias(
            "ItemCategoryID"
        ),
        F.col(
            "material.CategoryID"
        ).alias(
            "MaterialCategoryID"
        ),
        F.col("item.OrderUnit").alias(
            "ItemUnit"
        ),
        F.col(
            "material.UnitOfMeasure"
        ).alias(
            "MaterialUnit"
        )
    )
)

po_item_material_error_count = (
    po_item_material_consistency_df
    .filter(
        (
            F.col("ItemCategoryID")
            != F.col(
                "MaterialCategoryID"
            )
        )
        |
        (
            F.col("ItemUnit")
            != F.col("MaterialUnit")
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_purchase_order_item"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "PO-item category and unit match "
        "the material master"
    ),
    failed_record_count=(
        po_item_material_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#validate intentional contract scenarios
po_contract_validation_df = (
    po_item_df.alias("item")
    .join(
        po_header_df.alias("header"),
        F.col("item.POID")
        == F.col("header.POID"),
        "inner"
    )
    .join(
        contract_df.alias("contract"),
        F.col("item.ContractID")
        == F.col(
            "contract.ContractID"
        ),
        "left"
    )
    .select(
        F.col("item.POItemID"),
        F.col("item.ContractID"),
        F.col(
            "item.SimulationContractScenario"
        ),
        F.col("item.CategoryID").alias(
            "ItemCategoryID"
        ),
        F.col("item.Currency").alias(
            "ItemCurrency"
        ),
        F.col("header.SupplierID").alias(
            "POSupplierID"
        ),
        F.col("header.OrderDate"),
        F.col(
            "contract.ContractID"
        ).alias(
            "MatchedContractID"
        ),
        F.col(
            "contract.SupplierID"
        ).alias(
            "ContractSupplierID"
        ),
        F.col(
            "contract.CategoryID"
        ).alias(
            "ContractCategoryID"
        ),
        F.col(
            "contract.Currency"
        ).alias(
            "ContractCurrency"
        ),
        F.col(
            "contract.ContractStartDate"
        ),
        F.col(
            "contract.ContractEndDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_relationship_error = (
    F.col(
        "MatchedContractID"
    ).isNull()
    |
    (
        F.col("POSupplierID")
        != F.col(
            "ContractSupplierID"
        )
    )
    |
    (
        F.col("ItemCategoryID")
        != F.col(
            "ContractCategoryID"
        )
    )
)

contract_date_is_valid = (
    (
        F.col("OrderDate")
        >= F.col(
            "ContractStartDate"
        )
    )
    &
    (
        F.col("OrderDate")
        <= F.col(
            "ContractEndDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_scenario_error_count = (
    po_contract_validation_df
    .filter(
        (
            (
                F.col(
                    "SimulationContractScenario"
                )
                == "COMPLIANT_CONTRACT"
            )
            &
            (
                F.col(
                    "ContractID"
                ).isNull()
                |
                contract_relationship_error
                |
                (
                    ~contract_date_is_valid
                )
            )
        )
        |
        (
            (
                F.col(
                    "SimulationContractScenario"
                )
                == "INVALID_DATE_CONTRACT"
            )
            &
            (
                F.col(
                    "ContractID"
                ).isNull()
                |
                contract_relationship_error
                |
                contract_date_is_valid
            )
        )
        |
        (
            (
                F.col(
                    "SimulationContractScenario"
                )
                == "NO_CONTRACT_REFERENCE"
            )
            &
            F.col(
                "ContractID"
            ).isNotNull()
        )
        |
        (
            ~F.col(
                "SimulationContractScenario"
            ).isin(
                "COMPLIANT_CONTRACT",
                "INVALID_DATE_CONTRACT",
                "NO_CONTRACT_REFERENCE"
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_purchase_order_item"
    ),
    validation_category=(
        "Scenario Integrity"
    ),
    validation_rule=(
    "Contract simulation scenario "
    "matches supplier, category and "
    "contract validity dates"
    ),
    failed_record_count=(
        contract_scenario_error_count
    )
)

print("PO-item consistency validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#validate good receipts
goods_receipt_validation_df = (
    goods_receipt_df.alias("receipt")
    .join(
        po_item_df.alias("item"),
        F.col("receipt.POItemID")
        == F.col("item.POItemID"),
        "inner"
    )
    .join(
        po_header_df.alias("header"),
        F.col("item.POID")
        == F.col("header.POID"),
        "inner"
    )
    .select(
        F.col(
            "receipt.GoodsReceiptID"
        ),
        F.col(
            "receipt.POID"
        ).alias(
            "ReceiptPOID"
        ),
        F.col(
            "item.POID"
        ).alias(
            "ItemPOID"
        ),
        F.col(
            "receipt.POItemID"
        ),
        F.col(
            "receipt.MaterialID"
        ).alias(
            "ReceiptMaterialID"
        ),
        F.col(
            "item.MaterialID"
        ).alias(
            "ItemMaterialID"
        ),
        F.col(
            "receipt.UnitOfMeasure"
        ).alias(
            "ReceiptUnit"
        ),
        F.col(
            "item.OrderUnit"
        ).alias(
            "ItemUnit"
        ),
        F.col(
            "receipt.SupplierID"
        ).alias(
            "ReceiptSupplierID"
        ),
        F.col(
            "header.SupplierID"
        ).alias(
            "POSupplierID"
        ),
        F.col(
            "receipt.BusinessUnitID"
        ).alias(
            "ReceiptBusinessUnitID"
        ),
        F.col(
            "header.BusinessUnitID"
        ).alias(
            "POBusinessUnitID"
        ),
        F.col("header.OrderDate"),
        F.col(
            "item.RequestedDeliveryDate"
        ),
        F.col("receipt.ReceiptDate"),
        F.col(
            "receipt.QuantityReceived"
        ),
        F.col(
            "receipt.IsLateReceipt"
        ),
        F.col("receipt.DaysLate"),
        F.col("item.POItemStatus")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_days_late = F.greatest(
    F.datediff(
        F.col("ReceiptDate"),
        F.col(
            "RequestedDeliveryDate"
        )
    ),
    F.lit(0)
)

expected_late_flag = (
    F.col("ReceiptDate")
    > F.col(
        "RequestedDeliveryDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

goods_receipt_consistency_error_count = (
    goods_receipt_validation_df
    .filter(
        (
            F.col("ReceiptPOID")
            != F.col("ItemPOID")
        )
        |
        (
            F.col("ReceiptMaterialID")
            != F.col("ItemMaterialID")
        )
        |
        (
            F.col("ReceiptUnit")
            != F.col("ItemUnit")
        )
        |
        (
            F.col("ReceiptSupplierID")
            != F.col("POSupplierID")
        )
        |
        (
            F.col(
                "ReceiptBusinessUnitID"
            )
            != F.col(
                "POBusinessUnitID"
            )
        )
        |
        (
            F.col("ReceiptDate")
            < F.col("OrderDate")
        )
        |
        (
            F.col("ReceiptDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
        |
        (
            F.col("QuantityReceived")
            <= F.lit(0)
        )
        |
        (
            F.col("IsLateReceipt")
            != expected_late_flag
        )
        |
        (
            F.col("DaysLate")
            != expected_days_late
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_goods_receipt"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "Goods receipts match PO items "
        "and delivery-date logic"
    ),
    failed_record_count=(
        goods_receipt_consistency_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cancelled_item_receipt_count = (
    goods_receipt_validation_df
    .filter(
        F.col("POItemStatus")
        == "Cancelled"
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_goods_receipt"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "Cancelled PO items have no "
        "goods receipts"
    ),
    failed_record_count=(
        cancelled_item_receipt_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

received_quantity_by_item_df = (
    goods_receipt_df
    .groupBy("POItemID")
    .agg(
        F.sum(
            "QuantityReceived"
        ).alias(
            "TotalReceivedQuantity"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

po_item_receipt_summary_df = (
    po_item_df
    .select(
        "POItemID",
        "Quantity",
        "POItemStatus"
    )
    .join(
        received_quantity_by_item_df,
        "POItemID",
        "left"
    )
    .withColumn(
        "TotalReceivedQuantity",
        F.coalesce(
            F.col(
                "TotalReceivedQuantity"
            ),
            F.lit(0)
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

receipt_status_error_count = (
    po_item_receipt_summary_df
    .filter(
        (
            F.col("POItemStatus").isin(
                "Fully Received",
                "Closed"
            )
            &
            (
                F.col(
                    "TotalReceivedQuantity"
                )
                + F.lit(
                    QUANTITY_TOLERANCE
                )
                < F.col("Quantity")
            )
        )
        |
        (
            F.col("POItemStatus").isin(
                "Open",
                "Partially Received"
            )
            &
            (
                F.col(
                    "TotalReceivedQuantity"
                )
                >= F.col("Quantity")
            )
        )
        |
        (
            (
                F.col("POItemStatus")
                == "Cancelled"
            )
            &
            (
                F.col(
                    "TotalReceivedQuantity"
                )
                > F.lit(0)
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_goods_receipt"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "Cumulative receipt quantity "
        "matches PO-item status"
    ),
    failed_record_count=(
        receipt_status_error_count
    )
)

print("Goods-receipt validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Invoices
invoice_header_po_validation_df = (
    invoice_header_df.alias("invoice")
    .join(
        po_header_df.alias("po"),
        F.col("invoice.POID")
        == F.col("po.POID"),
        "inner"
    )
    .select(
        F.col("invoice.InvoiceID"),
        F.col(
            "invoice.SupplierID"
        ).alias(
            "InvoiceSupplierID"
        ),
        F.col(
            "po.SupplierID"
        ).alias(
            "POSupplierID"
        ),
        F.col(
            "invoice.BusinessUnitID"
        ).alias(
            "InvoiceBusinessUnitID"
        ),
        F.col(
            "po.BusinessUnitID"
        ).alias(
            "POBusinessUnitID"
        ),
        F.col(
            "invoice.Currency"
        ).alias(
            "InvoiceCurrency"
        ),
        F.col(
            "po.Currency"
        ).alias(
            "POCurrency"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_header_po_error_count = (
    invoice_header_po_validation_df
    .filter(
        (
            F.col("InvoiceSupplierID")
            != F.col("POSupplierID")
        )
        |
        (
            F.col(
                "InvoiceBusinessUnitID"
            )
            != F.col(
                "POBusinessUnitID"
            )
        )
        |
        (
            F.col("InvoiceCurrency")
            != F.col("POCurrency")
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "Invoice supplier, business unit "
        "and currency match the PO"
    ),
    failed_record_count=(
        invoice_header_po_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_item_consistency_df = (
    invoice_item_df.alias("invoice_item")
    .join(
        invoice_header_df.alias(
            "invoice_header"
        ),
        F.col(
            "invoice_item.InvoiceID"
        )
        == F.col(
            "invoice_header.InvoiceID"
        ),
        "inner"
    )
    .join(
        po_item_df.alias("po_item"),
        F.col(
            "invoice_item.POItemID"
        )
        == F.col(
            "po_item.POItemID"
        ),
        "inner"
    )
    .select(
        F.col(
            "invoice_item.InvoiceItemID"
        ),
        F.col(
            "invoice_item.POID"
        ).alias(
            "InvoiceItemPOID"
        ),
        F.col(
            "invoice_header.POID"
        ).alias(
            "InvoiceHeaderPOID"
        ),
        F.col(
            "po_item.POID"
        ).alias(
            "POItemPOID"
        ),
        F.col(
            "invoice_item.MaterialID"
        ).alias(
            "InvoiceMaterialID"
        ),
        F.col(
            "po_item.MaterialID"
        ).alias(
            "POMaterialID"
        ),
        F.col(
            "invoice_item.CategoryID"
        ).alias(
            "InvoiceCategoryID"
        ),
        F.col(
            "po_item.CategoryID"
        ).alias(
            "POCategoryID"
        ),
        F.col(
            "invoice_item.ContractID"
        ).alias(
            "InvoiceContractID"
        ),
        F.col(
            "po_item.ContractID"
        ).alias(
            "POContractID"
        ),
        F.col(
            "invoice_item.Currency"
        ).alias(
            "InvoiceItemCurrency"
        ),
        F.col(
            "invoice_header.Currency"
        ).alias(
            "InvoiceHeaderCurrency"
        ),
        F.col(
            "invoice_item.UnitOfMeasure"
        ).alias(
            "InvoiceUnit"
        ),
        F.col(
            "po_item.OrderUnit"
        ).alias(
            "POUnit"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_item_consistency_error_count = (
    invoice_item_consistency_df
    .filter(
        (
            F.col("InvoiceItemPOID")
            != F.col(
                "InvoiceHeaderPOID"
            )
        )
        |
        (
            F.col("InvoiceItemPOID")
            != F.col("POItemPOID")
        )
        |
        (
            F.col("InvoiceMaterialID")
            != F.col("POMaterialID")
        )
        |
        (
            F.col("InvoiceCategoryID")
            != F.col("POCategoryID")
        )
        |
        (
            ~F.col(
                "InvoiceContractID"
            ).eqNullSafe(
                F.col("POContractID")
            )
        )
        |
        (
            F.col("InvoiceItemCurrency")
            != F.col(
                "InvoiceHeaderCurrency"
            )
        )
        |
        (
            F.col("InvoiceUnit")
            != F.col("POUnit")
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_item"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "Invoice item matches invoice "
        "header and PO item"
    ),
    failed_record_count=(
        invoice_item_consistency_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_arithmetic_error_count = (
    invoice_item_df
    .filter(
        (
            F.abs(
                F.col("NetAmount")
                - F.round(
                    F.col(
                        "InvoicedQuantity"
                    )
                    * F.col(
                        "InvoiceUnitPrice"
                    ),
                    2
                )
            )
            > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col("TaxAmount")
                - F.round(
                    F.col("NetAmount")
                    * F.col("TaxRate"),
                    2
                )
            )
            > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col("GrossAmount")
                - (
                    F.col("NetAmount")
                    + F.col("TaxAmount")
                )
            )
            > F.lit(0.01)
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_item"
    ),
    validation_category=(
        "Arithmetic"
    ),
    validation_rule=(
        "Invoice net, tax and gross "
        "amounts are calculated correctly"
    ),
    failed_record_count=(
        invoice_arithmetic_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_arithmetic_error_count = (
    invoice_item_df
    .filter(
        (
            F.abs(
                F.col("NetAmount")
                - F.round(
                    F.col(
                        "InvoicedQuantity"
                    )
                    * F.col(
                        "InvoiceUnitPrice"
                    ),
                    2
                )
            )
            > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col("TaxAmount")
                - F.round(
                    F.col("NetAmount")
                    * F.col("TaxRate"),
                    2
                )
            )
            > F.lit(0.01)
        )
        |
        (
            F.abs(
                F.col("GrossAmount")
                - (
                    F.col("NetAmount")
                    + F.col("TaxAmount")
                )
            )
            > F.lit(0.01)
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_item"
    ),
    validation_category=(
        "Arithmetic"
    ),
    validation_rule=(
        "Invoice net, tax and gross "
        "amounts are calculated correctly"
    ),
    failed_record_count=(
        invoice_arithmetic_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_dispute_field_error_count = (
    invoice_header_df
    .filter(
        (
            F.col("DisputeFlag")
            &
            F.col(
                "DisputeReason"
            ).isNull()
        )
        |
        (
            ~F.col("DisputeFlag")
            &
            F.col(
                "DisputeReason"
            ).isNotNull()
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "Invoice dispute flag and reason "
        "are consistent"
    ),
    failed_record_count=(
        invoice_dispute_field_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_match_status_error_count = (
    invoice_item_df
    .filter(
        (
            F.col(
                "DuplicateInvoiceLineFlag"
            )
            &
            (
                F.col(
                    "ThreeWayMatchStatus"
                )
                != "DUPLICATE_INVOICE"
            )
        )
        |
        (
            ~F.col(
                "DuplicateInvoiceLineFlag"
            )
            &
            (
                F.col(
                    "ThreeWayMatchStatus"
                )
                == "DUPLICATE_INVOICE"
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_item"
    ),
    validation_category=(
        "Scenario Integrity"
    ),
    validation_rule=(
        "Duplicate invoice-line flag "
        "matches three-way-match status"
    ),
    failed_record_count=(
        duplicate_match_status_error_count
    )
)

print("Invoice validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Duplicate invoices
duplicate_invoice_validation_df = (
    invoice_header_df.alias("duplicate")
    .filter(
        F.col(
            "duplicate.DuplicateInvoiceFlag"
        )
    )
    .join(
        invoice_header_df.alias("original"),
        F.col(
            "duplicate.OriginalInvoiceID"
        )
        == F.col(
            "original.InvoiceID"
        ),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_invoice_relationship_error_count = (
    duplicate_invoice_validation_df
    .filter(
        F.col(
            "original.InvoiceID"
        ).isNull()
        |
        F.col(
            "original.DuplicateInvoiceFlag"
        )
        |
        (
            F.col(
                "duplicate.InvoiceNumber"
            )
            != F.col(
                "original.InvoiceNumber"
            )
        )
        |
        (
            F.col(
                "duplicate.SupplierID"
            )
            != F.col(
                "original.SupplierID"
            )
        )
        |
        (
            F.col(
                "duplicate.POID"
            )
            != F.col(
                "original.POID"
            )
        )
        |
        (
            F.abs(
                F.col(
                    "duplicate.TotalInvoiceAmount"
                )
                - F.col(
                    "original.TotalInvoiceAmount"
                )
            )
            > F.lit(0.01)
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Duplicate Invoice"
    ),
    validation_rule=(
        "Duplicate invoice references an "
        "equivalent original invoice"
    ),
    failed_record_count=(
        duplicate_invoice_relationship_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unexpected_original_invoice_id_count = (
    invoice_header_df
    .filter(
        (
            ~F.col(
                "DuplicateInvoiceFlag"
            )
        )
        &
        F.col(
            "OriginalInvoiceID"
        ).isNotNull()
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Duplicate Invoice"
    ),
    validation_rule=(
        "Only duplicate invoices contain "
        "OriginalInvoiceID"
    ),
    failed_record_count=(
        unexpected_original_invoice_id_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_number_summary_df = (
    invoice_header_df
    .groupBy("InvoiceNumber")
    .agg(
        F.count("*").alias(
            "InvoiceCount"
        ),
        F.sum(
            F.col(
                "DuplicateInvoiceFlag"
            ).cast("int")
        ).alias(
            "DuplicateCount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoice_number_group_error_count = (
    invoice_number_summary_df
    .filter(
        (
            (
                F.col("InvoiceCount")
                == 1
            )
            &
            (
                F.col("DuplicateCount")
                != 0
            )
        )
        |
        (
            (
                F.col("InvoiceCount")
                > 1
            )
            &
            (
                F.col("DuplicateCount")
                != (
                    F.col("InvoiceCount")
                    - F.lit(1)
                )
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Duplicate Invoice"
    ),
    validation_rule=(
        "Repeated invoice numbers contain "
        "one original and flagged duplicates"
    ),
    failed_record_count=(
        invoice_number_group_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_invoice_item_validation_df = (
    invoice_item_df.alias("duplicate")
    .filter(
        F.col(
            "duplicate."
            "DuplicateInvoiceLineFlag"
        )
    )
    .join(
        invoice_item_df.alias("original"),
        F.col(
            "duplicate."
            "OriginalInvoiceItemID"
        )
        == F.col(
            "original.InvoiceItemID"
        ),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_invoice_item_error_count = (
    duplicate_invoice_item_validation_df
    .filter(
        F.col(
            "original.InvoiceItemID"
        ).isNull()
        |
        F.col(
            "original."
            "DuplicateInvoiceLineFlag"
        )
        |
        (
            F.col(
                "duplicate.POItemID"
            )
            != F.col(
                "original.POItemID"
            )
        )
        |
        (
            F.col(
                "duplicate.InvoicedQuantity"
            )
            != F.col(
                "original.InvoicedQuantity"
            )
        )
        |
        (
            F.col(
                "duplicate.InvoiceUnitPrice"
            )
            != F.col(
                "original.InvoiceUnitPrice"
            )
        )
        |
        (
            F.col(
                "duplicate.GrossAmount"
            )
            != F.col(
                "original.GrossAmount"
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_item"
    ),
    validation_category=(
        "Duplicate Invoice"
    ),
    validation_rule=(
        "Duplicate invoice items are exact "
        "copies of original items"
    ),
    failed_record_count=(
        duplicate_invoice_item_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unexpected_original_item_id_count = (
    invoice_item_df
    .filter(
        (
            ~F.col(
                "DuplicateInvoiceLineFlag"
            )
        )
        &
        F.col(
            "OriginalInvoiceItemID"
        ).isNotNull()
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_item"
    ),
    validation_category=(
        "Duplicate Invoice"
    ),
    validation_rule=(
        "Only duplicate invoice lines "
        "contain OriginalInvoiceItemID"
    ),
    failed_record_count=(
        unexpected_original_item_id_count
    )
)

print("Duplicate-invoice validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validate Savings Project
savings_buyer_validation_df = (
    savings_project_df.alias("project")
    .join(
        buyer_df.alias("buyer"),
        F.col("project.BuyerID")
        == F.col("buyer.BuyerID"),
        "inner"
    )
)

savings_buyer_error_count = (
    savings_buyer_validation_df
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

register_validation(
    table_name=(
        "bronze_savings_project"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "Savings-project business unit "
        "matches the project buyer"
    ),
    failed_record_count=(
        savings_buyer_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

savings_contract_validation_df = (
    savings_project_df
    .filter(
        F.col("ContractID").isNotNull()
    )
    .alias("project")
    .join(
        contract_df.alias("contract"),
        F.col("project.ContractID")
        == F.col(
            "contract.ContractID"
        ),
        "inner"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

savings_contract_error_count = (
    savings_contract_validation_df
    .filter(
        (
            F.col("project.SupplierID")
            != F.col(
                "contract.SupplierID"
            )
        )
        |
        (
            F.col("project.CategoryID")
            != F.col(
                "contract.CategoryID"
            )
        )
        |
        (
            F.col("project.Currency")
            != F.col(
                "contract.Currency"
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_savings_project"
    ),
    validation_category=(
        "Cross-Table Consistency"
    ),
    validation_rule=(
        "Savings-project contract matches "
        "supplier, category and currency"
    ),
    failed_record_count=(
        savings_contract_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

savings_date_error_count = (
    savings_project_df
    .filter(
        (
            F.col("ProjectCreatedDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
        |
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
                "ActualCompletionDate"
            )
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
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

register_validation(
    table_name=(
        "bronze_savings_project"
    ),
    validation_category=(
        "Date Integrity"
    ),
    validation_rule=(
        "Savings-project dates are "
        "chronologically valid"
    ),
    failed_record_count=(
        savings_date_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

savings_date_error_count = (
    savings_project_df
    .filter(
        (
            F.col("ProjectCreatedDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
        |
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
                "ActualCompletionDate"
            )
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
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

register_validation(
    table_name=(
        "bronze_savings_project"
    ),
    validation_category=(
        "Date Integrity"
    ),
    validation_rule=(
        "Savings-project dates are "
        "chronologically valid"
    ),
    failed_record_count=(
        savings_date_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

savings_amount_error_count = (
    savings_project_df
    .filter(
        (
            F.col("BaselineSpend")
            <= F.lit(0)
        )
        |
        (
            F.col("ForecastedSavings")
            <= F.lit(0)
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

register_validation(
    table_name=(
        "bronze_savings_project"
    ),
    validation_category=(
        "Amount Integrity"
    ),
    validation_rule=(
        "Savings-project monetary values "
        "are valid"
    ),
    failed_record_count=(
        savings_amount_error_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

savings_status_error_count = (
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
                F.col(
                    "CancellationDate"
                ).isNotNull()
                |
                (
                    F.col(
                        "ApprovedSavings"
                    )
                    <= F.lit(0)
                )
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
                |
                (
                    F.col("ApprovalStatus")
                    != "Approved"
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
                F.col(
                    "ActualCompletionDate"
                ).isNotNull()
                |
                (
                    F.col(
                        "ApprovedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col("SavingsLevel")
                    != "L0"
                )
                |
                (
                    F.col("ApprovalStatus")
                    != "Rejected"
                )
            )
        )
        |
        (
            (
                F.col("ProjectStatus")
                == "Idea"
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
                        "ApprovedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col("SavingsLevel")
                    != "L0"
                )
                |
                (
                    F.col("ApprovalStatus")
                    != "Draft"
                )
            )
        )
        |
        (
            (
                F.col("ProjectStatus")
                == "Validated"
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
                        "ApprovedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col("SavingsLevel")
                    != "L1"
                )
                |
                (
                    F.col("ApprovalStatus")
                    != "Under Review"
                )
            )
        )
        |
        (
            (
                F.col("ProjectStatus")
                == "Negotiation"
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
                        "ApprovedSavings"
                    )
                    <= F.lit(0)
                )
                |
                (
                    F.col(
                        "RealizedSavings"
                    )
                    != F.lit(0)
                )
                |
                (
                    F.col("SavingsLevel")
                    != "L2"
                )
                |
                (
                    F.col("ApprovalStatus")
                    != "Approved"
                )
            )
        )
        |
        (
            ~F.col("ProjectStatus").isin(
                "Idea",
                "Validated",
                "Negotiation",
                "Implemented",
                "Cancelled"
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_savings_project"
    ),
    validation_category=(
        "Scenario Integrity"
    ),
    validation_rule=(
        "Savings-project status, level, "
        "approval and amounts are consistent"
    ),
    failed_record_count=(
        savings_status_error_count
    )
)

print("Savings-project validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#validate general business rules
invalid_procurement_type_count = (
    category_df
    .filter(
        ~F.col(
            "ProcurementType"
        ).isin(
            "Direct",
            "Indirect",
            "Capital"
        )
    )
    .count()
)

register_validation(
    table_name="bronze_category",
    validation_category=(
        "Domain Values"
    ),
    validation_rule=(
        "Procurement type is valid"
    ),
    failed_record_count=(
        invalid_procurement_type_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_material_count = (
    material_df
    .filter(
        F.col("StandardCost")
        <= F.lit(0)
    )
    .count()
)

register_validation(
    table_name="bronze_material",
    validation_category=(
        "Amount Integrity"
    ),
    validation_rule=(
        "Material standard cost is positive"
    ),
    failed_record_count=(
        invalid_material_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_contract_count = (
    contract_df
    .filter(
        (
            F.col("ContractEndDate")
            < F.col("ContractStartDate")
        )
        |
        (
            F.col("ContractValue")
            <= F.lit(0)
        )
        |
        (
            F.col("NegotiatedUnitPrice")
            <= F.lit(0)
        )
    )
    .count()
)

register_validation(
    table_name="bronze_contract",
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "Contract dates and values are valid"
    ),
    failed_record_count=(
        invalid_contract_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_po_header_count = (
    po_header_df
    .filter(
        (
            F.col("OrderDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
        |
        (
            F.col("TotalAmount")
            <= F.lit(0)
        )
        |
        (
            ~F.col("POStatus").isin(
                "Open",
                "Partially Received",
                "Fully Received",
                "Closed",
                "Cancelled"
            )
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_purchase_order_header"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "PO header dates, amounts and "
        "statuses are valid"
    ),
    failed_record_count=(
        invalid_po_header_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_po_item_count = (
    po_item_df
    .filter(
        (
            F.col("Quantity")
            <= F.lit(0)
        )
        |
        (
            F.col("UnitPrice")
            <= F.lit(0)
        )
        |
        (
            F.col("LineAmount")
            <= F.lit(0)
        )
        |
        (
            F.col(
                "RequestedDeliveryDate"
            ).isNull()
        )
        |
        (
            F.abs(
                F.col("LineAmount")
                - F.round(
                    F.col("Quantity")
                    * F.col("UnitPrice"),
                    2
                )
            )
            > F.lit(0.01)
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_purchase_order_item"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "PO-item quantities, prices and "
        "line amounts are valid"
    ),
    failed_record_count=(
        invalid_po_item_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_invoice_header_count = (
    invoice_header_df
    .filter(
        (
            F.col("InvoiceDate")
            > F.col("PostingDate")
        )
        |
        (
            F.col("PostingDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
        |
        (
            F.col("DueDate")
            < F.col("PostingDate")
        )
        |
        (
            F.col(
                "TotalInvoiceAmount"
            )
            <= F.lit(0)
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_invoice_header"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "Invoice dates and total amount "
        "are valid"
    ),
    failed_record_count=(
        invalid_invoice_header_count
    )
)

print("General business-rule validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#validate exchange rate completeness
invalid_exchange_rate_count = (
    exchange_rate_df
    .filter(
        (
            F.col("ExchangeRateEUR")
            <= F.lit(0)
        )
        |
        (
            F.col("RateDate")
            < F.lit(
                START_DATE.isoformat()
            ).cast("date")
        )
        |
        (
            F.col("RateDate")
            > F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_exchange_rate"
    ),
    validation_category=(
        "Business Rule"
    ),
    validation_rule=(
        "Exchange rates are positive and "
        "within the configured period"
    ),
    failed_record_count=(
        invalid_exchange_rate_count
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exchange_rate_coverage_df = (
    exchange_rate_df
    .groupBy("Currency")
    .agg(
        F.countDistinct(
            "RateDate"
        ).alias(
            "DistinctRateDates"
        ),
        F.min("RateDate").alias(
            "MinimumRateDate"
        ),
        F.max("RateDate").alias(
            "MaximumRateDate"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exchange_rate_coverage_error_count = (
    exchange_rate_coverage_df
    .filter(
        (
            F.col(
                "DistinctRateDates"
            )
            != EXPECTED_EXCHANGE_RATE_DAYS
        )
        |
        (
            F.col("MinimumRateDate")
            != F.lit(
                START_DATE.isoformat()
            ).cast("date")
        )
        |
        (
            F.col("MaximumRateDate")
            != F.lit(
                AS_OF_DATE.isoformat()
            ).cast("date")
        )
    )
    .count()
)

register_validation(
    table_name=(
        "bronze_exchange_rate"
    ),
    validation_category=(
        "Coverage"
    ),
    validation_rule=(
        "Every currency has one daily rate "
        "for the full reporting period"
    ),
    failed_record_count=(
        exchange_rate_coverage_error_count
    ),
    validation_details=(
        f"Expected days per currency: "
        f"{EXPECTED_EXCHANGE_RATE_DAYS}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

actual_currency_count = (
    exchange_rate_df
    .select("Currency")
    .distinct()
    .count()
)

register_validation(
    table_name=(
        "bronze_exchange_rate"
    ),
    validation_category=(
        "Coverage"
    ),
    validation_rule=(
        f"Currency count equals "
        f"{EXPECTED_CURRENCY_COUNT}"
    ),
    failed_record_count=(
        abs(
            actual_currency_count
            - EXPECTED_CURRENCY_COUNT
        )
    ),
    validation_details=(
        f"Expected: "
        f"{EXPECTED_CURRENCY_COUNT}; "
        f"actual: {actual_currency_count}"
    )
)

display(
    exchange_rate_coverage_df
    .orderBy("Currency")
)

print("Exchange-rate validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Persist Validation results
validation_results_df = (
    persist_validation_results()
)

validation_rule_count = (
    validation_results_df.count()
)

print(
    f"Persisted {validation_rule_count:,} "
    f"validation rules to "
    f"{MONITORING_TABLE}."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# CELL ********************

#Validation Summary
validation_summary_df = (
    validation_results_df
    .groupBy(
        "ValidationStatus",
        "Severity"
    )
    .agg(
        F.count("*").alias(
            "ValidationRuleCount"
        ),
        F.sum(
            "FailedRecordCount"
        ).alias(
            "TotalFailedRecords"
        )
    )
    .orderBy(
        "ValidationStatus",
        "Severity"
    )
)

display(validation_summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Summary by Table
validation_table_summary_df = (
    validation_results_df
    .groupBy("TableName")
    .agg(
        F.count("*").alias(
            "ValidationRuleCount"
        ),
        F.sum(
            F.when(
                F.col(
                    "ValidationStatus"
                )
                == "PASSED",
                1
            ).otherwise(0)
        ).alias(
            "PassedRules"
        ),
        F.sum(
            F.when(
                F.col(
                    "ValidationStatus"
                )
                == "FAILED",
                1
            ).otherwise(0)
        ).alias(
            "FailedRules"
        ),
        F.sum(
            "FailedRecordCount"
        ).alias(
            "FailedRecordCount"
        )
    )
    .orderBy("TableName")
)

display(validation_table_summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Failed Rules
failed_validation_df = (
    validation_results_df
    .filter(
        F.col(
            "ValidationStatus"
        )
        == "FAILED"
    )
    .orderBy(
        F.desc(
            "FailedRecordCount"
        ),
        "TableName",
        "ValidationRule"
    )
)

display(failed_validation_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

failed_rules_df = (
    spark.table(
        "monitoring_bronze_data_quality_results"
    )
    .filter(
        F.col("ValidationStatus")
        == "FAILED"
    )
    .select(
        "ValidationID",
        "TableName",
        "ValidationCategory",
        "ValidationRule",
        "Severity",
        "FailedRecordCount",
        "ValidationDetails"
    )
    .orderBy(
        F.desc("FailedRecordCount"),
        "TableName"
    )
)

display(failed_rules_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Final Quality gate
critical_failure_count = (
    validation_results_df
    .filter(
        (
            F.col("Severity")
            == "ERROR"
        )
        &
        (
            F.col(
                "ValidationStatus"
            )
            == "FAILED"
        )
    )
    .count()
)

warning_count = (
    validation_results_df
    .filter(
        F.col(
            "ValidationStatus"
        )
        == "WARNING"
    )
    .count()
)

passed_rule_count = (
    validation_results_df
    .filter(
        F.col(
            "ValidationStatus"
        )
        == "PASSED"
    )
    .count()
)

print(
    f"Passed rules: "
    f"{passed_rule_count:,}"
)

print(
    f"Warnings: "
    f"{warning_count:,}"
)

print(
    f"Critical failed rules: "
    f"{critical_failure_count:,}"
)

if critical_failure_count > 0:
    raise AssertionError(
        f"Bronze validation failed with "
        f"{critical_failure_count} "
        f"critical rule failures. "
        f"Review table "
        f"{MONITORING_TABLE}."
    )

print(
    "BRONZE QUALITY GATE PASSED."
)

print(
    "The Bronze layer is ready for "
    "Silver transformations."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

failed_rules = (
    spark.table(
        "monitoring_bronze_data_quality_results"
    )
    .filter(
        F.col("ValidationStatus")
        == "FAILED"
    )
    .select(
        "TableName",
        "ValidationCategory",
        "ValidationRule",
        "FailedRecordCount",
        "ValidationDetails"
    )
    .collect()
)

for row in failed_rules:
    print("=" * 80)
    print(
        f"TABLE: {row['TableName']}"
    )
    print(
        f"CATEGORY: "
        f"{row['ValidationCategory']}"
    )
    print(
        f"RULE: "
        f"{row['ValidationRule']}"
    )
    print(
        f"FAILED RECORDS: "
        f"{row['FailedRecordCount']}"
    )
    print(
        f"DETAILS: "
        f"{row['ValidationDetails']}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
