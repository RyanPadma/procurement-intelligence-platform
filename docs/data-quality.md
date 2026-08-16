# Enterprise Procurement Intelligence Platform
## Data Quality and Validation

This document describes how data quality is validated across the procurement analytics platform.

> **Scope:** The project uses synthetic data. Validation results demonstrate engineering controls within the portfolio implementation and should not be interpreted as production SLAs.

---

## 1. Validation Strategy

Data quality is embedded throughout the platform rather than performed only at the reporting stage.

The validation flow follows the data architecture:

```text
Bronze source validation
        ↓
Silver transformation validation
        ↓
Gold model validation
        ↓
Databricks ML validation
        ↓
Fabric ML output validation
        ↓
Semantic model / reporting reconciliation
```

The goal is to detect problems as close as possible to the layer where they are introduced.

---

## 2. Validation Principles

The project follows five main principles.

### Validate the expected grain

Every table should contain records at its intended analytical grain.

Examples:

- purchase-order facts should not duplicate PO-item spend
- supplier-risk predictions should remain at supplier prediction-snapshot grain
- savings opportunities should remain at supplier-category grain

### Reconcile between layers

Important totals and business classifications are checked between upstream and downstream datasets.

Examples include:

- spend reconciliation
- contract-compliance reconciliation
- savings reconciliation
- Databricks-to-Fabric ML reconciliation

### Validate relationships

Gold facts and dimensions are checked for broken relationships and invalid historical alignment.

### Validate business rules

Technical correctness alone is not sufficient.

Procurement logic such as contract compliance, supplier history, invoice matching, and ML outputs must also produce logically consistent results.

### Persist important validation outputs

Data-quality results are retained as monitoring outputs where appropriate rather than existing only as temporary notebook messages.

---

# 3. Validation by Data Layer

## Bronze Validation

Bronze represents synthetic ERP-style source data.

Typical validation checks include:

- expected schema
- non-empty output
- required business keys
- duplicate source keys
- valid quantities and amounts
- valid foreign-key references
- valid date ranges

The objective is to prevent malformed synthetic source records from propagating into Silver.

---

## Silver Validation

Silver contains standardized and business-enriched data.

Typical validation areas include:

- datatype consistency
- duplicate removal
- missing-value handling
- EUR conversion completeness
- contract mapping
- maverick-spend classification
- invoice matching
- supplier performance derivation

Silver validation checks both technical transformation quality and procurement business logic.

---

## Gold Validation

Gold is the reporting and analytical consumption layer.

Validation focuses on:

- fact-table grain
- dimension-key uniqueness
- referential integrity
- SCD Type 2 alignment
- spend reconciliation
- KPI-ready business logic
- fact/dimension relationship completeness

Gold tables are not considered reporting-ready until these controls pass.

---

# 4. Core Validation Categories

## Schema Validation

Checks whether required columns and datatypes are available before downstream logic executes.

Typical failures include:

- missing columns
- unexpected renaming
- incorrect datatypes
- incompatible schema changes

Schema validation reduces silent downstream failures.

---

## Duplicate-Grain Checks

Duplicate detection is performed according to each table's intended business grain.

Examples:

```text
Purchase Order Fact
→ expected PO-item grain

Supplier Risk Prediction
→ expected supplier + prediction snapshot grain

Savings Opportunity
→ expected supplier + category grain
```

A row count alone is not sufficient; uniqueness must be tested using the correct business key combination.

---

## Referential-Integrity Checks

Facts should resolve correctly to their analytical dimensions.

Examples:

- supplier keys
- category keys
- material keys
- business-unit keys
- contract keys
- date keys

Typical checks include:

```text
Fact foreign key exists
        ↓
Matching dimension key found
        ↓
No unintended orphan records
```

Orphaned records are treated as model-quality issues because they can create incomplete or misleading reporting.

---

# 5. Spend Reconciliation

Spend is one of the most important financial controls in the model.

Reconciliation verifies that transformation logic does not unintentionally create or remove spend.

Conceptually:

```text
Upstream eligible spend
        ≈
Downstream eligible spend
```

Differences are investigated rather than assumed to be acceptable.

Potential causes include:

- duplicate joins
- incorrect grain
- missing dimension mappings
- currency conversion issues
- filtering differences

---

# 6. Contract-Compliance Reconciliation

Contract compliance is validated independently from the final Power BI measure.

The objective is to ensure that:

```text
Eligible Spend
=
Contract-Compliant Spend
+
Maverick Spend
```

subject to the project's defined eligibility rules.

The corresponding percentages should remain logically consistent:

```text
Contract Compliance %
+
Maverick Spend %
≈
100%
```

Small numerical differences may occur because of rounding, but material differences indicate transformation or classification issues.

---

# 7. Supplier SCD Type 2 Validation

`dim_supplier` uses Slowly Changing Dimension Type 2.

Validation therefore includes more than checking whether supplier IDs exist.

The model must ensure that each historical transaction resolves to the correct supplier version.

Conceptually:

```text
Supplier Business Key
        +
Transaction Date
        ↓
Correct Effective-Date Range
        ↓
Supplier Surrogate Key
```

Key controls include:

- no overlapping supplier effective-date ranges
- correct current-version indicator
- valid start/end dates
- successful as-of resolution
- no unintended fact-to-supplier mismatches

This prevents current supplier attributes from incorrectly rewriting historical reporting.

---

# 8. Invoice and Matching Validation

Invoice analytics require consistency between purchasing, receipt, and invoice records.

Validation areas include:

- expected invoice grain
- invoice-to-PO relationships
- goods-receipt linkage
- expected versus invoiced value
- invoice exception logic
- three-way match classification

The purpose is to ensure that invoice KPIs are based on coherent purchasing events rather than disconnected records.

---

# 9. ML Validation

Machine-learning outputs are validated before they are consumed by the semantic model.

Validation differs by model because each output has a different analytical grain.

---

## Supplier Risk Prediction

Typical checks include:

- expected supplier prediction grain
- supplier-key completeness
- prediction-score range
- valid risk classification
- duplicate prediction snapshots
- source-to-output reconciliation

The model output is treated as a prediction snapshot, not an operational supplier-performance fact.

---

## Pricing Anomaly Prediction

Typical checks include:

- scored PO-item grain
- valid anomaly score
- binary anomaly flag
- no duplicate scored items
- correct item-level linkage back to procurement spend

Validated synthetic snapshot:

- **21,752** PO items scored
- **1,216** pricing anomalies
- **5.59%** anomaly rate

The validation ensures that anomaly counts and rates reconcile to the scored population.

---

## Savings Opportunity

Typical checks include:

- supplier-category grain
- unique opportunity keys
- non-negative analytical values where required
- valid prioritization fields
- source-spend reconciliation
- opportunity-count reconciliation

Validated synthetic snapshot:

- **983** supplier-category opportunities
- **955** positive savings opportunities
- **471** actionable opportunities
- **€97.55M** modeled potential annual savings

These values are synthetic portfolio outputs, not realized business savings.

---

# 10. Databricks-to-Fabric Reconciliation

ML results are generated in Azure Databricks and promoted back into Fabric Gold.

The promotion step is validated so the Fabric-side analytical tables accurately represent the Databricks outputs.

Typical checks include:

- source row count versus promoted row count
- analytical grain
- primary/business-key uniqueness
- score preservation
- key-field completeness
- aggregate reconciliation

This is important because the semantic model consumes the Fabric Gold outputs rather than Databricks development tables directly.

---

# 11. Persisted Monitoring Outputs

Important validation results are persisted where appropriate.

This allows quality checks to be reviewed after notebook execution and supports a monitoring-oriented design.

Conceptually, a persisted result can capture:

| Field | Example |
|---|---|
| Rule | Duplicate supplier prediction |
| Layer | Gold ML |
| Status | PASS |
| Actual | 0 |
| Expected | 0 |
| Validation timestamp | execution timestamp |

This approach separates validation evidence from transient notebook logs.

---

# 12. Final Fabric-Side Validation Result

The final Fabric-side ML validation notebook executed:

| Result | Count |
|---|---:|
| Total rules | **64** |
| PASS | **64** |
| FAIL | **0** |

This represents the validated DEV portfolio environment.

The result demonstrates that the implemented validation rules passed for the tested synthetic dataset.

It does **not** imply that production data would always pass or that the project provides a production availability SLA.

---

# 13. Example Validation Pattern

A simplified rule pattern is:

```python
actual = dataframe.count()
expected_minimum = 1

status = "PASS" if actual >= expected_minimum else "FAIL"
```

Business validations extend this idea by comparing actual results with expected logical relationships.

For example:

```text
Rule:
Contract-compliant spend + maverick spend = eligible spend

Actual:
Calculated reconciliation difference

Expected:
Difference within accepted tolerance

Status:
PASS / FAIL
```

The important design principle is that a rule produces an explicit result instead of relying only on visual notebook inspection.

---

# 14. Failure Handling

A failed validation should identify:

1. **what failed**
2. **where it failed**
3. **actual result**
4. **expected result**
5. **which downstream process may be affected**

Conceptually:

```text
Validation failure
      ↓
Identify affected layer/table
      ↓
Inspect transformation or relationship
      ↓
Correct issue
      ↓
Rerun affected processing
      ↓
Revalidate
```

For a production system, critical failures would normally be integrated into orchestration gates and alerting.

Automated failure gates are documented as a productionization extension for this portfolio.

---

# 15. Validation vs. Production Monitoring

The current project demonstrates robust analytical validation, but production monitoring would require additional operational controls.

## Implemented in the portfolio

- schema validation
- duplicate-grain checks
- spend reconciliation
- contract-compliance reconciliation
- supplier SCD2 alignment
- referential-integrity checks
- ML prediction validation
- score-range validation
- savings reconciliation
- Databricks-to-Fabric reconciliation
- persisted monitoring outputs
- final consolidated validation

## Future production extensions

- automated pipeline failure gates
- notifications and alerting
- SLA monitoring
- freshness checks
- volume-anomaly monitoring
- source-arrival monitoring
- historical rule trending
- automated incident creation
- environment-specific monitoring thresholds

---

# 16. Key Data Quality Controls

| Control | Purpose |
|---|---|
| Schema validation | Prevent structural incompatibility |
| Duplicate-grain validation | Protect fact and prediction grain |
| Referential integrity | Prevent orphan analytical records |
| Spend reconciliation | Protect financial totals |
| Compliance reconciliation | Validate procurement classification |
| SCD2 as-of validation | Preserve historical supplier context |
| Invoice matching validation | Protect invoice and three-way-match analytics |
| ML grain validation | Prevent duplicated or misaligned predictions |
| Score-range validation | Detect invalid model outputs |
| Databricks/Fabric reconciliation | Validate cross-platform promotion |
| Persisted rule results | Retain validation evidence |

---

## Related Documentation

- `README.md` — project overview
- `docs/architecture/architecture.md` — platform architecture
- `docs/architecture/data-model.md` — analytical model and table grains
- `docs/ml/` — ML implementation
- `docs/cicd/` — source control and deployment
