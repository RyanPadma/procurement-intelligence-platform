# Enterprise Procurement Intelligence Platform
## Data Quality & Validation

This document explains how data quality is validated across the procurement analytics platform from source generation through Fabric Gold and ML output promotion.

> **Portfolio note:** The project uses synthetic data. Validation results demonstrate implemented engineering controls within the portfolio environment and are not production SLAs.

---

## 1. Validation Strategy

Data quality is embedded throughout the platform rather than treated as a final reporting check.

```text
Bronze source validation
        ↓
Silver transformation validation
        ↓
Gold dimensional-model validation
        ↓
Databricks ML validation
        ↓
Fabric ML Gold promotion validation
        ↓
Semantic-model consumption
```

The objective is simple:

> **Catch structural, financial, relational, and ML-output issues before they reach Power BI.**

---

## 2. Validation Principles

The project follows five core principles.

### Validate the intended grain

Every analytical table has a defined grain.

Examples:

```text
fact_purchase_order
→ PO-item analytical grain

ml_supplier_risk_prediction
→ Supplier × Prediction Date

ml_pricing_anomaly_prediction
→ PO Item × Prediction Date

ml_savings_opportunity
→ Supplier × Category × Prediction Date
```

Duplicate detection therefore uses business-grain keys rather than relying only on row counts.

### Reconcile important measures

Key values are checked between layers so transformation logic does not silently create or remove business value.

Examples include:

- eligible spend
- compliant vs. maverick spend
- savings components
- ML output row counts
- Databricks-to-Fabric promotion totals

### Validate relationships

Gold facts and ML outputs are checked for:

- missing surrogate keys
- orphaned relationships
- invalid Supplier SCD2 alignment
- incomplete dimension mapping

### Validate business rules

Technical correctness alone is not enough.

The platform also checks procurement logic such as:

- contract-compliance reconciliation
- invoice matching
- supplier history alignment
- savings plausibility
- ML score ranges

### Persist important validation outputs

Validation results are retained where appropriate so quality evidence is auditable after notebook execution.

---

## 3. Validation by Layer

| Layer | Main controls |
|---|---|
| Bronze | schema, required keys, valid ranges, duplicate source keys |
| Silver | standardization, EUR conversion, contract logic, invoice matching |
| Gold | fact grain, dimension uniqueness, SCD2, referential integrity |
| Databricks ML | feature/score validity, coverage, output grain |
| Fabric ML Gold | surrogate-key mapping, reconciliation, persisted output validation |

---

# 4. Bronze Validation

Bronze contains synthetic ERP-style source data.

Typical checks include:

- output is not empty
- required source keys are populated
- expected columns exist
- quantities and amounts are within valid ranges
- foreign-key relationships are valid
- dates fall within expected periods
- duplicate source keys are controlled

The goal is to prevent malformed synthetic records from propagating into Silver.

---

# 5. Silver Validation

Silver standardizes Bronze data and applies procurement business logic.

Validation covers:

- datatype consistency
- duplicate handling
- missing-value treatment
- EUR conversion completeness
- contract mapping
- maverick-spend classification
- invoice matching
- supplier delivery and quality derivation

### Currency control

EUR-normalized analytical values must reconcile to governed exchange-rate logic.

Raw source-currency values remain available so conversion issues can be traced rather than hidden.

---

# 6. Gold Model Validation

Gold is the main analytical consumption layer.

Validation focuses on:

- explicit fact grain
- unique dimension keys
- referential integrity
- Supplier SCD Type 2 alignment
- financial reconciliation
- KPI-ready business logic

The analytical model is not considered reporting-ready until these checks pass.

---

## 7. Spend and Compliance Reconciliation

Spend is a core financial control.

Conceptually:

```text
Upstream Eligible Spend
        ≈
Gold Eligible Spend
```

For contract governance:

```text
Eligible Spend
=
Contract-Compliant Spend
+
Maverick Spend
```

and therefore:

```text
Contract Compliance %
+
Maverick Spend %
≈
100%
```

Material differences indicate issues such as:

- duplicated joins
- missing mappings
- incorrect grain
- filter mismatches
- currency-conversion errors

---

## 8. Supplier SCD Type 2 Validation

`dim_supplier` preserves historical supplier context.

A fact row must resolve to the supplier version valid at the relevant date.

```text
Supplier Business Key
        +
Transaction Date
        ↓
Effective-date lookup
        ↓
Correct Supplier Surrogate Key
```

Validation checks:

- no invalid overlapping effective periods
- valid current-version indicators
- correct effective-from/effective-to dates
- successful historical as-of resolution
- no unintended supplier-key gaps

This prevents current supplier attributes from rewriting historical reporting.

---

## 9. Invoice and Three-Way-Match Validation

Invoice analytics require consistent relationships between:

```text
Purchase Order
      ↓
Goods Receipt
      ↓
Invoice
```

Validation areas include:

- invoice-to-PO linkage
- goods-receipt linkage
- expected vs. invoiced value
- invoice exception logic
- dispute logic
- three-way-match classification

The purpose is to ensure invoice KPIs are based on coherent procurement events.

---

# 10. ML Quality Gates

Machine-learning outputs are validated before they become reporting data products.

## Supplier Risk

Checks include:

- supplier prediction grain
- SupplierKey completeness
- duplicate prediction snapshots
- valid score range
- valid high-risk classification
- row-count reconciliation

Validated Gold output:

- **356 supplier predictions**
- **204 high-risk suppliers**

---

## Pricing Anomaly

Checks include:

- PO-item scoring grain
- unique scored items
- valid anomaly score
- valid anomaly flag
- Gold key completeness
- scored population reconciliation

Validated output:

- **21,752 PO items scored**
- **1,216 pricing anomalies**
- **5.59% anomaly rate**

---

## Savings Opportunity

DB_06 applies an explicit quality gate before the prescriptive output is promoted.

Checks include:

- supplier-category-prediction-date grain uniqueness
- complete business keys
- savings-component reconciliation
- non-negative potential savings
- potential savings not exceeding spend
- plausible savings percentages
- negotiation priority score between 0 and 100
- valid opportunity ranks
- positive opportunity population
- actionable opportunity population
- minimum pricing-signal coverage
- minimum supplier-risk coverage

<!-- SCREENSHOT: docs/screenshots/ml/06-savings-opportunity-results.jpg -->
![Savings opportunity quality gate](screenshots/ml/06-savings-opportunity-results.jpg)

*DB_06 evidence showing the savings-opportunity quality gate passing, with 100% pricing-signal coverage, 100% supplier-risk coverage, 955 positive opportunities, and 471 actionable opportunities.*

---

# 11. Databricks → Fabric Gold Reconciliation

ML outputs are generated in Databricks but consumed from Fabric Gold.

DB_07 therefore validates the cross-platform promotion step.

Checks include:

- source vs. target row count
- business-grain uniqueness
- surrogate-key mapping
- missing dimensional keys
- score preservation
- classification preservation
- savings-value reconciliation

<!-- SCREENSHOT: docs/screenshots/ml/07-ml-output-promotion-validation.jpg -->
![ML Gold promotion validation](screenshots/ml/07-ml-output-promotion-validation.jpg)

*DB_07 evidence showing the three ML products promoted to physical Fabric Gold tables after validation.*

Validated persisted snapshots:

| ML Gold product | Rows |
|---|---:|
| Supplier Risk Prediction | **356** |
| Pricing Anomaly Prediction | **21,752** |
| Savings Opportunity | **983** |

Post-write reconciliation confirmed:

| Check | Difference |
|---|---:|
| High-risk supplier classification | **0** |
| Pricing anomaly classification | **0** |
| Potential Annual Savings | **€0.00** |

---

# 12. Final Fabric Validation

The final Fabric-side validation notebook consolidates the ML Gold checks after promotion.

Validated portfolio result:

| Result | Count |
|---|---:|
| Total validation rules | **64** |
| PASS | **64** |
| FAIL | **0** |

<img width="1144" height="780" alt="02-final-ml-gold-validation" src="https://github.com/user-attachments/assets/944509fd-308a-45d3-ab78-c3bc453b2e09" />


*NB_40 evidence confirming the promoted supplier-risk, pricing-anomaly, and savings-opportunity outputs and the consolidated result of 64 PASS and 0 FAIL.*

The same validation output confirms:

- **356** supplier-risk rows
- **204** high-risk suppliers
- **21,752** pricing-anomaly rows
- **1,216** pricing anomalies
- **983** savings-opportunity rows
- **955** positive opportunities
- **471** actionable opportunities
- **€97.55M** modeled potential annual savings

This is the strongest final evidence that the ML outputs are ready for semantic-model consumption in the validated DEV environment.

---

## 13. Persisted Monitoring

Important validation results are persisted instead of existing only in notebook output.

A monitoring result can capture fields such as:

| Field | Example |
|---|---|
| Rule | Duplicate supplier prediction |
| Layer | Gold ML |
| Status | PASS |
| Actual | 0 |
| Expected | 0 |
| Validation timestamp | execution timestamp |

The ML promotion process also persists:

```text
monitoring_ml_gold_promotion_results
```

This creates an auditable record of the promotion checks.

---

## 14. Failure Handling

A failed validation should answer four questions:

1. **What failed?**
2. **Where did it fail?**
3. **What was the actual result?**
4. **What downstream process could be affected?**

Conceptually:

```text
Validation Failure
        ↓
Identify affected layer/table
        ↓
Inspect transformation / relationship
        ↓
Correct issue
        ↓
Rerun affected processing
        ↓
Revalidate
```

A production implementation would integrate critical failures into automated orchestration gates and alerting.

---

## 15. Portfolio Validation vs. Production Monitoring

### Implemented

- schema checks
- duplicate-grain validation
- financial reconciliation
- contract-compliance reconciliation
- SCD2 alignment
- referential integrity
- invoice matching validation
- ML grain validation
- ML score validation
- savings reconciliation
- Databricks-to-Fabric reconciliation
- persisted monitoring outputs
- consolidated Fabric validation

### Production extensions

A production implementation would additionally include:

- automated pipeline failure gates
- notifications and alerting
- freshness monitoring
- source-arrival monitoring
- volume-anomaly detection
- SLA monitoring
- historical rule trending
- automated incident creation
- environment-specific thresholds

These are documented as productionization extensions rather than represented as completed portfolio functionality.

---

## 16. Validation Summary

| Control | Purpose |
|---|---|
| Schema validation | Protect structural compatibility |
| Duplicate-grain checks | Preserve intended table grain |
| Referential integrity | Prevent orphan analytical records |
| Spend reconciliation | Protect financial totals |
| Compliance reconciliation | Validate procurement classification |
| SCD2 as-of validation | Preserve historical supplier context |
| Invoice matching validation | Protect invoice and match KPIs |
| ML grain validation | Prevent duplicated/misaligned predictions |
| Score-range validation | Detect invalid model outputs |
| Databricks/Fabric reconciliation | Validate cross-platform promotion |
| Persisted monitoring | Retain validation evidence |
| Final 64-rule validation | Confirm Gold ML reporting readiness |

---

## Related Documentation

- [Main README](../README.md)
- [Technical Architecture](architecture/architecture.md)
- [Data Model](architecture/data-model.md)
- [ML Pipeline & Fabric Integration](ml/ml-pipeline.md)
- [Power BI Report Guide](../power-bi/report-guide.md)
- [CI/CD & Deployment](cicd/CI-CD.md)
