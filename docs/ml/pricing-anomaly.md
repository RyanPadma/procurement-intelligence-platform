# Pricing Anomaly Detection

This document summarizes the pricing-anomaly workflow implemented in Azure Databricks.

> **Portfolio note:** The model uses synthetic procurement data. Anomalies identify unusual pricing patterns for review; they do not automatically represent confirmed overcharges.

---

## 1. Objective

The model identifies PO items whose purchasing prices behave unusually relative to historical and governed pricing context.

```text
PO-item pricing history
        ↓
Leakage-safe benchmarks
        ↓
Isolation Forest
        ↓
Anomaly score and flag
        ↓
Fabric Gold → Power BI
```

The output is intended to help procurement focus investigation on transactions with unusual price behavior and meaningful spend exposure.

---

## 2. Leakage-Safe Feature Engineering

`DB_04_Pricing_Anomaly_Feature_Engineering` operates at **PO-item grain**.

The central design rule is:

> The current PO item's price never contributes to its own historical benchmark.

Historical windows therefore use only transactions that occurred before the item being evaluated.

Benchmarks are engineered at three levels:

- same material
- same supplier + material
- category

Governed contract-price information is also carried into the feature set when available.

The PO-item grain was validated across **75,994** eligible historical and current records.

---

## 3. Model Feature Contract

The Isolation Forest uses **19 numeric features** covering:

- price-to-history ratios
- percentage deviations
- price z-scores
- supplier-material price variability
- category deviations
- governed contract-price variance
- log unit price and quantity
- historical benchmark availability

Examples include:

```text
MaterialPriceZScore
SupplierMaterialPriceZScore
CategoryPriceZScore
AbsoluteContractPriceVariancePct
SupplierMaterialHistoricalCVPct
BenchmarkCoverageCount
```

A rule-based extreme-price flag is retained only as a **diagnostic proxy**. It is **not used as a training target**.

---

## 4. Temporal Design

The feature store contains **54,023 historical rows** through 2025.

Model development is deliberately separated by time:

```text
2023–2024 → development
2025      → untouched temporal diagnostic
2023–2025 → final production training
2026      → scoring
```

| Population | Rows |
|---|---:|
| Development, 2023–2024 | 22,569 |
| 2025 temporal diagnostic | 18,940 |
| Production training, 2023–2025 | 41,509 |
| 2026 scoring | 21,752 |

All **21,752** 2026 pricing rows were model-eligible.

---

## 5. Isolation Forest

Isolation Forest is used because the problem is naturally unsupervised: confirmed anomaly labels are not available.

The pipeline applies median imputation before the model.

The development model uses a **5% review rate** to establish the anomaly threshold.

A normalized anomaly score from **0 to 100** is produced for business consumption.

---

## 6. 2025 Temporal Diagnostic

The untouched 2025 population is used to test whether the unsupervised ranking aligns with independent pricing signals.

| Metric | Result |
|---|---:|
| Predicted anomaly rate | **5.79%** |
| Extreme-price proxy prevalence | 8.40% |
| ROC-AUC vs. proxy | **0.7958** |
| PR-AUC vs. proxy | **0.2857** |
| Precision vs. proxy | 35.64% |
| Recall vs. proxy | 24.58% |
| Precision lift vs. proxy baseline | **4.24×** |

A second diagnostic compares anomalies against governed contract-price exceptions:

| Metric | Result |
|---|---:|
| Overall contract-price exception rate | 1.38% |
| Exception rate among anomalies | **11.03%** |
| Enrichment lift | **7.97×** |

These proxies are validation aids, not ground-truth anomaly labels.

---

## 7. Production Scoring

The final model is retrained on **41,509 rows from 2023–2025** and then scores 2026.

Validated result:

| Metric | Result |
|---|---:|
| PO items scored | **21,752** |
| Pricing anomalies | **1,216** |
| Anomaly rate | **5.59%** |

The output is promoted into:

```text
ml_pricing_anomaly_prediction
```

Gold validation found:

- duplicate grain: 0
- missing PurchaseOrderFactKey: 0
- missing dimensional keys: 0
- invalid anomaly scores: 0

---

## 8. Business Use

An anomaly flag is not treated as proof of savings.

Instead:

```text
Anomaly
   +
Spend Exposure
   +
Supplier / Material / Category Context
   +
Contract Pricing Evidence
        ↓
Procurement Review
```

The model therefore supports investigation and also feeds the downstream Savings Opportunity Engine.

---

## 9. Key Engineering Controls

- PO-item grain is explicitly validated.
- The current transaction is excluded from its own benchmark.
- Historical benchmark availability is modeled explicitly.
- A diagnostic proxy is kept separate from training.
- 2025 is untouched during development.
- Production training incorporates 2025 only after temporal diagnostics.
- Anomaly score ranges and row counts are validated.
- MLflow retains both diagnostic and production model runs.
- Databricks results are reconciled after promotion to Fabric Gold.

---

## Related Notebooks

- `DB_04_Pricing_Anomaly_Feature_Engineering`
- `DB_05_Train_Pricing_Anomaly_Model`
- `DB_07_Score_and_Write_ML_Outputs`

Related docs: `supplier-risk.md`, `savings-opportunity.md`, `data-quality.md`, and `ml-pipeline.md`.
