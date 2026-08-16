# Supplier Risk Modeling

This document summarizes the supplier-risk ML workflow implemented in Azure Databricks.

> **Portfolio note:** The model uses synthetic data. Results demonstrate the modeling process and controls, not production predictive performance.

---

## 1. Objective

The model predicts whether a supplier is likely to experience **high operational risk in the following year**.

The output is designed to complement historical supplier-performance reporting with a forward-looking risk signal that can be evaluated together with spend exposure, delivery performance, quality, and disputes.

```text
Historical supplier behavior
        ↓
Future-risk target
        ↓
Model training and temporal validation
        ↓
2026 supplier risk score
        ↓
Fabric Gold → Power BI
```

---

## 2. Leakage-Safe Target Design

The target is deliberately future-looking.

```text
2022 features → 2023 outcome
2023 features → 2024 outcome
2024 features → 2025 outcome
```

2026 is **not** used as a training outcome because the year is incomplete.

A supplier is labeled `HighRiskNextYearFlag = 1` when at least two future adverse conditions occur.

The adverse-condition thresholds are derived from the training population:

- low future OTD: bottom quartile
- high overdue-delivery exposure: top quartile
- high invoice-dispute rate: top quartile
- high invoice-exception rate: top quartile

This creates a reproducible target without teaching the model to reproduce a current-year risk formula.

---

## 3. Feature Engineering

`DB_02_Supplier_Risk_Feature_Engineering` builds a supplier-year dataset from Fabric Gold.

Feature areas include:

- delivery performance and trends
- invoice disputes and exceptions
- spend and spend volatility
- maverick-spend behavior
- supplier ESG-style attributes
- synthetic financial-risk attributes
- country and region
- prior-year history availability

Key preparation results:

| Dataset | Result |
|---|---:|
| Supplier-year source rows | 1,869 |
| Labeled training rows | 1,009 |
| 2026 scoring suppliers | 356 |
| Final training columns | 47 |

Future outcome fields are removed before the training dataset is persisted.

For 2026 scoring, partial-year spend is annualized using a factor of **1.7217**.

---

## 4. Model Development

`DB_03_Train_Supplier_Risk_Model` compares three supervised models:

- Logistic Regression
- Random Forest
- Gradient Boosting

The model uses:

- **39 numeric features**
- **5 categorical features**
- median imputation for numeric missing values
- `Unknown` replacement plus one-hot encoding for categorical missing values

### Temporal design

```text
2022–2023 → model development
2024      → untouched temporal test
2026      → scoring population
```

Development uses **5-fold grouped cross-validation by SupplierID** so records from the same supplier are not split carelessly across folds.

Model selection uses out-of-fold **PR-AUC**.

---

## 5. Selected Model

**Random Forest** produced the strongest development result and was selected.

The operating threshold was optimized separately using development predictions.

| Item | Result |
|---|---:|
| Selected model | Random Forest |
| Decision threshold | **0.418** |
| Development rows | 664 |
| Temporal test rows | 345 |
| 2026 suppliers scored | 356 |

After temporal validation, the production candidate is retrained on all labeled history from **2022–2024**.

MLflow tracks the candidate models, temporal-test model, and production scoring model.

---

## 6. Temporal Test Result

The untouched 2024 test provides a realistic view of model performance.

| Metric | 2024 result |
|---|---:|
| High-risk prevalence | 27.83% |
| ROC-AUC | **0.5494** |
| PR-AUC | **0.3588** |
| PR baseline | 0.2783 |
| Precision | **0.3007** |
| Recall | **0.4792** |
| F1 | **0.3695** |

Confusion matrix:

```text
TN 142   FP 107
FN  50   TP  46
```

The model ranks risk better than the prevalence baseline, but predictive strength remains limited.

The original BRD precision target of **85% was not met**.

That limitation is intentionally retained in the portfolio rather than hidden. The model is therefore positioned as an **experimental baseline**, not a production-ready risk classifier.

---

## 7. 2026 Scoring Output

The production candidate scores **356 suppliers**.

The output includes a 0–100 `SupplierRiskScore` and a high-risk classification.

The predictions are promoted into:

```text
ml_supplier_risk_prediction
```

In the validated Gold snapshot:

- 356 supplier predictions were written
- 204 suppliers were classified high risk
- duplicate prediction grain: 0
- missing SupplierKey: 0
- invalid risk scores: 0
- invalid risk flags: 0

---

## 8. Why This Model Matters

The useful output is not the score alone.

```text
Supplier Risk Score
        +
Spend Exposure
        +
Delivery / Quality Performance
        ↓
Procurement Prioritization
```

The Power BI layer can therefore distinguish between a high-risk supplier with limited exposure and one whose risk affects substantial procurement spend.

---

## 9. Key Engineering Controls

- Future outcomes are separated from current-year features.
- 2026 partial-year outcomes are excluded from training.
- Future outcome columns are removed before model training.
- Supplier grouping is used during cross-validation.
- 2024 remains untouched until final temporal evaluation.
- Missing values are handled inside the preprocessing pipeline.
- Candidate models are compared rather than selecting one arbitrarily.
- MLflow tracks experiments and production candidates.
- Gold promotion validates keys, grain, score ranges, and classifications.

---

## Related Notebooks

- `DB_02_Supplier_Risk_Feature_Engineering`
- `DB_03_Train_Supplier_Risk_Model`
- `DB_07_Score_and_Write_ML_Outputs`

Related docs: `architecture.md`, `data-model.md`, `data-quality.md`, and `ml-pipeline.md`.
