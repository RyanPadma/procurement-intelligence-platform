# Savings Opportunity Engine

This document summarizes the prescriptive savings-opportunity workflow implemented in Azure Databricks.

> **Portfolio note:** Potential savings are modeled from synthetic data. They represent analytical opportunities, not realized business savings.

---

## 1. Objective

The Savings Opportunity Engine converts procurement signals into a prioritized supplier-category action list.

It combines:

- eligible spend
- pricing-anomaly evidence
- maverick spend
- supplier risk
- category concentration
- negotiation potential

```text
Spend + Pricing + Compliance + Risk
                 ↓
Supplier × Category opportunity
                 ↓
Potential Annual Savings
                 ↓
Negotiation Priority
```

The output is intentionally different from `fact_savings`, which tracks the actual synthetic savings pipeline and realization lifecycle.

---

## 2. Input Population

`DB_06_Build_Savings_Opportunity_Engine` starts from the same governed spend basis used by procurement KPIs.

For the 2026 YTD population:

| Metric | Result |
|---|---:|
| Eligible PO items | **20,632** |
| YTD eligible spend | **€2.218B** |
| Annualization factor | **1.7217** |

Pricing-anomaly signals are joined from DB_05.

Coverage is **100%** across the eligible PO-item population.

Supplier-risk predictions are also joined with **100% coverage** at the final supplier-category grain.

---

## 3. Pricing Savings Benchmark

A benchmark hierarchy is used to estimate price opportunity:

1. negotiated contract benchmark
2. same supplier + same material historical benchmark
3. same material historical benchmark across suppliers

Category-average price deviation is **not** used directly to calculate euro savings because materials within the same category can have fundamentally different unit prices.

Category pricing remains useful as an anomaly signal, but not as a direct cash benchmark.

---

## 4. PO-Item Savings Evidence

Pricing opportunity is estimated from the excess embedded in current spend.

Conceptually:

```text
Actual Price = Benchmark × (1 + variance)

Estimated Excess
= Actual Spend × variance / (1 + variance)
```

The engine also incorporates modeled recovery from maverick-spend leakage.

Pricing variance is capped at **50%**, and the configured maverick recovery rate is **3%**.

These are modeling assumptions used to produce a controlled portfolio demonstration.

---

## 5. Supplier-Category Aggregation

PO-item evidence is aggregated to:

```text
Supplier × Category × Prediction Date
```

This produces **983 supplier-category opportunities**.

The engine then adds:

- annualized eligible spend
- annualized pricing opportunity
- annualized maverick opportunity
- category concentration
- supplier risk score
- pricing anomaly rate
- maverick-spend percentage

Savings components are explicitly reconciled before ranking.

---

## 6. Negotiation Priority

The engine creates normalized priority components and combines them into a **0–100 Negotiation Priority Score**.

It also determines:

- primary opportunity driver
- priority band
- savings opportunity rank
- actionable opportunity flag

The purpose is to separate **financial potential** from **action priority**.

A large theoretical saving may not always be the highest-priority negotiation once risk, concentration, and other signals are considered.

---

## 7. Validated Portfolio Output

| Metric | Result |
|---|---:|
| Supplier-category opportunities | **983** |
| Positive savings opportunities | **955** |
| Actionable opportunities | **471** |
| Modeled potential annual savings | **€97.55M** |
| Pricing signal coverage | **100%** |
| Supplier risk coverage | **100%** |

The engine run is tracked in MLflow and the final output includes lineage metadata.

---

## 8. Quality Controls

The DB_06 quality gate validates that:

- output contains rows
- supplier-category-prediction-date grain is unique
- business keys are complete
- savings components reconcile
- potential savings are non-negative
- potential savings do not exceed spend
- savings percentages remain plausible
- priority scores remain between 0 and 100
- ranks are valid
- positive opportunities exist
- actionable opportunities exist
- pricing and supplier-risk coverage exceed required thresholds

All DB_06 quality gates passed in the validated portfolio run.

---

## 9. Gold Promotion

The final output is promoted into:

```text
ml_savings_opportunity
```

During DB_07 promotion:

- 983 rows were preserved
- duplicate grain: 0
- missing dimensional keys: 0
- negative savings: 0
- invalid priority scores: 0
- invalid ranks: 0
- all savings suppliers had supplier-risk predictions

Potential Annual Savings also reconciled exactly after the Gold write.

---

## 10. Business Use

The engine is designed to answer:

> Where should procurement focus first, and why?

A Power BI user can evaluate an opportunity together with:

- supplier
- category
- annualized spend
- estimated pricing opportunity
- maverick leakage
- supplier risk
- concentration
- negotiation priority

This turns descriptive analytics and model signals into a prescriptive procurement workflow.

---

## Related Notebooks

- `DB_05_Train_Pricing_Anomaly_Model`
- `DB_06_Build_Savings_Opportunity_Engine`
- `DB_07_Score_and_Write_ML_Outputs`

Related docs: `supplier-risk.md`, `pricing-anomaly.md`, `data-quality.md`, and `ml-pipeline.md`.
