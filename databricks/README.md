# Azure Databricks ML Layer

This folder contains the Azure Databricks implementation used to extend the Microsoft Fabric procurement data platform with machine learning and prescriptive analytics.

The Databricks layer consumes governed Gold data from Microsoft Fabric OneLake, performs feature engineering and ML scoring, and promotes validated outputs back into Fabric Gold for consumption by the Direct Lake semantic model and Power BI.

## Workflow

```text
Microsoft Fabric Gold
        ↓
OneLake / ABFSS
        ↓
Azure Databricks
        │
        ├── Supplier Risk Prediction
        │
        ├── Pricing Anomaly Detection
        │
        └── Savings Opportunity Engine
        ↓
Validated ML Outputs
        ↓
Microsoft Fabric Gold
        ↓
Direct Lake Semantic Model
        ↓
Power BI
```

# Notebooks

## DB01 - OneLake Access

Establishes secure connectivity between Azure Databricks and Microsoft Fabric OneLake.

Authentication credentials are retrieved through Databricks secret management rather than stored directly in notebook source code.

Environment-specific Fabric workspace and Lakehouse identifiers are represented as configuration values/placeholders in the public repository.

## DB02 - Supplier Risk Feature Engineering

Builds supplier-level features used by the supplier risk model.

Feature domains include:

- Delivery performance
- Invoice disputes
- Procurement spend behavior
- Spend volatility
- Supplier attributes
- Synthetic risk indicators

The resulting feature dataset is used by DB03.

## DB03 - Supplier Risk Model

Trains and evaluates a Random Forest classifier for supplier risk.

The model is treated as an experimental portfolio baseline rather than a production-performance claim.

Latest validated scoring population:

- 356 suppliers scored
- 204 classified as high risk

Model experiments and metrics were tracked using MLflow.

<img width="1899" height="846" alt="01-mlflow-supplier-risk-experiment" src="https://github.com/user-attachments/assets/f891f063-0dcd-4a55-b178-463b636de14c" />


## DB04 - Pricing Anomaly Feature Engineering

Creates PO-item-level features for pricing anomaly detection.

Feature engineering incorporates signals such as:

- Historical purchasing prices
- Contract price variance
- Category-level pricing behavior
- Supplier pricing behavior
- Transaction characteristics

## DB05 - Pricing Anomaly Detection

Applies Isolation Forest to identify unusual purchasing-price behavior.

Latest validated scoring snapshot:

- 21,752 PO items scored
- 1,216 anomalies
- 5.59% anomaly rate

A governed anomaly threshold is persisted downstream rather than relying directly on the default Isolation Forest prediction label.

<img width="1911" height="873" alt="02-pricing-anomaly-results" src="https://github.com/user-attachments/assets/4241905d-d112-4d2c-ae8b-5e9ce97fa9ba" />


## DB06 - Savings Opportunity Engine

Creates a prescriptive supplier-category opportunity model.

The engine combines:

- Pricing anomaly signals
- Maverick-spend leakage
- Supplier risk
- Annualized eligible spend
- Negotiation potential

Latest validated synthetic portfolio results:

- 983 supplier-category opportunities
- 955 positive opportunities
- 471 actionable opportunities
- €97.55M modeled potential annual savings

These values are simulated portfolio outputs and do not represent realized business savings.

<img width="1905" height="867" alt="03-savings-opportunity-results" src="https://github.com/user-attachments/assets/87b404fd-08dd-4f8e-8117-3f0b07fcd4ef" />


## DB07 - ML Output Promotion

Validates, enriches, and promotes ML outputs from Databricks back into Microsoft Fabric Gold.

The promoted datasets are:

- ml_supplier_risk_prediction
- ml_pricing_anomaly_prediction
- ml_savings_opportunity

DB07 includes:

- Grain validation
- Duplicate checks
- Supplier SCD2 as-of resolution
- Referential-integrity validation
- Deterministic prediction keys
- Source-to-Gold reconciliation
- Snapshot-aware persistence

The notebook promotes existing ML outputs and does not retrain or rescore the models.

## ML Output Grains
| Dataset             | Grain                                 |
| ------------------- | ------------------------------------- |
| Supplier Risk       | Supplier × Prediction Date            |
| Pricing Anomaly     | PO Item × Prediction Date             |
| Savings Opportunity | Supplier × Category × Prediction Date |


## Security

Secrets are not stored in this repository.

Databricks retrieves Azure/Fabric authentication values securely through secret scopes using dbutils.secrets.get().

Any environment-specific resource identifiers in the public version are parameterized or replaced with placeholders.

## Disclaimer

This project uses synthetic procurement data.

Model outputs, supplier risk classifications, anomaly results, and savings opportunities are created for portfolio demonstration purposes and should not be interpreted as production forecasts or realized financial results.
