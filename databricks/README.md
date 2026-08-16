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
