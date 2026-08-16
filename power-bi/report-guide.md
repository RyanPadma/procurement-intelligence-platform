# Power BI Report Guide

The Power BI report is the decision layer of the Enterprise Procurement Intelligence Platform. It combines governed procurement KPIs, supplier-performance analytics, ML predictions, and savings intelligence through a **Direct Lake semantic model**.

> **Portfolio note:** All values are generated from synthetic data and are presented only to demonstrate the analytical solution.

---

## Report at a Glance

| Page | Primary question |
|---|---|
| Executive Overview | Where does procurement performance require leadership attention? |
| Spend & Contract Compliance | Where is spend leakage and maverick purchasing concentrated? |
| Supplier Performance & Risk | Which suppliers combine weak performance, high risk, and material exposure? |
| Pricing Anomaly & Savings Intelligence | Where do pricing signals indicate investigation or negotiation potential? |
| Savings Pipeline & Realization | How is the savings portfolio progressing from forecast to realization? |

The report follows a deliberate progression:

```text
Executive performance
        ↓
Spend governance
        ↓
Supplier performance and risk
        ↓
Pricing and savings opportunity
        ↓
Savings execution and realization
```

---

# 1. Executive Overview

**Audience:** CPO / Procurement Leadership  
**Purpose:** Provide one management view of spend governance, supplier performance, risk, and savings potential.

<img width="1252" height="685" alt="01-executive-overview" src="https://github.com/user-attachments/assets/e537d3f7-26f2-4994-8c85-a3e618bb36ac" />

### Headline KPIs

| KPI | Result |
|---|---:|
| Total Eligible Spend | **€7.46B** |
| Contract Compliance | **67.19%** |
| Supplier OTD | **86.91%** |
| Realized Savings | **€68.63M** |
| Potential Annual Savings | **€97.55M** |
| High-Risk Spend | **77.28%** |

### What the page shows

- **Spend and Contract Compliance Trend** combines yearly eligible spend with compliance development.
- **Top 10 Categories by Annual Savings Opportunity** highlights where modeled savings potential is concentrated.
- **Supplier Risk Ranking** combines risk score with eligible-spend exposure.
- Year, Business Unit, and Category filters allow management-level slicing.

### Decision supported

The page answers:

> **Where should procurement leadership focus first?**

It combines current performance with forward-looking ML signals instead of separating them into isolated dashboards.

**Modeling note:** ML risk and opportunity KPIs use the latest available prediction snapshot.

---

# 2. Spend & Contract Compliance

**Audience:** Procurement Leadership / Category Managers  
**Purpose:** Explain the financial scale and location of off-contract purchasing.

<img width="1251" height="681" alt="02-spend-compliance" src="https://github.com/user-attachments/assets/05cd1fdf-0bc7-4a5f-bf8d-2df4382d65cc" />

### Headline KPIs

| KPI | Result |
|---|---:|
| Total Eligible Spend | **€7.46B** |
| Compliant Spend | **€5.01B** |
| Maverick Spend | **€2.45B** |
| Contract Compliance | **67.19%** |
| Maverick Spend | **32.81%** |
| Purchase Orders | **20K** |

### Main analytical views

**Spend and Contract Compliance Trend**  
Shows how eligible spend and compliance changed from 2022 through 2026.

**Top Categories by Maverick Spend**  
Identifies the categories driving the largest off-contract spend exposure.

**Top Suppliers by Maverick Spend**  
Provides supplier-level detail including:

- eligible spend
- compliant spend
- maverick spend
- compliance %
- maverick spend %

**Contract Compliance by Business Unit**  
Compares compliant and maverick-spend shares across organizational units.

### Decision supported

```text
Maverick Spend
      ↓
Category / Supplier / Business Unit
      ↓
Source of leakage identified
      ↓
Contract or sourcing action
```

The page moves from the enterprise compliance rate to the specific suppliers and categories creating the leakage.

---

# 3. Supplier Performance & Risk

**Audience:** Supplier Management / Category Managers  
**Purpose:** Combine operational supplier performance with predictive risk.

<img width="1246" height="682" alt="03-supplier-performance-risk" src="https://github.com/user-attachments/assets/132733a3-14c8-4866-91fb-a36a7cbaa82b" />

### Headline KPIs

| KPI | Result |
|---|---:|
| Supplier OTD | **86.91%** |
| Supplier Quality Index | **96%** |
| Three-Way Match | **91.89%** |
| Invoice Dispute Rate | **4.33%** |
| High-Risk Suppliers | **204** |
| High-Risk Supplier % | **57.30%** |

### Main analytical views

**Supplier Performance Trend**  
Tracks OTD and Supplier Quality Index over time.

**Top 10 Suppliers by Risk Score**  
Surfaces the suppliers with the highest latest ML risk scores.

**Top Suppliers by Overdue Deliveries**  
Highlights current operational delivery issues.

**Supplier Performance & Risk Detail**  
Brings operational and predictive indicators into one supplier-level view:

- Supplier OTD %
- Supplier Quality Index
- Three-Way Match %
- Invoice Dispute %
- Risk Score
- Eligible Spend

### Important filter behavior

`Performance Year` filters the operational supplier-performance KPIs.

Supplier-risk metrics use the **latest available ML prediction snapshot** rather than pretending the prediction is an ordinary historical transaction.

This reflects the underlying data model, where:

```text
fact_supplier_performance
        ≠
ml_supplier_risk_prediction
```

### Decision supported

> **Which suppliers need attention because performance weakness or predicted risk is combined with meaningful procurement exposure?**

---

# 4. Pricing Anomaly & Savings Intelligence

**Audience:** Strategic Sourcing / Category Managers  
**Purpose:** Turn ML pricing signals into prioritized procurement opportunities.

<img width="1240" height="688" alt="04-pricing-savings-intelligence" src="https://github.com/user-attachments/assets/a50f528c-a7ce-435c-a9b4-07de16d61089" />

### Headline KPIs

| KPI | Result |
|---|---:|
| Pricing Items Scored | **21,752** |
| Pricing Anomalies | **1,216** |
| Pricing Anomaly Rate | **5.59%** |
| Anomalous Spend | **€139.84M** |
| Potential Annual Savings | **€97.55M** |
| Actionable Savings | **€96.93M** |

### Main analytical views

**Top Categories by Pricing Anomalies**  
Shows where unusual transaction pricing occurs most frequently.

**Top Categories by Savings Opportunity**  
Ranks categories by modeled savings potential.

**Suppliers with Highest Pricing Anomaly Exposure**  
Combines anomaly count/rate with the eligible spend affected by anomalous transactions.

**Top Supplier-Category Savings Opportunities**  
Turns ML and procurement signals into a sourcing action list containing:

- supplier
- category
- potential savings
- actionable savings
- negotiation priority
- priority score

### Decision supported

```text
Pricing Anomaly
       +
Spend Exposure
       +
Maverick Leakage
       +
Supplier Risk
       ↓
Supplier-Category Opportunity
       ↓
Negotiation Priority
```

This is the page where the platform moves from **predictive analytics to prescriptive procurement action**.

Pricing anomaly and opportunity measures use the latest available ML prediction snapshot.

---

# 5. Savings Pipeline & Realization

**Audience:** Procurement Leadership / Savings Owners  
**Purpose:** Track execution of the synthetic savings portfolio separately from ML-generated future opportunity.

<img width="1246" height="690" alt="05-savings-pipeline-realization" src="https://github.com/user-attachments/assets/046ab8fd-f7da-4cca-9dba-d0c7a795a39a" />

### Headline KPIs

| KPI | Result |
|---|---:|
| Active Projects | **1,811** |
| Forecasted Savings | **€201.22M** |
| Weighted Forecast | **€107.25M** |
| Approved Savings | **€116.61M** |
| Realized Savings | **€68.63M** |
| Realization | **58.85%** |

### Main analytical views

**Savings Pipeline & Realization Trend**  
Compares forecasted, approved, and realized savings across the savings timeline.

**Top Categories by Weighted Savings Forecast**  
Highlights where the active savings pipeline is concentrated.

**Approved vs. Realized Savings by Business Unit**  
Shows where approved savings are translating into realized results.

**Savings Project Portfolio**  
Provides project-level execution detail including:

- project
- supplier
- category
- forecast
- weighted forecast
- approved savings
- realized savings
- project status

### Important distinction

The report deliberately separates:

```text
Modeled opportunity
ml_savings_opportunity
```

from:

```text
Savings execution
fact_savings
```

Therefore **€97.55M Potential Annual Savings** is not treated as realized savings.

The savings pipeline independently tracks forecast, approval, implementation, and realization.

---

# 6. Cross-Page Design

The five pages are connected through a common business narrative.

### Spend governance

```text
€7.46B Eligible Spend
→ 67.19% Contract Compliance
→ €2.45B Maverick Spend
```

### Supplier management

```text
86.91% OTD
+ 96% Quality Index
+ Risk Prediction
→ Supplier Prioritization
```

### Pricing intelligence

```text
21,752 Items Scored
→ 1,216 Anomalies
→ €139.84M Anomalous Spend
```

### Prescriptive opportunity

```text
Pricing + Compliance + Risk
→ €97.55M Potential Annual Savings
→ €96.93M Actionable Savings
```

### Savings execution

```text
€201.22M Forecast
→ €116.61M Approved
→ €68.63M Realized
```

This progression keeps **opportunity identification** separate from **savings realization**.

---

# 7. Semantic Model Principles Visible in the Report

The report consumes a governed **Direct Lake semantic model**.

Several modeling decisions are visible in the report behavior:

- Procurement KPIs use consistent semantic-model measures across pages.
- Supplier operational history and ML risk predictions remain at different grains.
- ML pages use the latest prediction snapshot.
- Historical operational KPIs retain their own date filtering.
- Currency-normalized financial measures are consumed from governed upstream data.
- Savings opportunity and savings realization remain separate analytical processes.

Power BI is therefore primarily responsible for **business interaction and presentation**, not data cleansing or ML logic.

---

# 8. Report Design Strengths

The implemented report follows four useful design principles.

### KPI-first

Each page starts with a compact KPI strip before moving into diagnostic detail.

### Overview → Driver → Detail

Most pages follow the same analytical pattern:

```text
Headline KPI
    ↓
Trend / ranking
    ↓
Supplier or category driver
    ↓
Detailed action table
```

### Consistent filtering

Slicers are positioned consistently at the top of each page and adapt to the business process being analyzed.

### ML with business context

Risk scores and anomaly flags are never shown in isolation. They are connected to:

- spend
- supplier
- category
- operational performance
- savings potential

That makes the ML outputs useful to procurement rather than merely technically interesting.

---

# 9. Screenshot Assets

Recommended repository structure:

```text
power-bi/
├── screenshots/
│   ├── 01-executive-overview.jpg
│   ├── 02-spend-compliance.jpg
│   ├── 03-supplier-performance-risk.jpg
│   ├── 04-pricing-savings-intelligence.jpg
│   └── 05-savings-pipeline-realization.jpg
└── theme/
```

One full-page screenshot per report page is sufficient for the portfolio.

The Fabric-generated report and semantic-model definitions remain under `/fabric`; `/power-bi` contains presentation-friendly assets for GitHub reviewers.

---

## Related Documentation

- `README.md` — project overview
- `docs/architecture/architecture.md` — solution architecture
- `docs/architecture/data-model.md` — analytical model
- `docs/data-quality.md` — validation framework
- `docs/ml/` — ML and prescriptive analytics
- `docs/cicd/CI-CD.md` — source control and deployment
