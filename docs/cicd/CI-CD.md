# CI/CD Implementation

## Scope

This project uses native Microsoft Fabric Git integration and deployment pipelines to demonstrate a controlled analytics development lifecycle.

## Implemented

- Fabric DEV workspace connected to GitHub
- Dedicated `dev` branch for active development
- Stable `main` branch
- Pull-request based promotion from `dev` to `main`
- Fabric-generated workspace definitions stored under `/fabric`
- Three-stage Fabric deployment pipeline
- Successful DEV → TEST artifact deployment
- TEST notebook dependencies rebound to TEST workspace artifacts

## Deployment model

```text
Fabric DEV
    ↕
GitHub dev
    ↓
Pull Request
    ↓
GitHub main

Fabric Deployment Pipeline
    ↓
TEST
    ↓
Validation
    ↓
PROD promotion stage
```

## Important environment behavior

Fabric deployment pipelines promote analytical item definitions but do not copy physical Delta Lake table data between environments.

The deployed TEST environment therefore receives Lakehouse definitions and supported artifact metadata, while target-environment physical data must be initialized independently.

The intended production pattern is:

```text
Artifact deployment
    ↓
Environment initialization
    ↓
Data orchestration
    ↓
Validation gates
    ↓
Production approval
```

For the portfolio implementation, successful DEV → TEST artifact deployment was demonstrated and validated. Full data duplication into TEST/PROD was intentionally not performed because the dataset is synthetic and the objective of this section is to demonstrate source control and deployment lifecycle design rather than duplicate compute/storage usage.

## Evidence

1. GitHub `/fabric` repository
<img width="1894" height="993" alt="1  GIthub repository showing Fabric" src="https://github.com/user-attachments/assets/d1130d68-aafe-498d-a950-72e8a39ed022" />

2. `dev` and `main` branches
<img width="1900" height="769" alt="2  Dev and main branches" src="https://github.com/user-attachments/assets/eead4c25-d02e-4edc-9795-1d86c2987d18" />

3. Merged pull request
 <img width="1354" height="876" alt="3  Merged pull request" src="https://github.com/user-attachments/assets/6468d7c2-0024-4f4f-8ad7-f53cafebdba5" />

4. Fabric Git Integration configuration
<img width="1014" height="924" alt="4  Git Integration" src="https://github.com/user-attachments/assets/8d21f6bd-b790-4d25-8b60-3378ab92c2ba" />

5. Fabric Development → Test → Production deployment pipeline
<img width="1566" height="583" alt="5  Deployment Pipeline" src="https://github.com/user-attachments/assets/e7045e74-56a5-4b28-8da8-f6038a40a951" />

6. Successful DEV → TEST deployment status
<img width="1912" height="826" alt="6" src="https://github.com/user-attachments/assets/ddc39907-9aac-41b5-816f-6fa1e3dd4640" />

