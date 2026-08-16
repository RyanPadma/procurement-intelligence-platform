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

Recommended screenshots:

1. GitHub `/fabric` repository
2. `dev` and `main` branches
3. Merged pull request
4. Fabric Git Integration configuration
5. Fabric Development → Test → Production deployment pipeline
6. Successful DEV → TEST deployment status
