# Databricks Asset Bundles (DAB) — CI/CD Design

> **Status:** Documented and designed, not yet executed end-to-end. CLI installation
> on the local Windows environment hit persistent PATH/tooling issues; the design
> below reflects the correct, standard DAB pattern for this project and is ready
> to run once CLI access is resolved (or from a Linux/WSL/CI runner environment).

## Why DAB, and why it fits this project specifically

Databricks Asset Bundles is Databricks' own infrastructure-as-code tool for
defining and deploying jobs, notebooks, and workflows as version-controlled
YAML configuration. Unlike the ADF-based CI/CD approach (which required an
Azure Service Principal — blocked by tenant policy on this account), DAB
authenticates via the Databricks CLI's own auth (personal access token or
OAuth), sidestepping that specific blocker entirely while still achieving
genuine environment promotion (Dev → Prod).

## How it would work for this project

A single `databricks.yml` file at the repo root defines:
- The three notebook tasks (Bronze ingestion, Silver transform, Gold transform)
  as a single multi-task Job, mirroring the Databricks Workflow already built
  and running in Dev
- Two **targets**: `dev` and `prod`, each pointing at a different Databricks
  workspace URL and a different Unity Catalog catalog/schema set
- Deployment is then a single CLI command per environment:
  `databricks bundle deploy --target dev` or `--target prod`

## Example `databricks.yml` (design reference)

```yaml
bundle:
  name: haleon-adb-pharma-pipeline

resources:
  jobs:
    pharma_sales_pipeline:
      name: pharma-sales-pipeline-${bundle.target}
      tasks:
        - task_key: bronze_ingestion
          notebook_task:
            notebook_path: ../notebooks/01_bronze_ingestion.py
        - task_key: silver_transform
          depends_on:
            - task_key: bronze_ingestion
          notebook_task:
            notebook_path: ../notebooks/02_silver_transform.py
        - task_key: gold_transform
          depends_on:
            - task_key: silver_transform
          notebook_task:
            notebook_path: ../notebooks/03_gold_transform.py
      schedule:
        quartz_cron_expression: "0 0 9 * * ?"
        timezone_id: "Asia/Kolkata"

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-7405616681310823.3.azuredatabricks.net

  prod:
    mode: production
    workspace:
      host: <prod-workspace-url>
```

## The deployment flow, once CLI access is available

1. `databricks auth login --host <dev-workspace-url>` — authenticate once
2. `databricks bundle validate --target dev` — checks the YAML is well-formed
   before deploying anything
3. `databricks bundle deploy --target dev` — deploys/updates the job definition
   in the Dev workspace
4. `databricks bundle run pharma_sales_pipeline --target dev` — triggers a run
   to confirm it works
5. Repeat with `--target prod` once Dev is verified, pointing at the Prod
   workspace and its own Unity Catalog schemas

## How this would plug into GitHub Actions (the CD half)

```yaml
name: CD - Deploy Bundle to Prod

on:
  workflow_dispatch:   # manual trigger only, by design — no auto-deploy to Prod

jobs:
  deploy-prod:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Deploy bundle to Prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
        run: databricks bundle deploy --target prod
```

This workflow is intentionally set to `workflow_dispatch` (manual trigger)
rather than automatic on push — a deliberate, standard practice for
Prod deployments, requiring a human to explicitly initiate it rather than
deploying automatically on every merge.

## Honest summary

The CI half of this project's CI/CD (GitHub Actions validating notebook syntax
and lint on every push) is fully built and passing. The CD half — using DAB to
promote from Dev to Prod — is fully designed and documented above, matching
Databricks' standard recommended pattern, but has not been executed end-to-end
due to local CLI installation issues on Windows. The logic, structure, and
commands are correct and ready to run in an environment where the CLI installs
cleanly (e.g. WSL, a Linux CI runner, or once local PATH issues are resolved).
