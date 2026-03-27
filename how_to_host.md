# How to Host — Azure Deployment Guide

This document describes how to deploy the full CS Analytics solution to Azure, covering the PostgreSQL database, Dash web dashboard, Flask webhook receiver, CSV pipeline app, and the background ingestion/enrichment jobs.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Azure Services Used](#azure-services-used)
3. [Prerequisites](#prerequisites)
4. [Step 1 — Provision Azure Database for PostgreSQL](#step-1--provision-azure-database-for-postgresql)
5. [Step 2 — Apply Database Migrations](#step-2--apply-database-migrations)
6. [Step 3 — Set Up Azure Container Registry](#step-3--set-up-azure-container-registry)
7. [Step 4 — Deploy the CSV Pipeline App](#step-4--deploy-the-csv-pipeline-app)
8. [Step 5 — Deploy the Dash Web Dashboard](#step-5--deploy-the-dash-web-dashboard)
9. [Step 6 — Deploy the Webhook Receiver](#step-6--deploy-the-webhook-receiver)
10. [Step 7 — Schedule Ingestion & Enrichment Jobs](#step-7--schedule-ingestion--enrichment-jobs)
11. [Step 8 — Configure Azure Blob Storage (Optional)](#step-8--configure-azure-blob-storage-optional)
12. [Environment Variables Reference](#environment-variables-reference)
13. [Networking & Security](#networking--security)
14. [Monitoring & Logs](#monitoring--logs)
15. [Cost Optimization](#cost-optimization)

---

## Architecture Overview

The solution consists of four deployable components:

| Component | Framework | Default Port | Purpose |
|---|---|---|---|
| **Web Dashboard** | Plotly Dash | 8050 | Interactive analytics UI (tickets, health, root cause, operations) |
| **Webhook Receiver** | Flask | 5002 | Receives TeamSupport webhook events, triggers real-time ingest |
| **CSV Pipeline** | Flask | 5001 / 80 | Upload CSVs for LLM-driven pass analysis (Pass 1→5) |
| **Background Jobs** | Python scripts | N/A | `run_ingest.py sync`, `run_enrich_db.py`, `db.py migrate` |

All components connect to a shared **PostgreSQL** database (schema `tickets_ai`) and call the **Matcha LLM API** and **TeamSupport API** as external services.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Azure Cloud                              │
│                                                                 │
│  ┌──────────────────┐   ┌──────────────────┐                    │
│  │  Container App   │   │  Container App   │                    │
│  │  Dash Web (8050) │   │  Webhook (5002)  │                    │
│  └────────┬─────────┘   └────────┬─────────┘                    │
│           │                      │                               │
│           ▼                      ▼                               │
│  ┌──────────────────────────────────────────┐                    │
│  │   Azure Database for PostgreSQL          │                    │
│  │   Schema: tickets_ai                     │                    │
│  └──────────────────────────────────────────┘                    │
│           ▲                      ▲                               │
│           │                      │                               │
│  ┌────────┴─────────┐   ┌───────┴──────────┐                    │
│  │  Container App   │   │  Container Job   │                    │
│  │  CSV Pipeline    │   │  Ingest/Enrich   │                    │
│  │  (port 80)       │   │  (scheduled)     │                    │
│  └──────────────────┘   └──────────────────┘                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                         │
         ▼                         ▼
  ┌──────────────┐         ┌──────────────┐
  │ Matcha LLM   │         │ TeamSupport  │
  │ API          │         │ API          │
  └──────────────┘         └──────────────┘
```

---

## Azure Services Used

| Azure Service | Purpose |
|---|---|
| **Azure Database for PostgreSQL — Flexible Server** | Primary database (schema `tickets_ai`) |
| **Azure Container Registry (ACR)** | Store Docker images for all components |
| **Azure Container Apps** | Host the web dashboard, webhook receiver, and CSV pipeline |
| **Azure Container Apps Jobs** | Run scheduled ingestion and enrichment scripts |
| **Azure Blob Storage** | (Optional) Store CSV pipeline result files |
| **Azure Key Vault** | (Recommended) Securely store API keys and database credentials |
| **Azure Log Analytics** | Centralized logging for all Container Apps |

---

## Prerequisites

- **Azure CLI** installed and authenticated (`az login`)
- **Docker** installed locally (for building images, or use ACR Tasks for cloud builds)
- **Python 3.11+** installed locally (for running migrations)
- An Azure subscription with permissions to create resource groups and resources
- TeamSupport API credentials (`TS_KEY`, `TS_USER_ID`)
- Matcha LLM API key (`MATCHA_API_KEY`, `MATCHA_MISSION_ID`)

---

## Step 1 — Provision Azure Database for PostgreSQL

### 1.1 Create the Server

```bash
RESOURCE_GROUP="cs-analytics-rg"
LOCATION="eastus"
PG_SERVER="cs-analytics-pg"
PG_ADMIN_USER="csadmin"
PG_ADMIN_PASS="<generate-a-strong-password>"

# Create resource group
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

# Create PostgreSQL Flexible Server
az postgres flexible-server create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --location "$LOCATION" \
  --admin-user "$PG_ADMIN_USER" \
  --admin-password "$PG_ADMIN_PASS" \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --storage-size 32 \
  --version 16 \
  --public-access 0.0.0.0 \
  --yes
```

> **Note:** `--public-access 0.0.0.0` allows Azure services to connect. For production, restrict to your Container Apps subnet via VNet integration.

### 1.2 Create the Database

```bash
az postgres flexible-server db create \
  --resource-group "$RESOURCE_GROUP" \
  --server-name "$PG_SERVER" \
  --database-name Work
```

### 1.3 Build the Connection String

The `DATABASE_URL` used by the application follows this format:

```
postgresql://<admin-user>:<password>@<server-name>.postgres.database.azure.com:5432/Work?sslmode=require
```

Example:

```
postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require
```

### 1.4 Configure Firewall (for local migration)

To run migrations from your local machine, temporarily add your IP:

```bash
MY_IP=$(curl -s ifconfig.me)
az postgres flexible-server firewall-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --rule-name allow-local \
  --start-ip-address "$MY_IP" \
  --end-ip-address "$MY_IP"
```

---

## Step 2 — Apply Database Migrations

The project uses a custom migration system in `db.py` with SQL files in `migrations/`. Run migrations against the Azure PostgreSQL instance:

```bash
# Set the connection string to point to Azure
export DATABASE_URL="postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require"
export DATABASE_SCHEMA="tickets_ai"

# Run all pending migrations
python db.py migrate
```

This will:
- Create the `tickets_ai` schema if it doesn't exist
- Create the `_migrations` tracking table
- Apply all numbered SQL files from `migrations/` in order
- Skip already-applied migrations

> **Tip:** Remove the temporary local firewall rule after migrations are complete if you plan to use VNet-only access.

---

## Step 3 — Set Up Azure Container Registry

```bash
ACR_NAME="csanalyticsacr"

az acr create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$ACR_NAME" \
  --sku Basic \
  --admin-enabled true
```

Login to the registry:

```bash
az acr login --name "$ACR_NAME"
```

---

## Step 4 — Deploy the CSV Pipeline App

The CSV pipeline already has a Dockerfile and deploy script. The existing `pipeline/deploy.sh` automates this.

### 4.1 Using the Existing Deploy Script

```bash
# Update pipeline/azure.env with your values
cat > pipeline/azure.env << 'EOF'
RESOURCE_GROUP=cs-analytics-rg
ACR_NAME=csanalyticsacr
APP_NAME=csv-pipeline
IMAGE=csanalyticsacr.azurecr.io/csv-pipeline:latest
EOF

# Deploy (creates resource group, ACR, Container App environment, and deploys)
./pipeline/deploy.sh
```

### 4.2 Set Environment Variables

After initial deployment, inject secrets:

```bash
az containerapp update \
  --name csv-pipeline \
  --resource-group "$RESOURCE_GROUP" \
  --set-env-vars \
    MATCHA_URL="https://matcha.harriscomputer.com/rest/api/v1/completions" \
    MATCHA_API_KEY="<your-matcha-api-key>" \
    MATCHA_MISSION_ID="27301" \
    DATABASE_URL="postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require" \
    DATABASE_SCHEMA="tickets_ai" \
    AZURE_STORAGE_CONNECTION_STRING="<your-blob-connection-string>"
```

---

## Step 5 — Deploy the Dash Web Dashboard

The web dashboard does not have a Dockerfile yet. Create one:

### 5.1 Create the Dockerfile

Create a file at `web/Dockerfile`:

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install core + web dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    pandas \
    pyyaml \
    sqlalchemy \
    dash>=2.14 \
    dash-ag-grid>=31.0 \
    dash-iconify>=0.1.2 \
    dash-mantine-components>=0.14 \
    plotly>=5.18 \
    gunicorn

# Copy application code
COPY config.py db.py ts_client.py matcha_client.py prompt_store.py glossary.py ./
COPY matcha.py analytics_queries.py pipeline_stages.py ./
COPY pass1_parser.py pass2_parser.py pass3_parser.py ./
COPY pass4/ ./pass4/
COPY pass5/ ./pass5/
COPY prompts/ ./prompts/
COPY rollups/ ./rollups/
COPY enrichment/ ./enrichment/
COPY ingest/ ./ingest/
COPY migrations/ ./migrations/
COPY web/ ./web/

EXPOSE 8050

CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "--threads", "4", "--timeout", "120", "web.app:server"]
```

### 5.2 Build and Deploy

```bash
# Build and push to ACR
az acr build \
  --registry "$ACR_NAME" \
  --image cs-web-dashboard:latest \
  --file web/Dockerfile \
  .

# Create the Container App
az containerapp up \
  --name cs-web-dashboard \
  --resource-group "$RESOURCE_GROUP" \
  --environment csv-pipeline-env \
  --image "$ACR_NAME.azurecr.io/cs-web-dashboard:latest" \
  --target-port 8050 \
  --ingress external

# Set environment variables
az containerapp update \
  --name cs-web-dashboard \
  --resource-group "$RESOURCE_GROUP" \
  --set-env-vars \
    DATABASE_URL="postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require" \
    DATABASE_SCHEMA="tickets_ai" \
    WEB_PORT="8050" \
    WEB_DEBUG="0" \
    MATCHA_URL="https://matcha.harriscomputer.com/rest/api/v1/completions" \
    MATCHA_API_KEY="<your-matcha-api-key>" \
    MATCHA_MISSION_ID="27301"
```

> **Important:** Set `WEB_DEBUG=0` in production to disable Dash debug mode.

---

## Step 6 — Deploy the Webhook Receiver

### 6.1 Create the Dockerfile

Create a file at `webhook/Dockerfile`:

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    flask \
    gunicorn \
    psycopg2-binary \
    requests \
    pandas \
    pyyaml \
    sqlalchemy

# Copy application code
COPY config.py db.py ts_client.py matcha_client.py prompt_store.py glossary.py ./
COPY matcha.py pipeline_stages.py ./
COPY pass1_parser.py pass2_parser.py pass3_parser.py ./
COPY pass4/ ./pass4/
COPY pass5/ ./pass5/
COPY prompts/ ./prompts/
COPY rollups/ ./rollups/
COPY enrichment/ ./enrichment/
COPY ingest/ ./ingest/
COPY migrations/ ./migrations/
COPY webhook/ ./webhook/

EXPOSE 5002

CMD ["gunicorn", "--bind", "0.0.0.0:5002", "--workers", "2", "--threads", "4", "--timeout", "120", "webhook.app:app"]
```

### 6.2 Build and Deploy

```bash
# Build and push to ACR
az acr build \
  --registry "$ACR_NAME" \
  --image cs-webhook:latest \
  --file webhook/Dockerfile \
  .

# Create the Container App
az containerapp up \
  --name cs-webhook \
  --resource-group "$RESOURCE_GROUP" \
  --environment csv-pipeline-env \
  --image "$ACR_NAME.azurecr.io/cs-webhook:latest" \
  --target-port 5002 \
  --ingress external

# Set environment variables
az containerapp update \
  --name cs-webhook \
  --resource-group "$RESOURCE_GROUP" \
  --set-env-vars \
    DATABASE_URL="postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require" \
    DATABASE_SCHEMA="tickets_ai" \
    WEBHOOK_SECRET="<generate-a-strong-secret>" \
    TS_BASE="https://app.na2.teamsupport.com/api/json" \
    TS_KEY="<your-ts-api-key>" \
    TS_USER_ID="<your-ts-user-id>" \
    MATCHA_URL="https://matcha.harriscomputer.com/rest/api/v1/completions" \
    MATCHA_API_KEY="<your-matcha-api-key>" \
    MATCHA_MISSION_ID="27301"
```

### 6.3 Configure TeamSupport Webhook

After deployment, retrieve the webhook URL:

```bash
WEBHOOK_URL=$(az containerapp show \
  --name cs-webhook \
  --resource-group "$RESOURCE_GROUP" \
  --query "properties.configuration.ingress.fqdn" -o tsv)
echo "Webhook endpoint: https://${WEBHOOK_URL}/webhook/teamsupport"
```

Configure this URL in TeamSupport's webhook settings. Include the `WEBHOOK_SECRET` as a Bearer token in the `Authorization` header, or configure TeamSupport to sign payloads with HMAC-SHA256 using the same secret.

---

## Step 7 — Schedule Ingestion & Enrichment Jobs

The background sync and enrichment pipeline should run on a schedule. Use **Azure Container Apps Jobs** for this.

### 7.1 Create the Job Dockerfile

Create a file at `jobs/Dockerfile`:

```dockerfile
FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    pandas \
    pyyaml \
    sqlalchemy

COPY config.py db.py ts_client.py matcha_client.py prompt_store.py glossary.py ./
COPY matcha.py pipeline_stages.py action_classifier.py activity_cleaner.py ./
COPY pass1_parser.py pass2_parser.py pass3_parser.py ./
COPY pass4/ ./pass4/
COPY pass5/ ./pass5/
COPY prompts/ ./prompts/
COPY rollups/ ./rollups/
COPY enrichment/ ./enrichment/
COPY ingest/ ./ingest/
COPY migrations/ ./migrations/
COPY run_ingest.py run_enrich_db.py run_all.py run_rollups.py ./
COPY run_sentiment.py run_priority.py run_complexity.py ./
COPY run_export.py run_ticket_pass1.py run_ticket_pass2.py run_ticket_pass3.py ./
COPY run_pass4.py run_pass5.py ./
```

### 7.2 Build and Register the Job Image

```bash
az acr build \
  --registry "$ACR_NAME" \
  --image cs-jobs:latest \
  --file jobs/Dockerfile \
  .
```

### 7.3 Create Scheduled Jobs

**Sync job** (runs every 15 minutes — pulls new/changed tickets from TeamSupport):

```bash
az containerapp job create \
  --name cs-sync-job \
  --resource-group "$RESOURCE_GROUP" \
  --environment csv-pipeline-env \
  --image "$ACR_NAME.azurecr.io/cs-jobs:latest" \
  --trigger-type Schedule \
  --cron-expression "*/15 * * * *" \
  --cpu 0.5 \
  --memory 1Gi \
  --replica-timeout 1800 \
  --command "python" "db.py" "migrate" "&&" "python" "run_ingest.py" "sync" \
  --env-vars \
    DATABASE_URL="postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require" \
    DATABASE_SCHEMA="tickets_ai" \
    TS_BASE="https://app.na2.teamsupport.com/api/json" \
    TS_KEY="<your-ts-api-key>" \
    TS_USER_ID="<your-ts-user-id>"
```

**Enrichment job** (runs every 30 minutes — runs LLM passes on new tickets):

```bash
az containerapp job create \
  --name cs-enrich-job \
  --resource-group "$RESOURCE_GROUP" \
  --environment csv-pipeline-env \
  --image "$ACR_NAME.azurecr.io/cs-jobs:latest" \
  --trigger-type Schedule \
  --cron-expression "15,45 * * * *" \
  --cpu 0.5 \
  --memory 1Gi \
  --replica-timeout 3600 \
  --command "python" "run_enrich_db.py" \
  --env-vars \
    DATABASE_URL="postgresql://csadmin:<password>@cs-analytics-pg.postgres.database.azure.com:5432/Work?sslmode=require" \
    DATABASE_SCHEMA="tickets_ai" \
    MATCHA_URL="https://matcha.harriscomputer.com/rest/api/v1/completions" \
    MATCHA_API_KEY="<your-matcha-api-key>" \
    MATCHA_MISSION_ID="27301" \
    FORCE_ENRICHMENT="0"
```

> **Tip:** For the initial backfill run, set `FORCE_ENRICHMENT=1` and `INITIAL_BACKFILL_DAYS=0` to process all historical tickets. Revert to `FORCE_ENRICHMENT=0` for subsequent incremental runs.

---

## Step 8 — Configure Azure Blob Storage (Optional)

The CSV pipeline can store result files in Azure Blob Storage.

```bash
STORAGE_ACCOUNT="csanalyticsstorage"

# Create storage account
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS

# Create the container
az storage container create \
  --name csv-pipeline-results \
  --account-name "$STORAGE_ACCOUNT"

# Get connection string
STORAGE_CONN=$(az storage account show-connection-string \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query connectionString -o tsv)

# Update the CSV pipeline app with the connection string
az containerapp update \
  --name csv-pipeline \
  --resource-group "$RESOURCE_GROUP" \
  --set-env-vars \
    AZURE_STORAGE_CONNECTION_STRING="$STORAGE_CONN"
```

---

## Environment Variables Reference

All environment variables used across the solution:

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | **Yes** | *(none)* | PostgreSQL connection string |
| `DATABASE_SCHEMA` | No | `tickets_ai` | PostgreSQL schema name |
| `TS_BASE` | No | `https://app.na2.teamsupport.com/api/json` | TeamSupport API base URL |
| `TS_KEY` | **Yes** | *(none)* | TeamSupport API key |
| `TS_USER_ID` | **Yes** | *(none)* | TeamSupport user ID |
| `MATCHA_URL` | No | `https://matcha.harriscomputer.com/rest/api/v1/completions` | Matcha LLM endpoint |
| `MATCHA_API_KEY` | **Yes** | *(none)* | Matcha API key |
| `MATCHA_MISSION_ID` | No | `27301` | Matcha mission ID |
| `WEBHOOK_SECRET` | Recommended | `""` | Webhook auth secret (Bearer token or HMAC key) |
| `WEB_PORT` | No | `8050` | Dash web app port |
| `WEB_DEBUG` | No | `1` | Dash debug mode (`0` for production) |
| `MAX_TICKETS` | No | `0` | Max tickets to pull (0 = unlimited) |
| `TARGET_TICKET` | No | `""` | Comma-separated ticket numbers to target |
| `RUN_SENTIMENT` | No | `1` | Enable sentiment analysis |
| `RUN_PRIORITY` | No | `1` | Enable priority scoring |
| `RUN_COMPLEXITY` | No | `1` | Enable complexity scoring |
| `FORCE_ENRICHMENT` | No | `1` | Bypass hash-based skip (use `0` for incremental) |
| `TS_WRITEBACK` | No | `0` | Write results back to TeamSupport |
| `SKIP_OUTPUT_FILES` | No | `1` | Skip writing JSON artifact files |
| `LOG_TO_FILE` | No | `1` | Enable file logging |
| `LOG_API_CALLS` | No | `1` | Log API calls to `api_calls.json` |
| `SAFETY_BUFFER_MINUTES` | No | `10` | Overlap buffer for incremental sync |
| `INITIAL_BACKFILL_DAYS` | No | `0` | Days to backfill (0 = all) |
| `STALE_TICKET_DAYS` | No | `3` | Re-fetch stale ticket threshold |
| `AZURE_STORAGE_CONNECTION_STRING` | No | `""` | Azure Blob Storage connection (CSV pipeline) |
| `OUTPUT_DIR` | No | `./output` | Local output directory |

---

## Networking & Security

### Use Azure Key Vault for Secrets

Instead of setting secrets as plain environment variables, use Key Vault references:

```bash
# Create a Key Vault
az keyvault create \
  --name cs-analytics-kv \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION"

# Store secrets
az keyvault secret set --vault-name cs-analytics-kv --name database-url --value "<connection-string>"
az keyvault secret set --vault-name cs-analytics-kv --name ts-key --value "<api-key>"
az keyvault secret set --vault-name cs-analytics-kv --name matcha-api-key --value "<api-key>"
az keyvault secret set --vault-name cs-analytics-kv --name webhook-secret --value "<secret>"

# Enable managed identity on Container Apps and grant Key Vault access
az containerapp identity assign --name cs-web-dashboard --resource-group "$RESOURCE_GROUP" --system-assigned
az containerapp identity assign --name cs-webhook --resource-group "$RESOURCE_GROUP" --system-assigned
az containerapp identity assign --name csv-pipeline --resource-group "$RESOURCE_GROUP" --system-assigned
```

### VNet Integration

For production, place the PostgreSQL server and Container Apps in the same VNet:

```bash
# Create VNet
az network vnet create \
  --resource-group "$RESOURCE_GROUP" \
  --name cs-analytics-vnet \
  --address-prefix 10.0.0.0/16

# Create subnets
az network vnet subnet create \
  --resource-group "$RESOURCE_GROUP" \
  --vnet-name cs-analytics-vnet \
  --name pg-subnet \
  --address-prefix 10.0.1.0/24 \
  --delegations Microsoft.DBforPostgreSQL/flexibleServers

az network vnet subnet create \
  --resource-group "$RESOURCE_GROUP" \
  --vnet-name cs-analytics-vnet \
  --name apps-subnet \
  --address-prefix 10.0.2.0/23
```

Then create the PostgreSQL server and Container Apps environment within these subnets. This eliminates the need for public database access.

### Restrict Webhook Access

- Always set `WEBHOOK_SECRET` in production
- Consider limiting the webhook Container App's ingress to TeamSupport's IP ranges using Container Apps IP restrictions
- Use HTTPS only (Container Apps provides TLS termination automatically)

### Restrict Dashboard Access

- If the dashboard should be internal-only, set ingress to `internal` instead of `external`
- For authenticated access, place Azure Application Gateway or Azure Front Door with AAD authentication in front of the Container App

---

## Monitoring & Logs

### View Container App Logs

```bash
# Stream logs from the web dashboard
az containerapp logs show \
  --name cs-web-dashboard \
  --resource-group "$RESOURCE_GROUP" \
  --follow

# View job execution history
az containerapp job execution list \
  --name cs-sync-job \
  --resource-group "$RESOURCE_GROUP"
```

### Log Analytics Queries

Container Apps automatically forward `stdout`/`stderr` to the Log Analytics workspace attached to the environment. Example queries:

```kusto
// Errors in the last 24 hours
ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(24h)
| where Log_s contains "ERROR" or Log_s contains "Traceback"
| project TimeGenerated, ContainerAppName_s, Log_s
| order by TimeGenerated desc

// Webhook events received
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "cs-webhook"
| where Log_s contains "ticket_id"
| summarize count() by bin(TimeGenerated, 1h)
```

### Health Check Endpoints

| Component | Health Endpoint |
|---|---|
| Webhook | `GET /webhook/health` → `{"status": "ok"}` |
| CSV Pipeline | `GET /` → upload page (200 OK) |
| Web Dashboard | `GET /` → Dash app (200 OK) |

Configure Container Apps health probes:

```bash
az containerapp update \
  --name cs-webhook \
  --resource-group "$RESOURCE_GROUP" \
  --set-env-vars WEBHOOK_SECRET="<secret>" \
  --container-name cs-webhook \
  --revision-suffix health-probe
```

---

## Cost Optimization

| Resource | Recommendation |
|---|---|
| **PostgreSQL** | Start with `Standard_B1ms` (Burstable). Scale up if query latency increases. |
| **Container Apps** | Use Consumption plan (pay per request). Set min replicas to 0 for the CSV pipeline (scales to zero when idle). Keep min replicas at 1 for webhook and dashboard for responsiveness. |
| **Container App Jobs** | Only billed for execution time. Free when idle. |
| **Blob Storage** | Standard LRS is sufficient for pipeline results. |
| **ACR** | Basic tier is adequate for this workload. |

### Scaling Configuration

```bash
# Dashboard: always-on with 1-3 replicas
az containerapp update \
  --name cs-web-dashboard \
  --resource-group "$RESOURCE_GROUP" \
  --min-replicas 1 \
  --max-replicas 3

# Webhook: always-on with 1-2 replicas
az containerapp update \
  --name cs-webhook \
  --resource-group "$RESOURCE_GROUP" \
  --min-replicas 1 \
  --max-replicas 2

# CSV Pipeline: scale to zero when idle
az containerapp update \
  --name csv-pipeline \
  --resource-group "$RESOURCE_GROUP" \
  --min-replicas 0 \
  --max-replicas 3
```

---

## Quick-Start Checklist

```
[ ] 1. Create Azure resource group
[ ] 2. Provision Azure Database for PostgreSQL Flexible Server
[ ] 3. Create the `Work` database
[ ] 4. Run `python db.py migrate` against the Azure database
[ ] 5. Create Azure Container Registry
[ ] 6. Build and deploy CSV Pipeline via `pipeline/deploy.sh`
[ ] 7. Build and deploy Web Dashboard Container App
[ ] 8. Build and deploy Webhook Receiver Container App
[ ] 9. Create scheduled Container App Jobs for sync/enrich
[ ] 10. Set all environment variables (especially secrets) on each app
[ ] 11. (Optional) Provision Azure Blob Storage for CSV pipeline
[ ] 12. (Optional) Set up Key Vault and VNet integration
[ ] 13. Configure TeamSupport webhook URL to point to Azure endpoint
[ ] 14. Verify health endpoints and run initial backfill
[ ] 15. Set FORCE_ENRICHMENT=0 for incremental operation
```
