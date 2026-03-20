#!/usr/bin/env bash
set -euo pipefail

# Deploy the CSV Pipeline app to Azure Container Apps.
#
# Usage:
#   cd <project-root>
#   ./pipeline/deploy.sh              # uses defaults from azure.env
#   ./pipeline/deploy.sh --build-only # just rebuild the image, then update the container app
#
# Environment variables for the Matcha LLM API should be set via
# `az containerapp update --set-env-vars` after initial deploy, or
# passed inline below.

# Must run from the project root so the Docker build context has access
# to matcha_client.py, parsers, prompts, etc.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Load defaults from azure.env
ENV_FILE="${SCRIPT_DIR}/azure.env"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

RG="${RESOURCE_GROUP:?Set RESOURCE_GROUP in pipeline/azure.env}"
APP_NAME="${APP_NAME:?Set APP_NAME in pipeline/azure.env}"
ACR_NAME="${ACR_NAME:?Set ACR_NAME in pipeline/azure.env}"
IMAGE_TAG="${IMAGE:-${ACR_NAME}.azurecr.io/${APP_NAME}:latest}"
TARGET_PORT=80

echo "==> Build context: ${PROJECT_ROOT}"
echo "==> Dockerfile:    pipeline/Dockerfile"
echo "==> Image:         ${IMAGE_TAG}"

BUILD_ONLY=false
if [[ "${1:-}" == "--build-only" ]]; then
  BUILD_ONLY=true
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  LOCATION="${LOCATION:-eastus}"
  ENV_NAME="${ENV_NAME:-csv-pipeline-env}"

  echo "==> Creating resource group..."
  az group create --name "${RG}" --location "${LOCATION}" --output none

  echo "==> Creating ACR..."
  az acr create \
    --resource-group "${RG}" \
    --name "${ACR_NAME}" \
    --sku Basic \
    --admin-enabled true \
    --output none || true
fi

echo "==> Building & pushing image..."
az acr build \
  --registry "${ACR_NAME}" \
  --image "${APP_NAME}:latest" \
  --file pipeline/Dockerfile \
  .

if [[ "${BUILD_ONLY}" == true ]]; then
  echo "==> Updating container app with new image..."
  az containerapp update \
    --name "${APP_NAME}" \
    --resource-group "${RG}" \
    --image "${IMAGE_TAG}" \
    -o none
else
  LOCATION="${LOCATION:-eastus}"
  ENV_NAME="${ENV_NAME:-csv-pipeline-env}"

  echo "==> Creating Container Apps environment..."
  az containerapp env create \
    --name "${ENV_NAME}" \
    --resource-group "${RG}" \
    --location "${LOCATION}" \
    --output none || true

  echo "==> Deploying Container App..."
  az containerapp up \
    --name "${APP_NAME}" \
    --resource-group "${RG}" \
    --environment "${ENV_NAME}" \
    --image "${IMAGE_TAG}" \
    --target-port "${TARGET_PORT}" \
    --ingress external
fi

echo ""
echo "Deployment complete."
echo ""
echo "Get URL:"
echo "  az containerapp show --name \"${APP_NAME}\" --resource-group \"${RG}\" --query properties.configuration.ingress.fqdn -o tsv"
