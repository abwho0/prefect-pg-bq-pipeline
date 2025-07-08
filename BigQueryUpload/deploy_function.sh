#!/bin/bash

# This script deploys the automated-pipeline-function to Google Cloud Functions.

gcloud functions deploy automated-pipeline-function \
    --runtime python312 \
    --entry-point main \
    --trigger-http \
    --timeout 540s \
    --memory 1GB \
    --region us-central1 \
    --allow-unauthenticated \
    --set-env-vars POSTGRES_HOST="34.93.7.102",POSTGRES_DB="ajar",POSTGRES_USER="tech",POSTGRES_PORT="5432",GCS_BUCKET_NAME="ajar-bigquery-staging-bucket",BIGQUERY_PROJECT_ID="ajar-kw" \
    --set-secrets \
        POSTGRES_PASSWORD=postgres-db-password:latest