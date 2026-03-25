#!/bin/bash
set -e

airflow db migrate

airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}"
