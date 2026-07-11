#!/bin/bash
set -e

mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
chown -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins}
chmod -R g+rw /opt/airflow/{logs,dags,plugins}
