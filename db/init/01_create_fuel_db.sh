#!/bin/sh
# สร้างฐานข้อมูลปลายทางแยกจากฐานข้อมูล metadata ของ Airflow
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow -c "CREATE DATABASE \"${FUEL_DB:-fueldb}\";"
