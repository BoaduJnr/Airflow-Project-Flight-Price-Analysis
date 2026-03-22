SELECT 'CREATE DATABASE airflow_metadata'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_metadata')\gexec

SELECT 'CREATE DATABASE flight_analytics_bd'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'flight_analytics_bd')\gexec

SELECT 'CREATE DATABASE e_commerce_events_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'e_commerce_events_db')\gexec