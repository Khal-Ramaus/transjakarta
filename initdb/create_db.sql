-- Membuat database untuk data ETL
CREATE DATABASE transport_db;

-- Membuat database untuk metadata Airflow (jika belum ada)
-- CREATE DATABASE airflow;

-- Memberikan hak akses penuh ke user 'airflow' pada kedua database
GRANT ALL PRIVILEGES ON DATABASE transport_db TO airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;