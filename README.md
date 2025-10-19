# Proyek ETL Transjakarta

[![Airflow CI/CD](https://img.shields.io/badge/Airflow-2.x-orange.svg?style=flat-square)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-blue.svg?style=flat-square)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Language-Python%203.8+-green.svg?style=flat-square)](https://www.python.org/)

Proyek ini mengimplementasikan pipeline **Extract, Transform, Load (ETL)** menggunakan **Apache Airflow** untuk memproses data transaksi Transjakarta dari berbagai sumber CSV ke dalam *data warehouse* **PostgreSQL**.


<hr>

## Daftar Isi
- [1. Cara Menjalankan Pipeline](#1-cara-menjalankan-pipeline-🚀)
- [2. Dependensi yang Dibutuhkan](#2-dependensi-yang-dibutuhkan-📦)
- [3. Struktur Folder](#3-struktur-folder-📁)
- [4. Detail Airflow DAG](#4-detail-airflow-dag)

<hr>

## 1. Cara Menjalankan Pipeline 🚀

*Pipeline* ini dirancang untuk dijalankan menggunakan **Docker Compose**, yang mengatur lingkungan Airflow, PostgreSQL, dan inisialisasi volume data.

### A. Persiapan Lingkungan

1.  **Clone Repository:**
    ```bash
    git clone [https://proptrader.oanda.com/es/](https://proptrader.oanda.com/es/)
    cd [NAMA FOLDER REPO]
    ```

2.  **Siapkan Data Sumber:**
    Pastikan semua *file* CSV yang dibutuhkan (misalnya, `dummy_transaksi_bus.csv`, `dummy_routes.csv`, dll.) berada di dalam folder **`data_source/`**.

3.  **Bangun dan Jalankan Kontainer Docker:**
    Jalankan Docker Compose untuk membangun dan memulai semua layanan (Airflow *Scheduler*, *Webserver*, dan PostgreSQL).

    ```bash
    docker-compose up --build -d
    ```

### B. Eksekusi ETL

1.  **Akses Airflow UI:**
    Buka *browser* Anda dan navigasikan ke `http://localhost:8080`. (Gunakan *default credentials* `airflow:airflow` jika belum diubah).

2.  **Membuat user:**
    Jalankan command berikut:
    ```bash
    docker exec -it airflow-webserver airflow users create --username admin --password admin --firstname Airflow --lastname Admin --role Admin --email admin@example.com
    ```

3.  **Trigger DAG:**
    * Cari DAG dengan nama **`transjakarta_etl_pipeline`**.
    * Geser *toggle* ke **ON** jika *pipeline* belum aktif.
    * Klik tombol **Trigger DAG** (ikon *play*) untuk memulai proses ETL.

### C. Verifikasi Hasil

* **PostgreSQL:** 
    Jalankan command berikut:
    ```bash
    docker exec -it <postgres_container_name> psql -U airflow -d transport_db
    ```
    Lalu Periksa tabel laporan (`report_route`, `report_card_type`, `report_fare`) di *database* `transport_db`. Silahkan gunakan query berikut: 
    ```bash
    Select * from report_route
    Select * from report_card_type
    Select * from report_fare
    ```
* **Output CSV:** Laporan CSV juga akan disimpan di folder **`output/`**.



<hr>

## 2. Dependensi yang Dibutuhkan 📦

Proyek ini menggunakan container Docker untuk mengisolasi lingkungan, tetapi dependensi utamanya adalah:

### A. Sistem & Alat
* **Docker** dan **Docker Compose**
* **Git**

### B. Python Libraries (Terdaftar di `requirements.txt` / Env Airflow)
* `apache-airflow`
* `pandas`
* `sqlalchemy`
* `psycopg2-binary`

### C. Database
* **PostgreSQL** (Berjalan sebagai kontainer terpisah).


<hr>

## 3. Struktur Folder 📁

Struktur *repository* diorganisir sebagai berikut:

├── dags/
│   └── etl_transport_dag.py     # Definisi alur kerja Airflow (DAG) utama.
├── data_source/
│   ├── dummy_transaksi_bus.csv  # Data sumber mentah.
│   └── ...                      # 4 file CSV data sumber lainnya.
├── output/
│   ├── report_route.csv         # Hasil laporan dalam format CSV (Setelah ETL sukses).
│   └── ...                      # Laporan CSV lainnya.
├── etl_scripts/
│   ├── transform_load.py        # Logika ETL (Transformasi Pandas dan Load ke DB/CSV).
│   └── config/
│       └── postgres_conn.py     # Logika koneksi database PostgreSQL.
├── docker-compose.yml           # Konfigurasi untuk menjalankan semua layanan via Docker.
├── Dockerfile                   # Konfigurasi environment Airflow (instalasi Python libs).
└── README.md                    # Dokumentasi ini.



<hr>

## 4. Detail Airflow DAG

Pipeline ETL utama didefinisikan dalam `etl_transport_dag.py`.

| Properti | Nilai | Deskripsi |
| :--- | :--- | :--- |
| **`dag_id`** | `transjakarta_etl_pipeline` | Nama unik DAG. |
| **`start_date`** | `2025-10-19` | Tanggal mulai eksekusi (dengan `catchup=False`). |
| **`schedule`** | `0 7 * * *` (Cron) | Jadwal harian pada pukul **07:00 pagi** waktu Asia/Jakarta. |
| **`task_id`** | `run_full_etl_process` | Tugas tunggal yang menjalankan seluruh proses ETL. |
| **`python_callable`**| `run_etl_process` | Fungsi Python yang berisi logika **E-T-L** (Extract data CSV, Transformasi Pandas, Load ke PostgreSQL & CSV). |