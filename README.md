# ETL Pipeline Transjakarta

[![Airflow CI/CD](https://img.shields.io/badge/Airflow-2.x-orange.svg?style=flat-square)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-blue.svg?style=flat-square)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Language-Python%203.8+-green.svg?style=flat-square)](https://www.python.org/)

Proyek ini mengimplementasikan pipeline **Extract, Transform, Load (ETL)** menggunakan **Apache Airflow** untuk memproses data transaksi Transjakarta dari berbagai sumber CSV ke dalam *data warehouse* **PostgreSQL**.



## 1. Cara Menjalankan Pipeline

*Pipeline* ini dirancang untuk dijalankan menggunakan **Docker Compose**, yang mengatur lingkungan Airflow, PostgreSQL, dan inisialisasi volume data.

### A. Persiapan Lingkungan

1.  **Clone Repository:**
    ```bash
    git clone https://github.com/Khal-Ramaus/transjakarta.git
    cd transjakarta
    ```

2.  **Siapkan Data Source:**
    Pastikan semua *file* CSV yang dibutuhkan (misalnya, `dummy_transaksi_bus.csv`, `dummy_routes.csv`, dll.) berada di dalam folder **`data_source/`**.

3.  **Build Docker container:**
    Jalankan Docker Compose untuk membangun dan memulai semua layanan (Airflow *Scheduler*, *Webserver*, dan PostgreSQL).

    ```bash
    docker-compose up --build -d
    ```

### B. Eksekusi ETL

1.  **Membuat user:**
    Jalankan command berikut:
    ```bash
    docker exec -it airflow-webserver airflow users create --username admin --password admin --firstname Airflow --lastname Admin --role Admin --email admin@example.com
    ```

2.  **Akses Airflow UI:**
    Buka *browser* Anda dan navigasikan ke `http://localhost:8080`. (Gunakan *credential* user: admin dan password: admin.

3.  **Trigger DAG:**
    * Cari DAG dengan nama **`transjakarta_etl_pipeline`**.
    * Geser *toggle* ke **ON** jika *pipeline* belum aktif.
    * Klik tombol **Trigger DAG** (ikon *play*) untuk memulai proses ETL.

### C. Verifikasi Hasil

* **PostgreSQL:** 
    Jalankan command berikut:
    ```bash
    docker exec -it postgres psql -U airflow -d transport_db
    ```
    Lalu Periksa tabel laporan (`report_route`, `report_card_type`, `report_fare`) di *database* `transport_db`. Silahkan gunakan query berikut: 
    ```bash
    Select * from report_route;
    Select * from report_card_type;
    Select * from report_fare;
    ```
* **Output CSV:** Laporan CSV juga akan disimpan di folder **`output/`**.



<hr>

## 2. Dependensi yang Dibutuhkan

Proyek ini menggunakan container Docker untuk mengisolasi lingkungan, tetapi dependensi utamanya adalah:

### A. System and tools
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


## 3. Detail Airflow DAG

Pipeline ETL utama didefinisikan dalam `etl_transport_dag.py`.

| Properti | Nilai | Deskripsi |
| :--- | :--- | :--- |
| **`dag_id`** | `transjakarta_etl_pipeline` | Nama unik DAG. |
| **`start_date`** | `2025-10-19` | Tanggal mulai eksekusi (dengan `catchup=False`). |
| **`schedule`** | `0 7 * * *` (Cron) | Jadwal harian pada pukul **00:00** waktu Asia/Jakarta. |
| **`task_id`** | `run_full_etl_process` | Tugas tunggal yang menjalankan seluruh proses ETL. |
| **`python_callable`**| `run_etl_process` | Fungsi Python yang berisi logika **E-T-L** (Extract data CSV, Transformasi Pandas, Load ke PostgreSQL & CSV). |

## 4. Struktur folder
```
.
├── dags/
│   └── etl_transport_dag.py       # Script DAG yang berguna untuk menjalankan proses ETL sesuai jadwal.
├── data_source/                   # Berisi data source berupa file csv
│   ├── dummy_transaksi_bus.csv    # Data input transaksi Bus.
│   ├── dummy_transaksi_halte.csv  # Data input transaksi Halte.
│   ├── dummy_realisasi_bus.csv    # Data input Realisasi Bus.    
│   ├── dummy_routes.csv           # Data input code dan nama Rute bus.
│   └── dummy_shelter_corridor.csv # Data input Koridor
├── output/                        # Output hasil ETL. Akan keluar setelah menjalankan DAG Airflow
│   ├── report_route.csv           # Hasil laporan Rute gabungan (Bus & Halte) dalam CSV.
│   ├── report_fare.csv            # Hasil laporan tarif gabungan (Bus & Halte) dalam CSV.
│   └── report_card_type.csv       # Hasil laporan Card Type gabungan (Bus & Halte) dalam CSV.
├── etl_scripts/
│   ├── transform_load.py        # Script ETL: Transformasi Pandas, Load ke PostgreSQL & CSV.
│   └── config/
│       └── postgres_conn.py     # Konfigurasi koneksi database.
├── docker-compose.yml           # Pengaturan dan orkestrasi Docker untuk Airflow dan PostgreSQL.
└── README.md                    # Dokumentasi ini.
```