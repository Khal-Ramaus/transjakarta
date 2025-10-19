import pandas as pd
import re
import os 
from config.postgres_conn import get_db_engine
from sqlalchemy import text

# A. Configure file path
BASE_PATH = "/opt/airflow/"
SOURCE_PATH = BASE_PATH + "data_source/"
OUTPUT_PATH = BASE_PATH + "output/" 

# List of columns from csv
HALTE_COLUMNS = ['uuid', 'waktu_transaksi', 'shelter_name_var', 'terminal_name_var', 'card_number_var', 'card_type_var', 'balance_before_int', 'fare_int', 'balance_after_int', 'transcode_txt', 'gate_in_boo', 'p_latitude_flo', 'p_longitude_flo', 'status_var', 'free_service_boo', 'insert_on_dtm']
BUS_COLUMNS = ['uuid', 'waktu_transaksi', 'armada_id_var', 'no_body_var', 'card_number_var', 'card_type_var', 'balance_before_int', 'fare_int', 'balance_after_int', 'transcode_txt', 'gate_in_boo', 'p_latitude_flo', 'p_longitude_flo', 'status_var', 'free_service_boo', 'insert_on_dtm']

# Added new columns after transform
## dummy_transaksi_halte
HALTE_COLUMNS_FINAL = HALTE_COLUMNS + ['is_pelanggan'] 
## dummy_transaksi_bus
BUS_COLUMNS_FINAL = BUS_COLUMNS + ['is_pelanggan', 'no_body_var_std'] 

CSV_FILES = {
    'routes': 'dummy_routes.csv',
    'shelter_corridor': 'dummy_shelter_corridor.csv',
    'realisasi_bus': 'dummy_realisasi_bus.csv',
    'transaksi_halte': 'dummy_transaksi_halte.csv',
    'transaksi_bus': 'dummy_transaksi_bus.csv'
}

# tables name
HALTE_TABLE_NAME = 'dummy_transaksi_halte'
BUS_TABLE_NAME = 'dummy_transaksi_bus'

# new table for reports. 
REPORT_TABLES = {
    'card_type': 'report_card_type', # Jumlah Pelanggan dan amount berdasarkan tipe kartu (grouped by tanggal, card_type, gate_in_boo)
    'route': 'report_route', # Jumlah Pelanggan dan amount berdasarkan rute (grouped by tanggal, route code, route name, gate_in_boo)
    'fare': 'report_fare' # Jumlah Pelanggan dan amount berdasarkan tarif (grouped by tanggal, tarif, gate_in_boo)
}

# Database Setup (Create Tables)
def create_database_tables():
    """Generate reports table in PostgreSQL."""
    engine = get_db_engine()
    print(f"Ensure reports table created....")
    
    # Table Schemas
    ddl_statements = [
        # dummy_transaksi_halte
        text(f"""
        DROP TABLE IF EXISTS {HALTE_TABLE_NAME};
        CREATE TABLE {HALTE_TABLE_NAME} (
            uuid VARCHAR(100),
            waktu_transaksi TIMESTAMP,
            shelter_name_var VARCHAR(100),
            terminal_name_var VARCHAR(100),
            card_number_var VARCHAR(50),
            card_type_var VARCHAR(50),
            balance_before_int INTEGER,
            fare_int INTEGER,
            balance_after_int INTEGER,
            transcode_txt VARCHAR(50),
            gate_in_boo BOOLEAN,
            p_latitude_flo FLOAT,
            p_longitude_flo FLOAT,
            status_var VARCHAR(10),
            free_service_boo BOOLEAN,
            insert_on_dtm TIMESTAMP,
            is_pelanggan BOOLEAN
        );
        """),
        # dummy_transaksi_bus
        text(f"""
        DROP TABLE IF EXISTS {BUS_TABLE_NAME};
        CREATE TABLE {BUS_TABLE_NAME} (
            uuid VARCHAR(100),
            waktu_transaksi TIMESTAMP,
            armada_id_var VARCHAR(50),
            no_body_var VARCHAR(50),
            card_number_var VARCHAR(50),
            card_type_var VARCHAR(50),
            balance_before_int INTEGER,
            fare_int INTEGER,
            balance_after_int INTEGER,
            transcode_txt VARCHAR(50),
            gate_in_boo BOOLEAN,
            p_latitude_flo FLOAT,
            p_longitude_flo FLOAT,
            status_var VARCHAR(10),
            free_service_boo BOOLEAN,
            insert_on_dtm TIMESTAMP,
            is_pelanggan BOOLEAN,
            no_body_var_std VARCHAR(10)
        );
        """),
        # REPORT 1: Card Type
        text(f"""
        DROP TABLE IF EXISTS {REPORT_TABLES['card_type']};
        CREATE TABLE {REPORT_TABLES['card_type']} (
            tanggal DATE,
            card_type_var VARCHAR(50),
            gate_in_boo BOOLEAN,
            jumlah_pelanggan INTEGER,
            total_amount INTEGER
        );
        """),
        # REPORT 2: Route
        text(f"""
        DROP TABLE IF EXISTS {REPORT_TABLES['route']};
        CREATE TABLE {REPORT_TABLES['route']} (
            tanggal DATE,
            route_code VARCHAR(10),
            route_name VARCHAR(100),
            gate_in_boo BOOLEAN,
            jumlah_pelanggan INTEGER,
            total_amount INTEGER
        );
        """),
        # REPORT 3: Fare
        text(f"""
        DROP TABLE IF EXISTS {REPORT_TABLES['fare']};
        CREATE TABLE {REPORT_TABLES['fare']} (
            tanggal DATE,
            tarif INTEGER, 
            gate_in_boo BOOLEAN,
            jumlah_pelanggan INTEGER,
            total_amount INTEGER
        );
        """),
    ]
    

    with engine.begin() as connection: 
        for ddl in ddl_statements:
            connection.execute(ddl)
    
    print("Final transaction tables and report tables created....")


# Extract (Load CSV to Pandas DataFrames)

def extract_data():
    """Extract all 5 Dataframes.."""
    data = {}
    print("Extract dataframes from CSV...")
    for key, filename in CSV_FILES.items():
        try:
            df = pd.read_csv(SOURCE_PATH + filename, low_memory=False)
            data[key] = df
            print(f"Succeeded extract {filename} ({len(df)} rows).")
        except Exception as e:
            print(f"ERROR: Failed to extract {filename}. Error: {e}")
            raise 
            
    return data


# Data Transformed

def standardize_bus_body(no_body):
    """Transformed no_body_var into XXX-###."""
    if pd.isna(no_body):
        return None
    no_body = str(no_body).upper().strip()
    cleaned_str = re.sub(r'[^A-Z0-9\s]', '', no_body)
    match = re.search(r'([A-Z]+)\s*(\d+)', cleaned_str)
    
    if match:
        route_code = match.group(1)
        number_part = match.group(2)
        formatted_number = number_part.zfill(3)
        return f"{route_code}-{formatted_number}"
    else:
        return no_body 

def transform_data(data: dict):
    """Transform all Dataframes"""
    print("Started to transform Dataframes...")
    
    for key in ['transaksi_bus', 'transaksi_halte']:
        df = data[key]
        rows_before = len(df)
        
        # 1. Duplicate check
        df.drop_duplicates(subset=['uuid'], keep='first', inplace=True)
        rows_after = len(df)
        print(f"INFO: {key} - Remove {rows_before - rows_after} Duplicate.")
        
        # 2. Defined Pelanggan (is_pelanggan)
        df['is_pelanggan'] = df['status_var'].astype(str).str.upper() == 'S'
        
        # 3. Convert Datatype
        df['waktu_transaksi'] = pd.to_datetime(df['waktu_transaksi'], errors='coerce')
        df['insert_on_dtm'] = pd.to_datetime(df['insert_on_dtm'], errors='coerce')
        
        data[key] = df

    # 4. Transformed no_body_var (no_body_var_std)
    df_bus = data['transaksi_bus']
    df_bus['no_body_var_std'] = df_bus['no_body_var'].apply(standardize_bus_body)
    data['transaksi_bus'] = df_bus
    
    # 5. Convert Data Realisasi Bus
    df_realisasi = data['realisasi_bus']
    df_realisasi['tanggal_realisasi'] = pd.to_datetime(df_realisasi['tanggal_realisasi']).dt.date
    data['realisasi_bus'] = df_realisasi
    
    print("INFO: Transformasi data selesai.")
    return data


# Generate Reports


def generate_reports_pandas(data: dict) -> dict:
    """Generates 3 reports to DataFrames."""
    print("Generates 3 reports to DataFrames...")
    
    df_routes = data['routes']
    df_realisasi = data['realisasi_bus']
    df_shelter = data['shelter_corridor']
    
    # Filter Pelanggan
    df_bus_cust = data['transaksi_bus'].query("is_pelanggan == True").copy()
    df_halte_cust = data['transaksi_halte'].query("is_pelanggan == True").copy()
    
    df_bus_cust['tanggal'] = df_bus_cust['waktu_transaksi'].dt.date
    df_halte_cust['tanggal'] = df_halte_cust['waktu_transaksi'].dt.date
    
    # Combine Data Bus and Halte for Report 1 & 3
    df_bus_cust_common = df_bus_cust[['tanggal', 'card_type_var', 'fare_int', 'gate_in_boo', 'uuid']]
    df_halte_cust_common = df_halte_cust[['tanggal', 'card_type_var', 'fare_int', 'gate_in_boo', 'uuid']]
    df_combined = pd.concat([df_bus_cust_common, df_halte_cust_common], ignore_index=True)
    
    # Report 1: Card Type
    report_1 = df_combined.groupby(['tanggal', 'card_type_var', 'gate_in_boo']).agg(
        jumlah_pelanggan=('uuid', 'count'),
        total_amount=('fare_int', 'sum')
    ).reset_index()
    
    # Report 2: Report Route 

    # Transaksi bus to realisasi bus to routes
    df_bus_route_join = df_bus_cust.merge(
        df_realisasi, 
        left_on=['tanggal', 'no_body_var_std'], 
        right_on=['tanggal_realisasi', 'bus_body_no'], 
        how='left'
    )
    df_bus_final_report = df_bus_route_join.merge(
        df_routes[['route_code', 'route_name']],
        left_on='rute_realisasi',
        right_on='route_code',
        how='left'
    )[['tanggal', 'route_code', 'route_name', 'gate_in_boo', 'fare_int', 'uuid']]

    # Transaction Halte to Routes
    df_halte_route_join = df_halte_cust.merge(
        df_shelter,
        on='shelter_name_var',
        how='left'
    )

    # Replace column name
    df_halte_route_join.rename(columns={'corridor_code': 'route_code_shelter'}, inplace=True) 
    
    # route_code_shelter to string
    df_halte_route_join['route_code_shelter'] = df_halte_route_join['route_code_shelter'].astype(str)
    # ensure route_code data type is string
    df_routes['route_code'] = df_routes['route_code'].astype(str) 

    df_halte_final_report = df_halte_route_join.merge(
        df_routes[['route_code', 'route_name']],
        left_on='route_code_shelter',
        right_on='route_code',
        how='left'
    )[['tanggal', 'route_code', 'route_name', 'gate_in_boo', 'fare_int', 'uuid']]

    # Combine dataframes
    df_final_route_report = pd.concat([df_bus_final_report, df_halte_final_report], ignore_index=True)

    # Handling missing values'
    df_final_route_report['route_code'] = df_final_route_report['route_code'].fillna('UNKNOWN')
    df_final_route_report['route_name'] = df_final_route_report['route_name'].fillna('UNKNOWN')

    # GROUP BY FINAL
    report_2 = df_final_route_report.groupby(['tanggal', 'route_code', 'route_name', 'gate_in_boo']).agg(
        jumlah_pelanggan=('uuid', 'count'),
        total_amount=('fare_int', 'sum')
    ).reset_index()

    # Report 3: Tarif
    report_3 = df_combined.groupby(['tanggal', 'fare_int', 'gate_in_boo']).agg(
        jumlah_pelanggan=('uuid', 'count'),
        total_amount=('fare_int', 'sum')
    ).reset_index()
    report_3.rename(columns={'fare_int': 'tarif'}, inplace=True)
    
    print("Dataframes report created.....")
    
    # Returns all dataframes
    return {
        'card_type': report_1,
        'route': report_2, 
        'fare': report_3
    }


# Load Final Tables and Reports (PostgreSQL dan CSV)

def load_data_and_report(data: dict):
    """Load data from csv and transformed report to PostgreSQL and CSV."""
    engine = get_db_engine()
    print("Load data from csv to PostgreSQL...")
    

    os.makedirs(OUTPUT_PATH, exist_ok=True) 
    
    # 1. Load Data Transaksi (mode: 'append')
    
    # Transaksi Halte:
    df_halte_final = data['transaksi_halte'][HALTE_COLUMNS_FINAL]
    df_halte_final.to_sql(HALTE_TABLE_NAME, con=engine, if_exists='append', index=False)
    print(f"Succeeded load {len(df_halte_final)} rows to table {HALTE_TABLE_NAME} (FINAL).")

    # Transaksi Bus:
    df_bus_final = data['transaksi_bus'][BUS_COLUMNS_FINAL]
    df_bus_final.to_sql(BUS_TABLE_NAME, con=engine, if_exists='append', index=False)
    print(f"Succeeded load {len(df_bus_final)} rows to table {BUS_TABLE_NAME} (FINAL).")

    # 2. Generate Reports and Load to PostgreSQL/CSV
    
    report_data = generate_reports_pandas(data) 
    print("Load reports to PostgreSQL (Mode: 'replace')...")
    
    for key, df_report in report_data.items():
        table_name = REPORT_TABLES[key]
        
        # A. LOAD TO POSTGRESQL
        df_report.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Succeeded load {len(df_report)} rows to tabel Report: {table_name}")

        # B. SAVE TO CSV
        csv_filename = OUTPUT_PATH + f"report_{key}.csv"
        df_report.to_csv(csv_filename, index=False)
        print(f"Succeeded save report to: {csv_filename}") 

    print("Load Finished....")



# ETL Function (Dipanggil oleh Airflow DAG)

def run_etl_process():
    """ETL Function. will be called by Airflow DAG"""
    create_database_tables()
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data_and_report(transformed_data)
    
    print("SUCCESS: Proses ETL harian selesai.")
    return "ETL Completed Successfully"