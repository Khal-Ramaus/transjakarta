import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Get Database credential
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')
DB_NAME = 'transport_db' 
DB_PORT = '5432'

# URL SQLAlchemy Conn
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_engine() -> Engine:
    """Get SQLAlchemy Engine conn."""
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        # Connection try
        with engine.connect() as connection:
            print(f"Succeeded connect to database: {DB_NAME}!")
        return engine
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        raise