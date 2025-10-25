from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv  # Biblioteca para ler .env

# -------------------- LOAD ENV VARIABLES --------------------
load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'airflow')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')

# -------------------- FILE PATHS --------------------
AIRFLOW_RAW_DATA_PATH = '/opt/airflow/data/raw_data.csv'
AIRFLOW_CLEAN_DATA_PATH = '/opt/airflow/data/clean_data.csv'
AIRFLOW_TRANSFORMED_DATA_PATH = '/opt/airflow/data/transformed_data.csv'

# -------------------- DEFAULT ARGS --------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# -------------------- FUNCTIONS --------------------
def extract_data():
    url = 'https://gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2025/precos-gasolina-etanol-07.csv'
    response = requests.get(url)
    response.raise_for_status()
    
    os.makedirs('/opt/airflow/data/', exist_ok=True)
    with open(AIRFLOW_RAW_DATA_PATH, 'wb') as f:
        f.write(response.content)
    
    print("✅ Extraction completed")
    return 'extract_complete'

def clean_data():
    print("🔄 Starting cleaning process...")
    df = pd.read_csv(AIRFLOW_RAW_DATA_PATH, sep=';', decimal=',')
    
    drop_columns = [
        'Complemento', 'Bairro', 'Numero Rua', 'Regiao - Sigla',
        'Estado - Sigla', 'Municipio', 'Nome da Rua', 'Valor de Compra'
    ]
    df = df.drop(columns=drop_columns, errors='ignore')
    
    df.to_csv(AIRFLOW_CLEAN_DATA_PATH, index=False)
    print("✅ Cleaning completed")
    return 'clean_complete'

def transform_data():
    print("🔄 Starting transformation process...")
    df = pd.read_csv(AIRFLOW_CLEAN_DATA_PATH)
    category_cols = ['Revenda', 'CNPJ da Revenda', 'Cep', 'Produto', 'Unidade de Medida', 'Bandeira']
    
    for col in category_cols:
        df[col] = df[col].astype('category')
    
    df['Data da Coleta'] = pd.to_datetime(df['Data da Coleta'], format='%d/%m/%Y')
    df['Year'] = df['Data da Coleta'].dt.year
    df['Month'] = df['Data da Coleta'].dt.month
    
    df.to_csv(AIRFLOW_TRANSFORMED_DATA_PATH, index=False)
    print("✅ Transformation completed")
    return 'transform_complete'

def load_to_postgres():
    print("🔄 Starting loading process...")
    df = pd.read_csv(AIRFLOW_TRANSFORMED_DATA_PATH)

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fuels (
        retailer TEXT,
        retailer_cnpj TEXT,
        zip_code TEXT,
        product TEXT,
        collection_date DATE,
        sale_price NUMERIC,
        unit TEXT,
        brand TEXT,
        year INT,
        month INT
    );
    """)
    conn.commit()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO fuels (
                retailer, retailer_cnpj, zip_code, product, collection_date,
                sale_price, unit, brand, year, month
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['Revenda'], row['CNPJ da Revenda'], row['Cep'], row['Produto'],
            row['Data da Coleta'], row['Valor de Venda'], row['Unidade de Medida'],
            row['Bandeira'], row['Year'], row['Month']
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Loading completed")
    return 'load_complete'

def validate_db():
    """Validate data loaded in database"""
    print("🔄 Starting validation process...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM fuels;")
    row_count = cursor.fetchone()[0]
    if row_count == 0:
        raise ValueError("⚠️ Validation failed: no rows in fuels table!")

    cursor.execute("SELECT COUNT(*) FROM fuels WHERE retailer IS NULL OR product IS NULL OR sale_price IS NULL;")
    null_count = cursor.fetchone()[0]
    
    if null_count > 0:
        raise ValueError(f"⚠️ Validation failed: {null_count} rows with critical null values!")

    cursor.close()
    conn.close()
    print("✅ Database validation passed")
    return 'validate_complete'

# -------------------- DAG --------------------
with DAG(
    'etl_fuels',
    default_args=default_args,
    description='ETL pipeline for fuel sales data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_extract = PythonOperator(task_id='extract_data', python_callable=extract_data)
    task_clean = PythonOperator(task_id='clean_data', python_callable=clean_data)
    task_transform = PythonOperator(task_id='transform_data', python_callable=transform_data)
    task_load = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)
    task_validate = PythonOperator(task_id='validate_db', python_callable=validate_db)

    task_extract >> task_clean >> task_transform >> task_load >> task_validate
