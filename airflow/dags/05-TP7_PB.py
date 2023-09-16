import logging
import os
import pandas as pd
import datetime as dt
from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.helpers import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    '05_TP1_PB',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['pg-data','tp7'],
) as dag:

    diretorio = "dags/input/bus-api-small"

    lista_arquivos = os.listdir(diretorio)

    tasks = []

    @task
    def extract(diretorio, arquivo):
        log.info(f"======> Arquivo: {arquivo}")

        df = pd.read_json(f"{diretorio}/{arquivo}", lines=True)

        # df['dia'] = pd.to_datetime(arquivo.split("_")[0], format='%Y-%m-%d')
        df['dia'] = arquivo.split("_")[0]
        df.set_index('dia', inplace=True)

        df['ordem'] = df['ordem'].astype(str)
        df['latitude'] = df['latitude'].str.replace(',', '.').astype(float)
        df['longitude'] = df['longitude'].str.replace(',', '.').astype(float)
        df['datahora'] = pd.to_datetime(df['datahora'], unit='ms')
        df['velocidade'] = df['velocidade'].astype(int)
        df['linha'] = df['linha'].astype(str)
        df['datahoraenvio'] = pd.to_datetime(df['datahoraenvio'], unit='ms')
        df['datahoraservidor'] = pd.to_datetime(df['datahoraservidor'], unit='ms')

        postgres_hook = PostgresHook(postgres_conn_id="pg-data")
        
        df.to_sql(
            "bus",
            postgres_hook.get_sqlalchemy_engine(),
            if_exists="append",
            chunksize=1000
        )

    for arquivo in lista_arquivos:
        tasks.append(extract(diretorio, arquivo))

    chain(*tasks)

