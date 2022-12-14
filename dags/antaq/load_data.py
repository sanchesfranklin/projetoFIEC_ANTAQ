import os
from pyspark.sql import SparkSession

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

filePathAtracacao = 'data/raw/Atracacao/atracacao.parquet'
filePathCarga = 'data/raw/Carga/carga.parquet'

spark = SparkSession.builder.appName("etl_antaq").getOrCreate()

# conexão com o banco
mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')

def load_dados_atracacao():

    # recupera o arquivo .parquet
    df = spark.read.parquet(filePathAtracacao)
    tableNameAtracao = 'atracacao_fato'
    # faz o insert no banco
    mssql_hook.insert_rows(
            table= tableNameAtracao,
            rows=df.toLocalIterator(),
            target_fields=df.columns,
            commit_every=1000
        )

def load_dados_carga():

    # recupera o arquivo .parquet
    df = spark.read.parquet(filePathCarga)
    tableNameCarga = 'carga_fato'
    # faz o insert no banco
    mssql_hook.insert_rows(
            table= tableNameCarga,
            rows=df.toLocalIterator(),
            target_fields=df.columns,
            commit_every=1000
        )

    
    
    