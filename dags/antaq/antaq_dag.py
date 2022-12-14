from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

import antaq.captura_dados as captura
import antaq.extrai_dados as extrair
import antaq.tranforma_dados_atracacao as transforma_atracacao
import antaq.transforma_dados_carga as transforma_carga
import antaq.load_data as load

# email de notificação
email = "sanchesgabriellu@gmail.com"

#configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # Execute uma vez a cada 15 minutos 
    # 'schedule_interval': '*/2 * * * *'
    'schedule_interval': None

}

with DAG(dag_id='etl_antaq', 
        default_args=default_args, 
        schedule_interval=None,
        tags=['etl_antaq']
        ) as dag:
    
    # captura os dados
    captura_dados = PythonOperator(
        task_id='Captura_dados',
        python_callable= captura.download_files,
        do_xcom_push = False,
        dag=dag,    
    )

    # extrai os arquivos
    extrai_dados = PythonOperator(
        task_id='extrai_dados',
        python_callable= extrair.executa_extracao,
        do_xcom_push = False,
        dag=dag,    
    )

    # transforma dados atracação
    transforma_dados_atracacao = PythonOperator(
        task_id='transforma_dados_atracacao',
        python_callable= transforma_atracacao.salvaArquivo,
        do_xcom_push = False,
        dag=dag,    
    )

    # transforma dados carga
    transforma_dados_carga = PythonOperator(
        task_id='transforma_dados_carga',
        python_callable= transforma_carga.salvaArquivo,
        do_xcom_push = False,
        dag=dag, 
    )

    # load dados atracação no banco de dados
    load_dados_atracacao_bd = PythonOperator(
        task_id='load_dados_atracacao_bd',
        python_callable= load.load_dados_atracacao,
        do_xcom_push = False,
        dag=dag,    
    )

    # load dados carga no banco de dados
    load_dados_carga_bd = PythonOperator(
        task_id='load_dados_carga_bd',
        python_callable= load.load_dados_carga,
        do_xcom_push = False,
        dag=dag,    
    )

    '''
    # limpa dados baixados
    clean_dados = BashOperator(
        task_id='clean_dados',
        bash_command = "scripts/clean.sh",
        dag=dag,
    )
    '''

    # notificação por email
    email_send = EmailOperator(
        task_id = "notificacao",
        to = email,
        subject= 'Pipeline Finalizado',
        html_content='<p> Pipeline ETL ANTAQ finalizado!</p>',
        dag=dag
    )
    
    # dependências entre as tarefas
    captura_dados >> extrai_dados >> transforma_dados_atracacao >> transforma_dados_carga >> load_dados_atracacao_bd >> load_dados_carga_bd >> email_send