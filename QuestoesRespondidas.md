## Questões Respondidas do Projeto ##

A ) Observando os dados da fonte coletada sugiro utilizar a estrutura de um Banco de Dados relacional devido estar bem estruturado, definido e como consultado na fonte existe um relacionamento entre eles.

B ) Para esta questão foram criados scripts automatizados em Python, e também em Spark para processamento dos dados, abaixo listo os arquivos
*  Script de Captura: captura_dados.py
*  Script de Extração: extrai_dados.py
*  Script de Transformação Spark: 
    * transforma_dados_atracacao.py
    * transforma_dados_carga.py

C ) Foi desenvolvido uma query em SQL para atender o requisito dos dados solicitados, conforme também o script onde foi desenvolvido para a estrutura dos dados no servidor.
*  init-databaseFiec.sql
*  consultaExcel.sql

D ) Foi desenvolvido uma pipeline com Airflow, onde está seguindo o fluxo de ETL, ou seja Extraindo, Transformando e Carregando os dados em nosso banco de dados SQL Server, realizando o envio
de uma notificação via Email quando a pipeline é finalizada, abaixo segue as imagens:

### DAG ###

![Dag](https://user-images.githubusercontent.com/55898372/207714909-00d2ce30-7a12-462e-8cce-91b1c0f42f9f.jpeg)

### Notificação via Email ###

![Notificacao_pipeline](https://user-images.githubusercontent.com/55898372/207714950-6c8938f6-a86f-44c6-995c-a1abd51f73e3.jpg)


