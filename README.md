# projetoFIEC_ANTAQ
Projeto de Engenharia de Dados realizando um ETL com dados da ANTAQ. Este projeto foi construído utilizando a arquitetura do Docker, com as imagens:

* Airflow (Disponibilizada na documentação)
* SQL Server 2019
* Spark

Foi adicionado no docker-compose algumas variáveis de ambiente para utilização das configurações do SMTP.

Servidor de Email SMTP utilizado neste projeto foi o mailtrap, mais informações pode ser encontrado no site: https://mailtrap.io/

Para o funcionamento do Spark, precisei criar um Dockerfile, para realizar a instalação dos pacotes utilizados no projeto, como principalemente para criar a variável de ambiente do JAVA para o correto funcionamento do Spark no projeto

### Utilizando o Projeto ###

- Clone do repositório
git clone https://github.com/sanchesfranklin/projetoFIEC_ANTAQ.git

- Configurando o usuário correto para uso do Airflow (Linux)
echo -e "AIRFLOW_UID=$(id -u)" > .env

- Inicializando o projeto
docker-compose up -d --build

- Abrindo a Interface
http://localhost:8080

- Credenciais
Login: airflow
Senha: airflow

### Uma observação a este projeto ###
Para a criação da estrutura do Banco de Dados SQL ainda não consegui rodar o script init-databaseFiec.sh automaticamente assim que o container inicializa.
Dessa forma para a criação da estrutura tive que realizar a configuração manualmente, executando o script no SGBD

SGBD gratuito para Linux, MAC e Windows que utilizei neste projeto, e que também me auxiliou nas tarefas:
https://dbeaver.io/download/

Verificar o IP do container SQL e realizar a configuração simples no SGBD.

Realizar a configuração também da conexão do Airflow com o SQL Server



