import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

path_atracacao = 'data/landing/Atracacao/'

spark = SparkSession.builder.appName("etl_antaq").getOrCreate()
#spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())
#spark = SparkSession.builder.appName("FiecAntaq").master("spark://172.21.0.5:7077").getOrCreate()
#spark = SparkContext()
#conf = SparkConf().setAppName("FiecAntaq").setMaster(master_spark)
#spark = SparkContext(conf=conf)

# definindo o inferSchema manual
schema_df = StructType([
    StructField("IDAtracacao", IntegerType()),
    StructField("CDTUP", StringType()),
    StructField("IDBerco", StringType()),
    StructField("Berço", StringType()),
    StructField("Porto Atracação", StringType()),
    StructField("Apelido Instalação Portuária", StringType()),
    StructField("Complexo Portuário", StringType()),
    StructField("Tipo da Autoridade Portuária", StringType()),
    StructField("Data Atracação", StringType()),
    StructField("Data Chegada", StringType()),
    StructField("Data Desatracação", StringType()),
    StructField("Data Início Operação", StringType()),
    StructField("Data Término Operação", StringType()),
    StructField("Ano", IntegerType()),
    StructField("Mes", IntegerType()),
    StructField("Tipo de Operação", StringType()),
    StructField("Tipo de Navegação da Atracação", StringType()),
    StructField("Nacionalidade do Armador", StringType()),
    StructField("FlagMCOperacaoAtracacao", StringType()),
    StructField("Terminal", StringType()),
    StructField("Município", StringType()),
    StructField("UF", StringType()),
    StructField("SGUF", StringType()),
    StructField("Região Geográfica", StringType()),
    StructField("Nº da Capitania", StringType()),
    StructField("Nº do IMO", StringType())
])

# df_raw = spark.read.csv('data/landing/Atracacao/unzip/*')
df_raw = spark.read\
        .format("CSV")\
        .option("sep",";")\
        .schema(schema_df)\
        .option("header", "true")\
        .load("data/landing/Atracacao/unzip")

# renomeando as colunas para evitar erros na leitura do data
df_raw = df_raw\
    .withColumnRenamed('Berço', 'Berco')\
    .withColumnRenamed('Porto Atracação', 'PortoAtracacao')\
    .withColumnRenamed('Apelido Instalação Portuária', 'ApelidoInstalacaoPortuaria')\
    .withColumnRenamed('Complexo Portuário', 'ComplexoPortuario')\
    .withColumnRenamed('Tipo da Autoridade Portuária', 'TipoAutoridadePortuaria')\
    .withColumnRenamed('Data Atracação', 'DataAtracacao')\
    .withColumnRenamed('Data Chegada', 'DataChegada')\
    .withColumnRenamed('Data Desatracação', 'DataDesatracacao')\
    .withColumnRenamed('Data Início Operação', 'DataInicioOperacao')\
    .withColumnRenamed('Data Término Operação', 'DataTerminoOperacao')\
    .withColumnRenamed('Tipo de Operação', 'TipoOperacao')\
    .withColumnRenamed('Tipo de Navegação da Atracação', 'TipoNavegacaoAtracacao')\
    .withColumnRenamed('Região Geográfica', 'RegiaoGeografica')\
    .withColumnRenamed('Nº da Capitania', 'NumeroCapitania')\
    .withColumnRenamed('Nº do IMO', 'NumeroIMO')\
    .withColumnRenamed('Nacionalidade do Armador', 'NacionalidadeArmador')\
    .withColumnRenamed('Município', 'Municipio')

# alterando formatos das datas
df_raw = df_raw\
    .withColumn('DataAtracacao', to_timestamp(df_raw.DataAtracacao, "dd/MM/yyyy HH:mm:ss"))\
    .withColumn('DataChegada', to_timestamp(df_raw.DataChegada, "dd/MM/yyyy HH:mm:ss"))\
    .withColumn('DataDesatracacao', to_timestamp(df_raw.DataDesatracacao, "dd/MM/yyyy HH:mm:ss"))\
    .withColumn('DataInicioOperacao', to_timestamp(df_raw.DataInicioOperacao, "dd/MM/yyyy HH:mm:ss"))\
    .withColumn('DataTerminoOperacao', to_timestamp(df_raw.DataTerminoOperacao, "dd/MM/yyyy HH:mm:ss"))

# deletando valores nulls
df_raw = df_raw.na.drop(how="any")

# salvando em parquet
def salvaArquivo():
    df_raw.write.parquet("data/raw/Atracacao/atracacao.parquet", mode='overwrite')

    # testando a visualização
    '''
    print('Visualizando o DataFrame')
    dataFLeitura = spark.read\
        .option("header", "True")\
        .option("sep", ";")\
        .option("inferSchema", "True")\
        .parquet("data/raw/Atracacao/atracacao.parquet")
    
    dataFLeitura.show()
    print('Filtrando dado')
    dataFLeitura.where("Ano = 2021").show(5)
    '''

