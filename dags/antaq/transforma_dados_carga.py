import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp

path_carga = 'data/landing/Carga/'

spark = SparkSession.builder.appName("etl_antaq").getOrCreate()

# definindo o schema manual
schema_df = StructType([
    StructField("IDCarga", IntegerType()),
    StructField("IDAtracacao", IntegerType()),
    StructField("Origem", StringType()),
    StructField("Destino", StringType()),
    StructField("CDMercadoria", StringType()),
    StructField("Tipo Operação da Carga", StringType()),
    StructField("Carga Geral Acondicionamento", StringType()),
    StructField("ConteinerEstado", StringType()),
    StructField("Tipo Navegação", StringType()),
    StructField("FlagAutorizacao", StringType()),
    StructField("FlagCabotagem", StringType()),
    StructField("FlagCabotagemMovimentacao", StringType()),
    StructField("FlagConteinerTamanho", StringType()),
    StructField("FlagLongoCurso", StringType()),
    StructField("FlagMCOperacaoCarga", StringType()),
    StructField("FlagOffshore", StringType()),
    StructField("FlagTransporteViaInterioir", StringType()),
    StructField("Percurso Transporte em vias Interiores", StringType()),
    StructField("Percurso Transporte Interiores", StringType()),
    StructField("STNaturezaCarga", StringType()),
    StructField("STSH2", StringType()),
    StructField("STSH4", StringType()),
    StructField("Natureza da Carga", StringType()),
    StructField("Sentido", StringType()),
    StructField("TEU", StringType()),
    StructField("QTCarga", StringType()),
    StructField("VLPesoCargaBruta", StringType())
])

# criando o dataframe
df_raw = spark.read\
        .format("CSV")\
        .option("sep",";")\
        .schema(schema_df)\
        .option("header", "true")\
        .load("data/landing/Carga/unzip")

# renomeando as colunas para evitar erros na leitura do data
df_raw = df_raw\
    .withColumnRenamed('Tipo Operação da Carga', 'TipoOperacaoCarga')\
    .withColumnRenamed('Carga Geral Acondicionamento', 'CargaGeralAcondicionamento')\
    .withColumnRenamed('Tipo Navegação', 'TipoNavegacao')\
    .withColumnRenamed('Percurso Transporte em vias Interiores', 'PercursoTransporteViasInteriores')\
    .withColumnRenamed('Percurso Transporte Interiores', 'PercursoTransporteInteriores')\
    .withColumnRenamed('Natureza da Carga', 'NaturezaCarga')


# deletando valores nulls
df_raw = df_raw.na.drop(how="any")

# salvando em parquet
def salvaArquivo():
    df_raw.write.parquet("data/raw/Carga/carga.parquet", mode='overwrite')

    # testando a visualização
    
    print('Visualizando o DataFrame')
    dataFLeitura = spark.read\
        .option("header", "True")\
        .option("sep", ";")\
        .option("inferSchema", "True")\
        .parquet("data/raw/Carga/carga.parquet")
    
    dataFLeitura.show()
    '''
    print('Mostrando as colunas')
    dataFLeitura.printSchema()
    '''

'''

IDCarga;
IDAtracacao;
Origem;
Destino;
CDMercadoria;
Tipo Operação da Carga;
Carga Geral Acondicionamento;
ConteinerEstado;
Tipo Navegação;
FlagAutorizacao;
FlagCabotagem;
FlagCabotagemMovimentacao;
FlagConteinerTamanho;
FlagLongoCurso;
FlagMCOperacaoCarga;
FlagOffshore;
FlagTransporteViaInterioir;
Percurso Transporte em vias Interiores;
Percurso Transporte Interiores;
STNaturezaCarga;
STSH2;
STSH4;
Natureza da Carga;
Sentido;
TEU;
QTCarga;
VLPesoCargaBruta

'''