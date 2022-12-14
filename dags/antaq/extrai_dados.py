import os
from zipfile import ZipFile

path = 'data/landing/'
pathAtracacao = 'data/landing/Atracacao'
pathCarga = 'data/landing/Carga'


years = [2019, 2020, 2021]

def extrair_arquivos(path_file, path_output):
    try:
        with ZipFile(path_file, 'r') as zip:
            zip.extractall(path_output)
    except:
        print(f"Ocorreu um erro ao extrair o arquivo {path_file}") 

def executa_extracao():
    for year in years:
        # Atracacao
        filePath_name = f'{pathAtracacao}/Antaq_{year}_atracacao.zip'
        extrair_arquivos(filePath_name, f"{pathAtracacao}/unzip")
        # Carga
        filePath_name = f'{pathCarga}/Antaq_{year}_carga.zip'
        extrair_arquivos(filePath_name, f"{pathCarga}/unzip")