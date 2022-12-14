import requests
import zipfile
import os
from io import BytesIO

urlBase = 'http://web.antaq.gov.br/'
endpoint = 'Sistemas/ArquivosAnuario/Arquivos/{}.zip'

path = 'data/landing'
path_atracacao = 'data/landing/Atracacao'
path_carga = 'data/landing/Carga'

years = [2019, 2020, 2021]


def file_download(uri, pathFile):
    res = requests.get(uri)
    if res.status_code == 200:
        with open(pathFile, 'wb') as newFile:
            newFile.write(res.content)
        print('Arquivo baixado!')
    else:
        res.raise_for_status()

def download_files():
    # Download atracação
    for year in years:
        path_fileName = os.path.join(path_atracacao, f'Antaq_{year}_atracacao.zip')
        uri = os.path.join(urlBase, endpoint.format(f'{year}Atracacao'))
        file_download(uri, path_fileName)

    # Download carga
    for year in years:
        path_fileName = os.path.join(path_carga, f'Antaq_{year}_carga.zip')
        uri = os.path.join(urlBase, endpoint.format(f'{year}Carga'))
        file_download(uri, path_fileName)
