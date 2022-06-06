from corpo import find_nodrop
from corpo import SendMail

from datetime import datetime, timedelta
import time
import logging
import configparser
import pandas as pd


def trata_cribs(cribs_sujos):
    cribs = []
    if type(cribs_sujos) == str:
        cribs_sujos = cribs_sujos.split('-')
        cribs_sujos = range(int(cribs_sujos[0]), int(cribs_sujos[1])+1)
        for crib in cribs_sujos:
            cribs.append(crib)
    else:
        cribs.append(int(cribs_sujos))

    print(cribs)
    return cribs


config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

horarios = config.get('funcionamento', 'horarios').replace(' ', '').split(',')  # horarios de funcionamento.

nome_arquivo_pronto = config.get('funcionamento', 'nome_arquivo_pronto')
pasta_prontos = config.get('funcionamento', 'pasta_prontos') + '\\' + nome_arquivo_pronto
pasta_crib = config.get('funcionamento', 'pasta_crib') + '\\' + nome_arquivo_pronto

'''Configuração de funcionamento - CRIBS'''
# cribs = config.get('funcionamento', 'cribs')
# cribs = trata_cribs(cribs)

nome_arquivo = config.get('funcionamento', 'arquivo_de_config')

if __name__ == '__main__':
    ontem: str = '2022-06-01'  # (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # '2022-05-23'  #
    anteontem: str = '2022-05-31'  # "(datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')  # '2022-05-22'  #

    '''TREAD'''
    dados = pd.read_excel(nome_arquivo)
    for i in dados.index:
        cribs = dados['cribs-interval'][i]
        cribs = trata_cribs(cribs)
        cribs_name = dados['nome_empr'][i]
        cribs_emails = dados['emails'][i]
        inactive = dados['inactive'][i]
        time.sleep(1)
        nodrops = find_nodrop.RelatorioNodrop(cribs, ontem, anteontem)
        SendMail.SendMailTread(cribs_emails, ontem, nodrops.dict_contagem, cribs_name, nodrops.nome_relat)
