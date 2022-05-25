from corpo import find_nodrop

from datetime import datetime, date, timedelta
import time
import logging
import configparser


def trata_cribs(cribs_sujos):
    if len(cribs_sujos) > 0:
        cribs_sujos = cribs_sujos.replace(' ', '').split(',')
        cribs_limpos = list(map(int, cribs_sujos))
    else:
        numcribs = range(1, 300)
        cribs_limpos = []
        for i in numcribs:
            cribs_limpos.append(i)
    return cribs_limpos


config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

horarios = config.get('funcionamento', 'horarios').replace(' ', '').split(',')  # horarios de funcionamento.

nome_arquivo_pronto = config.get('funcionamento', 'nome_arquivo_pronto')
pasta_prontos = config.get('funcionamento', 'pasta_prontos') + '\\' + nome_arquivo_pronto
pasta_crib = config.get('funcionamento', 'pasta_crib') + '\\' + nome_arquivo_pronto

'''Configuração de funcionamento - CRIBS'''
cribs = config.get('funcionamento', 'cribs')
cribs = trata_cribs(cribs)

if __name__ == '__main__':
    ontem = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  #'2022-05-23'  #
    anteontem = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')  #'2022-05-22'  #

    '''TREAD'''
    nodrops = find_nodrop.GetNoDrop(cribs, ontem, anteontem)
    print(len(nodrops.dict_nodrops))
    print(nodrops.dict_nodrops)
    print(nodrops.list_cancel)

    # Create_files.Cria_Arquivos(dados, pasta_prontos, pasta_crib)

    time.sleep(60)
