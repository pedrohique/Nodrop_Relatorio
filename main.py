from corpo import find_nodrop

from datetime import datetime, timedelta
import time
import logging
import configparser
import pandas as pd


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
# cribs = config.get('funcionamento', 'cribs')
# cribs = trata_cribs(cribs)
cribs = [150, 151, 152, 153, 154]

if __name__ == '__main__':
    ontem: str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # '2022-05-23'  #
    anteontem: str = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')  # '2022-05-22'  #

    '''TREAD'''
    nodrops = find_nodrop.RelatorioNodrop(cribs, ontem, anteontem)
    # print(nodrops.dict_nodrops)
    # print(nodrops.dict_cancl)
    # print(nodrops.dict_issue)
    # dict_geral = {}
    #
    # for i in nodrops.dict_nodrops.keys():
    #     nodrops.dict_nodrops[i].append('Não Queda')
    #
    # for i in nodrops.dict_issue.keys():
    #     nodrops.dict_issue[i].append('Retirada')
    #
    # for i in nodrops.dict_cancl.keys():
    #     nodrops.dict_cancl[i].append('Cancelada')
    #
    # dict_geral.update(nodrops.dict_issue)
    # dict_geral.update(nodrops.dict_nodrops)
    # dict_geral.update(nodrops.dict_cancl)
    # print(dict_geral)
    # df = pd.DataFrame(columns=['crib', 'bin',
    #                            'item', 'employee_trans', 'transdate',
    #                            'transdate_limpo', 'quantity',
    #                            'typedesc', 'user1', 'user2', 'binqty'])
    # print(dict_geral)
    # colunas = ['crib', 'bin', 'item', 'employee', 'Transdate', 'quantity', 'TypeDescription', 'Centro de Custo', 'Função', 'Binquantity', 'resultado']
    # df = pd.DataFrame.from_dict(dict_geral, orient='index', columns=colunas)
    # print(df.head())
    # df.to_excel('teste.xlsx')
    # df.to_excel('teste.xlsx' columns= ['crib', 'bin',
    #                                                              'item', 'employee_trans', 'transdate',
    #                                                              'transdate_limpo', 'quantity',
    #                                                              'typedesc', 'user1', 'user2', 'binqty'])

    # Create_files.Cria_Arquivos(dados, pasta_prontos, pasta_crib)

    #time.sleep(60)
