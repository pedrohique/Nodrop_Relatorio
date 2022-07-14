from corpo import find_nodrop
from corpo import SendMail

from datetime import datetime, timedelta, date
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

    #print(cribs)
    return cribs


config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')


'''Configuração de funcionamento - CRIBS'''
# cribs = config.get('funcionamento', 'cribs')
# cribs = trata_cribs(cribs)

nome_arquivo = config.get('funcionamento', 'arquivo_de_config')
hora_programada = config.get('funcionamento', 'hora_programada')

if __name__ == '__main__':
    while True:
        hora_atual = datetime.today().strftime('%H:%M')

        logging.info(f'consultando hora programada: {hora_programada}')
        if hora_programada == hora_atual:
            '''TREAD'''
            dados = pd.read_excel(nome_arquivo)
            for i in dados.index:
                cribs = dados['cribs-interval'][i]
                cribs = trata_cribs(cribs)
                cribs_name = dados['nome_empr'][i]
                cribs_emails = dados['emails'][i]
                inactive = dados['inactive'][i]
                time.sleep(1)
                if inactive == 0:
                    ontem: str = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')  # '2022-05-23'  #
                    anteontem: str = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')  # '2022-05-22'  #
                    logging.info(f'iniciando arquivo - data de relatorio: {ontem}')

                    nodrops = find_nodrop.RelatorioNodrop(cribs, ontem, anteontem)
                    SendMail.SendMailTread(cribs_emails, ontem, nodrops.dict_contagem, cribs_name, nodrops.nome_relat)
            time.sleep(60)
        else:
            hora_atual_obj = datetime.strptime(hora_atual, '%H:%M').time()
            hora_programada_obj = datetime.strptime(hora_programada, '%H:%M').time()
            falta = (datetime.combine(date.min, hora_programada_obj) - datetime.combine(date.min,
                                                                                        hora_atual_obj)) / timedelta(
                seconds=1)
            dia = timedelta(days=1) / timedelta(seconds=1)
            if falta < 0:
                falta = dia - (falta * -1)

            falta = int(falta)
            logging.info(f'hora programada invalida. Resta {falta} segundos para execução')
            time.sleep(falta)
