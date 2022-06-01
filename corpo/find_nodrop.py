import pandas as pd
import pyodbc
import cryptocode
import configparser
import logging

config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')


class GetNoDropWorker:

    def __init__(self, cribs, ontem, anteontem):
        """Dados Ingeridos"""
        self.cribs = cribs
        self.ontem = ontem
        self.anteontem = anteontem

        '''Dados Base'''
        self.list_eventlog_base = []  # lista de nodrops unicos
        self.list_eventlog = []  # lista de nodrops totais
        self.dict_nodrops = {}  # dicionario de dados com todas as transações que precisam ser canceladas.
        self.soma_nodrops = 0  # soma o numero de nodrops
        self.soma_cancelamentos = 0  # soma o numero de transações que foram canceladas manualmente
        self.soma_trans = 0  # soma o numero de transações que foram canceladas pelo sistema
        self.soma_trans_true = 0  # soma o numero de transações que podem ser canceladas
        self.soma_unfind = 0  # soma a quantidade de transações que não foram encontradas

        '''dados relatorio'''
        self.dict_geral = {}  # Dicionario geral para relatorio
        self.list_cancel = []  # Lista de cancelamentos para relatorio
        self.dict_cancl = {}  # dict de cancelamentos para relatorio
        self.dict_issue = {}  # dict de issues para relatorio
        self.dict_cancl_nomot = {}

        '''CNX DB'''
        server = config.get('dados_banco', 'server')
        port = config.get('dados_banco', 'port')
        database = config.get('dados_banco', 'database')
        uid = config.get('dados_banco', 'uid')
        pwd = config.get('dados_banco', 'pwd')

        uid = cryptocode.decrypt(uid, "i9brgroup")
        pwd = cryptocode.decrypt(pwd, "i9brgroup")
        try:
            self.cnxn = pyodbc.connect(
                f'DRIVER=SQL Server;SERVER={server};PORT={port};DATABASE={database};UID={uid};PWD={pwd};')
            self.cursor = self.cnxn.cursor()
            logging.info('conexão com o banco de dados efetuada com sucesso.')

        except:
            logging.error('não foi possivel conectar ao banco de dados')

    def select_nodrops(self):
        """O Objetivo desta classe é retornar uma lista dos nodrops unicos que serão analsados no proximo metodo"""

        def remove_repetidos(lista):  # retorna uma lista de nodrops unicos
            df = pd.DataFrame(lista, columns=['employee', 'cribbin', 'crib', 'eventlogdate'])
            df.sort_values(by=['crib'])
            df = df.drop_duplicates()
            lista_unica = df.values.tolist()

            return lista_unica

        logging.info('Iniciando pesquisa de nodrops')
        self.cursor.execute(
            f"select EventLogDate, EventLogMessage from EventLog where EventLogKey is null and "
            f"EventLogProgramName = 'CribMaster' and EventLogDate "
            f"BETWEEN CONVERT(datetime, '{self.anteontem}T23:00:00') AND "
            f"CONVERT(datetime, '{self.ontem}T23:59:59');")
        nodrops = self.cursor.fetchall()  # pega todos os nodrops do banco

        for i in nodrops:
            '''Limpa a msg sobre o crib extraindo os dados'''
            msg = i[1].split(' ')
            if msg[0] == 'No' and msg[1] == 'Drop' and msg[2] == 'detected':  # filtra pelo incio da frase
                employee = msg[8]
                cribin = msg[-1]
                crib = cribin.split('-')
                crib = int(crib[0])
                eventlogdate = i[0].strftime('%Y-%m-%d')

                if crib in self.cribs:  # seleciona apenas os cribs selecionados no main e adiciona a lista de nodrops
                    self.list_eventlog.append([employee, cribin, crib, eventlogdate])

        self.list_eventlog_base = remove_repetidos(
            self.list_eventlog)  # remove nodrops repetidos para realizar a contagem a baixo

    def select_trans_nodrop(self, employee, cribbin, canceltodo):  # seleciona todas as trans especificadas no nodrop
        self.cursor.execute(f"select t.transnumber, s.crib, s.bin,"
                            f" t.item, e.ID, t.Transdate, t.quantity, t.TypeDescription,"
                            f" e.User1, e.User2, t.binqty from trans t with(nolock)"
                            f" join EMPLOYEE e (nolock) on t.IssuedTo = e.ID "
                            f"join INVENTRY  i (nolock) on i.ItemNumber = t.Item "
                            f"join STATION s (nolock) on s.CribBin = t.CribBin "
                            f"where t.IssuedTo = '{employee}' and TypeDescription = 'ISSUE' "
                            f"and transdate BETWEEN CONVERT(datetime, '{self.anteontem}T22:00:00')"
                            f" AND CONVERT(datetime, '{self.ontem}T23:59:59') "
                            f"and Status IS NULL and t.CribBin = '{cribbin}'")

        transacoes = self.cursor.fetchall()
        list_trans_nodrop = []  # lista de transações possiveis para o
        # nodrop vai retornar sempre o total possivel
        contador = 0
        for trans in transacoes:  # procura as transações no dia de ontem
            transnumber = trans[0]
            crib = trans[1]
            bin = trans[2]
            item = trans[3]
            employee_trans = trans[4]
            transdate = trans[5]
            transdate_limpo = transdate.strftime('%Y-%m-%d')
            quantity = trans[6]
            typedesc = trans[7]
            user1 = trans[8]
            user2 = trans[9]
            binqty = trans[10]
            contador += contador + 1

            if transdate_limpo == self.ontem:
                modelo = f'{transnumber}, {crib}, {bin}, {item}, {employee_trans}, {transdate},' \
                         f' {quantity}, {typedesc},{user1}, {user2}, {binqty},NAO QUEDA'
                if modelo not in list_trans_nodrop:
                    list_trans_nodrop.append(
                        f'{transnumber}, {crib}, {bin}, {item}, {employee_trans}, {transdate}, {quantity},'
                        f' {typedesc},{user1}, {user2}, {binqty},NAO QUEDA')

        if len(list_trans_nodrop) == 0:  # Caso não encontre verifica
            # se as transações estão na data de ante ontem
            for trans in transacoes:
                transnumber = trans[0]
                crib = trans[1]
                bin = trans[2]
                item = trans[3]
                employee_trans = trans[4]
                transdate = trans[5]
                transdate_limpo = transdate.strftime('%Y-%m-%d')
                quantity = trans[6]
                typedesc = trans[7]
                user1 = trans[8]
                user2 = trans[9]
                binqty = trans[10]
                if transdate_limpo == self.anteontem:
                    modelo = f'{transnumber}, {crib}, {bin}, {item}, {employee_trans}, {transdate}, {quantity},' \
                             f' {typedesc},{user1}, {user2}, {binqty},NAO QUEDA'
                    if modelo not in list_trans_nodrop:
                        list_trans_nodrop.append(
                            f'{transnumber}, {crib}, {bin}, {item}, {employee_trans}, {transdate},'
                            f' {quantity}, {typedesc},{user1}, {user2}, {binqty},NAO QUEDA')

        logging.info('transações de nodrop selecionadas com sucesso.')
        if len(list_trans_nodrop) >= canceltodo:
            return list_trans_nodrop[
                   :canceltodo]  # ADICIONADO O INDEX, AGORA SE POSSUI 2 NODROPS A SER FEITO ELE VAI ADICIONAR
            # NA LISTA E RETORNAR DE ACORDO COM A QUANTIDADE DE NODROP
        else:
            return []

    def get_trans_nodrops(self):

        def count_cancel(cursor, unic_employee, unic_cribin, ontem):
            """Verfica quantos nodrops ja foram realzados para aquele nodrop unico"""
            cursor.execute(f"select t.transnumber, s.crib, s.bin,"
                           f" t.item, e.ID, t.Transdate, t.quantity, t.TypeDescription,"
                           f" e.User1, e.User2, t.binqty from trans t with(nolock)"
                           f" join EMPLOYEE e (nolock) on t.IssuedTo = e.ID "
                           f"join INVENTRY  i (nolock) on i.ItemNumber = t.Item "
                           f"join STATION s (nolock) on s.CribBin = t.CribBin "
                           f"where t.IssuedTo = '{unic_employee}' and TypeDescription = 'CANCL' "
                           f"and transdate >= CONVERT(datetime, '{ontem}T00:00:00') and t.CribBin = '{unic_cribin}'")
            # f"select transnumber, RelatedKey, crib, bin, item, employee, Transdate, quantity,"
            # f" TypeDescription, User1, User2, binqty "
            # f"from Trans where issuedto = '{unic_employee}' and TypeDescription = 'CANCL' and"
            # f" CribBin = '{unic_cribin}' and transdate >= CONVERT(datetime, '{ontem}T00:00:00') ")
            transacoes = cursor.fetchall()
            self.list_cancel.append(transacoes)
            return len(transacoes)

        for nodrop_unic in self.list_eventlog_base:
            '''Para cada nodrop unico no eventlog separa os dados para realizar a contagem na função conunt_cancel'''
            employee = nodrop_unic[0]
            cribin = nodrop_unic[1]
            contagem = self.list_eventlog.count(nodrop_unic)  # conta quantidade de nodrops
            count_cancel_var = count_cancel(self.cursor, employee, cribin,
                                            self.ontem)  # conta quantidade de cancelamentos para cada nodrop unico
            self.soma_cancelamentos += count_cancel_var  # statisticas
            self.soma_nodrops += contagem  # statisticas
            cancl_to_do = contagem - count_cancel_var  # nodrops que podem ser realizados
            while cancl_to_do != 0:
                self.soma_trans_true += 1  # statisticas soma o numero de transações que podem ser canceladas
                '''linha abaixo retorna uma lista com as transações de cada nodrop.'''
                trans_correta = self.select_trans_nodrop(employee, cribin, cancl_to_do)
                # caso a transação ja tenha sido cancelada manualmente ela vai retornar none
                cancl_to_do -= 1  # diminui a quantidade de nodrops que não foram realizados
                if len(trans_correta) > 0:
                    self.soma_trans += 1  # soma o numero de transações que foram canceladas pelo sistema
                    for trans in trans_correta:
                        '''adiciona a transação no dicionario'''
                        trans = trans.split(',')  #
                        transnumber = trans[0].replace("'", '')
                        crib = trans[1].replace(' ', '')
                        bin = trans[2].replace(' ', '')
                        item = trans[3].replace(' ', '')
                        employee = trans[4].replace(' ', '')
                        Transdate = trans[5]
                        quantity = trans[6].replace(' ', '')
                        TypeDescription = trans[7].replace(' ', '')
                        # type_trans = trans[8]
                        user1 = trans[8].replace(' ', '')
                        if user1 == 'None':
                            user1 = 'NA'
                        user2 = trans[9].replace(' ', '')
                        if user2 == 'None':
                            user2 = 'NA'
                        binqty = trans[10].replace(' ', '')
                        self.dict_nodrops[transnumber] = [str(crib), bin, item, employee, str(Transdate), str(quantity),
                                                          TypeDescription, user1, user2, binqty]
                else:
                    self.soma_unfind += 1  # soma a quantidade de transações que não foram encontradas

            logging.info(
                f'Nodrops encontrados: {self.soma_nodrops}, Cancl encontrados: {self.soma_cancelamentos}, '
                f'Nodrops possiveis: {self.soma_trans_true}, total de cancelamentos efetuados: {self.soma_trans}, '
                f'quantidade de transações que não foram encontradas : {self.soma_unfind}')
            print(
                f'Nodrops encontrados: {self.soma_nodrops}, Cancl encontrados: {self.soma_cancelamentos}, '
                f'Nodrops possiveis: {self.soma_trans_true}, total de cancelamentos efetuados: {self.soma_trans}, '
                f'quantidade de transações que não foram encontradas : {self.soma_unfind}')
            print('----------------------------------------------------------')

    '''Aqui pra baixo começa o desenvolvimento dos relatorios'''

    def limpa_cancl(self):
        """Trata as informações de cancels criadas pela função count cancel dentro do metodo get_trans_nodrop"""
        for cancl in self.list_cancel:
            for canc in cancl:
                # print(cancl[0][0], cancl[0][1], cancl[0][2], cancl[0][3],
                # cancl[0][4], cancl[0][5], cancl[0][6], cancl[0][7], cancl[0][8], cancl[0][9]
                # , cancl[0][10])
                transnumber = canc[0]
                crib = canc[1]
                bin = canc[2].replace(' ', '')
                item = canc[3].replace(' ', '')
                employee = canc[4].replace(' ', '')
                Transdate = canc[5]
                quantity = canc[6]
                TypeDescription = canc[7].replace(' ', '')
                user1 = canc[8].replace(' ', '')
                if user1 == 'None':
                    user1 = 'NA'
                user2 = str(canc[9]).replace(' ', '')
                if user2 == 'None':
                    user2 = 'NA'
                binqty = canc[10]
                self.dict_cancl[transnumber] = [str(crib), bin, item, employee, str(Transdate), str(quantity),
                                                TypeDescription, user1, user2, binqty]

    def list_trans(self):
        """Busca as transações de issues no banco"""
        tuple_cribs = tuple(self.cribs)
        self.cursor.execute(f"select t.transnumber, s.crib, s.bin,"
                            f" t.item, e.ID, t.Transdate, t.quantity, t.TypeDescription,"
                            f" e.User1, e.User2, t.binqty from trans t with(nolock)"
                            f" join EMPLOYEE e (nolock) on t.IssuedTo = e.ID "
                            f"join INVENTRY  i (nolock) on i.ItemNumber = t.Item "
                            f"join STATION s (nolock) on s.CribBin = t.CribBin "
                            f"where TypeDescription = 'ISSUE' and status IS NULL "
                            f"and transdate BETWEEN CONVERT(datetime, '{self.ontem}T00:00:00') "
                            f"AND CONVERT(datetime, '{self.ontem}T23:59:59')"
                            f" and t.Crib in {tuple_cribs}")
        trans_listing = self.cursor.fetchall()
        for trans in trans_listing:
            '''adiciona a transação no dicionario'''
            transnumber = trans[0]
            crib = trans[1]
            bin = trans[2].replace(' ', '')
            item = trans[3].replace(' ', '')
            employee = trans[4].replace(' ', '')
            Transdate = trans[5]
            quantity = trans[6]
            TypeDescription = trans[7].replace(' ', '')
            user1 = trans[8].replace(' ', '')
            if user1 == 'None':
                user1 = 'NA'
            user2 = trans[9].replace(' ', '')
            if user2 == 'None':
                user2 = 'NA'
            binqty = trans[10]
            self.dict_issue[transnumber] = [str(crib), bin, item, employee, str(Transdate), str(quantity),
                                            TypeDescription, user1, user2, binqty]

    def busca_cancl(self):
        """Busca as transações de cancl sem motivo aparente"""
        tuple_cribs = tuple(self.cribs)
        self.cursor.execute(f"select t.transnumber, s.crib, s.bin,"
                            f" t.item, e.ID, t.Transdate, t.quantity, t.TypeDescription,"
                            f" e.User1, e.User2, t.binqty from trans t with(nolock)"
                            f" join EMPLOYEE e (nolock) on t.IssuedTo = e.ID "
                            f"join INVENTRY  i (nolock) on i.ItemNumber = t.Item "
                            f"join STATION s (nolock) on s.CribBin = t.CribBin "
                            f"where TypeDescription = 'CANCL' "
                            f"and transdate BETWEEN CONVERT(datetime, '{self.ontem}T00:00:00') "  # {self.ontem}
                            f"AND CONVERT(datetime, '{self.ontem}T23:59:59')"
                            f" and t.Crib in {tuple_cribs}")
        trans_cancl = self.cursor.fetchall()
        for cancl in trans_cancl:
            if cancl[0] not in self.dict_cancl.keys():
                transnumber = cancl[0]
                crib = cancl[1]
                bin = cancl[2].replace(' ', '')
                item = cancl[3].replace(' ', '')
                employee = cancl[4].replace(' ', '')
                Transdate = cancl[5]
                quantity = cancl[6]
                TypeDescription = cancl[7].replace(' ', '')
                user1 = cancl[8].replace(' ', '')
                if user1 == 'None':
                    user1 = 'NA'
                user2 = cancl[9].replace(' ', '')
                if user2 == 'None':
                    user2 = 'NA'
                binqty = cancl[10]
                self.dict_cancl_nomot[transnumber] = [str(crib), bin, item, employee, str(Transdate), str(quantity),
                                                      TypeDescription, user1, user2, binqty]
        print(self.dict_cancl_nomot)

    def trata_relat(self):
        def altera_dados():
            for trans in self.dict_nodrops.keys():
                self.dict_nodrops[trans].append('NAO QUEDA')

            for trans in self.dict_issue.keys():
                self.dict_issue[trans].append('ENTREGUE')

            for trans in self.dict_cancl.keys():
                self.dict_cancl[trans].append('CANCELADO POR NAO QUEDA')

            for trans in self.dict_cancl_nomot.keys():
                self.dict_cancl_nomot[trans].append('CANCELADO')

        altera_dados()
        self.dict_geral.update(self.dict_issue)
        self.dict_geral.update(self.dict_nodrops)
        self.dict_geral.update(self.dict_cancl)
        self.dict_geral.update(self.dict_cancl_nomot)

    def cria_relat(self):
        colunas = ['crib', 'bin', 'item', 'employee', 'Transdate', 'quantity', 'TypeDescription', 'Centro de Custo',
                   'Função', 'Binquantity', 'resultado']
        df = pd.DataFrame.from_dict(self.dict_geral, orient='index', columns=colunas)
        df.to_excel(f'teste.xlsx')

    def count_trans(self):
        issue_qtd = int()
        nodrop_qtd = int()
        cancel_qtd = int()
        cancelnodrop_qtd = int()
        dict_results = {}
        for trans in self.dict_geral:
            tipo = self.dict_geral[trans][-1]
            qtd = int(self.dict_geral[trans][-6])
            if qtd < 0:
                qtd = qtd * -1
            if tipo == 'ENTREGUE':
                issue_qtd += qtd
            elif tipo == 'NAO QUEDA':
                nodrop_qtd += qtd
            elif tipo == 'CANCELADO':
                cancel_qtd += qtd
            elif tipo == 'CANCELADO POR NAO QUEDA':
                cancelnodrop_qtd += qtd
        print(issue_qtd, nodrop_qtd, cancel_qtd, cancelnodrop_qtd)


class GetNoDrop(GetNoDropWorker):
    def __init__(self, cribs, ontem, anteontem):
        super().__init__(cribs, ontem, anteontem)
        self.select_nodrops()
        self.get_trans_nodrops()


class RelatorioNodrop(GetNoDropWorker):
    def __init__(self, cribs, ontem, anteontem):
        super().__init__(cribs, ontem, anteontem)
        self.select_nodrops()
        self.get_trans_nodrops()
        self.limpa_cancl()
        self.list_trans()
        self.busca_cancl()
        self.trata_relat()
        self.cria_relat()
        self.count_trans()
