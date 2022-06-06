# encoding: utf-8
import cryptocode #descriptografa a senho do config file
import configparser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from bs4 import BeautifulSoup
from datetime import  datetime

config = configparser.ConfigParser()
config.read('config.ini')

class SendMail:

    def __init__(self, emails, data, results, nome_empresa, relat_name):
        '''pega a chave do password e desencrypta'''
        key = 'i9brgroup'
        password_cript = config.get('enviar_email', 'password')
        password = cryptocode.decrypt(password_cript, key)

        self.emails = emails
        self.host = config.get('enviar_email', 'server')
        self.port = config.get('enviar_email', 'port')
        self.user = config.get('enviar_email', 'user')
        self.password = password
        self.data = data
        self.results = results #dicionario com os resultados
        self.nome_empresa = nome_empresa
        self.relat_name = relat_name

        '''email data'''
        self.soup = None
        self.con = None
        self.email_msg = None

        if type(self.emails) != list:
            self.emails = self.emails.split(',')

    def Change_html(self):
        arquivo = config.get('enviar_email', 'html_caminho')

        with open(arquivo, encoding='utf-8') as arc:
            self.soup = BeautifulSoup(arc, "html.parser", from_encoding=["latin-1", "utf-8"])
            self.soup.data.replace_with(str(self.data))
            self.soup.data1.replace_with(str(self.data))
            self.soup.qtd_issue.replace_with(str(self.results['ENTREGUE']))
            self.soup.qtd_nodrops.replace_with(str(self.results['NAO QUEDA']))
            self.soup.qtd_cancels.replace_with(str(self.results['CANCELADO']))
            self.soup.qtd_cancelnodrop.replace_with(str(self.results['CANCELADO POR NAO QUEDA']))
            self.soup.nome.replace_with(str(self.nome_empresa))
            self.soup = self.soup.decode()



    def connect(self):  # conect
        self.con = smtplib.SMTP(self.host, self.port)
        self.con.login(self.user, self.password)


    def body(self):  # edita o email
        message = self.soup
        self.email_msg = MIMEMultipart()
        self.email_msg['From'] = self.user
        self.email_msg['To'] = ','.join(
                self.emails)  # o problema de passar emails em lista é o cabeçalho que só aceita strings
        self.email_msg['Subject'] = f'RELATORIO ESPECIAL DE FLUXO - {self.nome_empresa} - {self.data}'
        self.email_msg.attach(MIMEText(message, 'html'))


    def send(self):  # envia o email
        filename = self.relat_name  # falta renomear o arquivo automaticamente
        attachment = open(self.relat_name, 'rb')  # falta renomear o arquivo automaticamente
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        self.email_msg.attach(part)
        attachment.close()

        de = self.email_msg['From']
        to = self.emails
        self.con.sendmail(de, to, self.email_msg.as_string())
        self.con.quit()


class SendMailTread(SendMail):
    def __init__(self, emails, data, results, nome_empresa, relat_name):
        super().__init__(emails, data, results, nome_empresa, relat_name)
        self.Change_html()
        self.connect()
        self.body()
        self.send()