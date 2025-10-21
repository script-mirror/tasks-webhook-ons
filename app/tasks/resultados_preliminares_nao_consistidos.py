import pandas as pd
from typing import Optional
from pathlib import Path
import pdb
import sys
import glob
import os
import re
import numpy as np
import datetime

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants, html_to_image
from middle.utils.file_manipulation import extract_zip
from middle.message.sender import send_email_message, send_whatsapp_message
from middle.airflow import trigger_dag
logger = setup_logger()
constants = Constants()


class ResultadosPreliminaresNaoConsistidos(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.gerador_tabela = GeradorTabela()
        self.trigger_dag = trigger_dag

        
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime.datetime] = None):
        logger.info("Iniciando workflow do produto Resultados Preliminares Nao Consistidos...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Resultados Preliminares Nao Consistidos finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Resultados Preliminares Nao Consistidos")
            raise
        
    def run_process(self, file_path):
        
        df_load, rev, assunto = self.process_file(file_path)
        
        self.gerador_tabela.run_process(df_load, rev, assunto)
        
        month_match = re.search(r'_(\d{6})_', os.path.basename(file_path))

        if month_match:
        
            payload = {key: self.payload.dict().get(key) for key in self.payload.dict().keys()}
            payload['sensibilidade'] = "NAO_CONSISTIDO"

            self.trigger_dag(dag_id="1.12-PROSPEC_CONSISTIDO", conf=payload)
    
    def process_file(self, file_path):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            
            unzip_path = extract_zip(file_path)
            logger.debug("Arquivo extraído para: %s", unzip_path)
            
            load_path = glob.glob(os.path.join(unzip_path, "*"))[0]
            
            files = os.listdir(load_path)
            
            for f in files:
                if '.xls' in f:
                    load_path = os.path.join(load_path, f)
                    break
            
            rev = file_path.split('_')[-1].split('.')[0]
            
            assunto = file_path.split('/')[-1].split('.')[0]
            
            configs = {}
            if rev == 'PMO':
                configs = {'sheet': 'Tab-5-6-7', 'rows': 10, 'skip': 3, 'x': 1}
            else:
                configs = {'sheet': 'REV-2', 'rows': 11, 'skip': 4, 'x': 2}
                
            df_load = pd.read_excel(load_path, sheet_name=configs['sheet'],nrows = configs['rows']-configs['skip']+1,skiprows=configs['skip'], header=None)
            
            df_load = df_load.replace(np.nan, '', regex=True)

            df_load.at[0, configs['x']] = str(df_load.loc[0][configs['x']].strftime('%d/%m/%Y'))
            df_load.at[2, configs['x']] = str(df_load.loc[2][configs['x']].strftime('%d/%m/%Y'))
            
            rev = file_path.split('_')[-1].split('.')[0]
            
            return [df_load, rev, assunto]
            
        except Exception as e:
            logger.error("Falha em processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        pass
    

class GeradorTabela:
    def __init__(self):
        self.user = constants.EMAIL_REV_ENA
        self.destinatarios_email = [constants.EMAIL_MIDDLE, constants.EMAIL_FRONT]
        self.destinatario_whatsapp = constants.WHATSAPP_PREMISSAS_PRECO
        
        
    def run_process(self, df_load: pd.DataFrame, rev: str, assunto: str):
        logger.info("Gerando tabela e enviando para os canais de comunicação...")
        try:

            html = self.gerar_html(df_load, rev)   

            self.enviar_email(html, assunto)

            self.enviar_whatsapp(html, assunto)
            
            logger.info("Finalizado o processo de geração e envio da tabela.")
            
        except Exception as e:
            logger.error("Erro no processamento de geracao da tabela: %s", str(e), exc_info=True)
            raise
        
        
    def gerar_html(self, df_load: pd.DataFrame, rev: str):
        try:
            
            rev = rev

            html = '''<html><head>'''
            html = html + ''' <style type="text/css">'''
            html = html + '''table {font-family: arial, sans-serif;border-collapse: collapse;width: 1000px;}'''
            html = html + '''td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}'''
            html = html + '''tr:nth-child(4n + 2) td, tr:nth-child(4n + 3) td {background-color: #F2ECEC;}'''
            html = html + '''.umaLinha tr:nth-child(odd) td{ background-color: #F2ECEC;}'''
            html = html + '''.umaLinha tr:nth-child(even) td{ background-color: #ffffff;}'''
            html = html + '''</style></head><body>'''
            html += '<table class="umaLinha" class="dataframe"><tbody><tr>'
            html += '<th rowspan="2">REGIOES</th>'

            if rev == 'PMO':
                html += '<th colspan="2">'+df_load.loc[0][1]+'<br> a <br>'+df_load.loc[2][1]+'</th>'
                html += '<th colspan="2">Previsao<br>Mensal Inicial<br>'+df_load.loc[2][3]+'</th></tr>'
                html += '<tr><td>(Mwmed)</td><td>% MLT</td><td>(Mwmed)</td><td>MLT%</td></tr>'
            else:
                html += '<th colspan="2">'+df_load.loc[0][2]+'<br> a <br>'+df_load.loc[2][2]+'</th>'
                html += '<th colspan="2">Revisao da<br>Previsao Mensal<br>Incluindo Verificado</th>'
                html += '<th colspan="2">Previsao<br>Mensal Inicial<br>'+df_load.loc[2][6]+'</th></tr>'
                html += '<tr><td>(Mwmed)</td><td>% MLT</td><td>(Mwmed)</td><td>MLT%</td><td>(Mwmed)</td><td>% MLT</td></tr>'


            for i,row in df_load.iterrows():
                if i >= 4:
                    html += '<tr>'
                    if rev == 'PMO':
                        html += '<td>{}</td>'.format(row[0])
                        html += '<td>{:6.0f}</td>'.format(row[1])
                        html += '<td>{:6.0f}</td>'.format(row[2])
                        html += '<td>{:6.0f}</td>'.format(row[3])
                        html += '<td>{:6.0f}</td>'.format(row[4])
                    else:
                        html += '<td>{}</td>'.format(row[1])
                        html += '<td>{:6.0f}</td>'.format(row[2])
                        html += '<td>{:6.0f}</td>'.format(row[3])
                        html += '<td>{:6.0f}</td>'.format(row[4])
                        html += '<td>{:6.0f}</td>'.format(row[5])
                        html += '<td>{:6.0f}</td>'.format(row[6])
                        html += '<td>{:6.0f}</td>'.format(row[7])

                    html += '</tr>'

            html = html + '</tbody></table></body> </html>'

            return html
        
        except Exception as e:
            logger.error("Falha ao gerar HTML: %s", str(e), exc_info=True)
            raise

    def enviar_email(self, html: str, assunto: str):
        try:
            
            send_email_message(
                user=self.user,
                destinatario=self.destinatarios_email,
                mensagem=html,
                assunto=assunto,
                arquivos=[]
            )

            logger.info("E-mail enviado para %s com assunto '%s'.", self.destinatario, assunto)
            
        except Exception as e:
            logger.error("Falha ao enviar e-mail: %s", str(e), exc_info=True)
            raise
        
    def enviar_whatsapp(self, html: str, assunto: str):
        try:
            
            image_binary = html_to_image(html)

            send_whatsapp_message(destinatario=self.destinatario_whatsapp,mensagem=assunto,arquivo=image_binary)
            
        except Exception as e:
            logger.error("Falha ao enviar mensagem WhatsApp: %s", str(e), exc_info=True)
            raise
          
if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Resultados Preliminares Nao Consistidos...")
    try:
        payload = {
            "dataProduto": "20/09/2025 - 26/09/2025",
            "filename": "Nao_Consistido_202509_REV3.zip",
            "macroProcesso": "Programação da Operação",
            "nome": "Resultados preliminares não consistidos  (vazões semanais - PMO)",
            "periodicidade": "2025-09-20T00:00:00",
            "periodicidadeFinal": "2025-09-26T23:59:59",
            "processo": "Previsão de Vazões e Geração de Cenários - PMO",
            "s3Key": "webhooks/Resultados preliminares não consistidos  (vazões semanais - PMO)/68cc6cb792d5b4018cefcb5c_Nao_Consistido_202509_REV3.zip",
            "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy83OS9Qcm9kdXRvcy8yNDYvTmFvX0NvbnNpc3RpZG9fMjAyNTA5X1JFVjMuemlwIiwidXNlcm5hbWUiOiJnaWxzZXUubXVobGVuQHJhaXplbi5jb20iLCJub21lUHJvZHV0byI6IlJlc3VsdGFkb3MgcHJlbGltaW5hcmVzIG7Do28gY29uc2lzdGlkb3MgICh2YXrDtWVzIHNlbWFuYWlzIC0gUE1PKSIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzU4MzE0Mjc5LCJuYmYiOjE3NTgyMjc2Mzl9.0gWgGwt_9zqu5loodmFmYaLX-KvRRStLZ2Ou3yZw8wk",
            "webhookId": "68cc6cb792d5b4018cefcb5c"
        }
        
        payload = WebhookSintegreSchema(**payload)
        
        resultados_preliminares = ResultadosPreliminaresNaoConsistidos(payload)
        
        resultados_preliminares.run_workflow()

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Resultados Preliminares Nao Consistidos: %s", str(e), exc_info=True)
        raise