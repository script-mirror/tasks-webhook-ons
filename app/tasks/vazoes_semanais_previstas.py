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
import requests

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants, html_to_image, get_auth_header
from middle.utils.file_manipulation import extract_zip
from middle.utils.date_utils import SemanaOperativa
from middle.message.sender import send_email_message, send_whatsapp_message
from middle.airflow import trigger_dag
logger = setup_logger()
constants = Constants()


class VazoesSemanaisPrevistasPMO(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.gerador_tabela = GeradorTabela()
        self.trigger_dag = trigger_dag

        
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        logger.info("Iniciando workflow dos produtos de vazão semanal prevista...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow dos produtos de vazão semanal prevista finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow dos produtos de vazão semanal prevista")
            raise
        
    def run_process(self, file_path):
        
        process_result = self.process_file(file_path)
        
        month_match = re.search(r'_(\d{6})_', os.path.basename(file_path))
        if not month_match:
            raise ValueError("Não foi possível extrair mês do nome do arquivo.")

        if 'Nao_Consistido' in file_path:
            month_match = re.search(r'_(\d{6})_', os.path.basename(file_path))
            payload = {key: self.payload.dict().get(key) for key in self.payload.dict().keys()}
            payload['sensibilidade'] = "NAO_CONSISTIDO"
            
            rev = file_path.split('_')[-1].split('.')[0]
            assunto = file_path.split('/')[-1].split('.')[0]
            self.gerador_tabela.run_process(process_result, rev, assunto)
        else:
            self.post_data(process_result)

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

            if 'Nao_Consistido' in file_path:
                logger.info("Processando arquivo não consistido...")
                process_result = self._process_nao_consistido(file_path, load_path)
                
            else:
                logger.info("Processando arquivo consistido...")
                process_result = self._process_consistido(file_path, load_path)
                
            return process_result
            
        except Exception as e:
            logger.error("Falha em processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
    def _process_nao_consistido(self, file_path: str, load_path: str):
        configs = {}
        rev = file_path.split('_')[-1].split('.')[0]
        
        if rev == 'PMO':
            configs = {'sheet': 'Tab-5-6-7', 'rows': 10, 'skip': 3, 'x': 1}
        else:
            configs = {'sheet': 'REV-2', 'rows': 11, 'skip': 4, 'x': 2}
            
        df_load = pd.read_excel(load_path, sheet_name=configs['sheet'],nrows = configs['rows']-configs['skip']+1,skiprows=configs['skip'], header=None)
        df_load = df_load.replace(np.nan, '', regex=True)
        df_load.at[0, configs['x']] = str(df_load.loc[0][configs['x']].strftime('%d/%m/%Y'))
        df_load.at[2, configs['x']] = str(df_load.loc[2][configs['x']].strftime('%d/%m/%Y'))
        
        return df_load
    
    def _process_consistido(self, file_path: str, load_path: str):
        df_load = pd.ExcelFile(load_path)
        abas = df_load.sheet_names
        
        submercados = {
            'SUDESTE': '1',
            'SUL': '2',
            'NORDESTE': '3',
            'NORTE': '4',
        }
        
        if 'PMO' in file_path:
            aba_data = 'Capa'
            aba_NE = 'Tab-12'
            aba_S_NE_N = 'Tab-13-14-15'
        else:
            aba_data = 'REV-1'
            aba_NE = 'REV-5'
            aba_S_NE_N = 'REV-6'
            
        df_dt = df_load.parse(aba_data, header=None)
        dt = df_dt.dropna().values.tolist()[-2][0]
        
        if dt.weekday() != 5:
            dt = dt + datetime.timedelta(days=1)
            
        dados_rev = SemanaOperativa(dt.date())
        
        logger.info('{} (REV{})'.format(dados_rev.date.strftime('%m/%Y'), dados_rev.current_revision))
        
        df_SE = df_load.parse(aba_NE, header=None)
        df_S_NE_N = df_load.parse(aba_S_NE_N, header=None)  
        
        df_SE[0] = df_SE[0].str.upper()
        df_S_NE_N[0] = df_S_NE_N[0].str.upper()

        bacias_NE = df_SE[0].unique().tolist()
        bacias_S_NE_N = df_S_NE_N[0].unique().tolist()

        bacias_porcentagem = [bacia for bacia in bacias_NE if isinstance(bacia, str) and bacia[-1] == '%']
        bacias_porcentagem += [bacia for bacia in bacias_S_NE_N if isinstance(bacia, str) and bacia[-1] == '%']

        for sub in list(submercados.keys()):
            bacias_porcentagem.remove('{} %'.format(sub))

        bacias = [bacia[:-2] for bacia in bacias_porcentagem]

        df_ena_submercado = df_SE[df_SE[0].isin(submercados)].copy()
        df_ena_submercado = pd.concat([df_ena_submercado,df_S_NE_N[df_S_NE_N[0].isin(submercados)].copy()])
        df_ena_submercado[0] = df_ena_submercado[0].replace(submercados)
        
        
        df_ena_bacia = df_SE[df_SE[0].isin(bacias)].copy()
        df_ena_bacia = pd.concat([df_ena_bacia,df_S_NE_N[df_S_NE_N[0].isin(bacias)].copy()])

        bacias_segmentadas = requests.get(
                constants.GET_ONS_BACIAS_SEGMENTADAS,
                headers=get_auth_header()
        ).json()

        bacias = {}
        for bacia in bacias_segmentadas:
            bacias[bacia['str_bacia']] = bacia['cd_bacia']
        bacias['PARANAPANEMA (SE)'] = bacias['PARANAPANEMA']

        df_ena_bacia[0] = df_ena_bacia[0].replace(bacias)


        df_ena_bacia_porcentagem = df_SE[df_SE[0].isin(bacias_porcentagem)].copy()
        df_ena_bacia_porcentagem = pd.concat([df_ena_bacia_porcentagem,df_S_NE_N[df_S_NE_N[0].isin(bacias_porcentagem)].copy()])
        df_ena_bacia_porcentagem[0] = df_ena_bacia_porcentagem[0].str[:-2]
        df_ena_bacia_porcentagem[0] = df_ena_bacia_porcentagem[0].replace(bacias)
        
        num_semanas = 6
        col_inicial = 3
        dt_format = "%Y-%m-%d"
        
        valores_ena_submercado = []
        for index, row in df_ena_submercado.iterrows():
            for sem in range(0, num_semanas):
                dt = dados_rev.first_day_of_month + datetime.timedelta(days=sem*7)
                valores_ena_submercado.append((dados_rev.ref_year, dados_rev.ref_month, dados_rev.current_revision, row[0], dt.strftime(dt_format), round(row[sem+col_inicial])))
        
        valores_ena_bacia = []
        for index, row in df_ena_bacia.iterrows():
            for sem in range(0, num_semanas):
                dt = dados_rev.first_day_of_month + datetime.timedelta(days=sem*7)
                valor_numerico = round(row[sem+col_inicial])
                valorPercent = round(df_ena_bacia_porcentagem[df_ena_bacia_porcentagem[0] == row[0]][sem+col_inicial].item(),2)
                valores_ena_bacia.append((dados_rev.ref_year, dados_rev.ref_month, dados_rev.current_revision, row[0], dt.strftime(dt_format), valor_numerico, valorPercent))
        
        colunas_tb_ve = ['vl_ano', 'vl_mes', 'cd_revisao', 'cd_submercado', 'dt_inicio_semana', 'vl_ena']
        df_load_tb_ve = pd.DataFrame(valores_ena_submercado, columns=colunas_tb_ve)
        
        colunas_tb_ve_bacias = ['vl_ano', 'vl_mes', 'cd_revisao', 'cd_bacia', 'dt_inicio_semana', 'vl_ena', 'vl_perc_mlt']
        df_load_tb_ve_bacias = pd.DataFrame(valores_ena_bacia, columns=colunas_tb_ve_bacias)

        dict_load = {
            'df_load_tb_ve': df_load_tb_ve,
            'df_load_tb_ve_bacias': df_load_tb_ve_bacias
        }
        
        return dict_load
        
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        try:
            logger.info("Enviando dados processados para o banco...")
            
            df_tb_ve = process_result['df_load_tb_ve']
            df_tb_ve_bacias = process_result['df_load_tb_ve_bacias']    
            
            res = requests.post(
                constants.POST_PREV_SEMANAL_ENA,   
                headers=get_auth_header(),
                json=df_tb_ve.to_dict(orient='records')
            )
            res.raise_for_status()
            if res.status_code == 200:
                logger.info("Dados da tabela TB_VE enviados com sucesso. Status Code: %s", res.status_code)
            else:
                raise Exception("Falha ao enviar dados da tabela TB_VE. Status Code: {}".format(res.status_code)) 
            
            
            res_bacias = requests.post(
                constants.POST_PREV_SEMANAL_ENA_POR_BACIA,   
                headers=get_auth_header(),          
                json=df_tb_ve_bacias.to_dict(orient='records')
            )
            res_bacias.raise_for_status()
            if res_bacias.status_code == 200:
                logger.info("Dados da tabela TB_VE_BACIAS enviados com sucesso. Status Code: %s", res_bacias.status_code)
            else:
                raise Exception("Falha ao enviar dados da tabela TB_VE_BACIAS. Status Code: {}".format(res_bacias.status_code))
            
        
            
        except Exception as e:
            logger.error("Falha ao enviar dados para o banco: %s", str(e), exc_info=True)
            raise    
        
        
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
    logger.info("Iniciando manualmente o workflow dos produtos de vazão semanal prevista...")
    try:
        payload = {
  "dataProduto": "25/10/2025 - 31/10/2025",
  "filename": "Consistido_202510_REV4.zip",
  "macroProcesso": "Programação da Operação",
  "nome": "Resultados preliminares consistidos (vazões semanais - PMO)",
  "periodicidade": "2025-10-25T00:00:00",
  "periodicidadeFinal": "2025-10-31T23:59:59",
  "processo": "Previsão de Vazões e Geração de Cenários - PMO",
  "s3Key": "webhooks/Resultados preliminares consistidos (vazões semanais - PMO)/3262c74b-3a97-456b-8cbf-17ed8ede725a_Consistido_202510_REV4.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy83OS9Qcm9kdXRvcy8yNDUvQ29uc2lzdGlkb18yMDI1MTBfUkVWNC56aXAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUmVzdWx0YWRvcyBwcmVsaW1pbmFyZXMgY29uc2lzdGlkb3MgKHZhesO1ZXMgc2VtYW5haXMgLSBQTU8pIiwiSXNGaWxlIjoiVHJ1ZSIsImlzcyI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiYXVkIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJleHAiOjE3NjEzNDE1NTMsIm5iZiI6MTc2MTI1NDkxM30.kw2CjN7KqBm9CJ_SImJ_Lvu98-2lah0c75L4ytT1NcA",
  "webhookId": "3262c74b-3a97-456b-8cbf-17ed8ede725a"
}

# {
#   "dataProduto": "27/09/2025 - 03/10/2025",
#   "filename": "Consistido_202510_PMO.zip",
#   "macroProcesso": "Programação da Operação",
#   "nome": "Resultados preliminares consistidos (vazões semanais - PMO)",
#   "periodicidade": "2025-09-27T00:00:00",
#   "periodicidadeFinal": "2025-10-03T23:59:59",
#   "processo": "Previsão de Vazões e Geração de Cenários - PMO",
#   "s3Key": "webhooks/Resultados preliminares consistidos (vazões semanais - PMO)/68d5c041b128ca9a97ee5e68_Consistido_202510_PMO.zip",
#   "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy83OS9Qcm9kdXRvcy8yNDUvQ29uc2lzdGlkb18yMDI1MTBfUE1PLnppcCIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJSZXN1bHRhZG9zIHByZWxpbWluYXJlcyBjb25zaXN0aWRvcyAodmF6w7VlcyBzZW1hbmFpcyAtIFBNTykiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1ODkyNTQ4OSwibmJmIjoxNzU4ODM4ODQ5fQ.MPZqzCw-0aDe-_MogGKXaw6lYMRlVHeMs6WY686iLmU",
#   "webhookId": "68d5c041b128ca9a97ee5e68"
# }
        
        payload = WebhookSintegreSchema(**payload)
        
        vazoes_semanais = VazoesSemanaisPrevistasPMO(payload)
        
        vazoes_semanais.run_workflow()

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento dos produtos de vazão semanal prevista: %s", str(e), exc_info=True)
        raise