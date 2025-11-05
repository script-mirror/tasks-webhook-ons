import pandas as pd
from typing import Optional
from pathlib import Path
from datetime import datetime
import pdb
import sys
import os
import requests
import pdb
import sys
import os
import requests

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants
from middle.message.sender import send_email_message
from middle.utils import ( 
    get_auth_header
)
logger = setup_logger()
constants = Constants()


class VazoesDiariasPrevistasPDP(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
    
        self.gerador_tabela = GeradorTabela()
        self.headers = get_auth_header()
        
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        logger.info("Iniciando workflow do produto Precipitação por Satélite - ONS...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Precipitação por Satélite - ONS finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Precipitação por Satélite - ONS")
            raise
        
    def run_process(self, file_path):
        filename_splited = os.path.basename(file_path).split('_')
        diaPrevisao = int(filename_splited[3])
        mesPrevisao = int(filename_splited[4])
        anoPrevisao = int(filename_splited[5])
        dt_previsao = datetime(anoPrevisao, mesPrevisao, diaPrevisao)
        
        df_load = self.process_file(file_path, dt_previsao)
        
        self.post_data(df_load)
        
        self.gerador_tabela.run_process(dt_previsao)
        
        pass
    
    def process_file(self, file_path, dt_previsao):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            info_planilha = {
                'SUDESTE': {'sheet_name':'Diária_6', 'posicao_info':[168, 169]},
                'SUL': {'sheet_name':'Diária_7', 'posicao_info':[56, 57]},
                'NORDESTE': {'sheet_name':'Diária_7', 'posicao_info':[70, 71]},
                'NORTE': {'sheet_name':'Diária_7', 'posicao_info':[125, 126]},
            }
            
            cd_submercado = {'SUDESTE':1, 'SUL':2, 'NORDESTE':3, 'NORTE':4}
            
            values_to_insert = []
            for submercado in info_planilha.keys():
                df_load = pd.read_excel(file_path, sheet_name=info_planilha[submercado]['sheet_name'], skiprows=4)
                
                colunas = df_load.columns.tolist()
                
                index_submercado = df_load[df_load[colunas[4]]==submercado].index[0]
                
                df_load = df_load.loc[index_submercado+1:index_submercado+2][colunas[2:]]
                
                df_load.columns = pd.to_datetime(df_load.columns).strftime("%Y-%m-%d")
                
                df_load = df_load.T
                
                df_load = df_load.reset_index()

                df_load['cd_submercado'] = cd_submercado[submercado]
                
                df_load['dt_previsao'] = dt_previsao.strftime("%Y-%m-%d")
                
                df_load.columns.values[0] = 'dt_ref'
                
                df_load.columns.values[1] = 'vl_mwmed'
                
                df_load.columns.values[2] = 'vl_perc_mlt'
                
                df_load = df_load[['cd_submercado', 'dt_previsao', 'dt_ref', 'vl_mwmed', 'vl_perc_mlt']]
                
                values_to_insert.append(df_load)
                
            df_load = pd.concat(values_to_insert, ignore_index=True) 
            
            return df_load
            
        except Exception as e:
            logger.error("Falha em processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        try:
            res = requests.post(
                constants.POST_PREV_ENA,
                json=process_result.to_dict('records'),
                headers=self.headers
            )
            if res.status_code != 200:
                res.raise_for_status()
                    
        except Exception as e:
            raise
    

class GeradorTabela:
    def __init__(self):
        self.user = constants.EMAIL_REV_ENA
        self.destinatarios_email = [constants.EMAIL_MIDDLE, constants.EMAIL_FRONT]
        self.header = get_auth_header()
        pass
    
    def run_process(self, dt_previsao):
        html = self.gerar_html(dt_previsao)
        
        assunto = '[ENAS] DESSEM ONS - {}'.format((dt_previsao + datetime.timedelta(days=2)).strftime('%d/%m/%Y'))
        
        self.enviar_email(assunto, html)
        pass
    
    
    def gerar_html(self, dt_previsao):
        cd_submercado = {1:'Sudeste', 2:'Sul', 3:'Nordeste', 4:'Norte'}
        
        previsao_vazao = self._get_data(constants.GET_PREV_ENA, params={'dataProduto': dt_previsao})
        previsao_vazao['cd_submercado'] = previsao_vazao['cd_submercado'].replace(cd_submercado)
        previsao_vazao['dt_ref'] = pd.to_datetime(previsao_vazao['dt_ref'] , format="%Y-%m-%d")
        previsao_vazao['vl_perc_mlt'] = previsao_vazao['vl_perc_mlt'].round(1)
        
        html = ""
        style = """
            <style>
                table {
                    border-collapse: collapse;
                    width: 40%;
                }
                th, td {
                    border: 1px solid #ddd;
                    padding: 3px;
                    text-align: center;
                }
                th {
                    background-color: #f2f2f2;
                }
                .row-header {
                    background-color: rgb(172, 172, 172);
                }
                .row-mlt {
                    background-color: rgba(199, 222, 245, 0.432);
                }
                .row-transparent {
                    background-color: transparent;
                    border: none;
                }
                .legend-container {
                    display: flex;
                    align-items: flex-start;
                    gap: 20px;
                    margin-bottom: 15px;
                }
                .legend {
                    border: 1px solid #ddd;
                    display: flex;
                    flex-direction: row;
                    padding: 10px;
                    background-color: white;
                }
                .legend-item {
                    display: flex;
                    align-items: center;
                    margin: 5px 5px 5px 20px;
                }
                .legend-color {
                    width: 20px;
                    height: 20px;
                    margin-right: 10px;
                }
                .mwmed-color {
                    background-color: #f2f2f2;
                }
                .mlt-color {
                    background-color: rgba(199, 222, 245, 0.432);
                }
            </style>
        """
        
        html_title = '<div class="legend-container"><div><h4>Relatório de Previsão de Vazões Diárias</h4>'
        html_title += '<p>ENAS Diárias ONS - DESSEM</p></div>'
        html_title += '<div class="legend"><h4>Legenda:</h4>'
        html_title += '<div class="legend-item"><div class="legend-color mwmed-color"></div><span>MWMed</span></div>'
        html_title += '<div class="legend-item"><div class="legend-color mlt-color"></div><span>% MLT</span></div>'
        html_title += '</div></div><br>'
        
        html_table_header = '<table><thead>'
        html_table_header += f'<tr><th class="row-transparent"></th><th class="row-header" colspan="4">Submercado</th><tr>'
        
        html_table_header += '<tr><th class="row-header"> Data Referente</th>'
        
        for submercado in previsao_vazao['cd_submercado'].unique():
            html_table_header += f'<th>{submercado}</th>'
        
        html_table_header += '</tr></thead>'
        
        html_table_body = '<tbody>'
        
        for dt_referente in previsao_vazao['dt_ref'].unique():
            html_table_body += f'<tr><th rowspan="2">{dt_referente.strftime("%d/%m/%Y")}</th>'
            
            for vl_mwmed in previsao_vazao.loc[previsao_vazao['dt_ref']==dt_referente, 'vl_mwmed']:
                html_table_body += f'<td>{vl_mwmed}</td>'
                
            
            html_table_body += '</tr><tr class="row-mlt">'
            
            for vl_perc_mlt in previsao_vazao.loc[previsao_vazao['dt_ref']==dt_referente, 'vl_perc_mlt']:
                html_table_body += f'<td>{vl_perc_mlt}</td>'
                
            html_table_body += '</tr>'
            
        html_table_body += '</tbody></table>'
        
        html_credits = '<br><br><p>Envio por WX</p>'
        
        html += style + html_title + html_table_header + html_table_body + html_credits
        
        return html
    
    def enviar_email(self, assunto: str, html: str):
        try:
            request = send_email_message(
                user=self.user,
                destinatario=self.destinatarios_email,
                mensagem=html,
                assunto=assunto,
                arquivos=[]
            )
            
            if not (200 <= request.status_code < 300):
                raise Exception(f"Falha ao enviar email. Status Code: {request.status_code}, Response: {request.text}")
            
        except Exception as e:
            logger.error("Erro ao enviar email: %s", str(e), exc_info=True)
            raise
        
    def _get_data(self, url_in: str, params:dict={}) -> pd.DataFrame:
        try:
            res = requests.get(
                url = url_in,
                params=params,
                headers=self.header
            )
            if res.status_code != 200:
                logger.error(f"Erro na requisição à API: status {res.status_code}, response: {res.text}")
                res.raise_for_status()
            logger.debug("Dados da API obtidos com sucesso")
            return pd.DataFrame(res.json())
        except requests.RequestException as e:
            logger.error(f"Erro ao acessar a API: {str(e)}")
            raise
    


if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Precipitação por Satélite - ONS...")
    try:
        payload = {
  "dataProduto": "07/11/2025",
  "filename": "Relatorio_previsao_diaria_05_11_2025_para_07_11_2025.xls",
  "macroProcesso": "Programação da Operação",
  "nome": "Relatório dos resultados finais consistidos da previsão diária (PDP)",
  "periodicidade": "2025-11-07T00:00:00",
  "periodicidadeFinal": "2025-11-07T23:59:59",
  "processo": "Previsão de Vazões Diárias - PDP",
  "s3Key": "webhooks/Relatório dos resultados finais consistidos da previsão diária (PDP)/02e099e2-7194-4c48-854e-fa77c675e1a8_Relatorio_previsao_diaria_05_11_2025_para_07_11_2025.xls",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy84Mi9Qcm9kdXRvcy81NDgvUmVsYXRvcmlvX3ByZXZpc2FvX2RpYXJpYV8wNV8xMV8yMDI1X3BhcmFfMDdfMTFfMjAyNS54bHMiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUmVsYXTDs3JpbyBkb3MgcmVzdWx0YWRvcyBmaW5haXMgY29uc2lzdGlkb3MgZGEgcHJldmlzw6NvIGRpw6FyaWEgKFBEUCkiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc2MjQ1NDcyMSwibmJmIjoxNzYyMzY4MDgxfQ.0j8n82VFzAW3B9siALLYBmwubVwWbbTVtcHAwz4AE9Q",
  "webhookId": "02e099e2-7194-4c48-854e-fa77c675e1a8"
}
        
        payload = WebhookSintegreSchema(**payload)
        
        vazoes_diarias = VazoesDiariasPrevistasPDP(payload)
        
        vazoes_diarias.run_workflow()
        

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Precipitação por Satélite - ONS: %s", str(e), exc_info=True)
        raise