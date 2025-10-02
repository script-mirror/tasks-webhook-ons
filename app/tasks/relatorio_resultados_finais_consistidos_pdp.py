import pandas as pd
from typing import Optional
from pathlib import Path
import datetime
import pdb
import sys
import glob
import os
import requests

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants
from middle.utils.file_manipulation import extract_zip
from middle.utils import ( # noqa: E402
    get_auth_header
)
logger = setup_logger()
constants = Constants()


class RelatorioResultadosFinaisConsistidosPDP(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.gerador_tabela = GeradorTabela()
        self.headers = get_auth_header()
        self.consts = Constants()
        
        
    def run_workflow(self):
        logger.info("Iniciando workflow do produto Precipitação por Satélite - ONS...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Precipitação por Satélite - ONS finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Precipitação por Satélite - ONS")
            raise
        
    def run_process(self, file_path):
        
        df_load = self.process_file(file_path)
        
        self.post_data(df_load)
        
        # self.gerador_tabela.run_process(df_load)
        
        pass
    
    def process_file(self, file_path):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            filepath_splited = os.path.basename(file_path).split('_')
            dia_previsao = int(filepath_splited[3])
            mes_pevisao = int(filepath_splited[4])
            ano_previsao = int(filepath_splited[5])
            
            dt_previsao = datetime.datetime(ano_previsao, mes_pevisao, dia_previsao)
            
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
                self.consts.POST_PREV_ENA,
                json=process_result.to_dict('records'),
                headers=self.headers
            )
            if res.status_code != 200:
                res.raise_for_status()
                    
        except Exception as e:
            raise
    

class GeradorTabela:
    def __init__(self):
        pass
    
    def run_process(self):
        pass
    
    
    def gerar_html(self):
        pass
    
    def enviar_email(self, assunto: str, corpo: str, arquivos: list):
        pass
    
    
    


if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Precipitação por Satélite - ONS...")
    try:
        payload = {
  "dataProduto": "30/09/2025",
  "filename": "Relatorio_previsao_diaria_28_09_2025_para_30_09_2025.xls",
  "macroProcesso": "Programação da Operação",
  "nome": "Relatório dos resultados finais consistidos da previsão diária (PDP)",
  "periodicidade": "2025-09-30T00:00:00",
  "periodicidadeFinal": "2025-09-30T23:59:59",
  "processo": "Previsão de Vazões Diárias - PDP",
  "s3Key": "webhooks/Relatório dos resultados finais consistidos da previsão diária (PDP)/68d98616995e02e90c3fc514_Relatorio_previsao_diaria_28_09_2025_para_30_09_2025.xls",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy84Mi9Qcm9kdXRvcy81NDgvUmVsYXRvcmlvX3ByZXZpc2FvX2RpYXJpYV8yOF8wOV8yMDI1X3BhcmFfMzBfMDlfMjAyNS54bHMiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUmVsYXTDs3JpbyBkb3MgcmVzdWx0YWRvcyBmaW5haXMgY29uc2lzdGlkb3MgZGEgcHJldmlzw6NvIGRpw6FyaWEgKFBEUCkiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1OTE3Mjc0MiwibmJmIjoxNzU5MDg2MTAyfQ.sQMIZ57iv4NHLxfcWGTMA9Mnh_8yiiRxtbXF64qIc6Q",
  "webhookId": "68d98616995e02e90c3fc514"
}
        
        payload = WebhookSintegreSchema(**payload)
        
        relatorio_resultados = RelatorioResultadosFinaisConsistidosPDP(payload)
        
        relatorio_resultados.run_workflow()
        

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Precipitação por Satélite - ONS: %s", str(e), exc_info=True)
        raise