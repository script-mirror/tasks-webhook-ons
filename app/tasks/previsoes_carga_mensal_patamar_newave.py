import sys
import os
import requests
import pandas as pd
import glob
import pdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.schema import WebhookSintegreSchema
from middle.utils import setup_logger, Constants,HtmlBuilder, get_auth_header
from app.webhook_products_interface import WebhookProductsInterface
from middle.utils.file_manipulation import extract_zip
from middle.message.sender import send_whatsapp_message

logger = setup_logger()
constants = Constants()

class CargaPatamarNewave(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.post_previsoes_carga = constants.POST_NEWAVE_PREVISOES_CARGAS
        self.update_nw = UpdateSistemaCadic()
        self.gerar_tabela = GerarTabelaDiferenca()
        self.gerar_dat = GerarCadicSistema()  
        self.trigger_dag = TriggerDagExterna()
        logger.info("Initialized CargaPatamarNewave with payload: %s", payload)
    
    
    def run_workflow(self):
        logger.info("Iniciando workflow do produto Previsões de Carga Mensal e por Patamar...")
        try:
            file_path = self.download_extract_files()
            
            self.run_process(file_path)
                        
            logger.info("Workflow do produto Previsões de Carga Mensal e por Patamar finalizado com sucesso!")
        except Exception as e:
            logger.error("Erro no fluxo de processamento das Previsões de Carga Mensal por Patamar do Newave: %s", str(e), exc_info=True)
            raise
        
    def run_process(self, file_path):
        
        process_result = self.process_file(file_path)
        
        self.post_data(process_result)
        
        # self.gerar_dat.run_process(process_result)
        
        self.update_nw.run_process()
            
        self.gerar_tabela.run_process()
            
        # self.trigger_dag.run_process()
        
        
    def process_file(self, file_path) -> pd.DataFrame:
        logger.info("Reading week load data from base path: %s", file_path)
        try:
            MAP_COLUMNS = {
                'DATE':       'data_referente',
                'SOURCE':     'submercado',
                'LOAD_sMMGD': 'vl_carga',
                'LOAD_cMMGD': 'vl_carga_global',
                'TYPE':        'patamar',
                'GAUGE':       'horas',
                'Exp_CGH':     'vl_exp_pch_mmgd',
                'Exp_EOL':     'vl_exp_eol_mmgd',
                'Exp_UFV':     'vl_exp_ufv_mmgd',
                'Exp_UTE':     'vl_exp_pct_mmgd', 
                'Base_CGH':    'vl_base_pch_mmgd',
                'Base_EOL':    'vl_base_eol_mmgd',
                'Base_UFV':    'vl_base_ufv_mmgd',
                'Base_UTE':    'vl_base_pct_mmgd',
                'Base_MMGD':   'vl_base_total_mmgd',
                'Exp_MMGD':    'vl_exp_total_mmgd',
                'REVISION':    'data_revisao'}
            
            MAP_PAT = {'MIDDLE': 'medio','LOW':'leve','HIGH':'pesada', 'MEDIUM': 'media' }
            MAP_SS  = {'SUDESTE': 'SE', 'SUL':'S','NORDESTE':'NE', 'NORTE':'N'}
            LOAD_QUADRI = False
            
            unzip_path = extract_zip(file_path)
            logger.debug("Extracted zip file to: %s", unzip_path)
            
            load_path = glob.glob(os.path.join(unzip_path, '*xlsx'))[0]
            logger.info("Found file: %s",load_path)
            
            if 'quad' in os.path.basename(load_path): 
                LOAD_QUADRI= True
            
            df_load = pd.read_excel(load_path, dtype={'DATE': str, 'REVISION': str})
            
            logger.debug("Read weekly load data with %d rows", len(df_load))
            df_load = df_load.drop('WEEK', axis=1)
            df_load = df_load.rename(columns=MAP_COLUMNS)
            df_load['patamar'] = df_load['patamar'].replace(MAP_PAT)
            df_load['submercado'] = df_load['submercado'].replace(MAP_SS)
            
            delta_days = 0
            if 'quad' in os.path.basename(load_path): 
                LOAD_QUADRI= True
                delta_days = 15
            
            date = pd.to_datetime(df_load['data_revisao'].unique()[0]) + timedelta(days=delta_days)
            date_produto = (date + relativedelta(months=1)).replace(day=1).strftime('%Y-%m-%d')
            df_load['data_produto'] = date_produto
            df_load['quadrimestral'] = LOAD_QUADRI
            
            logger.info("Successfully processed load data with %d rows", len(df_load))
            
            return df_load
        
        except Exception as e:
            logger.error("Failed to read week load data: %s", str(e), exc_info=True)
            raise

    def post_data(self, process_result: pd.DataFrame) -> dict:
        logger.info("Inserindo valores de vazão observado do produto Previsões de Carga Mensal por Patamar do Newave. Qntd de linhas inseridas: %d", len(process_result))
        try:
            res = requests.post(
                url=self.post_previsoes_carga,
                json=process_result.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                logger.error("Failed to post data to database: status %d, response: %s",
                           res.status_code, res.text)
                res.raise_for_status()
            logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return {"msg": "Dados de carga previstas enviadas com sucesso!"}
        
        except Exception as e:
            logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise
            
                   
class UpdateSistemaCadic():
    def __init__(self):
        self.consts = Constants()
        self.header = get_auth_header()
        self.url_previsoes_carga = self.consts.GET_NEWAVE_PREVISOES_CARGAS
        self.url_mmgd_base = self.consts.PUT_NEWAVE_CADIC_TOTAL_MMGD_BASE
        self.url_mmgd_total = self.consts.PUT_NEWAVE_SISTEMA_MMGD_TOTAL
        
        logger.info("Initialized update sistema and c_adic" )
    
    def run_process(self):
        self.update_cadic()
        self.update_sistema()
        
        
    def update_sistema(self):
        df_data = self.get_data(self.url_previsoes_carga)
        self.put_data(self.url_mmgd_total, df_data )
        pass
        
    def update_cadic(self):
        df_data = self.get_data(self.url_previsoes_carga)
        self.put_data(self.url_mmgd_base, df_data )
        pass
        
    def get_data(self, produto: str) -> pd.DataFrame:
        res = requests.get(self.base_url_api + produto, headers=self.header)
        if res.status_code != 200:
            res.raise_for_status()
        return pd.DataFrame(res.json())   
    
    def put_data(self, produto: str, data_in: pd.DataFrame ):
        res = requests.get(self.base_url_api + produto,
                           json=data_in.to_dict('records'),
                           headers=self.header)
        if res.status_code != 200:
            res.raise_for_status()
        return  res  

class GerarCadicSistema():
    def __init__(self):
       self.consts = Constants()
       
    def run_process(self, process_result):
        self.gerar_cadic(process_result)
        self.gerar_sistema(process_result)
    
    def gerar_cadic(self,process_result):
        cadic_values = process_result
        pdb.set_trace()
        df_cadic = cadic_values[['vl_ano', 'vl_mes','submercado', 'vl_base_total_mmgd']]
        
        pass
    
    def gerar_sistema(self,process_result):
        sistema_values = process_result
        
        df_sist = sistema_values[['vl_ano', 'vl_mes','SOURCE', 'LOAD_sMMGD', 'Exp_CGH', 'Exp_EOL', 'Exp_UFV', 'Exp_UTE']]
        
        df_sist.rename(columns={
                'SOURCE': 'cd_submercado',
                'LOAD_sMMGD': 'vl_energia_total',
                'Exp_CGH': 'vl_geracao_pch_mmgd',
                'Exp_EOL': 'vl_geracao_eol_mmgd',
                'Exp_UFV': 'vl_geracao_ufv_mmgd',
                'Exp_UTE': 'vl_geracao_pct_mmgd'
            }, inplace=True)
        pass 

class GerarTabelaDiferenca():
    def __init__(self):
        self.consts = Constants()
        self.header = get_auth_header()
        self.url_html_to_image = self.consts.URL_HTML_TO_IMAGE
        self.url_carga_global  = self.consts.GET_NEWAVE_SISTEMA_CARGAS_TOTAL_CARGA_GLOBAL
        self.url_carga_liquida = self.consts.GET_NEWAVE_SISTEMA_CARGAS_TOTAL_CARGA_LIQUIDA
        self.url_unsi          = self.consts.GET_NEWAVE_SISTEMA_TOTAL_UNSI
        self.url_mmgd_total    = self.consts.GET_NEWAVE_SISTEMA_MMGD_TOTAL
        self.url_ande          = self.consts.GET_NEWAVE_CADIC_TOTAL_ANDE
        
        logger.info("Initialized generate table" )
    
    def run_process(self):
        html = self.generate_table()
        image_path = self.transform_html_to_image(html)
        self.enviar_tabela_whatsapp_email(image_path)
        
    def generate_table(self):        
        dados = {
            'dados_unsi':self.get_data(self.url_unsi),
            'dados_ande': self.get_data(self.url_ande),
            'dados_mmgd_total': self.get_data(self.url_mmgd_total),
            'dados_carga_global': self.get_data(self.url_carga_global),
            'dados_carga_liquida': self.get_data(self.url_carga_liquida)
        }
        
        html_tabela_diferenca = HtmlBuilder.gerar_html(
            'diferenca_cargas', 
            dados
        )
       
        print(html_tabela_diferenca)
        
        return html_tabela_diferenca
    
    def transform_html_to_image(self,html):
        
        api_html_payload = {
                "html": html,
                "options": {
                  "type": "png",
                  "quality": 100,
                  "trim": True,
                  "deviceScaleFactor": 2
                }
        }
            
        html_api_endpoint = f"{self.url_html_to_image}/convert"
        
        request_html_api = requests.post(
            html_api_endpoint,
            headers=self.headers,
            json=api_html_payload,  
        )
        
        if request_html_api.status_code < 200 or request_html_api.status_code >= 300:
                raise ValueError(f"Erro ao converter HTML em imagem: {request_html_api.text}")
        
    def enviar_tabela_whatsapp_email(
        self,
        image_path,
    ):
        """
        Envia a tabela de diferença de cargas por WhatsApp e email.
        
        :return: Dicionário com o status e mensagem do envio.
        """
        try:
            logger.info("Enviando tabela de diferença de cargas por WhatsApp e email...")
            
            data_produto_str = self.dataProduto
            versao = "preliminar"
            
            image_path = image_path
            if not image_path or not os.path.exists(image_path):
                raise ValueError(f"Arquivo de imagem não encontrado: {image_path}")
            
            if 'quad' in self.filename:
                msg_whatsapp = f"Diferença de Cargas NEWAVE {versao} (Quadrimestral x Definitivo anterior) - {data_produto_str}"
            else:
                msg_whatsapp = f"Diferença de Cargas NEWAVE {versao} (Preliminar atual atualizado x Definitivo anterior) - {data_produto_str}"
            
            request_whatsapp = send_whatsapp_message(
                destinatario="Debug",
                mensagem=msg_whatsapp,
                arquivo=image_path,
            )
            
            if request_whatsapp.status_code < 200 or request_whatsapp.status_code >= 300:
                raise ValueError(f"Erro ao enviar mensagem por WhatsApp: {request_whatsapp.text}")
                    
        except Exception as e:
            logger.error(f"Erro ao enviar tabela por WhatsApp e email: {e}")
            raise 
        
    def get_data(self, url: str) -> pd.DataFrame:
        res = requests.get(url, headers=self.header)
        if res.status_code != 200:
            res.raise_for_status()
        return res.json()
    
class TriggerDagExterna():
    pass




if __name__ == '__main__':
    logger.info("Iniciando workflow do produto manualmente...")
    try:
        
        payload = {
            "dataProduto": "09/2025",
            "filename": "RV0_PMO_Setembro_2025_carga_mensal.zip",
            "macroProcesso": "Programação da Operação",
            "nome": "Previsões de carga mensal e por patamar - NEWAVE",
            "periodicidade": "2025-09-01T00:00:00",
            "periodicidadeFinal": "2025-09-30T23:59:59",
            "processo": "Previsão de Carga para o PMO",
            "s3Key": "webhooks/Previsões de carga mensal e por patamar - NEWAVE/68accd5eb790e437d652a03d_RV0_PMO_Setembro_2025_carga_mensal.zip",
            "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS80Ny9Qcm9kdXRvcy8yMjkvUlYwX1BNT19TZXRlbWJyb18yMDI1X2NhcmdhX21lbnNhbC56aXAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUHJldmlzw7VlcyBkZSBjYXJnYSBtZW5zYWwgZSBwb3IgcGF0YW1hciAtIE5FV0FWRSIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzU2MjQxODcwLCJuYmYiOjE3NTYxNTUyMzB9.azluL7uoHY-tRZ21TuXHFX3R9yNNOS1urVts6xgsghM",
            "webhookId": "68accd5eb790e437d652a03d"
        }
        
        payload = WebhookSintegreSchema(**payload)
        
        previsoescarga = CargaPatamarNewave(payload)
        previsoescarga.run_workflow()
        
    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Previsões de Carga Mensal por Patamar do Newave: %s", str(e), exc_info=True)
        raise