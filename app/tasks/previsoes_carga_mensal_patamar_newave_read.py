import sys
import os
import requests
import pandas as pd
import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.schema import WebhookSintegreSchema  # noqa: E402
from middle.utils import setup_logger, Constants,HtmlBuilder, get_auth_header, sanitize_string  # noqa: E402
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from middle.utils.file_manipulation import extract_zip
from middle.s3 import (handle_webhook_file, get_latest_webhook_product,)

logger = setup_logger()
constants = Constants()

class CargaPatamarNewave(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.update_nw = UpdateSistemaCadic
        self.gerar_tabela = GerarTabelaDiferenca
        self.trigger_dag = TriggerDagExterna
        self.url_post = constants.BASE_URL + '/api/v2/decks/newave/previsoes-cargas'
        
        logger.info("Initialized CargaPatamarNewave with payload: %s", payload)
    
    def run_workflow(self):
        logger.info("Starting run_workflow for CargaPatamarNewave")
        try:
            file_path = self.download_extract_files()
            
            self.run_process(self, file_path)
                        
            logger.info("Workflow do produto CargaPatamarNewave terminado com sucesso!")
            
        except Exception as e:
            logger.error("Erro no fluxo de processamento das Previsões de Carga Mensal por Patamar do Newave: %s", str(e), exc_info=True)
            raise
        
    def run_process(self, file_path):
        
        process_result = self.process_file(file_path)
        self.post_database(process_result)
        
        self.update_nw.run_process()
            
        self.gerar_tabela.run_process()
            
        self.trigger_dag.run_process()
        
        
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

    def post_database(self, process_result: pd.DataFrame) -> dict:
        logger.info("Posting load data to database, rows: %d", len(process_result))
        try:
            res = requests.post(

                json=process_result.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                logger.error("Failed to post data to database: status %d, response: %s",
                           res.status_code, res.text)
                res.raise_for_status()
            logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return
        
        except Exception as e:
            logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise
            
                   
class UpdateSistemaCadic():
    def __init__(self):
        self.consts = Constants()
        self.header = get_auth_header()
        self.url_previsoes_carga = self.consts.POST_NEWAVE_PREVISOES_CARGAS
        self.url_mmgd_base = self.consts.BASE_URL + '/api/v2/decks/newave/cadic/total_mmgd_base'
        self.url_mmgd_total = self.consts.BASE_URL + '/api/v2/decks/newave/sistema/mmgd_total'
        
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
    
    def _put_data(self, produto: str, data_in: pd.DataFrame ):
        res = requests.get(self.base_url_api + produto,
                           json=data_in.to_dict('records'),
                           headers=self.header)
        if res.status_code != 200:
            res.raise_for_status()
        return  res  

class GerarTabelaDiferenca():
    def __init__(self):
        self.consts = Constants()
        self.header = get_auth_header()
        self.url_unsi          = self.consts.BASE_URL + '/api/v2/decks/newave/sistema/total_unsi'
        self.url_carga_global  = self.consts.BASE_URL + '/api/v2/decks/newave/sistema/cargas/total_carga_global'
        self.url_carga_liquida = self.consts.BASE_URL + '/api/v2/decks/newave/sistema/cargas/total_carga_liquida'
        self.url_unsi          = self.consts.BASE_URL + '/api/v2/decks/newave/sistema/total_unsi'
        self.url_mmgd_base     = self.consts.BASE_URL + '/api/v2/decks/newave/cadic/total_mmgd_base'
        self.url_ande          = self.consts.BASE_URL + '/api/v2/decks/newave/cadic/total_ande'
        
        logger.info("Initialized generate table" )
    
    def run_process(self):
        self.generate_table()
    
    
    def generate_table(self):        
        dados = {
        'dados_unsi':self.get_data(self.url_unsi),
        'dados_ande': self.get_data(self.url_ande),
        'dados_mmgd_total': self.get_data(self.url_mmgd_base),
        'dados_carga_global': self.get_data(self.url_carga_global),
        'dados_carga_liquida': self.get_data(self.url_carga_liquida)
            }
        
        html_tabela_diferenca = HtmlBuilder.gerar_html(
                'diferenca_cargas', 
                dados
            )
       
        print(html_tabela_diferenca)
              
        
    def get_data(self, url: str) -> pd.DataFrame:
        res = requests.get(url, headers=self.header)
        if res.status_code != 200:
            res.raise_for_status()
        return res.json()
    
class TriggerDagExterna():
    pass




if __name__ == '__main__':
    logger.info("Starting CargaPatamarNewave script execution")
    try:
        
        payload = {}
        
        payload = WebhookSintegreSchema(**payload)
        
        previsoescarga = CargaPatamarNewave({})
        previsoescarga.run_workflow()
        
        logger.info("Script execution completed successfully")
    except Exception as e:
        logger.error("Script execution failed: %s", str(e), exc_info=True)
        raise