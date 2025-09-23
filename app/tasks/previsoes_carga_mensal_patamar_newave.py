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
from middle.utils import setup_logger, Constants, get_auth_header, html_to_image
from app.webhook_products_interface import WebhookProductsInterface
from middle.utils.file_manipulation import extract_zip
from middle.message.sender import send_whatsapp_message
from middle.airflow import trigger_dag

logger = setup_logger()
constants = Constants()

class CargaPatamarNewave(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.trigger_dag = trigger_dag   
        self.post_previsoes_carga = constants.POST_NEWAVE_PREVISOES_CARGAS
        self.dataProduto = payload.dataProduto
        self.filename = payload.filename
        self.update_deck_preliminar = UpdateSistemaCadic(self.dataProduto)
        self.gerar_deck_quad = GerarDeckQuadrimestral(self.dataProduto)
        self.gerar_tabela = GerarTabelaDiferenca()
        logger.info("Initialized CargaPatamarNewave with payload: %s", payload)
    
    
    def run_workflow(self):
        logger.info("Iniciando workflow do produto Previsões de Carga Mensal e por Patamar...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
                        
            logger.info("Workflow do produto Previsões de Carga Mensal e por Patamar finalizado com sucesso!")
        except Exception as e:
            logger.error("Erro no fluxo de processamento das Previsões de Carga Mensal por Patamar do Newave: %s", str(e), exc_info=True)
            raise
        
    def run_process(self, file_path):
        
        process_result = self.process_file(file_path)
        
        self.post_data(process_result)
        
        self.trigger_dag(dag_id="1.18-PROSPEC_UPDATE", conf={"produto": "CARGA-NEWAVE"})
        
        if 'quad' in file_path:
            self.gerar_deck_quad.run_process()
        else:
            self.update_deck_preliminar.run_process()
        
        self.gerar_tabela.run_workflow()
            
        
        
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
    def __init__(self,  dataProduto):
        self.dataProduto = dataProduto
        self.headers = get_auth_header()
        self.base_url_api = constants.BASE_URL
        self.url_previsoes_carga = constants.GET_NEWAVE_PREVISOES_CARGAS
        self.url_mmgd_base = constants.PUT_NEWAVE_CADIC_TOTAL_MMGD_BASE
        self.url_mmgd_total = constants.PUT_NEWAVE_SISTEMA_MMGD_TOTAL
        logger.info("Initialized update sistema and c_adic" )
    
    def run_process(self):
        df_data = self._get_data(self.url_previsoes_carga, self.dataProduto)
        
        self.update_sistema(df_data, self.dataProduto)
        self.update_cadic(df_data, self.dataProduto)
        
        
    def update_sistema(self, df_data, dataProduto):
        
        df_data = df_data[df_data['patamar'] == 'media']
        
        df_data['data_referente'] = pd.to_datetime(df_data['data_referente'])
        dataProduto = pd.to_datetime('01/' + dataProduto, format='%d/%m/%Y')
        df_data = df_data[df_data['data_referente'] >= dataProduto]
        
        submercado_maps = {
            "SE": 1,
            "S": 2,
            "NE": 3,
            "N": 4
        }
        
        df_sist_update = df_data[['submercado','data_referente', 'data_produto', 'vl_exp_pch_mmgd', 'vl_exp_pct_mmgd', 'vl_exp_eol_mmgd', 'vl_exp_ufv_mmgd']]
        df_sist_update = df_sist_update.rename(columns={
            'submercado': 'cd_submercado',
            'data_produto': 'dt_deck',
            'vl_exp_pch_mmgd': 'vl_geracao_pch_mmgd',
            'vl_exp_pct_mmgd': 'vl_geracao_pct_mmgd',
            'vl_exp_eol_mmgd': 'vl_geracao_eol_mmgd',
            'vl_exp_ufv_mmgd': 'vl_geracao_ufv_mmgd'
        })
        df_sist_update['data_referente'] = pd.to_datetime(df_sist_update['data_referente'])
        df_sist_update['vl_ano'] = df_sist_update['data_referente'].dt.year
        df_sist_update['vl_mes'] = df_sist_update['data_referente'].dt.month
        df_sist_update = df_sist_update.drop(columns=('data_referente'))
        df_sist_update['cd_submercado'] = df_sist_update['cd_submercado'].map(submercado_maps)
        
        df_sist_update = df_sist_update[['cd_submercado','dt_deck','vl_ano','vl_mes', 'vl_geracao_pch_mmgd','vl_geracao_pct_mmgd','vl_geracao_eol_mmgd','vl_geracao_ufv_mmgd']]
        df_sist_update.reset_index().drop(columns=['index'])
        
        self._put_data(self.url_mmgd_total, df_sist_update)
        pass
        
    def update_cadic(self, df_data, dataProduto):
        
        df_data = df_data[df_data['patamar'] == 'media']
        
        df_data['data_referente'] = pd.to_datetime(df_data['data_referente'])
        dataProduto = pd.to_datetime('01/' + dataProduto, format='%d/%m/%Y')
        df_data = df_data[df_data['data_referente'] >= dataProduto]
        
        df_cadic_update = df_data[['submercado','data_referente','data_produto',
                  'vl_base_pch_mmgd','vl_base_eol_mmgd','vl_base_ufv_mmgd','vl_base_pct_mmgd']].rename(
        columns={'submercado':'cd_submercado','data_produto':'dt_deck'}
        )

        dref = pd.to_datetime(df_cadic_update['data_referente'])
        df_cadic_update['vl_ano'] = dref.dt.year
        df_cadic_update['vl_mes'] = dref.dt.month
        df_cadic_update = df_cadic_update.drop(columns='data_referente')
    
        base_cols = ['vl_base_pch_mmgd','vl_base_eol_mmgd','vl_base_ufv_mmgd','vl_base_pct_mmgd']
        df_cadic_update['vl_mmgd_total'] = df_cadic_update[base_cols].sum(axis=1)
    
        agg = (df_cadic_update.groupby(['vl_ano','vl_mes','dt_deck','cd_submercado'], as_index=False)['vl_mmgd_total'].sum())
        df_cadic_update = (agg.pivot(index=['vl_ano','vl_mes','dt_deck'], columns='cd_submercado', values='vl_mmgd_total')
                   .fillna(0)
                   .rename(columns={'SE':'vl_mmgd_se','S':'vl_mmgd_s','NE':'vl_mmgd_ne','N':'vl_mmgd_n'})
                   .reset_index())
    
        for c in ['vl_mmgd_se','vl_mmgd_s','vl_mmgd_ne','vl_mmgd_n']:
            if c not in df_cadic_update: df_cadic_update[c] = 0.0
        df_cadic_update = df_cadic_update[['vl_ano','vl_mes','vl_mmgd_se','vl_mmgd_s','vl_mmgd_ne','vl_mmgd_n','dt_deck']]
        
        df_cadic_update.columns.name = None
        
        df_cadic_update = df_cadic_update[['vl_ano','vl_mes','dt_deck','vl_mmgd_se','vl_mmgd_s','vl_mmgd_ne','vl_mmgd_n']]
        
        self._put_data(self.url_mmgd_base, df_cadic_update)
        pass
        
    def _get_data(self, url: str, params) -> pd.DataFrame:
        res = requests.get(url, headers=self.headers, params = params)
        if res.status_code != 200:
            res.raise_for_status()
        return pd.DataFrame(res.json())   
    
    def _put_data(self, url: str, data_in: pd.DataFrame ):
        res = requests.put(url,
                           json=data_in.to_dict('records'),
                           headers=self.headers)
        if res.status_code != 200:
            res.raise_for_status()
        return  res  

class GerarDeckQuadrimestral():
    def __init__(self, dataProduto):
        self.dataProduto = dataProduto
        self.url_previsoes_carga = constants.GET_NEWAVE_PREVISOES_CARGAS
        self.url_sist_last_deck = constants.GET_NEWAVE_SISTEMA_LAST_DECK
        self.url_cadic_last_deck = constants.GET_NEWAVE_CADIC_LAST_DECK
        self.url_post_sist = constants.POST_NEWAVE_SISTEMA
        self.url_post_cadic = constants.POST_NEWAVE_CADIC
        
        self.headers = get_auth_header()
    
    def run_process(self):
        df_data = self._get_data(self.url_previsoes_carga, self.dataProduto)
        
        self.gerar_sist_deck(df_data, self.dataProduto)
        self.gerar_cadic_deck(df_data, self.dataProduto)
        
        
    def gerar_sist_deck(self, df_data, dataProduto):
                
        df_data = df_data[df_data['patamar'] == 'medio']
        
        df_last_sist = self._get_data(self.url_sist_last_deck, dataProduto)
        
        df_data['data_referente'] = pd.to_datetime(df_data['data_referente'])
        dataProduto = pd.to_datetime('01/' + dataProduto, format='%d/%m/%Y')
        df_data = df_data[df_data['data_referente'] >= dataProduto]
        
        submercado_maps = {
            "SE": 1,
            "S": 2,
            "NE": 3,
            "N": 4
        }
        
        df_sist_update = df_data[['submercado','data_referente', 'data_produto', 'vl_exp_pch_mmgd', 'vl_exp_pct_mmgd', 'vl_exp_eol_mmgd', 'vl_exp_ufv_mmgd']]
        df_sist_update = df_sist_update.rename(columns={
            'submercado': 'cd_submercado',
            'data_produto': 'dt_deck',
            'vl_exp_pch_mmgd': 'vl_geracao_pch_mmgd',
            'vl_exp_pct_mmgd': 'vl_geracao_pct_mmgd',
            'vl_exp_eol_mmgd': 'vl_geracao_eol_mmgd',
            'vl_exp_ufv_mmgd': 'vl_geracao_ufv_mmgd'
        })
        df_sist_update['data_referente'] = pd.to_datetime(df_sist_update['data_referente'])
        df_sist_update['vl_ano'] = df_sist_update['data_referente'].dt.year
        df_sist_update['vl_mes'] = df_sist_update['data_referente'].dt.month
        df_sist_update = df_sist_update.drop(columns=('data_referente'))
        df_sist_update['cd_submercado'] = df_sist_update['cd_submercado'].map(submercado_maps)
        df_sist_update['versao'] = 'quadrimestral'
        
        df_sist_update.reset_index().drop(columns=['index'])
        
        df_last_sist = df_last_sist[['vl_energia_total', 'vl_geracao_pch','vl_geracao_pct','vl_geracao_eol','vl_geracao_ufv']]
        
        df_final = pd.concat(
            [df_sist_update.reset_index(drop=True),
            df_last_sist.reset_index(drop=True)],
            axis=1
        )
        
        self._post_data(self.url_post_sist, df_final)
        
        pass
    
    def gerar_cadic_deck(self, df_data, dataProduto):
        
        df_data = df_data[df_data['patamar'] == 'medio']
        
        df_last_cadic = self._get_data(self.url_cadic_last_deck, dataProduto)
        
        df_data['data_referente'] = pd.to_datetime(df_data['data_referente'])
        dataProduto = pd.to_datetime('01/' + dataProduto, format='%d/%m/%Y')
        df_data = df_data[df_data['data_referente'] >= dataProduto]
        
        df_cadic_update = df_data[['submercado','data_referente','data_produto',
                  'vl_base_pch_mmgd','vl_base_eol_mmgd','vl_base_ufv_mmgd','vl_base_pct_mmgd']].rename(
        columns={'submercado':'cd_submercado','data_produto':'dt_deck'}
        )

        dref = pd.to_datetime(df_cadic_update['data_referente'])
        df_cadic_update['vl_ano'] = dref.dt.year
        df_cadic_update['vl_mes'] = dref.dt.month
        df_cadic_update = df_cadic_update.drop(columns='data_referente')
    
        base_cols = ['vl_base_pch_mmgd','vl_base_eol_mmgd','vl_base_ufv_mmgd','vl_base_pct_mmgd']
        df_cadic_update['vl_mmgd_total'] = df_cadic_update[base_cols].sum(axis=1)
    
        agg = (df_cadic_update.groupby(['vl_ano','vl_mes','dt_deck','cd_submercado'], as_index=False)['vl_mmgd_total'].sum())
        df_cadic_update = (agg.pivot(index=['vl_ano','vl_mes','dt_deck'], columns='cd_submercado', values='vl_mmgd_total')
                   .fillna(0)
                   .rename(columns={'SE':'vl_mmgd_se','S':'vl_mmgd_s','NE':'vl_mmgd_ne','N':'vl_mmgd_n'})
                   .reset_index())
    
        for c in ['vl_mmgd_se','vl_mmgd_s','vl_mmgd_ne','vl_mmgd_n']:
            if c not in df_cadic_update: df_cadic_update[c] = 0.0
        df_cadic_update = df_cadic_update[['vl_ano','vl_mes','vl_mmgd_se','vl_mmgd_s','vl_mmgd_ne','vl_mmgd_n','dt_deck']]
        df_cadic_update['versao'] = 'quadrimestral'
        
        df_cadic_update.columns.name = None
        
        df_final = pd.concat(
            [df_cadic_update.reset_index(drop=True),
            df_last_cadic.reset_index(drop=True)],
            axis=1
        )
        
        self._post_data(self.url_post_cadic, df_final)
        
        
        pass
        
    def _get_data(self, url: str, params) -> pd.DataFrame:
        res = requests.get(url, headers=self.headers, params = params)
        if res.status_code != 200:
            res.raise_for_status()
        return pd.DataFrame(res.json())    
    
    def _post_data(self, url: str, df: pd.DataFrame) -> pd.DataFrame:
        res = requests.post(
                url,
                headers=self.headers,
                json=df.to_dict('records'),
            )
        if res.status_code != 200:
            res.raise_for_status()
            
class GerarTabelaDiferenca():  
    def __init__(self):
        self.constants = Constants()
        self.headers = get_auth_header()
        self.url_html_to_image = self.constants.URL_HTML_TO_IMAGE
        self.url_unsi          = self.constants.GET_NEWAVE_SISTEMA_TOTAL_UNSI
        self.url_carga_global  = self.constants.GET_NEWAVE_SISTEMA_CARGAS_TOTAL_CARGA_GLOBAL
        self.url_carga_liquida = self.constants.GET_NEWAVE_SISTEMA_CARGAS_TOTAL_CARGA_LIQUIDA
        self.url_mmgd_total    = self.constants.GET_NEWAVE_SISTEMA_MMGD_TOTAL
        self.url_ande          = self.constants.GET_NEWAVE_CADIC_TOTAL_ANDE
    
    def run_workflow(self):
        self.run_process()
    
    def run_process(self):
        self.generate_table()
        
    def generate_table(self):        
        dict_data = {
            'dados_unsi':self.get_data(self.url_unsi),
            'dados_ande': self.get_data(self.url_ande),
            'dados_mmgd_total': self.get_data(self.url_mmgd_total),
            'dados_carga_global': self.get_data(self.url_carga_global),
            'dados_carga_liquida': self.get_data(self.url_carga_liquida)
        }
        dict_caption = {
            'dados_unsi': 'Diferença UNSI ',
            'dados_ande': 'Diferença ANDE',
            'dados_mmgd_total': 'Diferença MMGD Total',
            'dados_carga_global': 'Diferença Carga Global',  
            'dados_carga_liquida': 'Diferença Carga Líquida'}
        
        html = ""
        for data in dict_data:
            html += self.generate_dif(dict_data[data][1]['data'], dict_data[data][0]['data'], dict_caption[data])
            html += '<br><br>'
        
        css = '<style type="text/css">'
        css += 'caption {background-color: #E0E0E0; color: black;}'
        css += 'th {background-color: #E0E0E0; color: black; min-width: 60px;}'
        css += 'td {min-width: 60px;}'
        css += 'table {text-align: center; border-collapse: collapse; border 2px solid black !important}'
        css += '</style>'
        html = html.replace('<style type="text/css">\n</style>\n', css)

        image_binary = html_to_image(html)
        date_last = datetime.strptime(dict_data['dados_unsi'][0]['dt_deck'], '%Y-%m-%d').strftime('%m/%Y')
        date_now = datetime.strptime(dict_data['dados_unsi'][1]['dt_deck'], '%Y-%m-%d').strftime('%m/%Y')
        msg = f"DIFERENÇAS DE CARGA ENTRE OS NW:\nNW {date_now} {dict_data['dados_unsi'][1]['versao'].upper()}\n" +\
                      f"NW {date_last} {dict_data['dados_unsi'][0]['versao'].upper()}\n"
                      
        send_whatsapp_message(self.constants.WHATSAPP_DEBUG, msg, image_binary)
        print(html)
    
    def generate_dif(self, df, df_last, caption):
        df = pd.DataFrame(df)
        df = df.pivot(index=["vl_ano"], columns="vl_mes", values=df.keys()[-1]).reset_index()
        df = df.set_index('vl_ano')
        df.columns.name = 'ANO'
        df.index.name = None
        
        df_last = pd.DataFrame(df_last)
        df_last = df_last.pivot(index=["vl_ano"], columns="vl_mes", values=df_last.keys()[-1]).reset_index()
        df_last = df_last.set_index('vl_ano')
        df_last.columns.name = 'ANO'
        df_last.index.name = None
        
        df_dif = df - df_last
        df_dif['Média'] = df_dif.iloc[:, 1:].mean(axis=1, skipna=True)
        df_dif = df_dif.rename(columns={1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'})
        df_dif = df_dif.style.format(na_rep='', precision=0)
        df_dif = df_dif.set_caption(caption)
       
        return df_dif.to_html()
    
  
    def get_data(self, url: str) -> pd.DataFrame:
        res = requests.get(url, headers=self.headers)
        if res.status_code != 200:
            res.raise_for_status()
        return res.json()


if __name__ == '__main__':
    logger.info("Iniciando workflow do produto manualmente...")
    try:
        
        payload = {
  "dataProduto": "10/2025",
  "filename": "RV0_PMO_Outubro_2025_carga_mensal.zip",
  "macroProcesso": "Programação da Operação",
  "nome": "Previsões de carga mensal e por patamar - NEWAVE",
  "periodicidade": "2025-10-01T00:00:00",
  "periodicidadeFinal": "2025-10-31T23:59:59",
  "processo": "Previsão de Carga para o PMO",
  "s3Key": "webhooks/Previsões de carga mensal e por patamar - NEWAVE/68d1c91a450014d70a3e5a4b_RV0_PMO_Outubro_2025_carga_mensal.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS80Ny9Qcm9kdXRvcy8yMjkvUlYwX1BNT19PdXR1YnJvXzIwMjVfY2FyZ2FfbWVuc2FsLnppcCIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJQcmV2aXPDtWVzIGRlIGNhcmdhIG1lbnNhbCBlIHBvciBwYXRhbWFyIC0gTkVXQVZFIiwiSXNGaWxlIjoiVHJ1ZSIsImlzcyI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiYXVkIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJleHAiOjE3NTg2NjU2MTAsIm5iZiI6MTc1ODU3ODk3MH0.kRh6NGPhw1fEHGNRKU7LbxE0ktwqaDqiopjcuGOTyts",
  "webhookId": "68d1c91a450014d70a3e5a4b"
}
        
        payload = WebhookSintegreSchema(**payload)
        
        previsoescarga = CargaPatamarNewave(payload)
        previsoescarga.run_workflow()
        
    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Previsões de Carga Mensal por Patamar do Newave: %s", str(e), exc_info=True)
        raise