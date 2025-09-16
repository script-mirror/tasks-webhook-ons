import pdb
import sys
import os
import requests
import shutil
import pandas as pd
import glob
from PIL import Image
from datetime import datetime, timedelta
import aspose.words as aw
from middle.utils import SemanaOperativa
from middle.utils import html_to_image
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from middle.utils import HtmlBuilder
from bs4 import BeautifulSoup
from middle.message import send_whatsapp_message, send_email_message
from app.schema import WebhookSintegreSchema  # noqa: E402
from middle.utils import setup_logger, Constants, get_auth_header, sanitize_string  # noqa: E402
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from middle.utils.file_manipulation import extract_zip, create_directory # noqa: E402
from middle.s3 import ( # noqa: E402
    handle_webhook_file,
    get_latest_webhook_product,
)
from middle.airflow import trigger_dag, trigger_dag_legada

# Configura o logger globalmente uma única vez
logger = setup_logger()
constants = Constants()

class CargaPatamarDecomp():
    
    def __init__(self):
        self.read_carga_patamar = ReadCargaPatamar()  
        self.trigger_dag = trigger_dag      
        self.read_carga_semanal = ReadCargaSemanal()
        self.generate_table = GenerateTable()
        logger.info("Initialized CargaPatamarDecomp with payload")
    
    def run_workflow(self):
        logger.info("Starting workflow for CargaPatamarDecomp")
        self.run_process()
   
    def run_process(self):
        logger.info("Running process for CargaPatamarDecomp")
        self.read_carga_patamar.run_workflow()
        logger.debug("Triggering DAG 1.18-PROSPEC_UPDATE with conf: {'produto': 'CARGA-DECOMP'}")
        self.trigger_dag(dag_id="1.18-PROSPEC_UPDATE", conf={"produto": "CARGA-DECOMP"})
        self.read_carga_semanal.run_workflow()
        self.generate_table.run_workflow()
     
     
class ReadCargaPatamar:
    
    def __init__(self):
        self.logger = logger  # Usa o logger global
        self.logger.info("Initialized ReadCargaPatamar")
    
    def run_workflow(self):
        self.logger.info("Starting run_workflow for ReadCargaPatamar")
        try:
            os.makedirs(constants.PATH_TMP, exist_ok=True)
            self.logger.debug("Created temporary directory: %s", constants.PATH_TMP)
            
            payload = get_latest_webhook_product(constants.WEBHOOK_CARGA_DECOMP)[0]
            self.logger.debug("Retrieved latest webhook product: %s", payload)
            base_path = handle_webhook_file(payload, constants.PATH_TMP)
            self.logger.info("Webhook file handled, base path: %s", base_path)
            
            self.run_process(base_path)
            self.logger.info("run_workflow completed successfully")
            
        except Exception as e:
            self.logger.error("run_workflow failed: %s", str(e), exc_info=True)
            raise

    def run_process(self, base_path):
        self.logger.info("Processing load data from base path: %s", base_path)
        df_load = self.read_week_load(base_path)
        self.logger.info("Successfully processed load data with %d rows", len(df_load))
        self.post_data(df_load)
        
    def read_week_load(self, base_path):
        self.logger.info("Reading week load data from base path: %s", base_path)
        try:
            unzip_path = extract_zip(base_path)
            self.logger.debug("Extracted zip file to: %s", unzip_path)
            
            week_load_path = glob.glob(os.path.join(unzip_path, 'Semanal_DP*'))[0]
            month_load_path = glob.glob(os.path.join(unzip_path, 'Mensal_DP*'))[0]
            self.logger.info("Found weekly file: %s and monthly file: %s", week_load_path, month_load_path)
            
            file_rv = int(os.path.basename(week_load_path).split('Rev')[1][:2])
            self.logger.info("Extracted file revision: %d", file_rv)
            
            df_week_load = pd.read_excel(week_load_path)
            self.logger.debug("Read weekly load data with %d rows", len(df_week_load))
            
            df_week_load['SO'] = pd.to_datetime(df_week_load['SO']) - timedelta(days=6)
            self.logger.debug("Adjusted SO column in weekly data by subtracting 6 days")
            
            deck_date = SemanaOperativa(min(df_week_load['SO'].unique()))
            self.logger.info("Calculated deck date: %s", deck_date.week_start)
            
            if int(deck_date.current_revision) != file_rv:
                self.logger.warning("Revision mismatch: file revision %d does not match deck revision %s",
                                  file_rv, deck_date.current_revision)
            
            df_month_load = pd.read_excel(month_load_path)
            self.logger.debug("Read monthly load data with %d rows", len(df_month_load))
            
            df_month_load = df_month_load.drop('mes', axis=1)
            self.logger.debug("Dropped 'mes' column from monthly data")
            
            df_month_load['SO'] = max(df_week_load['SO'].unique()) + timedelta(days=7)
            self.logger.debug("Set SO column in monthly data to: %s", df_month_load['SO'].iloc[0])
            
            df_load = pd.concat([df_week_load, df_month_load], axis=0).reset_index(drop=True)
            self.logger.debug("Concatenated weekly and monthly data, total rows: %d", len(df_load))
            
            df_load = df_load.drop(['cod_ss', 'cod_pat'], axis=1)
            self.logger.debug("Dropped columns 'cod_ss' and 'cod_pat'")
            
            df_load.columns = df_load.columns.str.lower()
            self.logger.debug("Converted column names to lowercase")
            
            df_load = df_load.rename(columns={'so': 'semana_operativa', 'subsistema': 'submercado'})
            self.logger.debug("Renamed columns: 'so' to 'semana_operativa', 'subsistema' to 'submercado'")
            
            df_load['data_produto'] = min(df_load['semana_operativa'].unique())
            df_load['data_produto'] = df_load['data_produto'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df_load['semana_operativa'] = df_load['semana_operativa'].apply(lambda x: x.strftime('%Y-%m-%d'))
            self.logger.debug("Formatted date columns 'data_produto' and 'semana_operativa' to YYYY-MM-DD")
            
            df_load['patamar'] = df_load['patamar'].apply(lambda x: sanitize_string(text=x))
            df_load['submercado'] = df_load['submercado'].apply(lambda x: sanitize_string(text=x).upper())
            self.logger.debug("Sanitized and formatted 'patamar' and 'submercado' columns")
            shutil.rmtree(base_path, ignore_errors=True)
            self.logger.debug("Removed temporary directory: %s", base_path)
            return df_load
        
        except Exception as e:
            self.logger.error("Failed to read week load data: %s", str(e), exc_info=True)
            raise
    
    def post_data(self, data_in: pd.DataFrame) -> dict:
        self.logger.info("Posting load data to database, rows: %d", len(data_in))
        try:
            res = requests.post(
                constants.POST_DECOMP_CARGA_DECOMP,
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                self.logger.error("Failed to post data to database: status %d, response: %s",
                                res.status_code, res.text)
                res.raise_for_status()
            
            self.logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return pd.DataFrame(res.json())
        
        except Exception as e:
            self.logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise
    
class ReadCargaSemanal:
    
    def __init__(self):
        self.logger = logger  # Usa o logger global
        self.logger.info("Initialized ReadCargaSemanal")
    
    def run_workflow(self):
        self.logger.info("Starting run_workflow for ReadCargaSemanal")
        try:
            os.makedirs(constants.PATH_ARQUIVOS_TEMP, exist_ok=True)
            self.logger.debug("Created temporary directory: %s", constants.PATH_ARQUIVOS_TEMP)
            
            payload = get_latest_webhook_product(constants.WEBHOOK_CARGA_DECOMP)[0]
            self.logger.debug("Retrieved latest webhook product: %s", payload)
            
            base_path = handle_webhook_file(payload, constants.PATH_ARQUIVOS_TEMP)
            self.logger.info("Webhook file handled, base path: %s", base_path)
            
            self.run_process(base_path)
            self.logger.info("run_workflow completed successfully")
            
        except Exception as e:
            self.logger.error("run_workflow failed: %s", str(e), exc_info=True)
            raise
    
    def run_process(self, base_path):
        self.logger.info("Processing load data from base path: %s", base_path)
        df_load = self.read_week_load(base_path)
        self.logger.info("Successfully processed load data with %d rows", len(df_load))
        self.post_data(df_load)
        
    def read_week_load(self, base_path):
        self.logger.info("Reading week load data from base path: %s", base_path)
        unzip_path = extract_zip(base_path)
        self.logger.debug("Extracted zip file to: %s", unzip_path)
        patamar_load_path = glob.glob(os.path.join(unzip_path, 'Semanal_DP*'))[0]
        week_load_path = glob.glob(os.path.join(unzip_path, '*arga_*.xlsx'))[0]
        self.logger.info("Found patamar file: %s and weekly file: %s", patamar_load_path, week_load_path)

        df_patamar_load = pd.read_excel(patamar_load_path)
        data_produto = (min(df_patamar_load['SO'].unique()) - timedelta(days=6)).strftime('%Y-%m-%d')
        self.logger.debug("Calculated data_produto: %s", data_produto)
        
        df_week_load = pd.read_excel(week_load_path)
        self.logger.debug("Read weekly load data with %d rows", len(df_week_load))
        
        df_week_load.dropna(axis=1, how='all', inplace=True)
        df_week_load.dropna(axis=0, how='all', inplace=True)
        df_week_load = df_week_load.reset_index(drop=True)      
        df_week_load = df_week_load[df_week_load['Unnamed: 2'].isin(self.MAP_SUBSYSTEMS()[0])].reset_index(drop=True)
        df_week_load.columns = df_week_load.iloc[0]
        df_week_load = df_week_load.drop(0).reset_index(drop=True)
        df_week_load.dropna(axis=1, inplace=True)
        self.logger.debug("Cleaned and filtered weekly load data, rows: %d", len(df_week_load))
        
        df_week_load = df_week_load.rename(columns={
            'Subsistemas/Sistemas': 'subsistema',
            'Mensal': 'MENSAL',
            '1ª Semana': 'RV0',
            '2ª Semana': 'RV1',
            '3ª Semana': 'RV2',
            '4ª Semana': 'RV3',
            '5ª Semana': 'RV4',
            '6ª Semana': 'RV5',
            '7ª Semana': 'RV6'
        })
        self.logger.debug("Renamed columns in weekly load data")
        
        df_week_load["subsistema"] = df_week_load["subsistema"].map(self.MAP_SUBSYSTEMS()[1])
        df_week_load['data_produto'] = data_produto
        df_week_load = df_week_load.melt(id_vars=["subsistema", "data_produto"], var_name='periodo', value_name='carga')
        df_week_load['carga'] = df_week_load['carga'].astype(int)
        self.logger.debug("Transformed weekly load data into melted format, rows: %d", len(df_week_load))
        
        shutil.rmtree(base_path, ignore_errors=True)
        self.logger.debug("Removed temporary directory: %s", base_path)
        return df_week_load
    
    def MAP_SUBSYSTEMS(self):
        subsystems = [
            "Subsistemas/Sistemas",
            'Subsistema Nordeste',
            'Subsistema Norte',
            'Subsistema Sudeste/C.Oeste',
            'Subsistema Sul',
            'Sistema Interligado Nacional'
        ]
        mapping = {
            'Subsistemas/Sistemas': 'SIN',
            'Subsistema Nordeste': 'NE',
            'Subsistema Norte': 'N',
            'Subsistema Sudeste/C.Oeste': 'SE',
            'Subsistema Sul': 'S',
            'Sistema Interligado Nacional': 'SIN'
        }
        return subsystems, mapping
    
    def post_data(self, data_in: pd.DataFrame) -> dict:
        self.logger.info("Posting load data to database, rows: %d", len(data_in))
        try:
            res = requests.post(
                constants.POST_DECOMP_CARGA_PMO,
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                self.logger.error("Failed to post data to database: status %d, response: %s",
                                res.status_code, res.text)
                res.raise_for_status()
            
            self.logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return pd.DataFrame(res.json())
        
        except Exception as e:
            self.logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise

class GenerateTable:
    
    def __init__(self):
        self.logger = logger  # Usa o logger global
        self.logger.info("Initialized GenerateTable")
    
    def run_workflow(self):
        self.logger.info("Starting run_workflow for GenerateTable")
        self.run_process()

    def run_process(self):
        self.logger.info("Generating table")
        self.generate_table()
        self.logger.info("Table generation completed")
        
    def generate_table(self):
        self.logger.info("Starting table generation")
        df_rv = self.get_data('')
        self.logger.debug("Retrieved RV data with %d rows", len(df_rv))
        
        df_rv = df_rv.pivot(index=["subsistema", "data_produto"], columns="periodo", values="carga").reset_index()
        df_rv.columns.name = 'SUBMERCADO'
        date_rv = pd.to_datetime(df_rv['data_produto'].unique()[0])
        date_rv0 = SemanaOperativa(date_rv).first_day_of_month.strftime('%Y-%m-%d')
        date_last_rv = (date_rv - timedelta(days=7)).strftime('%Y-%m-%d')
        self.logger.debug("Calculated dates: date_rv=%s, date_rv0=%s, date_last_rv=%s", 
                         date_rv, date_rv0, date_last_rv)
        
        df_last_rv = self.get_data({'data_produto': date_last_rv})
        self.logger.debug("Retrieved last RV data with %d rows", len(df_last_rv))
        df_last_rv = df_last_rv.pivot(index=["subsistema", "data_produto"], columns="periodo", values="carga").reset_index()
        df_last_rv.columns.name = 'SUBMERCADO' 
        df_last_rv = df_last_rv.drop(['data_produto'], axis=1)
        df_last_rv = df_last_rv.set_index("subsistema")
        df_last_rv = df_last_rv.loc[['SE','S','NE','N','SIN']]
        
        df_pmo = self.get_data({'data_produto': date_rv0})
        self.logger.debug("Retrieved PMO data with %d rows", len(df_pmo))
        df_pmo = df_pmo.pivot(index=["subsistema", "data_produto"], columns="periodo", values="carga").reset_index()
        df_pmo.columns.name = 'SUBMERCADO' 
        df_pmo = df_pmo.drop(['data_produto'], axis=1)
        df_pmo = df_pmo.set_index("subsistema")
        df_pmo = df_pmo.loc[['SE','S','NE','N','SIN']]
        
        df_rv = df_rv.drop(['data_produto'], axis=1)
        df_rv = df_rv.set_index("subsistema")
        df_rv = df_rv.loc[['SE','S','NE','N','SIN']]
        
        dif_pmo = df_rv - df_pmo
        dif_rv = df_rv - df_last_rv
        dif_pmo.index.name = None    
        dif_rv.index.name = None
        dif_rv_html = dif_rv.style.format('{:.0f}')
        dif_pmo_html = dif_pmo.style.format('{:.0f}')
       
        rv = int(SemanaOperativa(date_rv).current_revision)
        last_rv = int(SemanaOperativa(pd.to_datetime(date_last_rv)).current_revision)
        self.logger.info("Calculated revisions: RV=%d, last_RV=%d", rv, last_rv)
        
        dif_rv_html.set_caption(f"Atualização de carga DC (RV{rv} - RV{last_rv})")
        dif_pmo_html.set_caption(f"Atualização de carga DC (RV{rv} - PMO)")

        css = '<style type="text/css">'
        css += 'caption {background-color: #666666; color: white;}'
        css += 'th {background-color: #666666; color: white; min-width: 80px;}'  # largura do cabeçalho
        css += 'td {min-width: 80px;}'  # largura das células
        css += 'table {text-align: center; border-collapse: collapse; border 2px solid black !important}'  # centralizar e ajustar tabela
        css += '</style>'

        if rv == 0:
            html = dif_rv_html.to_html()
            self.logger.info("Sending only RV%d - RV%d table due to identical RV1 and RV0", rv, rv)
            destinatarioEmail = constants.EMAIL_MIDDLE
            whatsapp = constants.WHATSAPP_MIDDLE
        else:
            html = dif_rv_html.to_html()
            html += '<br><br>'
            html += dif_pmo_html.to_html()
            self.logger.info("Sending both RV%d - RV%d and RV%d - PMO tables", rv, last_rv, rv)
            destinatarioEmail = [constants.EMAIL_MIDDLE, constants.EMAIL_FRONT]
            whatsapp = constants.WHATSAPP_PMO
        
        html = html.replace('<style type="text/css">\n</style>\n', css)
        self.logger.debug("Generated HTML for tables")
        image_binary = html_to_image(html)
        self.logger.debug("Converted HTML to image")
        
        send_whatsapp_message(whatsapp, f'REVISÃO DE CARGA RV{rv}', image_binary)
        self.logger.info("Sent WhatsApp message for RV %d", rv)
        send_email_message(user=constants.EMAIL_RODADAS, destinatario=destinatarioEmail, mensagem=html, assunto=f'REVISÃO DE CARGA RV{rv}')
        self.logger.info("Sent email for RV %d to %s", rv, destinatarioEmail)

    def get_data(self, date) -> dict:
        self.logger.info("Retrieving data from database with params: %s", date)
        try:
            res = requests.get(
                constants.GET_DECOMP_CARGA_PMO,
                params=date,
                headers=get_auth_header()
            )
            if res.status_code != 200:
                self.logger.error("Failed to get data from database: status %d, response: %s",
                                res.status_code, res.text)
                res.raise_for_status()
            
            self.logger.info("Successfully retrieved data from database, response status: %d", res.status_code)
            return pd.DataFrame(res.json())
        
        except Exception as e:
            self.logger.error("Failed to get data from database: %s", str(e), exc_info=True)
            raise    

if __name__ == '__main__':
    logger.info("Starting CargaPatamarDecomp script execution")
    """ try:
        carga = CargaPatamarDecomp()
        carga.run_workflow()
        logger.info("Script execution completed successfully")
    except Exception as e:
        logger.error("Script execution failed: %s", str(e), exc_info=True)
        raise"""