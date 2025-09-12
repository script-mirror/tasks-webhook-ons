import pdb
import sys
import os
import requests
import pandas as pd
import glob
from datetime import datetime, timedelta
from middle.utils import SemanaOperativa
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.schema import WebhookSintegreSchema  # noqa: E402
from middle.utils import setup_logger, Constants, get_auth_header, sanitize_string  # noqa: E402
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from middle.utils.file_manipulation import extract_zip, create_directory # noqa: E402
from middle.s3 import ( # noqa: E402
    handle_webhook_file,
    get_latest_webhook_product,
)
from middle.airflow import trigger_dag, trigger_dag_legada

logger = setup_logger()
constants = Constants()

class CargaPatamarDecomp(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        logger.info("Initialized CargaPatamarDecomp with payload: %s", payload)
    
    def run_workflow(self):
        logger.info("Starting run_workflow for CargaPatamarDecomp")
        try:
            os.makedirs(constants.PATH_TMP, exist_ok=True)
            logger.debug("Created temporary directory: %s", constants.PATH_TMP)
            
            payload = get_latest_webhook_product(constants.WEBHOOK_CARGA_DECOMP)[0]
            logger.debug("Retrieved latest webhook product: %s", payload)
            base_path = handle_webhook_file(payload, constants.PATH_TMP)
            logger.info("Webhook file handled, base path: %s", base_path)
            
            self.run_process( base_path)
            logger.info("run_workflow completed successfully")
            self.trigger_dags()
            logger.info("Triggered Airflow DAG successfully")
            
            
        except Exception as e:
            logger.error("run_workflow failed: %s", str(e), exc_info=True)
            raise

    def run_process(self, base_path):
        df_load = self.read_week_load(base_path)
        logger.info("Successfully processed load data with %d rows", len(df_load))
        self.post_data(df_load)
        
    def read_week_load(self, base_path):
        logger.info("Reading week load data from base path: %s", base_path)
        try:
            unzip_path = extract_zip(base_path)
            logger.debug("Extracted zip file to: %s", unzip_path)
            
            week_load_path = glob.glob(os.path.join(unzip_path, 'Semanal_DP*'))[0]
            month_load_path = glob.glob(os.path.join(unzip_path, 'Mensal_DP*'))[0]
            logger.info("Found weekly file: %s and monthly file: %s", week_load_path, month_load_path)
            
            file_rv = int(os.path.basename(week_load_path).split('Rev')[1][:2])
            logger.info("Extracted file revision: %d", file_rv)
            
            df_week_load = pd.read_excel(week_load_path)
            logger.debug("Read weekly load data with %d rows", len(df_week_load))
            
            df_week_load['SO'] = pd.to_datetime(df_week_load['SO']) - timedelta(days=6)
            logger.debug("Adjusted SO column in weekly data by subtracting 6 days")
            
            deck_date = SemanaOperativa(min(df_week_load['SO'].unique()))
            logger.info("Calculated deck date: %s", deck_date.week_start)
            
            if int(deck_date.current_revision) != file_rv:
                logger.warning("Revision mismatch: file revision %d does not match deck revision %s",
                             file_rv, deck_date.current_revision)
            
            df_month_load = pd.read_excel(month_load_path)
            logger.debug("Read monthly load data with %d rows", len(df_month_load))
            
            df_month_load = df_month_load.drop('mes', axis=1)
            logger.debug("Dropped 'mes' column from monthly data")
            
            df_month_load['SO'] = max(df_week_load['SO'].unique()) + timedelta(days=7)
            logger.debug("Set SO column in monthly data to: %s", df_month_load['SO'].iloc[0])
            
            df_load = pd.concat([df_week_load, df_month_load], axis=0).reset_index(drop=True)
            logger.debug("Concatenated weekly and monthly data, total rows: %d", len(df_load))
            
            df_load = df_load.drop(['cod_ss', 'cod_pat'], axis=1)
            logger.debug("Dropped columns 'cod_ss' and 'cod_pat'")
            
            df_load.columns = df_load.columns.str.lower()
            logger.debug("Converted column names to lowercase")
            
            df_load = df_load.rename(columns={'so': 'semana_operativa', 'subsistema': 'submercado'})
            logger.debug("Renamed columns: 'so' to 'semana_operativa', 'subsistema' to 'submercado'")
            
            df_load['data_produto'] = min(df_load['semana_operativa'].unique())
            df_load['data_produto'] = df_load['data_produto'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df_load['semana_operativa'] = df_load['semana_operativa'].apply(lambda x: x.strftime('%Y-%m-%d'))
            logger.debug("Formatted date columns 'data_produto' and 'semana_operativa' to YYYY-MM-DD")
            
            df_load['patamar'] = df_load['patamar'].apply(lambda x: sanitize_string(text=x))
            df_load['submercado'] = df_load['submercado'].apply(lambda x: sanitize_string(text=x).upper())
            logger.debug("Sanitized and formatted 'patamar' and 'submercado' columns")
            
            return df_load
        
        except Exception as e:
            logger.error("Failed to read week load data: %s", str(e), exc_info=True)
            raise
    
    def post_data(self, data_in: pd.DataFrame) -> dict:
        logger.info("Posting load data to database, rows: %d", len(data_in))
        try:
            res = requests.post(
                constants.BASE_URL + '/api/v2/decks/carga-decomp',
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                logger.error("Failed to post data to database: status %d, response: %s",
                           res.status_code, res.text)
                res.raise_for_status()
            
            logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return pd.DataFrame(res.json())
        
        except Exception as e:
            logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise
    
    
    def trigger_dags(self):
        trigger_dag(
            dag_id="1.18-PROSPEC_UPDATE", conf={"produto": "CARGA-DECOMP"}
        )
        conf = self.payload.model_dump() if type(self.payload) is WebhookSintegreSchema else self.payload
        if isinstance(conf, dict):
            for k, v in conf.items():
                if isinstance(v, (datetime, pd.Timestamp)):
                    conf[k] = str(v)
        trigger_dag_legada(
            dag_id="WEBHOOK", conf=conf
        )
       

class CargaSemanalDecomp():
    
    def __init__(self):
        
        logger.info("Initialized CargaPatamarDecomp with payload: ")
    
    def run_workflow(self):
        logger.info("Starting run_workflow for CargaPatamarDecomp")
        try:
            os.makedirs(constants.PATH_ARQUIVOS_TEMP, exist_ok=True)
            logger.debug("Created temporary directory: %s", constants.PATH_ARQUIVOS_TEMP)
            
            payload = get_latest_webhook_product(constants.WEBHOOK_CARGA_DECOMP)[0]
            logger.debug("Retrieved latest webhook product: %s", payload)
            
            base_path = handle_webhook_file(payload, constants.PATH_ARQUIVOS_TEMP)
            logger.info("Webhook file handled, base path: %s", base_path)
            
            self.run_process( base_path)
            logger.info("run_workflow completed successfully")
            
        except Exception as e:
            logger.error("run_workflow failed: %s", str(e), exc_info=True)
            raise
    def run_process(self, base_path):
        df_load = self.read_week_load(base_path)
        logger.info("Successfully processed load data with %d rows", len(df_load))
        
        
    def read_week_load(self, base_path):
        logger.info("Reading week load data from base path: %s", base_path)
        unzip_path = extract_zip(base_path)
        logger.debug("Extracted zip file to: %s", unzip_path)
        
        week_load_path = glob.glob(os.path.join(unzip_path, '*arga_*.xlsx'))[0]
        logger.info("Found weekly file: %s and monthly file: %s", week_load_path)
        
        file_rv = int(os.path.basename(week_load_path).split('Rev')[1][:2])
        logger.info("Extracted file revision: %d", file_rv)
        
        df_week_load = pd.read_excel(week_load_path)
        logger.debug("Read weekly load data with %d rows", len(df_week_load))


if __name__ == '__main__':
    logger.info("Starting CargaPatamarDecomp script execution")
    try:
        carga = CargaSemanalDecomp()
        carga.run_workflow()
        logger.info("Script execution completed successfully")
    except Exception as e:
        logger.error("Script execution failed: %s", str(e), exc_info=True)
        raise