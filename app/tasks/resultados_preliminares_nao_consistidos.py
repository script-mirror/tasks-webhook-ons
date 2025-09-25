import pandas as pd
from typing import Optional
from pathlib import Path
import pdb
import sys
import glob
import os

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants
from middle.utils.file_manipulation import extract_zip
logger = setup_logger()
constants = Constants()


class ResultadosPreliminaresNaoConsistidos(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        
        
    def run_workflow(self):
        logger.info("Iniciando workflow do produto Resultados Preliminares Nao Consistidos...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Resultados Preliminares Nao Consistidos finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Resultados Preliminares Nao Consistidos")
            raise
        
    def run_process(self, file_path):
        process_result = self.process_file(file_path)
        
        self.post_data(process_result)
        
        pass
    
    def process_file(self, file_path):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            unzip_path = extract_zip(file_path)
            logger.debug("Arquivo extraído para: %s", unzip_path)
            
            load_path = glob.glob(os.path.join(unzip_path, '*xlsx'))[0]
            
            df_load = pd.read_excel(load_path)
        except Exception as e:
            logger.error("Falha em processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        pass
    

if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Resultados Preliminares Nao Consistidos...")
    try:
        payload = {
            "dataProduto": "24/09/2025",
            "filename": "psat_24092025.txt",
            "macroProcesso": "Programação da Operação",
            "nome": "Precipitação por Satélite – ONS",
            "periodicidade": "2025-09-24T00:00:00",
            "periodicidadeFinal": "2025-09-24T23:59:59",
            "processo": "Meteorologia e clima",
            "s3Key": "webhooks/Precipitação por Satélite – ONS/68d42644450014d70a3e6056_psat_24092025.txt",
            "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8zOC9Qcm9kdXRvcy80ODcvcHNhdF8yNDA5MjAyNS50eHQiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUHJlY2lwaXRhw6fDo28gcG9yIFNhdMOpbGl0ZSDigJMgT05TIiwiSXNGaWxlIjoiVHJ1ZSIsImlzcyI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiYXVkIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJleHAiOjE3NTg4MjA1MzEsIm5iZiI6MTc1ODczMzg5MX0.2mgFER2IWoeq20j5_iarX3-Df9935DJOoj_1F8peL-E",
            "webhookId": "68d42644450014d70a3e6056"
        }
        
        payload = WebhookSintegreSchema(**payload)
        

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Resultados Preliminares Nao Consistidos: %s", str(e), exc_info=True)
        raise