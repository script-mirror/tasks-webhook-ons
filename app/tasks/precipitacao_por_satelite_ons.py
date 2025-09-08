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
from app.schema import WebhookPayloadSchema

from middle.utils import setup_logger, Constants
from middle.utils.file_manipulation import extract_zip
logger = setup_logger()
constants = Constants()


class PrecipitacaoPorSateliteONS(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookPayloadSchema]):
        super().__init__(payload)
        
        
    def run_workflow(self):
        logger.info("Iniciando workflow do produto Precipitação por Satélite - ONS...")
        try:
            file_path = self.download_extract_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Precipitação por Satélite - PMO finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Precipitação por Satélite - PMO")
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
    logger.info("Iniciando manualmente o workflow do produto Precipitação por Satélite - ONS...")
    try:
        payload = {
            "dataProduto": "06/09/2025",
            "filename": "Vazões Observadas - 09-06-2025 a 06-09-2025.xlsx",
            "macroProcesso": "Programação da Operação",
            "nome": "Relatório de Acompanhamento Hidrológico",
            "periodicidade": "2025-09-06T00:00:00",
            "periodicidadeFinal": "2025-09-06T23:59:59",
            "processo": "Acompanhamento das Condições Hidroenergéticas",
            "s3Key": "webhooks/Relatório de Acompanhamento Hidrológico/68bd994351c7b8ba11d2d11c_Vazões Observadas - 09-06-2025 a 06-09-2025.xlsx",
            "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy81Ni9Qcm9kdXRvcy8yMzQvVmF6w7VlcyBPYnNlcnZhZGFzIC0gMDktMDYtMjAyNSBhIDA2LTA5LTIwMjUueGxzeCIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJSZWxhdMOzcmlvIGRlIEFjb21wYW5oYW1lbnRvIEhpZHJvbMOzZ2ljbyIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzU3MzQyNjQzLCJuYmYiOjE3NTcyNTYwMDN9.j_wawNct0JI1P3fIzLBfL1LWiEElb4pkeOHSaGG3AQ0",
            "webhookId": "68bd994351c7b8ba11d2d11c"
        }
        
        payload = WebhookPayloadSchema(**payload)
        
        relatorioacompanhamento = RelatorioAcompanhamentoHidrologico(payload)
        relatorioacompanhamento.run_workflow()
    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Precipitação por Satélite - ONS: %s", str(e), exc_info=True)
        raise