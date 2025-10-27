import pandas as pd
from typing import Optional
from pathlib import Path
import pdb
import sys
import glob
import os
from datetime import datetime, date

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants, html_to_image
from middle.utils.file_manipulation import extract_zip
from middle.message.sender import send_email_message, send_whatsapp_message
from middle.airflow import trigger_dag
logger = setup_logger()
constants = Constants()


class NotasTecnicasMedioPrazo(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.dt_produto = self.payload.dataProduto
        self.trigger_dag = trigger_dag

        
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        logger.info("Iniciando workflow do produto Notas Técnicas - Medio Prazo...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Notas Técnicas - Medio Prazo finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Notas Técnicas - Medio Prazo")
            raise
        
    def run_process(self, file_path):
        
        excel_file = self.process_file(file_path)
        
        self.enviar_whatsapp(excel_file, assunto=f"Notas Técnicas - Médio Prazo({self.dt_produto})")
        
        if f"GTMIN_CCEE_{(date.today().replace(day=1, month=date.today().month+1)).strftime('%m%Y')}" in excel_file.upper():
            self.trigger_dag(dag_id="1.17-NEWAVE_ONS-TO-CCEE", conf=payload)
    
    def process_file(self, file_path):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            unzip_path = extract_zip(file_path)
            
            excel_file = glob.glob(os.path.join(unzip_path, "*.xlsx"))
            
            return excel_file[0]
            
        except Exception as e:
            logger.error("Falha em processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        pass
    
    
    def enviar_whatsapp(self, arquivo: str, assunto: str):
        try:
            
            send_whatsapp_message(destinatario=self.destinatario_whatsapp,mensagem=assunto,arquivo=arquivo)
            
        except Exception as e:
            logger.error("Falha ao enviar mensagem WhatsApp: %s", str(e), exc_info=True)
            raise
          
          
if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Notas Técnicas - Medio Prazo...")
    try:
        payload = {
            "dataProduto": "10/2025",
            "filename": "Notas Técnicas - Medio Prazo.zip",
            "macroProcesso": "Programação da Operação",
            "nome": "Notas Técnicas - Medio Prazo",
            "periodicidade": "2025-10-01T00:00:00",
            "periodicidadeFinal": "2025-10-31T23:59:59",
            "processo": "Médio Prazo",
            "s3Key": "webhooks/Notas Técnicas - Medio Prazo/68dab8ed995e02e90c3fc60c_Notas Técnicas - Medio Prazo.zip",
            "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiIvc2l0ZXMvOS81Mi83MS9Qcm9kdXRvcy8yODgvMjItMDktMjAyNV8xMjQwMDAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiTm90YXMgVMOpY25pY2FzIC0gTWVkaW8gUHJhem8iLCJJc0ZpbGUiOiJGYWxzZSIsImlzcyI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiYXVkIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJleHAiOjE3NTkyNTEyOTMsIm5iZiI6MTc1OTE2NDY1M30.ADydMSGC2K68XRiKvJWReJ73HY_eh4LLWhSki9uYTB8",
            "webhookId": "68dab8ed995e02e90c3fc60c"
        }
        
        payload = WebhookSintegreSchema(**payload)
        
        notas_tecnicas = NotasTecnicasMedioPrazo(payload)
        
        notas_tecnicas.run_workflow()

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Resultados Preliminares Nao Consistidos: %s", str(e), exc_info=True)
        raise