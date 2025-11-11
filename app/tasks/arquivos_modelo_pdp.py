from datetime import datetime
from typing import Optional
from pathlib import Path
import pdb
import sys
import pandas as pd
import glob
import os
import requests

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import ( 
    get_auth_header
)

from middle.utils.file_manipulation import extract_zip
from middle.utils import setup_logger, Constants
logger = setup_logger()
constants = Constants()

class ArquivosModelosPDP(WebhookProductsInterface):
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
    
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        logger.info("Iniciando workflow do produto Arquivos dos modelos de previsão de vazões diárias - PDP...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Arquivos dos modelos de previsão de vazões diárias - PDP finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Arquivos dos modelos de previsão de vazões diárias - PDP")
            raise
        
    def run_process(self, file_path):
        
        process_result = self.process_file(file_path)
        
        self.post_data(process_result)
        
        pass
    
    def process_file(self, file_path):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            unzip_path = extract_zip(file_path)
            path_lista_vazoes = project_root / "app" / "files" / "relatorio_acompanhamento_hidrologico" / "info_vazao_obs.json"
            
            df_lista_vazoes = pd.read_json(path_lista_vazoes)

            values = []
            for subbacia in df_lista_vazoes.keys():
                try:
                    if subbacia == 'GOV. JAYME CANET':
                        subbacia = 'MAUA'
                    
                    file_subbacia = glob.glob(os.path.join(unzip_path,'**',f'{subbacia}.txt'),recursive=True)[0]
                    
                    if not file_subbacia:
                        raise FileNotFoundError(f"Arquivo não encontrado para a subbacia: {subbacia}")
                except Exception as e:
                    logger.warning(str(e))
                    
                df_subbacia = pd.read_csv(file_subbacia,sep='|',header=None)
                df_subbacia['subbacia'] = subbacia
                values += df_subbacia[['subbacia',0,3,4,5]].values.tolist()

            df_load = pd.DataFrame(values,columns=['txt_subbacia','cd_estacao','txt_tipo_vaz','dt_referente','vl_vaz'])
            
            return df_load
                
        except Exception as e:
            logger.error("Erro ao processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
            
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        try:
            
            res = requests.post(
                constants.POST_RODADAS_VAZAO_OBSERVADA_PDP,   
                headers=get_auth_header(),
                json=process_result.to_dict(orient='records')
            )
            
            res.raise_for_status()
            if res.status_code >= 200 and res.status_code < 300:
                logger.info("Dados da tabela (tb_vazoes_obs) em (db_rodadas) enviados com sucesso. Status Code: %s", res.status_code)
            else:
                raise Exception("Falha ao enviar dados da tabela (tb_vazoes_obs) em (db_rodadas). Status Code: {}".format(res.status_code)) 
            
        except Exception as e:
            raise
        

if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Arquivos dos modelos de previsão de vazões diárias - PDP...")
    try:
        payload = {
  "dataProduto": "09/11/2025",
  "filename": "Modelos_Chuva_Vazao_20251109.zip",
  "macroProcesso": "Programação da Operação",
  "nome": "Arquivos dos modelos de previsão de vazões diárias - PDP",
  "periodicidade": "2025-11-09T00:00:00",
  "periodicidadeFinal": "2025-11-09T23:59:59",
  "processo": "Previsão de Vazões Diárias - PDP",
  "s3Key": "webhooks/Arquivos dos modelos de previsão de vazões diárias - PDP/c093350a-fc51-496a-b346-d2e8a83dc142_Modelos_Chuva_Vazao_20251109.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy84Mi9Qcm9kdXRvcy8yMzgvTW9kZWxvc19DaHV2YV9WYXphb18yMDI1MTEwOS56aXAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiQXJxdWl2b3MgZG9zIG1vZGVsb3MgZGUgcHJldmlzw6NvIGRlIHZhesO1ZXMgZGnDoXJpYXMgLSBQRFAiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc2Mjc5NjY5MiwibmJmIjoxNzYyNzEwMDUyfQ.cUYJldK7DPmvrCHofKfyS-RgZhfxCHnonYge8C93oiw",
  "webhookId": "c093350a-fc51-496a-b346-d2e8a83dc142"
}
        
        payload = WebhookSintegreSchema(**payload)
        
        arquivos_modelos = ArquivosModelosPDP(payload)
        
        arquivos_modelos.run_workflow()
        

    except Exception as e:
        logger.error("Erro no fluxo manual de processamento dos Arquivos dos modelos de previsão de vazões diárias - PDP: %s", str(e), exc_info=True)
        raise