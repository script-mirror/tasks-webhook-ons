from abc import ABC, abstractmethod
from middle import s3
from middle.utils import setup_logger, extract_zip
from typing import Optional, Dict, Any
from .schema import WebhookSintegreSchema
from .webhook_service import get_webhook_payload
logger = setup_logger()


class WebhookProductsInterface(ABC):
    """
    Interface para os produtos do webhook ONS.
    Define os metodos base que devem ser implementados no ETL dos produtos do webhook.
    """
    
    def __init__(self, payload: Optional[WebhookSintegreSchema] = None):
        self.payload:WebhookSintegreSchema = payload
        self.workflow_results:dict = {}

    @abstractmethod
    def run_workflow(self):
        """
        Ponto de entrada da execucao do etl.
        Deve implementar a logica de execucao para cada produto.
        """
        pass

    
    def download_files(self) -> str:
        """
        Método responsável por baixar os arquivos necessários com base nos detalhes do produto.
        
        :param payload: Utiliza dos detalhes do produto para determinar quais arquivos baixar.
        :return: Um dicionário com o status do download e mensagens associadas.
        """
        
        id_produto = self.payload.webhookId
        filename = self.payload.filename
        path_to_send = "/tmp"
        
        try: 
            filepath_to_extract = s3.download_from_s3(id_produto, filename, path_to_send)
            logger.info(f"Arquivo {filename} baixado com sucesso para {path_to_send}")
            if ".zip" in filename:
                filepath_to_extract = extract_zip(filepath_to_extract, filename, path_to_send)
            
            
            return {"status": "success", "filepath": filepath_to_extract}
        
        except Exception as e:
            
            logger.error(f"Erro ao baixar arquivo do S3: {e}")
            raise Exception(f"Erro ao baixar arquivo do S3: {e}")
        