import os
from abc import ABC, abstractmethod
from middle import s3
from middle.utils import setup_logger, extract_zip
from typing import Optional, Dict, Any
from .schema import WebhookPayloadSchema
import pandas as pd
# from .webhook_service import get_webhook_payload
from middle.utils import Constants
logger = setup_logger()
constants = Constants()

class WebhookProductsInterface(ABC):
    """
    Interface para os produtos do webhook ONS.
    Define os metodos base que devem ser implementados no ETL dos produtos do webhook.
    """
    
    def __init__(self, payload: Optional[WebhookPayloadSchema] = None):
        self.payload:WebhookPayloadSchema = payload
        self.run_workflow_results:dict = {}



    @abstractmethod
    def run_workflow(self, filepath:Optional[str] = None):
        """
        Ponto de entrada da execucao do etl.
        Deve implementar a logica de execucao para cada produto e invocar o run_process para rodar a lógica.
        """
        pass
    
    @abstractmethod
    def run_process(self, filepath: str):
        pass

    
    def download_extract_files(self) -> str:
        """
        Método responsável por baixar os arquivos necessários com base nos detalhes do produto.
        
        :param payload: Utiliza dos detalhes do produto para determinar quais arquivos baixar.
        :return: Um dicionário com o status do download e mensagens associadas.
        """
        
        id_produto = self.payload.webhookId
        filename = self.payload.filename
        path_to_send = constants.PATH_TMP
        os.makedirs(path_to_send, exist_ok=True)
        logger.debug("Criado caminho temporário para o arquivo: %s", path_to_send)
        
        try: 
            filepath_to_extract = s3.download_from_s3(id_produto, filename, path_to_send)
            logger.info(f"Arquivo {filename} baixado com sucesso para {path_to_send}")
            
            return filepath_to_extract
        
        except Exception as e:
            
            logger.error(f"Erro ao baixar arquivo do S3: {e}")
            raise Exception(f"Erro ao baixar arquivo do S3: {e}")
     
        
    def process_file(self, basepath) -> str:
        """
        Método responsável pela leitura do arquivo baixado

        Args:
            basepath: caminho do arquivo extraido do webhook
        """
        pass
        
        
    @abstractmethod
    def post_data(self, process_result: pd.DataFrame) -> dict:
        """
        Método responsável por inserir os dados lidos e processados no banco através da nossa API

        Args:
            process_result: dados processados para inserção
        """
        