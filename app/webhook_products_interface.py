from abc import ABC, abstractmethod
from middle import s3
from middle.utils import setup_logger 
from typing import Optional,Dict, Any
logger = setup_logger()


class WebhookProductsInterface(ABC):
    """
    Interface for webhook products.
    This interface defines the methods that must be implemented by any class that handles webhook products.
    """
    
    def __init__(self, conf: Optional[Dict[str, Any]] = None):
        """
        Initialize the interface with configuration.
        
        Args:
            conf: Configuration dictionary containing product details.
        """
        self.conf = conf or {}
        self.product_details = None
        self.workflow_results = {}

    @abstractmethod
    def run_workflow(self):
        """
        Execute the flow for processing webhook products.
        This method should be implemented to handle the specific logic for processing webhook products.
        """
        pass

    def get_product_details(self):
        """
        Método responsável por obter os detalhes do produto a partir da configuração fornecida.
        
        :param conf: Details da dag_run com a chave 'product_details'.
        :return: Um dicionário com os detalhes do produto
        """
        try:
            if not self.conf or 'product_details' not in self.conf:
                raise ValueError("Configuração inválida: configuração ou 'product' não encontrado.")

            product_details = self.conf['product_details']
            
            logger.info("Payload do DECK Newave recebido com sucesso!")
            
            return product_details
        
        except Exception as e:
            logger.error(f"Erro em obter o payload do DECK Newave: {e}")
    
    def download_files(self, product_details):
        """
        Método responsável por baixar os arquivos necessários com base nos detalhes do produto.
        
        :param product_details: Utiliza dos detalhes do produto para determinar quais arquivos baixar.
        :return: Um dicionário com o status do download e mensagens associadas.
        """
        
        id_produto = product_details.get('webhookId')
        file_name = product_details.get('filename')
        path_to_send = "/tmp"
        
        try: 
            filepath_to_extract = s3.download_from_s3(id_produto, file_name, path_to_send)
            logger.info(f"Arquivo {file_name} baixado com sucesso para {path_to_send}")
            
            return {"status": "success", "filepath": filepath_to_extract}
        
        except Exception as e:
            
            logger.error(f"Erro ao baixar arquivo do S3: {e}")
            return {"status": "error", "message": str(e)}
        