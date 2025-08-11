from abc import ABC, abstractmethod
from middle import s3
from middle.utils import setup_logger 
logger = setup_logger()


class WebhookProductsInterface(ABC):
    """
    Interface for webhook products.
    This interface defines the methods that must be implemented by any class that handles webhook products.
    """

    @abstractmethod
    def run_workflow(self):
        """
        Execute the flow for processing webhook products.
        This method should be implemented to handle the specific logic for processing webhook products.
        """
        pass

    def get_product_details(self):
        """
        Retrieves product details from the configuration.
        
        :param conf: Configuration dictionary containing product details.
        :return: A dictionary with product details.
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
        Download files related to the webhook product.
        This method should implement the logic for downloading necessary files based on product details.
        
        :param product_details: Details of the product to download files for.
        :return: A dictionary with the status and message of the download operation.
        """
        
        id_produto = product_details.get('webhookId')
        file_name = product_details.get('filename')
        path_download = "/tmp"
        
        try: 
            s3.download_from_s3(id_produto, file_name, path_download)
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo do S3: {e}")
            return {"status": "error", "message": str(e)}
        
        pass