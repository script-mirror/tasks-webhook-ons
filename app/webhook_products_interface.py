from abc import ABC, abstractmethod
from middle import s3
from middle.utils import setup_logger 
logger = setup_logger()


class WebhookProductsInterface(ABC):
    """
    Interface para os produtos do webhook ONS.
    Define os metodos base que devem ser implementados no ETL dos produtos do webhook.
    """

    @abstractmethod
    def run_workflow(self):
        """
        Ponto de entrada da execucao do etl.
        Deve implementar a logica de execucao para cada produto.
        """
        pass

    
    def download_files(self, payload_webhook: dict):
        """
        Download files related to the webhook product.
        This method should implement the logic for downloading necessary files based on product details.
        
        :param payload_webhook: Details of the product to download files for.
        :return: A dictionary with the status and message of the download operation.
        """
        
        id_produto = payload_webhook.get('webhookId')
        file_name = payload_webhook.get('filename')
        path_download = "/tmp"
        
        try: 
            s3.download_from_s3(id_produto, file_name, path_download)
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo do S3: {e}")
            return {"status": "error", "message": str(e)}
        