from typing import Optional, Dict, Any
from app.webhook_products_interface import WebhookProductsInterface

class DecksNewave(WebhookProductsInterface):
    def __init__(self, conf: Optional[Dict[str, Any]] = None):
        self.conf = conf
        
        # Fluxo de execução
        self.product_details = self.get_product_details()
        self.payload_download = self.download_arquivos(product_details)
        self.payload_task2 = self.extrair_arquivos(self.payload_download)
        
    def run_workflow(self):
        """
        Executa o fluxo de tarefas para o DECK Newave.
        """
        print("Iniciando o fluxo de tarefas para o DECK Newave...")
        
        if self.payload_download.get("status") == "success":
            print("Tarefa 1 concluída com sucesso.")
        else:
            print("Tarefa 1 falhou.")
            return
        
        if self.payload_task2.get("status") == "success":
            print("Tarefa 2 concluída com sucesso.")
        else:
            print("Tarefa 2 falhou.")
            return
        
        print("Fluxo de tarefas para o DECK Newave concluído com sucesso.")    
        
    def download_arquivos(product_details) -> Dict[str, Any]:
        """
        Tarefa para baixar arquivos do DECK Newave.
        """
        
        try:
            if not product_details:
                raise ValueError("Detalhes do produto não fornecidos.")
            
            print(f"Baixando arquivos para o produto: {product_details['nomeProduto']}")
            
            return {"status": "success", "message": "Arquivos baixados com sucesso."}
        
        except Exception as e:
            print(f"Erro ao baixar arquivos: {e}")
            return {"status": "error", "message": str(e)}
        
    def extrair_arquivos(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tarefa para extrair arquivos do DECK Newave.
        """
        
        try:
            if not payload:
                raise ValueError("Payload da tarefa 1 não fornecido.")
            
            print("Extraindo arquivos do DECK Newave...")
            
            return {"status": "success", "message": "Arquivos extraídos com sucesso."}
        
        except Exception as e:
            print(f"Erro ao extrair arquivos: {e}")
            return {"status": "error", "message": str(e)}
    
    
if __name__ == "__main__":
    debug_conf = {}
    
    decks_newave = DecksNewave(debug_conf)
    
    decks_newave.get_product_details()