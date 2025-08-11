from typing import Dict, Any, Optional
class TasksDecksNewave:
    """
    Essa classe é responsável por gerenciar as tarefas relacionadas aos decks do Newave.
    """

    def __init__(self, conf: Optional[Dict[Any]] = None):
        self.conf = conf if conf else {}
        
        pass

    def get_product_details(self, conf) -> Dict[Any]:
        """
        Retorna os detalhes do produto.
        """
        try:
            if conf: 
                return self.conf.get('product_details', {})
            else:
                raise Exception("Configuração não fornecida.")
        except Exception as e:
            print(f"Erro ao obter detalhes do produto: {str(e)}")
           
        
        
    