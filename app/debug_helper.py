import datetime
import inspect
import re
from typing import Dict, Any, Optional, Callable, Type
import pandas as pd
from app.webhook_products_interface import WebhookProductsInterface

class Debugger:
    """
    Classe auxiliar genérica para debugar métodos de qualquer classe de produto.

        if __name__ == "__main__":
        
        from app.debug_helper import Debugger

        metodo_para_debugar = "metodo"  

        payload = {}

        debugger = Debugger()

        debugger.testar_metodo(ClasseDoProduto, metodo_para_debugar, payload)
    """
    
    def __init__(self):
        """
        Inicializa o debugger com valores padrão.
        """
        self.results_cache = {}
    
    def _detect_method_dependencies(self, instance, method: Callable) -> Dict[str, str]:
        """
        Detecta automaticamente as dependências de um método baseado nos parâmetros 
        nomeados com sufixo '_result'.
        
        Args:
            instance: Instância da classe que contém o método
            method: Método a ser analisado
            
        Returns:
            Dicionário mapeando nome de parâmetro para nome do método correspondente
        """
        # Obter os parâmetros do método
        sig = inspect.signature(method)
        dependencies = {}
        
        # Para cada parâmetro, tentar identificar o método correspondente
        for param_name in sig.parameters:
            if param_name == 'self':
                continue
                
            # Se o parâmetro termina com '_result', é provavelmente uma dependência
            if param_name.endswith('_result'):
                # Extrair o nome do método removendo o sufixo '_result'
                method_name = param_name[:-7]  # remove '_result'
                
                # Verificar se o método existe na instância
                if hasattr(instance, method_name):
                    dependencies[param_name] = method_name
                
        return dependencies

    def testar_metodo(self, produto_class: Type[WebhookProductsInterface], metodo: str, 
                     payload: Optional[Dict[str, Any]] = None, 
                     args_override: Optional[Dict[str, Any]] = None) -> Any:
        """
        Testa um método específico executando suas dependências automaticamente.
        
        Args:
            produto_class: Classe do produto a ser testado
            metodo: Nome do método a ser testado
            payload: Payload a ser usado (opcional)
            args_override: Argumentos específicos para sobrescrever (opcional)
            
        Returns:
            Resultado da execução do método
        """
        from pprint import pprint
        import json
        
        # Criar instância do produto
        if payload is None:
            print("Aviso: Payload vazio fornecido")
            payload = {}
        
        # Converter payload para WebhookPayloadSchema se for um dicionário
        if isinstance(payload, dict) and payload:
            try:
                from app.schema import WebhookPayloadSchema
                payload_obj = WebhookPayloadSchema(**payload)
            except Exception as e:
                print(f"Aviso: Não foi possível converter payload para WebhookPayloadSchema: {e}")
                payload_obj = payload
        else:
            payload_obj = payload
            
        # Criar instância do produto
        instance = produto_class(payload_obj)
        
        # Verificar se o método existe
        if not hasattr(instance, metodo):
            raise ValueError(f"Método {metodo} não encontrado na classe {produto_class.__name__}")
        
        method = getattr(instance, metodo)
        
        # Determinar dependências
        dependencies = self._detect_method_dependencies(instance, method)
        print(f"Dependências do método {metodo}: {list(dependencies.keys())}")
        
        # Preparar argumentos para o método
        args = {}
        
        # Executar dependências para obter os resultados
        for param_name, method_name in dependencies.items():
            # Se já temos um resultado em cache, usá-lo
            if method_name in self.results_cache:
                args[param_name] = self.results_cache[method_name]
                print(f"Usando resultado em cache para {method_name}")
                continue
                
            # Se o método é download_extract_files, executar primeiro
            if method_name == "download_extract_files" and hasattr(instance, method_name):
                print(f"Executando {method_name}...")
                download_result = getattr(instance, method_name)()
                # Adicionar data_produto e filename ao resultado se disponíveis no payload
                if isinstance(download_result, dict):
                    if hasattr(payload_obj, 'dataProduto') and 'data_produto' not in download_result:
                        download_result['data_produto'] = payload_obj.dataProduto
                    if hasattr(payload_obj, 'filename') and 'filename' not in download_result:
                        download_result['filename'] = payload_obj.filename
                self.results_cache[method_name] = download_result
                args[param_name] = download_result
                continue
                
            # Executar dependência recursivamente
            if hasattr(instance, method_name):
                print(f"Executando dependência: {method_name}")
                dep_method = getattr(instance, method_name)
                # Verificar dependências da dependência
                dep_dependencies = self._detect_method_dependencies(instance, dep_method)
                dep_args = {}
                for dep_param, dep_method_name in dep_dependencies.items():
                    if dep_method_name in self.results_cache:
                        dep_args[dep_param] = self.results_cache[dep_method_name]
                
                result = dep_method(**dep_args)
                self.results_cache[method_name] = result
                args[param_name] = result
        
        # Sobrescrever argumentos se fornecidos
        if args_override:
            args.update(args_override)
            
        print(f"\nExecutando método {metodo} com argumentos:")
        pprint(args)
        print("\n")
        
        # Executar o método
        return method(**args)