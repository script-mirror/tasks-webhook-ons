import requests
from .schema import WebhookSintegreSchema
from middle.utils import Constants

constants = Constants()

def get_webhook_payload() -> WebhookSintegreSchema:
    """
    Função para obter o payload do webhook.

    :return: Instância de WebhookSintegreSchema com os dados do payload.
    """
    response = requests.get(f"https://api.example.com/webhook/payload")
    
    if response.status_code == 200:
        return WebhookSintegreSchema(**response.json())
    else:
        raise Exception(f"Erro ao obter payload do webhook: {response.status_code}")