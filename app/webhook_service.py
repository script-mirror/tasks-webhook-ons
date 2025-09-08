import requests
from .schema import WebhookPayloadSchema
from middle.utils import Constants

constants = Constants()

def get_webhook_payload() -> WebhookPayloadSchema:
    """
    Função para obter o payload do webhook.

    :return: Instância de WebhookPayloadSchema com os dados do payload.
    """
    response = requests.get(f"https://api.example.com/webhook/payload")
    
    if response.status_code == 200:
        return WebhookPayloadSchema(**response.json())
    else:
        raise Exception(f"Erro ao obter payload do webhook: {response.status_code}")