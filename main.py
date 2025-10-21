import sys
from middle.utils import setup_logger
from app.constants import PRODUCT_MAPPING
from app.schema import WebhookSintegreSchema
from middle.utils import sanitize_string
from app.webhook_products_interface import WebhookProductsInterface

logger = setup_logger()


def webhook_handler(payload: WebhookSintegreSchema):
    payload.nome = sanitize_string(payload.nome, space_char="_")
    product_handler: WebhookProductsInterface | None = PRODUCT_MAPPING[payload.nome]
    if not product_handler:
        logger.error(f"Produto {payload.nome} nao encontrado no mapeamento")
        raise ValueError("Produto nao mapeado")
    product_handler = PRODUCT_MAPPING[payload.nome](payload)
    result = product_handler.run_workflow()
    return result

if __name__ == "__main__":
    logger.info("Iniciando aplicacao webhook ONS")
    if len(sys.argv) >= 1:
        payload = sys.argv[1]
        payload = eval(payload)
        webhook_handler(WebhookSintegreSchema(**payload))
        logger.info(f"Payload: {payload}")
    else:
        raise ValueError("Payload nao fornecido corretamente")    
    
    logger.info("Aplicacao finalizada com sucesso")