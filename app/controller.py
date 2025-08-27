from middle.utils import setup_logger
from .constants import PRODUCT_MAPPING
from .schema import WebhookSintegreSchema
from fastapi import APIRouter, HTTPException
from middle.utils import sanitize_string
from .webhook_products_interface import WebhookProductsInterface

logger = setup_logger()

router = APIRouter()

@router.post("/webhook")
def webhook_handler(payload: WebhookSintegreSchema):
    payload.nome = sanitize_string(payload.nome, space_char="_")
    product_handler: WebhookProductsInterface | None = PRODUCT_MAPPING[payload.nome]
    if not product_handler:
        logger.error(f"Produto {payload.nome} nao encontrado no mapeamento")
        raise HTTPException(status_code=404, detail="Produto nao mapeado")
    product_handler = PRODUCT_MAPPING[payload.nome](payload)
    result = product_handler.run_workflow()
    return result