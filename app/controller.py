from middle.utils import setup_logger
from .constants import PRODUCT_MAPPING
from .schema import WebhookSintegreSchema
from fastapi import APIRouter, HTTPException

logger = setup_logger()

router = APIRouter()

@router.post("/webhook")
def webhook_handler(payload: WebhookSintegreSchema):
    product_handler = PRODUCT_MAPPING[payload.nome](payload)
    result = product_handler.run_workflow()
    return result
    
