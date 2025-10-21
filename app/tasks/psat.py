import pdb
from datetime import datetime
import os
import sys
from typing import Optional
from pdf2image import convert_from_path
from io import BytesIO
import pdfplumber
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.schema import WebhookSintegreSchema
from app.webhook_products_interface import WebhookProductsInterface
from middle.message import send_whatsapp_message, send_email_message
from middle.utils import (
    Constants,
    get_auth_header,
    setup_logger,
)

constants = Constants()
logger = setup_logger()


class Psat(WebhookProductsInterface):
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        pass