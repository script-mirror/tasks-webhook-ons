import os
import sys
import pdb
import requests
from datetime import datetime
import pandas as pd
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.schema import WebhookSintegreSchema  # noqa: E402
from middle.utils import ( # noqa: E402
    setup_logger,
    Constants,
    get_auth_header,
)
from middle.airflow import trigger_dag

from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
logger = setup_logger()
constants = Constants()


class Acomph(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        logger.info("Inicializando Acomph")
        super().__init__(payload)
        self.trigger_dag = trigger_dag
    
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        pass

    def post_data(self, df: pd.DataFrame) -> dict:
        pass

class GenerateTable():
    def __init__(self):
        pass
    
    def get_acomph(self):
        pass


if __name__ == "__main__":
    obj = Acomph(WebhookSintegreSchema(
            **{
  "dataProduto": "24/09/2025",
  "filename": "ECMWF_precipitacao14d_20250924.zip",
  "macroProcesso": "Programacao da Operacao",
  "nome": "Modelo ECMWF",
  "periodicidade": "2025-09-24T00:00:00",
  "periodicidadeFinal": "2025-09-24T23:59:59",
  "processo": "Meteorologia e clima",
  "s3Key": "webhooks/Modelo ECMWF/68d3bf45450014d70a3e5f9b_ECMWF_precipitacao14d_20250924.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8zOC9Qcm9kdXRvcy81NTEvRUNNV0ZfcHJlY2lwaXRhY2FvMTRkXzIwMjUwOTI0LnppcCIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJNb2RlbG8gRUNNV0YiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1ODc5NDE2NSwibmJmIjoxNzU4NzA3NTI1fQ.RybnfpUWZ5Ml27NTeSlVJj4iXEJtnOSMeI5iluXqEaI",
  "webhookId": "68d3bf45450014d70a3e5f9b"
}
    ))
    obj.run_workflow()
