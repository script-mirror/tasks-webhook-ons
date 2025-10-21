from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class WebhookSintegreSchema(BaseModel):
    dataProduto: str
    filename: str
    macroProcesso: str
    nome: str
    periodicidade: datetime
    periodicidadeFinal: Optional[datetime] = None
    processo: str
    s3Key: str
    url: str
    webhookId: str