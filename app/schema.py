from pydantic import BaseModel
import datetime
from typing import Optional


class WebhookSintegreSchema(BaseModel):
    dataProduto: str
    filename: str
    macroProcesso: str
    nome: str
    periodicidade: datetime.datetime
    periodicidadeFinal: Optional[datetime.datetime] = None
    processo: str
    s3Key: str
    url: str
    webhookId: str