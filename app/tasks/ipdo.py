import pdb
from datetime import datetime, date
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


class Ipdo(WebhookProductsInterface):
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        if not filepath:
            filepath = self.download_files()
    
        self.run_process(filepath)
        

    def run_process(self, filepath: str):
        body = self.process_file(filepath)
        self.post_data(body)
        imagem = self.pdf_to_jpg(filepath, 2)
        mensagem = filepath[filepath.rfind("/")+1:-4].replace("-", " ", 1).replace("-", "/")
        send_whatsapp_message("condicao hidrica", mensagem, imagem)
        send_email_message(
            user=constants.EMAIL_IPDO,
            destinatario=[constants.EMAIL_MIDDLE, constants.EMAIL_FRONT],
            mensagem=mensagem,
            arquivos=[filepath]
        )


    def post_data(self, process_result: dict) -> dict:
        res = requests.post(
            constants.ENDPOINT_IPDO, json=process_result, headers=get_auth_header()
        )
        res.raise_for_status()
        return res.json()
    
    
    def process_file(self, path: str):
        print(f"Leitura do arquivo: {path}")
        with pdfplumber.open(path) as pdf:
            texto = "".join(page.extract_text() for page in pdf.pages)
        linhas = texto.split('\n')
        meses = {
            'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
            'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
        }
        data_str = linhas[1].split(',')[1].strip()
        dia, mes_nome, _, ano = data_str.split(' ')
        mes = meses[mes_nome]
        data_final = date(int(ano), mes, int(dia))

        carga_sul = int(linhas[40].split()[1].replace('.', ''))
        carga_sudeste = int(linhas[41].split()[3].replace('.', ''))
        carga_norte = int(linhas[42].split()[1].replace('.', ''))
        carga_nordeste = int(linhas[43].split()[1].replace('.', ''))
        return {
            "dt_referente": str(data_final),
            "carga_se": carga_sudeste,
            "carga_s": carga_sul,
            "carga_ne": carga_nordeste,
            "carga_n": carga_norte
            }
        

    def pdf_to_jpg(
        self, 
        path: str,
        page_number: int,
        path_output: str = None
        ):
        pages = convert_from_path(path, 100)
        img = pages[page_number-1]
        img_bytes = BytesIO()
        img.save(img_bytes, 'JPEG')
        img_bytes.seek(0)
        if path_output is not None:
            img.save(path_output, 'JPEG')
        return img_bytes.getvalue()


        
if __name__ == "__main__":
    teste = Ipdo(WebhookSintegreSchema(**{
  "dataProduto": "28/08/2025",
  "filename": "IPDO-28-08-2025.pdf",
  "macroProcesso": "Operação do Sistema",
  "nome": "IPDO (Informativo Preliminar Diário da Operação)",
  "periodicidade": "2025-08-28T00:00:00",
  "periodicidadeFinal": "2025-08-28T23:59:59",
  "processo": "Operação em Tempo Real",
  "s3Key": "webhooks/IPDO (Informativo Preliminar Diário da Operação)/68b19e9b51c7b8ba11d2c903_IPDO-28-08-2025.pdf",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvNy8zOS9Qcm9kdXRvcy8xNTUvSVBETy0yOC0wOC0yMDI1LnBkZiIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJJUERPIChJbmZvcm1hdGl2byBQcmVsaW1pbmFyIERpw6FyaW8gZGEgT3BlcmHDp8OjbykiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1NjU1NzU3OSwibmJmIjoxNzU2NDcwOTM5fQ.yWe68er5Y0puGMtFmkzh0i31DLAiDo1iySRCwOYNlrk",
  "webhookId": "68b19e9b51c7b8ba11d2c903"
}))
    teste.run_workflow()