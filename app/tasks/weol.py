import datetime
import os
import sys
import zipfile
import io
import csv
import pandas as pd
import requests
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.schema import WebhookSintegreSchema
from app.webhook_products_interface import WebhookProductsInterface
from middle.utils import html_to_image, get_auth_header, setup_logger, Constants
from middle.message import send_whatsapp_message, send_email_message
from middle.utils.date_utils import SemanaOperativa
from middle.airflow import trigger_dag
constants = Constants()
logger = setup_logger()


class Weol(WebhookProductsInterface):
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        
    def run_workflow(self, filepath: Optional[str] = None):
        if not filepath:
            filepath = self.download_files()
    
        self.run_process(filepath)

    def run_process(self, filepath: str):
        self.deck_prev_eolica_semanal_previsao_final(filepath)
        self.deck_prev_eolica_semanal_patamares(filepath)
        
        data_produto = datetime.datetime.strptime(self.payload.dataProduto, "%d/%m/%Y").date()
        
        self.gerar_tabela_mensal({'data': data_produto})
        self.gerar_tabela_semanal({'data': data_produto})
        self.gerar_tabela_diferenca({'data': data_produto})
        trigger_dag("1.18-PROSPEC_UPDATE", {"produto": "EOLICA"})

    def ler_csv_prev_weol_para_dicionario(self, file):
        reader = csv.reader(file, delimiter=';')
        headers = next(reader)[1:]
        data_dict = {}
        for row in reader:
            regiao = row[0]
            valores = row[1:]
            for i in range(0, len(headers), 3):
                data_key = f"{headers[i]} {headers[i+2]}"
                if data_key not in data_dict:
                    data_dict[data_key] = {}
                if regiao != "Patamares":
                    data_dict[data_key][regiao] = {
                        "pesado": valores[i],
                        "medio": valores[i+1],
                        "leve": valores[i+2]
                    }
        return data_dict

    def deck_prev_eolica_semanal_previsao_final(self, filepath: str):
        try:
            zip_file = zipfile.ZipFile(filepath)
        except Exception as e:
            logger.error(f"Erro ao abrir arquivo ZIP: {filepath} - {e}")
            return
        
        prev_eol_csv_path = [x for x in zip_file.namelist() if "Arquivos Saida/Previsoes Subsistemas Finais/Total/Prev_" in x and ".csv" in x]
        if not prev_eol_csv_path:
            logger.error("Arquivo CSV de previsão eólica não encontrado")
            return
        prev_eol_csv_path = prev_eol_csv_path[0]
        
        body_weol = []
        with zip_file.open(prev_eol_csv_path) as file:
            content = file.read()
            info_weol = self.ler_csv_prev_weol_para_dicionario(io.StringIO(content.decode("latin-1")))
            for data in info_weol.keys():
                data_inicio: datetime.datetime = datetime.datetime.strptime(data.split(" ")[0], "%d/%m/%Y")
                data_fim: datetime.datetime = datetime.datetime.strptime(data.split(" ")[1], "%d/%m/%Y")
                for submercado in info_weol[data].keys():                
                    for patamar in info_weol[data][submercado].keys():
                        body_weol.append({
                            "inicio_semana": str(data_inicio.date()),
                            "final_semana": str(data_fim.date()),
                            "rv_atual": SemanaOperativa(data_inicio.date()).current_revision,
                            "mes_eletrico": SemanaOperativa(data_inicio.date()).ref_month,
                            "submercado": submercado,
                            "patamar": patamar,
                            "valor": info_weol[data][submercado][patamar],
                            "data_produto": str(datetime.datetime.strptime(self.payload.dataProduto, "%d/%m/%Y").date())
                        })
            
        self.post_data(body_weol)

    def post_data(self, process_result: list) -> dict:
        post_decks_weol = requests.post(
            "https://tradingenergiarz.com/api/v2/decks/weol",
            json=process_result,
            headers=get_auth_header()
        )
        if post_decks_weol.status_code == 200:
            logger.info("WEOL inserido com sucesso")
        else:
            logger.error(f"Erro ao inserir WEOL. status code: {post_decks_weol.status_code}")
        post_decks_weol.raise_for_status()
        return post_decks_weol.json()

    def deck_prev_eolica_semanal_patamares(self, filepath: str):
        try:
            zip_file = zipfile.ZipFile(filepath)
        except Exception as e:
            logger.error(f"Erro ao abrir arquivo ZIP: {filepath} - {e}")
            return
            
        patamates_csv_path = [x for x in zip_file.namelist() if "Arquivos Entrada/Dados Cadastrais/Patamares_" in x and ".csv" in x]
        if not patamates_csv_path:
            logger.error("Arquivo CSV de patamares não encontrado")
            return
        patamates_csv_path = patamates_csv_path[0]
        
        with zip_file.open(patamates_csv_path) as file:
            content = file.read()
            df_patamares = pd.read_csv(io.StringIO(content.decode("latin-1")), sep=";")
            df_patamares.columns = [x[0].lower() + x[1:] for x in df_patamares.columns]
        
        df_patamares.columns = ['inicio','patamar','cod_patamar','dia_semana','dia_tipico','tipo_dia','intervalo','dia','semana','mes']
        
        post_patamates = requests.post(
            "https://tradingenergiarz.com/api/v2/decks/patamares",
            json=df_patamares.to_dict("records"),
            headers=get_auth_header()
        )
        if post_patamates.status_code == 200:
            logger.info("Patamares inseridos com sucesso")
        else:
            logger.error(f"Erro ao inserir patamares. status code: {post_patamates.status_code}")
        
    def gerar_tabela_mensal(self, parametros):
        data: datetime.date = parametros['data']
        
        res = requests.get(
            "https://tradingenergiarz.com/api/v2/decks/weol/weighted-average/month/table",
            params={"dataProduto": str(data), "quantidadeProdutos": 15},
            headers=get_auth_header()
        )
        html = res.json()["html"]
        imagem = html_to_image(html)

        mensagem = f"WEOL Mensal ({(data + datetime.timedelta(days=1)).strftime('%d/%m/%Y')})"
        send_whatsapp_message("debug", mensagem, imagem)
        # send_email_message(
            # user="WEOL",
            # destinatario=[constants.EMAIL_MIDDLE if hasattr(constants, 'EMAIL_MIDDLE') else "middle@example.com", 
                        #  constants.EMAIL_FRONT if hasattr(constants, 'EMAIL_FRONT') else "front@example.com"],
            # mensagem=mensagem,
            # arquivos=[]
        # )

    def gerar_tabela_semanal(self, parametros):
        data: datetime.date = parametros['data']
        
        res = requests.get(
            "https://tradingenergiarz.com/api/v2/decks/weol/weighted-average/week/table",
            params={"dataProduto": str(data), "quantidadeProdutos": 15},
            headers=get_auth_header()
        )
        html = res.json()["html"]
        imagem = html_to_image(html)

        mensagem = f"WEOL Semanal ({(data + datetime.timedelta(days=1)).strftime('%d/%m/%Y')})"
        send_whatsapp_message("debug", mensagem, imagem)
        # send_email_message(
        #     user="WEOL",
        #     destinatario=[constants.EMAIL_MIDDLE if hasattr(constants, 'EMAIL_MIDDLE') else "middle@example.com", 
        #                  constants.EMAIL_FRONT if hasattr(constants, 'EMAIL_FRONT') else "front@example.com"],
        #     mensagem=mensagem,
        #     arquivos=[]
        # )

    def gerar_tabela_diferenca(self, parametros):
        data: datetime.date = parametros['data']
        
        res = requests.get(
            "https://tradingenergiarz.com/api/v2/decks/weol/diff-table",
            params={"dataProduto": str(data)},
            headers=get_auth_header()
        )
        html = res.json()["html"]
        imagem = html_to_image(html)

        mensagem = f"WEOL (D-1) ({(data + datetime.timedelta(days=1)).strftime('%d/%m/%Y')})"
        send_whatsapp_message("debug", mensagem, imagem)
        # send_email_message(
        #     user="WEOL",
        #     destinatario=[constants.EMAIL_MIDDLE if hasattr(constants, 'EMAIL_MIDDLE') else "middle@example.com", 
        #                  constants.EMAIL_FRONT if hasattr(constants, 'EMAIL_FRONT') else "front@example.com"],
        #     mensagem=mensagem,
        #     arquivos=[]
        # )


if __name__ == "__main__":
    teste = Weol(WebhookSintegreSchema(**{
  "dataProduto": "13/10/2025",
  "filename": "Deck_PrevMes_20251013.zip",
  "macroProcesso": "Planejamento da Operação",
  "nome": "DECKS DA PREVISÃO DE GERAÇÃO EÓLICA SEMANAL WEOL-SM",
  "periodicidade": "2025-10-13T00:00:00",
  "periodicidadeFinal": "2025-10-13T23:59:59",
  "processo": "Modelos Fontes Intermitentes",
  "s3Key": "webhooks/DECKS DA PREVISÃO DE GERAÇÃO EÓLICA SEMANAL WEOL-SM/6fe5214a-7166-4b6f-b504-be144eb1a643_Deck_PrevMes_20251013.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOC8xMDMvMTA1L1Byb2R1dG9zLzc4NS9EZWNrX1ByZXZNZXNfMjAyNTEwMTMuemlwIiwidXNlcm5hbWUiOiJnaWxzZXUubXVobGVuQHJhaXplbi5jb20iLCJub21lUHJvZHV0byI6IkRFQ0tTIERBIFBSRVZJU8ODTyBERSBHRVJBw4fDg08gRcOTTElDQSBTRU1BTkFMIFdFT0wtU00iLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc2MDUzNTIyMCwibmJmIjoxNzYwNDQ4NTgwfQ.IshI9gocj33WILQ6QRIpE86kVdQ4hsXk5qZdo5Vhfn0",
  "webhookId": "6fe5214a-7166-4b6f-b504-be144eb1a643"
}))
    teste.run_workflow()