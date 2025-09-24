import os
import sys
import pdb
import requests
import datetime 
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
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
logger = setup_logger()
constants = Constants()


class PreciptacaoPrevista(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        logger.info("Inicializando PreciptacaoPrevista")
        super().__init__(payload)
        logger.info("PreciptacaoPrevista inicializada com sucesso")
    
    def run_workflow(self, filepath: Optional[str] = None):
        logger.info("Iniciando execucao do workflow de Precipitacao Prevista")
        
        if not filepath:
            filepath = self.download_files()
            logger.info(f"Download concluido, arquivos salvos em: {filepath}")
        else:
            logger.info(f"Usando caminho de arquivo fornecido: {filepath}")
            
        logger.info("Iniciando extracao dos arquivos")
        filepath = self.extract_files(filepath)
        
        logger.info("Iniciando conversao dos arquivos para DataFrame")
        df = self.file_to_df(filepath)
        
        logger.info("Iniciando envio dos dados para a API")
        self.post_data(df)
        logger.info("Workflow de Precipitacao Prevista executado com sucesso")

    def file_to_df(self, filepath: str) -> pd.DataFrame:
        logger.info(f"Processando arquivo de precipitacao em: {filepath}")
        arquivo = [x for x in os.popen(f'ls {filepath}').read().split("\n")[:-1] if '_m_' in x][0]
        data_rodada = datetime.datetime.strptime(arquivo[-10:-4], '%d%m%y')
        df = pd.read_fwf(os.path.join(filepath, arquivo), header=None)
        df.columns = ["cd_subbacia", "lat", "lon", *[
            f'{(data_rodada+datetime.timedelta(days=x+1)).date()}' for x in range(
                len(df.columns.to_list()[3:])
            )]]
        df.drop(columns=["lat", "lon"], inplace=True)
        df_postos = self.get_postos()
        df = df.melt(id_vars=["cd_subbacia"], var_name="dt_prevista", value_name="vl_chuva")
        df = df.merge(df_postos, on="cd_subbacia", how="left").drop(columns=["cd_subbacia"]).rename(columns={"id":"cd_subbacia"})
        df['modelo'] = f'{arquivo.split('_')[0].replace('40', '')}-ONS'
        df['dt_rodada'] = f'{data_rodada}'
        logger.info(f"Arquivo processado: {len(df)} registros gerados")
        return df
        
    def get_postos(self) -> pd.DataFrame:
        logger.info("Buscando dados dos postos")
        res = requests.get(f'{constants.BASE_URL}/api/v2/rodadas/subbacias', headers=get_auth_header())
        res.raise_for_status()
        df_postos = pd.DataFrame(res.json())
        return df_postos[["id", "nome"]].rename(columns={"nome":"cd_subbacia"})
    
    def post_data(self, df: pd.DataFrame) -> dict:
        res = requests.post(
            f'{constants.BASE_URL}/api/v2/rodadas/subbacias',
            json=df.to_dict(orient='records'),
            headers=get_auth_header()
        )
        res.raise_for_status()
        return res.json()


if __name__ == "__main__":
    obj = PreciptacaoPrevista(WebhookSintegreSchema(
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
