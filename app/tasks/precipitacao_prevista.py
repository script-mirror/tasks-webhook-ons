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
from middle.airflow import trigger_dag
from middle.utils.file_manipulation import extract_zip

from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
logger = setup_logger()
constants = Constants()


class PreciptacaoPrevista(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema] = None):
        logger.info("Inicializando PreciptacaoPrevista")
        super().__init__(payload)
        self.trigger_dag = trigger_dag
    
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime.datetime] = None):
        logger.info("Iniciando execucao do workflow de Precipitacao Prevista")
        
        if not filepath and not self.payload:
            raise ValueError("É necessário fornecer um filepath ou um payload válido")
            
        if not filepath:
            if not self.payload:
                raise ValueError("É necessário fornecer um filepath quando não há payload válido")
            filepath = self.download_files()
            logger.info(f"Download concluido, arquivos salvos em: {filepath}")
        else:
            logger.info(f"Usando caminho de arquivo fornecido: {filepath}")
            if isinstance(manually_date, datetime.datetime):
                manually_date = manually_date.strftime('%Y-%m-%d')
            
        logger.info("Iniciando processamento do produto Precipitacao Prevista")
        process_result = self.process_file(filepath)
        
        logger.info("Iniciando envio dos dados para a API")
        self.post_data(process_result)
        logger.info("Workflow de Precipitacao Prevista executado com sucesso")

        if manually_date:
            logger.warning("Triggando dag pconjunto de origem local")
            conf = {"execution_date": manually_date}
            self.trigger_dag(dag_id="pconjunto", conf=conf)
        else:
            logger.warning("Triggando dag pconjunto com informações do payload")
            self.trigger_dag(dag_id="pconjunto")

    def process_file(self, filepath: str) -> pd.DataFrame:
        logger.info(f"Processando arquivo de precipitacao em: {filepath}")
        
        filepath = extract_zip(filepath)
        logger.info(f"Arquivo extraído com sucesso para {filepath}")
        
        arquivo = [x for x in os.popen(f'ls {filepath}').read().split("\n")[:-1] if '_m_' in x][0]
        data_rodada = datetime.datetime.strptime(arquivo[-10:-4], '%d%m%y')
        df = pd.read_fwf(os.path.join(filepath, arquivo), header=None)
        df.columns = ["cd_subbacia", "lat", "lon", *[
            f'{(data_rodada+datetime.timedelta(days=x+1)).date()}' for x in range(
                len(df.columns.to_list()[3:])
            )]]
        df.drop(columns=["lat", "lon"], inplace=True)
        df_postos = self._get_postos()
        df = df.melt(id_vars=["cd_subbacia"], var_name="dt_prevista", value_name="vl_chuva")
        df = df.merge(df_postos, on="cd_subbacia", how="left").drop(columns=["cd_subbacia"]).rename(columns={"id":"cd_subbacia"})
        df['modelo'] = f'{arquivo.split('_')[0]}-ONS'
        df['dt_rodada'] = f'{data_rodada}'
        logger.info(f"Arquivo processado: {len(df)} registros gerados")
        return df
        
    def _get_postos(self) -> pd.DataFrame:
        logger.info("Buscando dados dos postos")
        res = requests.get(constants.GET_RODADAS_SUBBACIAS, headers=get_auth_header())
        res.raise_for_status()
        df_postos = pd.DataFrame(res.json())
        return df_postos[["id", "nome"]].rename(columns={"nome":"cd_subbacia"})
    
    def post_data(self, df: pd.DataFrame) -> dict:
        
        res = requests.post(
            constants.POST_RODADAS_CHUVA_PREVISAO_MODELOS,
            json=df.to_dict(orient='records'),
            headers=get_auth_header(),
            params={'rodar_smap': True, 'prev_estendida': False}
        )
        res.raise_for_status()
        return res.json()


if __name__ == "__main__":
        # payload = {
            
        # }
        
        # payload = WebhookSintegreSchema(**payload)
        
        filepath= "/home/diogopolastrine/Downloads/ECMWF_precipitacao14d_20251018.zip"
        preciptacao_prevista = PreciptacaoPrevista()
        
        preciptacao_prevista.run_workflow(filepath=filepath, manually_date=datetime.datetime(2025,10,18))
