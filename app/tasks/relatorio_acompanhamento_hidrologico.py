import pandas as pd
from typing import Optional
from pathlib import Path
import pdb
import sys
import glob
import os
import json
import requests
import math
import datetime
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, Constants, get_auth_header
from middle.utils.file_manipulation import extract_zip
logger = setup_logger()
constants = Constants()


class RelatorioAcompanhamentoHidrologico(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.consts = Constants()
        self.PATH_INFO_VAZOES_OBS_JSON = project_root / 'app' / 'files' / 'relatorio_acompanhamento_hidrologico' / 'info_vazao_obs.json'
        
        
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime.datetime] = None):
        logger.info("Iniciando workflow do produto Relatório de Acompanhamento Hídrico...")
        try:
            file_path = self.download_files()
            
            self.run_process(file_path)
            
            logger.info("Workflow do produto Relatório de Acompanhamento Hídrico finalizado com sucesso!  ")
        except Exception as e:
            logger.error("Erro no workflow do produto Relatório de Acompanhamento Hídrico")
            raise
        
    def run_process(self, file_path):
        process_result = self.process_file(file_path)
        
        self.post_data(process_result)
        
    
    
    def process_file(self, file_path):
        logger.info("Processando arquivos do produto... Arquivo encontrado: %s", file_path)
        try:
            
            df_load = pd.ExcelFile(file_path)
            
            with open(self.PATH_INFO_VAZOES_OBS_JSON, encoding='utf-8') as f:
                infoTrechos = json.load(f)
            
            process_result = []	
            for index, station_info in enumerate(infoTrechos):
                trecho = station_info

                nomeArquivoSaida = trecho
                tipoVazao = infoTrechos[station_info]['iniciais']
                sheet_name = infoTrechos[station_info]['sheet']
                codigoEstacao = infoTrechos[station_info]['codigoEstacao']

                print(nomeArquivoSaida+' ('+str(index+1)+'/'+str(len(infoTrechos))+')')

                df = df_load.parse(sheet_name, header=[4,6])
                df.index = df['Unnamed: 0_level_0']['DATA']
                df = df.drop( 'Unnamed: 0_level_0', axis=1, level=0)
                df = df[df.index.notnull()]

                for ii, comp in enumerate(infoTrechos[station_info]['composicao']):
                
                    if 'sheet' not in infoTrechos[station_info]['composicao'][comp]:
                        vazComp = df[comp][infoTrechos[station_info]['composicao'][comp]['tipoVazao']]

                    else:
                        df_temp = df_load.parse(infoTrechos[station_info]['composicao'][comp]['sheet'], header=[4,6])
                        df_temp.index = df_temp['Unnamed: 0_level_0']['DATA']
                        df_temp = df_temp.drop( 'Unnamed: 0_level_0', axis=1, level=0)
                        df_temp = df_temp[df_temp.index.notnull()]
                        vazComp = df_temp[comp][infoTrechos[station_info]['composicao'][comp]['tipoVazao']]

                    if ii == 0:
                        vaz_out = vazComp
                    else:
                        if 'tempoViagem' in infoTrechos[station_info]['composicao'][comp]:
                            tempoViagem = infoTrechos[station_info]['composicao'][comp]['tempoViagem']

                            diasViagem = math.ceil(tempoViagem/24)

                            if nomeArquivoSaida == 'FOA':
                                vaz_out -= (tempoViagem/(diasViagem*24))*vazComp + ((diasViagem*24 - tempoViagem)/(diasViagem*24))*vazComp.shift(periods=diasViagem)
                            else:
                                vaz_out += (tempoViagem/(diasViagem*24))*vazComp.shift(periods=diasViagem) + ((diasViagem*24 - tempoViagem)/(diasViagem*24))*vazComp
                        else:
                            vaz_out += vazComp
                if nomeArquivoSaida == 'GOV. JAYME CANET':
                    nomeArquivoSaida = 'MAUA'

                for dt, vaz in vaz_out.items():
                    process_result.append([nomeArquivoSaida, codigoEstacao, tipoVazao, dt.strftime('%Y-%m-%d 00:00:00'), round(vaz,2)])
            
            process_result = pd.DataFrame(process_result)
            process_result = process_result.dropna()
            
            process_result.rename(columns={0:'txt_subbacia', 1:'cd_estacao', 2:'txt_tipo_vaz', 3:'dt_referente', 4:'vl_vaz'}, inplace=True)
            
            process_result['cd_estacao'] = process_result['cd_estacao'].astype(int)
            process_result['dt_referente'] = pd.to_datetime(process_result['dt_referente']).dt.strftime('%Y-%m-%d')
            process_result['vl_vaz'] = process_result['vl_vaz'].astype(float)

            return process_result
            
        except Exception as e:
            logger.error("Falha em processar os arquivos do produto: %s", str(e), exc_info=True)
            raise
        
        
    def post_data(self, process_result: pd.DataFrame) -> dict:
        
        logger.info("Inserindo valores de vazão observado do produto Relatório de Acompanhamento Hídrico. Qntd de linhas inseridas: %d", len(process_result))
        try:
            res = requests.post(
                self.consts.POST_RODADAS_VAZAO_OBSERVADA_PDP,
                json=process_result.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                logger.error("Failed to post data to database: status %d, response: %s",
                           res.status_code, res.text)
                res.raise_for_status()
            logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return
        
        except Exception as e:
            logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise
    

if __name__ == '__main__':
    logger.info("Iniciando manualmente o workflow do produto Relatório de Acompanhamento Hídrico...")
    try:
        payload = {
            "dataProduto": "06/09/2025",
            "filename": "Vazões Observadas - 09-06-2025 a 06-09-2025.xlsx",
            "macroProcesso": "Programação da Operação",
            "nome": "Relatório de Acompanhamento Hidrológico",
            "periodicidade": "2025-09-06T00:00:00",
            "periodicidadeFinal": "2025-09-06T23:59:59",
            "processo": "Acompanhamento das Condições Hidroenergéticas",
            "s3Key": "webhooks/Relatório de Acompanhamento Hidrológico/68bd994351c7b8ba11d2d11c_Vazões Observadas - 09-06-2025 a 06-09-2025.xlsx",
            "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy81Ni9Qcm9kdXRvcy8yMzQvVmF6w7VlcyBPYnNlcnZhZGFzIC0gMDktMDYtMjAyNSBhIDA2LTA5LTIwMjUueGxzeCIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJSZWxhdMOzcmlvIGRlIEFjb21wYW5oYW1lbnRvIEhpZHJvbMOzZ2ljbyIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzU3MzQyNjQzLCJuYmYiOjE3NTcyNTYwMDN9.j_wawNct0JI1P3fIzLBfL1LWiEElb4pkeOHSaGG3AQ0",
            "webhookId": "68bd994351c7b8ba11d2d11c"
        }
        
        payload = WebhookSintegreSchema(**payload)
        
        relatorioacompanhamento = RelatorioAcompanhamentoHidrologico(payload)
        relatorioacompanhamento.run_workflow()
    except Exception as e:
        logger.error("Erro no fluxo manual de processamento das Relatório de Acompanhamento Hídrico: %s", str(e), exc_info=True)
        raise