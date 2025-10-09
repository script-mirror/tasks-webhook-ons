import os
import sys
from typing import Optional, Dict, Any
import glob
import pdb  # noqa: F401
import struct
import pandas as pd
import numpy as np
import datetime
import requests
from inewave.newave import Patamar, Cadic, Sistema, Dger

from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface # noqa: E402
from app.schema import WebhookSintegreSchema # noqa: E402

from middle.utils import ( # noqa: E402
    setup_logger,
    get_auth_header,
    Constants,
    extract_zip,
    SemanaOperativa,
)
from middle.airflow import trigger_dag # noqa: E402
from app.tasks.previsoes_carga_mensal_patamar_newave import GenerateTable # noqa: E402
constants = Constants()


class DecksNewave(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        self.logger = setup_logger()
        self.logger.debug("Initializing DecksNewave with payload: %s", payload)
        super().__init__(payload)
        self.dataProduto = self.payload.dataProduto
        self.filename = self.payload.filename
        self.trigger_dag = trigger_dag   
        self.process_functions = ProcessFunctions(payload)
        self.gerar_tabela = GenerateTable()
        self.read_vazoes = VazoesBinaryReader()
        self.update_wind = NewaveUpdater()
        self.headers = get_auth_header()
        self.constants = Constants()
        self.logger.info("DecksNewave initialized successfully")

    def run_workflow(self) -> Dict[str, Any]:
        self.logger.info("Starting workflow for file: %s", self.filename)
        try:
            file_path = self.download_files()
            self.logger.debug("Downloaded files to path: %s", file_path)
            self.run_process(file_path)
            self.logger.info("Workflow completed successfully")
            return {"status": "success", "message": "Workflow completed"}
        except Exception as e:
            self.logger.error("Workflow failed: %s", str(e))
            raise

    def run_process(self, file_path):
        self.logger.info("Processing file at path: %s", file_path)
        try:
            
            path = extract_zip(file_path)
            self.logger.debug("Extracted ZIP to path: %s", path)
            newave = [arq for arq in os.listdir(path) if "newave" in arq.lower()]
            self.logger.debug("Found newave files: %s", newave)
            path = extract_zip(os.path.join(path, newave[0]))
            
            
            dict_result = self.process_file(path)
            self.logger.debug("Processed file, result keys: %s", list(dict_result.keys()))
            
            dict_url = {
                'df_c_adic': self.constants.POST_NEWAVE_CADIC,
                'df_sistema': self.constants.POST_NEWAVE_SISTEMA,
                'df_patamar_usina': self.constants.POST_NEWAVE_PATAMAR_CARGA_USINAS,
                'df_patamar_intercambio': self.constants.POST_NEWAVE_PATAMAR_INTERCAMBIO
            }
            
            for chave, valor in dict_result.items():
                self.logger.debug("Posting data for key: %s to URL: %s", chave, dict_url[chave])
                self.post_data(valor, dict_url[chave])
                self.logger.info("Successfully posted data for key: %s", chave)
            
            self.logger.info("Triggering Airflow DAG: 1.17-NEWAVE_ONS-TO-CCEE")
            #self.trigger_dag(dag_id="1.17-NEWAVE_ONS-TO-CCEE", conf={})
            self.logger.info("DAG triggered successfully")
            
            self.logger.info("Running GenerateTable process")
            self.gerar_tabela.run_process()
            self.logger.info("GenerateTable process completed")
            
            self.logger.info("Running VazoesBinaryReader process")
            self.read_vazoes.run_process(path)
            self.logger.info("VazoesBinaryReader process completed")
            
        except Exception as e:
            self.logger.error("Error in run_process: %s", str(e))
            raise

    def process_file(self, file_path) -> dict:
        self.logger.info("Processing file at path: %s", file_path)
        try:
            dat_files = self.process_functions.extrair_arquivos_dat(file_path)
            self.logger.debug("Extracted DAT files: %s", dat_files)
            
            if 'preliminar' in self.filename.lower():
                self.logger.info("Updating wind data for preliminary deck")
                self.update_wind.update_eolica(dat_files['sistema'])
                self.logger.info("Wind data update completed")
            
            df_c_adic = self.process_functions.processar_deck_nw_cadic(dat_files['cadic'])
            self.logger.info("Processed c_adic, shape: %s", df_c_adic.shape)
            
            df_sistema = self.process_functions.processar_deck_nw_sist(dat_files['sistema'])
            self.logger.info("Processed sistema, shape: %s", df_sistema.shape)
            
            dict_patamar = self.process_functions.processar_deck_nw_patamar(dat_files['patamar'])
            self.logger.info("Processed patamar, keys: %s", list(dict_patamar.keys()))
            
            dict_result = {
                'df_c_adic': df_c_adic,
                'df_sistema': df_sistema,
                'df_patamar_usina': dict_patamar['df_patamar_carga_usinas'],
                'df_patamar_intercambio': dict_patamar['df_patamar_intercambio'],
            }
            self.logger.debug("Returning processed data with keys: %s", list(dict_result.keys()))
            return dict_result
        
        except Exception as e:
            self.logger.error("Error processing file: %s", str(e))
            raise
        
    def post_data(self, data_in: pd.DataFrame, url: str) -> dict:
        self.logger.info("Posting data to URL: %s, data shape: %s", url, data_in.shape)
        try:
            res = requests.post(
                url,
                json=data_in.to_dict('records'),
                headers=self.headers
            )
            if res.status_code != 200:
                res.raise_for_status()
            self.logger.info("Successfully posted data to %s, status code: %s", url, res.status_code)
            return res.json()
        except Exception as e:
            self.logger.error("Error posting data to %s: %s", url, str(e))
            raise


class ProcessFunctions:
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        self.logger = setup_logger()
        self.logger.debug("Initializing ProcessFunctions with payload: %s", payload)
        self.dataProduto = payload.dataProduto
        self.filename = payload.filename
        self.logger.info("ProcessFunctions initialized successfully")
    
    def _get_version_by_filename(self, filename: str) -> str:
        self.logger.debug("Determining version from filename: %s", filename)
        try:
            if 'preliminar' in filename.lower():
                self.logger.info("Version determined: preliminar")
                return 'preliminar'
            elif 'definitivo' in filename.lower():
                self.logger.info("Version determined: definitivo")
                return 'definitivo'  
            else:
                self.logger.error("Filename does not contain 'preliminar' or 'definitivo': %s", filename)
                raise ValueError("Nome do arquivo não contém 'preliminar' ou 'definitivo'.")
        except Exception as e:
            self.logger.error("Error determining version: %s", str(e))
            raise
    
    def extrair_arquivos_dat(self, zip_ref) -> dict:
        self.logger.info("Extracting DAT files from path: %s", zip_ref)
        try:    
            dict_paths = {'cadic': None, 'sistema': None, 'patamar': None}
            for file in os.listdir(zip_ref):
                if file.lower().startswith('c_adic'):
                    dict_paths['cadic'] = os.path.join(zip_ref, file)
                if file.lower().startswith('sistema'):
                    dict_paths['sistema'] = os.path.join(zip_ref, file)
                if file.lower().startswith('patamar'):
                    dict_paths['patamar'] = os.path.join(zip_ref, file)
            self.logger.info("Extracted DAT file paths: %s", dict_paths)
            return dict_paths
        except Exception as e:
            self.logger.error("Error extracting DAT files: %s", str(e))
            raise
    
    def processar_deck_nw_cadic(self, cadic_file: str) -> pd.DataFrame:
        self.logger.info("Processing c_adic file: %s", cadic_file)
        try:
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            self.logger.debug("Parsed dataProduto: %s", data_produto_datetime)
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            cadic_object = Cadic.read(cadic_file)
            df_c_adic = cadic_object.cargas.copy()
            self.logger.debug("Loaded c_adic data, shape: %s", df_c_adic.shape)

            if df_c_adic is None:
                self.logger.error("No c_adic data found in file: %s", cadic_file)
                raise ValueError("Dados do c_adic não encontrados no arquivo!")
            else:
                self.logger.info("c_adic data found: %s records", len(df_c_adic))
                self.logger.debug("c_adic data: %s", df_c_adic.head().to_dict())

                df_c_adic['data'] = pd.to_datetime(df_c_adic['data'], errors='coerce')
                df_c_adic = df_c_adic[df_c_adic['data'].dt.year < 9999]
                df_c_adic['vl_ano'] = df_c_adic['data'].dt.year.astype(int)
                df_c_adic['vl_mes'] = df_c_adic['data'].dt.month.astype(int)
                df_c_adic = df_c_adic.dropna(subset=['valor'])
                self.logger.debug("Processed c_adic data, shape after cleaning: %s", df_c_adic.shape)
                
                mapeamento_razao = {
                    'CONS.ITAIPU': 'vl_const_itaipu',
                    'ANDE': 'vl_ande',
                    'MMGD SE': 'vl_mmgd_se',
                    'MMGD S': 'vl_mmgd_s',
                    'MMGD NE': 'vl_mmgd_ne',
                    'BOA VISTA': 'vl_boa_vista',
                    'MMGD N': 'vl_mmgd_n'
                }
            
                df_c_adic['coluna'] = df_c_adic['razao'].map(mapeamento_razao)
                self.logger.debug("Mapped razao to columns: %s", df_c_adic['coluna'].unique())

                df_c_adic = df_c_adic.pivot_table(
                    index=['vl_ano', 'vl_mes'], 
                    columns='coluna',
                    values='valor',
                    aggfunc='first'
                ).reset_index()
                self.logger.debug("Pivoted c_adic data, shape: %s", df_c_adic.shape)
                
                df_c_adic['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
                df_c_adic['versao'] = versao
                self.logger.info("Final c_adic DataFrame ready, shape: %s", df_c_adic.shape)
                
                return df_c_adic
        except Exception as e:
            self.logger.error("Error processing c_adic file: %s", str(e))
            raise
    
    def processar_deck_nw_sist(self, sistema_file: str) -> pd.DataFrame:
        self.logger.info("Processing sistema file: %s", sistema_file)
        try:
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            self.logger.debug("Parsed dataProduto: %s", data_produto_datetime)
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            sistema_object = Sistema.read(sistema_file)
            sistema_mercado_energia_df = sistema_object.mercado_energia.copy()
            self.logger.debug("Loaded mercado_energia data, shape: %s", sistema_mercado_energia_df.shape)
            
            if sistema_mercado_energia_df is None:
                self.logger.error("No mercado_energia data found in file: %s", sistema_file)
                raise ValueError("Dados de sistema do mercado de energia não encontrados no arquivo!")
            
            self.logger.info("mercado_energia data found: %s records", len(sistema_mercado_energia_df))
            sistema_mercado_energia_df['data'] = pd.to_datetime(sistema_mercado_energia_df['data'], errors='coerce')
            sistema_mercado_energia_df = sistema_mercado_energia_df.dropna(subset=['data'])
            self.logger.debug("Cleaned mercado_energia data, shape: %s", sistema_mercado_energia_df.shape)
            
            sistema_mercado_energia_df['vl_ano'] = sistema_mercado_energia_df['data'].dt.year.astype(int)
            sistema_mercado_energia_df['vl_mes'] = sistema_mercado_energia_df['data'].dt.month.astype(int)
            
            sistema_mercado_energia_df = sistema_mercado_energia_df.rename(columns={
                'codigo_submercado': 'cd_submercado',
                'valor': 'vl_energia_total'
            })
            self.logger.debug("Renamed columns in mercado_energia: %s", sistema_mercado_energia_df.columns.tolist())
            
            sistema_geracao_unsi_df = sistema_object.geracao_usinas_nao_simuladas
            self.logger.debug("Loaded geracao_usinas_nao_simuladas, shape: %s", sistema_geracao_unsi_df.shape)
            
            sistema_geracao_unsi_df['tipo_geracao'] = sistema_geracao_unsi_df['indice_bloco'].map({
                1: 'vl_geracao_pch',
                2: 'vl_geracao_pct',
                3: 'vl_geracao_eol',
                4: 'vl_geracao_ufv',
                5: 'vl_geracao_pch_mmgd',
                6: 'vl_geracao_pct_mmgd',
                7: 'vl_geracao_eol_mmgd',
                8: 'vl_geracao_ufv_mmgd'
            })
            self.logger.debug("Mapped tipo_geracao: %s", sistema_geracao_unsi_df['tipo_geracao'].unique())
            
            sistema_geracao_unsi_df['vl_ano'] = sistema_geracao_unsi_df['data'].dt.year
            sistema_geracao_unsi_df['vl_mes'] = sistema_geracao_unsi_df['data'].dt.month
            
            sistema_geracao_unsi_df = sistema_geracao_unsi_df.pivot_table(
                index=['codigo_submercado', 'vl_ano', 'vl_mes'], 
                columns='tipo_geracao',
                values='valor',
                aggfunc='sum'
            ).reset_index()
            self.logger.debug("Pivoted geracao_usinas_nao_simuladas, shape: %s", sistema_geracao_unsi_df.shape)
            
            sistema_geracao_unsi_df = sistema_geracao_unsi_df.rename(columns={'codigo_submercado': 'cd_submercado'})
            
            nw_sistema_df = pd.merge(
                sistema_geracao_unsi_df, 
                sistema_mercado_energia_df,
                on=['cd_submercado', 'vl_ano', 'vl_mes'],
                how='left'
            )
            self.logger.debug("Merged sistema data, shape: %s", nw_sistema_df.shape)
            
            nw_sistema_df = nw_sistema_df[~((nw_sistema_df['vl_geracao_pch'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pct'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_eol'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_ufv'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pch_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pct_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_eol_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_ufv_mmgd'].fillna(0) == 0))
            ]
            self.logger.debug("Filtered sistema data, shape: %s", nw_sistema_df.shape)
            
            nw_sistema_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            nw_sistema_df['versao'] = versao
            
            ordem_colunas = [
                'cd_submercado', 'vl_ano', 'vl_mes', 'vl_energia_total',
                'vl_geracao_pch', 'vl_geracao_pct', 'vl_geracao_eol', 'vl_geracao_ufv',
                'vl_geracao_pch_mmgd', 'vl_geracao_pct_mmgd', 'vl_geracao_eol_mmgd', 'vl_geracao_ufv_mmgd',
                'dt_deck', 'versao'
            ]
            
            nw_sistema_df = nw_sistema_df.reindex(columns=ordem_colunas)
            self.logger.info("Final sistema DataFrame ready, shape: %s", nw_sistema_df.shape)
            
            return nw_sistema_df
        except Exception as e:
            self.logger.error("Error processing sistema file: %s", str(e))
            raise
    
    def processar_deck_nw_patamar(self, patamar_file: str) -> Dict[str, Any]:
        self.logger.info("Processing patamar file: %s", patamar_file)
        try:
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            self.logger.debug("Parsed dataProduto: %s", data_produto_datetime)
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            patamar_object = Patamar.read(patamar_file)
            carga_patamares_df = patamar_object.carga_patamares.copy()
            self.logger.debug("Loaded carga_patamares, shape: %s", carga_patamares_df.shape)

            if carga_patamares_df is None or carga_patamares_df.empty:
                self.logger.error("No carga_patamares data found in file: %s", patamar_file)
                raise ValueError("Não foi possível extrair a carga dos patamares NEWAVE")

            duracao_mensal_patamares_df = patamar_object.duracao_mensal_patamares.copy()
            self.logger.debug("Loaded duracao_mensal_patamares, shape: %s", duracao_mensal_patamares_df.shape)

            if duracao_mensal_patamares_df is None or duracao_mensal_patamares_df.empty:
                self.logger.error("No duracao_mensal_patamares data found in file: %s", patamar_file)
                raise ValueError("Não foi possível extrair a duração mensal dos patamares NEWAVE")

            intercambio_patamares_df = patamar_object.intercambio_patamares.copy()
            self.logger.debug("Loaded intercambio_patamares, shape: %s", intercambio_patamares_df.shape)

            if intercambio_patamares_df is None or intercambio_patamares_df.empty:
                self.logger.error("No intercambio_patamares data found in file: %s", patamar_file)
                raise ValueError("Não foi possível extrair os intercâmbios por patamares NEWAVE")

            usinas_nao_simuladas_df = patamar_object.usinas_nao_simuladas.copy()
            self.logger.debug("Loaded usinas_nao_simuladas, shape: %s", usinas_nao_simuladas_df.shape)

            if usinas_nao_simuladas_df is None or usinas_nao_simuladas_df.empty:
                self.logger.error("No usinas_nao_simuladas data found in file: %s", patamar_file)
                raise ValueError("Não foi possível extrair as usinas não simuladas do NEWAVE")

            patamares = {'1': 'Pesado', '2': 'Medio', '3': 'Leve'}
            indices_bloco = {1: 'PCH', 2: 'PCT', 3: 'EOL', 4: 'UFV', 5: 'PCH_MMGD', 6: 'PCT_MMGD', 7: 'EOL_MMGD', 8: 'UFV_MMGD'}
            submercados = {'1': 'SE', '2': 'S', '3': 'NE', '4': 'N', '11': 'FC'}

            carga_df = carga_patamares_df.copy()
            carga_df['patamar_nome'] = carga_df['patamar'].astype(str).map(patamares)
            carga_df['submercado_nome'] = carga_df['codigo_submercado'].astype(str).map(submercados)
            carga_df = carga_df.rename(columns={'data': 'dt_referente', 'valor': 'pu_demanda_med', 'codigo_submercado': 'submercado'})
            carga_df = carga_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 'pu_demanda_med']]
            self.logger.debug("Processed carga_df, shape: %s", carga_df.shape)

            duracao_df = duracao_mensal_patamares_df.copy()
            duracao_df['patamar_nome'] = duracao_df['patamar'].astype(str).map(patamares)
            duracao_df = duracao_df.rename(columns={'data': 'dt_referente', 'valor': 'duracao_mensal'})
            duracao_df = duracao_df[['dt_referente', 'patamar', 'patamar_nome', 'duracao_mensal']]
            self.logger.debug("Processed duracao_df, shape: %s", duracao_df.shape)

            intercambio_df = intercambio_patamares_df.copy()
            intercambio_df['patamar_nome'] = intercambio_df['patamar'].astype(str).map(patamares)
            intercambio_df['submercado_de_nome'] = intercambio_df['submercado_de'].astype(str).map(submercados)
            intercambio_df['submercado_para_nome'] = intercambio_df['submercado_para'].astype(str).map(submercados)
            intercambio_df = intercambio_df.rename(columns={'data': 'dt_referente', 'valor': 'pu_intercambio_med'})
            intercambio_df = intercambio_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado_de', 'submercado_de_nome', 
                                           'submercado_para', 'submercado_para_nome', 'pu_intercambio_med']]
            self.logger.debug("Processed intercambio_df, shape: %s", intercambio_df.shape)

            usinas_df = usinas_nao_simuladas_df.copy()
            usinas_df['patamar_nome'] = usinas_df['patamar'].astype(str).map(patamares)
            usinas_df['submercado_nome'] = usinas_df['codigo_submercado'].astype(str).map(submercados)
            usinas_df['indice_bloco_nome'] = usinas_df['indice_bloco'].map(indices_bloco)
            usinas_df = usinas_df.rename(columns={'data': 'dt_referente', 'codigo_submercado': 'submercado', 'valor': 'pu_montante_med'})
            usinas_df = usinas_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 
                                 'indice_bloco', 'indice_bloco_nome', 'pu_montante_med']]
            self.logger.debug("Processed usinas_df, shape: %s", usinas_df.shape)

            carga_transformada_df = carga_df.copy()
            carga_transformada_df['indice_bloco'] = 'CARGA'
            carga_transformada_df['valor_pu'] = carga_transformada_df['pu_demanda_med']
            carga_transformada_df = carga_transformada_df.drop(columns=['pu_demanda_med'])
            
            usinas_transformada_df = usinas_df.copy()
            usinas_transformada_df['valor_pu'] = usinas_transformada_df['pu_montante_med']
            usinas_transformada_df = usinas_transformada_df.drop(columns=['pu_montante_med'])
            
            patamar_carga_usinas_df = pd.concat([carga_transformada_df, usinas_transformada_df], ignore_index=True)
            self.logger.debug("Concatenated patamar_carga_usinas_df, shape: %s", patamar_carga_usinas_df.shape)

            patamar_carga_usinas_df = pd.merge(
                patamar_carga_usinas_df,
                duracao_df,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='left'
            )
            self.logger.debug("Merged patamar_carga_usinas_df with duracao_df, shape: %s", patamar_carga_usinas_df.shape)

            patamar_carga_usinas_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            patamar_carga_usinas_df['versao'] = versao
            patamar_carga_usinas_df = patamar_carga_usinas_df.drop(columns=['patamar', 'submercado'])
            patamar_carga_usinas_df = patamar_carga_usinas_df.rename(columns={'patamar_nome': 'patamar', 'submercado_nome': 'submercado'})
            
            patamar_carga_usinas_df['indice_bloco'] = patamar_carga_usinas_df.apply(
                lambda row: row['indice_bloco_nome'] if pd.notna(row.get('indice_bloco_nome')) else row['indice_bloco'], axis=1
            )
            if 'indice_bloco_nome' in patamar_carga_usinas_df.columns:
                patamar_carga_usinas_df = patamar_carga_usinas_df.drop(columns=['indice_bloco_nome'])
            self.logger.debug("Processed patamar_carga_usinas_df, shape: %s", patamar_carga_usinas_df.shape)

            colunas_tabela1 = [
                'dt_referente', 'patamar', 'submercado', 'valor_pu',
                'duracao_mensal', 'indice_bloco', 'dt_deck', 'versao'
            ]
            patamar_carga_usinas_df['dt_referente'] = patamar_carga_usinas_df['dt_referente'].dt.strftime('%Y-%m-%d')
            patamar_carga_usinas_df['dt_deck'] = patamar_carga_usinas_df['dt_deck'].astype(str)
            patamar_carga_usinas_df = patamar_carga_usinas_df[colunas_tabela1]
            self.logger.debug("Final patamar_carga_usinas_df, shape: %s", patamar_carga_usinas_df.shape)

            patamar_intercambio_df = pd.merge(
                intercambio_df,
                duracao_df,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='inner'
            )
            self.logger.debug("Merged patamar_intercambio_df, shape: %s", patamar_intercambio_df.shape)

            patamar_intercambio_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            if patamar_intercambio_df['dt_deck'].dtype == 'object':
                patamar_intercambio_df['dt_deck'] = patamar_intercambio_df['dt_deck'].astype(str)
            patamar_intercambio_df['versao'] = versao
            patamar_intercambio_df = patamar_intercambio_df.drop(columns=['patamar', 'submercado_de', 'submercado_para'])
            patamar_intercambio_df = patamar_intercambio_df.rename(columns={
                'patamar_nome': 'patamar',
                'submercado_de_nome': 'submercado_de',
                'submercado_para_nome': 'submercado_para'
            })
            self.logger.debug("Processed patamar_intercambio_df, shape: %s", patamar_intercambio_df.shape)

            colunas_tabela2 = [
                'dt_referente', 'patamar', 'submercado_de', 'submercado_para',
                'pu_intercambio_med', 'duracao_mensal', 'dt_deck', 'versao'
            ]
            patamar_intercambio_df['dt_referente'] = patamar_intercambio_df['dt_referente'].dt.strftime('%Y-%m-%d')
            patamar_intercambio_df['dt_deck'] = patamar_intercambio_df['dt_deck'].astype(str)
            patamar_intercambio_df['pu_intercambio_med'] = patamar_intercambio_df['pu_intercambio_med'].round(4)
            patamar_intercambio_df = patamar_intercambio_df[colunas_tabela2]
            patamar_intercambio_df = patamar_intercambio_df.replace([np.inf, -np.inf], np.nan)
            self.logger.debug("Final patamar_intercambio_df, shape: %s", patamar_intercambio_df.shape)

            result = {
                "df_patamar_carga_usinas": patamar_carga_usinas_df,
                "df_patamar_intercambio": patamar_intercambio_df,
            }
            self.logger.info("Patamar processing completed, result keys: %s", list(result.keys()))
            return result
        except Exception as e:
            self.logger.error("Error processing patamar file: %s", str(e))
            raise


class NewaveUpdater:
    def __init__(self):
        self.logger = setup_logger()
        self.logger.debug("Initializing NewaveUpdater")
        self.logger.info("NewaveUpdater initialized successfully")
    
    def get_dados_banco(self, endpoint):
        self.logger.info("Fetching data from endpoint: %s", endpoint)
        try:
            response = requests.get(endpoint, headers=get_auth_header())
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            self.logger.info("Successfully fetched data, shape: %s", df.shape)
            self.logger.debug("Fetched data sample: %s", df.head().to_dict())
            return df
        except requests.RequestException as e:
            self.logger.error("Error fetching data from API %s: %s", endpoint, str(e))
            raise

    def calculate_monthly_wind_average(self, df_data, df_pq, MAP_SUBMERCADO):
        self.logger.info("Calculating monthly wind generation averages")
        try:
            dates = pd.date_range(start=pd.to_datetime(min(df_data['inicioSemana'])).replace(day=1),
                                end=pd.to_datetime(max(df_data['inicioSemana'])) + pd.offsets.MonthEnd(0), freq='D')
            self.logger.debug("Generated date range: %s to %s", dates[0], dates[-1])
            
            df_diaria = pd.DataFrame()
            df_data['inicioSemana'] = pd.to_datetime(df_data['inicioSemana'])
            for date in dates:
                for ss in df_data['submercado'].unique():
                    data_rv = SemanaOperativa(date)
                    data_week = pd.to_datetime(data_rv.week_start)
                    filter = ((df_pq['data'] == date.replace(day=1)) & (df_pq['indice_bloco'] == 3) & 
                            (df_pq['codigo_submercado'] == MAP_SUBMERCADO[ss]))
                    valor = df_pq[filter]['valor'].values[0] if not df_pq[filter].empty else 0
                    if data_week in df_data['inicioSemana'].unique():
                        filter = (df_data['submercado'] == ss) & (df_data['inicioSemana'] == data_week)
                        valor = df_data.loc[filter, 'mediaPonderada'].values[0] if not df_data[filter].empty else valor
                    df_diaria = pd.concat([df_diaria, pd.DataFrame([{"data": date, "submercado": ss, "valor": valor}])], 
                                        ignore_index=True)
            self.logger.debug("Generated df_diaria, shape: %s", df_diaria.shape)
            
            df_diaria['valor'] = df_diaria['valor'].fillna(0)
            df_diaria['mes_ano'] = df_diaria['data'].dt.to_period('M')
            df_diaria = df_diaria.groupby(['mes_ano', 'submercado'])['valor'].mean().round(2).unstack()
            self.logger.info("Calculated monthly averages, shape: %s", df_diaria.shape)
            self.logger.debug("Monthly averages sample: %s", df_diaria.head().to_dict())
            return df_diaria
        except Exception as e:
            self.logger.error("Error calculating monthly wind averages: %s", str(e))
            raise
    
    def update_eolica(self, path_sistema):
        MAP_SUBMERCADO = {'SE': 1, 'S': 2, 'NE': 3, 'N': 4}
        self.logger.info("Updating wind generation for sistema file: %s", path_sistema)
        try:
            df_data = self.get_dados_banco(constants.ENDPOINT_WEOL_PONDERADO)
            update_count = 0
            dger = Dger.read(os.path.join(os.path.dirname(path_sistema), 'dger.dat'))
            sistema = Sistema.read(path_sistema)
            data_deck = datetime.datetime(dger.ano_inicio_estudo, dger.mes_inicio_estudo, 1)
            self.logger.debug("Study start date from dger.dat: %s", data_deck)
            
            df_pq = sistema.geracao_usinas_nao_simuladas
            self.logger.debug("Loaded non-simulated generation records, shape: %s", df_pq.shape)
            
            df_diaria = self.calculate_monthly_wind_average(df_data, df_pq, MAP_SUBMERCADO)
            self.logger.info("Generated monthly averages for %s months across %s submarkets", 
                            len(df_diaria), len(df_diaria.columns))
            
            for mes_ano in df_diaria.index:
                if data_deck.strftime('%Y-%m') == str(mes_ano):
                    self.logger.info("Updating wind generation for deck date: %s", data_deck.strftime('%Y-%m'))
                    for ss in df_diaria.keys():
                        filter = ((df_pq['data'] == mes_ano.start_time) & 
                                (df_pq['indice_bloco'] == 3) & 
                                (df_pq['codigo_submercado'] == MAP_SUBMERCADO[ss]))
                        if not df_pq[filter].empty:
                            old_value = df_pq.loc[filter, 'valor'].values[0]
                            new_value = df_diaria.loc[mes_ano][ss]
                            df_pq.loc[filter, 'valor'] = new_value
                            update_count += 1
                            self.logger.info("Updated submarket: %s, month: %s, old_value: %s, new_value: %s",
                                            ss, mes_ano, old_value, new_value)
            
            sistema.geracao_usinas_nao_simuladas = df_pq
            self.logger.info("Writing updated sistema to %s with %s value updates", path_sistema, update_count)
            sistema.write(path_sistema)
        except Exception as e:
            self.logger.error("Error updating eolica for file %s: %s", path_sistema, str(e))
            raise


class VazoesBinaryReader:
    def __init__(self):
        self.logger = setup_logger()
        self.logger.debug("Initializing VazoesBinaryReader")
        self.postos = 320
        self.fmt = f"<{self.postos}i"
        self.logger.info("VazoesBinaryReader initialized successfully")

    def run_process(self, path):  
        self.logger.info("Processing vazoes file from path: %s", path)
        try:

            self.logger.debug("Extracted newave ZIP to path: %s", path)
            vazoes = [arq for arq in os.listdir(path) if "vazoes.dat" in arq.lower()]
            self.logger.debug("Found vazoes files: %s", vazoes)
            file_path = os.path.join(path, vazoes[0])
            df_data = self.read_binary_file(file_path)
            self.logger.info("Read vazoes data, shape: %s", df_data.shape)
            self.post_data(df_data)
            self.logger.info("Vazoes data posted successfully")
        except Exception as e:
            self.logger.error("Error in VazoesBinaryReader run_process: %s", str(e))
            raise
        
    def read_binary_file(self, file_path):
        self.logger.info("Reading binary file: %s", file_path)
        try:
            df_all = pd.DataFrame()
            month = 1
            year = 1931
            with open(file_path, 'rb') as file:
                while True:
                    data = file.read(self.postos * 4)
                    if not data:
                        self.logger.debug("End of file reached")
                        break
                    if len(data) != self.postos * 4:
                        self.logger.warning("Incomplete line found: %s bytes", len(data))
                        break
                    df_data = pd.DataFrame(struct.unpack(self.fmt, data))
                    df_data.columns = ['vazao']
                    df_data['mes'] = month
                    df_data['ano'] = year
                    df_data['posto'] = [i+1 for i in range(self.postos)]
                    df_all = pd.concat([df_all, df_data], ignore_index=True)
                    self.logger.debug("Processed data for year %s, month %s, shape: %s", year, month, df_data.shape)
                    month += 1
                    if month > 12:
                        month = 1
                        year += 1
            self.logger.info("Finished reading binary file, total shape: %s", df_all.shape)
            return df_all
        except FileNotFoundError:
            self.logger.error("File not found: %s", file_path)
            raise
        except struct.error as e:
            self.logger.error("Error unpacking binary data: %s", str(e))
            raise
        except Exception as e:
            self.logger.error("Error reading binary file: %s", str(e))
            raise

    def post_data(self, data_in: pd.DataFrame) -> dict:
        self.logger.info("Posting vazoes data, shape: %s", data_in.shape)
        try:
            res = requests.post(
                constants.ENDPOINT_HISTORICO_VAZOES,
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                res.raise_for_status()
            self.logger.info("Successfully posted vazoes data, status code: %s", res.status_code)
            return res.json()
        except Exception as e:
            self.logger.error("Error posting vazoes data: %s", str(e))
            raise


if __name__ == "__main__":
    logger = setup_logger()
    logger.debug("Starting main execution")
    payload = {
        "dataProduto": "10/2025",
        "filename": "Deck NEWAVE Preliminar.zip",
        "macroProcesso": "Programação da Operação",
        "nome": "Deck NEWAVE Preliminar",
        "periodicidade": "2025-10-01T00:00:00",
        "periodicidadeFinal": "2025-10-31T23:59:59",
        "processo": "Médio Prazo",
        "s3Key": "webhooks/Deck NEWAVE Preliminar/68d166d4668482a24061e32f_Deck NEWAVE Preliminar.zip",
        "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiIvc2l0ZXMvOS81Mi83MS9Qcm9kdXRvcy8yODcvMjItMDktMjAyNV8xMjA2MDAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiRGVjayBORVdBVkUgUHJlbGltaW5hciIsIklzRmlsZSI6IkZhbHNlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1ODY0MDQ1MSwibmJmIjoxNzU4NTUzODExfQ.yp5GkfD7XC9jn2YiaNLsq8UMfmGOZBT9Pnvt9V1Wqzo",
        "webhookId": "68d166d4668482a24061e32f"
    }
    logger.debug("Payload: %s", payload)
    
    payload = WebhookSintegreSchema(**payload)
    logger.info("Parsed payload into WebhookSintegreSchema")
    
    decknewave = DecksNewave(payload)
    logger.info("Created DecksNewave instance")
    
    decknewave.run_workflow()
    logger.info("Main execution completed")