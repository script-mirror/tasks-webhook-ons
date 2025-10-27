import os
import sys
from typing import Optional, Dict, Any
import struct
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from inewave.newave import Patamar, Cadic, Sistema, Dger

from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from app.schema import WebhookSintegreSchema  # noqa: E402

from middle.utils import (  # noqa: E402
    setup_logger,
    get_auth_header,
    Constants,
    extract_zip,
    SemanaOperativa,
)
from middle.airflow import trigger_dag  # noqa: E402
from app.tasks.previsoes_carga_mensal_patamar_newave import GenerateTable  # noqa: E402
constants = Constants()


class DecksNewave(WebhookProductsInterface):
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        self.logger = setup_logger()
        self.logger.debug("Initializing DecksNewave with payload: %s", payload)
        super().__init__(payload)
        self.product_date = self.payload.dataProduto
        self.file_name = self.payload.filename
        self.trigger_dag = trigger_dag
        self.deck_processor = DeckProcessor(payload)
        self.table_generator = GenerateTable()
        self.flow_reader = VazoesBinaryReader()
        self.wind_updater = NewaveUpdater()
        self.headers = get_auth_header()
        self.constants = Constants()
        self.logger.info("DecksNewave initialized successfully")

    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None) -> Dict[str, Any]:
        pass
        self.logger.info("Starting workflow for file: %s", self.file_name)
        try:
            file_path = self.download_files()
            self.logger.debug("Downloaded files to path: %s", file_path)
            self.run_process(file_path)
            self.logger.info("Workflow completed successfully")
            return {"status": "success", "message": "Workflow completed"}
        except Exception as e:
            self.logger.error("Workflow failed: %s", str(e))
            raise

    def run_process(self, file_path: str):
        self.logger.info("Processing file at path: %s", file_path)
        try:
            extracted_path = extract_zip(file_path)
            self.logger.debug("Extracted ZIP to path: %s", extracted_path)
            newave_files = [file for file in os.listdir(extracted_path) if "newave" in file.lower()]
            self.logger.debug("Found newave files: %s", newave_files)
            extracted_newave_path = extract_zip(os.path.join(extracted_path, newave_files[0]))
            
            dict_processed_data = self.process_file(extracted_newave_path)
            self.logger.debug("Processed file, result keys: %s", list(dict_processed_data.keys()))

            dict_url_map = {
                'df_cadic': self.constants.POST_NEWAVE_CADIC,
                'df_system': self.constants.POST_NEWAVE_SISTEMA,
                'df_load_level_plants': self.constants.POST_NEWAVE_PATAMAR_CARGA_USINAS,
                'df_load_level_exchange': self.constants.POST_NEWAVE_PATAMAR_INTERCAMBIO
            }

            for key, df_data in dict_processed_data.items():
                self.logger.debug("Posting data for key: %s to URL: %s", key, dict_url_map[key])
                self.post_data(df_data, dict_url_map[key])
                self.logger.info("Successfully posted data for key: %s", key)

            self.logger.info("Triggering Airflow DAG: 1.17-NEWAVE_ONS-TO-CCEE")
            self.trigger_dag(dag_id="1.17-NEWAVE_ONS-TO-CCEE", conf={})
            self.logger.info("DAG triggered successfully")

            self.logger.info("Running GenerateTable process")
            self.table_generator.run_process()
            self.logger.info("GenerateTable process completed")

            self.logger.info("Running VazoesBinaryReader process")
            self.flow_reader.run_process(extracted_newave_path)
            self.logger.info("VazoesBinaryReader process completed")
        except Exception as e:
            self.logger.error("Error in run_process: %s", str(e))
            raise

    def process_file(self, file_path: str) -> Dict[str, pd.DataFrame]:
        self.logger.info("Processing file at path: %s", file_path)
        try:
            dict_file_paths = self.deck_processor.extract_dat_files(file_path)
            self.logger.debug("Extracted DAT files: %s", dict_file_paths)

            if 'preliminar' in self.file_name.lower():
                self.logger.info("Updating wind data for preliminary deck")
                self.wind_updater.update_wind_data(dict_file_paths)
                self.logger.info("Wind data update completed")

            df_cadic = self.deck_processor.process_cadic_deck(dict_file_paths['cadic'])
            self.logger.info("Processed cadic, shape: %s", df_cadic.shape)

            df_system = self.deck_processor.process_system_deck(dict_file_paths['system'])
            self.logger.info("Processed system, shape: %s", df_system.shape)

            dict_load_level_data = self.deck_processor.process_load_level_deck(dict_file_paths['load_level'])
            self.logger.info("Processed load_level, keys: %s", list(dict_load_level_data.keys()))

            dict_processed_data = {
                'df_cadic': df_cadic,
                'df_system': df_system,
                'df_load_level_plants': dict_load_level_data['df_load_level_plants'],
                'df_load_level_exchange': dict_load_level_data['df_load_level_exchange'],
            }
            self.logger.debug("Returning processed data with keys: %s", list(dict_processed_data.keys()))
            return dict_processed_data
        except Exception as e:
            self.logger.error("Error processing file: %s", str(e))
            raise

    def post_data(self, df_data: pd.DataFrame, url: str) -> Dict:
        self.logger.info("Posting data to URL: %s, data shape: %s", url, df_data.shape)
        try:
            response = requests.post(
                url,
                json=df_data.to_dict('records'),
                headers=self.headers
            )
            if response.status_code != 200:
                response.raise_for_status()
            self.logger.info("Successfully posted data to %s, status code: %s", url, response.status_code)
            return response.json()
        except Exception as e:
            self.logger.error("Error posting data to %s: %s", url, str(e))
            raise


class DeckProcessor:
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        self.logger = setup_logger()
        self.logger.debug("Initializing DeckProcessor with payload: %s", payload)
        self.product_date = payload.dataProduto
        self.file_name = payload.filename
        self.logger.info("DeckProcessor initialized successfully")

    def _get_version_by_file_name(self, file_name: str) -> str:
        self.logger.debug("Determining version from file name: %s", file_name)
        try:
            if 'preliminar' in file_name.lower():
                self.logger.info("Version determined: preliminar")
                return 'preliminar'
            elif 'definitivo' in file_name.lower():
                self.logger.info("Version determined: definitivo")
                return 'definitivo'
            else:
                self.logger.error("File name does not contain 'preliminar' or 'definitivo': %s", file_name)
                raise ValueError("Nome do arquivo não contém 'preliminar' ou 'definitivo'.")
        except Exception as e:
            self.logger.error("Error determining version: %s", str(e))
            raise

    def extract_dat_files(self, extracted_path: str) -> Dict[str, str]:
        self.logger.info("Extracting DAT files from path: %s", extracted_path)
        try:
            dict_file_paths = {'cadic': None, 'system': None, 'load_level': None}
            for file in os.listdir(extracted_path):
                if file.lower().startswith('c_adic'):
                    dict_file_paths['cadic'] = os.path.join(extracted_path, file)
                if file.lower().startswith('sistema'):
                    dict_file_paths['system'] = os.path.join(extracted_path, file)
                if file.lower().startswith('patamar'):
                    dict_file_paths['load_level'] = os.path.join(extracted_path, file)
                if file.lower().startswith('dger'):
                    dict_file_paths['dger'] = os.path.join(extracted_path, file)
            self.logger.info("Extracted DAT file paths: %s", dict_file_paths)
            return dict_file_paths
        except Exception as e:
            self.logger.error("Error extracting DAT files: %s", str(e))
            raise

    def process_cadic_deck(self, cadic_file_path: str) -> pd.DataFrame:
        self.logger.info("Processing cadic file: %s", cadic_file_path)
        try:
            product_date_str = self.product_date
            product_date = datetime.strptime(product_date_str, '%m/%Y')
            self.logger.debug("Parsed product_date: %s", product_date)
            version = self._get_version_by_file_name(self.file_name)

            cadic_object = Cadic.read(cadic_file_path)
            df_cadic = cadic_object.cargas.copy()
            self.logger.debug("Loaded cadic data, shape: %s", df_cadic.shape)

            if df_cadic is None:
                self.logger.error("No cadic data found in file: %s", cadic_file_path)
                raise ValueError("Dados do c_adic não encontrados no arquivo!")
            else:
                self.logger.info("cadic data found: %s records", len(df_cadic))
                self.logger.debug("cadic data: %s", df_cadic.head().to_dict())

                df_cadic['data'] = pd.to_datetime(df_cadic['data'], errors='coerce')
                df_cadic = df_cadic[df_cadic['data'].dt.year < 9999]
                df_cadic['vl_ano'] = df_cadic['data'].dt.year.astype(int)
                df_cadic['vl_mes'] = df_cadic['data'].dt.month.astype(int)
                df_cadic = df_cadic.dropna(subset=['valor'])
                self.logger.debug("Processed cadic data, shape after cleaning: %s", df_cadic.shape)

                reason_mapping = {
                    'CONS.ITAIPU': 'vl_const_itaipu',
                    'ANDE': 'vl_ande',
                    'MMGD SE': 'vl_mmgd_se',
                    'MMGD S': 'vl_mmgd_s',
                    'MMGD NE': 'vl_mmgd_ne',
                    'BOA VISTA': 'vl_boa_vista',
                    'MMGD N': 'vl_mmgd_n'
                }

                df_cadic['coluna'] = df_cadic['razao'].map(reason_mapping)
                self.logger.debug("Mapped razao to columns: %s", df_cadic['coluna'].unique())

                df_cadic = df_cadic.pivot_table(
                    index=['vl_ano', 'vl_mes'],
                    columns='coluna',
                    values='valor',
                    aggfunc='first'
                ).reset_index()
                self.logger.debug("Pivoted cadic data, shape: %s", df_cadic.shape)

                df_cadic['dt_deck'] = product_date.strftime('%Y-%m-%d')
                df_cadic['versao'] = version
                self.logger.info("Final cadic DataFrame ready, shape: %s", df_cadic.shape)

                return df_cadic
        except Exception as e:
            self.logger.error("Error processing cadic file: %s", str(e))
            raise

    def process_system_deck(self, system_file_path: str) -> pd.DataFrame:
        self.logger.info("Processing system file: %s", system_file_path)
        try:
            product_date_str = self.product_date
            product_date = datetime.strptime(product_date_str, '%m/%Y')
            self.logger.debug("Parsed product_date: %s", product_date)
            version = self._get_version_by_file_name(self.file_name)

            system_object = Sistema.read(system_file_path)
            df_system_energy_market = system_object.mercado_energia.copy()
            self.logger.debug("Loaded mercado_energia data, shape: %s", df_system_energy_market.shape)

            if df_system_energy_market is None:
                self.logger.error("No mercado_energia data found in file: %s", system_file_path)
                raise ValueError("Dados de sistema do mercado de energia não encontrados no arquivo!")

            self.logger.info("mercado_energia data found: %s records", len(df_system_energy_market))
            df_system_energy_market['data'] = pd.to_datetime(df_system_energy_market['data'], errors='coerce')
            df_system_energy_market = df_system_energy_market.dropna(subset=['data'])
            self.logger.debug("Cleaned mercado_energia data, shape: %s", df_system_energy_market.shape)

            df_system_energy_market['vl_ano'] = df_system_energy_market['data'].dt.year.astype(int)
            df_system_energy_market['vl_mes'] = df_system_energy_market['data'].dt.month.astype(int)

            df_system_energy_market = df_system_energy_market.rename(columns={
                'codigo_submercado': 'cd_submercado',
                'valor': 'vl_energia_total'
            })
            self.logger.debug("Renamed columns in mercado_energia: %s", df_system_energy_market.columns.tolist())

            df_system_non_simulated_plants = system_object.geracao_usinas_nao_simuladas
            self.logger.debug("Loaded geracao_usinas_nao_simuladas, shape: %s", df_system_non_simulated_plants.shape)

            df_system_non_simulated_plants['tipo_geracao'] = df_system_non_simulated_plants['indice_bloco'].map({
                1: 'vl_geracao_pch',
                2: 'vl_geracao_pct',
                3: 'vl_geracao_eol',
                4: 'vl_geracao_ufv',
                5: 'vl_geracao_pch_mmgd',
                6: 'vl_geracao_pct_mmgd',
                7: 'vl_geracao_eol_mmgd',
                8: 'vl_geracao_ufv_mmgd'
            })
            self.logger.debug("Mapped tipo_geracao: %s", df_system_non_simulated_plants['tipo_geracao'].unique())

            df_system_non_simulated_plants['vl_ano'] = df_system_non_simulated_plants['data'].dt.year
            df_system_non_simulated_plants['vl_mes'] = df_system_non_simulated_plants['data'].dt.month

            df_system_non_simulated_plants = df_system_non_simulated_plants.pivot_table(
                index=['codigo_submercado', 'vl_ano', 'vl_mes'],
                columns='tipo_geracao',
                values='valor',
                aggfunc='sum'
            ).reset_index()
            self.logger.debug("Pivoted geracao_usinas_nao_simuladas, shape: %s", df_system_non_simulated_plants.shape)

            df_system_non_simulated_plants = df_system_non_simulated_plants.rename(columns={'codigo_submercado': 'cd_submercado'})

            df_system = pd.merge(
                df_system_non_simulated_plants,
                df_system_energy_market,
                on=['cd_submercado', 'vl_ano', 'vl_mes'],
                how='left'
            )
            self.logger.debug("Merged system data, shape: %s", df_system.shape)

            df_system = df_system[~((df_system['vl_geracao_pch'].fillna(0) == 0) &
                                    (df_system['vl_geracao_pct'].fillna(0) == 0) &
                                    (df_system['vl_geracao_eol'].fillna(0) == 0) &
                                    (df_system['vl_geracao_ufv'].fillna(0) == 0) &
                                    (df_system['vl_geracao_pch_mmgd'].fillna(0) == 0) &
                                    (df_system['vl_geracao_pct_mmgd'].fillna(0) == 0) &
                                    (df_system['vl_geracao_eol_mmgd'].fillna(0) == 0) &
                                    (df_system['vl_geracao_ufv_mmgd'].fillna(0) == 0))]
            self.logger.debug("Filtered system data, shape: %s", df_system.shape)

            df_system['dt_deck'] = product_date.strftime('%Y-%m-%d')
            df_system['versao'] = version

            column_order = [
                'cd_submercado', 'vl_ano', 'vl_mes', 'vl_energia_total',
                'vl_geracao_pch', 'vl_geracao_pct', 'vl_geracao_eol', 'vl_geracao_ufv',
                'vl_geracao_pch_mmgd', 'vl_geracao_pct_mmgd', 'vl_geracao_eol_mmgd', 'vl_geracao_ufv_mmgd',
                'dt_deck', 'versao'
            ]

            df_system = df_system.reindex(columns=column_order)
            self.logger.info("Final system DataFrame ready, shape: %s", df_system.shape)

            return df_system
        except Exception as e:
            self.logger.error("Error processing system file: %s", str(e))
            raise

    def process_load_level_deck(self, load_level_file_path: str) -> Dict[str, pd.DataFrame]:
        self.logger.info("Processing load_level file: %s", load_level_file_path)
        try:
            product_date_str = self.product_date
            product_date = datetime.strptime(product_date_str, '%m/%Y')
            self.logger.debug("Parsed product_date: %s", product_date)
            version = self._get_version_by_file_name(self.file_name)

            load_level_object = Patamar.read(load_level_file_path)
            df_load_levels = load_level_object.carga_patamares.copy()
            self.logger.debug("Loaded carga_patamares, shape: %s", df_load_levels.shape)

            if df_load_levels is None or df_load_levels.empty:
                self.logger.error("No carga_patamares data found in file: %s", load_level_file_path)
                raise ValueError("Não foi possível extrair a carga dos patamares NEWAVE")

            df_monthly_duration = load_level_object.duracao_mensal_patamares.copy()
            self.logger.debug("Loaded duracao_mensal_patamares, shape: %s", df_monthly_duration.shape)

            if df_monthly_duration is None or df_monthly_duration.empty:
                self.logger.error("No duracao_mensal_patamares data found in file: %s", load_level_file_path)
                raise ValueError("Não foi possível extrair a duração mensal dos patamares NEWAVE")

            df_exchange = load_level_object.intercambio_patamares.copy()
            self.logger.debug("Loaded intercambio_patamares, shape: %s", df_exchange.shape)

            if df_exchange is None or df_exchange.empty:
                self.logger.error("No intercambio_patamares data found in file: %s", load_level_file_path)
                raise ValueError("Não foi possível extrair os intercâmbios por patamares NEWAVE")

            df_non_simulated_plants = load_level_object.usinas_nao_simuladas.copy()
            self.logger.debug("Loaded usinas_nao_simuladas, shape: %s", df_non_simulated_plants.shape)

            if df_non_simulated_plants is None or df_non_simulated_plants.empty:
                self.logger.error("No usinas_nao_simuladas data found in file: %s", load_level_file_path)
                raise ValueError("Não foi possível extrair as usinas não simuladas do NEWAVE")

            load_level_map = {'1': 'Pesado', '2': 'Medio', '3': 'Leve'}
            block_index_map = {1: 'PCH', 2: 'PCT', 3: 'EOL', 4: 'UFV', 5: 'PCH_MMGD', 6: 'PCT_MMGD', 7: 'EOL_MMGD', 8: 'UFV_MMGD'}
            submarket_map = {'1': 'SE', '2': 'S', '3': 'NE', '4': 'N', '11': 'FC'}

            df_load = df_load_levels.copy()
            df_load['patamar_nome'] = df_load['patamar'].astype(str).map(load_level_map)
            df_load['submercado_nome'] = df_load['codigo_submercado'].astype(str).map(submarket_map)
            df_load = df_load.rename(columns={'data': 'dt_referente', 'valor': 'pu_demanda_med', 'codigo_submercado': 'submercado'})
            df_load = df_load[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 'pu_demanda_med']]
            self.logger.debug("Processed load_df, shape: %s", df_load.shape)

            df_duration = df_monthly_duration.copy()
            df_duration['patamar_nome'] = df_duration['patamar'].astype(str).map(load_level_map)
            df_duration = df_duration.rename(columns={'data': 'dt_referente', 'valor': 'duracao_mensal'})
            df_duration = df_duration[['dt_referente', 'patamar', 'patamar_nome', 'duracao_mensal']]
            self.logger.debug("Processed duration_df, shape: %s", df_duration.shape)

            df_exchange = df_exchange.copy()
            df_exchange['patamar_nome'] = df_exchange['patamar'].astype(str).map(load_level_map)
            df_exchange['submercado_de_nome'] = df_exchange['submercado_de'].astype(str).map(submarket_map)
            df_exchange['submercado_para_nome'] = df_exchange['submercado_para'].astype(str).map(submarket_map)
            df_exchange = df_exchange.rename(columns={'data': 'dt_referente', 'valor': 'pu_intercambio_med'})
            df_exchange = df_exchange[['dt_referente', 'patamar', 'patamar_nome', 'submercado_de', 'submercado_de_nome',
                                      'submercado_para', 'submercado_para_nome', 'pu_intercambio_med']]
            self.logger.debug("Processed exchange_df, shape: %s", df_exchange.shape)

            df_plants = df_non_simulated_plants.copy()
            df_plants['patamar_nome'] = df_plants['patamar'].astype(str).map(load_level_map)
            df_plants['submercado_nome'] = df_plants['codigo_submercado'].astype(str).map(submarket_map)
            df_plants['indice_bloco_nome'] = df_plants['indice_bloco'].map(block_index_map)
            df_plants = df_plants.rename(columns={'data': 'dt_referente', 'codigo_submercado': 'submercado', 'valor': 'pu_montante_med'})
            df_plants = df_plants[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome',
                                   'indice_bloco', 'indice_bloco_nome', 'pu_montante_med']]
            self.logger.debug("Processed plants_df, shape: %s", df_plants.shape)

            df_transformed_load = df_load.copy()
            df_transformed_load['indice_bloco'] = 'CARGA'
            df_transformed_load['valor_pu'] = df_transformed_load['pu_demanda_med']
            df_transformed_load = df_transformed_load.drop(columns=['pu_demanda_med'])

            df_transformed_plants = df_plants.copy()
            df_transformed_plants['valor_pu'] = df_transformed_plants['pu_montante_med']
            df_transformed_plants = df_transformed_plants.drop(columns=['pu_montante_med'])

            df_load_level_plants = pd.concat([df_transformed_load, df_transformed_plants], ignore_index=True)
            self.logger.debug("Concatenated load_level_plants_df, shape: %s", df_load_level_plants.shape)

            df_load_level_plants = pd.merge(
                df_load_level_plants,
                df_duration,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='left'
            )
            self.logger.debug("Merged load_level_plants_df with duration_df, shape: %s", df_load_level_plants.shape)

            df_load_level_plants['dt_deck'] = product_date.strftime('%Y-%m-%d')
            df_load_level_plants['versao'] = version
            df_load_level_plants = df_load_level_plants.drop(columns=['patamar', 'submercado'])
            df_load_level_plants = df_load_level_plants.rename(columns={'patamar_nome': 'patamar', 'submercado_nome': 'submercado'})

            df_load_level_plants['indice_bloco'] = df_load_level_plants.apply(
                lambda row: row['indice_bloco_nome'] if pd.notna(row.get('indice_bloco_nome')) else row['indice_bloco'], axis=1
            )
            if 'indice_bloco_nome' in df_load_level_plants.columns:
                df_load_level_plants = df_load_level_plants.drop(columns=['indice_bloco_nome'])
            self.logger.debug("Processed load_level_plants_df, shape: %s", df_load_level_plants.shape)

            columns_table1 = [
                'dt_referente', 'patamar', 'submercado', 'valor_pu',
                'duracao_mensal', 'indice_bloco', 'dt_deck', 'versao'
            ]
            df_load_level_plants['dt_referente'] = df_load_level_plants['dt_referente'].dt.strftime('%Y-%m-%d')
            df_load_level_plants['dt_deck'] = df_load_level_plants['dt_deck'].astype(str)
            df_load_level_plants = df_load_level_plants[columns_table1]
            self.logger.debug("Final load_level_plants_df, shape: %s", df_load_level_plants.shape)

            df_load_level_exchange = pd.merge(
                df_exchange,
                df_duration,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='inner'
            )
            self.logger.debug("Merged load_level_exchange_df, shape: %s", df_load_level_exchange.shape)

            df_load_level_exchange['dt_deck'] = product_date.strftime('%Y-%m-%d')
            if df_load_level_exchange['dt_deck'].dtype == 'object':
                df_load_level_exchange['dt_deck'] = df_load_level_exchange['dt_deck'].astype(str)
            df_load_level_exchange['versao'] = version
            df_load_level_exchange = df_load_level_exchange.drop(columns=['patamar', 'submercado_de', 'submercado_para'])
            df_load_level_exchange = df_load_level_exchange.rename(columns={
                'patamar_nome': 'patamar',
                'submercado_de_nome': 'submercado_de',
                'submercado_para_nome': 'submercado_para'
            })
            self.logger.debug("Processed load_level_exchange_df, shape: %s", df_load_level_exchange.shape)

            columns_table2 = [
                'dt_referente', 'patamar', 'submercado_de', 'submercado_para',
                'pu_intercambio_med', 'duracao_mensal', 'dt_deck', 'versao'
            ]
            df_load_level_exchange['dt_referente'] = df_load_level_exchange['dt_referente'].dt.strftime('%Y-%m-%d')
            df_load_level_exchange['dt_deck'] = df_load_level_exchange['dt_deck'].astype(str)
            df_load_level_exchange['pu_intercambio_med'] = df_load_level_exchange['pu_intercambio_med'].round(4)
            df_load_level_exchange = df_load_level_exchange[columns_table2]
            df_load_level_exchange = df_load_level_exchange.replace([np.inf, -np.inf], np.nan)
            self.logger.debug("Final load_level_exchange_df, shape: %s", df_load_level_exchange.shape)

            dict_result = {
                "df_load_level_plants": df_load_level_plants,
                "df_load_level_exchange": df_load_level_exchange,
            }
            self.logger.info("Load level processing completed, result keys: %s", list(dict_result.keys()))
            return dict_result
        except Exception as e:
            self.logger.error("Error processing load_level file: %s", str(e))
            raise


class NewaveUpdater:
    def __init__(self):
        self.logger = setup_logger()
        self.logger.debug("Initializing NewaveUpdater")
        self.logger.info("NewaveUpdater initialized successfully")

    def get_database_data(self, endpoint: str) -> pd.DataFrame:
        self.logger.info("Fetching data from endpoint: %s", endpoint)
        try:
            response = requests.get(endpoint, headers=get_auth_header())
            response.raise_for_status()
            data = response.json()
            df_data = pd.DataFrame(data)
            self.logger.info("Successfully fetched data, shape: %s", df_data.shape)
            self.logger.debug("Fetched data sample: %s", df_data.head().to_dict())
            return df_data
        except requests.RequestException as e:
            self.logger.error("Error fetching data from API %s: %s", endpoint, str(e))
            raise

    def calculate_monthly_wind_average(self, df_data: pd.DataFrame, df_non_simulated: pd.DataFrame, dict_submarket_map: Dict[str, int]) -> pd.DataFrame:
        self.logger.info("Calculating monthly wind generation averages")
        try:
            dates = pd.date_range(start=pd.to_datetime(min(df_data['inicioSemana'])).replace(day=1),
                                  end=pd.to_datetime(max(df_data['inicioSemana'])) + pd.offsets.MonthEnd(0), freq='D')
            self.logger.debug("Generated date range: %s to %s", dates[0], dates[-1])

            df_daily = pd.DataFrame()
            df_data['inicioSemana'] = pd.to_datetime(df_data['inicioSemana'])
            for date in dates:
                for submarket in df_data['submercado'].unique():
                    week_data = SemanaOperativa(date)
                    week_start = pd.to_datetime(week_data.week_start)
                    filter_condition = ((df_non_simulated['data'] == date.replace(day=1)) &
                                        (df_non_simulated['indice_bloco'] == 3) &
                                        (df_non_simulated['codigo_submercado'] == dict_submarket_map[submarket]))
                    value = df_non_simulated[filter_condition]['valor'].values[0] if not df_non_simulated[filter_condition].empty else 0
                    if week_start in df_data['inicioSemana'].unique():
                        filter_condition = (df_data['submercado'] == submarket) & (df_data['inicioSemana'] == week_start)
                        value = df_data.loc[filter_condition, 'mediaPonderada'].values[0] if not df_data[filter_condition].empty else value
                    df_daily = pd.concat([df_daily, pd.DataFrame([{"data": date, "submercado": submarket, "valor": value}])],
                                         ignore_index=True)
            self.logger.debug("Generated daily_df, shape: %s", df_daily.shape)

            df_daily['valor'] = df_daily['valor'].fillna(0)
            df_daily['mes_ano'] = df_daily['data'].dt.to_period('M')
            df_daily = df_daily.groupby(['mes_ano', 'submercado'])['valor'].mean().round(2).unstack()
            self.logger.info("Calculated monthly averages, shape: %s", df_daily.shape)
            self.logger.debug("Monthly averages sample: %s", df_daily.head().to_dict())
            return df_daily
        except Exception as e:
            self.logger.error("Error calculating monthly wind averages: %s", str(e))
            raise

    def update_wind_data(self, system_file_path: dict):
        dict_submarket_map = {'SE': 1, 'S': 2, 'NE': 3, 'N': 4}
        self.logger.info("Updating wind generation for system file: %s", system_file_path['system'])
        try:
            df_data = self.get_database_data(constants.ENDPOINT_WEOL_PONDERADO)
            update_count = 0
            dger = Dger.read(system_file_path['dger'])
            deck_date = datetime(dger.ano_inicio_estudo, dger.mes_inicio_estudo, 1)
            self.logger.debug("Study start date from dger.dat: %s", deck_date)

            df_non_simulated = system.geracao_usinas_nao_simuladas
            self.logger.debug("Loaded non-simulated generation records, shape: %s", df_non_simulated.shape)

            df_daily = self.calculate_monthly_wind_average(df_data, df_non_simulated, dict_submarket_map)
            self.logger.info("Generated monthly averages for %s months across %s submarkets",
                            len(df_daily), len(df_daily.columns))

            for month_year in df_daily.index:
                if deck_date.strftime('%Y-%m') == str(month_year):
                    self.logger.info("Updating wind generation for deck date: %s", deck_date.strftime('%Y-%m'))
                    for submarket in df_daily.keys():
                        filter_condition = ((df_non_simulated['data'] == month_year.start_time) &
                                            (df_non_simulated['indice_bloco'] == 3) &
                                            (df_non_simulated['codigo_submercado'] == dict_submarket_map[submarket]))
                        if not df_non_simulated[filter_condition].empty:
                            old_value = df_non_simulated.loc[filter_condition, 'valor'].values[0]
                            new_value = df_daily.loc[month_year][submarket]
                            df_non_simulated.loc[filter_condition, 'valor'] = new_value
                            update_count += 1
                            self.logger.info("Updated submarket: %s, month: %s, old_value: %s, new_value: %s",
                                            submarket, month_year, old_value, new_value)

            system.geracao_usinas_nao_simuladas = df_non_simulated
            self.logger.info("Writing updated system to %s with %s value updates", system_file_path['system'], update_count)
            system.write(system_file_path['system'])
        except Exception as e:
            self.logger.error("Error updating wind data for file %s: %s", system_file_path['system'], str(e))
            raise


class VazoesBinaryReader:
    def __init__(self):
        self.logger = setup_logger()
        self.logger.debug("Initializing VazoesBinaryReader")
        self.postos = 320
        self.format_string = f"<{self.postos}i"
        self.logger.info("VazoesBinaryReader initialized successfully")

    def run_process(self, extracted_path: str):
        self.logger.info("Processing flow data from path: %s", extracted_path)
        try:
            self.logger.debug("Extracted newave ZIP to path: %s", extracted_path)
            flow_files = [file for file in os.listdir(extracted_path) if "vazoes.dat" in file.lower()]
            self.logger.debug("Found flow files: %s", flow_files)
            flow_file_path = os.path.join(extracted_path, flow_files[0])
            df_flow = self.read_binary_file(flow_file_path)
            self.logger.info("Read flow data, shape: %s", df_flow.shape)
            self.post_data(df_flow)
            self.logger.info("Flow data posted successfully")
        except Exception as e:
            self.logger.error("Error in VazoesBinaryReader run_process: %s", str(e))
            raise

    def read_binary_file(self, file_path: str) -> pd.DataFrame:
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
                    df_data = pd.DataFrame(struct.unpack(self.format_string, data))
                    df_data.columns = ['vazao']
                    df_data['mes'] = month
                    df_data['ano'] = year
                    df_data['posto'] = [i + 1 for i in range(self.postos)]
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

    def post_data(self, df_data: pd.DataFrame) -> Dict:
        self.logger.info("Posting flow data, shape: %s", df_data.shape)
        try:
            response = requests.post(
                constants.ENDPOINT_HISTORICO_VAZOES,
                json=df_data.to_dict('records'),
                headers=get_auth_header()
            )
            if response.status_code != 200:
                response.raise_for_status()
            self.logger.info("Successfully posted flow data, status code: %s", response.status_code)
            return response.json()
        except Exception as e:
            self.logger.error("Error posting flow data: %s", str(e))
            raise


if __name__ == "__main__":
    logger = setup_logger()
    logger.debug("Starting main execution")
    payload = {
        "dataProduto": "11/2025",
        "filename": "Deck NEWAVE Preliminar.zip",
        "macroProcesso": "Programação da Operação",
        "nome": "Deck NEWAVE Preliminar",
        "periodicidade": "2025-11-01T00:00:00",
        "periodicidadeFinal": "2025-11-30T23:59:59",
        "processo": "Médio Prazo",
        "s3Key": "webhooks/Deck NEWAVE Preliminar/4c9a79bd-d515-436e-b849-95b49061b27c_Deck NEWAVE Preliminar.zip",
        "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiIvc2l0ZXMvOS81Mi83MS9Qcm9kdXRvcy8yODcvMjctMTAtMjAyNV8xMjA2MDAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiRGVjayBORVdBVkUgUHJlbGltaW5hciIsIklzRmlsZSI6IkZhbHNlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc2MTY2NzExNiwibmJmIjoxNzYxNTgwNDc2fQ.zQlSC5JCd6SHytwuwFXwvZZhzHWccgONerg_HG3Wje4",
        "webhookId": "4c9a79bd-d515-436e-b849-95b49061b27c"
        }
    logger.debug("Payload: %s", payload)

    payload = WebhookSintegreSchema(**payload)
    logger.info("Parsed payload into WebhookSintegreSchema")

    deck_newave = DecksNewave(payload)
    logger.info("Created DecksNewave instance")

    deck_newave.run_workflow()
    logger.info("Main execution completed")