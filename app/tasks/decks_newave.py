import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, get_auth_header, HtmlBuilder, Constants, extract_zip
from middle.airflow import trigger_dag
from middle.message import send_whatsapp_message
constants = Constants()
logger = setup_logger()
html_builder = HtmlBuilder()

from typing import Optional, Dict, Any
import shutil
import zipfile
import glob
import pdb
import pandas as pd
import numpy as np
import datetime
import requests
import zipfile as zipFile
from inewave.newave import Patamar, Cadic, Sistema
sys.path.append(os.path.join(constants.PATH_PROJETOS, "estudos-middle/update_estudos"))
from update_newave import NewaveUpdater

class DecksNewave(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.dataProduto = self.payload.dataProduto
        self.filename = self.payload.filename
        self.trigger_dag = trigger_dag   
        self.process_functions = ProcessFunctions(payload)
        self.update_weol = UpdateWeol()
        self.gerar_tabela = GerarTabelaDiferenca(payload)
    
        logger.info("Inicializado DecksNewave com payload: %s", payload)
               
        
    def run_workflow(self) -> Dict[str, Any]:
        try:
            
            file_path = self.download_extract_files()
            
            self.run_process(file_path)

            logger.info("Workflow do produto DecksNewave terminado com sucesso!")
        
        except Exception as e:
            error_msg = f"Erro no fluxo de processamento do DecksNewave: {str(e)}"
            logger.error(error_msg)
            raise
        
        
    def run_process(self, file_path):
        
    
        process_result = self.process_file(file_path)
        
        self.post_data(process_result)
        
        if 'preliminar' in self.filename:
            self.payload['dt_ref'] = self.payload['dataProduto']
            self.trigger_dag(dag_id="1.17-NEWAVE_ONS-TO-CCEE", conf=self.payload)
        
        self.gerar_tabela.run_process()
        
    def update_wind(self, file_path):
        updater = NewaveUpdater()
        params = {}
        params['produto'] = 'EOLICA'
        params['id_estudo'] = None
        params['path_download'] = ''
        params['path_out'] = ''
        updater.update_eolica(params,[file_path])

    
    def process_file(self, file_path) -> dict:
        try:
            
            dat_files = self.process_functions.extrair_arquivos_dat(file_path)
            
            if 'preliminar' in self.filename.lower():
                self.update_wind(dat_files['sistema'])
            
            process_cadic_result = self.process_functions.processar_deck_nw_cadic(dat_files['cadic'])
            
            process_sist_result = self.process_functions.processar_deck_nw_sist(dat_files['sistema'])
            
            process_patamar_result = self.process_functions.processar_deck_nw_patamar(dat_files['patamar'])
            
            process_result = {
                'process_cadic_result': process_cadic_result,
                'process_sist_result': process_sist_result,
                'process_patamar_result': process_patamar_result
            }
            
            return process_result 
            
        except Exception as e:
            logger.error("Falha na leitura e processamento do arquivo: %s", str(e), exc_info=True)
            
    def post_data(self, process_result:pd.DataFrame) -> dict:
        try:
            logger.info("Enviando dados para a API...")
            
            auth_headers = get_auth_header()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
            
            base_url = constants.BASE_URL
            api_url = f"{base_url}/api/v2"
            
            nw_cadic_records = process_result.get('process_cadic_result', [])
            nw_sist_records = process_result.get('process_sist_result', [])
            patamar_process_result = process_result.get('process_patamar_result', [])
            nw_patamar_carga_usinas_records = patamar_process_result.get('patamar_carga_usinas_records', [])
            nw_patamar_intercambio_records = patamar_process_result.get('patamar_intercambio_records', [])
            
            sistema_url = f"{api_url}/decks/newave/sistema"
            cadic_url = f"{api_url}/decks/newave/cadic"
            patamar_carga_usinas_url = f"{api_url}/decks/newave/patamar/carga_usinas"
            patamar_intercambio_url = f"{api_url}/decks/newave/patamar/intercambio"
            
                
            logger.info(f"Enviando dados para: {sistema_url}")
                
            request_sistema = requests.post(
                sistema_url,
                headers=headers,
                json=nw_sist_records,
            )
            
            if request_sistema.status_code != 200:
                raise ValueError(f"Erro ao enviar carga do SISTEMA para API: {request_sistema.text}")

            logger.info(f"Enviando dados para: {cadic_url}")

            request_cadic = requests.post(
                cadic_url,
                headers=headers,
                json=nw_cadic_records,
            )
            
            if request_cadic.status_code != 200:
                raise ValueError(f"Erro ao enviar carga do CADIC para API: {request_cadic.text}")
            
            logger.info(f"Enviando dados para: {patamar_carga_usinas_url}")
            
            request_patamar_carga_usinas = requests.post(
                patamar_carga_usinas_url,
                headers=headers,
                json=nw_patamar_carga_usinas_records,  
            )
            
            if request_patamar_carga_usinas.status_code != 200:
                raise ValueError(f"Erro ao enviar patamar do newave de carga e usina para API: {request_patamar_carga_usinas.text}")
            
            logger.info(f"Enviando dados para: {patamar_intercambio_url}")
            
            request_patamar_intercambio = requests.post(
                patamar_intercambio_url,
                headers=headers,
                json=nw_patamar_intercambio_records,  
            )
            
            if request_patamar_intercambio.status_code != 200:
                raise ValueError(f"Erro ao enviar patamar do newave de intercambio para API: {request_patamar_intercambio.text}")
            
        
        except Exception as e:
            logger.error("Falha no import dos dados ao banco: %s", str(e), exc_info=True)
              
              
               
class ProcessFunctions():
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        self.dataProduto = payload.dataProduto
        self.filename = payload.filename
        
    
    def _get_version_by_filename(self, filename: str) -> str:
        try:
            if 'preliminar' in filename.lower():
                return 'preliminar'
            elif 'definitivo' in filename.lower():
                return 'definitivo'  
            else:
                raise ValueError("Nome do arquivo não contém 'preliminar' ou 'definitivo'.")
                
        except Exception as e:
            logger.error(f"Erro ao determinar a versão pelo nome do arquivo: {e}")
            raise
    
    def extrair_arquivos_dat(
        self,
        download_extract_filepath: Dict[str, Any]
    ) -> list:

        try:
            
            path = extract_zip(download_extract_filepath)
            zip_ref = glob.glob(os.path.join(path, '*.zip'))
            zip_ref = extract_zip(zip_ref[0])
                 
            dict_paths = {'cadic':None, 'sistema':None, 'patamar':None}

            for file in os.listdir(zip_ref):
                if file.lower().startswith('c_adic'):
                    dict_paths['cadic'] = zip_ref + '/' + file
                if file.lower().startswith('sistema'):
                    dict_paths['sistema'] = zip_ref + '/' + file
                if file.lower().startswith('patamar'):
                    dict_paths['patamar'] = zip_ref + '/' + file
            return dict_paths
                    
        except Exception as e:
            logger.error(f"Erro ao extrair arquivos .DAT: {e}")
            raise
    
    def processar_deck_nw_cadic( 
        self,
        cadic_file
    ) -> Dict[str, Any]:
        """
        Tarefa para processar os valores do C_ADIC.DAT.
        
        :param payload: Dicionário com os dados do C_ADIC.DAT.
        :return: Dicionário com o status e mensagem do processamento.
        """
        
        try:
            logger.info("Processando o C_ADIC.DAT do Deck Newave...")
            
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            cadic_object = Cadic.read(cadic_file)
            nw_cadic_df = cadic_object.cargas.copy()

            if nw_cadic_df is None:
                error_msg = "Dados do c_adic não encontrados no arquivo!"
                raise ValueError(error_msg)
            else:
                logger.info(f"Dados do c_adic encontrados: {nw_cadic_df} ")
                logger.info(f"Mercado de energia total carregado com sucesso. Total de registros: {len(nw_cadic_df)}")

                nw_cadic_df['data'] = pd.to_datetime(nw_cadic_df['data'], errors='coerce')
                nw_cadic_df = nw_cadic_df[nw_cadic_df['data'].dt.year < 9999]
                nw_cadic_df['vl_ano'] = nw_cadic_df['data'].dt.year.astype(int)
                nw_cadic_df['vl_mes'] = nw_cadic_df['data'].dt.month.astype(int)
                nw_cadic_df = nw_cadic_df.dropna(subset=['valor'])
                
                mapeamento_razao = {
                    'CONS.ITAIPU': 'vl_const_itaipu',
                    'ANDE': 'vl_ande',
                    'MMGD SE': 'vl_mmgd_se',
                    'MMGD S': 'vl_mmgd_s',
                    'MMGD NE': 'vl_mmgd_ne',
                    'BOA VISTA': 'vl_boa_vista',
                    'MMGD N': 'vl_mmgd_n'
                }
            
                nw_cadic_df['coluna'] = nw_cadic_df['razao'].map(mapeamento_razao)

                nw_cadic_df = nw_cadic_df.pivot_table(
                    index=['vl_ano', 'vl_mes'], 
                    columns='coluna',
                    values='valor',
                    aggfunc='first'  
                ).reset_index()
                
                nw_cadic_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
                
                nw_cadic_df['versao'] = versao
                
                nw_cadic_records = nw_cadic_df.to_dict('records')
                
                logger.info(f"- Valores do Cadic: ({len(nw_cadic_records)} registros)")
                
            return nw_cadic_records
                
                    
        
        except Exception as e:
            logger.error(f"Erro ao processar C_ADIC do Deck Newave: {e}")
            return {"status": "error", "message": str(e)}
    
    def processar_deck_nw_sist(
        self, 
        sistema_file: str
    ) -> Dict[str, Any]:
        """
        Tarefa para processar os valores do SISTEMA.DAT.
        
        :return: Dicionário com o status e mensagem do processamento.
        """
        
        try:
            logger.info("Processando o SISTEMA.DAT do Deck Newave...")
            
                     
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            sistema_object = Sistema.read(sistema_file)
            sistema_mercado_energia_df = sistema_object.mercado_energia.copy()   
            
            if sistema_mercado_energia_df is None:
                error_msg = "Dados de sistema do mercado de energia não encontrados no arquivo!"
                raise ValueError(error_msg)
            
            else:
                logger.info(f"Dados de sistema do mercado de energia encontrados: {sistema_mercado_energia_df} ")
                logger.info(f"Mercado de energia total carregado com sucesso. Total de registros: {len(sistema_mercado_energia_df)}")
            
            sistema_mercado_energia_df['data'] = pd.to_datetime(sistema_mercado_energia_df['data'], errors='coerce')
            sistema_mercado_energia_df = sistema_mercado_energia_df.dropna(subset=['data'])
            
            sistema_mercado_energia_df['vl_ano'] = sistema_mercado_energia_df['data'].dt.year.astype(int)
            sistema_mercado_energia_df['vl_mes'] = sistema_mercado_energia_df['data'].dt.month.astype(int)
            
            sistema_mercado_energia_df = sistema_mercado_energia_df.rename(columns={
                'codigo_submercado': 'cd_submercado',
                'valor': 'vl_energia_total'
            })
            
            sistema_geracao_unsi_df = sistema_object.geracao_usinas_nao_simuladas
            
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
            
            sistema_geracao_unsi_df['vl_ano'] = sistema_geracao_unsi_df['data'].dt.year
            sistema_geracao_unsi_df['vl_mes'] = sistema_geracao_unsi_df['data'].dt.month
            
            sistema_geracao_unsi_df = sistema_geracao_unsi_df.pivot_table(
                index=['codigo_submercado', 'vl_ano', 'vl_mes'], 
                columns='tipo_geracao',
                values='valor',
                aggfunc='sum'  
            ).reset_index()
            
            sistema_geracao_unsi_df = sistema_geracao_unsi_df.rename(columns={'codigo_submercado': 'cd_submercado'})
            
            nw_sistema_df = pd.merge(
                sistema_geracao_unsi_df, 
                sistema_mercado_energia_df,
                on=['cd_submercado', 'vl_ano', 'vl_mes'],
                how='left'
            )
            
            nw_sistema_df = nw_sistema_df[~((nw_sistema_df['vl_geracao_pch'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pct'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_eol'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_ufv'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pch_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pct_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_eol_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_ufv_mmgd'].fillna(0) == 0))
            ]
            
            nw_sistema_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            nw_sistema_df['versao'] = versao
            
            ordem_colunas = [
                'cd_submercado',
                'vl_ano',
                'vl_mes',
                'vl_energia_total',
                'vl_geracao_pch',
                'vl_geracao_pct',
                'vl_geracao_eol',
                'vl_geracao_ufv',
                'vl_geracao_pch_mmgd',
                'vl_geracao_pct_mmgd',
                'vl_geracao_eol_mmgd',
                'vl_geracao_ufv_mmgd',
                'dt_deck',
                'versao'
            ]
            
            nw_sistema_df = nw_sistema_df.reindex(columns=ordem_colunas)
            nw_sistema_records = nw_sistema_df.to_dict('records')
            
            logger.info(f"- Valores do Sistema: ({len(nw_sistema_records)} registros)")
            
            return nw_sistema_records
                    
        except Exception as e:
            logger.error(f"Erro ao processar os valores de carga do Sistema do DECK Newave : {e}")
            raise
    
    def processar_deck_nw_patamar(
        self,
        patamar_file: str
    ) -> Dict[str, Any]:
        """
        Tarefa para processar os valores do PATAMAR.DAT.
        
        :return: Dicionário com o status e mensagem do processamento.
        """
        
        try:
            logger.info("Processando o PATAMAR.DAT do Deck Newave...")
            
      
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            patamar_object = Patamar.read(patamar_file)

            patamares = {
                '1': 'Pesado',
                '2': 'Medio',
                '3': 'Leve'
            }

            indices_bloco = {
                1: 'PCH',
                2: 'PCT',
                3: 'EOL',
                4: 'UFV',
                5: 'PCH_MMGD',
                6: 'PCT_MMGD',
                7: 'EOL_MMGD',
                8: 'UFV_MMGD'
            }

            submercados = {
                '1': 'SE',
                '2': 'S',
                '3': 'NE',
                '4': 'N',
                '11': 'FC'
            }

            # Verificar se os dados foram extraídos corretamente
            carga_patamares_df = patamar_object.carga_patamares.copy() 

            if carga_patamares_df is None or carga_patamares_df.empty:
                error_msg = "Não foi possível extrair a carga dos patamares NEWAVE"
                raise ValueError(error_msg)


            duracao_mensal_patamares_df = patamar_object.duracao_mensal_patamares.copy() 

            if duracao_mensal_patamares_df is None or duracao_mensal_patamares_df.empty:
                error_msg = "Não foi possível extrair a duração mensal dos patamares NEWAVE"
                raise ValueError(error_msg)


            intercambio_patamares_df = patamar_object.intercambio_patamares.copy() 

            if intercambio_patamares_df is None or intercambio_patamares_df.empty:
                error_msg = "Não foi possível extrair os intercâmbios por patamares NEWAVE"
                raise ValueError(error_msg)


            usinas_nao_simuladas_df = patamar_object.usinas_nao_simuladas.copy()

            if usinas_nao_simuladas_df is None or usinas_nao_simuladas_df.empty:
                error_msg = "Não foi possível extrair as usinas não simuladas do NEWAVE"
                raise ValueError(error_msg)


            # Processamento das tabelas
            # 1. Processar carga_patamares_df
            carga_df = carga_patamares_df.copy()
            carga_df['patamar_nome'] = carga_df['patamar'].astype(str).map(patamares)
            carga_df['submercado_nome'] = carga_df['codigo_submercado'].astype(str).map(submercados)
            carga_df = carga_df.rename(columns={
                'data': 'dt_referente',
                'valor': 'pu_demanda_med',
                'codigo_submercado': 'submercado'
            })
            carga_df = carga_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 'pu_demanda_med']]

            # 2. Processar duracao_mensal_patamares_df
            duracao_df = duracao_mensal_patamares_df.copy()
            duracao_df['patamar_nome'] = duracao_df['patamar'].astype(str).map(patamares)
            duracao_df = duracao_df.rename(columns={
                'data': 'dt_referente',
                'valor': 'duracao_mensal'
            })
            duracao_df = duracao_df[['dt_referente', 'patamar', 'patamar_nome', 'duracao_mensal']]

            # 3. Processar intercambio_patamares_df
            intercambio_df = intercambio_patamares_df.copy()
            intercambio_df['patamar_nome'] = intercambio_df['patamar'].astype(str).map(patamares)
            intercambio_df['submercado_de_nome'] = intercambio_df['submercado_de'].astype(str).map(submercados)
            intercambio_df['submercado_para_nome'] = intercambio_df['submercado_para'].astype(str).map(submercados)
            intercambio_df = intercambio_df.rename(columns={
                'data': 'dt_referente',
                'valor': 'pu_intercambio_med'
            })
            intercambio_df = intercambio_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado_de', 'submercado_de_nome', 
                                           'submercado_para', 'submercado_para_nome', 'pu_intercambio_med']]

            # 4. Processar usinas_nao_simuladas_df
            usinas_df = usinas_nao_simuladas_df.copy()
            usinas_df['patamar_nome'] = usinas_df['patamar'].astype(str).map(patamares)
            usinas_df['submercado_nome'] = usinas_df['codigo_submercado'].astype(str).map(submercados)
            usinas_df['indice_bloco_nome'] = usinas_df['indice_bloco'].map(indices_bloco)
            usinas_df = usinas_df.rename(columns={
                'data': 'dt_referente',
                'codigo_submercado': 'submercado',
                'valor': 'pu_montante_med'
            })
            usinas_df = usinas_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 
                                 'indice_bloco', 'indice_bloco_nome', 'pu_montante_med']]

            # TABELA 1: Preparar DataFrame de carga com indice_bloco = 'CARGA'
            carga_transformada_df = carga_df.copy()
            carga_transformada_df['indice_bloco'] = 'CARGA'
            carga_transformada_df['valor_pu'] = carga_transformada_df['pu_demanda_med']
            carga_transformada_df = carga_transformada_df.drop(columns=['pu_demanda_med'])
            
            # Preparar DataFrame de usinas não simuladas
            usinas_transformada_df = usinas_df.copy()
            usinas_transformada_df['valor_pu'] = usinas_transformada_df['pu_montante_med']
            usinas_transformada_df = usinas_transformada_df.drop(columns=['pu_montante_med'])
            
            # Concatenar os dois DataFrames
            patamar_carga_usinas_df = pd.concat([carga_transformada_df, usinas_transformada_df], ignore_index=True)

            # Adicionar duração mensal
            patamar_carga_usinas_df = pd.merge(
                patamar_carga_usinas_df,
                duracao_df,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='left'
            )

            # Adicionar dados complementares na tabela 1
            patamar_carga_usinas_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            patamar_carga_usinas_df['versao'] = versao

            # Remover colunas numéricas originais e renomear as colunas de texto
            patamar_carga_usinas_df = patamar_carga_usinas_df.drop(columns=['patamar', 'submercado'])
            patamar_carga_usinas_df = patamar_carga_usinas_df.rename(columns={
                'patamar_nome': 'patamar',
                'submercado_nome': 'submercado'
            })
            
            # Para a coluna indice_bloco, usar o valor de indice_bloco_nome quando disponível, senão manter o valor atual
            patamar_carga_usinas_df['indice_bloco'] = patamar_carga_usinas_df.apply(
                lambda row: row['indice_bloco_nome'] if pd.notna(row.get('indice_bloco_nome')) else row['indice_bloco'], axis=1
            )
            
            # Remover a coluna indice_bloco_nome se existir
            if 'indice_bloco_nome' in patamar_carga_usinas_df.columns:
                patamar_carga_usinas_df = patamar_carga_usinas_df.drop(columns=['indice_bloco_nome'])

            # Selecionar colunas finais da tabela 1
            colunas_tabela1 = [
                'dt_referente', 'patamar', 'submercado', 'valor_pu',
                'duracao_mensal', 'indice_bloco', 'dt_deck', 'versao'
            ]

            patamar_carga_usinas_df['dt_referente'] = patamar_carga_usinas_df['dt_referente'].dt.strftime('%Y-%m-%d')
            patamar_carga_usinas_df['dt_deck'] = patamar_carga_usinas_df['dt_deck'].astype(str)
            patamar_carga_usinas_df = patamar_carga_usinas_df[colunas_tabela1]
            
            # TABELA 2: Intercâmbios por Patamares + Duração
            patamar_intercambio_df = pd.merge(
                intercambio_df,
                duracao_df,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='inner'
            )

            # Adicionar dados complementares na tabela 2
            patamar_intercambio_df['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            if patamar_intercambio_df['dt_deck'].dtype == 'object':
                patamar_intercambio_df['dt_deck'] = patamar_intercambio_df['dt_deck'].astype(str)
            patamar_intercambio_df['versao'] = versao

            # Remover colunas numéricas originais e renomear as colunas de texto
            patamar_intercambio_df = patamar_intercambio_df.drop(columns=['patamar', 'submercado_de', 'submercado_para'])
            patamar_intercambio_df = patamar_intercambio_df.rename(columns={
                'patamar_nome': 'patamar',
                'submercado_de_nome': 'submercado_de',
                'submercado_para_nome': 'submercado_para'
            })

            # Selecionar colunas finais da tabela 2
            colunas_tabela2 = [
                'dt_referente', 'patamar', 'submercado_de', 'submercado_para',
                'pu_intercambio_med', 'duracao_mensal', 'dt_deck', 'versao'
            ]

            patamar_intercambio_df['dt_referente'] = patamar_intercambio_df['dt_referente'].dt.strftime('%Y-%m-%d')
            patamar_intercambio_df['dt_deck'] = patamar_intercambio_df['dt_deck'].astype(str)
            
            patamar_intercambio_df['pu_intercambio_med'] = patamar_intercambio_df['pu_intercambio_med'].round(4)
            
            patamar_intercambio_df = patamar_intercambio_df[colunas_tabela2]
            patamar_intercambio_df = patamar_intercambio_df.replace([np.inf, -np.inf], np.nan)
            
            patamar_carga_usinas_records = patamar_carga_usinas_df.to_dict('records')
            patamar_intercambio_records = patamar_intercambio_df.to_dict('records')

            logger.info(f"- Carga e usinas: ({len(patamar_carga_usinas_records)} registros)")
            logger.info(f"- Intercâmbio:({len(patamar_intercambio_records)} registros)")
            
            return {
                "patamar_carga_usinas_records": patamar_carga_usinas_records,
                "patamar_intercambio_records": patamar_intercambio_records,
            }
        
        except Exception as e:
            logger.error(f"Erro ao processar PATAMAR.DAT: {e}")
            raise
 
class GerarTabelaDiferenca():  
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        constants = Constants()
        self.payload = payload
        self.headers = get_auth_header()
        self.dataProduto = payload.dataProduto
        self.filename = payload.filename
        self.url_html_to_image = constants.URL_HTML_TO_IMAGE
        self.url_unsi          = constants.GET_NEWAVE_SISTEMA_TOTAL_UNSI
        self.url_carga_global  = constants.GET_NEWAVE_SISTEMA_CARGAS_TOTAL_CARGA_GLOBAL
        self.url_carga_liquida = constants.GET_NEWAVE_SISTEMA_CARGAS_TOTAL_CARGA_LIQUIDA
        self.url_mmgd_total     = constants.GET_NEWAVE_SISTEMA_MMGD_TOTAL
        self.url_ande          = constants.GET_NEWAVE_CADIC_TOTAL_ANDE
        
    
    def run_process(self):
        tabela_html = self.gerar_tabela_diferenca_cargas()
        self.enviar_tabela_whatsapp_email(tabela_html)    
    
     
    def gerar_tabela_diferenca_cargas(
        self,
    ) -> Dict[str, Any]:
        """
        Gera uma tabela de diferença de cargas.
        
        :return: Dicionário com o status e mensagem da geração da tabela.
        """
        try:
            logger.info("Gerando tabela de diferença de cargas...")
            
            data_produto_str = self.dataProduto
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            dados = {
                'dados_unsi':self._get_data(self.url_unsi),
                'dados_ande': self._get_data(self.url_ande),
                'dados_mmgd_total': self._get_data(self.url_mmgd_total),
                'dados_carga_global': self._get_data(self.url_carga_global),
                'dados_carga_liquida': self._get_data(self.url_carga_liquida)
            }
        
            html_tabela_diferenca = html_builder.gerar_html(
                'diferenca_cargas',
                dados
            )

            api_html_payload = {
                "html": html_tabela_diferenca,
                "options": {
                  "type": "png",
                  "quality": 100,
                  "trim": True,
                  "deviceScaleFactor": 2
                }
            }
            
            html_api_endpoint = self.url_html_to_image
            
            request_html_api = requests.post(
                html_api_endpoint,
                headers=self.headers,
                json=api_html_payload,  
            )
            
            if request_html_api.status_code != 200:
                raise ValueError(f"Erro ao converter HTML em imagem: {request_html_api.text}")
            
            image_dir = "/projetos/arquivos/tmp/DeckNEWAVETabelas"
            os.makedirs(image_dir, exist_ok=True)
            
            image_filename = f"tabela_diferenca_cargas_{versao}_{data_produto_str.replace("/","_")}.png"
            
            image_path = os.path.join(image_dir, image_filename)
            
            with open(image_path, 'wb') as f:
                f.write(request_html_api.content)
            
            logger.info(f"Imagem salva em: {image_path}")
            
            return image_path
                
        
        except Exception as e:
            logger.error(f"Erro ao gerar tabela de diferença de cargas: {e}")
            raise
    
    def enviar_tabela_whatsapp_email(
        self,
        img_path
    ) -> Dict[str, Any]:
        """
        Envia a tabela de diferença de cargas por WhatsApp e email.
        
        :return: Dicionário com o status e mensagem do envio.
        """
        try:
            logger.info("Enviando tabela de diferença de cargas por WhatsApp e email...")
            
            data_produto_str = self.dataProduto
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            img_path = img_path
            if not img_path or not os.path.exists(img_path):
                raise ValueError(f"Arquivo de imagem não encontrado: {img_path}")
            
            msg_whatsapp = ''
            if versao == 'preliminar':
                msg_whatsapp = f"Diferença de Cargas NEWAVE {versao} (Preliminar Atual x Definitivo anterior) - {data_produto_str}"
            else:
                msg_whatsapp = f"Diferença de Cargas NEWAVE {versao} (Definitivo Atual x Preliminar anterior) - {data_produto_str}"
                
            request_whatsapp = send_whatsapp_message(
                destinatario="Debug",
                mensagem=msg_whatsapp,
                arquivo=img_path,
            )
            
            if request_whatsapp.status_code < 200 or request_whatsapp.status_code >= 300:
                raise ValueError(f"Erro ao enviar mensagem por WhatsApp: {request_whatsapp.text}")
                    
        except Exception as e:
            logger.error(f"Erro ao enviar tabela por WhatsApp e email: {e}")
            raise

    def _get_data(self, url: str) -> pd.DataFrame:
        res = requests.get(url, headers=self.headers)
        if res.status_code != 200:
            res.raise_for_status()
        return res.json()
        
    def _get_version_by_filename(self, filename: str) -> str:
        try:
            if 'preliminar' in filename.lower():
                return 'preliminar'
            elif 'definitivo' in filename.lower():
                return 'definitivo'  
            else:
                raise ValueError("Nome do arquivo não contém 'preliminar' ou 'definitivo'.")
                
        except Exception as e:
            logger.error(f"Erro ao determinar a versão pelo nome do arquivo: {e}")
            raise

    
    
if __name__ == "__main__":
   
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
   
   payload = WebhookSintegreSchema(**payload)
   
   decknewave = DecksNewave(payload)
   
   decknewave.run_workflow()