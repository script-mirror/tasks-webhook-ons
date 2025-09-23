import os
import sys

from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema

from middle.utils import setup_logger, get_auth_header, HtmlBuilder, Constants, extract_zip
from middle.airflow import trigger_dag
from middle.message import send_whatsapp_message
from app.tasks.previsoes_carga_mensal_patamar_newave import GerarTabelaDiferenca
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
        self.gerar_tabela = GerarTabelaDiferenca()
        self.headers = get_auth_header()
        self.constants = Constants()

    def run_workflow(self) -> Dict[str, Any]:
            
        file_path = self.download_files()
        
        self.run_process(file_path)
   
    def run_process(self, file_path):
        
    
        dict_result = self.process_file(file_path)
        dict_url = {
            'df_c_adic': self.constants.POST_NEWAVE_CADIC,
            'df_sistema': self.constants.POST_NEWAVE_SISTEMA,
            'df_patamar_usina': self.constants.POST_NEWAVE_PATAMAR_CARGA_USINAS,
            'df_patamar_intercambio': self.constants.POST_NEWAVE_PATAMAR_INTERCAMBIO
        }
        
        for chave, valor in dict_result.items():
            
            self.post_data(valor,dict_url[chave])
               
        self.trigger_dag(dag_id="1.17-NEWAVE_ONS-TO-CCEE", conf={})
        
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
            
        dat_files = self.process_functions.extrair_arquivos_dat(file_path)
        
        if 'preliminar' in self.filename.lower():
            self.update_wind(dat_files['sistema'])
        
        df_c_adic = self.process_functions.processar_deck_nw_cadic(dat_files['cadic'])
        
        df_sistema = self.process_functions.processar_deck_nw_sist(dat_files['sistema'])
        
        dict_patamar = self.process_functions.processar_deck_nw_patamar(dat_files['patamar'])
        
        dict_result = {
            'df_c_adic': df_c_adic,
            'df_sistema': df_sistema,
            'df_patamar_usina': dict_patamar['df_patamar_carga_usinas'],
            'df_patamar_intercambio': dict_patamar['df_patamar_intercambio'],
        }
        
        return dict_result 
        
    def post_data(self, data_in: pd.DataFrame, url: str) -> dict:
        try:
            res = requests.post(
                url,
                json=data_in.to_dict('records'),
                headers=self.headers
            )
            if res.status_code != 200:
                res.raise_for_status()
                    
        except Exception as e:
            raise
                         
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
        
        try:
            
            data_produto_str = self.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
            filename = self.filename
            versao = self._get_version_by_filename(filename)
            
            cadic_object = Cadic.read(cadic_file)
            df_c_adic = cadic_object.cargas.copy()

            if df_c_adic is None:
                error_msg = "Dados do c_adic não encontrados no arquivo!"
                raise ValueError(error_msg)
            else:
                logger.info(f"Dados do c_adic encontrados: {df_c_adic} ")
                logger.info(f"Mercado de energia total carregado com sucesso. Total de registros: {len(df_c_adic)}")

                df_c_adic['data'] = pd.to_datetime(df_c_adic['data'], errors='coerce')
                df_c_adic = df_c_adic[df_c_adic['data'].dt.year < 9999]
                df_c_adic['vl_ano'] = df_c_adic['data'].dt.year.astype(int)
                df_c_adic['vl_mes'] = df_c_adic['data'].dt.month.astype(int)
                df_c_adic = df_c_adic.dropna(subset=['valor'])
                
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

                df_c_adic = df_c_adic.pivot_table(
                    index=['vl_ano', 'vl_mes'], 
                    columns='coluna',
                    values='valor',
                    aggfunc='first'  
                ).reset_index()
                
                df_c_adic['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
                
                df_c_adic['versao'] = versao
                
                
            return df_c_adic
                
                    
        
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
            
            
            return nw_sistema_df
                    
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

            return {
                "df_patamar_carga_usinas": patamar_carga_usinas_df,
                "df_patamar_intercambio": patamar_intercambio_df,
            }
        
        except Exception as e:
            logger.error(f"Erro ao processar PATAMAR.DAT: {e}")
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