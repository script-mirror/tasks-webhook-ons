import os
import sys
import pandas as pd
import datetime
from typing import Optional, Dict, Any
import requests
import pdb

from middle.utils import setup_logger, get_auth_header, HtmlBuilder, Constants
from middle.message import send_whatsapp_message
constants = Constants()
logger = setup_logger()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from app.webhook_products_interface import WebhookProductsInterface
from app.schema import WebhookSintegreSchema



class PrevisoesCargaMensalPatamarNewave(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema] = None) -> None:
        """
        Inicializa a classe com o payload iguração fornecida.
        
        Args:
            conf: Dicionário com a configuração do produto.
        """
        super().__init__(payload)
        
    def run_workflow(
        self, 
        filepath:Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Executa o fluxo completo de processamento de forma sequencial.
        Cada etapa depende do resultado da etapa anterior.
        
        """
        try:
            if filepath:
                download_extract_filepath = filepath
            elif payload:
                download_extract_filepath = self.download_extract_files()
            else:
                raise ValueError("É necessário fornecer um filepath ou um payload válido.")
            
            processar_carga_mensal_para_atualizacao_result = self.processar_carga_mensal_para_atualizacao(payload, download_extract_filepath)
            
            processar_carga_mensal_para_api_result = self.processar_carga_mensal_para_api(payload, download_extract_filepath)
            
            self.atualizar_carga_mensal(payload, processar_carga_mensal_para_atualizacao_result)
            
            gerar_tabela_diferenca_cargas_result = self.gerar_tabela_diferenca_cargas(payload)
            
            self.enviar_tabela_whatsapp_email(payload, gerar_tabela_diferenca_cargas_result)
            
        except Exception as e:
            error_msg = f"Erro no fluxo de processamento do DECK Newave: {str(e)}"
            logger.error(error_msg)


    def _xlsx_to_df(filepath) -> pd.DataFrame:
        xlsx_files = [f for f in os.listdir(filepath) if f.endswith('.xlsx')]
        filename = xlsx_files[0]
            
        xlsx_file = os.path.join(filepath, filename)
        
        return pd.read_excel(xlsx_file)


    def processar_carga_mensal_para_atualizacao(
        self,
        payload: WebhookSintegreSchema,
        download_extract_filepath: Dict[str, Any]
    ) -> list:
        
        try:
            filepath = download_extract_filepath
            
            df_carga = self._xlsx_to_df(filepath)
            
            df_carga.drop(columns=['WEEK','GAUGE','LOAD_cMMGD', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE','Exp_MMGD','REVISION'], inplace=True, errors='ignore')
            df_carga = df_carga[df_carga['TYPE'] == 'MEDIUM']
            df_carga.drop(columns=['TYPE'], inplace=True, errors='ignore')
            
            df_carga['vl_ano'] = df_carga['DATE'].dt.year
            df_carga['vl_mes'] = df_carga['DATE'].dt.month
            df_carga.drop(columns=['DATE'], inplace=True, errors='ignore')
            
            df_atualizacao_sist = df_carga[['vl_ano', 'vl_mes','SOURCE', 'LOAD_sMMGD', 'Exp_CGH', 'Exp_EOL', 'Exp_UFV', 'Exp_UTE']]
            df_atualizacao_cadic = df_carga[['vl_ano', 'vl_mes','SOURCE', 'Base_MMGD']]
            
            df_atualizacao_sist.rename(columns={
                'SOURCE': 'cd_submercado',
                'LOAD_sMMGD': 'vl_energia_total',
                'Exp_CGH': 'vl_geracao_pch_mmgd',
                'Exp_EOL': 'vl_geracao_eol_mmgd',
                'Exp_UFV': 'vl_geracao_ufv_mmgd',
                'Exp_UTE': 'vl_geracao_pct_mmgd'
            }, inplace=True)
            
            cd_submercados = {
                'SUDESTE': '1',
                'SUL': '2',
                'NORDESTE': '3',
                'NORTE': '4'
            }
            
            df_atualizacao_sist['cd_submercado'] = df_atualizacao_sist['cd_submercado'].map(cd_submercados)
            df_atualizacao_sist['cd_submercado'] = df_atualizacao_sist['cd_submercado'].astype(int)
            
            df_atualizacao_sist['vl_energia_total'] = df_atualizacao_sist['vl_energia_total'].astype(int)
            
            for col in ['vl_geracao_pch_mmgd', 'vl_geracao_eol_mmgd', 'vl_geracao_ufv_mmgd', 'vl_geracao_pct_mmgd']:
                if col in df_atualizacao_sist.columns:
                    df_atualizacao_sist[col] = df_atualizacao_sist[col].astype(float).round(2)
            
            df_atualizacao_cadic.rename(columns={
                'SOURCE': 'cd_submercado',
                'Base_MMGD': 'vl_mmgd'
            }, inplace=True)
            
            cd_submercados_cadic = {
                'SUDESTE': 'se',
                'SUL': 's',
                'NORDESTE': 'ne',
                'NORTE': 'n'
            }
            
            df_atualizacao_cadic.loc[:, 'cd_submercado'] = df_atualizacao_cadic['cd_submercado'].map(cd_submercados_cadic)
            
            df_atualizacao_cadic = df_atualizacao_cadic.pivot_table(
                index=['vl_ano', 'vl_mes'],
                columns='cd_submercado',
                values='vl_mmgd',
                aggfunc='first'
            ).reset_index()
            
            df_atualizacao_cadic.columns.name = None
            
            df_atualizacao_cadic = df_atualizacao_cadic.rename(columns={
                'se': 'vl_mmgd_se',
                's': 'vl_mmgd_s',
                'ne': 'vl_mmgd_ne',
                'n': 'vl_mmgd_n'
            })
            
            for col in ['vl_mmgd_se', 'vl_mmgd_s', 'vl_mmgd_ne', 'vl_mmgd_n']:
                if col in df_atualizacao_cadic.columns:
                    df_atualizacao_cadic[col] = df_atualizacao_cadic[col].astype(float).round(0)
                    df_atualizacao_cadic[col] = df_atualizacao_cadic[col].fillna(0).astype(int)
                    
            df_atualizacao_cadic['versao'] = 'preliminar'
            df_atualizacao_sist['versao'] = 'preliminar'
            
            data_produto_str = payload.dataProduto
            data_produto_datetime = datetime.datetime.strptime(data_produto_str, '%m/%Y')
                
            df_atualizacao_cadic['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            df_atualizacao_sist['dt_deck'] = data_produto_datetime.strftime('%Y-%m-%d')
            
            df_atualizacao_cadic = df_atualizacao_cadic[['vl_ano', 'vl_mes', 'vl_mmgd_se', 'vl_mmgd_s', 'vl_mmgd_ne', 'vl_mmgd_n', 'versao', 'dt_deck']]
           
            return [df_atualizacao_sist, df_atualizacao_cadic]
            
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo XLSX: {e}")
            raise
  
  
    def processar_carga_mensal_para_api(
        self,
        payload: WebhookSintegreSchema,
        download_extract_filepath: Dict[str, Any]
    ) -> list:
        
        filepath = download_extract_filepath
            
        df_carga = self._xlsx_to_df(filepath)
        
        
        pass
            
  
    def atualizar_carga_mensal(
        self,
        payload: WebhookSintegreSchema,
        processar_carga_mensal_result: list
    ) -> None:
        
        try:
            df_atualizacao_sist, df_atualizacao_cadic = processar_carga_mensal_result
            
            logger.info("Atualizando SISTEMA e CADIC com os dados processados de Carga Mensal.")
            
            auth_headers = get_auth_header()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
            
            # base_url = constants.BASE_URL
            base_url = "http://localhost:8000"
            api_url = f"{base_url}/api/v2"
            
            url_sistema_mmgd_total = f"{api_url}/decks/newave/sistema/mmgd_total"
            url_cadic_mmgd_base = f"{api_url}/decks/newave/cadic/total_mmgd_base"
            
            url_last_sistema = f"{api_url}/decks/newave/sistema/last_deck"
            url_last_cadic = f"{api_url}/decks/newave/cadic/last_deck"
            url_post_cadic_quad = f"{api_url}/decks/newave/cadic/"
            url_post_sistema_quad = f"{api_url}/decks/newave/sistema/"
            
            headers = get_auth_header()
            headers['Content-Type'] = 'application/json'
            
            
            filename = payload.filename
            
            if 'quad' in filename.lower():
                
                response_last_cadic = requests.get(
                    url_last_cadic,
                    headers=headers
                )
                response_last_sistema = requests.get(
                    url_last_sistema,
                    headers=headers
                )
                
                df_last_cadic = pd.DataFrame(response_last_cadic.json())
                df_last_sist = pd.DataFrame(response_last_sistema.json())
                
                CADIC_KEYS_TO_KEEP = ['vl_ano','vl_mes','vl_const_itaipu','vl_ande','vl_boa_vista']
                SISTEMA_KEYS_TO_KEEP = ['vl_ano','vl_mes','cd_submercado', 'vl_geracao_pch', 'vl_geracao_eol', 'vl_geracao_ufv', 'vl_geracao_pct']
                
                df_last_cadic = df_last_cadic[CADIC_KEYS_TO_KEEP]
                df_last_sist = df_last_sist[SISTEMA_KEYS_TO_KEEP]
                
                cadic_atualizado = pd.merge(
                    df_last_cadic,
                    df_atualizacao_cadic,
                    on=["vl_ano", "vl_mes"],
                    how="left"  
                )
                
                sist_atualizado = pd.merge(
                    df_last_sist,
                    df_atualizacao_sist,
                    on=["vl_ano", "vl_mes","cd_submercado"],
                    how="left"  
                )
                
                cadic_atualizado['vl_boa_vista'] = 0
                
                cadic_atualizado = cadic_atualizado.to_dict(orient='records')
                sist_atualizado = sist_atualizado.to_dict(orient='records')
                
                response_cadic_mmgd_base = requests.post(
                    url_post_cadic_quad,
                    json=cadic_atualizado, 
                    headers=headers
                )
                
                response_sistema_mmgd_total = requests.post(
                    url_post_sistema_quad,
                    json=sist_atualizado,
                    headers=headers
                )

            else:
                
                sistema_payload = df_atualizacao_sist.to_dict(orient='records')
                cadic_payload = df_atualizacao_cadic.to_dict(orient='records')
                
                response_cadic_mmgd_base = requests.put(
                    url_cadic_mmgd_base,
                    json=cadic_payload, 
                    headers=headers
                )
                
                response_sistema_mmgd_total = requests.put(
                    url_sistema_mmgd_total, 
                    json=sistema_payload, 
                    headers=headers
                )
            
            if response_sistema_mmgd_total.status_code == 200 or response_sistema_mmgd_total.status_code == 201:
                logger.info("Carga mensal do sistema atualizada com sucesso.")
            else:
                logger.error(f"Erro ao atualizar carga mensal do sistema: {response_sistema_mmgd_total.text}")
                raise ValueError(f"Erro ao atualizar carga mensal do sistema: {response_sistema_mmgd_total.text}")
            
            if response_cadic_mmgd_base.status_code == 200 or response_cadic_mmgd_base.status_code == 201:
                logger.info("Carga mensal do CADIC atualizada com sucesso.")
            else:
                logger.error(f"Erro ao atualizar carga mensal do CADIC: {response_cadic_mmgd_base.text}")
                raise ValueError(f"Erro ao atualizar carga mensal do CADIC: {response_cadic_mmgd_base.text}")
        
        except Exception as e:
            logger.error(f"Erro ao atualizar carga mensal: {e}")
            raise

    def gerar_tabela_diferenca_cargas(
        self,
        payload: WebhookSintegreSchema,
    ) -> Dict[str, Any]:
        """
        Gera uma tabela de diferença de cargas.
        
        :return: Dicionário com o status e mensagem da geração da tabela.
        """
        try:
            logger.info("Gerando tabela de diferença de cargas...")
            
            data_produto_str = payload.dataProduto
            
            auth_headers = get_auth_header()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
            
            # base_url = constants.BASE_URL
            base_url = "http://localhost:8000"
            api_url = f"{base_url}/api/v2"
            image_api_url = f"https://tradingenergiarz.com/html-to-img"
            
            
            # Pegando valores do sistema de geração de usinas não simuladas (UNSI)
            sistema_unsi_url = f"{api_url}/decks/newave/sistema/total_unsi"
            sistema_unsi_response = requests.get(
                sistema_unsi_url,
                headers=headers
            )
            if sistema_unsi_response.status_code != 200:
                raise ValueError(f"Erro ao obter dados de geração UNSI: {sistema_unsi_response.text}")
            
            sistema_unsi_values = sistema_unsi_response.json() 
            
            # Pegando valores de carga de ANDE
            cadic_ande_url = f"{api_url}/decks/newave/cadic/total_ande"
            cadic_ande_response = requests.get(
                cadic_ande_url,
                headers=headers
            )
            if cadic_ande_response.status_code != 200:
                raise ValueError(f"Erro ao obter dados de carga do ANDE: {cadic_ande_response.text}")
            
            cadic_ande_values = cadic_ande_response.json() 
            
            # Pegando valores de MMGD Total 
            sistema_mmgd_total_url = f"{api_url}/decks/newave/sistema/mmgd_total"
            sistema_mmgd_total_response = requests.get(
                sistema_mmgd_total_url,
                headers=headers
            )
            if sistema_mmgd_total_response.status_code != 200:
                raise ValueError(f"Erro ao obter dados de MMGD Total: {sistema_mmgd_total_response.text}")
            
            sistema_mmgd_total_values = sistema_mmgd_total_response.json()
            
            # Pegando valores de geração de Carga Global
            carga_global_url = f"{api_url}/decks/newave/sistema/cargas/total_carga_global"
            carga_global_response = requests.get(
                carga_global_url,
                headers=headers
            )
            if carga_global_response.status_code != 200:
                raise ValueError(f"Erro ao obter dados de geração de carga global: {carga_global_response.text}")
            carga_global_values = carga_global_response.json()
            
            # Pegando valores de geração de Carga Líquida
            carga_liquida_url = f"{api_url}/decks/newave/sistema/cargas/total_carga_liquida"
            carga_liquida_response = requests.get(
                carga_liquida_url,
                headers=headers
            )
            if carga_liquida_response.status_code != 200:
                raise ValueError(f"Erro ao obter dados de geração de carga liquida: {carga_liquida_response.text}")
            carga_liquida_values = carga_liquida_response.json()
            
            dados = {
                'dados_unsi': sistema_unsi_values,
                'dados_ande': cadic_ande_values,
                'dados_mmgd_total': sistema_mmgd_total_values,
                'dados_carga_global': carga_global_values,
                'dados_carga_liquida': carga_liquida_values
            }
            
            html_builder = HtmlBuilder()
            
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
            
            html_api_endpoint = f"{image_api_url}/convert"
            
            request_html_api = requests.post(
                html_api_endpoint,
                headers=headers,
                json=api_html_payload,  
            )
            
            if request_html_api.status_code != 200:
                raise ValueError(f"Erro ao converter HTML em imagem: {request_html_api.text}")
            
            image_dir = "/tmp/deck_newave/images"
            os.makedirs(image_dir, exist_ok=True)
            
            image_filename = f"tabela_diferenca_cargas_preliminar_atualizado_{data_produto_str.replace("/","_")}.png"
            
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
        payload: WebhookSintegreSchema,
        gerar_tabela_diferenca_cargas_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Envia a tabela de diferença de cargas por WhatsApp e email.
        
        :return: Dicionário com o status e mensagem do envio.
        """
        try:
            logger.info("Enviando tabela de diferença de cargas por WhatsApp e email...")
            
            data_produto_str = payload.dataProduto
            versao = "preliminar"
            
            image_path = gerar_tabela_diferenca_cargas_result
            if not image_path or not os.path.exists(image_path):
                raise ValueError(f"Arquivo de imagem não encontrado: {image_path}")
            
            if 'quad' in payload.filename:
                msg_whatsapp = f"Diferença de Cargas NEWAVE {versao} (Quadrimestral x Definitivo anterior) - {data_produto_str}"
            else:
                msg_whatsapp = f"Diferença de Cargas NEWAVE {versao} (Preliminar atual atualizado x Definitivo anterior) - {data_produto_str}"
            
            request_whatsapp = send_whatsapp_message(
                destinatario="Debug",
                mensagem=msg_whatsapp,
                arquivo=image_path,
            )
            
            if request_whatsapp.status_code < 200 or request_whatsapp.status_code >= 300:
                raise ValueError(f"Erro ao enviar mensagem por WhatsApp: {request_whatsapp.text}")
                    
        except Exception as e:
            logger.error(f"Erro ao enviar tabela por WhatsApp e email: {e}")
            raise        
    
    def triggar_dag_externa():
        pass    
    
if __name__ == "__main__":
   
   payload = {
  "dataProduto": "09/2025", 
  "filename": "CargaMensal_2revquad2529.zip",
  "macroProcesso": "Programação da Operação",
  "nome": "Previsões de carga mensal e por patamar - NEWAVE",
  "periodicidade": "2025-09-01T00:00:00",
  "periodicidadeFinal": "2025-09-30T23:59:59",
  "processo": "Previsão de Carga para o PMO",
  "s3Key": "webhooks/Previsões de carga mensal e por patamar - NEWAVE/688d2cb494f9e32e8e798756_CargaMensal_2revquad2529.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS80Ny9Qcm9kdXRvcy8yMjkvQ2FyZ2FNZW5zYWxfMnJldnF1YWQyNTI5LnppcCIsInVzZXJuYW1lIjoiZ2lsc2V1Lm11aGxlbkByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJQcmV2aXPDtWVzIGRlIGNhcmdhIG1lbnNhbCBlIHBvciBwYXRhbWFyIC0gTkVXQVZFIiwiSXNGaWxlIjoiVHJ1ZSIsImlzcyI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiYXVkIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJleHAiOjE3NTQxNjkxMjMsIm5iZiI6MTc1NDA4MjQ4M30.kdmb2eKSpSmXep832Vrw6B7NAAdG_4On23P7cZlj3uM",
  "webhookId": "688d2cb494f9e32e8e798756"
}
   
   payload = WebhookSintegreSchema(**payload)
   
   previsoescargamensal = PrevisoesCargaMensalPatamarNewave(payload)
   
   previsoescargamensal.run_workflow()