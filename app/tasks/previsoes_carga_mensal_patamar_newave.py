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
            
            processar_carga_mensal_result = self.processar_carga_mensal(payload, download_extract_filepath)
            
            self.atualizar_carga_mensal(processar_carga_mensal_result)
            
            gerar_tabela_diferenca_cargas_result = self.gerar_tabela_diferenca_cargas(payload)
            
            self.enviar_tabela_whatsapp_email(payload, gerar_tabela_diferenca_cargas_result)
            
        except Exception as e:
            error_msg = f"Erro no fluxo de processamento do DECK Newave: {str(e)}"
            logger.error(error_msg)


    def processar_carga_mensal(
        self,
        payload: WebhookSintegreSchema,
        download_extract_filepath: Dict[str, Any]
    ) -> list:
        
        try:
            filepath = download_extract_filepath
            xlsx_files = [f for f in os.listdir(filepath) if f.endswith('.xlsx')]
            filename = xlsx_files[0]
            
            xlsx_file = os.path.join(filepath, filename)
            
            df_carga = pd.read_excel(xlsx_file)
            
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
            
            payload_filename = payload.filename
            
            if 'quad' in payload_filename.lower():
                df_atualizacao_cadic['vl_boa_vista'] = 0
                df_atualizacao_cadic = df_atualizacao_cadic[['vl_ano', 'vl_mes', 'vl_mmgd_se', 'vl_mmgd_s', 'vl_mmgd_ne', 'vl_mmgd_n', 'vl_boa_vista', 'versao', 'dt_deck']]
                
            return [df_atualizacao_sist, df_atualizacao_cadic]
            
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo XLSX: {e}")
            raise
  
    def atualizar_carga_mensal(
        self,
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
            
            base_url = constants.BASE_URL
            api_url = f"{base_url}/api/v2"
            
            url_sistema = f"{api_url}/decks/newave/sistema/mmgd_total"
            url_cadic = f"{api_url}/decks/newave/cadic/total_mmgd_base"
            
            headers = get_auth_header()
            headers['Content-Type'] = 'application/json'
            
            sistema_payload = df_atualizacao_sist.to_dict(orient='records')
            cadic_payload = df_atualizacao_cadic.to_dict(orient='records')
            
            response_sistema = requests.put(
                url_sistema, 
                json=sistema_payload, 
                headers=headers
            )
            
            response_cadic = requests.put(
                url_cadic,
                json=cadic_payload, 
                headers=headers
            )
            
            if response_sistema.status_code == 200:
                logger.info("Carga mensal do sistema atualizada com sucesso.")
            else:
                logger.error(f"Erro ao atualizar carga mensal do sistema: {response_sistema.text}")
                raise ValueError(f"Erro ao atualizar carga mensal do sistema: {response_sistema.text}")
            
            if response_cadic.status_code == 200:
                logger.info("Carga mensal do CADIC atualizada com sucesso.")
            else:
                logger.error(f"Erro ao atualizar carga mensal do CADIC: {response_cadic.text}")
                raise ValueError(f"Erro ao atualizar carga mensal do CADIC: {response_cadic.text}")
        
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
            
            base_url = constants.BASE_URL
            api_url = f"{base_url}/api/v2"
            image_api_url = f"{base_url}/html-to-img"
            
            
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
            
            html_tabela_diferenca = HtmlBuilder.gerar_html(
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
            
            image_filename = f"tabela_diferenca_cargas_preliminar_atualizado_{data_produto_str}.png"
            
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
            
            request_whatsapp = send_whatsapp_message(
                destinatario="Debug",
                mensagem=f"Diferença de Cargas NEWAVE {versao} Atualizado - {data_produto_str}",
                arquivo=image_path,
            )
            
            if request_whatsapp.status_code != 200:
                raise ValueError(f"Erro ao enviar mensagem por WhatsApp: {request_whatsapp.text}")
                    
        except Exception as e:
            logger.error(f"Erro ao enviar tabela por WhatsApp e email: {e}")
            raise        
        
    
if __name__ == "__main__":
   
   payload = {
  "dataProduto": "07/2025",
  "filename": "RV0_PMO_Julho_2025_carga_mensal.zip",
  "macroProcesso": "Programação da Operação",
  "nome": "Previsões de carga mensal e por patamar - NEWAVE",
  "periodicidade": "2025-07-01T00:00:00",
  "periodicidadeFinal": "2025-07-31T23:59:59",
  "processo": "Previsão de Carga para o PMO",
  "s3Key": "webhooks/Previsões de carga mensal e por patamar - NEWAVE/6859b987b1c148748afd1715_RV0_PMO_Julho_2025_carga_mensal.zip",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS80Ny9Qcm9kdXRvcy8yMjkvUlYwX1BNT19KdWxob18yMDI1X2NhcmdhX21lbnNhbC56aXAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUHJldmlzw7VlcyBkZSBjYXJnYSBtZW5zYWwgZSBwb3IgcGF0YW1hciAtIE5FV0FWRSIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzUwNzk3MzAzLCJuYmYiOjE3NTA3MTA2NjN9.gVtPlXEt8plvqdoYO0_SqVFybpbCvyuslLlexoBoO7Q",
  "webhookId": "6859b987b1c148748afd1715"
}
   
   payload = WebhookSintegreSchema(**payload)
   
   previsoescargamensal = PrevisoesCargaMensalPatamarNewave(payload)
   
   previsoescargamensal.run_workflow()