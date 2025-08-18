import sys
import os
import requests
import pandas as pd
import glob
import openpyxl
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.schema import WebhookSintegreSchema  # noqa: E402
from middle.utils import setup_logger, Constants, get_auth_header, sanitize_string  # noqa: E402
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from middle.utils.file_manipulation import extract_zip
from middle.s3 import (handle_webhook_file, get_latest_webhook_product,)

logger = setup_logger()
constants = Constants()

class Rdh(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
        self.today = datetime.today()

        logger.info("Initialized RDH with payload: %s", payload)
    
    def run_workflow(self):
        logger.info("Starting workflow for RDH")
        try:
            os.makedirs(constants.PATH_ARQUIVOS_TEMP, exist_ok=True)            
            payload = get_latest_webhook_product(constants.WEBHOOK_CARGA_DECOMP)[0]
            
            base_path = handle_webhook_file(payload, constants.PATH_ARQUIVOS_TEMP)
            logger.info("Webhook file handled, base path: %s", base_path)
            
            self.run_process(base_path)
            logger.info("Workflow completed successfully")
            
        except Exception as e:
            logger.error("Workflow failed: %s", str(e), exc_info=True)
            raise
        
        
    def run_process(self, base_path):
        
        df_load = self.read_week_load(base_path)
        logger.info("Successfully processed load data with %d rows", len(df_load))
        self.post_load_to_database(df_load)
             

    def find_header_row(self, file_path: Path, sheet_name: str, 
                       header_keyword: str = 'APROVEITAMENTO') -> int:
        """Encontra a linha onde o cabeçalho começa, procurando por uma palavra-chave."""
        workbook = openpyxl.load_workbook(file_path, data_only=True)
        sheet = workbook[sheet_name]
        
        for row_idx, row in enumerate(sheet.iter_rows(values_only=True), start=0):
            row_values = [str(cell).strip() if cell is not None else '' for cell in row]
            if any(header_keyword in val for val in row_values):
                return row_idx
        raise ValueError(f"Header with '{header_keyword}' not found in sheet '{sheet_name}'")

    def simplify_multiindex_columns(self, columns):
        """Simplifica os nomes do MultiIndex e mapeia para nomes padronizados."""
        column_aliases = {
            'APROVEITAMENTO': ['APROVEITAMENTO', 'USINA'],
            'POSTO': ['POSTO', 'CODIGO'],
            'RES.': ['RES.', 'NIVEL', 'NÍVEL'],
            'ARM.': ['ARM.', 'VOLUME', 'VOLUME (%VU)']
        }
        
        simplified_columns = {}
        for col in columns:
            simplified_name = None
            for level in col:
                if level and not level.startswith('Unnamed'):
                    simplified_name = level
                    break
            if simplified_name == 'VALORES DO DIA':
                simplified_name = col[-1]
            
            for standard_name, aliases in column_aliases.items():
                if simplified_name in aliases:
                    simplified_columns[col] = standard_name
                    break
            else:
                simplified_columns[col] = simplified_name
        
        return simplified_columns

    def read_hydro_data(self, file_path: Path) -> pd.DataFrame:
        """Lê dados hidráulicos de um arquivo Excel."""
        sheet_name: str = 'Hidráulico-Hidrológica'
        try:
            header_row = self.find_header_row(file_path, sheet_name)
            header_rows = [header_row, header_row + 1, header_row + 2]
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_rows)
            
            column_mapping = self.simplify_multiindex_columns(df.columns)
            df.columns = [column_mapping[col] for col in df.columns]
            
            columns_to_read = ['APROVEITAMENTO', 'POSTO', 'RES.', 'ARM.', 'TUR.', 'VER.',  'DFL.',  'AFL.', 'INC.', 'Usos', 'EVP.']            
            missing_columns = [col for col in columns_to_read if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing columns: {missing_columns}")
            
            filtered_df = df[columns_to_read]
            filtered_df.columns = ['APROVEITAMENTO', 'APROVEITAMENTO2','POSTO', 'RES.', 'ARM.', 'TUR.', 'VER.',  'DFL.',  'AFL.', 'INC.', 'Usos', 'EVP.']
            filtered_df = filtered_df.drop(filtered_df.columns[1], axis=1)
            # Tenta converter 'col1' para float e verifica se são inteiros
            df['col1_numeric'] = pd.to_numeric(filtered_df['POSTO'], errors='coerce')
            mask = df['col1_numeric'].notna() & (df['col1_numeric'] % 1 == 0)

            # Mantém apenas as linhas válidas e remove a coluna temporária
            filtered_df = filtered_df[mask].drop(columns='POSTO')


            print(f"Read {len(filtered_df)} rows from {file_path}")
            return filtered_df
            
        except FileNotFoundError:
            print(f"Error: File {file_path} not found.")
            raise
        except ValueError as ve:
            print(f"Error: {ve}")
            raise
        except Exception as e:
            print(f"Error processing file: {e}")
            raise
        
    def post_rdh_to_database(self, data_in: pd.DataFrame) -> dict:
        logger.info("Posting load data to database, rows: %d", len(data_in))
        try:
            res = requests.post(
                constants.BASE_URL + '/api/v2/decks/carga-decomp',
                json=data_in.to_dict('records'),
                headers=get_auth_header()
            )
            if res.status_code != 200:
                logger.error("Failed to post data to database: status %d, response: %s",
                           res.status_code, res.text)
                res.raise_for_status()
            
            logger.info("Successfully posted data to database, response status: %d", res.status_code)
            return pd.DataFrame(res.json())
        
        except Exception as e:
            logger.error("Failed to post data to database: %s", str(e), exc_info=True)
            raise


if __name__ == '__main__':
    logger.info("Starting CargaPatamarDecomp script execution")
    try:
        rdh = Rdh({})
        rdh.run_process(Path("C:/Users/cs341053/Downloads/RDH_05AGO2025.xlsx"))
        logger.info("Script execution completed successfully")
    except Exception as e:
        logger.error("Script execution failed: %s", str(e), exc_info=True)
        raise