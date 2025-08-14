import sys
import pdb
import requests
import datetime
import pdfplumber
import pandas as pd
from typing import Optional
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from app.schema import WebhookSintegreSchema  # noqa: E402
from middle.utils import setup_logger, Constants, get_auth_header  # noqa: E402
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402

logger = setup_logger()
constants = Constants()

MONTHS = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

class RelatorioLimitesIntercambioDecomp(WebhookProductsInterface):
    
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)
    
    def run_workflow(self):
        path_produto = self.download_files()
        data_produto = datetime.datetime.strptime(self.payload.dataProduto, '%m/%Y').date()
        self.read_table(path_produto, data_produto)


    def get_months_from_path(self, pdf_path: str):
        month_year_part = pdf_path.split("PMO_")[1].split(".pdf")[0]
        if "_" in month_year_part:
            month_year_part = month_year_part.split("_")[0]
        month_name, year = month_year_part.split("-")
        month_num = {v.lower(): k for k, v in MONTHS.items()}[month_name.lower()]
        next_month_num = (month_num % 12) + 1
        next_year = int(year) if next_month_num != 1 else int(year) + 1
        first_month_year = f"{year}-{str(month_num).zfill(2)}-1"
        second_month_year = f"{str(next_year)}-{str(next_month_num).zfill(2)}-1"
        return first_month_year, second_month_year


    def find_table_page(self, pdf_path: str, table_name: str):
        with pdfplumber.open(pdf_path) as pdf:
            for page_number, page in enumerate(pdf.pages):
                text = page.extract_text()
                if table_name in text:
                    return page_number + 1
        return None


    def extract_table_from_pdf(self, pdf_path: str, table_page: int):
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[table_page - 1]
            tables = page.extract_tables()
            if tables:
                df = pd.DataFrame(tables[0])
                df.columns = df.iloc[0]
                df = df[1:]
                df.reset_index(drop=True, inplace=True)
                return df
            else:
                logger.warning("Nenhuma tabela encontrada na página especificada.")
                return None


    def reformat_df_database(self, df:pd.DataFrame, dict_num, data_produto: datetime.date):
        first_month_year, second_month_year = data_produto, (data_produto + datetime.timedelta(days=31)).replace(day=1)

        reformatted_data = []    
        data = df.iloc[2:, :].copy()
        col_indices = {
            f"{first_month_year} Pesada": 2,
            f"{first_month_year} Média": 5,
            f"{first_month_year} Leve": 8,
            f"{second_month_year} Pesada": 11,
            f"{second_month_year} Média": 14,
            f"{second_month_year} Leve": 17
        }
        # Iterar sobre as linhas
        # data[[x for x in data.columns if x is not None]]
        # teste = [x for i, x in data.iterrows()]
        for index, row in data.iterrows():
            limite = row.iloc[1].strip() if pd.notna(row.iloc[1]) else None
            re_value = dict_num.get(limite)
            
            if re_value is not None:
                # Processar os valores para o primeiro mês
                patamares_indices = [("Pesada", col_indices[f"{first_month_year} Pesada"]),
                                    ("Media", col_indices[f"{first_month_year} Média"]),
                                    ("Leve", col_indices[f"{first_month_year} Leve"]),
                                    
                                    ("Pesada", col_indices[f"{second_month_year} Pesada"]),
                                    ("Media", col_indices[f"{second_month_year} Média"]),
                                    ("Leve", col_indices[f"{second_month_year} Leve"])]
                for i, patamares_indice in enumerate(patamares_indices):
                    # if limite == 'IPU50':
                        # pdb.set_trace()
                    patamar, col_idx = patamares_indice
                    value = row.iloc[col_idx].strip() if pd.notna(row.iloc[col_idx]) else None
                    i_aux = i - 1
                    while not value and i_aux >= 0 and i_aux < len(patamares_indices) - 1:
                        _, col_idx = patamares_indices[i_aux]
                        value = row.iloc[col_idx].strip() if pd.notna(row.iloc[col_idx]) else None
                        i_aux -= 1
                    if value and value.strip():
                        pdb.set_trace()
                        reformatted_data.append({
                            "RE": re_value,
                            "Limite": limite,
                            "Data": first_month_year,
                            "Patamar": patamar,
                            "Valor": float(value) * 1000
                        })
                
        
        reformatted_df = pd.DataFrame(reformatted_data)    
        reformatted_df.dropna(subset=['RE', 'Valor'], inplace=True)    
        reformatted_df['RE'] = reformatted_df['RE'].astype(int)    
        reformatted_df['data_produto'] = data_produto.strftime('%Y-%m-%d')
        pdb.set_trace()
        
        return reformatted_df


    def read_table(
        self,
        pdf_path: str,
        data_produto: datetime.date,
        table_name: str = "Tabela 4-1: Resultados dos Limites Elétricos",
    ):
        dict_num = {
            'IPU60': 462, 'IPU50': 461, 'Ger. MAD': 401, 'RNE': 403, 'FNS': 405, 'FNESE': 409,
            'FNNE': 413, 'FNEN': 415, 'EXPNE': 417, 'SE/CO→FIC': 419, 'EXPN': 427, 'FNS+FNESE': 429,
            'FSENE': 431, 'FSUL': 437, 'RSUL': 439, 'RSE': 441, '-RSE': 443, 'FETXG+FTRXG': 445,
            'FXGET+FXGTR': 447
        }
        table_page = self.find_table_page(pdf_path, table_name)
        if table_page is None:
            logger.warning("Tabela não encontrada no PDF.")
            return None
        else:
            df = self.extract_table_from_pdf(pdf_path, table_page)
            if df is not None:
                return self.reformat_df_database(df, dict_num, data_produto)
            return None


    def sanitaze_dataframe(self, df: pd.DataFrame):
        df.rename(columns=lambda x: x.lower(), inplace=True)
        df.rename(columns={'data': 'mes_ano'}, inplace=True)
        df['patamar'] = df['patamar'].str.lower()
        df['mes_ano'] = df['mes_ano'].astype(str)
        df['data_produto'] = df['data_produto'].astype(str)
        
        return df


    def post_data(self, df: pd.DataFrame):
        res = requests.post(
            f"{constants.BASE_URL}/api/v2/decks/restricoes-eletricas",
            headers=get_auth_header(),
            json=df.to_dict(orient='records'),
        )
        if res.status_code == 200:
            logger.info("Dados enviados com sucesso.")
        else:
            logger.error(f"Erro ao enviar dados: {res.status_code} - {res.text}")
            res.raise_for_status()
        pass


if __name__ == "__main__":
    teste = RelatorioLimitesIntercambioDecomp(WebhookSintegreSchema.construct(**{
  "nome": "relatorio_mensal_de_limites_de_intercambio_para_o_modelo_decomp",
  "processo": "Programação mensal da operação energética",
  "dataProduto": "08/2025",
  "macroProcesso": "Programação da Operação",
  "periodicidade": "2025-08-01T00:00:00",
  "periodicidadeFinal": "2025-08-31T23:59:59",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS81Mi9Qcm9kdXRvcy8zMDIvUlQtT05TIERQTCAwMjk4LTIwMjVfTGltaXRlcyBQTU9fQWdvc3RvLTIwMjUucGRmIiwidXNlcm5hbWUiOiJnaWxzZXUubXVobGVuQHJhaXplbi5jb20iLCJub21lUHJvZHV0byI6IlJlbGF0w7NyaW8gTWVuc2FsIGRlIExpbWl0ZXMgZGUgSW50ZXJjw6JtYmlvIHBhcmEgbyBNb2RlbG8gREVDT01QIiwiSXNGaWxlIjoiVHJ1ZSIsImlzcyI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiYXVkIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJleHAiOjE3NTM0NTI3MjEsIm5iZiI6MTc1MzM2NjA4MX0._Ni1aOw2HCpY1KvDmmOpkcitc6XssQ8yt4xDIFE46c4",
  "s3Key": "webhooks/Relatório Mensal de Limites de Intercâmbio para o Modelo DECOMP/68823e41d49e380e81e2ab3c_RT-ONS DPL 0298-2025_Limites PMO_Agosto-2025.pdf",
  "filename": "RT-ONS DPL 0298-2025_Limites PMO_Agosto-2025.pdf",
  "webhookId": "68823e41d49e380e81e2ab3c"
}
))
    teste.run_workflow()