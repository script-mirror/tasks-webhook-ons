import pdb
import pdfplumber
import pandas as pd
import requests
import datetime
from middle.utils import setup_logger, Constants
from app.webhook_products_interface import WebhookProductsInterface

logger = setup_logger()
constants = Constants()

# Dicionário para mapear meses em português para números
MONTHS = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

class RelatorioLimitesIntercambioDecomp(WebhookProductsInterface):
    
    def run_workflow(self, payload_webhook: dict):
        path_produto = self.download_files(payload_webhook)
        data_produto = datetime.datetime.strptime(payload_webhook['dataProduto'], '%m/%Y').date()
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


    def reformat_df_database(self, df:pd.DataFrame, dict_num, first_month_year, second_month_year):
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
        for index, row in data.iterrows():
            limite = row.iloc[1].strip() if pd.notna(row.iloc[1]) else None
            re_value = dict_num.get(limite)
            
            if re_value is not None:
                # Processar os valores para o primeiro mês
                for patamar, col_idx in [("Pesada", col_indices[f"{first_month_year} Pesada"]),
                                    ("Media", col_indices[f"{first_month_year} Média"]),
                                    ("Leve", col_indices[f"{first_month_year} Leve"])]:
                    value = row.iloc[col_idx].strip() if pd.notna(row.iloc[col_idx]) else None
                    if value and value.strip():
                        reformatted_data.append({
                            "RE": re_value,
                            "Limite": limite,
                            "Data": first_month_year,
                            "Patamar": patamar,
                            "Valor": float(value) * 1000
                        })
                
                # Processar os valores para o segundo mês
                for patamar, col_idx in [("Pesada", col_indices[f"{second_month_year} Pesada"]),
                                    ("Media", col_indices[f"{second_month_year} Média"]),
                                    ("Leve", col_indices[f"{second_month_year} Leve"])]:
                    value = row.iloc[col_idx].strip() if pd.notna(row.iloc[col_idx]) else None
                    if value and value.strip():
                        reformatted_data.append({
                            "RE": re_value,
                            "Limite": limite,
                            "Data": second_month_year,
                            "Patamar": patamar,
                            "Valor": float(value) * 1000
                        })
        
        reformatted_df = pd.DataFrame(reformatted_data)    
        reformatted_df.dropna(subset=['RE', 'Valor'], inplace=True)    
        reformatted_df['RE'] = reformatted_df['RE'].astype(int)    
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
                first_month_year, second_month_year = data_produto, data_produto.replace(month=data_produto.month+1)
                return self.reformat_df_database(df, dict_num, first_month_year, second_month_year)
            return None

    def sanitaze_dataframe(self, df: pd.DataFrame):
        df.rename(columns=lambda x: x.lower(), inplace=True)
        df.rename(columns={'data': 'mes_ano'}, inplace=True)
        df['patamar'] = df['patamar'].str.lower()
        return df

    def post_data(self, df: pd.DataFrame):
        res = requests.post(
            f"{constants.BASE_URL}/api/v2/decks/restricoes-eletricas"
        )
        if res.status_code == 200:
            logger.info("Dados enviados com sucesso.")
        else:
            logger.error(f"Erro ao enviar dados: {res.status_code} - {res.text}")
            res.raise_for_status()
        pass
if __name__ == "__main__":
    preliminar_path = "/home/arthur-moraes/WX2TB/Documentos/fontes/PMO/trading-middle-tasks-webhook-ons/RT-ONS DPL 0298-2025_Limites PMO_Agosto-2025.pdf"
    webhook_prod = RelatorioLimitesIntercambioDecomp()
    preliminar = webhook_prod.run_workflow(preliminar_path, datetime.date(2025, 8, 1))
    pdb.set_trace()