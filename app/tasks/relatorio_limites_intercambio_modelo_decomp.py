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
from middle.utils import ( # noqa: E402
    setup_logger,
    Constants,
    get_auth_header,
    extrair_mes_ano,
)
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
    
    def run_workflow(self, filepath: Optional[str] = None):
        if not filepath:
            filepath = self.download_extract_files()
    
        data_produto = self.get_data_produto(filepath)
        df = self.read_table(filepath, data_produto)
        df = self.sanitaze_dataframe(df)
        self.post_data(df)
        

    def get_data_produto(self, path_produto: str) -> datetime.date:
        with pdfplumber.open(path_produto) as pdf:
            data_produto = extrair_mes_ano(pdf.pages[0].extract_text())
        return data_produto



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

        for index, row in data.iterrows():
            limite = row.iloc[1].strip() if pd.notna(row.iloc[1]) else None
            re_value = dict_num.get(limite)
            
            if re_value is not None:
                patamares_indices = [("Pesada", col_indices[f"{first_month_year} Pesada"], first_month_year),
                                    ("Media", col_indices[f"{first_month_year} Média"], first_month_year),
                                    ("Leve", col_indices[f"{first_month_year} Leve"], first_month_year),
                                    
                                    ("Pesada", col_indices[f"{second_month_year} Pesada"], second_month_year),
                                    ("Media", col_indices[f"{second_month_year} Média"], second_month_year),
                                    ("Leve", col_indices[f"{second_month_year} Leve"], second_month_year)]
                for i, patamares_indice in enumerate(patamares_indices):
                    patamar, col_idx, mes_ano = patamares_indice
                    value = row.iloc[col_idx].strip() if pd.notna(row.iloc[col_idx]) else None
                    i_aux = i - 1
                    while not value and i_aux >= 0 and i_aux < len(patamares_indices) - 1:
                        _, col_idx, _ = patamares_indices[i_aux]
                        value = row.iloc[col_idx].strip() if pd.notna(row.iloc[col_idx]) else None
                        i_aux -= 1
                    if value and value.strip():
                        reformatted_data.append({
                            "RE": re_value,
                            "Limite": limite,
                            "Data": mes_ano,
                            "Patamar": patamar,
                            "Valor": float(value) * 1000
                        })
                
        
        reformatted_df = pd.DataFrame(reformatted_data)    
        reformatted_df.dropna(subset=['RE', 'Valor'], inplace=True)    
        reformatted_df['RE'] = reformatted_df['RE'].astype(int)    
        reformatted_df['data_produto'] = data_produto.strftime('%Y-%m-%d')
        
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
    teste = RelatorioLimitesIntercambioDecomp({})
    teste.run_workflow("/home/arthur-moraes/WX2TB/Documentos/fontes/PMO/trading-middle-tasks-webhook-ons/RT-ONS DPL 0298-2025_Limites PMO_Agosto-2025.pdf")