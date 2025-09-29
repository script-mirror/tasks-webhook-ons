import sys
import pdb
import io
import requests
import datetime 
import pdfplumber
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.table as tbl
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
    html_to_image,
    html_style,
)
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from middle.message import send_whatsapp_message
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
            filepath = self.download_files()
        tipo = "preliminar" if "preliminar" in filepath.lower() else "definitivo"
        data_produto = self.get_data_produto(filepath)
        df = self.run_process(filepath, data_produto)
        df = self.sanitaze_dataframe(df)
        df['tipo'] = tipo
        self.post_data(df)
        analyzer = GenerateTable()
        analyzer.run_workflow()
           
    def get_data_produto(self, path_produto: str) -> datetime.date:
        with pdfplumber.open(path_produto) as pdf:
            data_produto = extrair_mes_ano(pdf.pages[0].extract_text())
        return data_produto


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


    def run_process(
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
            constants.POST_RESTRICOES_ELETRICAS,
            headers=get_auth_header(),
            json=df.to_dict(orient='records'),
        )
        if res.status_code == 200:
            logger.info("Dados enviados com sucesso.")
        else:
            logger.error(f"Erro ao enviar dados: {res.status_code} - {res.text}")
            res.raise_for_status()
        pass


class GenerateTable:
    def __init__(self):
        logger.info("Inicializando IntercambioAnalyzer")
        self.consts = Constants()
        self.header = get_auth_header()

    def run_workflow(self):
        logger.info("Executando análise principal")
        try:
            self.calculate_differences()
            logger.info("Análise concluída com sucesso")
        except Exception as e:
            logger.error(f"Erro na execução da análise: {str(e)}")
            raise
      
    def get_data(self, url_in: str, params:dict={}) -> pd.DataFrame:
        try:
            res = requests.get(
                url = url_in,
                params=params,
                headers=self.header
            )
            if res.status_code != 200:
                logger.error(f"Erro na requisição à API: status {res.status_code}, response: {res.text}")
                res.raise_for_status()
            logger.debug("Dados da API obtidos com sucesso")
            return pd.DataFrame(res.json())
        except requests.RequestException as e:
            logger.error(f"Erro ao acessar a API: {str(e)}")
            raise

    def res_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Convertendo dados brutos em DataFrame pivotado")
        try:
            df['mes_ano_formatted'] = pd.to_datetime(df['mes_ano']).dt.strftime('%m/%y')
            df_pivot = df.pivot_table(
                index=['re', 'limite'],
                columns=['mes_ano_formatted', 'patamar'],
                values='valor',
                aggfunc='first'
            )
            df_pivot.columns = [
                f"{mes} {patamar.capitalize()}"
                for mes, patamar in df_pivot.columns
            ]
            df_pivot = df_pivot.reset_index().rename(columns={'re': 'RE', 'limite': 'Limite'})
            df_pivot = df_pivot.set_index('RE')
            logger.debug("DataFrame pivotado criado com sucesso")
            return df_pivot
        except Exception as e:
            logger.error(f"Erro ao converter DataFrame: {str(e)}")
            raise

    def calculate_differences(self) -> None:
        """Calcula as diferenças entre os limites de intercâmbio de duas datas e envia mensagens."""
        logger.info("Iniciando cálculo de diferenças")
        try:
            df_datas = self.get_data(self.consts.GET_RESTRICOES_ELETRICAS_HISTORICO).to_dict('records')
            if len(df_datas) < 2:
                logger.warning("Não há dados suficientes para comparar")
                return

            logger.info(f"Comparando datas: {df_datas[0]} e {df_datas[1]}")
            df1 = self.res_to_df(self.get_data(self.consts.GET_RESTRICOES_ELETRICAS, params={"data_produto": df_datas[0]['data_produto'], "tipo":df_datas[0]['tipo']}))
            df2 = self.res_to_df(self.get_data(self.consts.GET_RESTRICOES_ELETRICAS, params={"data_produto": df_datas[1]['data_produto'], "tipo":df_datas[1]['tipo']}))
            df = df1
            df1_months = [col for col in df1.columns if col != "Limite"]
            df2_months = [col for col in df2.columns if col != "Limite"]
            common_months = sorted(set(df1_months).intersection(df2_months))

            if not common_months:
                logger.warning("Nenhum mês comum encontrado entre os dois relatórios")
                return

            df_dif = pd.DataFrame(index=df1.index)
            df_dif["Limite"] = df1["Limite"]
            for month in common_months:
                df_dif[month] = df2[month] - df1[month]
            df_dif.dropna(how="all", subset=common_months, inplace=True)
            df_dif = df_dif.reset_index()
            df_dif = df_dif.set_index('RE')
            df_dif.columns.name = 'RE' 
            df_dif.index.name = 'RE' 
            df_dif.index.name = None 
            
            data_pmo = datetime.datetime.strptime(df_datas[0]['data_produto'], '%Y-%m-%d')
            data_ant = datetime.datetime.strptime(df_datas[1]['data_produto'], '%Y-%m-%d')          
            msg = f"Limites de intercambio {df_datas[0]['tipo']} para o PMO {data_pmo.month}/{data_pmo.year}"           
            msg_dif = f"Diferença dos Limites de intercambio ({str(data_pmo.month).zfill(2)}/{data_pmo.year} {df_datas[0]['tipo']} - {str(data_ant.month).zfill(2)}/{data_ant.year} {df_datas[1]['tipo']})"
            df_dif = df_dif.style.format(na_rep='', precision=0)
            df_dif = df_dif.set_caption(msg_dif)
            
            html = df_dif.to_html()         
            html = html.replace('<style type="text/css">\n</style>\n', html_style())            
            logger.debug("Generated HTML for table with CSS")              
            image_binary = html_to_image(html)

            logger.info(msg_dif)
            send_whatsapp_message(self.consts.WHATSAPP_DECKS,msg_dif, image_binary)
            logger.info("Cálculo de diferenças concluído e mensagens enviadas")
            
            df.columns.name = 'RE'
            df.index.name = None 
            df = df.style.format(na_rep='', precision=0)
            df = df.set_caption(msg)
            html = df.to_html()
            html = html.replace('<style type="text/css">\n</style>\n', html_style())            
            image_binary = html_to_image(html)
            logger.info(msg)
            send_whatsapp_message(self.consts.WHATSAPP_DECKS,msg, image_binary)
            
        except Exception as e:
            logger.error(f"Erro ao calcular diferenças: {str(e)}")
            raise


if __name__ == "__main__":
    analyzer = GenerateTable()
    analyzer.run_workflow()