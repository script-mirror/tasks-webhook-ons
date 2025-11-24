import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from middle.utils import html_to_image
from typing import Optional, Dict, Any
from middle.message import send_whatsapp_message
from middle.utils import setup_logger, Constants, html_style
from middle.utils.file_manipulation import extract_zip
from middle.s3 import handle_webhook_file, get_latest_webhook_product
from middle.airflow import trigger_dag  


class DeckDessem():
    def __init__(self):
        self.ons_to_ccee = DessemOnsToCcee()
        self.trigger_dag = trigger_dag
        self.consts = Constants()
        self.logger = setup_logger()    
    
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        self.logger.info("Starting DeckDessem workflow")
        try:
            self.run_process()
        except Exception as e:
            self.logger.error("DeckDessem workflow failed: %s", str(e), exc_info=True)
            raise
   
    def run_process(self):
        self.logger.info("Executing DeckDessem process")
        try:
            #self.trigger_dag(dag_id="-DESSEM_ONS-TO-CCEE", conf={})
            payload = get_latest_webhook_product(self.consts.WEBHOOK_DECK_DESSEM)[0]
            base_path = handle_webhook_file(payload, self.consts.PATH_TMP)
            path_deck_unzip = extract_zip(base_path)
            
            deck_date = self.read_data_deck(path_deck_unzip)
            
            df_rn = self.read_renovaveis(path_deck_unzip, deck_date )
            #elf.post_data(df)
            
            df_cmo = self.read_cmo_sist(path_deck_unzip, deck_date )
            #self.post_data(df)
            
            df_load = self.read_load_pdo(path_deck_unzip, deck_date)
            #self.post_data(df)
            
            
        except Exception as e:
            self.logger.error("DeckDessem process failed: %s", str(e), exc_info=True)
            raise
        
    def read_file(self, directory: str, file_find: str):
        self.logger.info(f"Searching for file with prefix '{file_find}' in: {directory}")
        for file in os.listdir(directory):
            if file.lower().startswith(file_find.lower()):
                file_path = os.path.join(directory, file)
                self.logger.info(f"File found: {file}")
                with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                    lines = f.readlines()
                self.logger.debug(f"File read: {len(lines)} lines")
                return lines

        error_msg = f"File with prefix '{file_find}' not found in {directory}"
        self.logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    def read_cmo_sist(self, path: str, deck_date, fonte:str) -> pd.DataFrame:
        self.logger.info("Reading load data from pdo_sist.dat")
        file = self.read_file(path, 'pdo_cmosist.dat')
        data, load = [], False

        for line in file:
            parts = line.split(';')

            if parts[0].strip().lower() == 'iper':
                load = True
                continue

            if load and '-' not in parts[0]:
                minuto = (int(parts[0].strip()) - 1) * 30
                date_load = deck_date + timedelta(minutes=minuto)
                if int(parts[0].strip()) < 49:
                    data.append({
                        'DATA': date_load.replace(minute=0, second=0, microsecond=0),
                        'DIA': int(date_load.day),
                        'HORA': int(date_load.hour),
                        'MINUTO': int(date_load.minute),
                        'SUB': parts[2].strip(),
                        'CMO': float(parts[4].strip())
                    })

        df = pd.DataFrame(data)
        df = df[df['SUB'] != 'FC']
        df['SUB'] = df['SUB'].replace({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
        df = df.groupby(['DATA','DIA', 'HORA', 'SUB'])['CMO'].mean().round(2).reset_index()
        df = df.drop('DIA', axis=1)
        df = df.drop('HORA', axis=1)
        df['FONTE'] = fonte
        return df
    
    
    def read_load_pdo(self, path: str, deck_date) -> pd.DataFrame:
        self.logger.info("Reading load data from pdo_sist.dat")
        pdo_file = self.read_file(path, 'pdo_sist.dat')
        data, load = [], False
        for line in pdo_file:
            parts = line.split(';') 
              
            if parts[0].strip().lower() == 'iper':
                load = True
                continue

            if load and '-' not in parts[0]:
                minuto = (int(parts[0].strip()) - 1) * 30
                date_load = deck_date + timedelta(minutes=minuto)
                if int(parts[0].strip()) < 49:
                    data.append({
                        'DATA': date_load,
                        'SUB': parts[2].strip(),
                        'CARGA': parts[4].strip() })

        df = pd.DataFrame(data)
        df = df[df['SUB'] != 'FC']
        df['SUB'] = df['SUB'].replace({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
        return df


    def read_tm(self, path: str, deck_date) -> pd.DataFrame:
        pdo_file = self.read_file(path, 'entdados.dat')
        data, load = [], False
        for line in pdo_file:
            words = line.split()
            parts = line.split(';')

            if parts[0].strip().lower() == 'iper':
                load = True
                continue

            if load and '-' not in parts[0] :
                minuto = (int(parts[0].strip()) - 1) * 30
                date_load = deck_date + timedelta(minutes=minuto)
                if int(parts[0].strip()) < 49:
                    data.append({
                        'DATA': date_load,
                        'SUB': parts[2].strip(),
                        'CARGA': parts[4].strip()
                    })

        df = pd.DataFrame(data)
        df = df[df['SUB'] != 'FC']
        df['SUB'] = df['SUB'].replace({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
        return df
    
    def read_renovaveis(self, path: str, date_deck) -> pd.DataFrame:
        file = self.read_file(path, 'renovaveis.dat')
        next_friday = (4 - date_deck.weekday() + 7) % 7
        end_date = date_deck + timedelta(days=next_friday+1)
        list_date = pd.date_range(start=date_deck, end=end_date, freq='30min')
        df = pd.DataFrame()

        for line in file:
            parts = line.split(';')

            if parts[0].strip().upper() == 'EOLICA':
                df = pd.concat([df, pd.DataFrame([pd.Series({
                        'BARRA': parts[1].strip(),
                        'FONTE': parts[2].split('_')[-1].strip(),
                        'SUB': None,
                        **{date: None for date in list_date}
                    })])], ignore_index=True)

            elif parts[0].strip().upper() == 'EOLICASUBM':
                df.loc[df['BARRA'] == parts[1].strip(), 'SUB'] = parts[2].strip()

            elif parts[0].strip().upper() == 'EOLICA-GERACAO':
                date = next((dt for dt in list_date if dt.day == int(parts[2].strip())), None)
                star_date = datetime(date.year, date.month, int(parts[2].strip()), int(parts[3].strip()), int(parts[4].strip())*30, 0)
                date = next((dt for dt in list_date if dt.day == int(parts[5].strip())), None)
                end_date = datetime(date.year, date.month, int(parts[5].strip()), int(parts[6].strip()), int(parts[7].strip())*30, 0)
                list_date_power = pd.date_range(start=star_date, end=end_date, freq='30min', inclusive='both')
                
                for dt in list_date_power:
                    df.loc[df['BARRA'] == parts[1].strip(), dt] = float(parts[8].strip())
        
        df = df.copy()  
        columns = df.columns[3:]
        df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')
        df_agg = df.groupby(['SUB', 'FONTE'], as_index=False)[columns].sum()
        df_melt = df_agg.melt(id_vars=['SUB', 'FONTE'],value_vars=columns, var_name='DATA', value_name='VALOR')
        df = df_melt.pivot_table(index=['SUB', 'DATA'], columns='FONTE', values='VALOR', aggfunc='sum', fill_value=0).reset_index()
        df['SUB'] = df['SUB'].replace({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
        return df
    
    
    def read_data_deck(self, path: str) -> pd.DataFrame:
        pdo_file = self.read_file(path, 'dadvaz.dat')
        reading_data = False
        for line in pdo_file:
            parts = line.split()
            if len(parts) > 3:
                if parts[3].lower() == 'ano':
                    reading_data = True
                    continue
                if reading_data:
                    if parts[3].isdigit():
                        return datetime(int(parts[3]), int(parts[2]),int(parts[1]))  


    def post_data(self,df):
        pass

if __name__ == '__main__':
    try:
        obj = DeckDessem()
        obj.run_workflow()
    except Exception as e:
        raise