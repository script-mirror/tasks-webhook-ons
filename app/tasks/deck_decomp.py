import sys
import os
import pandas as pd
from datetime import datetime
from middle.utils import html_to_image
from typing import Optional, Dict, Any
from pathlib import Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))
from middle.message import send_whatsapp_message
from middle.utils import setup_logger, Constants, html_style # noqa: E402
from middle.utils.file_manipulation import extract_zip
from middle.s3 import ( # noqa: E402
    handle_webhook_file,
    get_latest_webhook_product,
)
from middle.airflow import trigger_dag  # noqa: E402
from app.webhook_products_interface import WebhookProductsInterface  # noqa: E402
from app.schema import WebhookSintegreSchema  # noqa: E402

class DeckDecomp(WebhookProductsInterface):
    def __init__(self, payload: Optional[WebhookSintegreSchema]):
        super().__init__(payload)    
        self.read_cmo = ReadResultsDecomp()
        self.trigger_dag = trigger_dag
        self.logger = setup_logger()  
        self.consts = Constants()
        self.logger.debug("Initialized DeckDecomp instance")
    
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        self.logger.info("Starting DeckDecomp workflow")
        try:
            self.run_process()
        except Exception as e:
            self.logger.error("DeckDecomp workflow failed: %s", str(e), exc_info=True)
            raise
   
    def run_process(self):
        self.logger.info("Executing DeckDecomp process")
        try:
            self.read_cmo.run_workflow()
            self.trigger_dag(dag_id="1.16-DECOMP_ONS-TO-CCEE", conf={})
        except Exception as e:
            self.logger.error("DeckDecomp process failed: %s", str(e), exc_info=True)
            raise
    def post_data(self):
        pass
      
       
     
class ReadResultsDecomp:
    
    def __init__(self):
        self.logger = setup_logger()  # Usa o self.logger global
        self.logger.debug("Initialized ReadResultsDecomp instance")
        self.consts = Constants()
    def run_workflow(self, filepath: Optional[str] = None, manually_date: Optional[datetime] = None):
        self.logger.info("Starting ReadResultsDecomp workflow")
        try:
            self.logger.debug("Creating temporary directory: %s", self.consts.PATH_TMP)
            os.makedirs(self.consts.PATH_TMP, exist_ok=True)           
            self.logger.debug("Fetching latest webhook product for %s", self.consts.WEBHOOK_DECK_DECOMP_PRELIMINAR)
            payload = get_latest_webhook_product(self.consts.WEBHOOK_DECK_DECOMP_PRELIMINAR)[0]
            self.logger.info("Processing webhook file from payload")
            base_path = handle_webhook_file(payload, self.consts.PATH_TMP)
            
            self.run_process(base_path)
           
        except Exception as e:
            self.self.logger.error("ReadResultsDecomp workflow failed: %s", str(e), exc_info=True)
            raise

    def run_process(self, base_path):
        self.logger.info("Processing file at path: %s", base_path)
        try:
            self.logger.debug("Extracting zip file")
            unzip_path = extract_zip(base_path)
            self.logger.debug("Listing files in unzipped directory: %s", unzip_path)
            results_file = [file for file in os.listdir(unzip_path) if "resultados" in file.lower()]
            if not results_file:
                self.logger.error("No results file found in %s", unzip_path)
                raise FileNotFoundError("No file containing 'resultados' found")
            self.logger.info("Found results file: %s", results_file[0])
            unzip_path = extract_zip(os.path.join(unzip_path, results_file[0]))
            self.read_cmo(unzip_path)
        except Exception as e:
            self.logger.error("File processing failed: %s", str(e), exc_info=True)
            raise
        
    def read_cmo(self, unzip_path):
        self.logger.info("Reading CMO data from: %s", unzip_path)
        try:
            self.logger.debug("Searching for summary files")
            flow_files = [file for file in os.listdir(unzip_path) if "sumario" in file.lower()]
            if not flow_files:
                self.logger.error("No summary file found in %s", unzip_path)
                raise FileNotFoundError("No file containing 'sumario' found")
            self.logger.info("Reading summary file: %s", flow_files[0])
            
            with open(os.path.join(unzip_path, flow_files[0]), 'r', encoding='utf-8') as file:
                text = file.read()
            rev = None
            lines = text.split('\n')
            cost_data = []
            cost_headers = []
            in_cost_section = False
            self.logger.debug("Parsing summary file content")
            for line in lines:
                line_normalized = ' '.join(line.split()).upper() 
                
                if 'PMO' in line_normalized:
                    try:
                        rev = int(line.split('REV')[1].split('-')[0])
                        self.logger.info("Found revision number: %d", rev)
                    except (IndexError, ValueError) as e:
                        self.logger.error("Failed to parse revision number: %s", str(e))
                        raise
                             
                if "CUSTO MARGINAL DE OPERACAO" in line_normalized:
                    self.logger.debug("Entering CMO section")
                    in_cost_section = True
                    continue 
                if in_cost_section:
                    if "SSIS" in line_normalized and "SEM_01" in line_normalized:
                        cost_headers = line.split()
                        self.logger.debug("Found cost headers: %s", cost_headers)
                        continue
                    if "X----X" in line_normalized and not line.strip().startswith("Ssis"):
                        if cost_data:
                            self.logger.debug("Exiting CMO section")
                            break
                    if line.strip() and not "X----X" in line_normalized:
                        values = line.split()
                        if len(values) == len(cost_headers):
                            cost_entry = dict(zip(cost_headers, values))
                            desired_med = ["Med_SE", "Med_S", "Med_NE", "Med_N"]
                            ssis_value = cost_entry.get('Ssis', '')
                            if ssis_value.startswith("Med_") and ssis_value in desired_med:
                                cost_entry['Ssis'] = cost_entry['Ssis'].replace("Med_", "")
                                cost_data.append(cost_entry)
                                self.logger.debug("Added cost entry: %s", cost_entry)
                        else:
                            self.logger.warning("Skipping line with mismatched values: %s", line)

            if rev is None:
                self.logger.error("Revision number (REV) not found in the summary file")
                raise ValueError("Revision number (REV) not found in the summary file.")
            
            if not cost_data:
                self.logger.warning("No cost data extracted from summary file")
            
            self.logger.info("Creating DataFrame from %d cost entries", len(cost_data))
            df_cmo = pd.DataFrame(cost_data)
            df_cmo.columns.name = 'SUBMERCADO'
            df_cmo = df_cmo.set_index("Ssis")
            df_cmo.index.name = None  
            df_cmo = df_cmo.style.format(precision=0)
            df_cmo = df_cmo.set_caption(f'CMO ONS Decomp Preliminar - REV {rev} ')
            self.logger.debug("Converting DataFrame to HTML")
            html = df_cmo.to_html()
            html = html.replace('<style type="text/css">\n</style>\n', html_style())
            self.logger.info("Converting HTML to image")
            image_binary = html_to_image(html)
            msg = f'CMO ONS Decomp Preliminar - REV {rev} '
            self.logger.info("Sending WhatsApp message for REV %d", rev)
            send_whatsapp_message(self.consts.WHATSAPP_PMO, msg, image_binary)
            self.logger.info("Successfully processed and sent CMO data for REV %d", rev)
            
        except Exception as e:
            self.logger.error("Failed to read CMO data: %s", str(e), exc_info=True)
            raise


if __name__ == '__main__':
    try:
        obj = ReadResultsDecomp()
        obj.run_workflow()
    except Exception as e:
        raise