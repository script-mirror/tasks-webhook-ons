from app.tasks.relatorio_limites_intercambio_modelo_decomp import RelatorioLimitesIntercambioDecomp
import pdb

if __name__ == "__main__":
    import datetime
    import pandas as pd

    preliminar_path = "/home/arthur-moraes/WX2TB/Documentos/fontes/PMO/trading-middle-tasks-webhook-ons/RT-ONS DPL 0298-2025_Limites PMO_Agosto-2025.pdf"
    webhook_prod = RelatorioLimitesIntercambioDecomp()
    df = webhook_prod.read_table(preliminar_path, datetime.date(2025, 8, 1))
    df = webhook_prod.sanitaze_dataframe(df)
    pdb.set_trace()