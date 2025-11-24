from .tasks import (
   RelatorioAcompanhamentoHidrologico,
   RelatorioLimitesIntercambioDecomp,
   CargaPatamarDecomp,
   DecksNewave,
   DeckDecomp,
   CargaPatamarNewave,
   PreciptacaoPrevista,
   VazoesSemanaisPrevistasPMO,
   VazoesDiariasPrevistasPDP,
   NotasTecnicasMedioPrazo,
   Ipdo,
   Weol,
   ArquivosModelosPDP
)
from typing import Dict, Type
from .webhook_products_interface import WebhookProductsInterface
PRODUCT_MAPPING: Dict[str, Type[WebhookProductsInterface]]  = {
   "relatorio_de_acompanhamento_hidrologico": RelatorioAcompanhamentoHidrologico,
   "modelo_gefs":PreciptacaoPrevista,
   "modelo_ecmwf":PreciptacaoPrevista,
   "modelo_eta":PreciptacaoPrevista,
   "resultados_preliminares_nao_consistidos_vazoes_semanais_pmo": VazoesSemanaisPrevistasPMO,
   "resultados_preliminares_consistidos_vazoes_semanais_pmo": VazoesSemanaisPrevistasPMO,
   "resultados_finais_consistidos_vazoes_diarias_pdp": VazoesDiariasPrevistasPDP,
   "deck_e_resultados_decomp_valor_esperado": DeckDecomp,
   "arquivos_dos_modelos_de_previsao_de_vazoes_diarias_pdp": ArquivosModelosPDP,
   "carga_por_patamar_decomp": CargaPatamarDecomp,
   "deck_preliminar_decomp_valor_esperado": DeckDecomp,
   "previsoes_de_carga_mensal_e_por_patamar_newave": CargaPatamarNewave,
   "ipdo_informativo_preliminar_diario_da_operacao": Ipdo,
   "deck_newave_preliminar": DecksNewave,
   "deck_newave_definitivo": DecksNewave,
   "decks_da_previsao_de_geracao_eolica_semanal_weolsm": Weol,
   "preliminar_relatorio_mensal_de_limites_de_intercambio": RelatorioLimitesIntercambioDecomp,
   "relatorio_mensal_de_limites_de_intercambio_para_o_modelo_decomp": RelatorioLimitesIntercambioDecomp,
   "notas_tecnicas_medio_prazo": NotasTecnicasMedioPrazo,
   "acomph": None,
   "rdh": None,
   "historico_de_precipitacao_por_satelite_pmo": None,
   "decks_de_entrada_e_saida_modelo_dessem": None,
   "arquivos_de_previsao_de_carga_para_o_dessem": None,
   "decks_de_entrada_do_prevcargadessem": None,
   "dados_utilizados_na_previsao_de_geracao_eolica": None,
   "arquivos_de_previsao_de_carga_para_o_dessem_prevcargadessem": None,
   
}

