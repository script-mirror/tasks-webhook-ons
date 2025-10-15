from .tasks import (
   RelatorioAcompanhamentoHidrologico,
   RelatorioLimitesIntercambioDecomp,
   CargaPatamarDecomp,
   DecksNewave,
   DeckDecomp,
   CargaPatamarNewave,
   PreciptacaoPrevista,
   ResultadosPreliminaresNaoConsistidos,
   RelatorioResultadosFinaisConsistidosPDP,
   Ipdo,
   NotasTecnicasMedioPrazo
)
from typing import Dict, Type
from .webhook_products_interface import WebhookProductsInterface
PRODUCT_MAPPING: Dict[str, Type[WebhookProductsInterface]]  = {
   "relatorio_de_acompanhamento_hidrologico": RelatorioAcompanhamentoHidrologico,
   "modelo_gefs":PreciptacaoPrevista,
   "resultados_preliminares_nao_consistidos_vazoes_semanais_pmo": ResultadosPreliminaresNaoConsistidos,
   "relatorio_dos_resultados_finais_consistidos_da_previsao_diaria_pdp": RelatorioResultadosFinaisConsistidosPDP,
   "deck_e_resultados_decomp_valor_esperado": None,
   "resultados_finais_consistidos_vazoes_diarias_pdp": None,
   "resultados_preliminares_consistidos_vazoes_semanais_pmo": None,
   "arquivos_dos_modelos_de_previsao_de_vazoes_semanais_pmo": None,
   "arquivos_dos_modelos_de_previsao_de_vazoes_diarias_pdp": None,
   "acomph": None,
   "rdh": None,
   "historico_de_precipitacao_por_satelite_pmo": None,
   "modelo_eta":PreciptacaoPrevista,
   "carga_por_patamar_decomp": CargaPatamarDecomp,
   "deck_preliminar_decomp_valor_esperado": DeckDecomp,
   "decks_de_entrada_e_saida_modelo_dessem": None,
   "arquivos_de_previsao_de_carga_para_o_dessem": None,
   "decks_de_entrada_do_prevcargadessem": None,
   "previsoes_de_carga_mensal_e_por_patamar_newave": CargaPatamarNewave,
   "ipdo_informativo_preliminar_diario_da_operacao": Ipdo,
   "modelo_ecmwf":PreciptacaoPrevista,
   "dados_utilizados_na_previsao_de_geracao_eolica": None,
   "arquivos_de_previsao_de_carga_para_o_dessem_prevcargadessem": None,
   "deck_newave_preliminar": DecksNewave,
   "deck_newave_definitivo": DecksNewave,
   "decks_da_previsao_de_geracao_eolica_semanal_weol_sm": None,
   "preliminar_relatorio_mensal_de_limites_de_intercambio": RelatorioLimitesIntercambioDecomp,
   "relatorio_mensal_de_limites_de_intercambio_para_o_modelo_decomp": RelatorioLimitesIntercambioDecomp,
   "notas_tecnicas_medio_prazo": NotasTecnicasMedioPrazo
}

