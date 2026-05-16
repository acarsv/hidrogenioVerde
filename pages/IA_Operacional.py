import pandas as pd
import streamlit as st
from decimal import Decimal, InvalidOperation

from ia_operacional import (
    carregar_alertas,
    carregar_score_risco_rubrica,
    gerar_alertas_ia,
    marcar_alerta_resolvido,
)


def format_brl(value) -> str:
    try:
        value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        value = Decimal("0")
    formatted = f"{value:,.2f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


def format_currency_brl(valor) -> str:
    return f"R$ {format_brl(valor)}"


def format_percent_brl(value) -> str:
    return f"{format_brl(value)}%"


st.set_page_config(page_title="IA Operacional", layout="wide")
st.title("IA Operacional e Auditoria de Gargalos")

if "user" not in st.session_state or st.session_state.user is None:
    st.warning("Entre pelo aplicativo principal antes de acessar a IA Operacional.")
    st.stop()

if st.button("Executar análise IA", type="primary"):
    resultado = gerar_alertas_ia()
    st.success(
        f"Análise concluída: {resultado['criados']} alerta(s) criado(s), "
        f"{resultado['atualizados']} atualizado(s)."
    )
    st.rerun()

alertas = carregar_alertas("pendente")
total_alertas = len(alertas)
alertas_criticos = len(alertas[alertas["gravidade"] == "alta"]) if total_alertas else 0
pontos_atencao = len(alertas[alertas["gravidade"].isin(["media", "baixa"])]) if total_alertas else 0

c1, c2, c3 = st.columns(3)
c1.metric("Alertas críticos", alertas_criticos)
c2.metric("Pontos de atenção", pontos_atencao)
c3.metric("Situação normal", "Sim" if total_alertas == 0 else "Não")

with st.expander("Alertas pendentes", expanded=True):
    if total_alertas == 0:
        st.success("Nenhum alerta pendente.")
    else:
        st.dataframe(alertas, use_container_width=True, hide_index=True)

with st.expander("Score de risco por rubrica", expanded=True):
    score = carregar_score_risco_rubrica()
    if len(score) == 0:
        st.info("Nenhuma rubrica encontrada.")
    else:
        score_tabela = score.rename(columns={
            "codigo": "Rubrica",
            "nome": "Nome",
            "valor_orcado": "Valor orçado",
            "valor_reservado": "Valor reservado",
            "valor_utilizado": "Valor utilizado",
            "valor_comprometido": "Valor comprometido",
            "valor_solicitado": "Valor solicitado",
            "percentual_comprometido": "Percentual comprometido",
        }).copy()
        for coluna in [
            "Valor orçado",
            "Valor reservado",
            "Valor utilizado",
            "Valor comprometido",
            "Valor solicitado",
        ]:
            if coluna in score_tabela.columns:
                score_tabela[coluna] = score_tabela[coluna].apply(format_currency_brl)
        if "Percentual comprometido" in score_tabela.columns:
            score_tabela["Percentual comprometido"] = score_tabela["Percentual comprometido"].apply(format_percent_brl)
        st.dataframe(score_tabela, use_container_width=True, hide_index=True)

with st.expander("Gargalos de estoque/patrimônio"):
    gargalos_destino = alertas[alertas["tipo"].isin(["item_sem_patrimonio", "item_sem_estoque"])] if total_alertas else pd.DataFrame()
    if len(gargalos_destino) == 0:
        st.success("Nenhum gargalo de estoque ou patrimônio pendente.")
    else:
        st.dataframe(gargalos_destino, use_container_width=True, hide_index=True)

with st.expander("Gargalos financeiros"):
    gargalos_financeiros = alertas[alertas["tipo"].isin(["rubrica_critica", "saldo_insuficiente", "valor_divergente", "risco_orcamentario"])] if total_alertas else pd.DataFrame()
    if len(gargalos_financeiros) == 0:
        st.success("Nenhum gargalo financeiro pendente.")
    else:
        st.dataframe(gargalos_financeiros, use_container_width=True, hide_index=True)

if total_alertas:
    st.markdown("### Marcar alerta como resolvido")
    alerta_id = st.selectbox(
        "Alerta pendente",
        alertas["id"].tolist(),
        format_func=lambda item_id: f"#{item_id} - {alertas.loc[alertas.id == item_id, 'titulo'].iloc[0]}",
    )
    if st.button("Marcar como resolvido"):
        marcar_alerta_resolvido(alerta_id)
        st.success("Alerta marcado como resolvido.")
        st.rerun()
