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


TEXTOS_PT_BR = {
    "cotacao": "cotação",
    "Cotacao": "Cotação",
    "solicitacao": "solicitação",
    "Solicitacao": "Solicitação",
    "patrimonio": "patrimônio",
    "Patrimonio": "Patrimônio",
    "orcamento": "orçamento",
    "Orcamento": "Orçamento",
    "critica": "crítica",
    "Critica": "Crítica",
    "Pendencia": "Pendência",
    "pendencia": "pendência",
    "PENDENTE:": "Pendente:",
    "ERRO:": "Erro:",
    "ALERTA:": "Alerta:",
}

COLUNAS_IA = {
    "id": "ID",
    "tipo": "Tipo",
    "titulo": "Título",
    "descricao": "Descrição",
    "gravidade": "Gravidade",
    "origem": "Origem",
    "tabela_origem": "Tabela de origem",
    "registro_origem_id": "Registro de origem",
    "status": "Status",
    "sugestao_acao": "Sugestão de ação",
    "criado_em": "Criado em",
    "resolvido_em": "Resolvido em",
}

VALORES_IA = {
    "rubrica_critica": "Rubrica crítica",
    "saldo_insuficiente": "Saldo insuficiente",
    "cotacao_atrasada": "Cotação atrasada",
    "valor_divergente": "Valor divergente",
    "item_sem_patrimonio": "Item sem patrimônio",
    "item_sem_estoque": "Item sem estoque",
    "nota_fiscal_pendente": "Nota fiscal pendente",
    "fornecedor_recorrente": "Fornecedor recorrente",
    "risco_orcamentario": "Risco orçamentário",
    "baixa": "Baixa",
    "media": "Média",
    "alta": "Alta",
    "pendente": "Pendente",
    "resolvido": "Resolvido",
}

def normalizar_texto_portugues(valor):
    if valor is None or pd.isna(valor):
        return ""
    texto = str(valor)
    for origem, destino in TEXTOS_PT_BR.items():
        texto = texto.replace(origem, destino)
    return texto

def preparar_tabela_ia(df: pd.DataFrame) -> pd.DataFrame:
    tabela = df.rename(columns=COLUNAS_IA).copy()
    for coluna in tabela.columns:
        if tabela[coluna].dtype == "object" or pd.api.types.is_string_dtype(tabela[coluna]):
            tabela[coluna] = tabela[coluna].apply(
                lambda valor: VALORES_IA.get(str(valor), normalizar_texto_portugues(valor))
            )
    return tabela.fillna("")


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
        st.dataframe(preparar_tabela_ia(alertas), use_container_width=True, hide_index=True)

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
        st.dataframe(preparar_tabela_ia(gargalos_destino), use_container_width=True, hide_index=True)

with st.expander("Gargalos financeiros"):
    gargalos_financeiros = alertas[alertas["tipo"].isin(["rubrica_critica", "saldo_insuficiente", "valor_divergente", "risco_orcamentario"])] if total_alertas else pd.DataFrame()
    if len(gargalos_financeiros) == 0:
        st.success("Nenhum gargalo financeiro pendente.")
    else:
        st.dataframe(preparar_tabela_ia(gargalos_financeiros), use_container_width=True, hide_index=True)

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
