import os
from io import BytesIO
from datetime import date
from decimal import Decimal, InvalidOperation
import bcrypt
import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv(override=True)
st.set_page_config(page_title="Hidrogênio Verde - Compras", layout="wide")
APP_DEPLOY_VERSION = "2026-05-11.10"

def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        st.error("DATABASE_URL nao foi definida no arquivo .env.")
        st.stop()

    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as exc:
        parsed = urlparse(database_url)
        host = parsed.hostname or "host nao identificado"
        try:
            port = parsed.port or "porta padrao"
        except ValueError:
            port = "porta invalida na DATABASE_URL"
        user = parsed.username or "usuario nao identificado"
        st.error(
            "Nao foi possivel conectar ao Supabase. "
            f"Confira usuario, senha e host no .env. Host: {host}, porta: {port}, usuario: {user}."
        )
        st.caption(
            "Se a senha do banco tiver caracteres como @, #, %, /, : ou espaco, "
            "copie novamente a connection string URI do Supabase ou codifique a senha na URL."
        )
        with st.expander("Detalhe tecnico"):
            st.code(str(exc))
        st.stop()

def query(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if cur.description:
                return pd.DataFrame(cur.fetchall())
            return pd.DataFrame()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def execute(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def has_column(table_name: str, column_name: str) -> bool:
    df = query("""
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = %s
      and column_name = %s
    limit 1
    """, (table_name, column_name))
    return len(df) == 1

def ensure_financial_governance_schema():
    if not has_column("rubricas", "valor_minimo_operacional"):
        execute("alter table rubricas add column valor_minimo_operacional numeric(14,2) not null default 0")
    if not has_column("rubricas", "reserva_tecnica_percentual"):
        execute("alter table rubricas add column reserva_tecnica_percentual numeric(5,2) not null default 5")
    if not has_column("rubricas", "encerrada"):
        execute("alter table rubricas add column encerrada boolean not null default false")
    if not has_column("rubricas", "encerrada_em"):
        execute("alter table rubricas add column encerrada_em timestamptz")
    if not has_column("rubricas", "encerrada_por"):
        execute("alter table rubricas add column encerrada_por uuid references usuarios_app(id)")

    execute("""
    update rubricas
    set valor_minimo_operacional = case
        when tipo = 'material_permanente' then 2000
        when tipo = 'material_consumo' then 300
        when tipo = 'servico_pf' then 500
        else 0
    end
    where valor_minimo_operacional = 0
    """)

    execute("""
    create table if not exists movimentacoes_orcamento (
      id bigserial primary key,
      rubrica_id bigint not null references rubricas(id),
      usuario_id uuid references usuarios_app(id),
      operacao text not null,
      valor numeric(14,2) not null default 0,
      justificativa text,
      criado_em timestamptz not null default now()
    )
    """)

    execute("""
    create or replace view vw_orcamento as
    select
      r.id,
      r.codigo,
      r.nome,
      r.tipo,
      r.valor_orcado,
      r.valor_reservado,
      r.valor_utilizado,
      (
        r.valor_orcado
        - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
        - r.valor_reservado
        - r.valor_utilizado
      ) as saldo_disponivel,
      case
        when r.valor_orcado > 0 then round((r.valor_utilizado * 100.0 / r.valor_orcado), 2)
        else 0
      end as percentual_utilizado,
      r.valor_minimo_operacional,
      r.reserva_tecnica_percentual,
      round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2) as reserva_tecnica,
      case
        when (
          r.valor_orcado
          - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
          - r.valor_reservado
          - r.valor_utilizado
        ) > 0
         and (
          r.valor_orcado
          - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
          - r.valor_reservado
          - r.valor_utilizado
        ) < r.valor_minimo_operacional
        then (
          r.valor_orcado
          - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
          - r.valor_reservado
          - r.valor_utilizado
        )
        else 0
      end as saldo_residual,
      r.encerrada,
      case
        when r.valor_orcado > 0 then round(((round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2) + r.valor_reservado + r.valor_utilizado) * 100.0 / r.valor_orcado), 2)
        else 0
      end as percentual_comprometido
    from rubricas r
    where r.ativo = true
    """)

def ensure_permissions_schema():
    if not has_column("usuarios_app", "permissoes"):
        st.error("O banco precisa da coluna de permissões para iniciar o app.")
        st.caption("Execute este SQL no Supabase SQL Editor e reinicie o app no Streamlit Cloud.")
        st.code(
            "alter table usuarios_app add column permissoes text[] not null default array[]::text[];",
            language="sql",
        )
        st.stop()

    execute("""
    update usuarios_app
    set permissoes = array['orcamento','nova_exigencia','solicitacoes','cotacoes','compra_nota','itens_comprados','membros']
    where papel = 'admin' and (permissoes is null or cardinality(permissoes) = 0)
    """)

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def check_password(password: str, senha_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), senha_hash.encode())

def format_brl(value) -> str:
    try:
        value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        value = Decimal("0")
    formatted = f"{value:,.2f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")

def format_currency_brl(valor) -> str:
    return f"R$ {format_brl(valor)}"

def format_currency_brl_markdown(valor) -> str:
    return format_currency_brl(valor).replace("$", r"\$")

def format_percent_brl(value) -> str:
    return f"{format_brl(value)}%"

def financial_status(row) -> str:
    saldo_disponivel = Decimal(str(row.get("saldo_disponivel", 0)))
    valor_minimo = Decimal(str(row.get("valor_minimo_operacional", 0)))
    percentual_comprometido = Decimal(str(row.get("percentual_comprometido", 0)))

    if bool(row.get("encerrada", False)) or saldo_disponivel <= 0:
        return "Encerrado"
    if valor_minimo > 0 and saldo_disponivel < valor_minimo:
        return "Residual"
    if percentual_comprometido > 90:
        return "Critico"
    if percentual_comprometido > 70:
        return "Comprometido"
    return "Disponivel"

def status_alert_level(status: str) -> str:
    return {
        "Encerrado": "Cinza",
        "Residual": "Vermelho",
        "Critico": "Laranja",
        "Comprometido": "Amarelo",
        "Disponivel": "Verde",
        "Normal": "Verde",
    }.get(status, "Verde")

def risk_color_css(risk: str) -> str:
    return {
        "Verde": "#16a34a",
        "Amarelo": "#ca8a04",
        "Laranja": "#ea580c",
        "Vermelho": "#dc2626",
        "Cinza": "#6b7280",
    }.get(risk, "#16a34a")

def excede_saldo_disponivel(rubrica_id: int, valor: Decimal) -> tuple[bool, Decimal]:
    saldo_df = query("select saldo_disponivel from vw_orcamento where id=%s", (rubrica_id,))
    saldo = Decimal(str(saldo_df.iloc[0]["saldo_disponivel"])) if len(saldo_df) == 1 else Decimal("0")
    return valor > saldo, saldo

def parse_responsaveis(value) -> list[str]:
    if value is None or pd.isna(value):
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]

def nome_aba_excel(nome: str, usadas: set[str]) -> str:
    caracteres_invalidos = "[]:*?/\\"
    base = "".join("_" if char in caracteres_invalidos else char for char in str(nome or "Rubrica"))
    base = base.strip()[:31] or "Rubrica"
    nome_final = base
    contador = 2
    while nome_final in usadas:
        sufixo = f"_{contador}"
        nome_final = f"{base[:31 - len(sufixo)]}{sufixo}"
        contador += 1
    usadas.add(nome_final)
    return nome_final

def construir_planilha_itens_comprados(df: pd.DataFrame) -> bytes:
    planilha = df.copy()
    for coluna in ["Quantidade", "Valor da compra", "Valor da NF"]:
        planilha[coluna] = pd.to_numeric(planilha[coluna], errors="coerce").fillna(0)
    for coluna in ["Data de emissão", "Lançado em"]:
        planilha[coluna] = planilha[coluna].astype(str).replace({"NaT": "", "None": ""})

    resumo = (
        planilha
        .groupby(["Rubrica", "Nome da rubrica"], dropna=False)
        .agg(
            Itens=("Solicitação", "count"),
            Total_compra=("Valor da compra", "sum"),
            Total_nf=("Valor da NF", "sum"),
        )
        .reset_index()
        .rename(columns={
            "Total_compra": "Total da compra",
            "Total_nf": "Total da NF",
        })
    )

    arquivo = BytesIO()
    with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
        resumo.to_excel(writer, index=False, sheet_name="Resumo por rubrica")
        abas_usadas = {"Resumo por rubrica"}
        for rubrica, itens_rubrica in planilha.groupby("Rubrica", dropna=False):
            nome_aba = nome_aba_excel(rubrica, abas_usadas)
            itens_rubrica.to_excel(writer, index=False, sheet_name=nome_aba)

        for worksheet in writer.book.worksheets:
            for column_cells in worksheet.columns:
                largura = max(len(str(cell.value or "")) for cell in column_cells)
                worksheet.column_dimensions[column_cells[0].column_letter].width = min(max(largura + 2, 12), 50)

    arquivo.seek(0)
    return arquivo.getvalue()

@st.dialog("Atualizar responsáveis")
def atualizar_responsaveis_dialog():
    rubricas = query("""
    select id, codigo, nome, coalesce(responsaveis, '') as responsaveis
    from rubricas
    where ativo = true
    order by codigo
    """)
    if len(rubricas) == 0:
        st.info("Não há rubricas ativas para atualizar.")
        return

    rubrica_id = st.selectbox(
        "Rubrica",
        rubricas["id"].tolist(),
        format_func=lambda item_id: (
            f"{rubricas.loc[rubricas.id == item_id, 'codigo'].iloc[0]} - "
            f"{rubricas.loc[rubricas.id == item_id, 'nome'].iloc[0]}"
        ),
    )
    rubrica = rubricas.loc[rubricas.id == rubrica_id].iloc[0]
    responsaveis_atuais = parse_responsaveis(rubrica["responsaveis"])

    membros = query("""
    select split_part(trim(nome), ' ', 1) as usuario
    from usuarios_app
    where ativo = true
    order by usuario
    """)
    opcoes = membros["usuario"].tolist() if len(membros) else []
    for responsavel in responsaveis_atuais:
        if responsavel not in opcoes:
            opcoes.append(responsavel)

    responsaveis = st.multiselect(
        "Responsáveis",
        opcoes,
        default=responsaveis_atuais,
        placeholder="Selecione um ou mais responsáveis",
    )

    c1, c2 = st.columns(2)
    if c1.button("Salvar", type="primary", use_container_width=True):
        execute(
            "update rubricas set responsaveis=%s where id=%s",
            (", ".join(responsaveis) if responsaveis else None, int(rubrica_id)),
        )
        st.success("Responsáveis atualizados.")
        st.rerun()
    if c2.button("Cancelar", use_container_width=True):
        st.rerun()

@st.dialog("Remanejar saldo")
def remanejar_saldo_dialog(usuario_id):
    rubricas = query("""
    select id, codigo, nome, saldo_disponivel
    from vw_orcamento
    where encerrada = false
    order by codigo
    """)
    if len(rubricas) < 2:
        st.info("Sao necessarias pelo menos duas rubricas ativas para remanejamento.")
        return

    def label_rubrica(item_id):
        rubrica = rubricas.loc[rubricas.id == item_id].iloc[0]
        return f"{rubrica['codigo']} - {rubrica['nome']} ({format_currency_brl(rubrica['saldo_disponivel'])})"

    origem_id = st.selectbox("Rubrica origem", rubricas["id"].tolist(), format_func=label_rubrica)
    destino_id = st.selectbox("Rubrica destino", rubricas["id"].tolist(), format_func=label_rubrica)
    saldo_origem = Decimal(str(rubricas.loc[rubricas.id == origem_id, "saldo_disponivel"].iloc[0]))
    valor_maximo = float(max(saldo_origem, Decimal("0.01")))
    valor = st.number_input("Valor", min_value=0.01, max_value=valor_maximo, value=0.01, step=100.0)
    justificativa = st.text_area("Justificativa formal")

    c1, c2 = st.columns(2)
    if c1.button("Confirmar remanejamento", type="primary", use_container_width=True):
        valor_decimal = Decimal(str(valor))
        if origem_id == destino_id:
            st.error("A rubrica de origem deve ser diferente da rubrica de destino.")
        elif valor_decimal > saldo_origem:
            st.error("O valor informado supera o saldo disponivel da rubrica de origem.")
        elif not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            execute("update rubricas set valor_orcado = valor_orcado - %s where id = %s", (valor_decimal, int(origem_id)))
            execute("update rubricas set valor_orcado = valor_orcado + %s where id = %s", (valor_decimal, int(destino_id)))
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'remanejamento_saida',%s,%s)",
                (int(origem_id), usuario_id, valor_decimal, justificativa),
            )
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'remanejamento_entrada',%s,%s)",
                (int(destino_id), usuario_id, valor_decimal, justificativa),
            )
            st.success("Remanejamento registrado.")
            st.rerun()
    if c2.button("Cancelar", use_container_width=True):
        st.rerun()

@st.dialog("Reservar valor")
def reservar_valor_dialog(usuario_id):
    rubricas = query("""
    select id, codigo, nome, saldo_disponivel
    from vw_orcamento
    where encerrada = false
    order by codigo
    """)
    if len(rubricas) == 0:
        st.info("Nao ha rubricas abertas para reserva.")
        return

    def label_rubrica(item_id):
        rubrica = rubricas.loc[rubricas.id == item_id].iloc[0]
        return f"{rubrica['codigo']} - {rubrica['nome']} ({format_currency_brl(rubrica['saldo_disponivel'])})"

    rubrica_id = st.selectbox("Rubrica", rubricas["id"].tolist(), format_func=label_rubrica)
    saldo = Decimal(str(rubricas.loc[rubricas.id == rubrica_id, "saldo_disponivel"].iloc[0]))
    valor_maximo = float(max(saldo, Decimal("0.01")))
    valor = st.number_input("Valor reservado", min_value=0.01, max_value=valor_maximo, value=0.01, step=100.0)
    descricao = st.text_input("Descricao da reserva", value="Reserva financeira administrativa")
    justificativa = st.text_area("Justificativa")

    if st.button("Registrar reserva", type="primary", use_container_width=True):
        valor_decimal = Decimal(str(valor))
        if valor_decimal > saldo:
            st.error("O valor informado supera o saldo disponivel da rubrica.")
        elif not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            execute("""
            insert into solicitacoes_compra
              (rubrica_id, solicitante_id, gerente_id, descricao, quantidade, unidade, valor_estimado, justificativa, status, autorizado, autorizado_em)
            values (%s,%s,%s,%s,1,'un',%s,%s,'em_andamento',true,now())
            """, (int(rubrica_id), usuario_id, usuario_id, descricao, valor_decimal, justificativa))
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'reserva_financeira',%s,%s)",
                (int(rubrica_id), usuario_id, valor_decimal, justificativa),
            )
            sincronizar_orcamento()
            st.success("Reserva registrada.")
            st.rerun()

@st.dialog("Encerrar rubrica")
def encerrar_rubrica_dialog(usuario_id):
    rubricas = query("""
    select id, codigo, nome
    from vw_orcamento
    where encerrada = false
    order by codigo
    """)
    if len(rubricas) == 0:
        st.info("Nao ha rubricas abertas para encerrar.")
        return

    rubrica_id = st.selectbox(
        "Rubrica",
        rubricas["id"].tolist(),
        format_func=lambda item_id: f"{rubricas.loc[rubricas.id == item_id, 'codigo'].iloc[0]} - {rubricas.loc[rubricas.id == item_id, 'nome'].iloc[0]}",
    )
    justificativa = st.text_area("Justificativa de encerramento")
    if st.button("Encerrar oficialmente", type="primary", use_container_width=True):
        if not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            execute(
                "update rubricas set encerrada = true, encerrada_em = now(), encerrada_por = %s where id = %s",
                (usuario_id, int(rubrica_id)),
            )
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'encerramento',0,%s)",
                (int(rubrica_id), usuario_id, justificativa),
            )
            st.success("Rubrica encerrada.")
            st.rerun()

@st.dialog("Historico/Auditoria")
def historico_orcamento_dialog():
    historico = query("""
    select
      m.criado_em as "Data",
      r.codigo as "Rubrica",
      coalesce(u.nome, 'Sistema') as "Usuario",
      m.operacao as "Operacao",
      m.valor as "Valor",
      m.justificativa as "Justificativa"
    from movimentacoes_orcamento m
    join rubricas r on r.id = m.rubrica_id
    left join usuarios_app u on u.id = m.usuario_id
    order by m.criado_em desc
    limit 200
    """)
    if len(historico) == 0:
        st.info("Ainda nao ha movimentacoes orcamentarias registradas.")
        return
    historico["Valor"] = historico["Valor"].apply(format_currency_brl)
    st.dataframe(historico, use_container_width=True, hide_index=True)

def exibir_detalhe_rubrica(rubrica):
    detalhes = pd.DataFrame(
        [
            ("Codigo", rubrica["codigo"]),
            ("Rubrica", rubrica["nome"]),
            ("Tipo", rubrica["tipo"]),
            ("Responsavel", rubrica.get("responsaveis") or "-"),
            ("Valor orcado", format_currency_brl(rubrica["valor_orcado"])),
            ("Valor reservado", format_currency_brl(rubrica["valor_reservado"])),
            ("Valor utilizado", format_currency_brl(rubrica["valor_utilizado"])),
            ("Reserva tecnica", format_currency_brl(rubrica["reserva_tecnica"])),
            ("Reserva tecnica (%)", format_percent_brl(rubrica["reserva_tecnica_percentual"])),
            ("Minimo operacional", format_currency_brl(rubrica["valor_minimo_operacional"])),
            ("Disponivel operacional", format_currency_brl(rubrica["saldo_disponivel"])),
            ("Saldo residual", format_currency_brl(rubrica["saldo_residual"])),
            ("Indice comprometido", format_percent_brl(rubrica["percentual_comprometido"])),
            ("Percentual utilizado", format_percent_brl(rubrica["percentual_utilizado"])),
            ("Status financeiro", rubrica["status_financeiro"]),
            ("Risco", rubrica["risco"]),
            ("Encerrada", "Sim" if bool(rubrica["encerrada"]) else "Nao"),
        ],
        columns=["Campo", "Valor"],
    )
    with st.container(border=True):
        st.markdown(f"### Analise da rubrica: {rubrica['codigo']}")
        st.dataframe(
            detalhes,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Campo": st.column_config.TextColumn("Campo", width="medium"),
                "Valor": st.column_config.TextColumn("Valor", width="large"),
            },
        )

def cancelar_solicitacao(solicitacao_id, usuario_id):
    compra = query("""
    select c.id
    from compras c
    join solicitacoes_compra s on s.id = c.solicitacao_id
    where c.solicitacao_id=%s
    """, (solicitacao_id,))
    if len(compra) == 1:
        compra_id = int(compra.iloc[0]["id"])
        execute("delete from notas_fiscais where compra_id=%s", (compra_id,))
        execute("delete from compras where id=%s", (compra_id,))

    execute("update cotacoes set vencedora=false where solicitacao_id=%s", (solicitacao_id,))
    execute("update solicitacoes_compra set status='cancelado', autorizado=false, atualizado_em=now() where id=%s", (solicitacao_id,))
    execute("insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao) values (%s,'cancelado',%s,'Solicitação cancelada')", (solicitacao_id, usuario_id))

    sincronizar_orcamento()

def sincronizar_orcamento():
    execute("update rubricas set valor_reservado = 0, valor_utilizado = 0")
    execute("""
    update rubricas r
    set valor_reservado = totais.valor_total
    from (
        select rubrica_id, coalesce(sum(valor_estimado), 0) as valor_total
        from solicitacoes_compra
        where autorizado = true
          and status in ('em_andamento', 'cotado', 'aguardando_nota')
        group by rubrica_id
    ) totais
    where r.id = totais.rubrica_id
    """)
    execute("""
    update rubricas r
    set valor_utilizado = totais.valor_total
    from (
        select s.rubrica_id, coalesce(sum(c.valor_compra), 0) as valor_total
        from compras c
        join solicitacoes_compra s on s.id = c.solicitacao_id
        where s.status = 'finalizado'
        group by s.rubrica_id
    ) totais
    where r.id = totais.rubrica_id
    """)

try:
    ensure_permissions_schema()
    ensure_financial_governance_schema()
except psycopg2.Error as exc:
    st.error("Nao foi possivel preparar o banco de dados para iniciar o app.")
    st.caption("Confira se as tabelas foram criadas no Supabase e reinicie o app no Streamlit Cloud.")
    with st.expander("Detalhe tecnico"):
        st.code(str(exc))
    st.stop()

if "user" not in st.session_state:
    st.session_state.user = None

with st.sidebar:
    st.header("Acesso")
    st.caption(f"Versão: {APP_DEPLOY_VERSION}")
    if st.session_state.user is None:
        email = st.text_input("E-mail")
        senha = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            df = query("select * from usuarios_app where email=%s and ativo=true", (email,))
            if len(df) == 1 and check_password(senha, df.iloc[0]["senha_hash"]):
                st.session_state.user = df.iloc[0].to_dict()
                st.rerun()
            else:
                st.error("Login inválido.")
    else:
        st.write(f"Usuário: **{st.session_state.user['nome']}**")
        st.write(f"Papel: **{st.session_state.user['papel']}**")
        if st.button("Sair"):
            st.session_state.user = None
            st.rerun()

if st.session_state.user is None:
    st.info("Entre com usuário e senha para usar o sistema.")
    st.stop()

user = st.session_state.user
BASE_MENU_OPTIONS = [
    ("orcamento", "Orçamento"),
    ("nova_exigencia", "Nova exigência"),
    ("solicitacoes", "Solicitações"),
    ("cotacoes", "Cotações"),
    ("compra_nota", "Compra e nota fiscal"),
    ("itens_comprados", "Itens comprados"),
]
ADMIN_MENU_OPTIONS = BASE_MENU_OPTIONS + [("membros", "Membros")]

if user["papel"] == "admin":
    MENU_OPTIONS = ADMIN_MENU_OPTIONS
else:
    permissoes_usuario = set(user.get("permissoes") or [])
    MENU_OPTIONS = [item for item in BASE_MENU_OPTIONS if item[0] in permissoes_usuario]
    if not MENU_OPTIONS:
        MENU_OPTIONS = [("nova_exigencia", "Nova exigência")]

menu_labels = dict(MENU_OPTIONS)
menu_keys = [key for key, _ in MENU_OPTIONS]

if "menu_key" not in st.session_state or st.session_state.menu_key not in menu_keys:
    st.session_state.menu_key = menu_keys[0]

def selecionar_menu(menu_key):
    st.session_state.menu_key = menu_key

st.sidebar.markdown("### Módulo")
for menu_key, menu_label in MENU_OPTIONS:
    button_type = "primary" if st.session_state.menu_key == menu_key else "secondary"
    st.sidebar.button(
        menu_label,
        key=f"nav_{menu_key}",
        type=button_type,
        use_container_width=True,
        on_click=selecionar_menu,
        args=(menu_key,),
    )

menu = st.session_state.menu_key
titulo_pagina = menu_labels[menu]

st.markdown(
    f"""
    <div style="margin-top: -20px; margin-bottom: 20px;">
        <h2 style="margin-bottom: 0;">{titulo_pagina}</h2>
        <p style="color: gray; margin-top: 4px;">Módulo selecionado no menu lateral</p>
    </div>
    """,
    unsafe_allow_html=True
)

if menu == "orcamento":
    if st.button("Solicitar compra"):
        st.session_state.menu_key = "nova_exigencia"
        st.rerun()

    if user["papel"] in ["admin", "gerente"]:
        c_recalcular, c_responsaveis, c_reservar, c_remanejar, c_encerrar, c_historico = st.columns(6)
        if c_recalcular.button("Recalcular orçamento"):
            sincronizar_orcamento()
            st.success("Orçamento recalculado com base nas compras existentes.")
            st.rerun()
        if c_responsaveis.button("Atualizar responsáveis"):
            atualizar_responsaveis_dialog()
        if c_reservar.button("Reservar valor"):
            reservar_valor_dialog(user["id"])
        if c_remanejar.button("Remanejar saldo"):
            remanejar_saldo_dialog(user["id"])
        if c_encerrar.button("Encerrar rubrica"):
            encerrar_rubrica_dialog(user["id"])
        if c_historico.button("Histórico/Auditoria"):
            historico_orcamento_dialog()

    df = query("""
    select
      v.id,
      v.codigo,
      v.nome,
      coalesce(r.responsaveis, '') as responsaveis,
      v.tipo,
      v.valor_orcado,
      v.valor_reservado,
      v.valor_utilizado,
      v.reserva_tecnica,
      v.reserva_tecnica_percentual,
      v.valor_minimo_operacional,
      v.saldo_disponivel,
      v.saldo_residual,
      v.percentual_comprometido,
      v.percentual_utilizado,
      v.encerrada
    from vw_orcamento v
    join rubricas r on r.id = v.id
    order by v.codigo
    """)
    if len(df) == 0:
        st.info("Não há rubricas cadastradas no orçamento.")
        st.stop()

    df["status_financeiro"] = df.apply(financial_status, axis=1)
    df["risco"] = df["status_financeiro"].apply(status_alert_level)

    total_orcado = df.valor_orcado.sum()
    total_reservado = df.valor_reservado.sum()
    total_utilizado = df.valor_utilizado.sum()
    total_disponivel = df.saldo_disponivel.sum()
    saldo_residual_total = df.saldo_residual.sum()
    rubricas_criticas = df["status_financeiro"].isin(["Critico", "Residual", "Encerrado"]).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total orçado", format_currency_brl(total_orcado))
    c2.metric("Total reservado", format_currency_brl(total_reservado))
    c3.metric("Total utilizado", format_currency_brl(total_utilizado))
    c4, c5, c6 = st.columns(3)
    c4.metric("Disponível operacional", format_currency_brl(total_disponivel))
    c5.metric("Saldo residual", format_currency_brl(saldo_residual_total))
    c6.metric("Rubricas críticas", int(rubricas_criticas))

    alertas = df[df["status_financeiro"].isin(["Comprometido", "Critico", "Residual", "Encerrado"])].copy()
    if len(alertas):
        with st.expander("Alertas financeiros", expanded=True):
            for _, rubrica in alertas.iterrows():
                st.write(
                    f"{rubrica['codigo']} - {rubrica['nome']}: "
                    f"{rubrica['status_financeiro']} "
                    f"({format_currency_brl_markdown(rubrica['saldo_disponivel'])} operacional)"
                )

    df_orcamento = df.rename(columns={
        "codigo": "Código",
        "nome": "Rubrica",
        "responsaveis": "Responsável",
        "tipo": "Tipo",
        "valor_orcado": "Valor orçado",
        "valor_reservado": "Valor reservado",
        "valor_utilizado": "Valor utilizado",
        "reserva_tecnica": "Reserva técnica",
        "valor_minimo_operacional": "Mínimo operacional",
        "saldo_disponivel": "Disponível operacional",
        "saldo_residual": "Saldo residual",
        "percentual_comprometido": "Índice comprometido",
        "percentual_utilizado": "Percentual utilizado",
        "status_financeiro": "Status financeiro",
        "risco": "Risco",
    })
    df_orcamento["Índice comprometido"] = pd.to_numeric(df_orcamento["Índice comprometido"], errors="coerce").fillna(0)
    for coluna in [
        "Valor orçado",
        "Valor reservado",
        "Valor utilizado",
        "Reserva técnica",
        "Mínimo operacional",
        "Disponível operacional",
        "Saldo residual",
    ]:
        df_orcamento[coluna] = df_orcamento[coluna].apply(format_currency_brl)
    df_orcamento["Percentual utilizado"] = df_orcamento["Percentual utilizado"].apply(format_percent_brl)
    risco_labels = df_orcamento["Risco"].copy()
    df_orcamento["Risco"] = "●"
    colunas_orcamento = [
        "Código",
        "Rubrica",
        "Tipo",
        "Responsável",
        "Valor orçado",
        "Valor reservado",
        "Valor utilizado",
        "Reserva técnica",
        "Mínimo operacional",
        "Disponível operacional",
        "Saldo residual",
        "Índice comprometido",
        "Status financeiro",
        "Risco",
    ]
    df_orcamento_visual = df_orcamento[colunas_orcamento].style.apply(
        lambda coluna: [
            (
                f"color: {risk_color_css(risco_labels.loc[indice])}; "
                "font-size: 22px; font-weight: 700; text-align: center;"
            )
            for indice in coluna.index
        ],
        subset=["Risco"],
        axis=0,
    )
    st.caption("Clique em uma linha da tabela para abrir a visao de analise completa da rubrica abaixo.")
    evento_orcamento = st.dataframe(
        df_orcamento_visual,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Índice comprometido": st.column_config.ProgressColumn(
                "Índice comprometido",
                format="%.2f%%",
                min_value=0,
                max_value=100,
            ),
            "Risco": st.column_config.TextColumn("Risco", width="small"),
        },
    )
    selecao_orcamento = getattr(evento_orcamento, "selection", {})
    if isinstance(selecao_orcamento, dict):
        linhas_selecionadas = selecao_orcamento.get("rows", [])
    else:
        linhas_selecionadas = getattr(selecao_orcamento, "rows", [])
    if linhas_selecionadas:
        exibir_detalhe_rubrica(df.iloc[linhas_selecionadas[0]].to_dict())

    with st.expander("Parametros de governanca por rubrica"):
        rubrica_id = st.selectbox(
            "Rubrica",
            df["id"].tolist(),
            format_func=lambda item_id: f"{df.loc[df.id == item_id, 'codigo'].iloc[0]} - {df.loc[df.id == item_id, 'nome'].iloc[0]}",
            key="orcamento_parametros_rubrica",
        )
        rubrica = df.loc[df.id == rubrica_id].iloc[0]
        p1, p2 = st.columns(2)
        novo_minimo = p1.number_input(
            "Valor mínimo operacional",
            min_value=0.0,
            value=float(rubrica["valor_minimo_operacional"]),
            step=50.0,
        )
        nova_reserva = p2.number_input(
            "Reserva técnica (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(rubrica["reserva_tecnica_percentual"]),
            step=0.5,
        )
        if st.button("Salvar parâmetros da rubrica", type="primary"):
            execute(
                "update rubricas set valor_minimo_operacional=%s, reserva_tecnica_percentual=%s where id=%s",
                (Decimal(str(novo_minimo)), Decimal(str(nova_reserva)), int(rubrica_id)),
            )
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'parametros_governanca',0,%s)",
                (int(rubrica_id), user["id"], "Atualizacao de valor minimo operacional e reserva tecnica."),
            )
            st.success("Parâmetros atualizados.")
            st.rerun()

elif menu == "nova_exigencia":
    rubricas = query("select id, codigo || ' - ' || nome as label, saldo_disponivel from vw_orcamento where encerrada = false order by codigo")
    if len(rubricas) == 0:
        st.info("Não há rubricas abertas para novas solicitações.")
        st.stop()
    rubrica_label = st.selectbox("Rubrica/categoria", rubricas["label"])
    rubrica_id = int(rubricas.loc[rubricas["label"] == rubrica_label, "id"].iloc[0])
    saldo_atual = Decimal(str(rubricas.loc[rubricas["label"] == rubrica_label, "saldo_disponivel"].iloc[0]))
    st.caption(f"Disponível operacional: {format_currency_brl_markdown(saldo_atual)}")
    descricao = st.text_area("Descrição detalhada do item/serviço")
    quantidade = st.number_input("Quantidade", min_value=0.001, value=1.0)
    unidade = st.text_input("Unidade", value="un")
    valor_unitario_estimado = st.number_input("Valor unitário (R$)", min_value=0.0, value=0.0, key="nova_valor_unitario")
    valor_estimado = quantidade * valor_unitario_estimado
    st.text_input(
        "Valor total (R$)",
        value=format_currency_brl(valor_estimado),
        disabled=True,
        key=f"nova_valor_total_{quantidade}_{valor_unitario_estimado}_{valor_estimado:.2f}",
    )
    justificativa = st.text_area("Justificativa")
    if st.button("Enviar solicitação"):
        valor_estimado_decimal = Decimal(str(valor_estimado))
        excede_saldo, saldo_disponivel = excede_saldo_disponivel(rubrica_id, valor_estimado_decimal)
        if excede_saldo:
            st.error(
                "Solicitação não registrada. "
                f"O valor total ({format_currency_brl_markdown(valor_estimado_decimal)}) "
                f"supera o disponível operacional da rubrica ({format_currency_brl_markdown(saldo_disponivel)})."
            )
        else:
            execute("""
            insert into solicitacoes_compra (rubrica_id, solicitante_id, descricao, quantidade, unidade, valor_estimado, justificativa, status)
            values (%s,%s,%s,%s,%s,%s,%s,'solicitacao')
            """, (rubrica_id, user["id"], descricao, quantidade, unidade, valor_estimado, justificativa))
            st.success("Solicitação registrada.")

elif menu == "solicitacoes":
    df = query("""
    select s.id, r.codigo as rubrica, s.descricao, s.quantidade, s.valor_estimado as "Valor estimado", s.status, s.autorizado, s.criado_em
    from solicitacoes_compra s join rubricas r on r.id=s.rubrica_id
    where s.status not in ('finalizado','cancelado')
    order by s.id desc
    """)
    st.dataframe(
        df,
        use_container_width=True,
        column_config={
            "Valor estimado": st.column_config.NumberColumn("Valor estimado", format="R$ %.2f"),
        },
    )
    if user["papel"] in ["gerente", "admin"]:
        st.markdown("### Autorizar solicitação")
        if len(df) == 0:
            st.info("Não há solicitações ativas para autorizar ou cancelar.")
        else:
            sid = st.selectbox(
                "Solicitação",
                df["id"].tolist(),
                format_func=lambda x: f"#{x} - {df.loc[df.id == x, 'descricao'].iloc[0][:80]}",
                key="solicitacao_acao_id",
            )
            if st.button("Autorizar e colocar em andamento"):
                existe = query("""
                select id, rubrica_id, coalesce(valor_estimado, 0) as valor_estimado, autorizado
                from solicitacoes_compra
                where id=%s
                """, (sid,))
                if len(existe) != 1:
                    st.error("Solicitação não encontrada.")
                elif not bool(existe.iloc[0]["autorizado"]):
                    valor_autorizacao = Decimal(str(existe.iloc[0]["valor_estimado"]))
                    rubrica_autorizacao_id = int(existe.iloc[0]["rubrica_id"])
                    excede_saldo, saldo_disponivel = excede_saldo_disponivel(rubrica_autorizacao_id, valor_autorizacao)
                    if excede_saldo:
                        st.error(
                            "Solicitação não autorizada. "
                            f"O valor estimado ({format_currency_brl_markdown(valor_autorizacao)}) "
                            f"supera o disponível operacional da rubrica ({format_currency_brl_markdown(saldo_disponivel)})."
                        )
                    else:
                        execute("update solicitacoes_compra set autorizado=true, gerente_id=%s, autorizado_em=now(), status='em_andamento' where id=%s", (user["id"], sid))
                        execute("insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao) values (%s,'em_andamento',%s,'Autorizada pelo gerente')", (sid, user["id"]))
                        sincronizar_orcamento()
                        st.success("Solicitação autorizada.")
                        st.rerun()
                else:
                    st.info("Esta solicitação já estava autorizada.")
            if st.button("Cancelar solicitação"):
                cancelar_solicitacao(sid, user["id"])
                st.success("Solicitação cancelada e removida da lista.")
                st.rerun()

elif menu == "cotacoes":
    solicitacoes = query("select id, descricao, quantidade, valor_estimado from solicitacoes_compra where autorizado=true and status in ('em_andamento','cotado') order by id desc")
    if len(solicitacoes) == 0:
        st.warning("Não há solicitações autorizadas para cotação.")
    else:
        sid = st.selectbox("Solicitação", solicitacoes["id"].tolist(), format_func=lambda x: f"#{x} - {solicitacoes.loc[solicitacoes.id==x,'descricao'].iloc[0][:80]}")
        solicitacao = solicitacoes.loc[solicitacoes.id == sid].iloc[0]
        quantidade_solicitada = float(solicitacao["quantidade"])
        valor_estimado = float(solicitacao["valor_estimado"] or 0)
        valor_unitario_padrao = valor_estimado / quantidade_solicitada if quantidade_solicitada else 0.0
        st.number_input("Quantidade solicitada", min_value=0.001, value=quantidade_solicitada, disabled=True, key=f"cotacao_quantidade_solicitada_{sid}_{quantidade_solicitada}")
        ordem = st.selectbox("Cotação", [1,2,3], key=f"cotacao_ordem_{sid}")
        fornecedor = st.text_input("Fornecedor", key=f"cotacao_fornecedor_{sid}_{ordem}")
        cnpj = st.text_input("CNPJ/CPF", key=f"cotacao_cnpj_{sid}_{ordem}")
        contato = st.text_input("Telefone/E-mail", key=f"cotacao_contato_{sid}_{ordem}")
        valor_unit = st.number_input("Valor unitário", min_value=0.0, value=valor_unitario_padrao, key=f"cotacao_valor_unitario_{sid}_{ordem}")
        valor_total = quantidade_solicitada * valor_unit
        st.text_input("Valor total", value=format_currency_brl(valor_total), disabled=True, key=f"cotacao_valor_total_{sid}_{ordem}_{valor_total:.2f}")
        prazo = st.text_input("Prazo de entrega", key=f"cotacao_prazo_{sid}_{ordem}")
        pagamento = st.text_input("Forma de pagamento", key=f"cotacao_pagamento_{sid}_{ordem}")
        if st.button("Salvar cotação"):
            execute("""
            insert into cotacoes (solicitacao_id,ordem,fornecedor,cnpj_cpf,telefone_email,valor_unitario,valor_total,prazo_entrega,forma_pagamento)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (solicitacao_id,ordem) do update set fornecedor=excluded.fornecedor, cnpj_cpf=excluded.cnpj_cpf,
            telefone_email=excluded.telefone_email, valor_unitario=excluded.valor_unitario, valor_total=excluded.valor_total,
            prazo_entrega=excluded.prazo_entrega, forma_pagamento=excluded.forma_pagamento
            """, (sid, ordem, fornecedor, cnpj, contato, valor_unit, valor_total, prazo, pagamento))
            execute("update solicitacoes_compra set status='cotado' where id=%s", (sid,))
            st.success("Cotação salva.")
        st.dataframe(
            query('select ordem, fornecedor, valor_total as "Valor total", prazo_entrega, forma_pagamento, vencedora from cotacoes where solicitacao_id=%s order by ordem', (sid,)),
            use_container_width=True,
            column_config={
                "Valor total": st.column_config.NumberColumn("Valor total", format="R$ %.2f"),
            },
        )

elif menu == "compra_nota":
    solicitacoes_compra = query("""
    select id, descricao
    from solicitacoes_compra
    where autorizado=true and status in ('cotado','aguardando_nota')
    order by id desc
    """)
    if len(solicitacoes_compra) == 0:
        st.info("Não há solicitações pendentes para compra ou nota fiscal.")
        st.stop()
    sid = st.selectbox("Solicitação", solicitacoes_compra["id"].tolist(), format_func=lambda x: f"#{x} - {solicitacoes_compra.loc[solicitacoes_compra.id==x,'descricao'].iloc[0][:80]}")
    if st.button("Cancelar compra"):
        cancelar_solicitacao(sid, user["id"])
        st.success("Compra cancelada e solicitação removida dos registros ativos.")
        st.rerun()
    cotacoes_df = query('select id, ordem, fornecedor, valor_total as "Valor total", vencedora from cotacoes where solicitacao_id=%s order by ordem', (sid,))
    if len(cotacoes_df) == 0:
        st.warning("Não há cotações cadastradas para essa solicitação.")
        cotacao_id = None
    else:
        cotacoes_editadas = st.data_editor(
            cotacoes_df,
            use_container_width=True,
            hide_index=True,
            disabled=["id", "ordem", "fornecedor", "Valor total"],
            column_config={
                "Valor total": st.column_config.NumberColumn("Valor total", format="R$ %.2f"),
                "vencedora": st.column_config.CheckboxColumn("Vencedora"),
            },
            key=f"cotacoes_vencedora_{sid}",
        )
        vencedoras = cotacoes_editadas[cotacoes_editadas["vencedora"] == True]
        if len(vencedoras) == 1:
            cotacao_id = int(vencedoras.iloc[0]["id"])
        else:
            cotacao_id = None
            if len(vencedoras) > 1:
                st.error("Marque apenas uma cotação vencedora.")

    if st.button("Registrar compra"):
        if cotacao_id is None:
            st.error("Marque uma cotação vencedora na tabela.")
        else:
            df = query("select s.rubrica_id, c.valor_total from solicitacoes_compra s join cotacoes c on c.solicitacao_id=s.id where s.id=%s and c.id=%s", (sid, cotacao_id))
            if len(df) != 1:
                st.error("Cotação não encontrada para essa solicitação.")
            else:
                valor = Decimal(str(df.iloc[0]["valor_total"]))
                execute("update cotacoes set vencedora=false where solicitacao_id=%s", (sid,))
                execute("update cotacoes set vencedora=true where id=%s", (cotacao_id,))
                execute("insert into compras (solicitacao_id,cotacao_vencedora_id,valor_compra,comprador_id) values (%s,%s,%s,%s) on conflict (solicitacao_id) do update set cotacao_vencedora_id=excluded.cotacao_vencedora_id, valor_compra=excluded.valor_compra, comprador_id=excluded.comprador_id", (sid, cotacao_id, valor, user["id"]))
                execute("update solicitacoes_compra set status='aguardando_nota' where id=%s", (sid,))
                sincronizar_orcamento()
                st.success("Compra registrada. Orçamento atualizado e status: aguardando nota.")

    st.markdown("### Lançar nota fiscal")
    compra_df = query("""
    select c.id, c.valor_compra, co.fornecedor
    from compras c
    left join cotacoes co on co.id = c.cotacao_vencedora_id
    where c.solicitacao_id=%s
    """, (sid,))
    if len(compra_df) == 0:
        st.info("Registre a compra desta solicitação antes de lançar a nota fiscal.")
    else:
        compra_id = int(compra_df.iloc[0]["id"])
        valor_compra = float(compra_df.iloc[0]["valor_compra"])
        fornecedor_padrao = compra_df.iloc[0]["fornecedor"] or ""
        st.number_input("ID da compra", min_value=1, value=compra_id, disabled=True, key=f"nota_compra_id_{sid}_{compra_id}")
        numero_nf = st.text_input("Número da NF")
        fornecedor_nf = st.text_input("Fornecedor da NF", value=fornecedor_padrao)
        valor_nf = st.number_input("Valor da NF", min_value=0.0, value=valor_compra, key=f"nota_valor_nf_{compra_id}_{valor_compra:.2f}")
        data_nf = st.date_input("Data de emissão", value=date.today())
        if st.button("Consolidar nota e finalizar"):
            execute("insert into notas_fiscais (compra_id,numero_nf,fornecedor,valor_nf,data_emissao,lancado_por) values (%s,%s,%s,%s,%s,%s)", (compra_id, numero_nf, fornecedor_nf, valor_nf, data_nf, user["id"]))
            execute("update solicitacoes_compra set status='finalizado' where id=%s", (sid,))
            sincronizar_orcamento()
            st.success("Nota fiscal lançada. Compra finalizada.")

elif menu == "itens_comprados":
    df = query("""
    select
      s.id as "Solicitação",
      r.codigo as "Rubrica",
      r.nome as "Nome da rubrica",
      s.descricao as "Produto/serviço",
      s.quantidade as "Quantidade",
      c.valor_compra as "Valor da compra",
      co.fornecedor as "Fornecedor da cotação",
      nf.numero_nf as "Número da NF",
      nf.fornecedor as "Fornecedor da NF",
      nf.valor_nf as "Valor da NF",
      nf.data_emissao as "Data de emissão",
      nf.lancado_em as "Lançado em"
    from solicitacoes_compra s
    join rubricas r on r.id = s.rubrica_id
    join compras c on c.solicitacao_id = s.id
    left join cotacoes co on co.id = c.cotacao_vencedora_id
    left join notas_fiscais nf on nf.compra_id = c.id
    where s.status = 'finalizado'
    order by nf.lancado_em desc nulls last, c.comprado_em desc
    """)
    if len(df) == 0:
        st.info("Ainda não há itens comprados finalizados.")
    else:
        st.download_button(
            "Baixar planilha por rubrica",
            data=construir_planilha_itens_comprados(df),
            file_name=f"produtos_comprados_por_rubrica_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "Valor da compra": st.column_config.NumberColumn("Valor da compra", format="R$ %.2f"),
                "Valor da NF": st.column_config.NumberColumn("Valor da NF", format="R$ %.2f"),
            },
        )

elif menu == "membros":
    if user["papel"] != "admin":
        st.error("Acesso restrito ao administrador.")
        st.stop()

    paginas_permitidas = BASE_MENU_OPTIONS
    st.markdown("### Adicionar membro")
    nome = st.text_input("Nome", key="membro_nome")
    email = st.text_input("E-mail", key="membro_email")
    senha = st.text_input("Senha temporária", type="password", key="membro_senha")
    papel = st.selectbox("Papel", ["solicitante", "gerente", "compras", "admin"], key="membro_papel")
    permissoes = st.multiselect(
        "Páginas permitidas",
        [key for key, _ in paginas_permitidas],
        default=["nova_exigencia"],
        format_func=lambda key: dict(paginas_permitidas)[key],
        key="membro_permissoes",
    )

    if papel == "admin":
        permissoes = [key for key, _ in ADMIN_MENU_OPTIONS]

    if st.button("Adicionar membro"):
        if not nome or not email or not senha:
            st.error("Preencha nome, e-mail e senha.")
        else:
            execute("""
            insert into usuarios_app (nome,email,senha_hash,papel,permissoes,ativo)
            values (%s,%s,%s,%s,%s,true)
            on conflict (email) do update set
              nome=excluded.nome,
              senha_hash=excluded.senha_hash,
              papel=excluded.papel,
              permissoes=excluded.permissoes,
              ativo=true
            """, (nome, email, hash_password(senha), papel, permissoes))
            st.success("Membro adicionado ou atualizado.")

    st.markdown("### Editar membro")
    membros_edicao = query("""
    select nome, email, papel, permissoes, ativo
    from usuarios_app
    where ativo=true
    order by nome
    """)
    if len(membros_edicao) == 0:
        st.info("Nao ha membros ativos para editar.")
    else:
        email_editar = st.selectbox(
            "Membro",
            membros_edicao["email"].tolist(),
            format_func=lambda email: f"{membros_edicao.loc[membros_edicao.email == email, 'nome'].iloc[0]} ({email})",
            key="membro_editar_email",
        )
        membro_editar = membros_edicao.loc[membros_edicao.email == email_editar].iloc[0]
        permissoes_atuais = membro_editar["permissoes"] if isinstance(membro_editar["permissoes"], list) else []
        opcoes_papel = ["solicitante", "gerente", "compras", "admin"]
        papel_atual = membro_editar["papel"] if membro_editar["papel"] in opcoes_papel else "solicitante"

        chave_membro_edicao = email_editar.replace("@", "_").replace(".", "_")
        nome_editado = st.text_input("Nome", value=membro_editar["nome"], key=f"membro_editar_nome_{chave_membro_edicao}")
        papel_editado = st.selectbox(
            "Papel",
            opcoes_papel,
            index=opcoes_papel.index(papel_atual),
            key=f"membro_editar_papel_{chave_membro_edicao}",
        )
        opcoes_permissoes = [key for key, _ in paginas_permitidas]
        permissoes_validas = [permissao for permissao in permissoes_atuais if permissao in opcoes_permissoes]
        permissoes_editadas = st.multiselect(
            "Paginas permitidas",
            opcoes_permissoes,
            default=permissoes_validas,
            format_func=lambda key: dict(paginas_permitidas)[key],
            key=f"membro_editar_permissoes_{chave_membro_edicao}",
            disabled=papel_editado == "admin",
        )
        if papel_editado == "admin":
            permissoes_editadas = [key for key, _ in ADMIN_MENU_OPTIONS]
            st.caption("Administradores acessam todos os modulos.")

        if st.button("Salvar alteracoes do membro"):
            if not nome_editado.strip():
                st.error("Informe o nome do membro.")
            else:
                execute(
                    "update usuarios_app set nome=%s, papel=%s, permissoes=%s where email=%s",
                    (nome_editado.strip(), papel_editado, permissoes_editadas, email_editar),
                )
                if email_editar == user["email"]:
                    st.session_state.user["nome"] = nome_editado.strip()
                    st.session_state.user["papel"] = papel_editado
                    st.session_state.user["permissoes"] = permissoes_editadas
                st.success("Membro atualizado.")
                st.rerun()

    st.markdown("### Remover membro")
    membros_remocao = query("""
    select email, nome
    from usuarios_app
    where ativo=true
    order by nome
    """)
    if len(membros_remocao) == 0:
        st.info("Não há membros ativos para remover.")
    else:
        email_remover = st.selectbox(
            "Membro",
            membros_remocao["email"].tolist(),
            format_func=lambda email: f"{membros_remocao.loc[membros_remocao.email == email, 'nome'].iloc[0]} ({email})",
            key="membro_remover_email",
        )
        confirmar_remocao = st.checkbox("Confirmar remoção do membro selecionado", key="confirmar_remocao_membro")
        if st.button("Remover membro"):
            if email_remover == user["email"]:
                st.error("Você não pode remover o próprio usuário logado.")
            elif not confirmar_remocao:
                st.error("Marque a confirmação antes de remover.")
            else:
                execute("update usuarios_app set ativo=false where email=%s", (email_remover,))
                st.success("Membro removido do acesso.")
                st.rerun()

    st.markdown("### Membros cadastrados")
    membros = query("""
    select
      split_part(trim(nome), ' ', 1) as usuario,
      nome,
      email,
      papel,
      permissoes,
      ativo,
      criado_em
    from usuarios_app
    order by criado_em desc
    """)
    st.dataframe(membros, use_container_width=True)
