import os
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


load_dotenv(override=True)


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL nao definida.")
    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as exc:
        parsed = urlparse(database_url)
        host = parsed.hostname or "host nao identificado"
        raise RuntimeError(f"Nao foi possivel conectar ao banco. Host: {host}.") from exc


def query(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if cur.description:
                return pd.DataFrame(cur.fetchall())
            return pd.DataFrame()
    finally:
        conn.close()


def execute(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
    finally:
        conn.close()


def criar_schema_ia_operacional():
    execute("""
    create table if not exists public.alertas_ia (
        id bigint generated always as identity primary key,
        tipo text not null,
        titulo text not null,
        descricao text not null,
        gravidade text default 'media',
        origem text,
        tabela_origem text,
        registro_origem_id bigint,
        status text default 'pendente',
        sugestao_acao text,
        criado_em timestamp default now(),
        resolvido_em timestamp
    )
    """)
    execute("""
    create index if not exists idx_alertas_ia_status
        on public.alertas_ia(status)
    """)
    execute("""
    create index if not exists idx_alertas_ia_origem
        on public.alertas_ia(tipo, tabela_origem, registro_origem_id, status)
    """)
    execute("""
    create or replace view public.score_risco_rubrica as
    select
        r.id,
        r.codigo,
        r.nome,
        r.valor_orcado,
        r.valor_reservado,
        r.valor_utilizado,
        (coalesce(r.valor_reservado, 0) + coalesce(r.valor_utilizado, 0)) as valor_comprometido,
        coalesce(sum(s.valor_estimado) filter (
            where s.status <> 'cancelado'
        ), 0) as valor_solicitado,
        round(
            (
                (coalesce(r.valor_reservado, 0) + coalesce(r.valor_utilizado, 0))
                / nullif(r.valor_orcado, 0)
            ) * 100,
            2
        ) as percentual_comprometido
    from public.rubricas r
    left join public.solicitacoes_compra s on s.rubrica_id = r.id
    where r.ativo = true
    group by
        r.id,
        r.codigo,
        r.nome,
        r.valor_orcado,
        r.valor_reservado,
        r.valor_utilizado
    """)
    execute("alter table if exists public.alertas_ia enable row level security")
    execute("revoke all privileges on public.alertas_ia from anon")
    execute("revoke all privileges on public.alertas_ia from authenticated")
    execute("revoke all privileges on public.score_risco_rubrica from anon")
    execute("revoke all privileges on public.score_risco_rubrica from authenticated")


def decimal_value(value):
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def registrar_alerta(alerta):
    existente = query(
        """
        select id
        from alertas_ia
        where tipo=%s
          and coalesce(tabela_origem, '') = coalesce(%s, '')
          and coalesce(registro_origem_id, -1) = coalesce(%s, -1)
          and status='pendente'
        limit 1
        """,
        (
            alerta.get("tipo"),
            alerta.get("tabela_origem"),
            alerta.get("registro_origem_id"),
        ),
    )
    params = (
        alerta.get("tipo"),
        alerta.get("titulo"),
        alerta.get("descricao"),
        alerta.get("gravidade", "media"),
        alerta.get("origem"),
        alerta.get("tabela_origem"),
        alerta.get("registro_origem_id"),
        alerta.get("sugestao_acao"),
    )
    if len(existente):
        execute(
            """
            update alertas_ia
            set titulo=%s,
                descricao=%s,
                gravidade=%s,
                origem=%s,
                sugestao_acao=%s,
                criado_em=now()
            where id=%s
            """,
            (
                alerta.get("titulo"),
                alerta.get("descricao"),
                alerta.get("gravidade", "media"),
                alerta.get("origem"),
                alerta.get("sugestao_acao"),
                int(existente.iloc[0]["id"]),
            ),
        )
        return "atualizado"

    execute(
        """
        insert into alertas_ia
            (tipo, titulo, descricao, gravidade, origem, tabela_origem, registro_origem_id, sugestao_acao)
        values (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        params,
    )
    return "criado"


def verificar_rubrica_critica(rubrica):
    valor_orcado = decimal_value(rubrica.get("valor_orcado"))
    valor_comprometido = decimal_value(rubrica.get("valor_comprometido"))
    if valor_orcado <= 0:
        return None

    percentual = (valor_comprometido / valor_orcado) * Decimal("100")
    if percentual >= Decimal("80"):
        return {
            "tipo": "rubrica_critica",
            "titulo": f"Rubrica critica: {rubrica['codigo']}",
            "descricao": (
                f"A rubrica {rubrica['nome']} ja comprometeu "
                f"{percentual:.2f}% do orcamento."
            ),
            "gravidade": "alta" if percentual >= Decimal("90") else "media",
            "origem": "score_risco_rubrica",
            "tabela_origem": "rubricas",
            "registro_origem_id": int(rubrica["id"]),
            "sugestao_acao": "Revisar solicitacoes pendentes antes de aprovar novas compras.",
        }
    return None


def analisar_rubricas():
    score = query("select * from score_risco_rubrica order by percentual_comprometido desc nulls last")
    alertas = []
    for _, rubrica in score.iterrows():
        alerta = verificar_rubrica_critica(rubrica)
        if alerta:
            alertas.append(alerta)
    return alertas


def analisar_solicitacoes():
    return []


def analisar_cotacoes():
    atrasadas = query("""
    select id, descricao, criado_em
    from solicitacoes_compra
    where status in ('em_andamento', 'cotado')
      and criado_em < now() - interval '7 days'
    order by criado_em
    """)
    alertas = []
    for _, row in atrasadas.iterrows():
        alertas.append({
            "tipo": "cotacao_atrasada",
            "titulo": f"Cotacao atrasada: solicitacao #{row['id']}",
            "descricao": f"A solicitacao {row['descricao']} esta parada em cotacao ha mais de 7 dias.",
            "gravidade": "media",
            "origem": "solicitacoes_compra",
            "tabela_origem": "solicitacoes_compra",
            "registro_origem_id": int(row["id"]),
            "sugestao_acao": "Verificar fornecedores pendentes e atualizar a cotacao.",
        })
    return alertas


def analisar_compras():
    if len(query("""
        select 1
        from information_schema.views
        where table_schema='public' and table_name='vw_auditoria_itens_projeto'
        limit 1
    """)) == 0:
        return []

    auditoria = query("""
    select *
    from vw_auditoria_itens_projeto
    where status_auditoria <> 'OK'
    order by solicitacao_id, descricao
    """)
    alertas = []
    for _, row in auditoria.iterrows():
        status = str(row["status_auditoria"])
        tipo = "risco_orcamentario"
        gravidade = "media"
        sugestao = "Abrir a auditoria e corrigir a pendencia."

        if "valor cotado" in status or "valor da NF" in status:
            tipo = "valor_divergente"
            gravidade = "alta"
            sugestao = "Voltar o item para cotacao ou ajustar o valor solicitado quando a NF estiver correta."
        elif "nota fiscal" in status or "NF sem" in status:
            tipo = "nota_fiscal_pendente"
        elif "patrimonio" in status:
            tipo = "item_sem_patrimonio"
        elif "estoque" in status:
            tipo = "item_sem_estoque"

        alertas.append({
            "tipo": tipo,
            "titulo": f"Pendencia de auditoria: solicitacao #{row['solicitacao_id']}",
            "descricao": f"{row['descricao']}: {status}",
            "gravidade": gravidade,
            "origem": "vw_auditoria_itens_projeto",
            "tabela_origem": "solicitacoes_compra",
            "registro_origem_id": int(row["solicitacao_id"]),
            "sugestao_acao": sugestao,
        })
    return alertas


def analisar_patrimonio():
    return []


def gerar_alertas_ia():
    criar_schema_ia_operacional()
    alertas = []
    for analisador in (
        analisar_rubricas,
        analisar_solicitacoes,
        analisar_cotacoes,
        analisar_compras,
        analisar_patrimonio,
    ):
        alertas.extend(analisador())

    resultado = {"criados": 0, "atualizados": 0, "total": len(alertas)}
    for alerta in alertas:
        status = registrar_alerta(alerta)
        if status == "criado":
            resultado["criados"] += 1
        else:
            resultado["atualizados"] += 1
    return resultado


def carregar_alertas(status="pendente"):
    if status == "todos":
        return query("select * from alertas_ia order by criado_em desc")
    return query("select * from alertas_ia where status=%s order by criado_em desc", (status,))


def carregar_score_risco_rubrica():
    return query("select * from score_risco_rubrica order by percentual_comprometido desc nulls last")


def marcar_alerta_resolvido(alerta_id):
    execute(
        """
        update alertas_ia
        set status='resolvido',
            resolvido_em=now()
        where id=%s
        """,
        (alerta_id,),
    )
