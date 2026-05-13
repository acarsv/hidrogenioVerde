-- FASE 3 - Auditoria do projeto.
-- A origem rastreavel do item no modelo atual e pedido_itens.

create or replace view vw_auditoria_itens_projeto as
with cotacao_resumo as (
    select
        ci.pedido_item_id,
        count(*) as total_cotacoes,
        count(*) filter (where ci.vencedor = true) as total_vencedoras,
        max(c.fornecedor) filter (where ci.vencedor = true) as fornecedor_vencedor,
        sum(ci.valor_total) filter (where ci.vencedor = true) as valor_cotado_vencedor
    from cotacao_itens ci
    join cotacoes c on c.id = ci.cotacao_id
    where ci.pedido_item_id is not null
    group by ci.pedido_item_id
),
nota_resumo as (
    select
        nfi.pedido_item_id,
        count(nfi.id) as total_itens_nf,
        sum(nfi.valor_total) as valor_total_nf_item,
        string_agg(distinct nf.numero_nf, ', ') as notas_fiscais,
        string_agg(distinct nf.fornecedor, ', ') as fornecedores_nf,
        bool_or(nf.arquivo_url is not null and trim(nf.arquivo_url) <> '') as tem_arquivo_nf
    from nota_fiscal_itens nfi
    join notas_fiscais nf on nf.id = nfi.nota_fiscal_id
    where nfi.pedido_item_id is not null
    group by nfi.pedido_item_id
),
destino_resumo as (
    select
        nfi.pedido_item_id,
        max(p.id::text)::uuid as patrimonio_id,
        max(ec.id::text)::uuid as estoque_id,
        max(ats.id::text)::uuid as atesto_id
    from nota_fiscal_itens nfi
    left join patrimonio p on p.nota_fiscal_item_id = nfi.id
    left join estoque_consumo ec on ec.nota_fiscal_item_id = nfi.id
    left join atesto_servico ats on ats.nota_fiscal_item_id = nfi.id
    where nfi.pedido_item_id is not null
    group by nfi.pedido_item_id
)
select
    pi.id as pedido_item_id,
    s.id as solicitacao_id,
    r.id as rubrica_id,
    r.codigo as rubrica_codigo,
    r.nome as rubrica_nome,
    r.valor_orcado as rubrica_saldo_inicial,
    r.valor_reservado as rubrica_valor_reservado,
    r.valor_utilizado as rubrica_valor_utilizado,
    (
        r.valor_orcado
        - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
        - r.valor_reservado
        - r.valor_utilizado
    ) as rubrica_saldo_restante,

    pi.descricao,
    pi.tipo_item,
    pi.quantidade,
    pi.valor_total as valor_solicitado,

    s.status as status_solicitacao,
    s.autorizado,
    case when s.autorizado then pi.valor_total else 0 end as valor_autorizado,

    coalesce(cr.total_cotacoes, 0) as total_cotacoes,
    coalesce(cr.total_vencedoras, 0) as total_vencedoras,
    cr.fornecedor_vencedor,
    coalesce(cr.valor_cotado_vencedor, 0) as valor_cotado_vencedor,

    coalesce(nr.total_itens_nf, 0) as total_itens_nf,
    nr.notas_fiscais,
    nr.fornecedores_nf,
    coalesce(nr.valor_total_nf_item, 0) as valor_nf_item,
    coalesce(nr.tem_arquivo_nf, false) as tem_arquivo_nf,

    dr.patrimonio_id,
    dr.estoque_id,
    dr.atesto_id,

    case
        when pi.descricao is null or trim(pi.descricao) = ''
            then 'ERRO: item sem descricao'

        when pi.tipo_item not in ('permanente', 'consumo', 'servico')
            then 'ERRO: tipo de item invalido'

        when pi.valor_total <= 0
            then 'ERRO: item sem valor'

        when s.id is null
            then 'ERRO: item sem solicitacao'

        when coalesce(cr.total_cotacoes, 0) = 0
            then 'PENDENTE: item sem cotacao'

        when coalesce(cr.total_vencedoras, 0) = 0
            then 'PENDENTE: item sem fornecedor vencedor'

        when coalesce(cr.total_vencedoras, 0) > 1
            then 'ERRO: item com mais de um vencedor'

        when abs(coalesce(cr.valor_cotado_vencedor, 0) - pi.valor_total) > 0.01
            then 'ALERTA: valor cotado diverge do solicitado'

        when coalesce(nr.total_itens_nf, 0) = 0
            then 'PENDENTE: item sem nota fiscal'

        when abs(coalesce(nr.valor_total_nf_item, 0) - coalesce(cr.valor_cotado_vencedor, 0)) > 0.01
            then 'ERRO: valor da NF diverge da cotacao vencedora'

        when nr.fornecedores_nf is distinct from cr.fornecedor_vencedor
            then 'ERRO: fornecedor da NF diverge do vencedor'

        when coalesce(nr.tem_arquivo_nf, false) = false
            then 'PENDENTE: NF sem local/link no Drive'

        when pi.tipo_item = 'permanente' and dr.patrimonio_id is null
            then 'PENDENTE: permanente sem patrimonio'

        when pi.tipo_item = 'consumo' and dr.estoque_id is null
            then 'PENDENTE: consumo sem estoque'

        when pi.tipo_item = 'servico' and dr.atesto_id is null
            then 'PENDENTE: servico sem atesto'

        else 'OK'
    end as status_auditoria
from pedido_itens pi
join solicitacoes_compra s on s.id = pi.pedido_id
join rubricas r on r.id = pi.rubrica_id
left join cotacao_resumo cr on cr.pedido_item_id = pi.id
left join nota_resumo nr on nr.pedido_item_id = pi.id
left join destino_resumo dr on dr.pedido_item_id = pi.id;
