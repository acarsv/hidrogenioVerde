-- FASE 1 - Estrutura base de rastreabilidade por item.
-- Migração incremental adaptada ao esquema atual do app:
-- solicitacoes_compra.id, rubricas.id, cotacoes.id e notas_fiscais.id usam bigint.
-- As tabelas cotacoes e notas_fiscais ja existem, entao esta migracao preserva
-- os dados atuais e adiciona apenas colunas auxiliares quando necessario.

create extension if not exists pgcrypto;

create table if not exists solicitacao_itens (
    id uuid primary key default gen_random_uuid(),

    solicitacao_id bigint not null
        references solicitacoes_compra(id)
        on delete cascade,

    rubrica_id bigint
        references rubricas(id),

    descricao text not null,

    tipo_item text not null check (
        tipo_item in ('permanente', 'consumo', 'servico')
    ),

    quantidade numeric(12,2) not null default 1,

    valor_unitario numeric(14,2) not null default 0,

    valor_total numeric(14,2) generated always as
        (quantidade * valor_unitario) stored,

    status text default 'solicitado',

    observacoes text,

    created_at timestamp with time zone default now()
);

alter table cotacoes
    add column if not exists numero_cotacao text;

alter table cotacoes
    add column if not exists data_cotacao date default current_date;

alter table cotacoes
    add column if not exists observacoes text;

create table if not exists cotacao_itens (
    id uuid primary key default gen_random_uuid(),

    cotacao_id bigint not null
        references cotacoes(id)
        on delete cascade,

    solicitacao_item_id uuid not null
        references solicitacao_itens(id)
        on delete cascade,

    quantidade numeric(12,2) not null default 1,

    valor_unitario numeric(14,2) not null default 0,

    valor_total numeric(14,2) generated always as
        (quantidade * valor_unitario) stored,

    vencedor boolean default false,

    observacoes text,

    created_at timestamp with time zone default now()
);

alter table notas_fiscais
    add column if not exists solicitacao_id bigint
        references solicitacoes_compra(id)
        on delete set null;

alter table notas_fiscais
    add column if not exists observacoes text;

alter table notas_fiscais
    add column if not exists valor_total numeric(14,2) generated always as
        (valor_nf) stored;

create table if not exists nota_fiscal_itens (
    id uuid primary key default gen_random_uuid(),

    nota_fiscal_id bigint not null
        references notas_fiscais(id)
        on delete cascade,

    solicitacao_item_id uuid not null
        references solicitacao_itens(id)
        on delete restrict,

    descricao text not null,

    tipo_item text not null check (
        tipo_item in ('permanente', 'consumo', 'servico')
    ),

    quantidade numeric(12,2) not null default 1,

    valor_unitario numeric(14,2) not null default 0,

    valor_total numeric(14,2) generated always as
        (quantidade * valor_unitario) stored,

    observacoes text,

    created_at timestamp with time zone default now()
);

create or replace view vw_conferencia_notas_fiscais as
select
    nf.id,
    nf.numero_nf,
    nf.fornecedor,
    nf.valor_total as valor_nota,
    coalesce(sum(i.valor_total), 0) as valor_itens,
    nf.valor_total - coalesce(sum(i.valor_total), 0) as diferenca,
    case
        when nf.valor_total = coalesce(sum(i.valor_total), 0)
        then 'OK'
        else 'DIVERGENTE'
    end as status_conferencia
from notas_fiscais nf
left join nota_fiscal_itens i
    on i.nota_fiscal_id = nf.id
group by nf.id, nf.numero_nf, nf.fornecedor, nf.valor_total;
