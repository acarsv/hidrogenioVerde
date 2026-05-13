-- FASE 2 - Destino final do item da nota fiscal.
-- Regra:
-- permanente -> patrimonio
-- consumo    -> estoque_consumo
-- servico    -> atesto_servico

create extension if not exists pgcrypto;

create table if not exists patrimonio (
    id uuid primary key default gen_random_uuid(),

    nota_fiscal_item_id uuid not null unique
        references nota_fiscal_itens(id)
        on delete restrict,

    numero_patrimonio text,
    localizacao text,
    responsavel text,
    estado text default 'ativo',

    observacoes text,
    created_at timestamp with time zone default now()
);

create table if not exists estoque_consumo (
    id uuid primary key default gen_random_uuid(),

    nota_fiscal_item_id uuid not null unique
        references nota_fiscal_itens(id)
        on delete restrict,

    quantidade_entrada numeric(12,2) not null,
    quantidade_disponivel numeric(12,2) not null,

    unidade text,
    local_armazenamento text,
    responsavel text,

    observacoes text,
    created_at timestamp with time zone default now()
);

create table if not exists atesto_servico (
    id uuid primary key default gen_random_uuid(),

    nota_fiscal_item_id uuid not null unique
        references nota_fiscal_itens(id)
        on delete restrict,

    descricao_execucao text not null,
    responsavel_atesto text,
    data_atesto date default current_date,

    documento_comprovacao_url text,
    observacoes text,

    created_at timestamp with time zone default now()
);

create index if not exists idx_patrimonio_nota_fiscal_item_id
    on patrimonio(nota_fiscal_item_id);

create index if not exists idx_estoque_consumo_nota_fiscal_item_id
    on estoque_consumo(nota_fiscal_item_id);

create index if not exists idx_atesto_servico_nota_fiscal_item_id
    on atesto_servico(nota_fiscal_item_id);
