-- FASE 1B - Origem rastreavel do item no pedido/requerimento.
-- O app atual nao possui tabela "pedidos"; a tabela solicitacoes_compra
-- representa o pedido inicial enquanto status='solicitacao' e autorizado=false.
-- Por isso, pedido_itens nasce vinculado a solicitacoes_compra.
--
-- Nao remove solicitacao_itens criada na FASE 1. Ela fica preservada para
-- compatibilidade ate a adaptacao completa do Streamlit.

create extension if not exists pgcrypto;

create table if not exists pedido_itens (
    id uuid primary key default gen_random_uuid(),

    pedido_id bigint not null
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

    status text not null default 'requerido',

    observacoes text,

    created_at timestamp with time zone default now()
);

alter table cotacao_itens
    add column if not exists pedido_item_id uuid;

alter table nota_fiscal_itens
    add column if not exists pedido_item_id uuid;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'fk_cotacao_itens_pedido_item'
    ) then
        alter table cotacao_itens
            add constraint fk_cotacao_itens_pedido_item
            foreign key (pedido_item_id)
            references pedido_itens(id)
            on delete restrict;
    end if;
end $$;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'fk_nota_fiscal_itens_pedido_item'
    ) then
        alter table nota_fiscal_itens
            add constraint fk_nota_fiscal_itens_pedido_item
            foreign key (pedido_item_id)
            references pedido_itens(id)
            on delete restrict;
    end if;
end $$;

create index if not exists idx_pedido_itens_pedido_id
    on pedido_itens(pedido_id);

create index if not exists idx_cotacao_itens_pedido_item_id
    on cotacao_itens(pedido_item_id);

create index if not exists idx_nota_fiscal_itens_pedido_item_id
    on nota_fiscal_itens(pedido_item_id);
