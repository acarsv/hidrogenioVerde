create extension if not exists pgcrypto;

create table if not exists pedidos (
  id bigserial primary key,
  rubrica_id bigint not null references rubricas(id),
  solicitante_id uuid references usuarios_app(id),
  descricao text,
  justificativa text,
  status text not null default 'rascunho',
  solicitacao_id bigint references solicitacoes_compra(id) on delete set null,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create table if not exists pedido_rascunho_itens (
  id uuid primary key default gen_random_uuid(),
  pedido_id bigint not null references pedidos(id) on delete cascade,
  rubrica_id bigint references rubricas(id),
  descricao text not null,
  tipo_item text not null check (tipo_item in ('permanente', 'consumo', 'servico')),
  quantidade numeric(12,2) not null default 1,
  valor_unitario numeric(14,2) not null default 0,
  valor_total numeric(14,2) generated always as (quantidade * valor_unitario) stored,
  observacoes text,
  created_at timestamptz not null default now()
);

create index if not exists idx_pedidos_status on pedidos(status);
create index if not exists idx_pedidos_rubrica_id on pedidos(rubrica_id);
create index if not exists idx_pedido_rascunho_itens_pedido_id on pedido_rascunho_itens(pedido_id);
