-- Banco: HidrogenioVerde - controle de orçamento, solicitações, cotações e compras
-- Execute no Supabase: SQL Editor > New Query > Run

create extension if not exists pgcrypto;

create type papel_usuario as enum ('solicitante','gerente','compras','admin');
create type status_compra as enum ('solicitacao','em_andamento','cotado','comprado','aguardando_nota','finalizado','cancelado');
create type tipo_rubrica as enum ('material_consumo','material_permanente','servico_pf');

create table if not exists usuarios_app (
  id uuid primary key default gen_random_uuid(),
  nome text not null,
  email text unique not null,
  senha_hash text not null,
  papel papel_usuario not null default 'solicitante',
  ativo boolean not null default true,
  criado_em timestamptz not null default now()
);

create table if not exists rubricas (
  id bigserial primary key,
  codigo text unique not null,
  nome text not null,
  tipo tipo_rubrica not null,
  valor_orcado numeric(14,2) not null check (valor_orcado >= 0),
  valor_reservado numeric(14,2) not null default 0,
  valor_utilizado numeric(14,2) not null default 0,
  responsaveis text,
  ativo boolean not null default true
);

create table if not exists solicitacoes_compra (
  id bigserial primary key,
  numero text unique,
  rubrica_id bigint not null references rubricas(id),
  solicitante_id uuid references usuarios_app(id),
  gerente_id uuid references usuarios_app(id),
  descricao text not null,
  categoria text,
  quantidade numeric(14,3) not null default 1,
  unidade text default 'un',
  valor_estimado numeric(14,2),
  justificativa text,
  status status_compra not null default 'solicitacao',
  autorizado boolean not null default false,
  autorizado_em timestamptz,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create table if not exists cotacoes (
  id bigserial primary key,
  solicitacao_id bigint not null references solicitacoes_compra(id) on delete cascade,
  ordem smallint not null check (ordem between 1 and 3),
  fornecedor text not null,
  cnpj_cpf text,
  telefone_email text,
  valor_unitario numeric(14,2) not null,
  valor_total numeric(14,2) not null,
  prazo_entrega text,
  forma_pagamento text,
  arquivo_url text,
  vencedora boolean not null default false,
  criado_em timestamptz not null default now(),
  unique(solicitacao_id, ordem)
);

create table if not exists compras (
  id bigserial primary key,
  solicitacao_id bigint not null unique references solicitacoes_compra(id),
  cotacao_vencedora_id bigint references cotacoes(id),
  valor_compra numeric(14,2) not null,
  comprado_em timestamptz not null default now(),
  comprador_id uuid references usuarios_app(id),
  observacao text
);

create table if not exists notas_fiscais (
  id bigserial primary key,
  compra_id bigint not null references compras(id),
  numero_nf text,
  fornecedor text,
  valor_nf numeric(14,2) not null,
  data_emissao date,
  arquivo_url text,
  lancado_por uuid references usuarios_app(id),
  lancado_em timestamptz not null default now()
);

create table if not exists historico_status (
  id bigserial primary key,
  solicitacao_id bigint not null references solicitacoes_compra(id) on delete cascade,
  status_anterior status_compra,
  status_novo status_compra not null,
  usuario_id uuid references usuarios_app(id),
  observacao text,
  criado_em timestamptz not null default now()
);

create or replace view vw_orcamento as
select
  r.id,
  r.codigo,
  r.nome,
  r.responsaveis,
  r.tipo,
  r.valor_orcado,
  r.valor_reservado,
  r.valor_utilizado,
  (r.valor_orcado - r.valor_reservado - r.valor_utilizado) as saldo_disponivel,
  case when r.valor_orcado > 0 then round((r.valor_utilizado * 100.0 / r.valor_orcado),2) else 0 end as percentual_utilizado
from rubricas r
where r.ativo = true;

create or replace function atualizar_timestamp()
returns trigger as $$
begin
  new.atualizado_em = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_solicitacoes_timestamp on solicitacoes_compra;
create trigger trg_solicitacoes_timestamp before update on solicitacoes_compra
for each row execute function atualizar_timestamp();
