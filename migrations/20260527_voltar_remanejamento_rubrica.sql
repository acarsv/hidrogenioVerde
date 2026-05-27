alter table if exists public.movimentacoes_orcamento
  add column if not exists remanejamento_id text;

alter table if exists public.movimentacoes_orcamento
  add column if not exists estornado_em timestamptz;

alter table if exists public.movimentacoes_orcamento
  add column if not exists estornado_por uuid references public.usuarios_app(id);
