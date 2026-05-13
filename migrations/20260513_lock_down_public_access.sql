-- Bloqueia acesso direto via API publica do Supabase.
-- O app Streamlit continua acessando pelo backend com DATABASE_URL/postgres.

begin;

alter table if exists public.usuarios_app enable row level security;
alter table if exists public.rubricas enable row level security;
alter table if exists public.solicitacoes_compra enable row level security;
alter table if exists public.pedido_itens enable row level security;
alter table if exists public.solicitacao_itens enable row level security;
alter table if exists public.cotacoes enable row level security;
alter table if exists public.cotacao_itens enable row level security;
alter table if exists public.compras enable row level security;
alter table if exists public.notas_fiscais enable row level security;
alter table if exists public.nota_fiscal_itens enable row level security;
alter table if exists public.patrimonio enable row level security;
alter table if exists public.estoque_consumo enable row level security;
alter table if exists public.atesto_servico enable row level security;
alter table if exists public.historico_status enable row level security;
alter table if exists public.movimentacoes_orcamento enable row level security;

revoke all privileges on schema public from anon;
revoke all privileges on schema public from authenticated;

revoke all privileges on all tables in schema public from anon;
revoke all privileges on all tables in schema public from authenticated;

revoke all privileges on all sequences in schema public from anon;
revoke all privileges on all sequences in schema public from authenticated;

revoke all privileges on all functions in schema public from anon;
revoke all privileges on all functions in schema public from authenticated;

alter default privileges in schema public revoke all on tables from anon;
alter default privileges in schema public revoke all on tables from authenticated;
alter default privileges in schema public revoke all on sequences from anon;
alter default privileges in schema public revoke all on sequences from authenticated;
alter default privileges in schema public revoke all on functions from anon;
alter default privileges in schema public revoke all on functions from authenticated;

commit;
