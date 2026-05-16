-- Modulo: IA Operacional e Auditoria de Gargalos

begin;

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
);

create index if not exists idx_alertas_ia_status
    on public.alertas_ia(status);

create index if not exists idx_alertas_ia_origem
    on public.alertas_ia(tipo, tabela_origem, registro_origem_id, status);

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
    r.valor_utilizado;

alter table if exists public.alertas_ia enable row level security;

revoke all privileges on public.alertas_ia from anon;
revoke all privileges on public.alertas_ia from authenticated;
revoke all privileges on public.score_risco_rubrica from anon;
revoke all privileges on public.score_risco_rubrica from authenticated;

commit;
