-- FASE 3B - Integridade documental da nota fiscal.
-- Uma NF deve existir uma unica vez por numero + fornecedor.
-- Os itens do documento ficam em nota_fiscal_itens.

alter table notas_fiscais
    alter column compra_id drop not null;

do $$
declare
    grupo record;
    manter_id bigint;
    remover_ids bigint[];
begin
    for grupo in
        select
            lower(trim(numero_nf)) as numero_key,
            lower(trim(fornecedor)) as fornecedor_key,
            array_agg(id order by id) as ids,
            sum(valor_nf) as valor_nf_total,
            count(distinct compra_id) as total_compras,
            count(distinct solicitacao_id) as total_solicitacoes
        from notas_fiscais
        where numero_nf is not null
          and trim(numero_nf) <> ''
          and fornecedor is not null
          and trim(fornecedor) <> ''
        group by lower(trim(numero_nf)), lower(trim(fornecedor))
        having count(*) > 1
    loop
        manter_id := grupo.ids[1];
        remover_ids := grupo.ids[2:array_length(grupo.ids, 1)];

        update nota_fiscal_itens
        set nota_fiscal_id = manter_id
        where nota_fiscal_id = any(remover_ids);

        update notas_fiscais
        set valor_nf = grupo.valor_nf_total,
            compra_id = case when grupo.total_compras > 1 then null else compra_id end,
            solicitacao_id = case when grupo.total_solicitacoes > 1 then null else solicitacao_id end
        where id = manter_id;

        delete from notas_fiscais
        where id = any(remover_ids);
    end loop;
end $$;

create unique index if not exists idx_notas_fiscais_documento_unico
    on notas_fiscais (lower(trim(numero_nf)), lower(trim(fornecedor)))
    where numero_nf is not null
      and trim(numero_nf) <> ''
      and fornecedor is not null
      and trim(fornecedor) <> '';
