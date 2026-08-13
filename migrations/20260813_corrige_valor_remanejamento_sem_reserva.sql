do $$
declare
  remanejamento record;
  valor_informado numeric(14,2);
  diferenca numeric(14,2);
begin
  for remanejamento in
    select
      saida.remanejamento_id,
      saida.rubrica_id as origem_id,
      entrada.rubrica_id as destino_id,
      saida.valor as valor_registrado,
      substring(
        saida.justificativa
        from 'Valor total informado: R\$ ([0-9.]+,[0-9]{2})'
      ) as valor_informado_texto
    from movimentacoes_orcamento saida
    join movimentacoes_orcamento entrada
      on entrada.remanejamento_id = saida.remanejamento_id
     and entrada.operacao = 'remanejamento_entrada'
    where saida.operacao = 'remanejamento_saida'
      and saida.remanejamento_id is not null
      and saida.estornado_em is null
      and entrada.estornado_em is null
  loop
    if remanejamento.valor_informado_texto is not null then
      valor_informado := replace(
        replace(remanejamento.valor_informado_texto, '.', ''), ',', '.'
      )::numeric(14,2);
      diferenca := remanejamento.valor_registrado - valor_informado;
      if diferenca <> 0 then
        update rubricas
        set valor_orcado = valor_orcado + diferenca
        where id = remanejamento.origem_id;

        update rubricas
        set valor_orcado = valor_orcado - diferenca
        where id = remanejamento.destino_id;

        update movimentacoes_orcamento movimento
        set valor = valor_informado
        where movimento.remanejamento_id = remanejamento.remanejamento_id
          and movimento.operacao in ('remanejamento_saida', 'remanejamento_entrada');
      end if;
    end if;
  end loop;
end $$;
