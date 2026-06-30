create or replace view vw_historico_remanejamentos as
select
  saida.remanejamento_id,
  saida.criado_em,
  coalesce(usuario.nome, 'Sistema') as usuario,
  origem.codigo as origem_codigo,
  origem.nome as origem_nome,
  destino.codigo as destino_codigo,
  destino.nome as destino_nome,
  saida.valor,
  saida.justificativa,
  case
    when saida.estornado_em is not null or entrada.estornado_em is not null then 'estornado'
    else 'ativo'
  end as status,
  coalesce(saida.estornado_em, entrada.estornado_em) as estornado_em,
  coalesce(usuario_estorno.nome, 'Sistema') as estornado_por,
  retorno.criado_em as retorno_em,
  retorno.justificativa as justificativa_retorno
from movimentacoes_orcamento saida
join movimentacoes_orcamento entrada
  on entrada.remanejamento_id = saida.remanejamento_id
 and entrada.operacao = 'remanejamento_entrada'
join rubricas origem on origem.id = saida.rubrica_id
join rubricas destino on destino.id = entrada.rubrica_id
left join usuarios_app usuario on usuario.id = saida.usuario_id
left join usuarios_app usuario_estorno on usuario_estorno.id = coalesce(saida.estornado_por, entrada.estornado_por)
left join lateral (
  select criado_em, justificativa
  from movimentacoes_orcamento retorno
  where retorno.remanejamento_id = saida.remanejamento_id
    and retorno.operacao = 'retorno_remanejamento_saida'
  order by retorno.criado_em desc, retorno.id desc
  limit 1
) retorno on true
where saida.operacao = 'remanejamento_saida'
  and saida.remanejamento_id is not null;
