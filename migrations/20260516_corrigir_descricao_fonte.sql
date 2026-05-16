-- Corrige descrições afetadas por junção indevida de palavras na exibição/salvamento.

update public.pedido_itens
set descricao = replace(descricao, 'Fontede ', 'Fonte de ')
where descricao like '%Fontede %';

update public.solicitacoes_compra
set descricao = replace(descricao, 'Fontede ', 'Fonte de ')
where descricao like '%Fontede %';

update public.cotacao_itens
set observacoes = replace(observacoes, 'Fontede ', 'Fonte de ')
where observacoes like '%Fontede %';

update public.nota_fiscal_itens
set descricao = replace(descricao, 'Fontede ', 'Fonte de ')
where descricao like '%Fontede %';

update public.alertas_ia
set descricao = replace(descricao, 'Fontede ', 'Fonte de ')
where descricao like '%Fontede %';
