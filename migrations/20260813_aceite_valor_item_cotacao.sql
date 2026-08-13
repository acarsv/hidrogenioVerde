alter table cotacao_itens
  add column if not exists aceito boolean not null default false;

update cotacao_itens
set aceito = true
where valor_unitario > 0
  and aceito = false;
