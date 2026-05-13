-- FASE 1C - Permitir que o item rastreavel venha de pedido_itens.
-- Mantem solicitacao_item_id por compatibilidade, mas deixa de exigir seu uso
-- nas novas escritas de cotacao_itens e nota_fiscal_itens.

alter table cotacao_itens
    alter column solicitacao_item_id drop not null;

alter table nota_fiscal_itens
    alter column solicitacao_item_id drop not null;
