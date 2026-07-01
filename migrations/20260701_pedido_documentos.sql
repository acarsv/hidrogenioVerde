create table if not exists pedido_documentos (
  id bigserial primary key,
  pedido_id bigint not null,
  solicitacao_id bigint references solicitacoes_compra(id) on delete set null,
  categoria text not null default 'documento',
  google_drive_file_id text,
  google_drive_link text,
  pasta_google_drive_link text,
  nome_arquivo text not null,
  mime_type text,
  tamanho_bytes bigint,
  observacao text,
  enviado_por uuid references usuarios_app(id),
  criado_em timestamptz not null default now()
);

create index if not exists idx_pedido_documentos_pedido_id
  on pedido_documentos(pedido_id);
