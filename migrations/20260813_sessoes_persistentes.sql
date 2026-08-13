create table if not exists sessoes_app (
  id bigserial primary key,
  usuario_id uuid not null references usuarios_app(id) on delete cascade,
  token_hash text not null unique,
  criado_em timestamptz not null default now(),
  revogado_em timestamptz
);

create index if not exists idx_sessoes_app_usuario
  on sessoes_app(usuario_id);
