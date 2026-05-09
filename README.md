# HidrogenioVerde DB App

Sistema em Python/Streamlit para controle de orçamento por rubricas, solicitação de compra, 3 cotações, autorização, compra, nota fiscal e atualização automática do saldo.

## Passos no VS Code
1. Abra esta pasta no VS Code.
2. Crie o ambiente:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. No Supabase, abra `SQL Editor` e execute `schema.sql` e depois `seed_rubricas.sql`.
4. Copie `.env.example` para `.env` e coloque a string de conexão do Supabase.
5. Execute:
   ```bash
   streamlit run app.py
   ```

## Fluxo operacional
Solicitação → autorização do gerente → 3 cotações → escolha da vencedora → compra → aguardando nota → nota fiscal lançada → finalizado.
