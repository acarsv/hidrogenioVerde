import os
import mimetypes
import re
import json
from uuid import uuid4
from io import BytesIO
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import bcrypt
import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv
from urllib.parse import urlparse
from ia_operacional import (
    carregar_alertas,
    carregar_score_risco_rubrica,
    criar_schema_ia_operacional,
    gerar_alertas_ia,
    marcar_alerta_resolvido,
)

load_dotenv(override=True)
st.set_page_config(page_title="Hidrogênio Verde - Compras", layout="wide")
APP_DEPLOY_VERSION = "2026-05-11.10"
PERIODO_PRESTACAO_INICIO = date(2026, 3, 1)
PERIODO_PRESTACAO_FIM = date(2027, 3, 31)

def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        st.error("DATABASE_URL nao foi definida no arquivo .env.")
        st.stop()

    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as exc:
        parsed = urlparse(database_url)
        host = parsed.hostname or "host nao identificado"
        try:
            port = parsed.port or "porta padrao"
        except ValueError:
            port = "porta invalida na DATABASE_URL"
        user = parsed.username or "usuario nao identificado"
        st.error(
            "Nao foi possivel conectar ao Supabase. "
            f"Confira usuario, senha e host no .env. Host: {host}, porta: {port}, usuario: {user}."
        )
        st.caption(
            "Se a senha do banco tiver caracteres como @, #, %, /, : ou espaco, "
            "copie novamente a connection string URI do Supabase ou codifique a senha na URL."
        )
        with st.expander("Detalhe tecnico"):
            st.code(str(exc))
        st.stop()

def query(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if cur.description:
                return pd.DataFrame(cur.fetchall())
            return pd.DataFrame()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def execute(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def acquire_startup_schema_lock():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select pg_try_advisory_lock(2026052602)")
            locked = cur.fetchone()[0]
            if not locked:
                conn.close()
                return None
        return conn
    except Exception:
        conn.close()
        raise

def release_startup_schema_lock(conn):
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_unlock(2026052602)")
    finally:
        conn.close()

def config_value(nome, alternativa=None):
    valor = os.environ.get(nome)
    if valor:
        return valor
    try:
        if nome in st.secrets:
            return st.secrets[nome]
        if alternativa and alternativa in st.secrets:
            return st.secrets[alternativa]
    except Exception:
        pass
    return os.environ.get(alternativa) if alternativa else None

def google_drive_folder_url(folder_id):
    return f"https://drive.google.com/drive/folders/{folder_id}"


def extrair_google_drive_folder_id(link):
    if not link:
        return None
    parsed = urlparse(str(link).strip())
    partes = [parte for parte in parsed.path.split("/") if parte]
    if "folders" in partes:
        indice = partes.index("folders")
        if len(partes) > indice + 1:
            return partes[indice + 1]
    return None


def nome_seguro_drive(valor):
    texto = re.sub(r"[^0-9A-Za-zÀ-ÿ._ -]+", " ", str(valor or "")).strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto[:120] or "sem_fornecedor"


def escapar_drive_query(valor):
    return str(valor).replace("\\", "\\\\").replace("'", "\\'")


def carregar_service_account_info(service_account_json):
    if not isinstance(service_account_json, str):
        return dict(service_account_json)
    try:
        return json.loads(service_account_json)
    except json.JSONDecodeError:
        texto = service_account_json.strip()
        match = re.search(r'("private_key"\s*:\s*")(.*?)(",\s*"client_email")', texto, flags=re.DOTALL)
        if match:
            chave = match.group(2).replace("\\n", "\n")
            chave = chave.replace("\r\n", "\n").replace("\r", "\n")
            chave = chave.replace("\n", "\\n")
            texto = texto[:match.start(2)] + chave + texto[match.end(2):]
            try:
                return json.loads(texto)
            except json.JSONDecodeError:
                pass
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON está mal formatado. Cole o JSON completo como bloco '''...''' "
            "no Streamlit Secrets e mantenha as quebras da private_key como \\n."
        )


def descrever_erro_google_drive(exc, folder_config="GOOGLE_DRIVE_COTACOES_FOLDER_ID"):
    status = getattr(getattr(exc, "resp", None), "status", None)
    motivo = ""
    mensagem = ""
    try:
        conteudo = exc.content.decode("utf-8") if isinstance(exc.content, bytes) else str(exc.content)
        detalhe = json.loads(conteudo)
        erro = detalhe.get("error", {})
        mensagem = erro.get("message", "")
        erros = erro.get("errors") or []
        if erros:
            motivo = erros[0].get("reason", "")
    except Exception:
        pass

    if motivo == "storageQuotaExceeded":
        return (
            "O Google Drive criou a pasta, mas recusou o arquivo por quota de armazenamento da service account "
            "(storageQuotaExceeded). Use uma pasta em Drive compartilhado do Google Workspace ou configure upload "
            "com OAuth de um usuário real do Google Drive; em pasta de Meu Drive compartilhada, a service account "
            "pode ficar sem quota para armazenar arquivos."
        )
    if status in (401, 403):
        detalhe = f" Detalhe Google: {motivo or mensagem}." if (motivo or mensagem) else ""
        return (
            "Google Drive recusou o upload por falta de permissão. "
            f"Compartilhe a pasta {folder_config} com o e-mail da service account como Editor "
            "e confirme se a API Google Drive está habilitada no projeto."
            f"{detalhe}"
        )
    if status == 404:
        return (
            "A pasta do Google Drive não foi encontrada pela service account. "
            f"Confira o {folder_config} e compartilhe essa pasta com a service account."
        )
    if motivo or mensagem:
        return f"Erro do Google Drive ao enviar arquivo ({status or 'sem status'} - {motivo or mensagem})."
    return "Erro do Google Drive ao enviar arquivo. Confira permissões, ID da pasta e API habilitada."


def descrever_erro_oauth_refresh(exc):
    texto = str(exc)
    if "invalid_grant" in texto:
        return (
            "Google OAuth recusou o REFRESH_TOKEN (invalid_grant). Gere um novo token com gerar_token_drive.py "
            "e confira se GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET e GOOGLE_OAUTH_REFRESH_TOKEN no Streamlit "
            "Secrets vieram da mesma credencial client_secret.json."
        )
    if "invalid_client" in texto or "unauthorized_client" in texto:
        return (
            "Google OAuth recusou CLIENT_ID/CLIENT_SECRET. Confira se os valores no Streamlit Secrets são exatamente "
            "os gerados pelo gerar_token_drive.py e pertencem ao mesmo OAuth Client."
        )
    return "Google OAuth não conseguiu renovar o acesso ao Drive. Gere um novo REFRESH_TOKEN e atualize os Secrets."


def upload_cotacao_google_drive(uploaded_file, solicitacao_id, ordem, rubrica_id=None, fornecedor=None, pasta_url=None):
    folder_id = config_value("GOOGLE_DRIVE_COTACOES_FOLDER_ID", "GOOGLE_DRIVE_FOLDER_ID")
    oauth_client_id = config_value("GOOGLE_OAUTH_CLIENT_ID", "CLIENT_ID")
    oauth_client_secret = config_value("GOOGLE_OAUTH_CLIENT_SECRET", "CLIENT_SECRET")
    oauth_refresh_token = config_value("GOOGLE_OAUTH_REFRESH_TOKEN", "REFRESH_TOKEN")
    service_account_json = config_value("GOOGLE_SERVICE_ACCOUNT_JSON")
    service_account_file = config_value("GOOGLE_APPLICATION_CREDENTIALS")

    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_COTACOES_FOLDER_ID não foi definido no Streamlit Secrets ou no .env.")
    tem_oauth = bool(oauth_client_id and oauth_client_secret and oauth_refresh_token)
    if not tem_oauth and not service_account_json and not service_account_file:
        raise RuntimeError(
            "Defina GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET e GOOGLE_OAUTH_REFRESH_TOKEN "
            "ou GOOGLE_SERVICE_ACCOUNT_JSON no Streamlit Secrets ou no .env."
        )

    try:
        from google.oauth2.credentials import Credentials
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        from googleapiclient.errors import HttpError
        from google.auth.exceptions import RefreshError
    except ImportError as exc:
        raise RuntimeError(
            "Dependencias do Google Drive ausentes. Instale google-api-python-client e google-auth."
        ) from exc

    scopes = ["https://www.googleapis.com/auth/drive"]
    if tem_oauth:
        credentials = Credentials(
            token=None,
            refresh_token=oauth_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=oauth_client_id,
            client_secret=oauth_client_secret,
            scopes=scopes,
        )
    elif service_account_json:
        credentials = service_account.Credentials.from_service_account_info(
            carregar_service_account_info(service_account_json),
            scopes=scopes,
        )
    else:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes,
        )

    try:
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        pasta_nome = f"rubrica_{rubrica_id or solicitacao_id}_cotacao_{ordem}_{nome_seguro_drive(fornecedor)}"
        pasta_link_id = extrair_google_drive_folder_id(pasta_url)
        parent_folder_id = folder_id
        cotacao_folder_id = None

        if pasta_link_id:
            try:
                pasta_link = service.files().get(
                    fileId=pasta_link_id,
                    fields="id, name, mimeType",
                    supportsAllDrives=True,
                ).execute()
                if (
                    pasta_link.get("mimeType") == "application/vnd.google-apps.folder"
                    and str(pasta_link.get("name", "")).startswith(f"rubrica_{rubrica_id or solicitacao_id}_cotacao_{ordem}_")
                ):
                    cotacao_folder_id = pasta_link_id
                elif pasta_link.get("mimeType") == "application/vnd.google-apps.folder":
                    parent_folder_id = pasta_link_id
            except HttpError:
                # Link antigo/inacessível na cotação não deve impedir novo upload.
                parent_folder_id = folder_id
                cotacao_folder_id = None

        if not cotacao_folder_id:
            existentes = service.files().list(
                q=(
                    f"'{parent_folder_id}' in parents and "
                    "mimeType = 'application/vnd.google-apps.folder' and "
                    f"name = '{escapar_drive_query(pasta_nome)}' and trashed = false"
                ),
                fields="files(id, name)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ).execute().get("files", [])
            if existentes:
                cotacao_folder_id = existentes[0]["id"]
            else:
                pasta = service.files().create(
                    body={
                        "name": pasta_nome,
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [parent_folder_id],
                    },
                    fields="id",
                    supportsAllDrives=True,
                ).execute()
                cotacao_folder_id = pasta["id"]

        pasta_confirmada = service.files().get(
            fileId=cotacao_folder_id,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        if not pasta_confirmada.get("id"):
            raise RuntimeError("Não foi possível confirmar a pasta da cotação no Google Drive.")

        filename = uploaded_file.name
        content_type = uploaded_file.type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        conteudo = uploaded_file.getvalue()
        media = MediaIoBaseUpload(BytesIO(conteudo), mimetype=content_type, resumable=False)
        metadata = {
            "name": f"solicitacao_{solicitacao_id}_cotacao_{ordem}_{filename}",
            "parents": [cotacao_folder_id],
        }
        criado = service.files().create(
            body=metadata,
            media_body=media,
            fields="id, name, parents, webViewLink",
            supportsAllDrives=True,
        ).execute()
        if cotacao_folder_id not in criado.get("parents", []):
            raise RuntimeError("O arquivo foi enviado, mas o Google Drive não confirmou vínculo com a pasta da cotação.")
        return {
            "folder_id": cotacao_folder_id,
            "folder_link": pasta_confirmada.get("webViewLink") or google_drive_folder_url(cotacao_folder_id),
            "file_id": criado.get("id"),
            "file_link": criado.get("webViewLink"),
            "nome_arquivo": filename,
            "mime_type": content_type,
            "tamanho_bytes": getattr(uploaded_file, "size", None) or len(conteudo),
        }
    except HttpError as exc:
        raise RuntimeError(descrever_erro_google_drive(exc)) from exc
    except RefreshError as exc:
        raise RuntimeError(descrever_erro_oauth_refresh(exc)) from exc


def upload_nota_fiscal_google_drive(uploaded_file, numero_nf, fornecedor, pasta_url=None):
    notafiscal_root_id = config_value("GOOGLE_DRIVE_NOTAFISCAL_FOLDER_ID")
    folder_id = notafiscal_root_id or config_value("GOOGLE_DRIVE_FOLDER_ID")
    oauth_client_id = config_value("GOOGLE_OAUTH_CLIENT_ID", "CLIENT_ID")
    oauth_client_secret = config_value("GOOGLE_OAUTH_CLIENT_SECRET", "CLIENT_SECRET")
    oauth_refresh_token = config_value("GOOGLE_OAUTH_REFRESH_TOKEN", "REFRESH_TOKEN")
    service_account_json = config_value("GOOGLE_SERVICE_ACCOUNT_JSON")
    service_account_file = config_value("GOOGLE_APPLICATION_CREDENTIALS")

    if not folder_id:
        raise RuntimeError(
            "GOOGLE_DRIVE_NOTAFISCAL_FOLDER_ID ou GOOGLE_DRIVE_FOLDER_ID nao foi definido no "
            "Streamlit Secrets ou no .env."
        )
    tem_oauth = bool(oauth_client_id and oauth_client_secret and oauth_refresh_token)
    if not tem_oauth and not service_account_json and not service_account_file:
        raise RuntimeError(
            "Defina GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET e GOOGLE_OAUTH_REFRESH_TOKEN "
            "ou GOOGLE_SERVICE_ACCOUNT_JSON no Streamlit Secrets ou no .env."
        )

    try:
        from google.oauth2.credentials import Credentials
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        from googleapiclient.errors import HttpError
        from google.auth.exceptions import RefreshError
    except ImportError as exc:
        raise RuntimeError(
            "Dependencias do Google Drive ausentes. Instale google-api-python-client e google-auth."
        ) from exc

    scopes = ["https://www.googleapis.com/auth/drive"]
    if tem_oauth:
        credentials = Credentials(
            token=None,
            refresh_token=oauth_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=oauth_client_id,
            client_secret=oauth_client_secret,
            scopes=scopes,
        )
    elif service_account_json:
        credentials = service_account.Credentials.from_service_account_info(
            carregar_service_account_info(service_account_json),
            scopes=scopes,
        )
    else:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes,
        )

    try:
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        parent_folder_id = folder_id
        pasta_link_id = extrair_google_drive_folder_id(pasta_url)
        nota_folder_id = None
        pasta_nome = f"nf_{nome_seguro_drive(numero_nf)}_{nome_seguro_drive(fornecedor)}"

        if pasta_link_id:
            try:
                pasta_link = service.files().get(
                    fileId=pasta_link_id,
                    fields="id, name, mimeType",
                    supportsAllDrives=True,
                ).execute()
                if (
                    pasta_link.get("mimeType") == "application/vnd.google-apps.folder"
                    and str(pasta_link.get("name", "")).startswith("nf_")
                ):
                    nota_folder_id = pasta_link_id
                elif pasta_link.get("mimeType") == "application/vnd.google-apps.folder":
                    parent_folder_id = pasta_link_id
            except HttpError:
                parent_folder_id = folder_id
                nota_folder_id = None

        if not nota_folder_id:
            if notafiscal_root_id and parent_folder_id == folder_id:
                notafiscal_folder_id = notafiscal_root_id
            else:
                pastas_notafiscal = service.files().list(
                    q=(
                        f"'{parent_folder_id}' in parents and "
                        "mimeType = 'application/vnd.google-apps.folder' and "
                        "name = 'notafiscal' and trashed = false"
                    ),
                    fields="files(id, name)",
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                ).execute().get("files", [])
                if pastas_notafiscal:
                    notafiscal_folder_id = pastas_notafiscal[0]["id"]
                else:
                    pasta_notafiscal = service.files().create(
                        body={
                            "name": "notafiscal",
                            "mimeType": "application/vnd.google-apps.folder",
                            "parents": [parent_folder_id],
                        },
                        fields="id",
                        supportsAllDrives=True,
                    ).execute()
                    notafiscal_folder_id = pasta_notafiscal["id"]

            pastas_nf = service.files().list(
                q=(
                    f"'{notafiscal_folder_id}' in parents and "
                    "mimeType = 'application/vnd.google-apps.folder' and "
                    f"name = '{escapar_drive_query(pasta_nome)}' and trashed = false"
                ),
                fields="files(id, name)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ).execute().get("files", [])
            if pastas_nf:
                nota_folder_id = pastas_nf[0]["id"]
            else:
                pasta_nf = service.files().create(
                    body={
                        "name": pasta_nome,
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [notafiscal_folder_id],
                    },
                    fields="id",
                    supportsAllDrives=True,
                ).execute()
                nota_folder_id = pasta_nf["id"]

        pasta_confirmada = service.files().get(
            fileId=nota_folder_id,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        if not pasta_confirmada.get("id"):
            raise RuntimeError("Nao foi possivel confirmar a pasta da nota fiscal no Google Drive.")

        filename = uploaded_file.name
        content_type = uploaded_file.type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        conteudo = uploaded_file.getvalue()
        media = MediaIoBaseUpload(BytesIO(conteudo), mimetype=content_type, resumable=False)
        criado = service.files().create(
            body={
                "name": f"nf_{nome_seguro_drive(numero_nf)}_{filename}",
                "parents": [nota_folder_id],
            },
            media_body=media,
            fields="id, name, parents, webViewLink",
            supportsAllDrives=True,
        ).execute()
        if nota_folder_id not in criado.get("parents", []):
            raise RuntimeError("O arquivo foi enviado, mas o Google Drive nao confirmou vinculo com a pasta da nota fiscal.")
        return {
            "folder_id": nota_folder_id,
            "folder_link": pasta_confirmada.get("webViewLink") or google_drive_folder_url(nota_folder_id),
            "file_id": criado.get("id"),
            "file_link": criado.get("webViewLink"),
            "nome_arquivo": filename,
            "mime_type": content_type,
            "tamanho_bytes": getattr(uploaded_file, "size", None) or len(conteudo),
        }
    except HttpError as exc:
        raise RuntimeError(descrever_erro_google_drive(exc, "GOOGLE_DRIVE_NOTAFISCAL_FOLDER_ID ou GOOGLE_DRIVE_FOLDER_ID")) from exc
    except RefreshError as exc:
        raise RuntimeError(descrever_erro_oauth_refresh(exc)) from exc

def upload_comprovante_bancario_google_drive(uploaded_file, compra_id, fornecedor=None, pasta_url=None):
    comprovantes_root_id = config_value("GOOGLE_DRIVE_COMPROVANTES_FOLDER_ID")
    folder_id = comprovantes_root_id or config_value("GOOGLE_DRIVE_FOLDER_ID")
    oauth_client_id = config_value("GOOGLE_OAUTH_CLIENT_ID", "CLIENT_ID")
    oauth_client_secret = config_value("GOOGLE_OAUTH_CLIENT_SECRET", "CLIENT_SECRET")
    oauth_refresh_token = config_value("GOOGLE_OAUTH_REFRESH_TOKEN", "REFRESH_TOKEN")
    service_account_json = config_value("GOOGLE_SERVICE_ACCOUNT_JSON")
    service_account_file = config_value("GOOGLE_APPLICATION_CREDENTIALS")

    if not folder_id:
        raise RuntimeError(
            "GOOGLE_DRIVE_COMPROVANTES_FOLDER_ID ou GOOGLE_DRIVE_FOLDER_ID nao foi definido no "
            "Streamlit Secrets ou no .env."
        )
    tem_oauth = bool(oauth_client_id and oauth_client_secret and oauth_refresh_token)
    if not tem_oauth and not service_account_json and not service_account_file:
        raise RuntimeError(
            "Defina GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET e GOOGLE_OAUTH_REFRESH_TOKEN "
            "ou GOOGLE_SERVICE_ACCOUNT_JSON no Streamlit Secrets ou no .env."
        )

    try:
        from google.oauth2.credentials import Credentials
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        from googleapiclient.errors import HttpError
        from google.auth.exceptions import RefreshError
    except ImportError as exc:
        raise RuntimeError(
            "Dependencias do Google Drive ausentes. Instale google-api-python-client e google-auth."
        ) from exc

    scopes = ["https://www.googleapis.com/auth/drive"]
    if tem_oauth:
        credentials = Credentials(
            token=None,
            refresh_token=oauth_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=oauth_client_id,
            client_secret=oauth_client_secret,
            scopes=scopes,
        )
    elif service_account_json:
        credentials = service_account.Credentials.from_service_account_info(
            carregar_service_account_info(service_account_json),
            scopes=scopes,
        )
    else:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes,
        )

    try:
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        parent_folder_id = folder_id
        pasta_link_id = extrair_google_drive_folder_id(pasta_url)
        comprovante_folder_id = None
        pasta_nome = f"compra_{compra_id}_comprovantes_{nome_seguro_drive(fornecedor)}"

        if pasta_link_id:
            try:
                pasta_link = service.files().get(
                    fileId=pasta_link_id,
                    fields="id, name, mimeType",
                    supportsAllDrives=True,
                ).execute()
                if (
                    pasta_link.get("mimeType") == "application/vnd.google-apps.folder"
                    and str(pasta_link.get("name", "")).startswith(f"compra_{compra_id}_comprovantes_")
                ):
                    comprovante_folder_id = pasta_link_id
                elif pasta_link.get("mimeType") == "application/vnd.google-apps.folder":
                    parent_folder_id = pasta_link_id
            except HttpError:
                parent_folder_id = folder_id
                comprovante_folder_id = None

        if not comprovante_folder_id:
            if comprovantes_root_id and parent_folder_id == folder_id:
                comprovantes_folder_id = comprovantes_root_id
            else:
                pastas_comprovantes = service.files().list(
                    q=(
                        f"'{parent_folder_id}' in parents and "
                        "mimeType = 'application/vnd.google-apps.folder' and "
                        "name = 'comprovantes_bancarios' and trashed = false"
                    ),
                    fields="files(id, name)",
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                ).execute().get("files", [])
                if pastas_comprovantes:
                    comprovantes_folder_id = pastas_comprovantes[0]["id"]
                else:
                    pasta_comprovantes = service.files().create(
                        body={
                            "name": "comprovantes_bancarios",
                            "mimeType": "application/vnd.google-apps.folder",
                            "parents": [parent_folder_id],
                        },
                        fields="id",
                        supportsAllDrives=True,
                    ).execute()
                    comprovantes_folder_id = pasta_comprovantes["id"]

            pastas_compra = service.files().list(
                q=(
                    f"'{comprovantes_folder_id}' in parents and "
                    "mimeType = 'application/vnd.google-apps.folder' and "
                    f"name = '{escapar_drive_query(pasta_nome)}' and trashed = false"
                ),
                fields="files(id, name)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ).execute().get("files", [])
            if pastas_compra:
                comprovante_folder_id = pastas_compra[0]["id"]
            else:
                pasta_compra = service.files().create(
                    body={
                        "name": pasta_nome,
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [comprovantes_folder_id],
                    },
                    fields="id",
                    supportsAllDrives=True,
                ).execute()
                comprovante_folder_id = pasta_compra["id"]

        pasta_confirmada = service.files().get(
            fileId=comprovante_folder_id,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        if not pasta_confirmada.get("id"):
            raise RuntimeError("Nao foi possivel confirmar a pasta do comprovante bancario no Google Drive.")

        filename = uploaded_file.name
        content_type = uploaded_file.type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        conteudo = uploaded_file.getvalue()
        media = MediaIoBaseUpload(BytesIO(conteudo), mimetype=content_type, resumable=False)
        criado = service.files().create(
            body={
                "name": f"compra_{compra_id}_comprovante_{filename}",
                "parents": [comprovante_folder_id],
            },
            media_body=media,
            fields="id, name, parents, webViewLink",
            supportsAllDrives=True,
        ).execute()
        if comprovante_folder_id not in criado.get("parents", []):
            raise RuntimeError("O arquivo foi enviado, mas o Google Drive nao confirmou vinculo com a pasta do comprovante.")
        return {
            "folder_id": comprovante_folder_id,
            "folder_link": pasta_confirmada.get("webViewLink") or google_drive_folder_url(comprovante_folder_id),
            "file_id": criado.get("id"),
            "file_link": criado.get("webViewLink"),
            "nome_arquivo": filename,
            "mime_type": content_type,
            "tamanho_bytes": getattr(uploaded_file, "size", None) or len(conteudo),
        }
    except HttpError as exc:
        raise RuntimeError(descrever_erro_google_drive(exc, "GOOGLE_DRIVE_COMPROVANTES_FOLDER_ID ou GOOGLE_DRIVE_FOLDER_ID")) from exc
    except RefreshError as exc:
        raise RuntimeError(descrever_erro_oauth_refresh(exc)) from exc

def has_column(table_name: str, column_name: str) -> bool:
    df = query("""
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = %s
      and column_name = %s
    limit 1
    """, (table_name, column_name))
    return len(df) == 1

def cotacao_arquivos_df(cotacao_id):
    if not cotacao_id:
        return pd.DataFrame()
    return query("""
    select id, nome_arquivo, google_drive_link, mime_type, tamanho_bytes, criado_em
    from cotacao_arquivos
    where cotacao_id=%s
    order by criado_em desc, id desc
    """, (int(cotacao_id),))

def exibir_arquivos_cotacao(cotacao_id):
    arquivos = cotacao_arquivos_df(cotacao_id)
    if len(arquivos) == 0:
        return
    tabela = arquivos[["nome_arquivo", "google_drive_link", "criado_em"]].copy()
    tabela = tabela.rename(columns={
        "nome_arquivo": "Arquivo",
        "google_drive_link": "Link",
        "criado_em": "Enviado em",
    })
    st.markdown("### Arquivos vinculados à cotação")
    st.dataframe(
        tabela,
        use_container_width=True,
        hide_index=True,
        column_config={"Link": st.column_config.LinkColumn("Abrir arquivo")},
    )


def nota_fiscal_arquivos_df(nota_fiscal_id):
    if not nota_fiscal_id:
        return pd.DataFrame()
    return query("""
    select id, nome_arquivo, google_drive_link, mime_type, tamanho_bytes, criado_em
    from nota_fiscal_arquivos
    where nota_fiscal_id=%s
    order by criado_em desc, id desc
    """, (int(nota_fiscal_id),))


def comprovantes_bancarios_df(compra_id):
    if not compra_id:
        return pd.DataFrame()
    return query("""
    select
      cb.id,
      cb.nota_fiscal_id,
      cb.google_drive_link,
      cb.pasta_google_drive_link,
      cb.nome_arquivo,
      cb.mime_type,
      cb.tamanho_bytes,
      cb.observacao,
      cb.criado_em,
      nf.numero_nf
    from comprovantes_bancarios cb
    left join notas_fiscais nf on nf.id = cb.nota_fiscal_id
    where cb.compra_id=%s
    order by cb.criado_em desc, cb.id desc
    """, (int(compra_id),))


def exibir_comprovantes_bancarios(compra_id):
    arquivos = comprovantes_bancarios_df(compra_id)
    if len(arquivos) == 0:
        return
    tabela = arquivos[["numero_nf", "nome_arquivo", "google_drive_link", "observacao", "criado_em"]].copy()
    tabela = tabela.rename(columns={
        "numero_nf": "NF",
        "nome_arquivo": "Arquivo",
        "google_drive_link": "Link",
        "observacao": "Observacao",
        "criado_em": "Enviado em",
    })
    st.markdown("### Comprovantes bancários vinculados")
    st.dataframe(
        tabela,
        use_container_width=True,
        hide_index=True,
        column_config={"Link": st.column_config.LinkColumn("Abrir arquivo")},
    )


def exibir_arquivos_nota_fiscal(nota_fiscal_id):
    arquivos = nota_fiscal_arquivos_df(nota_fiscal_id)
    if len(arquivos) == 0:
        return
    tabela = arquivos[["nome_arquivo", "google_drive_link", "criado_em"]].copy()
    tabela = tabela.rename(columns={
        "nome_arquivo": "Arquivo",
        "google_drive_link": "Link",
        "criado_em": "Enviado em",
    })
    st.markdown("### Arquivos vinculados a nota fiscal")
    st.dataframe(
        tabela,
        use_container_width=True,
        hide_index=True,
        column_config={"Link": st.column_config.LinkColumn("Abrir arquivo")},
    )


def ensure_financial_governance_schema():
    if not has_column("cotacoes", "arquivo_url"):
        execute("alter table cotacoes add column arquivo_url text")
    if not has_column("cotacoes", "observacoes"):
        execute("alter table cotacoes add column observacoes text")
    if not has_column("cotacoes", "rubrica_id"):
        execute("alter table cotacoes add column rubrica_id bigint references rubricas(id)")
        execute("""
        update cotacoes c
        set rubrica_id = s.rubrica_id
        from solicitacoes_compra s
        where s.id = c.solicitacao_id and c.rubrica_id is null
        """)
    if not has_column("cotacao_itens", "descricao_item"):
        execute("alter table cotacao_itens add column descricao_item text")
    if not has_column("cotacao_itens", "tipo_item"):
        execute("alter table cotacao_itens add column tipo_item text")
    execute("""
    create table if not exists cotacao_arquivos (
      id bigserial primary key,
      cotacao_id bigint not null references cotacoes(id) on delete cascade,
      google_drive_file_id text not null,
      google_drive_link text,
      nome_arquivo text not null,
      mime_type text,
      tamanho_bytes bigint,
      criado_em timestamptz not null default now()
    )
    """)
    execute("""
    create table if not exists nota_fiscal_arquivos (
      id bigserial primary key,
      nota_fiscal_id bigint not null references notas_fiscais(id) on delete cascade,
      google_drive_file_id text not null,
      google_drive_link text,
      nome_arquivo text not null,
      mime_type text,
      tamanho_bytes bigint,
      criado_em timestamptz not null default now()
    )
    """)
    execute("""
    create table if not exists comprovantes_bancarios (
      id bigserial primary key,
      compra_id bigint not null references compras(id) on delete cascade,
      nota_fiscal_id bigint references notas_fiscais(id) on delete set null,
      google_drive_file_id text not null,
      google_drive_link text,
      pasta_google_drive_link text,
      nome_arquivo text not null,
      mime_type text,
      tamanho_bytes bigint,
      observacao text,
      enviado_por uuid references usuarios_app(id),
      criado_em timestamptz not null default now()
    )
    """)
    execute("""
    create table if not exists valores_extra_nao_debitados (
      id bigserial primary key,
      compra_id bigint references compras(id) on delete cascade,
      nota_fiscal_id bigint references notas_fiscais(id) on delete set null,
      rubrica_id bigint references rubricas(id),
      solicitacao_id bigint references solicitacoes_compra(id),
      tipo text not null default 'taxa_ted',
      descricao text not null,
      valor numeric(14,2) not null check (valor >= 0),
      responsavel_pagamento text,
      data_pagamento date,
      registrado_por uuid references usuarios_app(id),
      criado_em timestamptz not null default now()
    )
    """)
    if not has_column("rubricas", "valor_minimo_operacional"):
        execute("alter table rubricas add column valor_minimo_operacional numeric(14,2) not null default 0")
    if not has_column("rubricas", "reserva_tecnica_percentual"):
        execute("alter table rubricas add column reserva_tecnica_percentual numeric(5,2) not null default 5")
    if not has_column("rubricas", "encerrada"):
        execute("alter table rubricas add column encerrada boolean not null default false")
    if not has_column("rubricas", "encerrada_em"):
        execute("alter table rubricas add column encerrada_em timestamptz")
    if not has_column("rubricas", "encerrada_por"):
        execute("alter table rubricas add column encerrada_por uuid references usuarios_app(id)")

    execute("""
    update rubricas
    set valor_minimo_operacional = case
        when tipo = 'material_permanente' then 2000
        when tipo = 'material_consumo' then 300
        when tipo = 'servico_pf' then 500
        else 0
    end
    where valor_minimo_operacional = 0
    """)

    execute("""
    create table if not exists movimentacoes_orcamento (
      id bigserial primary key,
      rubrica_id bigint not null references rubricas(id),
      usuario_id uuid references usuarios_app(id),
      operacao text not null,
      valor numeric(14,2) not null default 0,
      justificativa text,
      remanejamento_id text,
      estornado_em timestamptz,
      estornado_por uuid references usuarios_app(id),
      criado_em timestamptz not null default now()
    )
    """)
    if not has_column("movimentacoes_orcamento", "remanejamento_id"):
        execute("alter table movimentacoes_orcamento add column remanejamento_id text")
    if not has_column("movimentacoes_orcamento", "estornado_em"):
        execute("alter table movimentacoes_orcamento add column estornado_em timestamptz")
    if not has_column("movimentacoes_orcamento", "estornado_por"):
        execute("alter table movimentacoes_orcamento add column estornado_por uuid references usuarios_app(id)")

    execute("""
    create or replace view vw_orcamento as
    select
      r.id,
      r.codigo,
      r.nome,
      r.tipo,
      r.valor_orcado,
      r.valor_reservado,
      r.valor_utilizado,
      (
        r.valor_orcado
        - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
        - r.valor_reservado
        - r.valor_utilizado
      ) as saldo_disponivel,
      case
        when r.valor_orcado > 0 then round((r.valor_utilizado * 100.0 / r.valor_orcado), 2)
        else 0
      end as percentual_utilizado,
      r.valor_minimo_operacional,
      r.reserva_tecnica_percentual,
      round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2) as reserva_tecnica,
      case
        when (
          r.valor_orcado
          - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
          - r.valor_reservado
          - r.valor_utilizado
        ) > 0
         and (
          r.valor_orcado
          - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
          - r.valor_reservado
          - r.valor_utilizado
        ) < r.valor_minimo_operacional
        then (
          r.valor_orcado
          - r.valor_reservado
          - r.valor_utilizado
        )
        else 0
      end as saldo_residual,
      r.encerrada,
      case
        when r.valor_orcado > 0 then round(((round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2) + r.valor_reservado + r.valor_utilizado) * 100.0 / r.valor_orcado), 2)
        else 0
      end as percentual_comprometido
    from rubricas r
    where r.ativo = true
    """)
    execute("""
    select pg_advisory_lock(2026052601);
    drop view if exists vw_auditoria_itens_projeto;
    create view vw_auditoria_itens_projeto as
    with cotacao_resumo as (
        select
            ci.pedido_item_id,
            count(*) as total_cotacoes,
            count(*) filter (where ci.vencedor = true) as total_vencedoras,
            max(c.fornecedor) filter (where ci.vencedor = true) as fornecedor_vencedor,
            sum(ci.valor_total) filter (where ci.vencedor = true) as valor_cotado_vencedor
        from cotacao_itens ci
        join cotacoes c on c.id = ci.cotacao_id
        where ci.pedido_item_id is not null
        group by ci.pedido_item_id
    ),
    nota_resumo as (
        select
            nfi.pedido_item_id,
            count(nfi.id) as total_itens_nf,
            sum(nfi.valor_total) as valor_total_nf_item,
            string_agg(distinct nf.numero_nf, ', ') as notas_fiscais,
            string_agg(distinct nf.fornecedor, ', ') as fornecedores_nf,
            bool_or(nf.arquivo_url is not null and trim(nf.arquivo_url) <> '') as tem_arquivo_nf
        from nota_fiscal_itens nfi
        join notas_fiscais nf on nf.id = nfi.nota_fiscal_id
        where nfi.pedido_item_id is not null
        group by nfi.pedido_item_id
    ),
    comprovante_resumo as (
        select
            cb.compra_id,
            count(cb.id) as total_comprovantes_bancarios,
            string_agg(distinct cb.nome_arquivo, ', ') as comprovantes_bancarios,
            bool_or(cb.google_drive_link is not null and trim(cb.google_drive_link) <> '') as tem_comprovante_bancario
        from comprovantes_bancarios cb
        group by cb.compra_id
    ),
    destino_resumo as (
        select
            nfi.pedido_item_id,
            max(p.id::text)::uuid as patrimonio_id,
            max(ec.id::text)::uuid as estoque_id,
            max(ats.id::text)::uuid as atesto_id
        from nota_fiscal_itens nfi
        left join patrimonio p on p.nota_fiscal_item_id = nfi.id
        left join estoque_consumo ec on ec.nota_fiscal_item_id = nfi.id
        left join atesto_servico ats on ats.nota_fiscal_item_id = nfi.id
        where nfi.pedido_item_id is not null
        group by nfi.pedido_item_id
    )
    select
        pi.id as pedido_item_id,
        s.id as solicitacao_id,
        c.id as compra_id,
        r.id as rubrica_id,
        r.codigo as rubrica_codigo,
        r.nome as rubrica_nome,
        r.valor_orcado as rubrica_saldo_inicial,
        r.valor_reservado as rubrica_valor_reservado,
        r.valor_utilizado as rubrica_valor_utilizado,
        (
            r.valor_orcado
            - round((r.valor_orcado * r.reserva_tecnica_percentual / 100.0), 2)
            - r.valor_reservado
            - r.valor_utilizado
        ) as rubrica_saldo_restante,

        pi.descricao,
        pi.tipo_item,
        pi.quantidade,
        pi.valor_total as valor_solicitado,

        s.status as status_solicitacao,
        s.autorizado,
        case when s.autorizado then pi.valor_total else 0 end as valor_autorizado,

        coalesce(cr.total_cotacoes, 0) as total_cotacoes,
        coalesce(cr.total_vencedoras, 0) as total_vencedoras,
        cr.fornecedor_vencedor,
        coalesce(cr.valor_cotado_vencedor, 0) as valor_cotado_vencedor,

        coalesce(nr.total_itens_nf, 0) as total_itens_nf,
        nr.notas_fiscais,
        nr.fornecedores_nf,
        coalesce(nr.valor_total_nf_item, 0) as valor_nf_item,
        greatest(
            pi.valor_total - coalesce(nullif(nr.valor_total_nf_item, 0), cr.valor_cotado_vencedor, 0),
            0
        ) as valor_economia,
        coalesce(nr.tem_arquivo_nf, false) as tem_arquivo_nf,
        coalesce(cbr.total_comprovantes_bancarios, 0) as total_comprovantes_bancarios,
        cbr.comprovantes_bancarios,
        coalesce(cbr.tem_comprovante_bancario, false) as tem_comprovante_bancario,

        dr.patrimonio_id,
        dr.estoque_id,
        dr.atesto_id,

        case
            when pi.descricao is null or trim(pi.descricao) = ''
                then 'ERRO: item sem descricao'
            when pi.tipo_item not in ('permanente', 'consumo', 'servico')
                then 'ERRO: tipo de item invalido'
            when pi.valor_total <= 0
                then 'ERRO: item sem valor'
            when s.id is null
                then 'ERRO: item sem solicitacao'
            when coalesce(cr.total_cotacoes, 0) = 0
                then 'PENDENTE: item sem cotacao'
            when coalesce(cr.total_vencedoras, 0) = 0
                then 'PENDENTE: item sem fornecedor vencedor'
            when coalesce(cr.total_vencedoras, 0) > 1
                then 'ERRO: item com mais de um vencedor'
            when coalesce(cr.valor_cotado_vencedor, 0) - pi.valor_total > 0.01
                then 'ERRO: valor cotado maior que solicitado'
            when coalesce(nr.total_itens_nf, 0) = 0
                then 'PENDENTE: item sem nota fiscal'
            when coalesce(nr.valor_total_nf_item, 0) - coalesce(cr.valor_cotado_vencedor, 0) > 0.01
                then 'ERRO: valor da NF maior que cotacao vencedora'
            when nr.fornecedores_nf is distinct from cr.fornecedor_vencedor
                then 'ERRO: fornecedor da NF diverge do vencedor'
            when coalesce(nr.tem_arquivo_nf, false) = false
                then 'PENDENTE: NF sem local/link no Drive'
            when coalesce(cbr.tem_comprovante_bancario, false) = false
                then 'PENDENTE: compra sem comprovante bancario'
            when pi.tipo_item = 'permanente' and dr.patrimonio_id is null
                then 'PENDENTE: permanente sem patrimonio'
            when pi.tipo_item = 'consumo' and dr.estoque_id is null
                then 'PENDENTE: consumo sem estoque'
            when pi.tipo_item = 'servico' and dr.atesto_id is null
                then 'PENDENTE: servico sem atesto'
            else 'OK'
        end as status_auditoria
    from pedido_itens pi
    join solicitacoes_compra s on s.id = pi.pedido_id
    join rubricas r on r.id = pi.rubrica_id
    left join compras c on c.solicitacao_id = s.id
    left join cotacao_resumo cr on cr.pedido_item_id = pi.id
    left join nota_resumo nr on nr.pedido_item_id = pi.id
    left join comprovante_resumo cbr on cbr.compra_id = c.id
    left join destino_resumo dr on dr.pedido_item_id = pi.id;
    select pg_advisory_unlock(2026052601);
    """)

def ensure_permissions_schema():
    if not has_column("usuarios_app", "permissoes"):
        st.error("O banco precisa da coluna de permissões para iniciar o app.")
        st.caption("Execute este SQL no Supabase SQL Editor e reinicie o app no Streamlit Cloud.")
        st.code(
            "alter table usuarios_app add column permissoes text[] not null default array[]::text[];",
            language="sql",
        )
        st.stop()

    execute("""
    update usuarios_app
    set permissoes = array['orcamento','nova_exigencia','solicitacoes','cotacoes','compra_nota','comprovantes_bancarios','destino_final','auditoria','ia_operacional','itens_comprados','membros']
    where papel = 'admin' and (permissoes is null or cardinality(permissoes) = 0)
    """)
    execute("""
    update usuarios_app
    set permissoes = array_append(permissoes, 'comprovantes_bancarios')
    where papel = 'admin' and not ('comprovantes_bancarios' = any(permissoes))
    """)

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def check_password(password: str, senha_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), senha_hash.encode())

def format_brl(value) -> str:
    try:
        value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        value = Decimal("0")
    formatted = f"{value:,.2f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")

def format_currency_brl(valor) -> str:
    return f"R$ {format_brl(valor)}"

def format_currency_brl_markdown(valor) -> str:
    return format_currency_brl(valor).replace("$", r"\$")

def apenas_digitos(valor) -> str:
    return re.sub(r"\D", "", str(valor or ""))

def format_cpf_cnpj(valor) -> str:
    digitos = apenas_digitos(valor)
    if len(digitos) == 11:
        return f"{digitos[:3]}.{digitos[3:6]}.{digitos[6:9]}-{digitos[9:]}"
    if len(digitos) == 14:
        return f"{digitos[:2]}.{digitos[2:5]}.{digitos[5:8]}/{digitos[8:12]}-{digitos[12:]}"
    return str(valor or "").strip()

def formatar_cpf_cnpj_session_state(chave):
    st.session_state[chave] = format_cpf_cnpj(st.session_state.get(chave, ""))

def format_percent_brl(value) -> str:
    return f"{format_brl(value)}%"

TEXTOS_PT_BR = {
    "cotacao": "cotação",
    "Cotacao": "Cotação",
    "solicitacao": "solicitação",
    "Solicitacao": "Solicitação",
    "patrimonio": "patrimônio",
    "Patrimonio": "Patrimônio",
    "orcamento": "orçamento",
    "Orcamento": "Orçamento",
    "critica": "crítica",
    "Critica": "Crítica",
    "Pendencia": "Pendência",
    "pendencia": "pendência",
    "descricao": "descrição",
    "Descricao": "Descrição",
    "esta": "está",
    "ja": "já",
    "ha": "há",
    "nao": "não",
    "PENDENTE:": "Pendente:",
    "ERRO:": "Erro:",
    "ALERTA:": "Alerta:",
}

COLUNAS_IA = {
    "id": "ID",
    "tipo": "Tipo",
    "titulo": "Título",
    "descricao": "Descrição",
    "gravidade": "Gravidade",
    "origem": "Origem",
    "tabela_origem": "Tabela de origem",
    "registro_origem_id": "Registro de origem",
    "status": "Status",
    "sugestao_acao": "Sugestão de ação",
    "criado_em": "Criado em",
    "resolvido_em": "Resolvido em",
}

VALORES_IA = {
    "rubrica_critica": "Rubrica crítica",
    "saldo_insuficiente": "Saldo insuficiente",
    "cotacao_atrasada": "Cotação atrasada",
    "valor_divergente": "Valor divergente",
    "item_sem_patrimonio": "Item sem patrimônio",
    "item_sem_estoque": "Item sem estoque",
    "nota_fiscal_pendente": "Nota fiscal pendente",
    "fornecedor_recorrente": "Fornecedor recorrente",
    "risco_orcamentario": "Risco orçamentário",
    "baixa": "Baixa",
    "media": "Média",
    "alta": "Alta",
    "pendente": "Pendente",
    "resolvido": "Resolvido",
}

def normalizar_texto_portugues(valor):
    if valor is None or pd.isna(valor):
        return ""
    texto = str(valor)
    for origem, destino in TEXTOS_PT_BR.items():
        texto = texto.replace(origem, destino)
    return texto

def preparar_tabela_ia(df: pd.DataFrame) -> pd.DataFrame:
    tabela = df.rename(columns=COLUNAS_IA).copy()
    for coluna in tabela.columns:
        if tabela[coluna].dtype == "object" or pd.api.types.is_string_dtype(tabela[coluna]):
            tabela[coluna] = tabela[coluna].apply(
                lambda valor: VALORES_IA.get(str(valor), normalizar_texto_portugues(valor))
            )
    return tabela.fillna("")

def financial_status(row) -> str:
    saldo_disponivel = Decimal(str(row.get("saldo_disponivel", 0)))
    valor_minimo = Decimal(str(row.get("valor_minimo_operacional", 0)))
    percentual_comprometido = Decimal(str(row.get("percentual_comprometido", 0)))

    if bool(row.get("encerrada", False)) or saldo_disponivel <= 0:
        return "Encerrado"
    if valor_minimo > 0 and saldo_disponivel < valor_minimo:
        return "Residual"
    if percentual_comprometido > 90:
        return "Critico"
    if percentual_comprometido > 70:
        return "Comprometido"
    return "Disponivel"

def status_alert_level(status: str) -> str:
    return {
        "Encerrado": "Cinza",
        "Residual": "Vermelho",
        "Critico": "Laranja",
        "Comprometido": "Amarelo",
        "Disponivel": "Verde",
        "Normal": "Verde",
    }.get(status, "Verde")

def risk_color_css(risk: str) -> str:
    return {
        "Verde": "#16a34a",
        "Amarelo": "#ca8a04",
        "Laranja": "#ea580c",
        "Vermelho": "#dc2626",
        "Cinza": "#6b7280",
    }.get(risk, "#16a34a")

def percentual_periodo_prestacao(hoje=None) -> float:
    hoje = hoje or date.today()
    total_dias = max((PERIODO_PRESTACAO_FIM - PERIODO_PRESTACAO_INICIO).days, 1)
    dias_passados = (min(max(hoje, PERIODO_PRESTACAO_INICIO), PERIODO_PRESTACAO_FIM) - PERIODO_PRESTACAO_INICIO).days
    return round((dias_passados * 100.0) / total_dias, 2)

def classificar_risco_prazo(percentual_compras, percentual_tempo, saldo_disponivel=0, encerrada=False) -> str:
    try:
        percentual_compras = float(percentual_compras or 0)
        percentual_tempo = float(percentual_tempo or 0)
        saldo_disponivel = float(saldo_disponivel or 0)
    except (TypeError, ValueError):
        percentual_compras = 0.0
        percentual_tempo = 0.0
        saldo_disponivel = 0.0

    if encerrada or saldo_disponivel <= 0:
        return "Cinza"
    if percentual_tempo >= 98 and percentual_compras < 100:
        return "Vermelho"
    if percentual_tempo <= 0:
        return "Verde"
    eficiencia = percentual_compras / percentual_tempo
    if eficiencia >= 0.9:
        return "Verde"
    if eficiencia >= 0.7:
        return "Amarelo"
    if eficiencia >= 0.5:
        return "Laranja"
    return "Vermelho"

def descrever_risco_prazo(risco: str) -> str:
    return {
        "Verde": "No ritmo",
        "Amarelo": "Atenção",
        "Laranja": "Atraso",
        "Vermelho": "Crítico",
        "Cinza": "Encerrado/sem saldo",
    }.get(risco, "No ritmo")

def descrever_status_financeiro(status: str) -> str:
    return {
        "Critico": "Crítico",
        "Disponivel": "Disponível",
    }.get(status, status)

def carregar_compras_por_mes_orcamento():
    return query("""
    select
      date_trunc('month', c.comprado_em)::date as mes,
      coalesce(sum(c.valor_compra), 0) as valor_compras,
      count(distinct c.id) as compras
    from compras c
    join solicitacoes_compra s on s.id = c.solicitacao_id
    where c.comprado_em::date between %s and %s
    group by 1
    order by 1
    """, (PERIODO_PRESTACAO_INICIO, PERIODO_PRESTACAO_FIM))

def carregar_valores_extra_nao_debitados(compra_id=None):
    filtro = ""
    params = []
    if compra_id is not None:
        filtro = "where v.compra_id = %s"
        params.append(int(compra_id))
    return query(f"""
    select
      v.id,
      v.compra_id,
      v.nota_fiscal_id,
      coalesce(r.codigo, '-') as rubrica,
      v.solicitacao_id,
      v.tipo,
      v.descricao,
      v.valor,
      v.responsavel_pagamento,
      v.data_pagamento,
      v.criado_em
    from valores_extra_nao_debitados v
    left join rubricas r on r.id = v.rubrica_id
    {filtro}
    order by v.criado_em desc
    """, tuple(params))

def exibir_resumo_valores_extra_nao_debitados():
    valores_extra = carregar_valores_extra_nao_debitados()
    total_extra = valores_extra["valor"].sum() if len(valores_extra) else Decimal("0")
    with st.expander("Valores extras nao debitados do projeto", expanded=False):
        st.metric("Total a pagar fora do projeto", format_currency_brl(total_extra))
        if len(valores_extra):
            tabela = valores_extra.rename(columns={
                "rubrica": "Rubrica",
                "solicitacao_id": "Solicitacao",
                "tipo": "Tipo",
                "descricao": "Descricao",
                "valor": "Valor",
                "responsavel_pagamento": "Responsavel pelo pagamento",
                "data_pagamento": "Data",
                "criado_em": "Registrado em",
            })[["Rubrica", "Solicitacao", "Tipo", "Descricao", "Valor", "Responsavel pelo pagamento", "Data", "Registrado em"]].copy()
            tabela["Valor"] = tabela["Valor"].apply(format_currency_brl)
            st.dataframe(tabela, use_container_width=True, hide_index=True)
        else:
            st.info("Ainda nao ha valores extras registrados.")

def excede_saldo_disponivel(rubrica_id: int, valor: Decimal) -> tuple[bool, Decimal]:
    saldo_df = query("select saldo_disponivel from vw_orcamento where id=%s", (rubrica_id,))
    saldo = Decimal(str(saldo_df.iloc[0]["saldo_disponivel"])) if len(saldo_df) == 1 else Decimal("0")
    return valor > saldo, saldo

CENTAVO = Decimal("0.01")

def arredondar_centavos(valor):
    return Decimal(str(valor)).quantize(CENTAVO, rounding=ROUND_HALF_UP)

def calcular_reserva_tecnica(valor_orcado, reserva_percentual):
    valor_orcado = Decimal(str(valor_orcado))
    reserva_percentual = Decimal(str(reserva_percentual or 0))
    return arredondar_centavos(valor_orcado * reserva_percentual / Decimal("100"))

def saldo_operacional_calculado(valor_orcado, reserva_percentual, valor_reservado=0, valor_utilizado=0):
    valor_orcado = Decimal(str(valor_orcado))
    valor_reservado = Decimal(str(valor_reservado or 0))
    valor_utilizado = Decimal(str(valor_utilizado or 0))
    return (
        valor_orcado
        - calcular_reserva_tecnica(valor_orcado, reserva_percentual)
        - valor_reservado
        - valor_utilizado
    )

def valor_orcado_para_reduzir_saldo_operacional(
    valor_operacional,
    valor_orcado_atual,
    reserva_percentual,
    saldo_operacional_atual=None,
    valor_reservado=0,
    valor_utilizado=0,
):
    valor_operacional = arredondar_centavos(valor_operacional)
    valor_orcado_atual = Decimal(str(valor_orcado_atual))
    reserva_percentual = Decimal(str(reserva_percentual or 0))
    valor_reservado = Decimal(str(valor_reservado or 0))
    valor_utilizado = Decimal(str(valor_utilizado or 0))
    fator_disponivel = Decimal("1") - (reserva_percentual / Decimal("100"))
    if fator_disponivel <= 0:
        raise ValueError("A reserva tecnica da rubrica impede calcular o remanejamento.")

    valor_orcado = arredondar_centavos(valor_operacional / fator_disponivel)
    if saldo_operacional_atual is None:
        saldo_atual = saldo_operacional_calculado(
            valor_orcado_atual,
            reserva_percentual,
            valor_reservado,
            valor_utilizado,
        )
    else:
        saldo_atual = Decimal(str(saldo_operacional_atual))
    if valor_operacional >= saldo_atual:
        saldo_alvo = Decimal("0")
    else:
        saldo_alvo = saldo_atual - valor_operacional

    def saldo_apos_reducao(reducao):
        return saldo_operacional_calculado(
            valor_orcado_atual - reducao,
            reserva_percentual,
            valor_reservado,
            valor_utilizado,
        )

    while saldo_apos_reducao(valor_orcado) > saldo_alvo and valor_orcado < valor_orcado_atual:
        valor_orcado += CENTAVO
    while valor_orcado > CENTAVO and saldo_apos_reducao(valor_orcado - CENTAVO) <= saldo_alvo:
        valor_orcado -= CENTAVO

    return arredondar_centavos(valor_orcado)

def parse_responsaveis(value) -> list[str]:
    if value is None or pd.isna(value):
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]

def nome_aba_excel(nome: str, usadas: set[str]) -> str:
    caracteres_invalidos = "[]:*?/\\"
    base = "".join("_" if char in caracteres_invalidos else char for char in str(nome or "Rubrica"))
    base = base.strip()[:31] or "Rubrica"
    nome_final = base
    contador = 2
    while nome_final in usadas:
        sufixo = f"_{contador}"
        nome_final = f"{base[:31 - len(sufixo)]}{sufixo}"
        contador += 1
    usadas.add(nome_final)
    return nome_final

def construir_planilha_itens_comprados(df: pd.DataFrame) -> bytes:
    planilha = df.copy()
    for coluna in ["Quantidade", "Valor da compra", "Valor da NF"]:
        planilha[coluna] = pd.to_numeric(planilha[coluna], errors="coerce").fillna(0)
    for coluna in ["Data de emissão", "Lançado em"]:
        planilha[coluna] = planilha[coluna].astype(str).replace({"NaT": "", "None": ""})

    resumo = (
        planilha
        .groupby(["Rubrica", "Nome da rubrica"], dropna=False)
        .agg(
            Itens=("Solicitação", "count"),
            Total_compra=("Valor da compra", "sum"),
            Total_nf=("Valor da NF", "sum"),
        )
        .reset_index()
        .rename(columns={
            "Total_compra": "Total da compra",
            "Total_nf": "Total da NF",
        })
    )

    arquivo = BytesIO()
    with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
        resumo.to_excel(writer, index=False, sheet_name="Resumo por rubrica")
        abas_usadas = {"Resumo por rubrica"}
        for rubrica, itens_rubrica in planilha.groupby("Rubrica", dropna=False):
            nome_aba = nome_aba_excel(rubrica, abas_usadas)
            itens_rubrica.to_excel(writer, index=False, sheet_name=nome_aba)

        for worksheet in writer.book.worksheets:
            for column_cells in worksheet.columns:
                largura = max(len(str(cell.value or "")) for cell in column_cells)
                worksheet.column_dimensions[column_cells[0].column_letter].width = min(max(largura + 2, 12), 50)

    arquivo.seek(0)
    return arquivo.getvalue()

COLUNAS_AUDITORIA = {
    "pedido_item_id": "Item do pedido (ID)",
    "compra_id": "Compra",
    "rubrica_id": "Rubrica (ID)",
    "rubrica_codigo": "Rubrica",
    "rubrica_nome": "Nome da rubrica",
    "rubrica_saldo_inicial": "Saldo inicial",
    "rubrica_valor_reservado": "Valor reservado",
    "rubrica_valor_utilizado": "Valor utilizado",
    "rubrica_saldo_restante": "Saldo restante",
    "solicitacao_id": "Solicitação",
    "descricao": "Descrição",
    "tipo_item": "Tipo do item",
    "quantidade": "Quantidade",
    "status_solicitacao": "Status da solicitação",
    "autorizado": "Autorizado",
    "existe_solicitacao": "Existe solicitação",
    "tem_valor": "Tem valor",
    "tipo_valido": "Tipo válido",
    "total_cotacoes": "Total de cotações",
    "total_vencedoras": "Cotações vencedoras",
    "fornecedor_vencedor": "Fornecedor vencedor",
    "tem_cotacao": "Tem cotação",
    "tem_vencedor": "Tem vencedor",
    "valor_bate": "Valor confere",
    "notas_fiscais": "Notas fiscais",
    "fornecedores_nf": "Fornecedor da NF",
    "total_itens_nf": "Itens na NF",
    "tem_arquivo_nf": "Local/link da NF informado",
    "tem_item_nf": "Tem item na NF",
    "valor_nf_bate": "Valor da NF confere",
    "fornecedor_bate": "Fornecedor confere",
    "total_comprovantes_bancarios": "Total de comprovantes bancários",
    "comprovantes_bancarios": "Comprovantes bancários",
    "tem_comprovante_bancario": "Tem comprovante bancário",
    "patrimonio_id": "Patrimônio",
    "estoque_id": "Estoque",
    "atesto_id": "Atesto",
    "status_auditoria": "Status da auditoria",
    "destino_correto": "Destino correto",
    "saldo_inicial": "Saldo inicial",
    "valor_solicitado": "Valor solicitado",
    "valor_autorizado": "Valor autorizado",
    "valor_empenhado_comprado": "Valor empenhado/comprado",
    "valor_reservado": "Valor reservado",
    "valor_utilizado": "Valor utilizado",
    "saldo_restante": "Saldo restante",
    "valor_cotado_vencedor": "Valor cotado vencedor",
    "valor_nf_item": "Valor do item na NF",
    "valor_economia": "Valor economizado",
    "valor_nota": "Valor da nota",
    "valor_itens": "Valor dos itens",
    "diferenca": "Diferença",
    "numero_nf": "Número da NF",
    "fornecedor": "Fornecedor",
    "status_conferencia": "Status da conferência",
}

COLUNAS_VALOR_AUDITORIA = {
    "Saldo inicial",
    "Valor solicitado",
    "Valor autorizado",
    "Valor empenhado/comprado",
    "Valor reservado",
    "Valor utilizado",
    "Saldo restante",
    "Valor cotado vencedor",
    "Valor do item na NF",
    "Valor economizado",
    "Valor da nota",
    "Valor dos itens",
    "Diferença",
}

def preparar_tabela_auditoria(df: pd.DataFrame) -> pd.DataFrame:
    tabela = df.rename(columns=COLUNAS_AUDITORIA).copy()
    for coluna in tabela.columns:
        if tabela[coluna].dtype == "object" or pd.api.types.is_string_dtype(tabela[coluna]):
            tabela[coluna] = tabela[coluna].apply(normalizar_texto_portugues)
    for coluna in COLUNAS_VALOR_AUDITORIA.intersection(tabela.columns):
        tabela[coluna] = tabela[coluna].apply(format_currency_brl)
    return tabela.fillna("")

@st.dialog("Atualizar responsáveis")
def atualizar_responsaveis_dialog():
    rubricas = query("""
    select id, codigo, nome, coalesce(responsaveis, '') as responsaveis
    from rubricas
    where ativo = true
    order by codigo
    """)
    if len(rubricas) == 0:
        st.info("Não há rubricas ativas para atualizar.")
        return

    rubrica_id = st.selectbox(
        "Rubrica",
        rubricas["id"].tolist(),
        format_func=lambda item_id: (
            f"{rubricas.loc[rubricas.id == item_id, 'codigo'].iloc[0]} - "
            f"{rubricas.loc[rubricas.id == item_id, 'nome'].iloc[0]}"
        ),
    )
    rubrica = rubricas.loc[rubricas.id == rubrica_id].iloc[0]
    responsaveis_atuais = parse_responsaveis(rubrica["responsaveis"])

    membros = query("""
    select split_part(trim(nome), ' ', 1) as usuario
    from usuarios_app
    where ativo = true
    order by usuario
    """)
    opcoes = membros["usuario"].tolist() if len(membros) else []
    for responsavel in responsaveis_atuais:
        if responsavel not in opcoes:
            opcoes.append(responsavel)

    responsaveis = st.multiselect(
        "Responsáveis",
        opcoes,
        default=responsaveis_atuais,
        placeholder="Selecione um ou mais responsáveis",
    )

    c1, c2 = st.columns(2)
    if c1.button("Salvar", type="primary", use_container_width=True):
        execute(
            "update rubricas set responsaveis=%s where id=%s",
            (", ".join(responsaveis) if responsaveis else None, int(rubrica_id)),
        )
        st.success("Responsáveis atualizados.")
        st.rerun()
    if c2.button("Cancelar", use_container_width=True):
        st.rerun()

@st.dialog("Remanejar saldo")
def remanejar_saldo_dialog(usuario_id):
    rubricas = query("""
    select id, codigo, nome, valor_orcado, valor_reservado, valor_utilizado, reserva_tecnica_percentual, saldo_disponivel
    from vw_orcamento
    where encerrada = false
    order by codigo
    """)
    if len(rubricas) < 2:
        st.info("Sao necessarias pelo menos duas rubricas ativas para remanejamento.")
        return

    def label_rubrica(item_id):
        rubrica = rubricas.loc[rubricas.id == item_id].iloc[0]
        return f"{rubrica['codigo']} - {rubrica['nome']} ({format_currency_brl(rubrica['saldo_disponivel'])})"

    origem_id = st.selectbox("Rubrica origem", rubricas["id"].tolist(), format_func=label_rubrica)
    destino_id = st.selectbox("Rubrica destino", rubricas["id"].tolist(), format_func=label_rubrica)
    rubrica_origem = rubricas.loc[rubricas.id == origem_id].iloc[0]
    saldo_origem = Decimal(str(rubrica_origem["saldo_disponivel"]))
    valor_maximo = float(max(saldo_origem, Decimal("0.01")))
    valor = st.number_input("Valor operacional a remanejar", min_value=0.01, max_value=valor_maximo, value=0.01, step=100.0)
    st.caption("O valor informado e abatido do disponivel operacional. O sistema ajusta o valor orcado considerando a reserva tecnica.")
    justificativa = st.text_area("Justificativa formal")

    c1, c2 = st.columns(2)
    if c1.button("Confirmar remanejamento", type="primary", use_container_width=True):
        valor_decimal = Decimal(str(valor))
        if origem_id == destino_id:
            st.error("A rubrica de origem deve ser diferente da rubrica de destino.")
        elif valor_decimal > saldo_origem:
            st.error("O valor informado supera o saldo disponivel da rubrica de origem.")
        elif not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            remanejamento_id = str(uuid4())
            valor_orcado_movimentado = valor_orcado_para_reduzir_saldo_operacional(
                valor_decimal,
                rubrica_origem["valor_orcado"],
                rubrica_origem["reserva_tecnica_percentual"],
                rubrica_origem["saldo_disponivel"],
                rubrica_origem["valor_reservado"],
                rubrica_origem["valor_utilizado"],
            )
            justificativa_auditoria = (
                f"{justificativa.strip()} | Valor operacional informado: {format_currency_brl(valor_decimal)}. "
                f"Valor orcado movimentado com reserva tecnica: {format_currency_brl(valor_orcado_movimentado)}."
            )
            execute("update rubricas set valor_orcado = valor_orcado - %s where id = %s", (valor_orcado_movimentado, int(origem_id)))
            execute("update rubricas set valor_orcado = valor_orcado + %s where id = %s", (valor_orcado_movimentado, int(destino_id)))
            execute(
                """
                insert into movimentacoes_orcamento
                  (rubrica_id, usuario_id, operacao, valor, justificativa, remanejamento_id)
                values (%s,%s,'remanejamento_saida',%s,%s,%s)
                """,
                (int(origem_id), usuario_id, valor_orcado_movimentado, justificativa_auditoria, remanejamento_id),
            )
            execute(
                """
                insert into movimentacoes_orcamento
                  (rubrica_id, usuario_id, operacao, valor, justificativa, remanejamento_id)
                values (%s,%s,'remanejamento_entrada',%s,%s,%s)
                """,
                (int(destino_id), usuario_id, valor_orcado_movimentado, justificativa_auditoria, remanejamento_id),
            )
            st.success("Remanejamento registrado.")
            st.rerun()
    if c2.button("Cancelar", use_container_width=True):
        st.rerun()

def voltar_remanejamento(remanejamento_id, usuario_id, justificativa_retorno):
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
            select
              saida.id as saida_id,
              entrada.id as entrada_id,
              saida.rubrica_id as origem_id,
              entrada.rubrica_id as destino_id,
              saida.valor,
              saida.justificativa
            from movimentacoes_orcamento saida
            join movimentacoes_orcamento entrada
              on entrada.remanejamento_id = saida.remanejamento_id
             and entrada.operacao = 'remanejamento_entrada'
            where saida.remanejamento_id=%s
              and saida.operacao = 'remanejamento_saida'
              and saida.estornado_em is null
              and entrada.estornado_em is null
            for update
            """, (remanejamento_id,))
            remanejamento = cur.fetchone()
            if not remanejamento:
                raise ValueError("Remanejamento nao encontrado ou ja estornado.")

            valor = Decimal(str(remanejamento["valor"]))
            origem_id = int(remanejamento["origem_id"])
            destino_id = int(remanejamento["destino_id"])

            cur.execute("""
            select id, valor_orcado, valor_reservado, valor_utilizado, reserva_tecnica_percentual
            from rubricas
            where id in (%s, %s)
            order by id
            for update
            """, (origem_id, destino_id))
            rubricas_travadas = {int(row["id"]): row for row in cur.fetchall()}
            if origem_id not in rubricas_travadas or destino_id not in rubricas_travadas:
                raise ValueError("Rubrica de destino nao encontrada.")
            rubrica_destino = rubricas_travadas[destino_id]
            saldo_destino_apos_retorno = saldo_operacional_calculado(
                Decimal(str(rubrica_destino["valor_orcado"])) - valor,
                rubrica_destino["reserva_tecnica_percentual"],
                rubrica_destino["valor_reservado"],
                rubrica_destino["valor_utilizado"],
            )
            if saldo_destino_apos_retorno < 0:
                raise ValueError(
                    "A rubrica que recebeu o remanejamento nao tem saldo operacional suficiente para devolver."
                )

            cur.execute("update rubricas set valor_orcado = valor_orcado - %s where id = %s", (valor, destino_id))
            cur.execute("update rubricas set valor_orcado = valor_orcado + %s where id = %s", (valor, origem_id))
            cur.execute("""
            update movimentacoes_orcamento
            set estornado_em=now(), estornado_por=%s
            where id in (%s, %s)
            """, (usuario_id, int(remanejamento["saida_id"]), int(remanejamento["entrada_id"])))
            justificativa_auditoria = (
                f"Estorno do remanejamento {remanejamento_id}. "
                f"Justificativa original: {remanejamento['justificativa'] or '-'} "
                f"Justificativa do retorno: {justificativa_retorno}"
            )
            cur.execute("""
            insert into movimentacoes_orcamento
              (rubrica_id, usuario_id, operacao, valor, justificativa, remanejamento_id)
            values (%s,%s,'retorno_remanejamento_saida',%s,%s,%s)
            """, (destino_id, usuario_id, valor, justificativa_auditoria, remanejamento_id))
            cur.execute("""
            insert into movimentacoes_orcamento
              (rubrica_id, usuario_id, operacao, valor, justificativa, remanejamento_id)
            values (%s,%s,'retorno_remanejamento_entrada',%s,%s,%s)
            """, (origem_id, usuario_id, valor, justificativa_auditoria, remanejamento_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@st.dialog("Voltar remanejamento")
def voltar_remanejamento_dialog(usuario_id):
    remanejamentos = query("""
    select
      saida.remanejamento_id,
      saida.criado_em,
      origem.codigo as origem_codigo,
      origem.nome as origem_nome,
      destino.codigo as destino_codigo,
      destino.nome as destino_nome,
      saida.valor,
      saida.justificativa
    from movimentacoes_orcamento saida
    join movimentacoes_orcamento entrada
      on entrada.remanejamento_id = saida.remanejamento_id
     and entrada.operacao = 'remanejamento_entrada'
    join rubricas origem on origem.id = saida.rubrica_id
    join rubricas destino on destino.id = entrada.rubrica_id
    where saida.operacao = 'remanejamento_saida'
      and saida.remanejamento_id is not null
      and saida.estornado_em is null
      and entrada.estornado_em is null
    order by saida.criado_em desc, saida.id desc
    """)
    if len(remanejamentos) == 0:
        st.info("Nao ha remanejamentos rastreaveis em aberto para voltar.")
        return

    remanejamento_id = st.selectbox(
        "Remanejamento",
        remanejamentos["remanejamento_id"].tolist(),
        format_func=lambda item_id: (
            f"{remanejamentos.loc[remanejamentos.remanejamento_id == item_id, 'origem_codigo'].iloc[0]} -> "
            f"{remanejamentos.loc[remanejamentos.remanejamento_id == item_id, 'destino_codigo'].iloc[0]} | "
            f"{format_currency_brl(remanejamentos.loc[remanejamentos.remanejamento_id == item_id, 'valor'].iloc[0])}"
        ),
    )
    selecionado = remanejamentos.loc[remanejamentos.remanejamento_id == remanejamento_id].iloc[0]
    st.write(
        f"Origem original: {selecionado['origem_codigo']} - {selecionado['origem_nome']} | "
        f"Destino original: {selecionado['destino_codigo']} - {selecionado['destino_nome']}"
    )
    st.write(f"Valor a devolver: {format_currency_brl(selecionado['valor'])}")
    st.caption(f"Justificativa original: {selecionado['justificativa'] or '-'}")
    st.warning("A volta retira o valor da rubrica destino original e devolve para a rubrica origem original.")
    justificativa = st.text_area("Justificativa da volta")

    c1, c2 = st.columns(2)
    if c1.button("Confirmar volta", type="primary", use_container_width=True):
        if not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            try:
                voltar_remanejamento(remanejamento_id, usuario_id, justificativa.strip())
            except ValueError as exc:
                st.error(str(exc))
            else:
                st.success("Remanejamento voltou para a rubrica de origem.")
                st.rerun()
    if c2.button("Cancelar", use_container_width=True):
        st.rerun()

@st.dialog("Reservar valor")
def reservar_valor_dialog(usuario_id):
    rubricas = query("""
    select id, codigo, nome, saldo_disponivel
    from vw_orcamento
    where encerrada = false
    order by codigo
    """)
    if len(rubricas) == 0:
        st.info("Nao ha rubricas abertas para reserva.")
        return

    def label_rubrica(item_id):
        rubrica = rubricas.loc[rubricas.id == item_id].iloc[0]
        return f"{rubrica['codigo']} - {rubrica['nome']} ({format_currency_brl(rubrica['saldo_disponivel'])})"

    rubrica_id = st.selectbox("Rubrica", rubricas["id"].tolist(), format_func=label_rubrica)
    saldo = Decimal(str(rubricas.loc[rubricas.id == rubrica_id, "saldo_disponivel"].iloc[0]))
    valor_maximo = float(max(saldo, Decimal("0.01")))
    valor = st.number_input("Valor reservado", min_value=0.01, max_value=valor_maximo, value=0.01, step=100.0)
    descricao = st.text_input("Descricao da reserva", value="Reserva financeira administrativa")
    justificativa = st.text_area("Justificativa")

    if st.button("Registrar reserva", type="primary", use_container_width=True):
        valor_decimal = Decimal(str(valor))
        if valor_decimal > saldo:
            st.error("O valor informado supera o saldo disponivel da rubrica.")
        elif not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            execute("""
            insert into solicitacoes_compra
              (rubrica_id, solicitante_id, gerente_id, descricao, quantidade, unidade, valor_estimado, justificativa, status, autorizado, autorizado_em)
            values (%s,%s,%s,%s,1,'un',%s,%s,'em_andamento',true,now())
            """, (int(rubrica_id), usuario_id, usuario_id, descricao, valor_decimal, justificativa))
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'reserva_financeira',%s,%s)",
                (int(rubrica_id), usuario_id, valor_decimal, justificativa),
            )
            sincronizar_orcamento()
            st.success("Reserva registrada.")
            st.rerun()

@st.dialog("Encerrar rubrica")
def encerrar_rubrica_dialog(usuario_id):
    rubricas = query("""
    select id, codigo, nome
    from vw_orcamento
    where encerrada = false
    order by codigo
    """)
    if len(rubricas) == 0:
        st.info("Nao ha rubricas abertas para encerrar.")
        return

    rubrica_id = st.selectbox(
        "Rubrica",
        rubricas["id"].tolist(),
        format_func=lambda item_id: f"{rubricas.loc[rubricas.id == item_id, 'codigo'].iloc[0]} - {rubricas.loc[rubricas.id == item_id, 'nome'].iloc[0]}",
    )
    justificativa = st.text_area("Justificativa de encerramento")
    if st.button("Encerrar oficialmente", type="primary", use_container_width=True):
        if not justificativa.strip():
            st.error("Informe uma justificativa para auditoria.")
        else:
            execute(
                "update rubricas set encerrada = true, encerrada_em = now(), encerrada_por = %s where id = %s",
                (usuario_id, int(rubrica_id)),
            )
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'encerramento',0,%s)",
                (int(rubrica_id), usuario_id, justificativa),
            )
            st.success("Rubrica encerrada.")
            st.rerun()

@st.dialog("Historico/Auditoria")
def historico_orcamento_dialog():
    historico = query("""
    select
      m.criado_em as "Data",
      r.codigo as "Rubrica",
      coalesce(u.nome, 'Sistema') as "Usuario",
      m.operacao as "Operacao",
      m.valor as "Valor",
      m.justificativa as "Justificativa"
    from movimentacoes_orcamento m
    join rubricas r on r.id = m.rubrica_id
    left join usuarios_app u on u.id = m.usuario_id
    order by m.criado_em desc
    limit 200
    """)
    if len(historico) == 0:
        st.info("Ainda nao ha movimentacoes orcamentarias registradas.")
        return
    historico["Valor"] = historico["Valor"].apply(format_currency_brl)
    st.dataframe(historico, use_container_width=True, hide_index=True)

def exibir_detalhe_rubrica(rubrica):
    detalhes = pd.DataFrame(
        [
            ("Codigo", rubrica["codigo"]),
            ("Rubrica", rubrica["nome"]),
            ("Tipo", rubrica["tipo"]),
            ("Responsavel", rubrica.get("responsaveis") or "-"),
            ("Valor orcado", format_currency_brl(rubrica["valor_orcado"])),
            ("Valor reservado", format_currency_brl(rubrica["valor_reservado"])),
            ("Valor utilizado", format_currency_brl(rubrica["valor_utilizado"])),
            ("Reserva tecnica", format_currency_brl(rubrica["reserva_tecnica"])),
            ("Reserva tecnica (%)", format_percent_brl(rubrica["reserva_tecnica_percentual"])),
            ("Minimo operacional", format_currency_brl(rubrica["valor_minimo_operacional"])),
            ("Disponivel operacional", format_currency_brl(rubrica["saldo_disponivel"])),
            ("Saldo residual", format_currency_brl(rubrica["saldo_residual"])),
            ("Indice comprometido", format_percent_brl(rubrica["percentual_comprometido"])),
            ("Percentual utilizado", format_percent_brl(rubrica["percentual_utilizado"])),
            ("Status financeiro", rubrica["status_financeiro"]),
            ("Risco", rubrica["risco"]),
            ("Encerrada", "Sim" if bool(rubrica["encerrada"]) else "Nao"),
        ],
        columns=["Campo", "Valor"],
    )
    with st.container(border=True):
        st.markdown(f"### Analise da rubrica: {rubrica['codigo']}")
        st.dataframe(
            detalhes,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Campo": st.column_config.TextColumn("Campo", width="medium"),
                "Valor": st.column_config.TextColumn("Valor", width="large"),
            },
        )

def cancelar_solicitacao(solicitacao_id, usuario_id):
    compra = query("""
    select c.id
    from compras c
    join solicitacoes_compra s on s.id = c.solicitacao_id
    where c.solicitacao_id=%s
    """, (solicitacao_id,))
    if len(compra) == 1:
        compra_id = int(compra.iloc[0]["id"])
        execute("delete from notas_fiscais where compra_id=%s", (compra_id,))
        execute("delete from compras where id=%s", (compra_id,))

    execute("update cotacoes set vencedora=false where solicitacao_id=%s", (solicitacao_id,))
    execute("update solicitacoes_compra set status='cancelado', autorizado=false, atualizado_em=now() where id=%s", (solicitacao_id,))
    execute("insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao) values (%s,'cancelado',%s,'Solicitação cancelada')", (solicitacao_id, usuario_id))

    sincronizar_orcamento()

def voltar_item_para_cotacao(pedido_item_id, usuario_id):
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
            select pi.id, pi.pedido_id, pi.descricao
            from pedido_itens pi
            where pi.id=%s
            """, (pedido_item_id,))
            item = cur.fetchone()
            if not item:
                raise ValueError("Item do pedido nao encontrado.")

            solicitacao_id = int(item["pedido_id"])
            descricao_item = item["descricao"]

            cur.execute("""
            delete from patrimonio
            where nota_fiscal_item_id in (
                select id from nota_fiscal_itens where pedido_item_id=%s
            )
            """, (pedido_item_id,))
            cur.execute("""
            delete from estoque_consumo
            where nota_fiscal_item_id in (
                select id from nota_fiscal_itens where pedido_item_id=%s
            )
            """, (pedido_item_id,))
            cur.execute("""
            delete from atesto_servico
            where nota_fiscal_item_id in (
                select id from nota_fiscal_itens where pedido_item_id=%s
            )
            """, (pedido_item_id,))

            cur.execute("select distinct nota_fiscal_id from nota_fiscal_itens where pedido_item_id=%s", (pedido_item_id,))
            notas_afetadas = [row["nota_fiscal_id"] for row in cur.fetchall()]
            cur.execute("delete from nota_fiscal_itens where pedido_item_id=%s", (pedido_item_id,))
            if notas_afetadas:
                cur.execute("""
                delete from notas_fiscais nf
                where nf.id = any(%s)
                  and not exists (
                      select 1 from nota_fiscal_itens nfi where nfi.nota_fiscal_id = nf.id
                  )
                """, (notas_afetadas,))

            cur.execute("update cotacao_itens set vencedor=false where pedido_item_id=%s", (pedido_item_id,))
            cur.execute("""
            update cotacoes c
            set vencedora = exists (
                select 1
                from cotacao_itens ci
                where ci.cotacao_id = c.id and ci.vencedor = true
            )
            where c.solicitacao_id=%s
            """, (solicitacao_id,))
            cur.execute("update pedido_itens set status='em_cotacao' where id=%s", (pedido_item_id,))

            cur.execute("""
            select coalesce(sum(nfi.valor_total), 0) as valor_total_real
            from nota_fiscal_itens nfi
            join pedido_itens pi on pi.id = nfi.pedido_item_id
            where pi.pedido_id=%s
            """, (solicitacao_id,))
            valor_total_real = Decimal(str(cur.fetchone()["valor_total_real"]))
            if valor_total_real > 0:
                cur.execute("""
                update compras
                set valor_compra=%s
                where solicitacao_id=%s
                """, (valor_total_real, solicitacao_id))
            else:
                cur.execute("delete from compras where solicitacao_id=%s", (solicitacao_id,))

            cur.execute("""
            update solicitacoes_compra
            set status='cotado', atualizado_em=now()
            where id=%s
            """, (solicitacao_id,))
            cur.execute("""
            insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao)
            values (%s,'cotado',%s,%s)
            """, (
                solicitacao_id,
                usuario_id,
                f"Item retornado para cotacao: {descricao_item}",
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    sincronizar_orcamento()


def voltar_compra_para_nota_fiscal(solicitacao_id, usuario_id):
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
            select id, descricao
            from solicitacoes_compra
            where id=%s
            """, (int(solicitacao_id),))
            solicitacao = cur.fetchone()
            if not solicitacao:
                raise ValueError("Solicitacao nao encontrada.")

            cur.execute("""
            delete from patrimonio
            where nota_fiscal_item_id in (
                select nfi.id
                from nota_fiscal_itens nfi
                join pedido_itens pi on pi.id = nfi.pedido_item_id
                where pi.pedido_id=%s
            )
            """, (int(solicitacao_id),))
            cur.execute("""
            delete from estoque_consumo
            where nota_fiscal_item_id in (
                select nfi.id
                from nota_fiscal_itens nfi
                join pedido_itens pi on pi.id = nfi.pedido_item_id
                where pi.pedido_id=%s
            )
            """, (int(solicitacao_id),))
            cur.execute("""
            delete from atesto_servico
            where nota_fiscal_item_id in (
                select nfi.id
                from nota_fiscal_itens nfi
                join pedido_itens pi on pi.id = nfi.pedido_item_id
                where pi.pedido_id=%s
            )
            """, (int(solicitacao_id),))

            cur.execute("""
            update solicitacoes_compra
            set status='aguardando_nota',
                atualizado_em=now()
            where id=%s
            """, (int(solicitacao_id),))
            cur.execute("""
            insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao)
            values (%s,'aguardando_nota',%s,%s)
            """, (
                int(solicitacao_id),
                usuario_id,
                f"Compra retornada do destino final para correcao de nota fiscal: {solicitacao['descricao']}",
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    sincronizar_orcamento()


@st.dialog("Voltar para nota fiscal")
def voltar_compra_para_nota_fiscal_dialog(itens_destino, usuario_id):
    solicitacoes = itens_destino[["solicitacao", "rubrica"]].drop_duplicates().copy()
    if len(solicitacoes) == 0:
        st.info("Nao ha compras com nota fiscal para retornar.")
        return

    solicitacao_id = st.selectbox(
        "Compra",
        solicitacoes["solicitacao"].tolist(),
        format_func=lambda valor: (
            f"Solicitacao #{valor} - "
            f"Rubrica {solicitacoes.loc[solicitacoes.solicitacao == valor, 'rubrica'].iloc[0]}"
        ),
        key="destino_voltar_solicitacao",
    )
    st.warning("Os registros de patrimonio, estoque ou atesto desta compra serao removidos para retornar a etapa da nota fiscal.")
    if st.button("Voltar para nota fiscal", type="primary", use_container_width=True):
        try:
            voltar_compra_para_nota_fiscal(int(solicitacao_id), usuario_id)
        except (ValueError, psycopg2.Error) as exc:
            st.error(str(exc))
        else:
            st.success("Compra retornada para a etapa de nota fiscal.")
            st.rerun()

def ajustar_valor_solicitado_para_nf(pedido_item_id, usuario_id):
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
            select
                pi.id,
                pi.pedido_id,
                pi.descricao,
                pi.quantidade,
                pi.valor_total as valor_solicitado,
                s.status as status_solicitacao,
                (
                    select coalesce(sum(ci.valor_total), 0)
                    from cotacao_itens ci
                    where ci.pedido_item_id = pi.id and ci.vencedor = true
                ) as valor_cotado_vencedor,
                coalesce(sum(nfi.valor_total), 0) as valor_nf_item
            from pedido_itens pi
            join solicitacoes_compra s on s.id = pi.pedido_id
            left join nota_fiscal_itens nfi on nfi.pedido_item_id = pi.id
            where pi.id=%s
            group by pi.id, pi.pedido_id, pi.descricao, pi.quantidade, pi.valor_total, s.status
            """, (pedido_item_id,))
            item = cur.fetchone()
            if not item:
                raise ValueError("Item do pedido nao encontrado.")

            quantidade = Decimal(str(item["quantidade"] or 0))
            valor_nf_item = Decimal(str(item["valor_nf_item"] or 0))
            valor_cotado_vencedor = Decimal(str(item["valor_cotado_vencedor"] or 0))
            valor_solicitado = Decimal(str(item["valor_solicitado"] or 0))
            if quantidade <= 0:
                raise ValueError("Quantidade do item deve ser maior que zero.")
            if valor_nf_item <= 0:
                raise ValueError("Nao existe valor de NF para ajustar este item.")
            if abs(valor_nf_item - valor_cotado_vencedor) > Decimal("0.01"):
                raise ValueError("A NF nao confere com a cotacao vencedora. Volte o item para cotacao.")

            novo_valor_unitario = (valor_nf_item / quantidade).quantize(Decimal("0.01"))
            solicitacao_id = int(item["pedido_id"])
            descricao_item = item["descricao"]

            cur.execute("""
            update pedido_itens
            set valor_unitario=%s
            where id=%s
            """, (novo_valor_unitario, pedido_item_id))

            cur.execute("""
            update solicitacoes_compra s
            set valor_estimado = totais.valor_total,
                quantidade = totais.quantidade_total,
                atualizado_em = now()
            from (
                select
                    pedido_id,
                    coalesce(sum(valor_total), 0) as valor_total,
                    coalesce(sum(quantidade), 0) as quantidade_total
                from pedido_itens
                where pedido_id=%s
                group by pedido_id
            ) totais
            where s.id = totais.pedido_id
            """, (solicitacao_id,))

            cur.execute("""
            insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao)
            values (%s,%s,%s,%s)
            """, (
                solicitacao_id,
                item["status_solicitacao"],
                usuario_id,
                (
                    f"Valor solicitado do item ajustado para o valor da NF: {descricao_item}. "
                    f"De R$ {valor_solicitado:.2f} para R$ {valor_nf_item:.2f}."
                ),
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    sincronizar_orcamento()


@st.dialog("Editar nota fiscal")
def editar_numero_arquivo_nf_dialog(rubrica_id):
    notas = query("""
    select distinct
      nf.id,
      nf.compra_id,
      nf.solicitacao_id,
      nf.numero_nf,
      nf.fornecedor,
      nf.valor_nf,
      nf.arquivo_url,
      nf.lancado_em
    from notas_fiscais nf
    join nota_fiscal_itens nfi on nfi.nota_fiscal_id = nf.id
    join pedido_itens pi on pi.id = nfi.pedido_item_id
    where pi.rubrica_id=%s
    order by nf.lancado_em desc nulls last, nf.id desc
    """, (int(rubrica_id),))
    if len(notas) == 0:
        st.info("Nao ha notas fiscais salvas nesta rubrica.")
        return

    nota_id = st.selectbox(
        "Nota fiscal",
        notas["id"].tolist(),
        format_func=lambda valor: (
            f"{notas.loc[notas.id == valor, 'numero_nf'].iloc[0]} - "
            f"{notas.loc[notas.id == valor, 'fornecedor'].iloc[0]} - "
            f"{format_currency_brl(notas.loc[notas.id == valor, 'valor_nf'].iloc[0])}"
        ),
        key=f"editar_nf_documento_{rubrica_id}",
    )
    nota = notas.loc[notas.id == nota_id].iloc[0]
    numero_nf = st.text_input("Numero da NF", value=str(nota["numero_nf"] or ""), key=f"editar_nf_numero_{nota_id}")
    arquivo_nf = st.file_uploader(
        "Substituir PDF da nota fiscal",
        type=["pdf"],
        key=f"editar_nf_pdf_{nota_id}",
    )
    local_nf = str(nota["arquivo_url"] or "").strip()
    if local_nf:
        st.link_button("Abrir pasta atual da nota fiscal", local_nf)
    exibir_arquivos_nota_fiscal(int(nota_id))

    st.markdown("### Valor extra nao debitado do projeto")
    valores_extra_nf = query("""
    select tipo, descricao, valor, responsavel_pagamento, data_pagamento, criado_em
    from valores_extra_nao_debitados
    where nota_fiscal_id=%s
    order by criado_em desc
    """, (int(nota_id),))
    if len(valores_extra_nf):
        total_extra_nf = valores_extra_nf["valor"].sum()
        st.metric("Total extra desta NF", format_currency_brl(total_extra_nf))
        tabela_extra_nf = valores_extra_nf.rename(columns={
            "tipo": "Tipo",
            "descricao": "Descricao",
            "valor": "Valor",
            "responsavel_pagamento": "Responsavel",
            "data_pagamento": "Data",
            "criado_em": "Registrado em",
        })[["Tipo", "Descricao", "Valor", "Responsavel", "Data", "Registrado em"]].copy()
        tabela_extra_nf["Valor"] = tabela_extra_nf["Valor"].apply(format_currency_brl)
        st.dataframe(tabela_extra_nf, use_container_width=True, hide_index=True)
    extra_tipo_nf = st.selectbox(
        "Tipo do valor extra",
        ["Taxa TED", "Tarifa bancaria", "Frete extra", "Outro"],
        key=f"editar_nf_extra_tipo_{nota_id}",
    )
    extra_valor_nf = st.number_input(
        "Valor que nao deve ser debitado do projeto",
        min_value=0.0,
        step=1.0,
        format="%.2f",
        key=f"editar_nf_extra_valor_{nota_id}",
    )
    extra_responsavel_nf = st.text_input(
        "Responsavel pelo pagamento extra",
        value="Gerente do projeto",
        key=f"editar_nf_extra_responsavel_{nota_id}",
    )
    extra_data_nf = st.date_input("Data do pagamento extra", value=date.today(), key=f"editar_nf_extra_data_{nota_id}")
    extra_descricao_nf = st.text_area(
        "Descricao do valor extra",
        value="Taxa gerada por pagamento via TED.",
        key=f"editar_nf_extra_descricao_{nota_id}",
    )
    if st.button("Registrar valor extra desta NF", use_container_width=True, key=f"editar_nf_extra_salvar_{nota_id}"):
        if Decimal(str(extra_valor_nf)) <= 0:
            st.error("Informe um valor extra maior que zero.")
        elif not str(extra_descricao_nf or "").strip():
            st.error("Informe a descricao do valor extra.")
        else:
            execute("""
            insert into valores_extra_nao_debitados
              (compra_id, nota_fiscal_id, rubrica_id, solicitacao_id, tipo, descricao, valor, responsavel_pagamento, data_pagamento, registrado_por)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                int(nota["compra_id"]),
                int(nota_id),
                int(rubrica_id),
                int(nota["solicitacao_id"]) if nota["solicitacao_id"] is not None else None,
                extra_tipo_nf,
                extra_descricao_nf.strip(),
                Decimal(str(extra_valor_nf)),
                extra_responsavel_nf.strip() or None,
                extra_data_nf,
                user["id"],
            ))
            st.success("Valor extra registrado sem debitar do projeto.")
            st.rerun()

    if st.button("Salvar correcao", type="primary", use_container_width=True):
        if not numero_nf.strip():
            st.error("Informe o numero da NF.")
            return

        nota_duplicada = query("""
        select id
        from notas_fiscais
        where lower(trim(numero_nf)) = lower(trim(%s))
          and lower(trim(fornecedor)) = lower(trim(%s))
          and id <> %s
        limit 1
        """, (numero_nf, nota["fornecedor"], int(nota_id)))
        if len(nota_duplicada):
            st.error("Ja existe outra nota fiscal com este numero para o fornecedor.")
            return

        upload_nf_resultado = None
        local_nf_final = local_nf
        if arquivo_nf is not None:
            try:
                upload_nf_resultado = upload_nota_fiscal_google_drive(
                    arquivo_nf,
                    numero_nf.strip(),
                    str(nota["fornecedor"] or "").strip(),
                    pasta_url=local_nf_final,
                )
                local_nf_final = upload_nf_resultado["folder_link"]
            except RuntimeError as exc:
                st.error(str(exc))
                st.stop()

        execute("""
        update notas_fiscais
        set numero_nf=%s,
            arquivo_url=coalesce(nullif(%s, ''), arquivo_url)
        where id=%s
        """, (numero_nf.strip(), local_nf_final, int(nota_id)))
        if upload_nf_resultado:
            execute("""
            insert into nota_fiscal_arquivos (
                nota_fiscal_id,
                google_drive_file_id,
                google_drive_link,
                nome_arquivo,
                mime_type,
                tamanho_bytes
            ) values (%s,%s,%s,%s,%s,%s)
            """, (
                int(nota_id),
                upload_nf_resultado["file_id"],
                upload_nf_resultado["file_link"],
                upload_nf_resultado["nome_arquivo"],
                upload_nf_resultado["mime_type"],
                upload_nf_resultado["tamanho_bytes"],
            ))
        st.success("Nota fiscal corrigida.")
        st.rerun()


def sincronizar_orcamento():
    execute("update rubricas set valor_reservado = 0, valor_utilizado = 0")
    execute("""
    update rubricas r
    set valor_reservado = totais.valor_total
    from (
        select
          pi.rubrica_id,
          coalesce(sum(pi.valor_total), 0) as valor_total
        from pedido_itens pi
        join solicitacoes_compra s on s.id = pi.pedido_id
        where s.status in ('solicitacao', 'em_andamento', 'cotado', 'aguardando_nota')
          and not exists (
              select 1
              from nota_fiscal_itens nfi
              left join patrimonio p on p.nota_fiscal_item_id = nfi.id
              left join estoque_consumo e on e.nota_fiscal_item_id = nfi.id
              left join atesto_servico a on a.nota_fiscal_item_id = nfi.id
              where nfi.pedido_item_id = pi.id
                and (p.id is not null or e.id is not null or a.id is not null)
          )
        group by pi.rubrica_id
    ) totais
    where r.id = totais.rubrica_id
    """)
    execute("""
    update rubricas r
    set valor_utilizado = totais.valor_total
    from (
        select
          pi.rubrica_id,
          coalesce(sum(nfi.valor_total), 0) as valor_total
        from nota_fiscal_itens nfi
        join pedido_itens pi on pi.id = nfi.pedido_item_id
        join solicitacoes_compra s on s.id = pi.pedido_id
        left join patrimonio p on p.nota_fiscal_item_id = nfi.id
        left join estoque_consumo e on e.nota_fiscal_item_id = nfi.id
        left join atesto_servico a on a.nota_fiscal_item_id = nfi.id
        where s.status = 'finalizado'
           or p.id is not null
           or e.id is not null
           or a.id is not null
        group by pi.rubrica_id
    ) totais
    where r.id = totais.rubrica_id
    """)

startup_schema_lock_conn = None
try:
    startup_schema_lock_conn = acquire_startup_schema_lock()
    if startup_schema_lock_conn:
        ensure_permissions_schema()
        ensure_financial_governance_schema()
        criar_schema_ia_operacional()
except psycopg2.Error as exc:
    st.error("Nao foi possivel preparar o banco de dados para iniciar o app.")
    st.caption("Confira se as tabelas foram criadas no Supabase e reinicie o app no Streamlit Cloud.")
    with st.expander("Detalhe tecnico"):
        st.code(str(exc))
    st.stop()
finally:
    release_startup_schema_lock(startup_schema_lock_conn)

if "user" not in st.session_state:
    st.session_state.user = None

with st.sidebar:
    st.header("Acesso")
    st.caption(f"Versão: {APP_DEPLOY_VERSION}")
    if st.session_state.user is None:
        email = st.text_input("E-mail")
        senha = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            df = query("select * from usuarios_app where email=%s and ativo=true", (email,))
            if len(df) == 1 and check_password(senha, df.iloc[0]["senha_hash"]):
                st.session_state.user = df.iloc[0].to_dict()
                st.rerun()
            else:
                st.error("Login inválido.")
    else:
        st.write(f"Usuário: **{st.session_state.user['nome']}**")
        st.write(f"Papel: **{st.session_state.user['papel']}**")
        if st.button("Sair"):
            st.session_state.user = None
            st.rerun()

if st.session_state.user is None:
    st.info("Entre com usuário e senha para usar o sistema.")
    st.stop()

user = st.session_state.user
BASE_MENU_OPTIONS = [
    ("orcamento", "Orçamento"),
    ("nova_exigencia", "Nova exigência"),
    ("solicitacoes", "Solicitações"),
    ("cotacoes", "Cotações"),
    ("compra_nota", "Compra e nota fiscal"),
    ("comprovantes_bancarios", "Comprovantes bancários"),
    ("destino_final", "Destino final"),
    ("auditoria", "Auditoria"),
    ("ia_operacional", "IA Operacional e Auditoria de Gargalos"),
    ("itens_comprados", "Itens comprados"),
]
ADMIN_MENU_OPTIONS = BASE_MENU_OPTIONS + [("membros", "Membros")]

if user["papel"] == "admin":
    MENU_OPTIONS = ADMIN_MENU_OPTIONS
else:
    permissoes_usuario = set(user.get("permissoes") or [])
    MENU_OPTIONS = [item for item in BASE_MENU_OPTIONS if item[0] in permissoes_usuario]
    if not MENU_OPTIONS:
        MENU_OPTIONS = [("nova_exigencia", "Nova exigência")]

menu_labels = dict(MENU_OPTIONS)
menu_keys = [key for key, _ in MENU_OPTIONS]

if "menu_key" not in st.session_state or st.session_state.menu_key not in menu_keys:
    st.session_state.menu_key = menu_keys[0]

def selecionar_menu(menu_key):
    st.session_state.menu_key = menu_key

st.sidebar.markdown("### Módulo")
for menu_key, menu_label in MENU_OPTIONS:
    button_type = "primary" if st.session_state.menu_key == menu_key else "secondary"
    st.sidebar.button(
        menu_label,
        key=f"nav_{menu_key}",
        type=button_type,
        use_container_width=True,
        on_click=selecionar_menu,
        args=(menu_key,),
    )

menu = st.session_state.menu_key
titulo_pagina = menu_labels[menu]

st.markdown(
    f"""
    <div style="margin-top: -20px; margin-bottom: 20px;">
        <h2 style="margin-bottom: 0;">{titulo_pagina}</h2>
        <p style="color: gray; margin-top: 4px;">Módulo selecionado no menu lateral</p>
    </div>
    """,
    unsafe_allow_html=True
)

if menu == "orcamento":
    if user["papel"] in ["admin", "gerente"]:
        c_recalcular, c_responsaveis, c_reservar, c_remanejar, c_voltar_remanejamento, c_encerrar, c_historico = st.columns(7)
        if c_recalcular.button("Recalcular orçamento"):
            sincronizar_orcamento()
            st.success("Orçamento recalculado com base nas compras existentes.")
            st.rerun()
        if c_responsaveis.button("Atualizar responsáveis"):
            atualizar_responsaveis_dialog()
        if c_reservar.button("Reservar valor"):
            reservar_valor_dialog(user["id"])
        if c_remanejar.button("Remanejar saldo"):
            remanejar_saldo_dialog(user["id"])
        if c_voltar_remanejamento.button("Voltar remanej."):
            voltar_remanejamento_dialog(user["id"])
        if c_encerrar.button("Encerrar rubrica"):
            encerrar_rubrica_dialog(user["id"])
        if c_historico.button("Histórico/Auditoria"):
            historico_orcamento_dialog()

    df = query("""
    select
      v.id,
      v.codigo,
      v.nome,
      coalesce(r.responsaveis, '') as responsaveis,
      v.tipo,
      v.valor_orcado,
      v.valor_reservado,
      v.valor_utilizado,
      v.reserva_tecnica,
      v.reserva_tecnica_percentual,
      v.valor_minimo_operacional,
      v.saldo_disponivel,
      v.saldo_residual,
      v.percentual_comprometido,
      v.percentual_utilizado,
      v.encerrada
    from vw_orcamento v
    join rubricas r on r.id = v.id
    order by v.codigo
    """)
    if len(df) == 0:
        st.info("Não há rubricas cadastradas no orçamento.")
        st.stop()

    df["status_financeiro"] = df.apply(financial_status, axis=1)
    df["risco"] = df["status_financeiro"].apply(status_alert_level)
    compras_rubrica = query("""
    select
      s.rubrica_id as id,
      coalesce(sum(c.valor_compra), 0) as valor_compras_periodo,
      count(distinct c.id) as qtd_compras_periodo
    from compras c
    join solicitacoes_compra s on s.id = c.solicitacao_id
    where c.comprado_em::date between %s and %s
    group by s.rubrica_id
    """, (PERIODO_PRESTACAO_INICIO, PERIODO_PRESTACAO_FIM))
    if len(compras_rubrica):
        df = df.merge(compras_rubrica, on="id", how="left")
    else:
        df["valor_compras_periodo"] = 0
        df["qtd_compras_periodo"] = 0
    df["valor_compras_periodo"] = pd.to_numeric(df["valor_compras_periodo"], errors="coerce").fillna(0)
    df["qtd_compras_periodo"] = pd.to_numeric(df["qtd_compras_periodo"], errors="coerce").fillna(0).astype(int)

    percentual_tempo_prestacao = percentual_periodo_prestacao()
    df["percentual_compras_periodo"] = df.apply(
        lambda linha: round((float(linha["valor_compras_periodo"]) * 100.0 / float(linha["valor_orcado"])), 2)
        if float(linha["valor_orcado"] or 0) > 0 else 0,
        axis=1,
    )
    df["risco_prazo"] = df.apply(
        lambda linha: classificar_risco_prazo(
            linha["percentual_compras_periodo"],
            percentual_tempo_prestacao,
            linha["saldo_disponivel"],
            bool(linha["encerrada"]),
        ),
        axis=1,
    )
    df["sinal_prazo"] = df["risco_prazo"].apply(descrever_risco_prazo)

    total_orcado = df.valor_orcado.sum()
    total_reservado = df.valor_reservado.sum()
    total_utilizado = df.valor_utilizado.sum()
    total_reserva_tecnica = df.reserva_tecnica.sum()
    diferenca_sem_reserva_tecnica = total_orcado - total_reservado - total_utilizado
    total_disponivel = df.saldo_disponivel.sum()
    total_compras_periodo = df.valor_compras_periodo.sum()
    percentual_compras_global = round((float(total_compras_periodo) * 100.0 / float(total_orcado)), 2) if float(total_orcado or 0) > 0 else 0
    eficiencia_compras = round((percentual_compras_global * 100.0 / percentual_tempo_prestacao), 2) if percentual_tempo_prestacao > 0 else 0
    risco_prazo_global = classificar_risco_prazo(percentual_compras_global, percentual_tempo_prestacao, total_disponivel, False)
    saldo_residual_total = df.saldo_residual.sum()
    rubricas_criticas = df["status_financeiro"].isin(["Critico", "Residual", "Encerrado"]).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total orçado", format_currency_brl(total_orcado))
    c2.metric("Total reservado", format_currency_brl(total_reservado))
    c3.metric("Total utilizado", format_currency_brl(total_utilizado))
    c4, c5, c6 = st.columns(3)
    c4.metric("Disponível operacional", format_currency_brl(total_disponivel))
    c5.metric("Saldo residual", format_currency_brl(saldo_residual_total))
    c6.metric("Rubricas críticas", int(rubricas_criticas))
    c7, c8, c9 = st.columns(3)
    c7.metric("Diferença sem reserva técnica", format_currency_brl(diferenca_sem_reserva_tecnica))
    c8.metric("Reserva técnica", format_currency_brl(total_reserva_tecnica))
    c9.metric("Total residual", format_currency_brl(saldo_residual_total))

    st.markdown("### Sinalização inteligente de compras")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Período da prestação", f"{percentual_tempo_prestacao:.2f}%")
    p2.metric("Compras executadas", format_currency_brl(total_compras_periodo))
    p3.metric("Progresso das compras", f"{percentual_compras_global:.2f}%")
    p4.metric("Eficiência tempo x compras", f"{eficiencia_compras:.2f}%")
    barra_tempo, barra_compras, sinal_risco = st.columns([2, 2, 1])
    with barra_tempo:
        st.caption("Tempo decorrido: mar/2026 até mar/2027")
        st.progress(min(max(percentual_tempo_prestacao / 100.0, 0), 1))
    with barra_compras:
        st.caption("Execução financeira por compras registradas")
        st.progress(min(max(percentual_compras_global / 100.0, 0), 1))
    with sinal_risco:
        st.caption("Risco do prazo")
        st.markdown(
            f"<div style='font-size: 34px; line-height: 1; color: {risk_color_css(risco_prazo_global)};'>●</div>"
            f"<div style='font-weight: 700;'>{descrever_risco_prazo(risco_prazo_global)}</div>",
            unsafe_allow_html=True,
        )
    compras_mes = carregar_compras_por_mes_orcamento()
    meses_periodo = pd.period_range(PERIODO_PRESTACAO_INICIO, PERIODO_PRESTACAO_FIM, freq="M")
    curva_compras = pd.DataFrame({"mes": [periodo.to_timestamp().date() for periodo in meses_periodo]})
    if len(compras_mes):
        compras_mes["mes"] = pd.to_datetime(compras_mes["mes"]).dt.date
        curva_compras = curva_compras.merge(compras_mes, on="mes", how="left")
    else:
        curva_compras["valor_compras"] = 0
        curva_compras["compras"] = 0
    curva_compras["valor_compras"] = pd.to_numeric(curva_compras["valor_compras"], errors="coerce").fillna(0)
    curva_compras["compras"] = pd.to_numeric(curva_compras["compras"], errors="coerce").fillna(0).astype(int)
    curva_compras["Mes"] = pd.to_datetime(curva_compras["mes"]).dt.strftime("%m/%Y")
    st.bar_chart(curva_compras.set_index("Mes")[["valor_compras"]], use_container_width=True)

    alertas = df[df["status_financeiro"].isin(["Comprometido", "Critico", "Residual", "Encerrado"])].copy()
    if len(alertas):
        with st.expander("Alertas financeiros", expanded=True):
            for _, rubrica in alertas.iterrows():
                st.write(
                    f"{rubrica['codigo']} - {rubrica['nome']}: "
                    f"{descrever_status_financeiro(rubrica['status_financeiro'])} "
                    f"({format_currency_brl_markdown(rubrica['saldo_disponivel'])} operacional)"
                )

    df_orcamento = df.rename(columns={
        "codigo": "Código",
        "nome": "Rubrica",
        "responsaveis": "Responsável",
        "tipo": "Tipo",
        "valor_orcado": "Valor orçado",
        "valor_reservado": "Valor reservado",
        "valor_utilizado": "Valor utilizado",
        "valor_compras_periodo": "Compras executadas",
        "percentual_compras_periodo": "Progresso das compras",
        "sinal_prazo": "Sinal prazo",
        "risco_prazo": "Risco prazo",
        "reserva_tecnica": "Reserva técnica",
        "valor_minimo_operacional": "Mínimo operacional",
        "saldo_disponivel": "Disponível operacional",
        "saldo_residual": "Saldo residual",
        "percentual_comprometido": "Índice comprometido",
        "percentual_utilizado": "Percentual utilizado",
        "status_financeiro": "Status financeiro",
        "risco": "Risco",
    })
    df_orcamento["Índice comprometido"] = pd.to_numeric(df_orcamento["Índice comprometido"], errors="coerce").fillna(0)
    df_orcamento["Progresso das compras"] = pd.to_numeric(df_orcamento["Progresso das compras"], errors="coerce").fillna(0)
    for coluna in [
        "Valor orçado",
        "Valor reservado",
        "Valor utilizado",
        "Compras executadas",
        "Reserva técnica",
        "Mínimo operacional",
        "Disponível operacional",
        "Saldo residual",
    ]:
        df_orcamento[coluna] = df_orcamento[coluna].apply(format_currency_brl)
    df_orcamento["Percentual utilizado"] = df_orcamento["Percentual utilizado"].apply(format_percent_brl)
    df_orcamento["Status financeiro"] = df_orcamento["Status financeiro"].apply(descrever_status_financeiro)
    risco_labels = df_orcamento["Risco"].copy()
    risco_prazo_labels = df_orcamento["Risco prazo"].copy()
    df_orcamento["Risco"] = "●"
    df_orcamento["Risco prazo"] = "●"
    colunas_orcamento = [
        "Código",
        "Rubrica",
        "Tipo",
        "Responsável",
        "Valor orçado",
        "Valor reservado",
        "Valor utilizado",
        "Compras executadas",
        "Progresso das compras",
        "Sinal prazo",
        "Reserva técnica",
        "Mínimo operacional",
        "Disponível operacional",
        "Saldo residual",
        "Índice comprometido",
        "Status financeiro",
        "Risco",
        "Risco prazo",
    ]
    df_orcamento_visual = df_orcamento[colunas_orcamento].style.apply(
        lambda coluna: [
            (
                f"color: {risk_color_css(risco_labels.loc[indice])}; "
                "font-size: 22px; font-weight: 700; text-align: center;"
            )
            for indice in coluna.index
        ],
        subset=["Risco"],
        axis=0,
    )
    df_orcamento_visual = df_orcamento_visual.apply(
        lambda coluna: [
            (
                f"color: {risk_color_css(risco_prazo_labels.loc[indice])}; "
                "font-size: 22px; font-weight: 700; text-align: center;"
            )
            for indice in coluna.index
        ],
        subset=["Risco prazo"],
        axis=0,
    )
    st.caption("Clique em uma linha da tabela para abrir a visao de analise completa da rubrica abaixo.")
    evento_orcamento = st.dataframe(
        df_orcamento_visual,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Índice comprometido": st.column_config.ProgressColumn(
                "Índice comprometido",
                format="%.2f%%",
                min_value=0,
                max_value=100,
            ),
            "Progresso das compras": st.column_config.ProgressColumn(
                "Progresso das compras",
                format="%.2f%%",
                min_value=0,
                max_value=100,
            ),
            "Risco": st.column_config.TextColumn("Risco", width="small"),
            "Risco prazo": st.column_config.TextColumn("Risco prazo", width="small"),
        },
    )
    selecao_orcamento = getattr(evento_orcamento, "selection", {})
    if isinstance(selecao_orcamento, dict):
        linhas_selecionadas = selecao_orcamento.get("rows", [])
    else:
        linhas_selecionadas = getattr(selecao_orcamento, "rows", [])
    if linhas_selecionadas:
        exibir_detalhe_rubrica(df.iloc[linhas_selecionadas[0]].to_dict())

    with st.expander("Parametros de governanca por rubrica"):
        rubrica_id = st.selectbox(
            "Rubrica",
            df["id"].tolist(),
            format_func=lambda item_id: f"{df.loc[df.id == item_id, 'codigo'].iloc[0]} - {df.loc[df.id == item_id, 'nome'].iloc[0]}",
            key="orcamento_parametros_rubrica",
        )
        rubrica = df.loc[df.id == rubrica_id].iloc[0]
        p1, p2 = st.columns(2)
        novo_minimo = p1.number_input(
            "Valor mínimo operacional",
            min_value=0.0,
            value=float(rubrica["valor_minimo_operacional"]),
            step=50.0,
        )
        nova_reserva = p2.number_input(
            "Reserva técnica (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(rubrica["reserva_tecnica_percentual"]),
            step=0.5,
        )
        if st.button("Salvar parâmetros da rubrica", type="primary"):
            execute(
                "update rubricas set valor_minimo_operacional=%s, reserva_tecnica_percentual=%s where id=%s",
                (Decimal(str(novo_minimo)), Decimal(str(nova_reserva)), int(rubrica_id)),
            )
            execute(
                "insert into movimentacoes_orcamento (rubrica_id, usuario_id, operacao, valor, justificativa) values (%s,%s,'parametros_governanca',0,%s)",
                (int(rubrica_id), user["id"], "Atualizacao de valor minimo operacional e reserva tecnica."),
            )
            st.success("Parâmetros atualizados.")
            st.rerun()

elif menu == "nova_exigencia":
    sincronizar_orcamento()
    rubricas = query("""
    select v.id, v.codigo || ' - ' || v.nome as label, v.saldo_disponivel, r.tipo
    from vw_orcamento v
    join rubricas r on r.id = v.id
    where v.encerrada = false
    order by v.codigo
    """)
    if len(rubricas) == 0:
        st.info("Não há rubricas abertas para novas solicitações.")
        st.stop()
    rubrica_label = st.selectbox("Rubrica/categoria", rubricas["label"])
    rubrica_id = int(rubricas.loc[rubricas["label"] == rubrica_label, "id"].iloc[0])
    tipo_rubrica = rubricas.loc[rubricas["label"] == rubrica_label, "tipo"].iloc[0]
    tipo_item_padrao = {
        "material_consumo": "consumo",
        "material_permanente": "permanente",
        "servico_pf": "servico",
    }.get(tipo_rubrica, "permanente")
    saldo_atual = Decimal(str(rubricas.loc[rubricas["label"] == rubrica_label, "saldo_disponivel"].iloc[0]))
    st.caption(f"Disponível operacional: {format_currency_brl_markdown(saldo_atual)}")
    if "nova_exigencia_form_version" not in st.session_state:
        st.session_state.nova_exigencia_form_version = 0
    if "nova_exigencia_sucesso" in st.session_state:
        st.success(st.session_state.pop("nova_exigencia_sucesso"))

    form_version = st.session_state.nova_exigencia_form_version
    descricao = st.text_area("Resumo do pedido/requerimento", key=f"nova_descricao_{form_version}")
    st.markdown("### Itens do pedido")
    itens_state_key = f"nova_exigencia_itens_dados_{form_version}_{rubrica_id}"
    itens_auto_desc_key = f"nova_exigencia_item_auto_desc_{form_version}_{rubrica_id}"
    itens_editor_key = f"nova_exigencia_itens_{form_version}_{rubrica_id}"
    if itens_state_key not in st.session_state:
        st.session_state[itens_state_key] = [
            {"descricao": "", "tipo_item": tipo_item_padrao, "quantidade": 1.0, "valor_unitario": 0.0, "observacoes": ""}
        ]

    descricao_auto = descricao.strip()
    itens_estado = list(st.session_state[itens_state_key])
    if not itens_estado:
        itens_estado = [{"descricao": "", "tipo_item": tipo_item_padrao, "quantidade": 1.0, "valor_unitario": 0.0, "observacoes": ""}]
    primeira_descricao = str(itens_estado[0].get("descricao") or "").strip()
    descricao_auto_anterior = str(st.session_state.get(itens_auto_desc_key) or "").strip()
    if descricao_auto and descricao_auto != descricao_auto_anterior and (not primeira_descricao or primeira_descricao == descricao_auto_anterior):
        itens_estado[0]["descricao"] = descricao_auto
        st.session_state[itens_auto_desc_key] = descricao_auto
        st.session_state[itens_state_key] = itens_estado
        if itens_editor_key in st.session_state:
            del st.session_state[itens_editor_key]

    def sincronizar_itens_nova_exigencia(editor_key, state_key, tipo_item_default):
        editor_state = st.session_state.get(editor_key, {})
        if not isinstance(editor_state, dict):
            return

        colunas_itens = ["descricao", "tipo_item", "quantidade", "valor_unitario", "observacoes"]
        dados = pd.DataFrame(st.session_state.get(state_key) or [], columns=colunas_itens)
        if dados.empty:
            dados = pd.DataFrame(
                [{"descricao": "", "tipo_item": tipo_item_default, "quantidade": 1.0, "valor_unitario": 0.0, "observacoes": ""}],
                columns=colunas_itens,
            )

        for indice, alteracoes_linha in editor_state.get("edited_rows", {}).items():
            indice = int(indice)
            if indice >= len(dados):
                continue
            for coluna, valor in alteracoes_linha.items():
                if coluna in dados.columns:
                    dados.at[indice, coluna] = valor

        for indice in sorted(editor_state.get("deleted_rows", []), reverse=True):
            indice = int(indice)
            if indice < len(dados):
                dados = dados.drop(dados.index[indice])

        for nova_linha in editor_state.get("added_rows", []):
            linha = {"descricao": "", "tipo_item": tipo_item_default, "quantidade": 1.0, "valor_unitario": 0.0, "observacoes": ""}
            linha.update({coluna: valor for coluna, valor in nova_linha.items() if coluna in linha})
            dados = pd.concat([dados, pd.DataFrame([linha])], ignore_index=True)

        st.session_state[state_key] = dados[colunas_itens].to_dict("records")

    itens_base = pd.DataFrame(st.session_state[itens_state_key])
    itens_editados = st.data_editor(
        itens_base,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "descricao": st.column_config.TextColumn("Item", required=True),
            "tipo_item": st.column_config.SelectboxColumn("Tipo", options=["permanente", "consumo", "servico"], required=True),
            "quantidade": st.column_config.NumberColumn("Quantidade", min_value=0.01, format="%.2f"),
            "valor_unitario": st.column_config.NumberColumn("Valor unitario", min_value=0.0, format="R$ %.2f"),
            "observacoes": st.column_config.TextColumn("Observacoes"),
        },
        key=itens_editor_key,
        on_change=sincronizar_itens_nova_exigencia,
        args=(itens_editor_key, itens_state_key, tipo_item_padrao),
    )
    itens_validos = itens_editados[itens_editados["descricao"].fillna("").str.strip() != ""].copy()
    if len(itens_validos):
        itens_validos["quantidade"] = pd.to_numeric(itens_validos["quantidade"], errors="coerce").fillna(0)
        itens_validos["valor_unitario"] = pd.to_numeric(itens_validos["valor_unitario"], errors="coerce").fillna(0)
        itens_validos["valor_total"] = itens_validos["quantidade"] * itens_validos["valor_unitario"]
    valor_estimado = float(itens_validos["valor_total"].sum()) if len(itens_validos) else 0.0
    st.metric("Valor total estimado", format_currency_brl(valor_estimado))
    justificativa = st.text_area("Justificativa", key=f"nova_justificativa_{form_version}")
    if st.button("Enviar solicitação", key=f"nova_enviar_{form_version}"):
        valor_estimado_decimal = Decimal(str(valor_estimado))
        excede_saldo, saldo_disponivel = excede_saldo_disponivel(rubrica_id, valor_estimado_decimal)
        if len(itens_validos) == 0:
            st.error("Informe pelo menos um item do pedido.")
        elif (itens_validos["quantidade"] <= 0).any():
            st.error("Todos os itens devem ter quantidade maior que zero.")
        elif excede_saldo:
            st.error(
                "Solicitação não registrada. "
                f"O valor total ({format_currency_brl_markdown(valor_estimado_decimal)}) "
                f"supera o disponível operacional da rubrica ({format_currency_brl_markdown(saldo_disponivel)})."
            )
        else:
            descricao_pedido = descricao.strip() or "; ".join(itens_validos["descricao"].astype(str).tolist())[:500]
            solicitacao_criada = query("""
            insert into solicitacoes_compra (rubrica_id, solicitante_id, descricao, quantidade, unidade, valor_estimado, justificativa, status)
            values (%s,%s,%s,%s,%s,%s,%s,'solicitacao')
            returning id
            """, (rubrica_id, user["id"], descricao_pedido, float(itens_validos["quantidade"].sum()), "itens", valor_estimado, justificativa))
            solicitacao_id = int(solicitacao_criada.iloc[0]["id"])
            for _, item in itens_validos.iterrows():
                execute("""
                insert into pedido_itens (pedido_id, rubrica_id, descricao, tipo_item, quantidade, valor_unitario, observacoes)
                values (%s,%s,%s,%s,%s,%s,%s)
                """, (
                    solicitacao_id,
                    rubrica_id,
                    str(item["descricao"]).strip(),
                    item["tipo_item"],
                    Decimal(str(item["quantidade"])),
                    Decimal(str(item["valor_unitario"])),
                    str(item.get("observacoes") or "").strip() or None,
                ))
            sincronizar_orcamento()
            st.session_state.nova_exigencia_sucesso = f"Solicitação #{solicitacao_id} registrada com {len(itens_validos)} item(ns)."
            st.session_state.nova_exigencia_form_version += 1
            st.rerun()

elif menu == "solicitacoes":
    df = query("""
    select s.id, r.codigo as rubrica, s.descricao, s.quantidade, s.valor_estimado as "Valor estimado", s.status, s.autorizado, s.criado_em
    from solicitacoes_compra s join rubricas r on r.id=s.rubrica_id
    where s.status not in ('finalizado','cancelado')
    order by s.id desc
    """)
    pode_editar_solicitacoes = user["papel"] in ["gerente", "admin"]
    df_editor = df.copy()
    if len(df_editor):
        df_editor["quantidade"] = pd.to_numeric(df_editor["quantidade"], errors="coerce")
        df_editor["Valor estimado"] = pd.to_numeric(df_editor["Valor estimado"], errors="coerce")
    tabela_solicitacoes = st.data_editor(
        df_editor,
        use_container_width=True,
        disabled=["id", "rubrica", "descricao", "status", "criado_em"],
        column_config={
            "quantidade": st.column_config.NumberColumn("quantidade", min_value=0.001, format="%.3f", disabled=not pode_editar_solicitacoes),
            "Valor estimado": st.column_config.NumberColumn("Valor estimado", min_value=0.0, format="R$ %.2f", disabled=not pode_editar_solicitacoes),
            "autorizado": st.column_config.CheckboxColumn("autorizado", disabled=not pode_editar_solicitacoes),
        },
        key="solicitacoes_editor",
    )
    if pode_editar_solicitacoes and st.button("Salvar alteracoes da tabela", key="solicitacoes_salvar_tabela"):
        alteracoes = []
        alteracoes_autorizacao = []
        original_por_id = df_editor.set_index("id")
        valores_invalidos = False
        remocoes_autorizacao = []
        for _, linha in tabela_solicitacoes.iterrows():
            solicitacao_id = int(linha["id"])
            original = original_por_id.loc[solicitacao_id]
            if pd.isna(linha["quantidade"]) or pd.isna(linha["Valor estimado"]):
                valores_invalidos = True
                continue
            quantidade = Decimal(str(linha["quantidade"]))
            valor_estimado = Decimal(str(linha["Valor estimado"]))
            quantidade_original = Decimal("0") if pd.isna(original["quantidade"]) else Decimal(str(original["quantidade"]))
            valor_original = Decimal("0") if pd.isna(original["Valor estimado"]) else Decimal(str(original["Valor estimado"]))
            quantidade_alterada = quantidade != quantidade_original
            valor_alterado = valor_estimado != valor_original
            if quantidade_alterada and not valor_alterado and quantidade_original > 0:
                valor_unitario_original = valor_original / quantidade_original
                valor_estimado = (valor_unitario_original * quantidade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if quantidade != quantidade_original or valor_estimado != valor_original:
                alteracoes.append((solicitacao_id, quantidade, valor_estimado, quantidade_original, valor_original))
            autorizado = bool(linha["autorizado"])
            autorizado_original = bool(original["autorizado"])
            if autorizado != autorizado_original:
                if autorizado:
                    alteracoes_autorizacao.append((solicitacao_id, valor_estimado))
                else:
                    remocoes_autorizacao.append(solicitacao_id)

        if valores_invalidos:
            st.error("Preencha quantidade e valor estimado antes de salvar.")
        elif remocoes_autorizacao:
            st.error("A autorizacao ja concedida nao pode ser removida pela tabela.")
        elif not alteracoes and not alteracoes_autorizacao:
            st.info("Nenhuma alteracao para salvar.")
        elif any(quantidade <= 0 for _, quantidade, _, _, _ in alteracoes):
            st.error("A quantidade deve ser maior que zero.")
        elif any(valor_estimado < 0 for _, _, valor_estimado, _, _ in alteracoes):
            st.error("O valor estimado nao pode ser negativo.")
        else:
            erro_autorizacao = None
            for solicitacao_id, valor_estimado in alteracoes_autorizacao:
                solicitacao = query("""
                select id, rubrica_id, coalesce(valor_estimado, 0) as valor_estimado, autorizado
                from solicitacoes_compra
                where id=%s
                """, (solicitacao_id,))
                if len(solicitacao) != 1:
                    erro_autorizacao = f"Solicitacao #{solicitacao_id} nao encontrada."
                    break
                rubrica_autorizacao_id = int(solicitacao.iloc[0]["rubrica_id"])
                saldo_df = query("select saldo_disponivel from vw_orcamento where id=%s", (rubrica_autorizacao_id,))
                saldo_disponivel = Decimal(str(saldo_df.iloc[0]["saldo_disponivel"])) if len(saldo_df) == 1 else Decimal("0")
                valor_atual_banco = Decimal(str(solicitacao.iloc[0]["valor_estimado"]))
                saldo_disponivel_para_autorizacao = saldo_disponivel + valor_atual_banco
                if Decimal(str(valor_estimado)) > saldo_disponivel_para_autorizacao:
                    erro_autorizacao = (
                        f"Solicitacao #{solicitacao_id} nao autorizada. "
                        f"O valor estimado ({format_currency_brl_markdown(valor_estimado)}) "
                        f"supera o disponivel operacional da rubrica ({format_currency_brl_markdown(saldo_disponivel_para_autorizacao)})."
                    )
                    break

            if erro_autorizacao:
                st.error(erro_autorizacao)
                st.stop()

            for solicitacao_id, quantidade, valor_estimado, quantidade_original, valor_original in alteracoes:
                execute("""
                update solicitacoes_compra
                set quantidade=%s, valor_estimado=%s, atualizado_em=now()
                where id=%s
                """, (quantidade, valor_estimado, solicitacao_id))
                execute("""
                update pedido_itens
                set quantidade=%s,
                    valor_unitario=%s
                where pedido_id=%s
                  and (select count(*) from pedido_itens where pedido_id=%s) = 1
                """, (
                    quantidade,
                    (valor_estimado / quantidade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                    solicitacao_id,
                    solicitacao_id,
                ))
                execute("""
                insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao)
                values (%s,%s,%s,%s)
                """, (
                    solicitacao_id,
                    str(df_editor.loc[df_editor["id"] == solicitacao_id, "status"].iloc[0]),
                    user["id"],
                    (
                        "Quantidade/valor estimado editados na tabela: "
                        f"quantidade {quantidade_original} -> {quantidade}; "
                        f"valor {format_currency_brl(valor_original)} -> {format_currency_brl(valor_estimado)}"
                    ),
                ))
            for solicitacao_id, _ in alteracoes_autorizacao:
                execute("update solicitacoes_compra set autorizado=true, gerente_id=%s, autorizado_em=now(), status='em_andamento', atualizado_em=now() where id=%s", (user["id"], solicitacao_id))
                execute("insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao) values (%s,'em_andamento',%s,'Autorizada pelo gerente na tabela')", (solicitacao_id, user["id"]))
            sincronizar_orcamento()
            st.success(f"{len(alteracoes) + len(alteracoes_autorizacao)} alteracao(oes) salva(s).")
            st.rerun()
    if user["papel"] in ["gerente", "admin"]:
        st.markdown("### Autorizar solicitação")
        if len(df) == 0:
            st.info("Não há solicitações ativas para autorizar ou cancelar.")
        else:
            sid = st.selectbox(
                "Solicitação",
                df["id"].tolist(),
                format_func=lambda x: f"#{x} - {df.loc[df.id == x, 'descricao'].iloc[0][:80]}",
                key="solicitacao_acao_id",
            )
            if st.button("Autorizar e colocar em andamento"):
                existe = query("""
                select id, rubrica_id, coalesce(valor_estimado, 0) as valor_estimado, autorizado
                from solicitacoes_compra
                where id=%s
                """, (sid,))
                if len(existe) != 1:
                    st.error("Solicitação não encontrada.")
                elif not bool(existe.iloc[0]["autorizado"]):
                    valor_autorizacao = Decimal(str(existe.iloc[0]["valor_estimado"]))
                    rubrica_autorizacao_id = int(existe.iloc[0]["rubrica_id"])
                    saldo_df = query("select saldo_disponivel from vw_orcamento where id=%s", (rubrica_autorizacao_id,))
                    saldo_disponivel = Decimal(str(saldo_df.iloc[0]["saldo_disponivel"])) if len(saldo_df) == 1 else Decimal("0")
                    saldo_disponivel_para_autorizacao = saldo_disponivel + valor_autorizacao
                    if valor_autorizacao > saldo_disponivel_para_autorizacao:
                        st.error(
                            "Solicitação não autorizada. "
                            f"O valor estimado ({format_currency_brl_markdown(valor_autorizacao)}) "
                            f"supera o disponível operacional da rubrica ({format_currency_brl_markdown(saldo_disponivel_para_autorizacao)})."
                        )
                    else:
                        execute("update solicitacoes_compra set autorizado=true, gerente_id=%s, autorizado_em=now(), status='em_andamento' where id=%s", (user["id"], sid))
                        execute("insert into historico_status (solicitacao_id,status_novo,usuario_id,observacao) values (%s,'em_andamento',%s,'Autorizada pelo gerente')", (sid, user["id"]))
                        sincronizar_orcamento()
                        st.success("Solicitação autorizada.")
                        st.rerun()
                else:
                    st.info("Esta solicitação já estava autorizada.")
            if st.button("Cancelar solicitação"):
                cancelar_solicitacao(sid, user["id"])
                st.success("Solicitação cancelada e removida da lista.")
                st.rerun()

elif menu == "cotacoes":
    rubricas_cotacao = query("""
    select distinct r.id, r.codigo, r.nome
    from rubricas r
    join solicitacoes_compra s on s.rubrica_id = r.id
    join pedido_itens pi on pi.pedido_id = s.id
    where s.autorizado=true
      and s.status in ('em_andamento','cotado')
    order by r.codigo, r.nome
    """)
    solicitacoes = rubricas_cotacao
    if len(solicitacoes) == 0:
        st.warning("Não há itens autorizados para cotação.")
    else:
        rubrica_id = st.selectbox(
            "Rubrica",
            rubricas_cotacao["id"].tolist(),
            format_func=lambda x: f"{rubricas_cotacao.loc[rubricas_cotacao.id==x,'codigo'].iloc[0]} - {rubricas_cotacao.loc[rubricas_cotacao.id==x,'nome'].iloc[0]}",
        )
        sid = f"rubrica_{rubrica_id}"
        pedido_itens = query("""
        select
          pi.id,
          pi.pedido_id,
          s.descricao as solicitacao,
          pi.descricao,
          pi.tipo_item,
          pi.quantidade,
          pi.valor_unitario,
          pi.valor_total
        from pedido_itens pi
        join solicitacoes_compra s on s.id = pi.pedido_id
        where s.rubrica_id=%s
          and s.autorizado=true
          and s.status in ('em_andamento','cotado')
        order by s.id desc, pi.created_at, pi.descricao
        """, (rubrica_id,))
        if len(pedido_itens) == 0:
            st.warning("Esta rubrica ainda não tem itens autorizados para cotação. Recrie pela tela Nova exigência ou migre os itens antes de cotar.")
            st.stop()

        st.markdown("### Cotações")
        cotacoes_salvas_v2 = query("""
        select
          c.id,
          c.solicitacao_id,
          c.ordem,
          c.fornecedor,
          c.cnpj_cpf,
          c.telefone_email,
          c.prazo_entrega,
          c.arquivo_url,
          c.observacoes,
          count(ci.id) as total_itens,
          coalesce(sum(ci.valor_total), c.valor_total, 0) as valor_total
        from cotacoes c
        left join solicitacoes_compra s on s.id = c.solicitacao_id
        left join cotacao_itens ci on ci.cotacao_id = c.id
        where coalesce(c.rubrica_id, s.rubrica_id)=%s
        group by c.id, c.solicitacao_id, c.ordem, c.fornecedor, c.cnpj_cpf, c.telefone_email, c.prazo_entrega, c.arquivo_url, c.observacoes, c.valor_total
        order by c.ordem
        """, (rubrica_id,))

        def cotacao_v2_itens(cotacao_id):
            return query("""
            select
              ci.id,
              ci.pedido_item_id,
              coalesce(ci.descricao_item, pi.descricao) as item,
              coalesce(ci.tipo_item, pi.tipo_item) as tipo,
              ci.quantidade,
              ci.valor_unitario,
              ci.valor_total,
              ci.observacoes
            from cotacao_itens ci
            join pedido_itens pi on pi.id = ci.pedido_item_id
            where ci.cotacao_id=%s
            order by ci.created_at, ci.id
            """, (cotacao_id,))

        def cotacao_v2_formatar_itens(itens_df):
            tabela = itens_df.copy()
            if len(tabela):
                tabela = tabela.rename(columns={
                    "item": "Item",
                    "tipo": "Tipo",
                    "quantidade": "Quantidade",
                    "valor_unitario": "Valor unitário",
                    "valor_total": "Valor total",
                    "observacoes": "Observações",
                })
                tabela["Valor unitário"] = tabela["Valor unitário"].apply(format_currency_brl)
                tabela["Valor total"] = tabela["Valor total"].apply(format_currency_brl)
                return tabela[["Item", "Tipo", "Quantidade", "Valor unitário", "Valor total", "Observações"]]
            return pd.DataFrame(columns=["Item", "Tipo", "Quantidade", "Valor unitário", "Valor total", "Observações"])

        def cotacao_v2_proxima_ordem():
            usadas = set(cotacoes_salvas_v2["ordem"].astype(int).tolist()) if len(cotacoes_salvas_v2) else set()
            for numero in [1, 2, 3]:
                if numero not in usadas:
                    return numero
            return None

        def cotacao_v2_carregar_estado(prefixo, itens_df):
            st.session_state[f"{prefixo}_itens"] = []
            for _, item in itens_df.iterrows():
                st.session_state[f"{prefixo}_itens"].append({
                    "linha_id": f"existente_{item['id']}",
                    "pedido_item_id": item["pedido_item_id"],
                    "Item": item["item"],
                    "Tipo": item["tipo"],
                    "Quantidade": float(item["quantidade"]),
                    "Valor unitario": float(item["valor_unitario"]),
                    "Observacoes": item["observacoes"] or "",
                    "Remover": False,
                })
            st.session_state[f"{prefixo}_loaded"] = True
            st.session_state[f"{prefixo}_editor_version"] = st.session_state.get(f"{prefixo}_editor_version", 0) + 1

        def cotacao_v2_formulario(prefixo, ordem, cotacao_atual=None, itens_existentes=None):
            cotacao_atual = cotacao_atual or {}
            editando_cotacao = bool(cotacao_atual.get("id"))
            itens_existentes = itens_existentes if itens_existentes is not None else pd.DataFrame()
            valores_iniciais = {
                f"{prefixo}_fornecedor": str(cotacao_atual.get("fornecedor", "") or ""),
                f"{prefixo}_cnpj": format_cpf_cnpj(cotacao_atual.get("cnpj_cpf", "")),
                f"{prefixo}_contato": str(cotacao_atual.get("telefone_email", "") or ""),
                f"{prefixo}_prazo": str(cotacao_atual.get("prazo_entrega", "") or ""),
                f"{prefixo}_arquivo_url": str(cotacao_atual.get("arquivo_url", "") or ""),
                f"{prefixo}_observacoes": str(cotacao_atual.get("observacoes", "") or ""),
            }
            for chave, valor in valores_iniciais.items():
                if chave not in st.session_state:
                    st.session_state[chave] = valor
            if f"{prefixo}_editor_version" not in st.session_state:
                st.session_state[f"{prefixo}_editor_version"] = 0
            if not st.session_state.get(f"{prefixo}_loaded"):
                cotacao_v2_carregar_estado(prefixo, itens_existentes)

            st.markdown(f"### {'Editar' if editando_cotacao else 'Criar'} cotação {ordem}")
            st.caption("Altere os dados e salve a edição da cotação existente." if editando_cotacao else "Preencha os dados da empresa, adicione os itens e salve a nova cotação.")
            fornecedor = st.text_input("Fornecedor", key=f"{prefixo}_fornecedor")
            cnpj = st.text_input("CNPJ/CPF", key=f"{prefixo}_cnpj", on_change=formatar_cpf_cnpj_session_state, args=(f"{prefixo}_cnpj",))
            contato = st.text_input("Telefone/E-mail", key=f"{prefixo}_contato")
            prazo = st.text_input("Prazo de entrega", key=f"{prefixo}_prazo")
            arquivo = st.file_uploader("Arquivo da cotação para o Google Drive", type=["pdf", "png", "jpg", "jpeg", "doc", "docx", "xls", "xlsx"], key=f"{prefixo}_arquivo")
            arquivo_url = st.text_input("Link da pasta da cotação no Google Drive", key=f"{prefixo}_arquivo_url")
            if str(arquivo_url or "").strip():
                st.link_button("Abrir pasta da cotação no Google Drive", str(arquivo_url).strip())
            observacoes_gerais = st.text_area("Observações gerais", key=f"{prefixo}_observacoes")
            exibir_arquivos_cotacao(cotacao_atual.get("id"))

            st.markdown("### Adicionar item à cotação")
            adicionar_todos = st.checkbox("Adicionar todos os itens autorizados desta rubrica", key=f"{prefixo}_adicionar_todos")
            if adicionar_todos:
                itens = list(st.session_state[f"{prefixo}_itens"])
                itens_ja_adicionados = {int(item["pedido_item_id"]) for item in itens if item.get("pedido_item_id") is not None}
                novos_itens = []
                for _, item in pedido_itens.iterrows():
                    pedido_item_id = int(item["id"])
                    if pedido_item_id in itens_ja_adicionados:
                        continue
                    novos_itens.append({
                        "linha_id": f"novo_{len(itens) + len(novos_itens) + 1}_{pedido_item_id}",
                        "pedido_item_id": pedido_item_id,
                        "Item": item["descricao"],
                        "Tipo": item["tipo_item"],
                        "Quantidade": float(item["quantidade"]),
                        "Valor unitario": float(item["valor_unitario"] or 0),
                        "Observacoes": "",
                        "Remover": False,
                    })
                if novos_itens:
                    st.session_state[f"{prefixo}_itens"] = itens + novos_itens
                    st.session_state[f"{prefixo}_editor_version"] += 1
                    st.success(f"{len(novos_itens)} item(ns) adicionado(s) a cotacao.")
                    st.rerun()
                else:
                    st.info("Todos os itens autorizados desta rubrica ja estao na cotacao.")
            item_id = st.selectbox(
                "Item da rubrica",
                pedido_itens["id"].tolist(),
                format_func=lambda valor: (
                    f"Solicitação #{int(pedido_itens.loc[pedido_itens.id == valor, 'pedido_id'].iloc[0])} - "
                    f"{pedido_itens.loc[pedido_itens.id == valor, 'descricao'].iloc[0]}"
                ),
                key=f"{prefixo}_item",
            )
            item_base = pedido_itens[pedido_itens["id"] == item_id].iloc[0]
            descricao_item = st.text_input("Descrição do produto", value=str(item_base["descricao"]), key=f"{prefixo}_desc_{item_id}")
            tipo_item = st.text_input("Tipo", value=str(item_base["tipo_item"]), key=f"{prefixo}_tipo_{item_id}")
            col_qtd, col_valor = st.columns(2)
            with col_qtd:
                quantidade = st.number_input("Quantidade", min_value=0.01, value=float(item_base["quantidade"]), format="%.2f", key=f"{prefixo}_qtd_{item_id}")
            with col_valor:
                valor_unitario = st.number_input("Valor unitário", min_value=0.0, value=float(item_base["valor_unitario"] or 0), format="%.2f", key=f"{prefixo}_valor_{item_id}")
            observacao_item = st.text_input("Observação do item", key=f"{prefixo}_obs_item_{item_id}")
            if st.button("Adicionar item à cotação", key=f"{prefixo}_adicionar"):
                itens = list(st.session_state[f"{prefixo}_itens"])
                itens.append({
                    "linha_id": f"novo_{len(itens) + 1}_{item_id}",
                    "pedido_item_id": item_id,
                    "Item": descricao_item.strip() or item_base["descricao"],
                    "Tipo": tipo_item.strip() or item_base["tipo_item"],
                    "Quantidade": float(quantidade),
                    "Valor unitario": float(valor_unitario),
                    "Observacoes": observacao_item.strip(),
                    "Remover": False,
                })
                st.session_state[f"{prefixo}_itens"] = itens
                st.session_state[f"{prefixo}_editor_version"] += 1

            st.markdown("### Itens da cotação")
            itens_editados = st.data_editor(
                pd.DataFrame(st.session_state[f"{prefixo}_itens"], columns=["linha_id", "pedido_item_id", "Item", "Tipo", "Quantidade", "Valor unitario", "Observacoes", "Remover"]),
                use_container_width=True,
                hide_index=True,
                disabled=["linha_id", "pedido_item_id"],
                column_config={
                    "linha_id": None,
                    "pedido_item_id": None,
                    "Quantidade": st.column_config.NumberColumn("Quantidade", min_value=0.01, format="%.2f"),
                    "Valor unitario": st.column_config.NumberColumn("Valor unitário", min_value=0.0, format="R$ %.2f"),
                    "Remover": st.column_config.CheckboxColumn("Remover"),
                },
                key=f"{prefixo}_editor_{st.session_state[f'{prefixo}_editor_version']}",
            )
            if len(itens_editados) and st.button("Remover item marcado", key=f"{prefixo}_remover"):
                itens_editados = itens_editados[itens_editados["Remover"] != True].copy()
                st.session_state[f"{prefixo}_itens"] = itens_editados.to_dict("records")
                st.session_state[f"{prefixo}_editor_version"] += 1
            if "Quantidade" not in itens_editados.columns:
                itens_editados = pd.DataFrame(columns=["linha_id", "pedido_item_id", "Item", "Tipo", "Quantidade", "Valor unitario", "Observacoes", "Remover"])
            st.session_state[f"{prefixo}_itens"] = itens_editados.to_dict("records")
            itens_editados["Quantidade"] = pd.to_numeric(itens_editados["Quantidade"], errors="coerce").fillna(0)
            itens_editados["Valor unitario numerico"] = pd.to_numeric(itens_editados["Valor unitario"], errors="coerce").fillna(0)
            itens_editados = itens_editados[itens_editados["Remover"] != True].copy()
            itens_editados["Valor total"] = itens_editados["Quantidade"].apply(lambda valor: Decimal(str(valor))) * itens_editados["Valor unitario numerico"].apply(lambda valor: Decimal(str(valor)))
            valor_total = Decimal(str(itens_editados["Valor total"].sum())) if len(itens_editados) else Decimal("0")
            if len(itens_editados):
                resumo = itens_editados[["Item", "Tipo", "Quantidade", "Valor unitario", "Valor total", "Observacoes"]].copy()
                resumo["Valor unitario"] = resumo["Valor unitario"].apply(format_currency_brl)
                resumo["Valor total"] = resumo["Valor total"].apply(format_currency_brl)
                resumo = resumo.rename(columns={"Valor unitario": "Valor unitário", "Observacoes": "Observações"})
                st.dataframe(resumo, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum item adicionado.")
            st.metric("Valor total da cotação", format_currency_brl(valor_total))

            texto_botao_salvar = "Salvar edição da cotação" if editando_cotacao else "Criar cotação"
            if st.button(texto_botao_salvar, key=f"{prefixo}_salvar"):
                cnpj_formatado = format_cpf_cnpj(cnpj)
                cnpj_digitos = apenas_digitos(cnpj_formatado)
                if not fornecedor.strip():
                    st.error("Informe o fornecedor.")
                elif cnpj_digitos and len(cnpj_digitos) not in (11, 14):
                    st.error("Informe um CPF com 11 digitos ou CNPJ com 14 digitos.")
                elif len(itens_editados) == 0:
                    st.error("Adicione pelo menos um item.")
                elif (itens_editados["Quantidade"] <= 0).any():
                    st.error("Todos os itens devem ter quantidade maior que zero.")
                elif (itens_editados["Valor unitario numerico"] < 0).any():
                    st.error("Todos os valores unitários devem ser maiores ou iguais a zero.")
                elif arquivo is None and not str(arquivo_url or "").strip():
                    st.error("Anexe o arquivo da cotação ou informe o link no Google Drive.")
                else:
                    arquivo_url_final = str(arquivo_url or "").strip()
                    upload_resultado = None
                    if arquivo is not None:
                        try:
                            upload_resultado = upload_cotacao_google_drive(
                                arquivo,
                                sid,
                                ordem,
                                rubrica_id=rubrica_id,
                                fornecedor=fornecedor,
                                pasta_url=arquivo_url_final,
                            )
                            arquivo_url_final = upload_resultado["folder_link"]
                        except RuntimeError as exc:
                            st.error(str(exc))
                            st.stop()
                    item_ancora_id = itens_editados.iloc[0]["pedido_item_id"]
                    solicitacao_ancora_id = int(cotacao_atual.get("solicitacao_id") or pedido_itens.loc[pedido_itens.id == item_ancora_id, "pedido_id"].iloc[0])
                    cotacao_id_atual = cotacao_atual.get("id")
                    if cotacao_id_atual:
                        cotacao_salva = query("""
                        update cotacoes
                        set rubrica_id=%s,
                            fornecedor=%s,
                            cnpj_cpf=%s,
                            telefone_email=%s,
                            valor_unitario=%s,
                            valor_total=%s,
                            prazo_entrega=%s,
                            forma_pagamento=%s,
                            arquivo_url=%s,
                            observacoes=%s
                        where id=%s
                        returning id
                        """, (rubrica_id, fornecedor, cnpj_formatado, contato, 0, valor_total, prazo, "", arquivo_url_final, observacoes_gerais.strip() or None, int(cotacao_id_atual)))
                    else:
                        cotacao_por_ordem = query("""
                        select c.id
                        from cotacoes c
                        left join solicitacoes_compra s on s.id = c.solicitacao_id
                        where coalesce(c.rubrica_id, s.rubrica_id)=%s and c.ordem=%s
                        limit 1
                        """, (rubrica_id, ordem))
                        if len(cotacao_por_ordem):
                            cotacao_salva = query("""
                            update cotacoes
                            set rubrica_id=%s,
                                fornecedor=%s,
                                cnpj_cpf=%s,
                                telefone_email=%s,
                                valor_unitario=%s,
                                valor_total=%s,
                                prazo_entrega=%s,
                                forma_pagamento=%s,
                                arquivo_url=%s,
                                observacoes=%s
                            where id=%s
                            returning id
                            """, (rubrica_id, fornecedor, cnpj_formatado, contato, 0, valor_total, prazo, "", arquivo_url_final, observacoes_gerais.strip() or None, int(cotacao_por_ordem.iloc[0]["id"])))
                        else:
                            cotacao_salva = query("""
                            insert into cotacoes (solicitacao_id,rubrica_id,ordem,fornecedor,cnpj_cpf,telefone_email,valor_unitario,valor_total,prazo_entrega,forma_pagamento,arquivo_url,observacoes)
                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            returning id
                            """, (solicitacao_ancora_id, rubrica_id, ordem, fornecedor, cnpj_formatado, contato, 0, valor_total, prazo, "", arquivo_url_final, observacoes_gerais.strip() or None))
                    cotacao_id = int(cotacao_salva.iloc[0]["id"])
                    if upload_resultado:
                        execute("""
                        insert into cotacao_arquivos (
                            cotacao_id,
                            google_drive_file_id,
                            google_drive_link,
                            nome_arquivo,
                            mime_type,
                            tamanho_bytes
                        )
                        values (%s,%s,%s,%s,%s,%s)
                        """, (
                            cotacao_id,
                            upload_resultado["file_id"],
                            upload_resultado["file_link"],
                            upload_resultado["nome_arquivo"],
                            upload_resultado["mime_type"],
                            upload_resultado["tamanho_bytes"],
                        ))
                    execute("delete from cotacao_itens where cotacao_id=%s", (cotacao_id,))
                    for _, item in itens_editados.iterrows():
                        execute("""
                        insert into cotacao_itens (cotacao_id, pedido_item_id, descricao_item, tipo_item, quantidade, valor_unitario, observacoes)
                        values (%s,%s,%s,%s,%s,%s,%s)
                        """, (cotacao_id, item["pedido_item_id"], str(item.get("Item") or "").strip() or None, str(item.get("Tipo") or "").strip() or None, Decimal(str(item["Quantidade"])), Decimal(str(item["Valor unitario numerico"])), str(item.get("Observacoes") or "").strip() or None))
                    solicitacoes_cotadas = pedido_itens[pedido_itens["id"].isin(itens_editados["pedido_item_id"])]["pedido_id"].dropna().unique().tolist()
                    for solicitacao_cotada_id in solicitacoes_cotadas:
                        execute("update solicitacoes_compra set status='cotado' where id=%s", (int(solicitacao_cotada_id),))
                    st.success("Cotação salva com os itens vinculados.")
                    st.session_state[f"cotacao_v2_editando_{sid}"] = None
                    st.rerun()

        modo_opcoes = ["Adicionar nova cotação"]
        if len(cotacoes_salvas_v2):
            modo_opcoes = ["Escolher cotação cadastrada", "Editar cotação cadastrada", "Adicionar nova cotação"]
        modo = st.radio("Ação", modo_opcoes, horizontal=True, key=f"cotacao_v2_modo_{sid}")

        if modo in ["Escolher cotação cadastrada", "Editar cotação cadastrada"]:
            cotacao_id = st.selectbox(
                "Cotação cadastrada para editar" if modo == "Editar cotação cadastrada" else "Cotação cadastrada",
                cotacoes_salvas_v2["id"].tolist(),
                format_func=lambda valor: f"Cotação {int(cotacoes_salvas_v2.loc[cotacoes_salvas_v2.id == valor, 'ordem'].iloc[0])} - {cotacoes_salvas_v2.loc[cotacoes_salvas_v2.id == valor, 'fornecedor'].iloc[0]} - {format_currency_brl(cotacoes_salvas_v2.loc[cotacoes_salvas_v2.id == valor, 'valor_total'].iloc[0])}",
                key=f"cotacao_v2_cadastrada_{sid}_{modo}",
            )
            cotacao_row = cotacoes_salvas_v2[cotacoes_salvas_v2["id"] == cotacao_id].iloc[0]
            itens_cadastrados = cotacao_v2_itens(cotacao_id)
            if modo == "Editar cotação cadastrada" or st.session_state.get(f"cotacao_v2_editando_{sid}") == int(cotacao_id):
                cotacao_v2_formulario(f"cotacao_v2_editar_{sid}_{cotacao_id}", int(cotacao_row["ordem"]), cotacao_row.to_dict(), itens_cadastrados)
            else:
                st.markdown(f"### Cotação {int(cotacao_row['ordem'])}")
                dados_empresa = pd.DataFrame([
                    ("Fornecedor", cotacao_row["fornecedor"]),
                    ("CNPJ/CPF", format_cpf_cnpj(cotacao_row["cnpj_cpf"])),
                    ("Telefone/E-mail", cotacao_row["telefone_email"]),
                    ("Prazo de entrega", cotacao_row["prazo_entrega"]),
                    ("Pasta Google Drive", cotacao_row["arquivo_url"]),
                    ("Observações gerais", cotacao_row["observacoes"]),
                ], columns=["Campo", "Valor"])
                st.dataframe(dados_empresa, use_container_width=True, hide_index=True)
                if str(cotacao_row["arquivo_url"] or "").strip():
                    st.link_button("Abrir pasta da cotação no Google Drive", str(cotacao_row["arquivo_url"]).strip())
                exibir_arquivos_cotacao(cotacao_id)
                st.markdown("### Itens da cotação")
                st.dataframe(cotacao_v2_formatar_itens(itens_cadastrados), use_container_width=True, hide_index=True)
                st.metric("Valor final da cotação", format_currency_brl(cotacao_row["valor_total"]))
                if st.button("Editar cotação", key=f"cotacao_v2_editar_botao_{sid}_{cotacao_id}"):
                    st.session_state[f"cotacao_v2_editando_{sid}"] = int(cotacao_id)
                    cotacao_v2_carregar_estado(f"cotacao_v2_editar_{sid}_{cotacao_id}", itens_cadastrados)
                    st.rerun()
        else:
            usadas = set(cotacoes_salvas_v2["ordem"].astype(int).tolist()) if len(cotacoes_salvas_v2) else set()
            nova_ordem = next((numero for numero in [1, 2, 3] if numero not in usadas), None)
            if nova_ordem is None:
                st.warning("Já existem 3 cotações cadastradas. Escolha uma cotação cadastrada e clique em Editar cotação.")
            else:
                cotacao_v2_formulario(f"cotacao_v2_nova_{sid}_{nova_ordem}", nova_ordem)

        if len(cotacoes_salvas_v2):
            resumo_cotacoes = cotacoes_salvas_v2[["ordem", "fornecedor", "total_itens", "valor_total", "prazo_entrega", "arquivo_url"]].copy()
            resumo_cotacoes = resumo_cotacoes.rename(columns={"ordem": "Cotação", "fornecedor": "Fornecedor", "total_itens": "Itens", "valor_total": "Valor total", "prazo_entrega": "Prazo", "arquivo_url": "Link"})
            resumo_cotacoes["Valor total"] = resumo_cotacoes["Valor total"].apply(format_currency_brl)
            st.markdown("### Cotações cadastradas")
            st.dataframe(resumo_cotacoes, use_container_width=True, hide_index=True, column_config={"Link": st.column_config.LinkColumn("Link")})

        st.stop()

elif menu == "compra_nota":
    exibir_resumo_valores_extra_nao_debitados()
    solicitacoes_compra = query("""
    select id, rubrica_id, descricao
    from solicitacoes_compra
    where autorizado=true and status in ('cotado','aguardando_nota')
    order by id desc
    """)
    if len(solicitacoes_compra) == 0:
        st.info("Não há solicitações pendentes para compra ou nota fiscal.")
        st.stop()
    sid = st.selectbox("Solicitação", solicitacoes_compra["id"].tolist(), format_func=lambda x: f"#{x} - {solicitacoes_compra.loc[solicitacoes_compra.id==x,'descricao'].iloc[0][:80]}")
    rubrica_compra_id = int(solicitacoes_compra.loc[solicitacoes_compra.id == sid, "rubrica_id"].iloc[0])
    if st.button("Cancelar compra"):
        cancelar_solicitacao(sid, user["id"])
        st.success("Compra cancelada e solicitação removida dos registros ativos.")
        st.rerun()
    cotacoes_itens_df = query("""
    select
      ci.id,
      ci.pedido_item_id,
      ci.cotacao_id,
      c.ordem,
      c.fornecedor,
      coalesce(ci.descricao_item, pi.descricao) as item,
      coalesce(ci.tipo_item, pi.tipo_item) as tipo_item,
      ci.quantidade,
      ci.valor_unitario,
      ci.valor_total as "Valor total",
      ci.vencedor
    from cotacao_itens ci
    join cotacoes c on c.id = ci.cotacao_id
    left join solicitacoes_compra sc on sc.id = c.solicitacao_id
    join pedido_itens pi on pi.id = ci.pedido_item_id
    where coalesce(c.rubrica_id, sc.rubrica_id)=%s
    order by pi.descricao, c.ordem
    """, (rubrica_compra_id,))
    if len(cotacoes_itens_df) == 0:
        st.warning("Não há itens cotados para essa rubrica.")
        st.stop()

    cotacoes_resumo = query("""
    select
      c.id,
      c.ordem,
      c.fornecedor,
      c.prazo_entrega,
      c.arquivo_url as "Cotação",
      coalesce(sum(ci.valor_total), 0) as valor_total,
      count(ci.id) as total_itens,
      c.vencedora
    from cotacoes c
    left join solicitacoes_compra sc on sc.id = c.solicitacao_id
    join cotacao_itens ci on ci.cotacao_id = c.id
    where coalesce(c.rubrica_id, sc.rubrica_id)=%s
    group by c.id, c.ordem, c.fornecedor, c.prazo_entrega, c.arquivo_url, c.vencedora
    order by valor_total asc, c.ordem
    """, (rubrica_compra_id,))

    st.markdown("### Propostas recebidas")
    cotacoes_resumo_exibicao = cotacoes_resumo.copy()
    cotacoes_resumo_exibicao["valor_total"] = cotacoes_resumo_exibicao["valor_total"].apply(format_currency_brl)
    st.dataframe(
        cotacoes_resumo_exibicao,
        use_container_width=True,
        hide_index=True,
        column_config={
            "id": None,
            "valor_total": st.column_config.TextColumn("Valor total"),
            "Cotação": st.column_config.LinkColumn("Cotação"),
            "vencedora": st.column_config.CheckboxColumn("Vencedora"),
        },
    )

    cotacao_vencedora_atual = cotacoes_resumo[cotacoes_resumo["vencedora"] == True]
    indice_padrao = 0
    if len(cotacao_vencedora_atual):
        cotacao_id_atual = int(cotacao_vencedora_atual.iloc[0]["id"])
        ids_resumo = [int(valor) for valor in cotacoes_resumo["id"].tolist()]
        if cotacao_id_atual in ids_resumo:
            indice_padrao = ids_resumo.index(cotacao_id_atual)

    cotacao_vencedora_id = st.selectbox(
        "Cotação vencedora",
        cotacoes_resumo["id"].tolist(),
        index=indice_padrao,
        format_func=lambda cotacao_id: (
            f"#{int(cotacoes_resumo.loc[cotacoes_resumo.id == cotacao_id, 'ordem'].iloc[0])} - "
            f"{cotacoes_resumo.loc[cotacoes_resumo.id == cotacao_id, 'fornecedor'].iloc[0]} - "
            f"{format_currency_brl(cotacoes_resumo.loc[cotacoes_resumo.id == cotacao_id, 'valor_total'].iloc[0])}"
        ),
        key=f"cotacao_vencedora_{sid}",
    )

    st.markdown("### Itens da proposta selecionada")
    itens_cotacao_selecionada = cotacoes_itens_df[cotacoes_itens_df["cotacao_id"] == cotacao_vencedora_id].copy()
    itens_cotacao_exibicao = itens_cotacao_selecionada[["ordem", "fornecedor", "item", "tipo_item", "quantidade", "valor_unitario", "Valor total"]].copy()
    itens_cotacao_exibicao = itens_cotacao_exibicao.rename(columns={
        "ordem": "Cotação",
        "fornecedor": "Fornecedor",
        "item": "Item",
        "tipo_item": "Tipo",
        "quantidade": "Quantidade",
        "valor_unitario": "Valor unitário",
    })
    itens_cotacao_exibicao["Valor unitário"] = itens_cotacao_exibicao["Valor unitário"].apply(format_currency_brl)
    itens_cotacao_exibicao["Valor total"] = itens_cotacao_exibicao["Valor total"].apply(format_currency_brl)
    st.dataframe(
        itens_cotacao_exibicao,
        use_container_width=True,
        hide_index=True,
    )

    if st.button("Registrar compra"):
        if len(itens_cotacao_selecionada) == 0:
            st.error("A cotação selecionada não tem itens vinculados.")
        else:
            execute("""
            update cotacao_itens ci
            set vencedor=false
            from cotacoes c
            left join solicitacoes_compra sc on sc.id = c.solicitacao_id
            where c.id = ci.cotacao_id
              and coalesce(c.rubrica_id, sc.rubrica_id)=%s
            """, (rubrica_compra_id,))
            execute("""
            update cotacoes c
            set vencedora=false
            from solicitacoes_compra sc
            where sc.id = c.solicitacao_id
              and coalesce(c.rubrica_id, sc.rubrica_id)=%s
            """, (rubrica_compra_id,))
            execute("update cotacao_itens set vencedor=true where cotacao_id=%s", (int(cotacao_vencedora_id),))
            execute("update cotacoes set vencedora=true where id=%s", (int(cotacao_vencedora_id),))
            valor = Decimal(str(itens_cotacao_selecionada["Valor total"].sum()))
            execute("""
            insert into compras (solicitacao_id,cotacao_vencedora_id,valor_compra,comprador_id)
            values (%s,%s,%s,%s)
            on conflict (solicitacao_id) do update set
              cotacao_vencedora_id=excluded.cotacao_vencedora_id,
              valor_compra=excluded.valor_compra,
              comprador_id=excluded.comprador_id
            """, (sid, int(cotacao_vencedora_id), valor, user["id"]))
            pedido_item_ids = []
            for valor_item_id in itens_cotacao_selecionada["pedido_item_id"].dropna().tolist():
                try:
                    pedido_item_ids.append(int(float(valor_item_id)))
                except (TypeError, ValueError):
                    continue
            if pedido_item_ids:
                solicitacoes_compra_vencedora = query("""
                select distinct pi.pedido_id
                from pedido_itens pi
                where pi.id = any(%s)
                """, (pedido_item_ids,))
                for solicitacao_compra_id in solicitacoes_compra_vencedora["pedido_id"].dropna().tolist():
                    execute("update solicitacoes_compra set status='aguardando_nota' where id=%s", (int(solicitacao_compra_id),))
            sincronizar_orcamento()
            st.success("Compra registrada pela cotação vencedora. Orçamento atualizado e status: aguardando nota.")

    cabecalho_nf, acao_editar_nf = st.columns([4, 1])
    cabecalho_nf.markdown("### Lançar nota fiscal")
    if acao_editar_nf.button("Editar NF", use_container_width=True, key=f"editar_nf_botao_{rubrica_compra_id}"):
        editar_numero_arquivo_nf_dialog(rubrica_compra_id)
    compra_df = query("""
    select c.id, c.valor_compra
    from compras c
    join cotacoes co on co.id = c.cotacao_vencedora_id
    left join solicitacoes_compra sc on sc.id = co.solicitacao_id
    where coalesce(co.rubrica_id, sc.rubrica_id)=%s
    order by c.comprado_em desc
    limit 1
    """, (rubrica_compra_id,))
    if len(compra_df) == 0:
        st.info("Registre a compra desta solicitação antes de lançar a nota fiscal.")
    else:
        compra_id = int(compra_df.iloc[0]["id"])
        valor_compra = float(compra_df.iloc[0]["valor_compra"])
        st.number_input("ID da compra", min_value=1, value=compra_id, disabled=True, key=f"nota_compra_id_{sid}_{compra_id}")
        with st.expander("Registrar valor extra nao debitado do projeto", expanded=False):
            st.caption("Use para taxas bancarias, TED ou outros custos que serao pagos fora do orcamento do projeto.")
            notas_compra_extra = query("""
            select id, numero_nf, fornecedor, valor_nf
            from notas_fiscais
            where compra_id=%s
            order by lancado_em desc
            """, (compra_id,))
            nota_extra_opcoes = [None] + notas_compra_extra["id"].tolist() if len(notas_compra_extra) else [None]
            nota_extra_id = st.selectbox(
                "Nota fiscal vinculada",
                nota_extra_opcoes,
                format_func=lambda valor: "Sem NF vinculada" if valor is None else (
                    f"NF {notas_compra_extra.loc[notas_compra_extra.id == valor, 'numero_nf'].iloc[0]} - "
                    f"{notas_compra_extra.loc[notas_compra_extra.id == valor, 'fornecedor'].iloc[0]}"
                ),
                key=f"extra_nota_{compra_id}",
            )
            extra_tipo = st.selectbox(
                "Tipo",
                ["Taxa TED", "Tarifa bancaria", "Frete extra", "Outro"],
                key=f"extra_tipo_{compra_id}",
            )
            extra_valor = st.number_input(
                "Valor nao debitado do projeto",
                min_value=0.0,
                step=1.0,
                format="%.2f",
                key=f"extra_valor_{compra_id}",
            )
            extra_responsavel = st.text_input(
                "Responsavel pelo pagamento",
                value="Gerente do projeto",
                key=f"extra_responsavel_{compra_id}",
            )
            extra_data = st.date_input("Data do pagamento/registro", value=date.today(), key=f"extra_data_{compra_id}")
            extra_descricao = st.text_area(
                "Descricao/justificativa",
                value="Taxa gerada por pagamento via TED.",
                key=f"extra_descricao_{compra_id}",
            )
            if st.button("Salvar valor extra", key=f"extra_salvar_{compra_id}"):
                if Decimal(str(extra_valor)) <= 0:
                    st.error("Informe um valor maior que zero.")
                elif not str(extra_descricao or "").strip():
                    st.error("Informe a descricao do valor extra.")
                else:
                    execute("""
                    insert into valores_extra_nao_debitados
                      (compra_id, nota_fiscal_id, rubrica_id, solicitacao_id, tipo, descricao, valor, responsavel_pagamento, data_pagamento, registrado_por)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        compra_id,
                        int(nota_extra_id) if nota_extra_id is not None else None,
                        rubrica_compra_id,
                        sid,
                        extra_tipo,
                        extra_descricao.strip(),
                        Decimal(str(extra_valor)),
                        extra_responsavel.strip() or None,
                        extra_data,
                        user["id"],
                    ))
                    st.success("Valor extra registrado sem debitar do projeto.")
                    st.rerun()
            valores_extra_compra = carregar_valores_extra_nao_debitados(compra_id)
            if len(valores_extra_compra):
                total_extra_compra = valores_extra_compra["valor"].sum()
                st.metric("Total extra desta compra", format_currency_brl(total_extra_compra))
                tabela_extra_compra = valores_extra_compra.rename(columns={
                    "tipo": "Tipo",
                    "descricao": "Descricao",
                    "valor": "Valor",
                    "responsavel_pagamento": "Responsavel",
                    "data_pagamento": "Data",
                    "criado_em": "Registrado em",
                })[["Tipo", "Descricao", "Valor", "Responsavel", "Data", "Registrado em"]].copy()
                tabela_extra_compra["Valor"] = tabela_extra_compra["Valor"].apply(format_currency_brl)
                st.dataframe(tabela_extra_compra, use_container_width=True, hide_index=True)

        with st.expander("Comprovante bancario", expanded=False):
            notas_comprovante = query("""
            select id, numero_nf, fornecedor, valor_nf
            from notas_fiscais
            where compra_id=%s
            order by lancado_em desc
            """, (compra_id,))
            comprovantes_salvos = comprovantes_bancarios_df(compra_id)
            pasta_comprovante_atual = ""
            if len(comprovantes_salvos):
                pasta_comprovante_atual = str(comprovantes_salvos.iloc[0]["pasta_google_drive_link"] or "").strip()
            if pasta_comprovante_atual:
                st.link_button("Abrir pasta dos comprovantes bancarios", pasta_comprovante_atual)

            nota_comprovante_opcoes = [None] + notas_comprovante["id"].tolist() if len(notas_comprovante) else [None]
            nota_comprovante_id = st.selectbox(
                "Nota fiscal vinculada ao comprovante",
                nota_comprovante_opcoes,
                format_func=lambda valor: "Sem NF vinculada" if valor is None else (
                    f"NF {notas_comprovante.loc[notas_comprovante.id == valor, 'numero_nf'].iloc[0]} - "
                    f"{notas_comprovante.loc[notas_comprovante.id == valor, 'fornecedor'].iloc[0]}"
                ),
                key=f"comprovante_nota_{compra_id}",
            )
            arquivo_comprovante = st.file_uploader(
                "Upload do comprovante bancario",
                type=["pdf", "png", "jpg", "jpeg"],
                key=f"comprovante_arquivo_{compra_id}",
            )
            link_pasta_comprovante = st.text_input(
                "Link da pasta de comprovantes no Google Drive",
                value=pasta_comprovante_atual,
                key=f"comprovante_pasta_{compra_id}",
            )
            observacao_comprovante = st.text_area(
                "Observacao do comprovante",
                key=f"comprovante_observacao_{compra_id}",
            )
            if st.button("Enviar comprovante bancario", use_container_width=True, key=f"comprovante_salvar_{compra_id}"):
                if arquivo_comprovante is None:
                    st.error("Anexe o comprovante bancario.")
                else:
                    fornecedor_comprovante = ""
                    if nota_comprovante_id is not None and len(notas_comprovante):
                        fornecedor_comprovante = str(notas_comprovante.loc[notas_comprovante.id == nota_comprovante_id, "fornecedor"].iloc[0] or "")
                    elif len(itens_cotacao_selecionada):
                        fornecedor_comprovante = str(itens_cotacao_selecionada["fornecedor"].iloc[0] or "")
                    try:
                        upload_comprovante = upload_comprovante_bancario_google_drive(
                            arquivo_comprovante,
                            compra_id,
                            fornecedor=fornecedor_comprovante,
                            pasta_url=str(link_pasta_comprovante or "").strip(),
                        )
                    except RuntimeError as exc:
                        st.error(str(exc))
                        st.stop()
                    execute("""
                    insert into comprovantes_bancarios (
                        compra_id,
                        nota_fiscal_id,
                        google_drive_file_id,
                        google_drive_link,
                        pasta_google_drive_link,
                        nome_arquivo,
                        mime_type,
                        tamanho_bytes,
                        observacao,
                        enviado_por
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        compra_id,
                        int(nota_comprovante_id) if nota_comprovante_id is not None else None,
                        upload_comprovante["file_id"],
                        upload_comprovante["file_link"],
                        upload_comprovante["folder_link"],
                        upload_comprovante["nome_arquivo"],
                        upload_comprovante["mime_type"],
                        upload_comprovante["tamanho_bytes"],
                        observacao_comprovante.strip() or None,
                        user["id"],
                    ))
                    st.success("Comprovante bancario enviado.")
                    st.rerun()
            exibir_comprovantes_bancarios(compra_id)

        itens_vencedores = query("""
        select
          ci.pedido_item_id,
          coalesce(ci.descricao_item, pi.descricao) as descricao,
          coalesce(ci.tipo_item, pi.tipo_item) as tipo_item,
          ci.quantidade,
          ci.valor_unitario,
          ci.valor_total,
          c.fornecedor
        from cotacao_itens ci
        join cotacoes c on c.id = ci.cotacao_id
        left join solicitacoes_compra sc on sc.id = c.solicitacao_id
        join pedido_itens pi on pi.id = ci.pedido_item_id
        where coalesce(c.rubrica_id, sc.rubrica_id)=%s and ci.vencedor=true
        order by c.fornecedor, pi.descricao
        """, (rubrica_compra_id,))
        itens_lancados = query("""
        select pedido_item_id
        from nota_fiscal_itens nfi
        join pedido_itens pi on pi.id = nfi.pedido_item_id
        where pi.rubrica_id=%s and nfi.pedido_item_id is not null
        """, (rubrica_compra_id,))
        ids_lancados = set(itens_lancados["pedido_item_id"].tolist()) if len(itens_lancados) else set()
        ids_vencedores = set(itens_vencedores["pedido_item_id"].tolist()) if len(itens_vencedores) else set()
        ids_vencedores_lancados = ids_lancados.intersection(ids_vencedores)
        itens_pendentes = itens_vencedores[~itens_vencedores["pedido_item_id"].isin(ids_lancados)].copy()

        if len(itens_pendentes) == 0:
            st.success("Todos os itens vencedores ja foram vinculados a notas fiscais.")
        else:
            opcoes_itens_nf = itens_pendentes["pedido_item_id"].tolist()
            itens_nf = st.multiselect(
                "Itens desta NF",
                opcoes_itens_nf,
                format_func=lambda item_id: (
                    f"{itens_pendentes.loc[itens_pendentes.pedido_item_id == item_id, 'descricao'].iloc[0]} - "
                    f"{itens_pendentes.loc[itens_pendentes.pedido_item_id == item_id, 'fornecedor'].iloc[0]}"
                ),
                key=f"nota_itens_{compra_id}",
            )
            itens_nf_df = itens_pendentes[itens_pendentes["pedido_item_id"].isin(itens_nf)].copy()
            fornecedor_padrao = ""
            if len(itens_nf_df) and itens_nf_df["fornecedor"].nunique() == 1:
                fornecedor_padrao = itens_nf_df["fornecedor"].iloc[0]
            valor_nf_padrao = float(itens_nf_df["valor_total"].sum()) if len(itens_nf_df) else 0.0
            numero_nf = st.text_input("Número da NF")
            fornecedor_nf = st.text_input("Fornecedor da NF", value=fornecedor_padrao)
            arquivo_nf = st.file_uploader(
                "Arquivo da nota fiscal para o Google Drive",
                type=["pdf", "png", "jpg", "jpeg", "xml"],
                key=f"nota_arquivo_{compra_id}",
            )
            local_nf = st.text_input("Local/link da NF no Google Drive")
            if str(local_nf or "").strip():
                st.link_button("Abrir pasta da nota fiscal no Google Drive", str(local_nf).strip())
            if numero_nf.strip() and fornecedor_nf.strip():
                nota_nf_existente = query("""
                select id, arquivo_url
                from notas_fiscais
                where lower(trim(numero_nf)) = lower(trim(%s))
                  and lower(trim(fornecedor)) = lower(trim(%s))
                limit 1
                """, (numero_nf, fornecedor_nf))
                if len(nota_nf_existente):
                    pasta_nf_existente = str(nota_nf_existente.iloc[0]["arquivo_url"] or "").strip()
                    if pasta_nf_existente and pasta_nf_existente != str(local_nf or "").strip():
                        st.link_button("Abrir pasta existente da nota fiscal", pasta_nf_existente)
                    exibir_arquivos_nota_fiscal(int(nota_nf_existente.iloc[0]["id"]))
            if len(itens_nf_df):
                itens_nf_editor = itens_nf_df[["pedido_item_id", "descricao", "fornecedor", "tipo_item", "quantidade", "valor_unitario"]].copy()
                itens_nf_editor = itens_nf_editor.rename(columns={
                    "descricao": "Item",
                    "fornecedor": "Fornecedor",
                    "tipo_item": "Tipo",
                    "quantidade": "Quantidade",
                    "valor_unitario": "Valor unitario NF",
                })
                itens_nf_editor = st.data_editor(
                    itens_nf_editor,
                    use_container_width=True,
                    hide_index=True,
                    disabled=["pedido_item_id", "Item", "Fornecedor", "Tipo", "Quantidade"],
                    column_config={
                        "pedido_item_id": None,
                        "Valor unitario NF": st.column_config.NumberColumn("Valor unitario NF", min_value=0.0, format="R$ %.2f"),
                    },
                    key=f"nota_itens_valores_{compra_id}_{len(itens_nf_df)}",
                )
                itens_nf_editor["Quantidade"] = pd.to_numeric(itens_nf_editor["Quantidade"], errors="coerce").fillna(0)
                itens_nf_editor["Valor unitario NF"] = pd.to_numeric(itens_nf_editor["Valor unitario NF"], errors="coerce").fillna(0)
                itens_nf_editor["Valor total NF"] = itens_nf_editor["Quantidade"] * itens_nf_editor["Valor unitario NF"]
                valor_nf_padrao = float(itens_nf_editor["Valor total NF"].sum())
                st.dataframe(
                    preparar_tabela_auditoria(itens_nf_editor[["Item", "Fornecedor", "Quantidade", "Valor unitario NF", "Valor total NF"]].rename(columns={
                        "Valor unitario NF": "Valor do item na NF",
                        "Valor total NF": "Valor da nota",
                    })),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                itens_nf_editor = pd.DataFrame()
            valor_nf = st.number_input("Valor da NF", min_value=0.0, value=valor_nf_padrao, key=f"nota_valor_nf_{compra_id}_{valor_nf_padrao:.2f}")
        data_nf = st.date_input("Data de emissão", value=date.today())
        if st.button("Salvar nota fiscal"):
            if len(itens_pendentes) == 0:
                st.info("Não há itens pendentes para lançar.")
            elif len(itens_nf_df) == 0:
                st.error("Selecione pelo menos um item para a nota fiscal.")
            elif itens_nf_df["fornecedor"].nunique() != 1:
                st.error("Uma NF deve conter itens de um único fornecedor vencedor.")
            elif not numero_nf.strip() or not fornecedor_nf.strip():
                st.error("Informe número da NF e fornecedor.")
            elif arquivo_nf is None and not local_nf.strip():
                st.error("Anexe o arquivo da NF ou informe o local/link no Google Drive.")
            elif fornecedor_nf.strip().lower() != str(itens_nf_df["fornecedor"].iloc[0]).strip().lower():
                st.error("O fornecedor da NF deve ser o mesmo fornecedor vencedor dos itens selecionados.")
            elif Decimal(str(valor_nf)) != Decimal(str(valor_nf_padrao)):
                st.error("O valor da NF deve bater com a soma dos itens selecionados.")
            else:
                upload_nf_resultado = None
                local_nf_final = local_nf.strip()
                if arquivo_nf is not None:
                    try:
                        upload_nf_resultado = upload_nota_fiscal_google_drive(
                            arquivo_nf,
                            numero_nf.strip(),
                            fornecedor_nf.strip(),
                            pasta_url=local_nf_final,
                        )
                        local_nf_final = upload_nf_resultado["folder_link"]
                    except RuntimeError as exc:
                        st.error(str(exc))
                        st.stop()
                nota_existente = query("""
                select id, valor_nf
                from notas_fiscais
                where lower(trim(numero_nf)) = lower(trim(%s))
                  and lower(trim(fornecedor)) = lower(trim(%s))
                limit 1
                """, (numero_nf, fornecedor_nf))
                if len(nota_existente):
                    nota_id = int(nota_existente.iloc[0]["id"])
                    valor_nf_atualizado = Decimal(str(nota_existente.iloc[0]["valor_nf"])) + Decimal(str(valor_nf))
                    execute("""
                    update notas_fiscais
                    set valor_nf=%s,
                        arquivo_url=coalesce(nullif(%s, ''), arquivo_url),
                        data_emissao=coalesce(data_emissao, %s),
                        lancado_por=coalesce(lancado_por, %s)
                    where id=%s
                    """, (valor_nf_atualizado, local_nf_final, data_nf, user["id"], nota_id))
                else:
                    nota_criada = query("""
                    insert into notas_fiscais (compra_id, solicitacao_id, numero_nf, fornecedor, valor_nf, data_emissao, arquivo_url, lancado_por)
                    values (%s,%s,%s,%s,%s,%s,%s,%s)
                    returning id
                    """, (compra_id, sid, numero_nf, fornecedor_nf, valor_nf, data_nf, local_nf_final, user["id"]))
                    nota_id = int(nota_criada.iloc[0]["id"])
                if upload_nf_resultado:
                    execute("""
                    insert into nota_fiscal_arquivos (
                        nota_fiscal_id,
                        google_drive_file_id,
                        google_drive_link,
                        nome_arquivo,
                        mime_type,
                        tamanho_bytes
                    ) values (%s,%s,%s,%s,%s,%s)
                    """, (
                        nota_id,
                        upload_nf_resultado["file_id"],
                        upload_nf_resultado["file_link"],
                        upload_nf_resultado["nome_arquivo"],
                        upload_nf_resultado["mime_type"],
                        upload_nf_resultado["tamanho_bytes"],
                    ))
                itens_nf_gravacao = itens_nf_df.merge(
                    itens_nf_editor[["pedido_item_id", "Valor unitario NF"]],
                    on="pedido_item_id",
                    how="left",
                )
                for _, item_nf in itens_nf_gravacao.iterrows():
                    execute("""
                    insert into nota_fiscal_itens
                      (nota_fiscal_id, pedido_item_id, descricao, tipo_item, quantidade, valor_unitario)
                    values (%s,%s,%s,%s,%s,%s)
                    """, (
                        nota_id,
                        item_nf["pedido_item_id"],
                        item_nf["descricao"],
                        item_nf["tipo_item"],
                        Decimal(str(item_nf["quantidade"])),
                        Decimal(str(item_nf["Valor unitario NF"])),
                    ))
                st.success("Nota fiscal salva. Finalize a compra somente depois de conferir a nota.")
                sincronizar_orcamento()

        st.markdown("### Finalizar")
        if st.button("Finalizar compra e nota fiscal", type="primary", key=f"finalizar_nf_{rubrica_compra_id}_{compra_id}"):
            itens_sem_nf = ids_vencedores.difference(ids_lancados)
            if itens_sem_nf:
                st.error("Salve a nota fiscal de todos os itens vencedores antes de finalizar.")
            else:
                total_real_nf = query("""
                select coalesce(sum(nfi.valor_total), 0) as valor_total_real
                from nota_fiscal_itens nfi
                where nfi.pedido_item_id = any(%s::uuid[])
                """, (itens_vencedores["pedido_item_id"].tolist(),))
                valor_total_real = Decimal(str(total_real_nf.iloc[0]["valor_total_real"])) if len(total_real_nf) else Decimal("0")
                execute("update compras set valor_compra=%s where id=%s", (valor_total_real, compra_id))
                solicitacoes_finalizadas = query("""
                select distinct pedido_id
                from pedido_itens
                where id = any(%s::uuid[])
                """, (itens_vencedores["pedido_item_id"].tolist(),))
                for solicitacao_finalizada_id in solicitacoes_finalizadas["pedido_id"].dropna().tolist():
                    execute("update solicitacoes_compra set status='finalizado' where id=%s", (int(solicitacao_finalizada_id),))
                sincronizar_orcamento()
                st.success("Compra e nota fiscal finalizadas.")
                st.rerun()

elif menu == "comprovantes_bancarios":
    compras_comprovante = query("""
    select
      c.id as compra_id,
      s.id as solicitacao_id,
      r.codigo as rubrica,
      r.nome as rubrica_nome,
      coalesce(co.fornecedor, '-') as fornecedor,
      c.valor_compra,
      c.comprado_em,
      s.status
    from compras c
    join solicitacoes_compra s on s.id = c.solicitacao_id
    join rubricas r on r.id = s.rubrica_id
    left join cotacoes co on co.id = c.cotacao_vencedora_id
    order by c.comprado_em desc, c.id desc
    """)
    if len(compras_comprovante) == 0:
        st.info("Registre uma compra antes de enviar comprovantes bancarios.")
        st.stop()

    compra_id = st.selectbox(
        "Compra",
        compras_comprovante["compra_id"].tolist(),
        format_func=lambda valor: (
            f"Compra #{int(valor)} - "
            f"Solicitacao #{int(compras_comprovante.loc[compras_comprovante.compra_id == valor, 'solicitacao_id'].iloc[0])} - "
            f"{compras_comprovante.loc[compras_comprovante.compra_id == valor, 'rubrica'].iloc[0]} - "
            f"{compras_comprovante.loc[compras_comprovante.compra_id == valor, 'fornecedor'].iloc[0]} - "
            f"{format_currency_brl(compras_comprovante.loc[compras_comprovante.compra_id == valor, 'valor_compra'].iloc[0])}"
        ),
        key="comprovantes_menu_compra",
    )
    compra_comprovante = compras_comprovante[compras_comprovante["compra_id"] == compra_id].iloc[0]
    fornecedor_comprovante_padrao = str(compra_comprovante["fornecedor"] or "")
    st.caption(
        f"Rubrica {compra_comprovante['rubrica']} - {compra_comprovante['rubrica_nome']} | "
        f"Status: {normalizar_texto_portugues(compra_comprovante['status'])}"
    )

    notas_comprovante = query("""
    select id, numero_nf, fornecedor, valor_nf
    from notas_fiscais
    where compra_id=%s
    order by lancado_em desc, id desc
    """, (int(compra_id),))
    comprovantes_salvos = comprovantes_bancarios_df(compra_id)
    if len(comprovantes_salvos):
        st.metric("Comprovantes vinculados", len(comprovantes_salvos))
        exibir_comprovantes_bancarios(compra_id)
    else:
        st.info("Esta compra ainda nao tem comprovante bancario.")

    modo_comprovante = st.radio(
        "Acao",
        ["Carregar novo comprovante", "Editar comprovante existente"],
        horizontal=True,
        key=f"comprovante_modo_{compra_id}",
    )

    nota_opcoes = [None] + notas_comprovante["id"].tolist() if len(notas_comprovante) else [None]
    def label_nota_comprovante(valor):
        if valor is None:
            return "Sem NF vinculada"
        return (
            f"NF {notas_comprovante.loc[notas_comprovante.id == valor, 'numero_nf'].iloc[0]} - "
            f"{notas_comprovante.loc[notas_comprovante.id == valor, 'fornecedor'].iloc[0]} - "
            f"{format_currency_brl(notas_comprovante.loc[notas_comprovante.id == valor, 'valor_nf'].iloc[0])}"
        )

    pasta_comprovante_atual = ""
    if len(comprovantes_salvos):
        pasta_comprovante_atual = str(comprovantes_salvos.iloc[0]["pasta_google_drive_link"] or "").strip()

    if modo_comprovante == "Carregar novo comprovante":
        st.markdown("### Carregar comprovante bancario")
        nota_id = st.selectbox(
            "Nota fiscal vinculada",
            nota_opcoes,
            format_func=label_nota_comprovante,
            key=f"comprovante_menu_nota_novo_{compra_id}",
        )
        arquivo_comprovante = st.file_uploader(
            "Arquivo do comprovante bancario",
            type=["pdf", "png", "jpg", "jpeg"],
            key=f"comprovante_menu_arquivo_novo_{compra_id}",
        )
        link_pasta = st.text_input(
            "Link da pasta de comprovantes no Google Drive",
            value=pasta_comprovante_atual,
            key=f"comprovante_menu_pasta_novo_{compra_id}",
        )
        observacao = st.text_area("Observacao", key=f"comprovante_menu_obs_novo_{compra_id}")
        if st.button("Enviar comprovante bancario", type="primary", use_container_width=True, key=f"comprovante_menu_salvar_novo_{compra_id}"):
            if arquivo_comprovante is None:
                st.error("Anexe o comprovante bancario.")
            else:
                if nota_id is not None and len(notas_comprovante):
                    fornecedor_upload = str(notas_comprovante.loc[notas_comprovante.id == nota_id, "fornecedor"].iloc[0] or "")
                else:
                    fornecedor_upload = fornecedor_comprovante_padrao
                try:
                    upload_comprovante = upload_comprovante_bancario_google_drive(
                        arquivo_comprovante,
                        int(compra_id),
                        fornecedor=fornecedor_upload,
                        pasta_url=str(link_pasta or "").strip(),
                    )
                except RuntimeError as exc:
                    st.error(str(exc))
                    st.stop()
                execute("""
                insert into comprovantes_bancarios (
                    compra_id,
                    nota_fiscal_id,
                    google_drive_file_id,
                    google_drive_link,
                    pasta_google_drive_link,
                    nome_arquivo,
                    mime_type,
                    tamanho_bytes,
                    observacao,
                    enviado_por
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    int(compra_id),
                    int(nota_id) if nota_id is not None else None,
                    upload_comprovante["file_id"],
                    upload_comprovante["file_link"],
                    upload_comprovante["folder_link"],
                    upload_comprovante["nome_arquivo"],
                    upload_comprovante["mime_type"],
                    upload_comprovante["tamanho_bytes"],
                    observacao.strip() or None,
                    user["id"],
                ))
                st.success("Comprovante bancario enviado.")
                st.rerun()
    else:
        st.markdown("### Editar comprovante existente")
        if len(comprovantes_salvos) == 0:
            st.info("Nao ha comprovante salvo para editar nesta compra.")
        else:
            comprovante_id = st.selectbox(
                "Comprovante",
                comprovantes_salvos["id"].tolist(),
                format_func=lambda valor: (
                    f"{comprovantes_salvos.loc[comprovantes_salvos.id == valor, 'nome_arquivo'].iloc[0]} - "
                    f"{comprovantes_salvos.loc[comprovantes_salvos.id == valor, 'criado_em'].iloc[0]}"
                ),
                key=f"comprovante_menu_editar_id_{compra_id}",
            )
            comprovante = comprovantes_salvos[comprovantes_salvos["id"] == comprovante_id].iloc[0]
            if str(comprovante["google_drive_link"] or "").strip():
                st.link_button("Abrir comprovante atual", str(comprovante["google_drive_link"]).strip())
            nota_atual = int(comprovante["nota_fiscal_id"]) if comprovante["nota_fiscal_id"] is not None and not pd.isna(comprovante["nota_fiscal_id"]) else None
            nota_id = st.selectbox(
                "Nota fiscal vinculada",
                nota_opcoes,
                index=nota_opcoes.index(nota_atual) if nota_atual in nota_opcoes else 0,
                format_func=label_nota_comprovante,
                key=f"comprovante_menu_nota_editar_{comprovante_id}",
            )
            arquivo_comprovante = st.file_uploader(
                "Substituir arquivo do comprovante bancario",
                type=["pdf", "png", "jpg", "jpeg"],
                key=f"comprovante_menu_arquivo_editar_{comprovante_id}",
            )
            link_pasta = st.text_input(
                "Link da pasta de comprovantes no Google Drive",
                value=str(comprovante["pasta_google_drive_link"] or ""),
                key=f"comprovante_menu_pasta_editar_{comprovante_id}",
            )
            observacao = st.text_area(
                "Observacao",
                value=str(comprovante["observacao"] or ""),
                key=f"comprovante_menu_obs_editar_{comprovante_id}",
            )
            if st.button("Salvar alteracoes do comprovante", type="primary", use_container_width=True, key=f"comprovante_menu_salvar_editar_{comprovante_id}"):
                if arquivo_comprovante is not None:
                    if nota_id is not None and len(notas_comprovante):
                        fornecedor_upload = str(notas_comprovante.loc[notas_comprovante.id == nota_id, "fornecedor"].iloc[0] or "")
                    else:
                        fornecedor_upload = fornecedor_comprovante_padrao
                    try:
                        upload_comprovante = upload_comprovante_bancario_google_drive(
                            arquivo_comprovante,
                            int(compra_id),
                            fornecedor=fornecedor_upload,
                            pasta_url=str(link_pasta or "").strip(),
                        )
                    except RuntimeError as exc:
                        st.error(str(exc))
                        st.stop()
                    execute("""
                    update comprovantes_bancarios
                    set nota_fiscal_id=%s,
                        google_drive_file_id=%s,
                        google_drive_link=%s,
                        pasta_google_drive_link=%s,
                        nome_arquivo=%s,
                        mime_type=%s,
                        tamanho_bytes=%s,
                        observacao=%s,
                        enviado_por=%s
                    where id=%s
                    """, (
                        int(nota_id) if nota_id is not None else None,
                        upload_comprovante["file_id"],
                        upload_comprovante["file_link"],
                        upload_comprovante["folder_link"],
                        upload_comprovante["nome_arquivo"],
                        upload_comprovante["mime_type"],
                        upload_comprovante["tamanho_bytes"],
                        observacao.strip() or None,
                        user["id"],
                        int(comprovante_id),
                    ))
                else:
                    execute("""
                    update comprovantes_bancarios
                    set nota_fiscal_id=%s,
                        pasta_google_drive_link=coalesce(nullif(%s, ''), pasta_google_drive_link),
                        observacao=%s,
                        enviado_por=%s
                    where id=%s
                    """, (
                        int(nota_id) if nota_id is not None else None,
                        str(link_pasta or "").strip(),
                        observacao.strip() or None,
                        user["id"],
                        int(comprovante_id),
                    ))
                st.success("Comprovante bancario atualizado.")
                st.rerun()

elif menu == "destino_final":
    itens_destino = query("""
    select
      nfi.id,
      s.id as solicitacao,
      r.codigo as rubrica,
      nfi.descricao,
      nfi.tipo_item,
      nfi.quantidade,
      nfi.valor_total,
      nf.numero_nf,
      nf.fornecedor,
      case
        when p.id is not null then 'patrimonio'
        when e.id is not null then 'estoque'
        when a.id is not null then 'atesto'
        else 'pendente'
      end as destino
    from nota_fiscal_itens nfi
    join notas_fiscais nf on nf.id = nfi.nota_fiscal_id
    join pedido_itens pi on pi.id = nfi.pedido_item_id
    join solicitacoes_compra s on s.id = pi.pedido_id
    join rubricas r on r.id = pi.rubrica_id
    left join patrimonio p on p.nota_fiscal_item_id = nfi.id
    left join estoque_consumo e on e.nota_fiscal_item_id = nfi.id
    left join atesto_servico a on a.nota_fiscal_item_id = nfi.id
    where s.status='finalizado'
    order by nf.lancado_em desc nulls last, nf.numero_nf, nfi.descricao
    """)
    if len(itens_destino) == 0:
        st.info("Ainda não há itens de nota fiscal para classificar.")
    else:
        _, acao_voltar_nf = st.columns([4, 1])
        if acao_voltar_nf.button("Voltar para NF", use_container_width=True):
            voltar_compra_para_nota_fiscal_dialog(itens_destino, user["id"])
        pendentes = itens_destino[itens_destino["destino"] == "pendente"].copy()
        st.metric("Itens pendentes de destino", len(pendentes))
        st.dataframe(
            itens_destino.rename(columns={
                "solicitacao": "Solicitação",
                "rubrica": "Rubrica",
                "descricao": "Item",
                "tipo_item": "Tipo",
                "quantidade": "Quantidade",
                "valor_total": "Valor total",
                "numero_nf": "NF",
                "fornecedor": "Fornecedor",
                "destino": "Destino",
            }),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Valor total": st.column_config.NumberColumn("Valor total", format="R$ %.2f"),
            },
        )

        if len(pendentes) == 0:
            st.success("Todos os itens de nota fiscal já têm destino final.")
        else:
            st.markdown("### Classificação final do item")
            item_id = st.selectbox(
                "Item da nota fiscal",
                pendentes["id"].tolist(),
                format_func=lambda item_id: (
                    f"{pendentes.loc[pendentes.id == item_id, 'numero_nf'].iloc[0]} - "
                    f"{pendentes.loc[pendentes.id == item_id, 'descricao'].iloc[0]} "
                    f"({pendentes.loc[pendentes.id == item_id, 'tipo_item'].iloc[0]})"
                ),
                key="destino_final_item_id",
            )
            item = pendentes.loc[pendentes.id == item_id].iloc[0]
            st.caption(
                f"Tipo: {item['tipo_item']} | Quantidade: {format_brl(item['quantidade'])} | "
                f"Valor: {format_currency_brl_markdown(item['valor_total'])}"
            )

            if item["tipo_item"] == "permanente":
                numero_patrimonio = st.text_input("Número de patrimônio", key=f"pat_numero_{item_id}")
                localizacao = st.text_input("Localização", key=f"pat_local_{item_id}")
                responsavel = st.text_input("Responsável", key=f"pat_resp_{item_id}")
                estado = st.selectbox("Estado", ["ativo", "manutencao", "baixado"], key=f"pat_estado_{item_id}")
                observacoes = st.text_area("Observações", key=f"pat_obs_{item_id}")
                if st.button("Registrar patrimônio", type="primary"):
                    execute("""
                    insert into patrimonio
                      (nota_fiscal_item_id, numero_patrimonio, localizacao, responsavel, estado, observacoes)
                    values (%s,%s,%s,%s,%s,%s)
                    """, (item_id, numero_patrimonio, localizacao, responsavel, estado, observacoes))
                    st.success("Item registrado como patrimônio.")
                    st.rerun()

            elif item["tipo_item"] == "consumo":
                quantidade_entrada = Decimal(str(item["quantidade"]))
                st.number_input("Quantidade de entrada", value=float(quantidade_entrada), disabled=True, key=f"est_qtd_{item_id}")
                unidade = st.text_input("Unidade", value="un", key=f"est_un_{item_id}")
                local_armazenamento = st.text_input("Local de armazenamento", key=f"est_local_{item_id}")
                responsavel = st.text_input("Responsável", key=f"est_resp_{item_id}")
                observacoes = st.text_area("Observações", key=f"est_obs_{item_id}")
                if st.button("Registrar estoque", type="primary"):
                    execute("""
                    insert into estoque_consumo
                      (nota_fiscal_item_id, quantidade_entrada, quantidade_disponivel, unidade, local_armazenamento, responsavel, observacoes)
                    values (%s,%s,%s,%s,%s,%s,%s)
                    """, (item_id, quantidade_entrada, quantidade_entrada, unidade, local_armazenamento, responsavel, observacoes))
                    st.success("Item registrado no estoque de consumo.")
                    st.rerun()

            elif item["tipo_item"] == "servico":
                descricao_execucao = st.text_area("Descrição da execução", key=f"serv_desc_{item_id}")
                responsavel_atesto = st.text_input("Responsável pelo atesto", key=f"serv_resp_{item_id}")
                data_atesto = st.date_input("Data do atesto", value=date.today(), key=f"serv_data_{item_id}")
                documento_url = st.text_input("URL do documento de comprovação", key=f"serv_doc_{item_id}")
                observacoes = st.text_area("Observações", key=f"serv_obs_{item_id}")
                if st.button("Registrar atesto de serviço", type="primary"):
                    if not descricao_execucao.strip():
                        st.error("Informe a descrição da execução do serviço.")
                    else:
                        execute("""
                        insert into atesto_servico
                          (nota_fiscal_item_id, descricao_execucao, responsavel_atesto, data_atesto, documento_comprovacao_url, observacoes)
                        values (%s,%s,%s,%s,%s,%s)
                        """, (
                            item_id,
                            descricao_execucao,
                            responsavel_atesto,
                            data_atesto,
                            documento_url.strip() or None,
                            observacoes,
                        ))
                        st.success("Atesto de serviço registrado.")
                        st.rerun()

elif menu == "auditoria":
    st.caption("Raio X da prestação de contas: pedido, autorização, cotação, nota fiscal, destino final e saldo da rubrica.")
    if st.button("Executar auditoria do projeto", type="primary"):
        st.session_state["auditoria_executada"] = True

    if st.session_state.get("auditoria_executada"):
        sincronizar_orcamento()
        auditoria = query("select * from vw_auditoria_itens_projeto order by rubrica_codigo, solicitacao_id, descricao")
        conferencia_nf = query("select * from vw_conferencia_notas_fiscais order by numero_nf")

        if len(auditoria) == 0:
            st.warning("Nenhum dado encontrado para auditoria.")
        else:
            total = len(auditoria)
            ok = len(auditoria[auditoria["status_auditoria"] == "OK"])
            pendencias = total - ok

            c1, c2, c3 = st.columns(3)
            c1.metric("Itens auditados", total)
            c2.metric("Itens OK", ok)
            c3.metric("Pendências", pendencias)

            with st.expander("1. Rubrica", expanded=True):
                rubrica_resumo = (
                    auditoria
                    .groupby(["rubrica_codigo", "rubrica_nome"], dropna=False)
                    .agg(
                        saldo_inicial=("rubrica_saldo_inicial", "first"),
                        valor_solicitado=("valor_solicitado", "sum"),
                        valor_autorizado=("valor_autorizado", "sum"),
                        valor_empenhado_comprado=("valor_cotado_vencedor", "sum"),
                        valor_reservado=("rubrica_valor_reservado", "first"),
                        valor_utilizado=("rubrica_valor_utilizado", "first"),
                        saldo_restante=("rubrica_saldo_restante", "first"),
                    )
                    .reset_index()
                )
                st.dataframe(
                    preparar_tabela_auditoria(rubrica_resumo),
                    use_container_width=True,
                    hide_index=True,
                )

            with st.expander("2. Solicitações", expanded=True):
                solicitacoes_auditoria = auditoria[[
                    "solicitacao_id",
                    "descricao",
                    "tipo_item",
                    "quantidade",
                    "valor_solicitado",
                    "status_solicitacao",
                    "autorizado",
                ]].copy()
                solicitacoes_auditoria["existe_solicitacao"] = solicitacoes_auditoria["solicitacao_id"].notna()
                solicitacoes_auditoria["tem_valor"] = solicitacoes_auditoria["valor_solicitado"].fillna(0) > 0
                solicitacoes_auditoria["tipo_valido"] = solicitacoes_auditoria["tipo_item"].isin(["permanente", "consumo", "servico"])
                st.dataframe(preparar_tabela_auditoria(solicitacoes_auditoria), use_container_width=True, hide_index=True)

            with st.expander("3. Cotações", expanded=True):
                cotacoes_auditoria = auditoria[[
                    "solicitacao_id",
                    "descricao",
                    "total_cotacoes",
                    "total_vencedoras",
                    "fornecedor_vencedor",
                    "valor_solicitado",
                    "valor_cotado_vencedor",
                    "valor_economia",
                ]].copy()
                cotacoes_auditoria["tem_cotacao"] = cotacoes_auditoria["total_cotacoes"] > 0
                cotacoes_auditoria["tem_vencedor"] = cotacoes_auditoria["total_vencedoras"] == 1
                cotacoes_auditoria["valor_bate"] = (
                    cotacoes_auditoria["valor_cotado_vencedor"].fillna(0)
                    - cotacoes_auditoria["valor_solicitado"].fillna(0)
                ).abs() <= 0.01
                st.dataframe(preparar_tabela_auditoria(cotacoes_auditoria), use_container_width=True, hide_index=True)

            with st.expander("4. Notas fiscais", expanded=True):
                notas_auditoria = auditoria[[
                    "descricao",
                    "notas_fiscais",
                    "fornecedor_vencedor",
                    "fornecedores_nf",
                    "valor_cotado_vencedor",
                    "valor_nf_item",
                    "total_itens_nf",
                    "tem_arquivo_nf",
                ]].copy()
                notas_auditoria["tem_item_nf"] = notas_auditoria["total_itens_nf"] > 0
                notas_auditoria["valor_nf_bate"] = (
                    notas_auditoria["valor_nf_item"].fillna(0)
                    - notas_auditoria["valor_cotado_vencedor"].fillna(0)
                ).abs() <= 0.01
                notas_auditoria["fornecedor_bate"] = notas_auditoria["fornecedores_nf"] == notas_auditoria["fornecedor_vencedor"]
                st.dataframe(preparar_tabela_auditoria(notas_auditoria), use_container_width=True, hide_index=True)
                st.markdown("#### Conferência NF x itens")
                st.dataframe(preparar_tabela_auditoria(conferencia_nf), use_container_width=True, hide_index=True)

            with st.expander("5. Comprovantes bancários", expanded=True):
                comprovantes_auditoria = auditoria[[
                    "compra_id",
                    "solicitacao_id",
                    "descricao",
                    "notas_fiscais",
                    "fornecedor_vencedor",
                    "total_comprovantes_bancarios",
                    "comprovantes_bancarios",
                    "tem_comprovante_bancario",
                    "status_auditoria",
                ]].copy()
                st.dataframe(preparar_tabela_auditoria(comprovantes_auditoria), use_container_width=True, hide_index=True)

            with st.expander("6. Destino final", expanded=True):
                destino_auditoria = auditoria[[
                    "descricao",
                    "tipo_item",
                    "patrimonio_id",
                    "estoque_id",
                    "atesto_id",
                    "status_auditoria",
                ]].copy()
                destino_auditoria["destino_correto"] = (
                    ((destino_auditoria["tipo_item"] == "permanente") & destino_auditoria["patrimonio_id"].notna())
                    | ((destino_auditoria["tipo_item"] == "consumo") & destino_auditoria["estoque_id"].notna())
                    | ((destino_auditoria["tipo_item"] == "servico") & destino_auditoria["atesto_id"].notna())
                )
                st.dataframe(preparar_tabela_auditoria(destino_auditoria), use_container_width=True, hide_index=True)

            with st.expander("7. Inconsistências", expanded=True):
                problemas = auditoria[auditoria["status_auditoria"] != "OK"].copy()
                if len(problemas) == 0:
                    st.success("Auditoria concluída: não foram encontradas inconsistências.")
                else:
                    st.error("Auditoria concluída com pendências.")
                    st.dataframe(
                        preparar_tabela_auditoria(problemas[[
                            "pedido_item_id",
                            "rubrica_codigo",
                            "solicitacao_id",
                            "descricao",
                            "tipo_item",
                            "valor_solicitado",
                            "valor_cotado_vencedor",
                            "valor_nf_item",
                            "valor_economia",
                            "status_auditoria",
                        ]]),
                        use_container_width=True,
                        hide_index=True,
                    )
                    problemas_retorno = problemas[
                        problemas["status_auditoria"].str.contains(
                            "valor cotado maior|valor da NF maior|fornecedor da NF diverge|mais de um vencedor",
                            case=False,
                            na=False,
                        )
                    ].copy()
                    if not problemas_retorno.empty:
                        st.markdown("#### Corrigir item")
                        item_corrigir_id = st.selectbox(
                            "Item que deve voltar para cotação",
                            problemas_retorno["pedido_item_id"].tolist(),
                            format_func=lambda item_id: (
                                f"Solicitação {problemas_retorno.loc[problemas_retorno.pedido_item_id == item_id, 'solicitacao_id'].iloc[0]} - "
                                f"{problemas_retorno.loc[problemas_retorno.pedido_item_id == item_id, 'descricao'].iloc[0]} - "
                                f"{normalizar_texto_portugues(problemas_retorno.loc[problemas_retorno.pedido_item_id == item_id, 'status_auditoria'].iloc[0])}"
                            ),
                            key="auditoria_item_corrigir",
                        )
                        confirmar_retorno = st.checkbox(
                            "Confirmo voltar este item para cotação e desfazer NF/destino final associados.",
                            key="confirmar_voltar_item_cotacao",
                        )
                        if st.button("Voltar item para cotação", type="primary"):
                            if not confirmar_retorno:
                                st.error("Marque a confirmação antes de voltar o item para cotação.")
                            else:
                                voltar_item_para_cotacao(item_corrigir_id, user["id"])
                                st.success("Item voltou para cotação. Revise a cotação vencedora e lance a NF novamente.")
                                st.rerun()
                    problemas_ajuste_valor = problemas[
                        (problemas["valor_nf_item"].fillna(0) > 0)
                        & (
                            (
                                problemas["valor_nf_item"].fillna(0)
                                - problemas["valor_cotado_vencedor"].fillna(0)
                            ).abs() <= 0.01
                        )
                        & (
                            (
                                problemas["valor_nf_item"].fillna(0)
                                - problemas["valor_solicitado"].fillna(0)
                            ).abs() > 0.01
                        )
                    ].copy()
                    if not problemas_ajuste_valor.empty:
                        st.markdown("#### Ajustar valor solicitado")
                        item_ajustar_id = st.selectbox(
                            "Item em que a NF está correta",
                            problemas_ajuste_valor["pedido_item_id"].tolist(),
                            format_func=lambda item_id: (
                                f"Solicitação {problemas_ajuste_valor.loc[problemas_ajuste_valor.pedido_item_id == item_id, 'solicitacao_id'].iloc[0]} - "
                                f"{problemas_ajuste_valor.loc[problemas_ajuste_valor.pedido_item_id == item_id, 'descricao'].iloc[0]} - "
                                f"Solicitado {format_currency_brl(problemas_ajuste_valor.loc[problemas_ajuste_valor.pedido_item_id == item_id, 'valor_solicitado'].iloc[0])} / "
                                f"NF {format_currency_brl(problemas_ajuste_valor.loc[problemas_ajuste_valor.pedido_item_id == item_id, 'valor_nf_item'].iloc[0])}"
                            ),
                            key="auditoria_item_ajustar_valor",
                        )
                        confirmar_ajuste_valor = st.checkbox(
                            "Confirmo que o valor da NF está correto e deve substituir o valor solicitado.",
                            key="confirmar_ajustar_valor_solicitado",
                        )
                        if st.button("Ajustar valor solicitado para valor da NF", type="secondary"):
                            if not confirmar_ajuste_valor:
                                st.error("Marque a confirmação antes de ajustar o valor solicitado.")
                            else:
                                ajustar_valor_solicitado_para_nf(item_ajustar_id, user["id"])
                                st.success("Valor solicitado ajustado para o valor da NF. A auditoria foi recalculada.")
                                st.rerun()

            st.markdown("### Dados completos da auditoria")
            st.dataframe(preparar_tabela_auditoria(auditoria), use_container_width=True, hide_index=True)

elif menu == "ia_operacional":
    st.caption("Painel de IA Operacional: alertas automaticos, score de risco por rubrica e gargalos do processo.")

    if st.button("Executar análise IA", type="primary"):
        resultado = gerar_alertas_ia()
        st.success(
            f"Análise concluída: {resultado['criados']} alerta(s) criado(s), "
            f"{resultado['atualizados']} atualizado(s)."
        )
        st.rerun()

    alertas = carregar_alertas("pendente")
    total_alertas = len(alertas)
    alertas_criticos = len(alertas[alertas["gravidade"] == "alta"]) if total_alertas else 0
    pontos_atencao = len(alertas[alertas["gravidade"].isin(["media", "baixa"])]) if total_alertas else 0
    situacao_normal = 1 if total_alertas == 0 else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Alertas críticos", alertas_criticos)
    c2.metric("Pontos de atenção", pontos_atencao)
    c3.metric("Situação normal", "Sim" if situacao_normal else "Não")

    with st.expander("Alertas críticos", expanded=True):
        criticos = alertas[alertas["gravidade"] == "alta"] if total_alertas else pd.DataFrame()
        if len(criticos) == 0:
            st.success("Nenhum alerta crítico pendente.")
        else:
            st.dataframe(
                preparar_tabela_ia(criticos[["id", "tipo", "titulo", "descricao", "sugestao_acao", "criado_em"]]),
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Pontos de atenção", expanded=True):
        atencao = alertas[alertas["gravidade"].isin(["media", "baixa"])] if total_alertas else pd.DataFrame()
        if len(atencao) == 0:
            st.success("Nenhum ponto de atenção pendente.")
        else:
            st.dataframe(
                preparar_tabela_ia(atencao[["id", "tipo", "titulo", "descricao", "sugestao_acao", "criado_em"]]),
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Score de risco por rubrica", expanded=True):
        score = carregar_score_risco_rubrica()
        if len(score) == 0:
            st.info("Nenhuma rubrica encontrada para cálculo de risco.")
        else:
            score_tabela = score.rename(columns={
                "codigo": "Rubrica",
                "nome": "Nome",
                "valor_orcado": "Valor orçado",
                "valor_reservado": "Valor reservado",
                "valor_utilizado": "Valor utilizado",
                "valor_comprometido": "Valor comprometido",
                "valor_solicitado": "Valor solicitado",
                "percentual_comprometido": "Percentual comprometido",
            }).copy()
            for coluna in [
                "Valor orçado",
                "Valor reservado",
                "Valor utilizado",
                "Valor comprometido",
                "Valor solicitado",
            ]:
                if coluna in score_tabela.columns:
                    score_tabela[coluna] = score_tabela[coluna].apply(format_currency_brl)
            if "Percentual comprometido" in score_tabela.columns:
                score_tabela["Percentual comprometido"] = score_tabela["Percentual comprometido"].apply(format_percent_brl)
            st.dataframe(score_tabela, use_container_width=True, hide_index=True)

    with st.expander("Gargalos de estoque/patrimônio", expanded=True):
        gargalos_destino = alertas[alertas["tipo"].isin(["item_sem_patrimonio", "item_sem_estoque"])] if total_alertas else pd.DataFrame()
        if len(gargalos_destino) == 0:
            st.success("Nenhum gargalo de estoque ou patrimônio pendente.")
        else:
            st.dataframe(
                preparar_tabela_ia(gargalos_destino[["id", "titulo", "descricao", "sugestao_acao"]]),
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Gargalos financeiros", expanded=True):
        gargalos_financeiros = alertas[alertas["tipo"].isin(["rubrica_critica", "saldo_insuficiente", "valor_divergente", "risco_orcamentario"])] if total_alertas else pd.DataFrame()
        if len(gargalos_financeiros) == 0:
            st.success("Nenhum gargalo financeiro pendente.")
        else:
            st.dataframe(
                preparar_tabela_ia(gargalos_financeiros[["id", "tipo", "titulo", "descricao", "sugestao_acao"]]),
                use_container_width=True,
                hide_index=True,
            )

    if total_alertas:
        st.markdown("### Marcar alerta como resolvido")
        alerta_id = st.selectbox(
            "Alerta pendente",
            alertas["id"].tolist(),
            format_func=lambda item_id: (
                f"#{item_id} - {alertas.loc[alertas.id == item_id, 'titulo'].iloc[0]}"
            ),
            key="ia_alerta_resolver",
        )
        if st.button("Marcar como resolvido"):
            marcar_alerta_resolvido(alerta_id)
            st.success("Alerta marcado como resolvido.")
            st.rerun()

    with st.expander("Todos os alertas registrados"):
        todos_alertas = carregar_alertas("todos")
        if len(todos_alertas) == 0:
            st.info("Nenhum alerta registrado.")
        else:
            st.dataframe(preparar_tabela_ia(todos_alertas), use_container_width=True, hide_index=True)

elif menu == "itens_comprados":
    df = query("""
    select
      s.id as "Solicitação",
      r.codigo as "Rubrica",
      r.nome as "Nome da rubrica",
      nfi.descricao as "Produto/serviço",
      nfi.quantidade as "Quantidade",
      nfi.valor_total as "Valor da compra",
      nf.fornecedor as "Fornecedor da cotação",
      nf.numero_nf as "Número da NF",
      nf.fornecedor as "Fornecedor da NF",
      nfi.valor_total as "Valor da NF",
      nf.data_emissao as "Data de emissão",
      nf.lancado_em as "Lançado em"
    from nota_fiscal_itens nfi
    join notas_fiscais nf on nf.id = nfi.nota_fiscal_id
    join pedido_itens pi on pi.id = nfi.pedido_item_id
    join solicitacoes_compra s on s.id = pi.pedido_id
    join rubricas r on r.id = pi.rubrica_id
    where s.status = 'finalizado'
    order by nf.lancado_em desc nulls last, nf.numero_nf, nfi.descricao
    """)
    if len(df) == 0:
        st.info("Ainda não há itens comprados finalizados.")
    else:
        st.download_button(
            "Baixar planilha por rubrica",
            data=construir_planilha_itens_comprados(df),
            file_name=f"produtos_comprados_por_rubrica_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "Valor da compra": st.column_config.NumberColumn("Valor da compra", format="R$ %.2f"),
                "Valor da NF": st.column_config.NumberColumn("Valor da NF", format="R$ %.2f"),
            },
        )

elif menu == "membros":
    if user["papel"] != "admin":
        st.error("Acesso restrito ao administrador.")
        st.stop()

    paginas_permitidas = BASE_MENU_OPTIONS
    st.markdown("### Adicionar membro")
    nome = st.text_input("Nome", key="membro_nome")
    email = st.text_input("E-mail", key="membro_email")
    senha = st.text_input("Senha temporária", type="password", key="membro_senha")
    papel = st.selectbox("Papel", ["solicitante", "gerente", "compras", "admin"], key="membro_papel")
    permissoes = st.multiselect(
        "Páginas permitidas",
        [key for key, _ in paginas_permitidas],
        default=["nova_exigencia"],
        format_func=lambda key: dict(paginas_permitidas)[key],
        key="membro_permissoes",
    )

    if papel == "admin":
        permissoes = [key for key, _ in ADMIN_MENU_OPTIONS]

    if st.button("Adicionar membro"):
        if not nome or not email or not senha:
            st.error("Preencha nome, e-mail e senha.")
        else:
            execute("""
            insert into usuarios_app (nome,email,senha_hash,papel,permissoes,ativo)
            values (%s,%s,%s,%s,%s,true)
            on conflict (email) do update set
              nome=excluded.nome,
              senha_hash=excluded.senha_hash,
              papel=excluded.papel,
              permissoes=excluded.permissoes,
              ativo=true
            """, (nome, email, hash_password(senha), papel, permissoes))
            st.success("Membro adicionado ou atualizado.")

    st.markdown("### Editar membro")
    membros_edicao = query("""
    select nome, email, papel, permissoes, ativo
    from usuarios_app
    where ativo=true
    order by nome
    """)
    if len(membros_edicao) == 0:
        st.info("Nao ha membros ativos para editar.")
    else:
        email_editar = st.selectbox(
            "Membro",
            membros_edicao["email"].tolist(),
            format_func=lambda email: f"{membros_edicao.loc[membros_edicao.email == email, 'nome'].iloc[0]} ({email})",
            key="membro_editar_email",
        )
        membro_editar = membros_edicao.loc[membros_edicao.email == email_editar].iloc[0]
        permissoes_atuais = membro_editar["permissoes"] if isinstance(membro_editar["permissoes"], list) else []
        opcoes_papel = ["solicitante", "gerente", "compras", "admin"]
        papel_atual = membro_editar["papel"] if membro_editar["papel"] in opcoes_papel else "solicitante"

        chave_membro_edicao = email_editar.replace("@", "_").replace(".", "_")
        nome_editado = st.text_input("Nome", value=membro_editar["nome"], key=f"membro_editar_nome_{chave_membro_edicao}")
        papel_editado = st.selectbox(
            "Papel",
            opcoes_papel,
            index=opcoes_papel.index(papel_atual),
            key=f"membro_editar_papel_{chave_membro_edicao}",
        )
        opcoes_permissoes = [key for key, _ in paginas_permitidas]
        permissoes_validas = [permissao for permissao in permissoes_atuais if permissao in opcoes_permissoes]
        permissoes_editadas = st.multiselect(
            "Paginas permitidas",
            opcoes_permissoes,
            default=permissoes_validas,
            format_func=lambda key: dict(paginas_permitidas)[key],
            key=f"membro_editar_permissoes_{chave_membro_edicao}",
            disabled=papel_editado == "admin",
        )
        if papel_editado == "admin":
            permissoes_editadas = [key for key, _ in ADMIN_MENU_OPTIONS]
            st.caption("Administradores acessam todos os modulos.")

        if st.button("Salvar alteracoes do membro"):
            if not nome_editado.strip():
                st.error("Informe o nome do membro.")
            else:
                execute(
                    "update usuarios_app set nome=%s, papel=%s, permissoes=%s where email=%s",
                    (nome_editado.strip(), papel_editado, permissoes_editadas, email_editar),
                )
                if email_editar == user["email"]:
                    st.session_state.user["nome"] = nome_editado.strip()
                    st.session_state.user["papel"] = papel_editado
                    st.session_state.user["permissoes"] = permissoes_editadas
                st.success("Membro atualizado.")
                st.rerun()

    st.markdown("### Remover membro")
    membros_remocao = query("""
    select email, nome
    from usuarios_app
    where ativo=true
    order by nome
    """)
    if len(membros_remocao) == 0:
        st.info("Não há membros ativos para remover.")
    else:
        email_remover = st.selectbox(
            "Membro",
            membros_remocao["email"].tolist(),
            format_func=lambda email: f"{membros_remocao.loc[membros_remocao.email == email, 'nome'].iloc[0]} ({email})",
            key="membro_remover_email",
        )
        confirmar_remocao = st.checkbox("Confirmar remoção do membro selecionado", key="confirmar_remocao_membro")
        if st.button("Remover membro"):
            if email_remover == user["email"]:
                st.error("Você não pode remover o próprio usuário logado.")
            elif not confirmar_remocao:
                st.error("Marque a confirmação antes de remover.")
            else:
                execute("update usuarios_app set ativo=false where email=%s", (email_remover,))
                st.success("Membro removido do acesso.")
                st.rerun()

    st.markdown("### Membros cadastrados")
    membros = query("""
    select
      split_part(trim(nome), ' ', 1) as usuario,
      nome,
      email,
      papel,
      permissoes,
      ativo,
      criado_em
    from usuarios_app
    order by criado_em desc
    """)
    st.dataframe(membros, use_container_width=True)
