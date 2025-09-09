# === auth.py ===
import os
import json
import time
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests

load_dotenv()

# === CONFIGURAÇÕES ===
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL")

# === CONEXÃO COM BANCO (PostgreSQL) ===
PG_CONN = psycopg2.connect(os.getenv("POSTGRES_URI"))

# === MARGEM DE SEGURANÇA EM SEGUNDOS (evita token vencer durante uso) ===
EXPIRATION_MARGIN = 120

# === TABELA DE TOKENS (schema conf) ===
TOKEN_TABLE = "conf.token"

# === FUNÇÃO: busca token no banco ===
def load_tokens_db():
    with PG_CONN.cursor() as cur:
        cur.execute(f"SELECT access_token, refresh_token, expires_at FROM {TOKEN_TABLE} LIMIT 1")
        row = cur.fetchone()
        if row:
            return {
                "access_token": row[0],
                "refresh_token": row[1],
                "expires_at": row[2]
            }
    return None

# === FUNÇÃO: salva token no banco ===
def save_tokens_db(tokens):
    with PG_CONN.cursor() as cur:
        cur.execute(f"DELETE FROM {TOKEN_TABLE}")
        cur.execute(
            f"INSERT INTO {TOKEN_TABLE} (access_token, refresh_token, expires_at) VALUES (%s, %s, %s)",
            (tokens["access_token"], tokens["refresh_token"], tokens["expires_at"])
        )
        PG_CONN.commit()

# === FUNÇÃO: renova token com refresh_token ===
import base64

def refresh_access_token():
    tokens = load_tokens_db()
    if not tokens:
        raise Exception("Nenhum token encontrado para renovação.")

    # Autenticando com base64
    client_credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_credentials = base64.b64encode(client_credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {b64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"]
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)

    if response.status_code == 429:
        raise Exception("Erro 429: Too Many Requests ao renovar token.")

    response.raise_for_status()
    new_data = response.json()

    expires_at = int(time.time()) + new_data["expires_in"]

    updated = {
        "access_token": new_data["access_token"],
        "refresh_token": new_data.get("refresh_token", tokens["refresh_token"]),
        "expires_at": expires_at
    }

    save_tokens_db(updated)
    return updated["access_token"]


# === FUNÇÃO: retorna token válido do banco ou renova ===
def get_valid_access_token():
    tokens = load_tokens_db()
    now = int(time.time())

    if not tokens:
        raise Exception("Token inexistente. Cadastre via OAuth inicial.")

    if tokens["expires_at"] - now < EXPIRATION_MARGIN:
        return refresh_access_token()

    return tokens["access_token"]
