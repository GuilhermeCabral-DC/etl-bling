# %% === obter_token.py ===
import os
import requests
import json
import psycopg2
import time
import base64
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL")
POSTGRES_URI = os.getenv("POSTGRES_URI")

# Tabela onde os tokens serão salvos
TOKEN_TABLE = "conf.token"

# Conexão com PostgreSQL
conn = psycopg2.connect(POSTGRES_URI)

print("\n🔐 Geração de Token Inicial via OAuth")
auth_code = input("Cole o 'code' retornado pelo Bling após autenticação: ").strip()

print("\n🔄 Requisitando tokens...")

# Dados para o body da requisição
data = {
    "grant_type": "authorization_code",
    "code": auth_code,
    "redirect_uri": REDIRECT_URI
}

# Monta o header com client_id e client_secret em base64
client_credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
b64_credentials = base64.b64encode(client_credentials.encode()).decode()

headers = {
    "Authorization": f"Basic {b64_credentials}",
    "Content-Type": "application/x-www-form-urlencoded"
}

# Faz a requisição
response = requests.post(TOKEN_URL, headers=headers, data=data)

# Exibe erro detalhado, se houver
if response.status_code != 200:
    print("⚠️ Erro na resposta:", response.status_code)
    print("🧾 Conteúdo da resposta:", response.text)
    response.raise_for_status()

# Lê os tokens
tokens = response.json()
access_token = tokens["access_token"]
refresh_token = tokens["refresh_token"]
expires_in = tokens["expires_in"]  # em segundos
expires_at = int(time.time()) + expires_in

# Limpa e insere no banco
with conn.cursor() as cur:
    cur.execute(f"DELETE FROM {TOKEN_TABLE}")
    cur.execute(
        f"INSERT INTO {TOKEN_TABLE} (access_token, refresh_token, expires_at) VALUES (%s, %s, %s)",
        (access_token, refresh_token, expires_at)
    )
    conn.commit()

print("\n✅ Token salvo com sucesso no banco de dados!")

# %%
