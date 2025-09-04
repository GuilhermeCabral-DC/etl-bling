# %% VALIDAR

import os
import json
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import base64
import requests

load_dotenv()

# === Variáveis de ambiente
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL") or "https://www.bling.com.br/Api/v3/oauth/token"
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1")

# === Caminho do tokens.json na raiz
token_path = Path(__file__).resolve().parent.parent / "tokens.json"

# === Carrega tokens
try:
    with open(token_path, "r") as f:
        tokens = json.load(f)
except FileNotFoundError:
    print(f"❌ Arquivo tokens.json não encontrado em {token_path}")
    exit(1)

access_token = tokens.get("access_token")
refresh_token = tokens.get("refresh_token")
expires_at = tokens.get("expires_at")
agora = int(time.time())

# === Valida token
print("\n🔎 Validação do Ambiente Bling API\n" + "-"*40)
print(f"CLIENT_ID: {'✅ OK' if CLIENT_ID else '❌ Faltando'}")
print(f"CLIENT_SECRET: {'✅ OK' if CLIENT_SECRET else '❌ Faltando'}")
print(f"REDIRECT_URI: {REDIRECT_URI or '❌ Faltando'}")
print(f"TOKEN_URL: {TOKEN_URL}")
print(f"Arquivo tokens.json: ✅ Encontrado em {token_path}")
print(f"Access Token: {access_token[:10]}... ✅")
print(f"Refresh Token: {refresh_token[:10]}... ✅")

# ✅ Corrige milissegundos se necessário
if expires_at > 1_000_000_000_000:
    expires_at = int(expires_at / 1000)

expira_em = datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M:%S")
print(f"Expira em: {expira_em}")
print(f"Tempo restante: {int((expires_at - agora) / 60)} minutos")

if expires_at <= agora:
    print("❌ Token expirado!")
elif expires_at - agora < 120:
    print("⚠️ Token vai expirar em menos de 2 minutos!")
else:
    print("✅ Token válido")

# === Monta headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

print("\n🧾 Headers de Autorização:")
for k, v in headers.items():
    print(f"  {k}: {v if k != 'Authorization' else v[:30] + '...'}")

# === Teste de chamada à API
print("\n🚀 Teste de chamada GET /usuarios/me:")
try:
    resp = requests.get("https://api.bling.com.br/Api/v3/usuarios/me", headers=headers, timeout=20)
    print(f"Status: {resp.status_code}")
    print("Resposta:", resp.json() if resp.status_code == 200 else resp.text)
except Exception as e:
    print(f"❌ Erro ao testar API: {e}")

# %%
