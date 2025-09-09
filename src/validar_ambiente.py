# %% VALIDAR

import os
import time
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import requests

load_dotenv()

# === Variáveis de ambiente
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL") or "https://www.bling.com.br/Api/v3/oauth/token"
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1")
POSTGRES_URI = os.getenv("POSTGRES_URI")

# === Conecta ao banco e busca token
conn = psycopg2.connect(POSTGRES_URI)
cur = conn.cursor()
cur.execute("SELECT access_token, refresh_token, expires_at FROM conf.token LIMIT 1")
row = cur.fetchone()

if not row:
    print("❌ Nenhum token encontrado na tabela conf.token")
    exit(1)

access_token, refresh_token, expires_at = row
agora = int(time.time())

# === Valida token
print("\n🔎 Validação do Ambiente Bling API\n" + "-"*40)
print(f"CLIENT_ID: {'✅ OK' if CLIENT_ID else '❌ Faltando'}")
print(f"CLIENT_SECRET: {'✅ OK' if CLIENT_SECRET else '❌ Faltando'}")
print(f"REDIRECT_URI: {REDIRECT_URI or '❌ Faltando'}")
print(f"TOKEN_URL: {TOKEN_URL}")
print("Token carregado do banco de dados: ✅")
print(f"Access Token: {access_token[:10]}... ✅")
print(f"Refresh Token: {refresh_token[:10]}... ✅")

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
print("\n🚀 Teste de chamada GET /empresas:")
try:
    resp = requests.get("https://api.bling.com.br/Api/v3/empresas/me/dados-basicos", headers=headers, timeout=20)
    print(f"Status: {resp.status_code}")
    print("Resposta:", resp.json() if resp.status_code == 200 else resp.text)
except Exception as e:
    print(f"❌ Erro ao testar API: {e}")
finally:
    cur.close()
    conn.close()

# %%
