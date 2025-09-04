# %% INICIALIZAÇÃO
import requests
import os
import time
import base64
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL") or "https://www.bling.com.br/Api/v3/oauth/token"

CODE_RECEBIDO = "b545a3f3034377470921041bab0efa8bc0b4d90f"  # atualize com o code gerado

data = {
    "grant_type": "authorization_code",
    "code": CODE_RECEBIDO,
    "redirect_uri": REDIRECT_URI
}

# Header com Auth + Content-Type correto
client_creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
client_creds_b64 = base64.b64encode(client_creds.encode()).decode()
headers = {
    "Authorization": f"Basic {client_creds_b64}",
    "Content-Type": "application/x-www-form-urlencoded"
}

resp = requests.post(TOKEN_URL, data=data, headers=headers)
print("Status:", resp.status_code)
print("Resposta:", resp.text)

if resp.ok:
    tokens = resp.json()
    expires_at = int(time.time()) + tokens["expires_in"]
    tokens_json = {
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
        "expires_at": expires_at
    }

    # 🔒 Garante que salva no mesmo local que o auth.py espera
    token_path = Path(__file__).resolve().parent.parent / "tokens.json"
    with open(token_path, "w") as f:
        json.dump(tokens_json, f, indent=2)

    print(f"✅ Arquivo tokens.json salvo em: {token_path}")
else:
    print("❌ Falha ao obter tokens. Verifique se o CODE ainda está válido.")

# %%
