# region ==== IMPORTS ====

import requests
import time
import json
import os
import base64

# endregion

# region ==== VARIÁVEIS DE AMBIENTE ====
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL")
TOKENS_PATH = os.getenv("TOKENS_PATH", "tokens.json")
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1")  # Pode controlar via .env ou config.py
# endregion

# region ==== FUNÇÃO: Carregar tokens ====
def load_tokens():
    with open(TOKENS_PATH, "r") as f:
        return json.load(f)
# endregion

# region ==== FUNÇÃO: Salvar tokens ====
def save_tokens(tokens):
    with open(TOKENS_PATH, "w") as f:
        json.dump(tokens, f)
# endregion

# region ==== FUNÇÃO: Renovar access_token ====
def refresh_access_token():
    tokens = load_tokens()
    refresh_token = tokens["refresh_token"]

    if DEBUG:
        print(f"DEBUG [refresh_access_token]: Tentando renovar com refresh_token={refresh_token[:6]}...")

    client_creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    client_creds_b64 = base64.b64encode(client_creds.encode()).decode()
    headers = {
        "Authorization": f"Basic {client_creds_b64}"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "redirect_uri": REDIRECT_URI
    }
    
    if DEBUG:
        print(f"DEBUG [refresh_access_token]: TOKEN_URL={TOKEN_URL}")
    resp = requests.post(TOKEN_URL, data=data, headers=headers)
    if DEBUG:
        print(f"DEBUG [refresh_access_token]: Status={resp.status_code} Body={resp.text}")

    resp.raise_for_status()
    new_tokens = resp.json()
    expires_in = new_tokens["expires_in"]
    expires_at = int(time.time()) + expires_in
    tokens.update({
        "access_token": new_tokens["access_token"],
        "refresh_token": new_tokens.get("refresh_token", refresh_token),
        "expires_at": expires_at
    })
    save_tokens(tokens)
    if DEBUG:
        print("DEBUG [refresh_access_token]: Novo access_token salvo. expires_at=", expires_at)
    return tokens["access_token"]
# endregion

# region ==== FUNÇÃO: Obter token válido (renova se necessário) ====
def get_valid_access_token():
    tokens = load_tokens()
    agora = int(time.time())
    if DEBUG:
        print(f"DEBUG [get_valid_access_token]: expires_at={tokens['expires_at']} agora={agora}")

    if agora > tokens["expires_at"] - 120:  # margem de 2 min
        if DEBUG:
            print("DEBUG [get_valid_access_token]: Token expirado ou próximo de expirar. Vai tentar renovar...")
        return refresh_access_token()
    if DEBUG:
        print("DEBUG [get_valid_access_token]: Token válido, retornando.")
    return tokens["access_token"]
# endregion