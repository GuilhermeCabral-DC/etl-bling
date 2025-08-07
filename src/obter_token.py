import requests
import os
import time
import base64
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = os.getenv("TOKEN_URL")



CODE_RECEBIDO = "0fa5f2d962fe20a4beb50e25bb0917b855356ecb"  # Cole aqui o code obtido

data = {
    "grant_type": "authorization_code",
    "code": CODE_RECEBIDO,
    "redirect_uri": REDIRECT_URI
}

# Adiciona Auth no header (OAuth2 padrão)
client_creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
client_creds_b64 = base64.b64encode(client_creds.encode()).decode()
headers = {
    "Authorization": f"Basic {client_creds_b64}"
}

resp = requests.post(TOKEN_URL, data=data, headers=headers)
print("Status:", resp.status_code)
print("Resposta:", resp.text)

if resp.ok:
    tokens = resp.json()
    print("\nSalvando tokens.json (lembre de calcular expires_at!)")
    expires_at = int(time.time()) + tokens["expires_in"]
    tokens_json = {
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
        "expires_at": expires_at
    }
    import json
    with open("tokens.json", "w") as f:
        json.dump(tokens_json, f, indent=2)
    print("Arquivo tokens.json criado!")
