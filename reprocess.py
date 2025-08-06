# %% INICIALIZAÇÃO
from dotenv import load_dotenv
import os
from src.bling_api import BlingAPI
from src.utils import (
    reprocessa_produtos_por_ids,
    reprocessar_todas_falhas,
    reprocessa_saldo_produtos_por_ids
)
from src.log import (
    log_etl
)

from src.config import (
    REPROCESSAR_FULL,
    REPROCESSAR_PRODUTO_MANUAL,
    REPROCESSAR_EMPRESA_MANUAL,
    REPROCESSAR_SALDO_MANUAL
)
   

load_dotenv()
api_key = os.getenv("BLING_API_KEY")
db_uri = os.getenv("POSTGRES_URI")
api = BlingAPI(api_key)


# %% ============= REPROCESSAR FULL =============

if __name__ == "__main__":
    if REPROCESSAR_FULL:
        reprocessar_todas_falhas(api, db_uri)


# %% ============= REPROCESSAR PRODUTO MANUAL =============

if __name__ == "__main__":
    if REPROCESSAR_PRODUTO_MANUAL:
        lista_ids = [
            "8500267409",
            
            
        ]
        reprocessa_produtos_por_ids(lista_ids, api, db_uri)
# %% ============= REPROCESSAR SALDO MANUAL =============
if __name__ == "__main__":
    if REPROCESSAR_SALDO_MANUAL:
        lista_ids = [
            "8500270867",
            "8500272210",
            "8500272210"            
        ]
        reprocessa_saldo_produtos_por_ids(lista_ids, api, db_uri)


# %%
