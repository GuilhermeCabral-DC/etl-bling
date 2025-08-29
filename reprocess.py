# %% INICIALIZAÇÃO
from dotenv import load_dotenv
import os
from src.bling_api import BlingAPI
from src.utils import (
    reprocessa_produtos_por_ids,
    reprocessar_todas_falhas,
    reprocessa_saldo_produtos_por_ids,
    reprocessa_pedidos_vendas_por_ids
)
from src.log import (
    log_etl
)

from src.config import (
    REPROCESSAR_FULL,
    REPROCESSAR_PRODUTO_MANUAL,
    REPROCESSAR_EMPRESA_MANUAL,
    REPROCESSAR_SALDO_MANUAL,
    REPROCESSAR_PEDIDOS_VENDAS_MANUAL
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
            "16484886977",
            "16484886969",
            "16484886940",
            "16484886890",
            "16484886888",
            "15943425988",
            "15943425975"
            
            
        ]
        reprocessa_produtos_por_ids(lista_ids, api, db_uri)
# %% ============= REPROCESSAR SALDO MANUAL =============
if __name__ == "__main__":
    if REPROCESSAR_SALDO_MANUAL:
        lista_ids = [
            "16484887040",
            "16484887009",
            "16484886981",
            "16448526146",
            "16434397995",
            "16400360776",
            "16400350017",
            "16400350014",
            "16265663142",
            "16223257302",
            "15808109934",
            "15016821540",
            "13280743555",
            "13132549775",
            "12544917556",
            "12441294757",
            "9611668602",
            "9487460075",
            "9451536520",
            "8839087255",
            "8839079747",
            "8756592846"                       
        ]
        reprocessa_saldo_produtos_por_ids(lista_ids, api, db_uri)


# %% ============= REPROCESSAR PEDIDOS DE VENDA MANUAL =============
if __name__ == "__main__":
    if REPROCESSAR_PEDIDOS_VENDAS_MANUAL:
        lista_ids = [
            "23639359387",
    
        ]
        reprocessa_pedidos_vendas_por_ids(lista_ids, api, db_uri)
# %%
