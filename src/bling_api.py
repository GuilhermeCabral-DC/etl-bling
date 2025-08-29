# region ============= INICIALIZAÇÃO =============
import requests
import time
from src.log import (
    log_etl,
)
from src.config import (
    DEBUG,
)
from src.auth import get_valid_access_token   # <---- NOVO: importa função de token dinâmico

BLING_API_URL = "https://api.bling.com.br/Api/v3"  # URL base da API do Bling


class BlingAPI:

    def __init__(self, api_key=None):
        # self.api_key = api_key
        # self.headers = {
        #     "Accept": "application/json",
        #     "Content-Type": "application/json",
        #     "Authorization": f"Bearer {self.api_key}"
        # }
        self.last_request_time = 0
# endregion

# region ============= GERA HEADER DINAMICAMENTE (NOVO) =============
    def get_headers(self):
        """
        Monta os headers sempre com o token válido.
        """
        token = get_valid_access_token()
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
    # endregion

# region ============= EMPRESA: GET =============

    def get_empresa(self):
        """
        Recupera os dados da empresa autenticada na API do Bling.
        """
        endpoint = "empresas/me/dados-basicos"
        try:
            resp = self.get(endpoint)
            if isinstance(resp, dict) and "data" in resp:
                return resp["data"]
            return resp
        except Exception as e:
            raise Exception(f"Erro ao buscar empresa: {e}")
# endregion

# region ============= REQUISIÇÃO GENÉRICA (GET) =============

    def get(self, endpoint, params=None):
        """
        Executa um GET na API do Bling com controle de rate limit, timeout e tratamento de erros.
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < 0.35:
            time.sleep(0.35 - elapsed)  # Aguarda para não exceder 3 req/s
        url = f"{BLING_API_URL}/{endpoint}"

        try:
            # response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=60)  # <--- NOVO
            self.last_request_time = time.time()
            if response.status_code == 429:
                time.sleep(2)
                return self.get(endpoint, params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise
        except Exception as ex:
            raise
# endregion

# region ============= PAGINAÇÃO PADRÃO (GENÉRICO) =============

    def get_all_paginated(self, endpoint, params=None, limit=100, data_path=None):
        """
        Busca todos os registros de um endpoint paginado.
        data_path: lista com chaves para navegar no JSON (ex: ['data'])
        """
        if params is None:
            params = {}
        page = 1
        resultados = []
        while True:
            page_params = params.copy() if params else {}
            page_params["limit"] = limit
            page_params["page"] = page
            data = self.get(endpoint, params=page_params)
            items = data
            if data_path:
                for key in data_path:
                    items = items.get(key, [])
            else:
                items = [data["data"]] if "data" in data else []
            if not items:
                break
            resultados.extend(items)
            if len(items) < limit:
                break
            page += 1
        return resultados
# endregion  

# region ============= VENDEDORES: BUSCA IDS (PAGINADO) =============

    def get_vendedores_ids_pagina(self, pagina: int, limit: int = 100, params: dict | None = None):
        """
        Busca os IDs dos vendedores de UMA página da API do Bling.
        """
        endpoint = "vendedores"
        page_params = (params or {}).copy()
        page_params["limite"] = limit
        page_params["pagina"] = pagina

        try:
            resp = self.get(endpoint, params=page_params)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                return []
            raise

        data = (
            resp["data"]
            if isinstance(resp, dict) and "data" in resp and isinstance(resp["data"], list)
            else resp if isinstance(resp, list)
            else []
        )
        return [item["id"] for item in data if isinstance(item, dict) and "id" in item]

# endregion

# region ============= VENDEDOR: DETALHE POR ID =============

    def get_vendedor_por_id(self, id_vendedor):
        endpoint = f"vendedores/{id_vendedor}"
        try:
            response = self.get(endpoint)
            return response
        except Exception as e:
            log_etl("VENDEDORES", "ERRO", f"Erro ao buscar detalhe do vendedor {id_vendedor}", erro=str(e))
            return None
# endregion

# region ============= PRODUTO: BUSCA IDS (PAGINADO) =============

    def get_produtos_ids_pagina(self, pagina: int, limit: int = 100, params: dict | None = None):
        """
        Busca os IDs dos produtos de UMA página da API do Bling.
        """
        endpoint = "produtos"
        page_params = (params or {}).copy()
        page_params["limite"] = limit
        page_params["pagina"] = pagina

        try:
            resp = self.get(endpoint, params=page_params)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                return []
            raise

        data = (
            resp["data"]
            if isinstance(resp, dict) and "data" in resp and isinstance(resp["data"], list)
            else resp if isinstance(resp, list)
            else []
        )
        return [item["id"] for item in data if isinstance(item, dict) and "id" in item
    ]
# endregion

# region ============= PRODUTO: DETALHE POR ID =============

    def get_produto_por_id(self, id_produto):
        endpoint = f"produtos/{id_produto}"
        try:
            response = self.get(endpoint)
            return response
        except Exception as e:
            log_etl("PRODUTOS", "ERRO", f"Erro ao buscar detalhe do produto {id_produto}", erro=str(e))
            return None
# endregion

# region ============= DEPÓSITOS: GET =============

    def get_depositos(self):
        """
        Recupera a lista de depósitos do Bling.
        """
        endpoint = "depositos"
        try:
            resp = self.get(endpoint)
            # Bling retorna uma lista em "data" ou diretamente uma lista
            if isinstance(resp, dict) and "data" in resp:
                return resp["data"]
            return resp  # fallback para outros formatos
        except Exception as e:
            raise Exception(f"Erro ao buscar depósitos: {e}")

# endregion

# region ============= PRODUTO: SALDO POR ID =============

    def get_saldo_produto_por_id(self, id_produto):
        endpoint = "estoques/saldos"
        params = {"idsProdutos[]": str(id_produto)}   # <--- Colchetes aqui!
        try:
            response = self.get(endpoint, params=params)
            # log_etl("SALDO_PROD_DEP", "DEBUG", f"Saldo retornado para ID {id_produto}: {response}")
            if isinstance(response, dict) and "data" in response:
                return response["data"]
            return response
        except requests.exceptions.HTTPError as e:
            # Log detalhado para entender o erro 400
            log_etl(
                "SALDO_PROD_DEP",
                "ERRO",
                f"Erro ao buscar saldo do produto {id_produto}",
                erro=f"status: {e.response.status_code}, resp: {e.response.text}"
            )
            if e.response.status_code == 400:
                return None
            return None
        except Exception as e:
            log_etl(
                "SALDO_PROD_DEP",
                "ERRO",
                f"Erro inesperado ao buscar saldo do produto {id_produto}",
                erro=str(e)
            )
            return None

# endregion

# region ============= PEDIDOS: VENDAS (LISTA IDS/PAGINADO) =============
    def get_pedidos_vendas_ids_pagina(self, pagina: int, limit: int = 100, params: dict | None = None):
        """
        Busca IDs de pedidos de venda de UMA página.
        Aceita filtros: dataAlteracaoInicial/dataAlteracaoFinal.
        """
        endpoint = "pedidos/vendas"
        page_params = (params or {}).copy()
        page_params["limite"] = limit
        page_params["pagina"] = pagina

        try:
            resp = self.get(endpoint, params=page_params)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                return []
            raise

        data = resp.get("data", []) if isinstance(resp, dict) else (resp if isinstance(resp, list) else [])
        return [item["id"] for item in data if isinstance(item, dict) and "id" in item]
# endregion

# region ============= PEDIDOS: VENDAS (DETALHE POR ID) =============
    def get_pedido_venda_por_id(self, id_pedido_venda: int):
        endpoint = f"pedidos/vendas/{id_pedido_venda}"
        try:
            response = self.get(endpoint)
            if isinstance(response, dict) and "data" in response and isinstance(response["data"], dict):
                return response["data"]
            return response
        except requests.exceptions.HTTPError as e:
            log_etl("PEDIDOS_VENDAS", "ERRO",
                    f"Erro ao buscar pedido {id_pedido_venda}",
                    erro=f"status: {e.response.status_code}, resp: {e.response.text}")
            if e.response.status_code == 404:
                return None
            return None
        except Exception as e:
            log_etl("PEDIDOS_VENDAS", "ERRO", f"Erro inesperado no pedido {id_pedido_venda}", erro=str(e))
            return None
# endregion

# region ============= CATEGORIAS: RECEITAS/DESPESAS (LISTA/PAGINADO) =============
    def get_categorias_rec_desp_ids_pagina(self, pagina: int, limit: int = 100):
        """
        Retorna apenas os IDs da página de /categorias/receitas-despesas.
        """
        endpoint = "categorias/receitas-despesas"
        params = {"pagina": pagina, "limite": limit}
        resp = self.get(endpoint, params=params)
        data = resp.get("data", []) if isinstance(resp, dict) else (resp or [])
        return [item.get("id") for item in data if isinstance(item, dict) and item.get("id") is not None]

    def get_categorias_rec_desp_pagina(self, pagina: int, limit: int = 100):
        """
        Retorna os itens completos da página (para mapear direto sem endpoint de detalhe).
        """
        endpoint = "categorias/receitas-despesas"
        params = {"pagina": pagina, "limite": limit}
        resp = self.get(endpoint, params=params)
        return resp.get("data", []) if isinstance(resp, dict) else (resp or [])
# endregion


