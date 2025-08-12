# %% INICIALIZAÇÃO

from dotenv import load_dotenv
import os, json
from datetime import datetime, timedelta
import time
from src.bling_api import BlingAPI
from src.db import (
    upsert_empresa_bling_bulk,
    upsert_categoria_produto_bling_bulk,
    upsert_grupo_produto_bling_bulk,
    upsert_produto_bling_bulk,
    upsert_canais_venda_bling_bulk,
    upsert_vendedores_bling,
    upsert_deposito_bling_bulk,
    upsert_saldo_produto_deposito_bulk,
    upsert_pedido_venda_bling_bulk,
    upsert_categoria_receita_despesa_bling_bulk
)
from src.transformers import (
    map_empresa,
    map_categoria_produto,
    map_grupo_produto,
    map_produtos,
    map_canais_venda,
    map_vendedores,
    map_deposito,
    map_saldo_produto_deposito,
    map_pedido_venda,
    map_categoria_receita_despesa
)
from src.utils import (
    atualizar_controle_carga,
    get_ultima_data_carga,
    registrar_falha_importacao,
    get_data_periodo_incremental,
    flush_buffer
)
from src.log import (
    log_etl,
    iniciar_log_etl,
    finalizar_log_etl,
)

from src.config import (
    DEBUG,
    MARGEM_MINUTOS_DRIFT,
    CARGA_FULL,
    BLING_FULL_INICIO,       
    BLING_FULL_FIM,
    DATA_FULL_INICIAL,
    MARGEM_DIAS_INCREMENTO,
    RODAR_EMPRESA,
    RODAR_CATEGORIA,
    RODAR_GRUPO_PRODUTO,
    RODAR_PRODUTO,
    RODAR_CANAIS_VENDA,
    RODAR_VENDEDOR,
    RODAR_DEPOSITOS,
    RODAR_SALDO_PRODUTO_DEPOSITO,
    RODAR_PEDIDOS_VENDAS,   
    RODAR_CATEGORIAS_RECEITAS_DESPESAS 
)

from src.date_utils import (
    format_bling_datetime
)


load_dotenv()
api_key = os.getenv("BLING_API_KEY")
db_uri = os.getenv("POSTGRES_URI")
api = BlingAPI(api_key)


# %% EMPRESA

if  RODAR_EMPRESA:
    log_etl("EMPRESA", "INÍCIO", "Carga da empresa iniciada")
    id_log_empresa = iniciar_log_etl(db_uri, tabela="empresa_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        # EMPRESA é sempre FULL: não existe incremental, paginação ou filtro de datas!
        try:
            empresa_dict = api.get_empresa()
            empresas = [empresa_dict]
        except Exception as api_erro:
            erro_msg = f"Falha na API de empresa: {api_erro}"
            log_etl("EMPRESA", "ERRO", erro=erro_msg)
            registrar_falha_importacao(
                db_uri=db_uri,
                entidade="empresa",
                id_referencia=None,
                erro=erro_msg
            )
            finalizar_log_etl(db_uri, id_log_empresa, status="erro", mensagem_erro=erro_msg)
            raise

        if DEBUG:
            #log_etl("EMPRESA", "DEBUG", f"Dados da empresa retornados: {empresa_dict}") << Completo
            log_etl("EMPRESA", "DEBUG", f"Dados da empresa retornados: {empresa_dict.get('id')}") # somente id

        log_etl("EMPRESA", "API", "Dados coletados da API", quantidade=1)

        lista_empresas = []
        for idx, e in enumerate(empresas, 1):
            try:
                mapped = map_empresa(e)
                lista_empresas.append(mapped)
            except Exception as erro:
                erro_msg = f"Falha ao mapear empresa idx {idx}: {erro}"
                log_etl("EMPRESA", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="empresa",
                    id_referencia=e.get("id"),
                    erro=erro_msg
                )

        if lista_empresas:
            upsert_empresa_bling_bulk(lista_empresas, db_uri)
            log_etl("EMPRESA", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_empresas))

        finalizar_log_etl(db_uri, id_log_empresa, status="finalizado")
        log_etl("EMPRESA", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        # Controle de carga: sempre FULL para empresa!
        atualizar_controle_carga(
            db_uri,
            "empresa",
            "stg.empresa_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("EMPRESA", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_empresa, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("EMPRESA", "DESLIGADA", "Carga da empresa está desligada (RODAR_EMPRESA = False)")


#%% CATEGORIA PRODUTO

if RODAR_CATEGORIA:
    log_etl("CATEGORIA", "INÍCIO", "Carga da categoria produto iniciada")
    id_log_cat = iniciar_log_etl(db_uri, tabela="categoria_produto_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        categorias = api.get_all_paginated("categorias/produtos", data_path=['data'])
        log_etl("CATEGORIA", "API", "Dados coletados da API", quantidade=len(categorias))

        lista_categorias = [map_categoria_produto(c) for c in categorias]
        upsert_categoria_produto_bling_bulk(lista_categorias, db_uri)
        log_etl("CATEGORIA", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_categorias))

        finalizar_log_etl(db_uri, id_log_cat, status="finalizado")
        log_etl("CATEGORIA", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))
        atualizar_controle_carga(
            db_uri,
            "categoria_produto",
            "stg.categoria_produto_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("CATEGORIA", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_cat, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("CATEGORIA", "DESLIGADA", "Carga da categoria está desligada (RODAR_CATEGORIA = False)")


#%% GRUPO PRODUTO

if RODAR_GRUPO_PRODUTO:
    log_etl("GRUPO PRODUTO", "INÍCIO", "Carga do grupo produto iniciada")
    id_log_grupo = iniciar_log_etl(db_uri, tabela="grupo_produto_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        grupos = api.get_all_paginated("grupos-produtos", data_path=['data'])
        log_etl("GRUPO PRODUTO", "API", "Dados coletados da API", quantidade=len(grupos))

        lista_grupos = [map_grupo_produto(g) for g in grupos]
        upsert_grupo_produto_bling_bulk(lista_grupos, db_uri)
        log_etl("GRUPO PRODUTO", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_grupos))

        finalizar_log_etl(db_uri, id_log_grupo, status="finalizado")
        log_etl("GRUPO PRODUTO", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))
        atualizar_controle_carga(
            db_uri,
            "grupo_produto",
            "stg.grupo_produto_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("GRUPO PRODUTO", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_grupo, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("GRUPO PRODUTO", "DESLIGADA", "Carga da grupo produto está desligada (RODAR_GRUPO_PRODUTO = False)")


#%% PRODUTO 

if RODAR_PRODUTO:
    log_etl("PRODUTOS", "INÍCIO", "Carga de produtos iniciada")
    id_log_prod = iniciar_log_etl(db_uri, tabela="produtos_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        params = None
        if not CARGA_FULL:
            # Usa a função incremental, margens e data inicial do config
            dt_inicial_full = DATA_FULL_INICIAL  # Ex: datetime(2021, 1, 1)
            data_ini, data_fim = get_data_periodo_incremental(
                db_uri,
                "produto",
                "api_to_stg",
                MARGEM_DIAS_INCREMENTO,
                dt_inicial_full
            )
            params = {
                "dataAlteracaoInicial": data_ini,
                "dataAlteracaoFinal": data_fim
            }
            if DEBUG:
                log_etl(
                    "PRODUTOS",
                    "DEBUG",
                    f"Incremental - dt_ultima_carga: {get_ultima_data_carga(db_uri, 'produto', 'api_to_stg')}, dataAlteracaoInicial: {data_ini}, dataAlteracaoFinal: {data_fim}"
                )

        pagina = 1
        limite = 100
        total_inseridos = 0
        maior_data = None

        while True:
            ids_produtos = api.get_produtos_ids_pagina(pagina, limit=limite, params=params)
            if not ids_produtos:
                break

            if DEBUG:
                log_etl(
                    "PRODUTOS",
                    "DEBUG",
                    f"Página {pagina}: {len(ids_produtos)} IDs coletados."
                )

            produtos_detalhados = []
            for idx, id_prod in enumerate(ids_produtos, 1):
                if DEBUG:
                    log_etl(
                        "PRODUTOS",
                        "DEBUG",
                        f"ID {((pagina-1)*limite)+idx}: {id_prod}"
                    )

                resp = api.get_produto_por_id(id_prod)
                if resp and "data" in resp:
                    produtos_detalhados.append(resp["data"])
                else:
                    erro_msg = f"Produto ID {id_prod} não retornou detalhes ou veio vazio."
                    # log_etl("PRODUTOS", "WARN", erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="produto",
                        id_referencia=id_prod,
                        erro=erro_msg
                    )
                time.sleep(0.35)

            log_etl(
                "PRODUTOS",
                "API",
                f"IDs coletados da página {pagina}",
                quantidade=len(produtos_detalhados)
            )

            produtos_mapeados = map_produtos(produtos_detalhados)
            upsert_produto_bling_bulk(db_uri, produtos_mapeados)
            log_etl(
                "PRODUTOS",
                "DB",
                f"Inseridos/atualizados no banco (página {pagina})",
                quantidade=len(produtos_mapeados)
            )

            total_inseridos += len(produtos_mapeados)
            pagina_maior_data = max(
                [p["dt_atualizacao"] for p in produtos_mapeados if p.get("dt_atualizacao")],
                default=None
            )
            if pagina_maior_data and (maior_data is None or pagina_maior_data > maior_data):
                maior_data = pagina_maior_data

            # TESTE: Limitar a 3 páginas, descomente a linha abaixo para testar:
            # log_etl("PRODUTOS", "DEBUG", f"Interrompendo na página {pagina}")  # Só para debug manual
            #if pagina >= 1: break

            pagina += 1

        finalizar_log_etl(db_uri, id_log_prod, status="finalizado")
        log_etl(
            "PRODUTOS",
            "FIM",
            "Carga finalizada",
            tempo=(time.time() - tempo_inicio),
            quantidade=total_inseridos
        )

        atualizar_controle_carga(
            db_uri,
            "produto",
            "stg.produto_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S'
        
        )
    except Exception as e:
        log_etl("PRODUTOS", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_prod, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("PRODUTOS", "DESLIGADA", "Carga de produtos está desligada (RODAR_PRODUTO = False)")


#%% CANAIS VENDA

if RODAR_CANAIS_VENDA:
    log_etl("CANAIS VENDA", "INÍCIO", "Carga de canais venda iniciada")
    id_log_cat = iniciar_log_etl(db_uri, tabela="canais_venda_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        if DEBUG:
            log_etl("CANAIS VENDA", "DEBUG", "Buscando canais de venda via API Bling")
        canais_venda = api.get_all_paginated("canais-venda", data_path=['data'])
        
        if DEBUG:
            log_etl("CANAIS VENDA", "DEBUG", f"Total canais recebidos: {len(canais_venda)}")
            for idx, c in enumerate(canais_venda, 1):
                log_etl("CANAIS VENDA", "DEBUG", f"ID {idx}: {c.get('id')}")

        log_etl("CANAIS VENDA", "API", "Dados coletados da API", quantidade=len(canais_venda))

        lista_canais_venda = [map_canais_venda(c) for c in canais_venda]
        upsert_canais_venda_bling_bulk(lista_canais_venda, db_uri)

        log_etl("CANAIS VENDA", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_canais_venda))

        finalizar_log_etl(db_uri, id_log_cat, status="finalizado")
        log_etl("CANAIS VENDA", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        atualizar_controle_carga(
            db_uri,
            "canais_venda",
            "stg.canais_venda_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("CANAIS VENDA", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_cat, status="erro", mensagem_erro=str(e))
        registrar_falha_importacao(
            db_uri,
            entidade="canais_venda",
            id_referencia=None,
            erro=str(e)
        )
        raise
else:
    log_etl("CANAIS VENDA", "DESLIGADA", "Carga da canais venda está desligada (RODAR_CANAIS_VENDA = False)")


#%%VENDEDORES

if RODAR_VENDEDOR:
    log_etl("VENDEDORES", "INÍCIO", "Carga de vendedores iniciada")
    id_log_vend = iniciar_log_etl(db_uri, tabela="vendedor_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        # 1. Buscar todos os IDs (etapa paginada)
        tempo_busca_ids = time.time()
        ids_vendedores = api.get_all_vendedores_ids()
        tempo_ids = time.time() - tempo_busca_ids

        # 2. Buscar os detalhes de cada vendedor
        vendedores_detalhados = []
        for id_vend in ids_vendedores:
            json = api.get_vendedor_por_id(id_vend)
            if json:
                registro = map_vendedores(json)
                if registro:
                    vendedores_detalhados.append(registro)

        log_etl("VENDEDORES", "API", "Dados detalhados coletados da API", quantidade=len(vendedores_detalhados))

        upsert_vendedores_bling(db_uri, vendedores_detalhados)
        log_etl("VENDEDORES", "DB", "Inseridos/atualizados no banco", quantidade=len(vendedores_detalhados))

        finalizar_log_etl(db_uri, id_log_vend, status="finalizado")
        log_etl("VENDEDORES", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))
        atualizar_controle_carga(
            db_uri,
            "vendedores",
            "stg.vendedor",
            "api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("VENDEDORES", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_vend, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("VENDEDORES", "DESLIGADA", "Carga de vendedores está desligada (RODAR_VENDEDOR = False)")


# %% DEPOSITOS

if RODAR_DEPOSITOS:
    log_etl("DEPOSITOS", "INÍCIO", "Carga de depósitos iniciada")
    id_log_deposito = iniciar_log_etl(db_uri, tabela="deposito_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        # Depósito é carga FULL!
        try:
            depositos_resp = api.get_depositos()
            depositos = depositos_resp if isinstance(depositos_resp, list) else [depositos_resp]
        except Exception as api_erro:
            erro_msg = f"Falha na API de depósitos: {api_erro}"
            log_etl("DEPOSITOS", "ERRO", erro=erro_msg)
            registrar_falha_importacao(
                db_uri=db_uri,
                entidade="depositos",
                id_referencia=None,
                erro=erro_msg
            )
            finalizar_log_etl(db_uri, id_log_deposito, status="erro", mensagem_erro=erro_msg)
            raise

        if DEBUG:
            for idx, d in enumerate(depositos, 1):
                log_etl("DEPOSITOS", "DEBUG", f"ID {idx}: {d.get('id')}")

        log_etl("DEPOSITOS", "API", "Dados coletados da API", quantidade=len(depositos))

        lista_depositos = []
        for idx, d in enumerate(depositos, 1):
            try:
                mapped = map_deposito(d)
                lista_depositos.append(mapped)
            except Exception as erro:
                erro_msg = f"Falha ao mapear deposito idx {idx}: {erro}"
                log_etl("DEPOSITOS", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="depositos",
                    id_referencia=d.get("id"),
                    erro=erro_msg
                )

        if lista_depositos:
            upsert_deposito_bling_bulk(lista_depositos, db_uri)
            log_etl("DEPOSITOS", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_depositos))

        finalizar_log_etl(db_uri, id_log_deposito, status="finalizado")
        log_etl("DEPOSITOS", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        # Controle de carga: FULL!
        atualizar_controle_carga(
            db_uri,
            "depositos",
            "stg.deposito_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("DEPOSITOS", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_deposito, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("DEPOSITOS", "DESLIGADA", "Carga de depósitos está desligada (RODAR_DEPOSITOS = False)")
# %% PRODUTO SALDO

if RODAR_SALDO_PRODUTO_DEPOSITO:
    log_etl("SALDO_PROD_DEP", "INÍCIO", "Carga de saldos produto x depósito iniciada")
    id_log_saldo = iniciar_log_etl(db_uri, tabela="saldo_produto_deposito_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        pagina = 1
        limite = 100
        total_inseridos = 0

        while True:
            ids_produtos = api.get_produtos_ids_pagina(pagina, limit=limite)
            if not ids_produtos:
                break

            if DEBUG:
                log_etl("SALDO_PROD_DEP", "DEBUG", f"Página {pagina}: {len(ids_produtos)} IDs coletados.")

            registros_batch = []
            for idx, id_prod in enumerate(ids_produtos, 1):
                if DEBUG:
                    log_etl("SALDO_PROD_DEP", "DEBUG", f"ID {((pagina-1)*limite)+idx}: {id_prod}")
                try:
                    saldo_data = api.get_saldo_produto_por_id(id_prod)
                    if saldo_data:
                        registros_batch.extend(map_saldo_produto_deposito(saldo_data))
                    else:
                        erro_msg = f"Produto ID {id_prod} não retornou saldo ou veio vazio."
                        registrar_falha_importacao(
                            db_uri=db_uri,
                            entidade="saldo_produto_deposito",
                            id_referencia=id_prod,
                            erro=erro_msg
                        )
                except Exception as erro:
                    erro_msg = f"Falha ao buscar saldo do produto {id_prod}: {erro}"
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="saldo_produto_deposito",
                        id_referencia=id_prod,
                        erro=erro_msg
                    )
                time.sleep(0.35)

            log_etl("SALDO_PROD_DEP", "API", f"Saldos coletados da página {pagina}", quantidade=len(registros_batch))
            upsert_saldo_produto_deposito_bulk(db_uri, registros_batch)
            log_etl("SALDO_PROD_DEP", "DB", f"Inseridos/atualizados no banco (página {pagina})", quantidade=len(registros_batch))

            total_inseridos += len(registros_batch)

            pagina += 1  # <--- incremento sempre após processar a página!

            # TESTE: Limitar a 2 páginas (processa páginas 1 e 2, depois para)
            """ if pagina > 2:
                log_etl("SALDO_PROD_DEP", "DEBUG", f"Interrompendo na página {pagina-1}")  # Loga a página final processada
                break """

        finalizar_log_etl(db_uri, id_log_saldo, status="finalizado")
        log_etl("SALDO_PROD_DEP", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri,
            "saldo_produto_deposito",
            "stg.saldo_produto_deposito_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl("SALDO_PROD_DEP", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_saldo, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("SALDO_PROD_DEP", "DESLIGADA", "Carga de saldos produto x depósito está desligada (RODAR_SALDO_PRODUTO_DEPOSITO = False)")



# %% PEDIDOS: VENDAS
if RODAR_PEDIDOS_VENDAS:
    ENT = "PEDIDOS_VENDAS"
    log_etl(ENT, "INÍCIO", "Carga de pedidos de venda iniciada")
    id_log = iniciar_log_etl(db_uri, tabela="pedido_venda_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        # ===== FULL (fixo por janela do config) =====
        if CARGA_FULL:
            dt_ini = (BLING_FULL_INICIO.strftime("%Y-%m-%d")
                    if isinstance(BLING_FULL_INICIO, datetime) else str(BLING_FULL_INICIO)[:10])
            dt_fim = (BLING_FULL_FIM.strftime("%Y-%m-%d")
                    if isinstance(BLING_FULL_FIM, datetime) else str(BLING_FULL_FIM)[:10])
        else:
            # ===== INCREMENTAL =====
            dt_ini, dt_fim = get_data_periodo_incremental(
                db_uri,
                "pedido_venda",
                "api_to_stg",
                MARGEM_DIAS_INCREMENTO,
                DATA_FULL_INICIAL
            )
            # drift mínimo: corte no fim (mas ainda só DATA, sem hora)
            dt_fim = (datetime.now() - timedelta(minutes=MARGEM_MINUTOS_DRIFT)).strftime("%Y-%m-%d")

        # >>> Para pedidos de venda (fato), filtramos por data de emissão
        params_base = {
            "dataInicial": dt_ini,
            "dataFinal":   dt_fim
        }

        if DEBUG:
            log_etl(ENT, "DEBUG", f"Janela usada: {params_base}")

        pagina = 1
        limite = 100
        total_inseridos = 0
        buffer = []
        BATCH_SIZE = 20
        MAX_PAGES = None  # opcional p/ testes

        while True:
            ids_pedidos = api.get_pedidos_vendas_ids_pagina(pagina, limit=limite, params=params_base)
            if not ids_pedidos:
                break

            if DEBUG:
                log_etl(ENT, "DEBUG", f"Página {pagina}: {len(ids_pedidos)} IDs coletados.")

            for idx, id_ped in enumerate(ids_pedidos, 1):
                if DEBUG:
                    log_etl(ENT, "DEBUG", f"ID {((pagina-1)*limite)+idx}: {id_ped}")
                try:
                    detalhe = api.get_pedido_venda_por_id(id_ped)
                    if detalhe:
                        buffer.append(map_pedido_venda(detalhe))
                        if len(buffer) >= BATCH_SIZE:
                            total_inseridos += flush_buffer(
                                db_uri=db_uri,
                                buffer=buffer,
                                upsert_fn=upsert_pedido_venda_bling_bulk,
                                batch_size=BATCH_SIZE,
                                ent_label=ENT,
                                log_fn=log_etl
                            )
                    else:
                        registrar_falha_importacao(
                            db_uri=db_uri,
                            entidade="pedido_venda",
                            id_referencia=id_ped,
                            erro=f"Pedido ID {id_ped} não retornou detalhe ou veio vazio."
                        )
                except Exception as erro:
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="pedido_venda",
                        id_referencia=id_ped,
                        erro=f"Falha ao buscar pedido {id_ped}: {erro}"
                    )
                time.sleep(0.35)

            log_etl(ENT, "API", f"Detalhes coletados da página {pagina}", quantidade=len(ids_pedidos))

            if MAX_PAGES and pagina >= MAX_PAGES:
                break
            pagina += 1

        total_inseridos += flush_buffer(
            db_uri=db_uri,
            buffer=buffer,
            upsert_fn=upsert_pedido_venda_bling_bulk,
            batch_size=BATCH_SIZE,
            ent_label=ENT,
            log_fn=log_etl
        )

        finalizar_log_etl(db_uri, id_log, status="finalizado")
        log_etl(ENT, "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri,
            "pedido_venda",
            "stg.pedido_venda_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S' if not CARGA_FULL else 'N'
        )

    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("PEDIDOS_VENDAS", "DESLIGADA", "Carga de pedidos de venda está desligada (RODAR_PEDIDOS_VENDAS = False)")



# %% CATEGORIAS RECEITAS/DESPESAS (DIMENSÃO - FULL COM LOG DETALHADO)
if RODAR_CATEGORIAS_RECEITAS_DESPESAS:
    ENT = "CAT_REC_DESP"
    log_etl(ENT, "INÍCIO", "Carga de categorias de receitas/despesas iniciada")
    id_log_cat_fin = iniciar_log_etl(db_uri, tabela="categoria_receita_despesa_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        pagina = 1
        limite = 100
        total_inseridos = 0
        MAX_PAGES = None  # defina 3 para teste

        while True:
            ids = api.get_categorias_rec_desp_ids_pagina(pagina, limit=limite)
            if not ids:
                break

            if DEBUG:
                log_etl(ENT, "DEBUG", f"Página {pagina}: {len(ids)} IDs coletados.")
                for idx, _id in enumerate(ids, 1):
                    log_etl(ENT, "DEBUG", f"ID {((pagina-1)*limite)+idx}: {_id}")

            # Busca itens completos da mesma página e mapeia
            itens = api.get_categorias_rec_desp_pagina(pagina, limit=limite)
            lista_mapeada = []
            for it in itens:
                try:
                    # raise Exception("Simulação de erro")  # <<< DESCOMENTE para testar falha
                    lista_mapeada.append(map_categoria_receita_despesa(it))
                except Exception as erro:
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="categoria_receita_despesa",
                        id_referencia=it.get("id"),
                        erro=f"Falha ao mapear categoria: {erro}"
                    )

            log_etl(ENT, "API", f"Itens coletados da página {pagina}", quantidade=len(lista_mapeada))

            if lista_mapeada:
                upsert_categoria_receita_despesa_bling_bulk(lista_mapeada, db_uri)
                log_etl(ENT, "DB", f"Inseridos/atualizados (página {pagina})", quantidade=len(lista_mapeada))
                total_inseridos += len(lista_mapeada)

            if MAX_PAGES and pagina >= MAX_PAGES:
                break
            pagina += 1

        finalizar_log_etl(db_uri, id_log_cat_fin, status="finalizado")
        log_etl(ENT, "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri,
            "categoria_receita_despesa",
            "stg.categoria_receita_despesa_bling",
            "api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'
        )
    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_cat_fin, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("CAT_REC_DESP", "DESLIGADA", "Carga está desligada (RODAR_CATEGORIAS_RECEITAS_DESPESAS = False)")




# %%
