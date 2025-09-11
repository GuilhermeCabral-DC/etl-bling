# %% INICIALIZAÇÃO
# SETUP:
# carrega .env, configura API, conexão e parâmetros globais

from dotenv import load_dotenv
import os, psycopg2
from datetime import datetime, timedelta
import time

from src.bling_api import BlingAPI
from src.db import (
    call_procedure,
    upsert_empresa_bling_bulk,
    upsert_categoria_produto_bling_bulk,
    upsert_grupo_produto_bling_bulk,
    upsert_produto_bling_bulk,
    upsert_canais_venda_bling_bulk,
    upsert_vendedores_bling_bulk,
    upsert_deposito_bling_bulk,
    upsert_saldo_produto_deposito_bulk,
    upsert_pedido_venda_bling_bulk,
    upsert_categoria_receita_despesa_bling_bulk,
    upsert_contato_bling_bulk,
    upsert_produto_estrutura_bling_bulk
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
    map_categoria_receita_despesa,
    map_contato,
    map_produto_estrutura
)
from src.utils import (
    atualizar_controle_carga,
    registrar_falha_importacao,
    get_data_periodo_incremental,
    flush_buffer,
    montar_filtro_pedidos,
    montar_filtro_contatos
)
from src.log import (
    log_etl,
    iniciar_log_etl,
    finalizar_log_etl,
)
from src.config import (
    DEBUG,
    CARGA_FULL,
    BLING_FULL_INICIO,
    BLING_FULL_FIM,
    DATA_FULL_INICIAL,
    MARGEM_DIAS_INCREMENTO,
    RODAR_PIPELINE_CARGA_COMPLETA,
    RODAR_EMPRESA,
    RODAR_CATEGORIA,
    RODAR_GRUPO_PRODUTO,
    RODAR_PRODUTO,
    RODAR_CANAIS_VENDA,
    RODAR_VENDEDOR,
    RODAR_DEPOSITOS,
    RODAR_SALDO_PRODUTO_DEPOSITO,
    RODAR_PEDIDOS_VENDAS,
    RODAR_CATEGORIAS_RECEITAS_DESPESAS,
    RODAR_CONTATO,
    RODAR_PRODUTO_ESTRUTURA
)
from src.date_utils import format_bling_datetime

load_dotenv()
api_key = os.getenv("BLING_API_KEY")
db_uri = os.getenv("POSTGRES_URI")
api = BlingAPI(api_key)

# 🔹 Calcula janela de carga global (usada por todos os blocos)
try:
    if CARGA_FULL:
        dt_ini = BLING_FULL_INICIO.strftime("%Y-%m-%d") if isinstance(BLING_FULL_INICIO, datetime) else str(BLING_FULL_INICIO)[:10]
        dt_fim = BLING_FULL_FIM.strftime("%Y-%m-%d") if isinstance(BLING_FULL_FIM, datetime) else str(BLING_FULL_FIM)[:10]
    else:
        dt_ini, dt_fim = get_data_periodo_incremental(
            db_uri,
            "stg.pedido_venda_bling",   # tabela referência para incremental
            "api_to_stg",
            MARGEM_DIAS_INCREMENTO,
            DATA_FULL_INICIAL
        )

    log_etl("ORQUESTRADOR", "INFO", f"Janela de carga definida: De {dt_ini} até {dt_fim}")

except Exception as e:
    log_etl("ORQUESTRADOR", "ERRO", erro=f"Falha ao calcular janela de carga: {e}")
    raise

# %% EMPRESA'
# EMPRESA: 
# Carga sempre FULL, sem filtros ou paginação

if RODAR_EMPRESA:
    log_etl("EMPRESA", "INÍCIO", "Carga da empresa iniciada")
    id_log_empresa = iniciar_log_etl(db_uri, tabela="empresa_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
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
                erro=erro_msg,
                id_log=id_log_empresa
            )
            finalizar_log_etl(db_uri, id_log_empresa, status="erro", mensagem_erro=erro_msg)
            raise

        if DEBUG:
            log_etl("EMPRESA", "DEBUG", f"Dados da empresa retornados: {empresa_dict.get('id')}")

        log_etl("EMPRESA", "API", "Dados coletados da API", quantidade=1)

        lista_empresas = []
        for idx, e in enumerate(empresas, 1):
            try:
                mapped = map_empresa(e)
                lista_empresas.append(mapped)
            except Exception as erro:
                erro_msg = f"Falha ao mapear empresa idx {idx}: {erro}"
                if DEBUG:
                    log_etl("EMPRESA", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="empresa",
                    id_referencia=e.get("id"),
                    erro=erro_msg,
                    id_log=id_log_empresa
                )

        if lista_empresas:
            upsert_empresa_bling_bulk(lista_empresas, db_uri)
            log_etl("EMPRESA", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_empresas))

        finalizar_log_etl(db_uri, id_log_empresa, status="finalizado")
        log_etl("EMPRESA", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        atualizar_controle_carga(
            db_uri=db_uri,
            tabela_fisica="stg.empresa_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N',
            entidade="empresa"
        )

    except Exception as e:
        log_etl("EMPRESA", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_empresa, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("EMPRESA", "DESLIGADA", "Carga da empresa está desligada (RODAR_EMPRESA = False)")

# %% CATEGORIA PRODUTO
# CATEGORIA PRODUTO
# Carga FULL paginada via endpoint categorias/produtos

if RODAR_CATEGORIA:
    log_etl("CATEGORIA", "INÍCIO", "Carga da categoria produto iniciada")
    id_log_cat = iniciar_log_etl(db_uri, tabela="categoria_produto_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        categorias = api.get_all_paginated("categorias/produtos", data_path=['data'])
        log_etl("CATEGORIA", "API", "Dados coletados da API", quantidade=len(categorias))

        lista_categorias = []
        for idx, c in enumerate(categorias, 1):
            try:
                mapped = map_categoria_produto(c)
                lista_categorias.append(mapped)
            except Exception as erro:
                erro_msg = f"Falha ao mapear categoria idx {idx}: {erro}"
                if DEBUG:
                    log_etl("CATEGORIA", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="categoria_produto",
                    id_referencia=c.get("id"),
                    erro=erro_msg,
                    id_log=id_log_cat
                )

        upsert_categoria_produto_bling_bulk(lista_categorias, db_uri)
        log_etl("CATEGORIA", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_categorias))

        finalizar_log_etl(db_uri, id_log_cat, status="finalizado")
        log_etl("CATEGORIA", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="produto",
            tabela_fisica="stg.categoria_produto_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )

    except Exception as e:
        log_etl("CATEGORIA", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_cat, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("CATEGORIA", "DESLIGADA", "Carga da categoria está desligada (RODAR_CATEGORIA = False)")

# %% GRUPO PRODUTO
# GRUPO PRODUTO
# Carga FULL da lista de grupos de produto (sem filtros ou incremental)

if RODAR_GRUPO_PRODUTO:
    log_etl("GRUPO PRODUTO", "INÍCIO", "Carga do grupo de produtos iniciada")
    id_log_grupo = iniciar_log_etl(db_uri, tabela="grupo_produto_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        # Requisição da lista completa de grupos de produto via paginação
        grupos = api.get_all_paginated("grupos-produtos", data_path=['data'])
        log_etl("GRUPO PRODUTO", "API", "Dados coletados da API", quantidade=len(grupos))

        lista_grupos = []
        for idx, g in enumerate(grupos, 1):
            try:
                mapped = map_grupo_produto(g)
                lista_grupos.append(mapped)
            except Exception as erro:
                erro_msg = f"Falha ao mapear grupo produto idx {idx}: {erro}"
                if DEBUG:
                    log_etl("GRUPO PRODUTO", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="grupo_produto",
                    id_referencia=g.get("id"),
                    erro=erro_msg,
                    id_log=id_log_grupo
                )

        if lista_grupos:
            upsert_grupo_produto_bling_bulk(lista_grupos, db_uri)
            log_etl("GRUPO PRODUTO", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_grupos))

        finalizar_log_etl(db_uri, id_log_grupo, status="finalizado")
        log_etl("GRUPO PRODUTO", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        # Atualiza controle de carga (apenas para rastreabilidade, pois é sempre FULL)
        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="produto",
            tabela_fisica="stg.grupo_produto_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )

    except Exception as e:
        log_etl("GRUPO PRODUTO", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_grupo, status="erro", mensagem_erro=str(e))
        raise

else:
    log_etl("GRUPO PRODUTO", "DESLIGADA", "Carga do grupo de produtos está desligada (RODAR_GRUPO_PRODUTO = False)")

# %% DEPOSITO
# DEPOSITO
# Carga FULL dos depósitos, sem filtros ou paginação

if RODAR_DEPOSITOS:
    log_etl("DEPOSITOS", "INÍCIO", "Carga de depósitos iniciada")
    id_log_deposito = iniciar_log_etl(db_uri, tabela="deposito_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        # Requisição à API (sem paginação)
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
                erro=erro_msg,
                id_log=id_log_deposito
            )
            finalizar_log_etl(db_uri, id_log_deposito, status="erro", mensagem_erro=erro_msg)
            raise

        # Log detalhado dos IDs (se DEBUG ativado)
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
                erro_msg = f"Falha ao mapear depósito idx {idx}: {erro}"
                if DEBUG:
                    log_etl("DEPOSITOS", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="depositos",
                    id_referencia=d.get("id"),
                    erro=erro_msg,
                    id_log=id_log_deposito
                )

        # Inserção em lote no banco
        if lista_depositos:
            upsert_deposito_bling_bulk(lista_depositos, db_uri)
            log_etl("DEPOSITOS", "DB", "Inseridos/atualizados no banco", quantidade=len(lista_depositos))

        finalizar_log_etl(db_uri, id_log_deposito, status="finalizado")
        log_etl("DEPOSITOS", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        # Atualização do controle de carga (FULL)
        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="estoque",
            tabela_fisica="stg.deposito_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'
        )

    except Exception as e:
        log_etl("DEPOSITOS", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_deposito, status="erro", mensagem_erro=str(e))
        raise

else:
    log_etl("DEPOSITOS", "DESLIGADA", "Carga de depósitos está desligada (RODAR_DEPOSITOS = False)")


# %% PRODUTO
# PRODUTO
# Coleta de produtos via ID paginado e fetch detalhado, com upsert em lote

if RODAR_PRODUTO:
    log_etl("PRODUTOS", "INÍCIO", "Carga de produtos iniciada")
    id_log_prod = iniciar_log_etl(db_uri, tabela="produtos_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        if CARGA_FULL:
            dt_ini = BLING_FULL_INICIO.strftime("%Y-%m-%d") if isinstance(BLING_FULL_INICIO, datetime) else str(BLING_FULL_INICIO)[:10]
            dt_fim = BLING_FULL_FIM.strftime("%Y-%m-%d") if isinstance(BLING_FULL_FIM, datetime) else str(BLING_FULL_FIM)[:10]
        else:
            dt_ini, dt_fim = get_data_periodo_incremental(
                db_uri,
                "stg.produto_bling",
                "api_to_stg",
                MARGEM_DIAS_INCREMENTO,
                DATA_FULL_INICIAL
            )

        params_base = {
            "dataInicial": dt_ini,
            "dataFinal": dt_fim
        }

        log_etl("PRODUTOS", "INFO", f"Janela de carga definida: De {dt_ini} até {dt_fim}")

        pagina = 1
        limite = 100
        total_inseridos = 0
        maior_data = None

        while True:
            params_api = {
                "dataAlteracaoInicial": params_base["dataInicial"],
                "dataAlteracaoFinal": params_base["dataFinal"]
            }

            ids_produtos = api.get_produtos_ids_pagina(pagina, limit=limite, params=params_api)
            if not ids_produtos:
                break

            if DEBUG:
                log_etl("PRODUTOS", "DEBUG", f"Página {pagina}: {len(ids_produtos)} IDs coletados.")

            produtos_detalhados = []
            for idx, id_prod in enumerate(ids_produtos, 1):
                if DEBUG:
                    log_etl("PRODUTOS", "DEBUG", f"ID {((pagina-1)*limite)+idx}: {id_prod}")
                try:
                    resp = api.get_produto_por_id(id_prod)
                    if resp and "data" in resp:
                        produtos_detalhados.append(resp["data"])
                    else:
                        erro_msg = f"Produto ID {id_prod} não retornou detalhes ou veio vazio."
                        if DEBUG:
                            log_etl("PRODUTOS", "WARN", erro=erro_msg)
                        registrar_falha_importacao(db_uri, "produto", id_prod, erro_msg, id_log_prod)
                except Exception as erro:
                    erro_msg = f"Erro ao buscar produto ID {id_prod}: {erro}"
                    if DEBUG:
                        log_etl("PRODUTOS", "WARN", erro=erro_msg)
                    registrar_falha_importacao(db_uri, "produto", id_prod, erro_msg, id_log_prod)

                time.sleep(0.35)

            log_etl("PRODUTOS", "API", f"IDs coletados da página {pagina}", quantidade=len(produtos_detalhados))

            produtos_mapeados = map_produtos(produtos_detalhados)
            upsert_produto_bling_bulk(db_uri, produtos_mapeados)
            log_etl("PRODUTOS", "DB", f"Inseridos/atualizados no banco (página {pagina})", quantidade=len(produtos_mapeados))

            total_inseridos += len(produtos_mapeados)

            pagina_maior_data = max(
                [p["dt_atualizacao"] for p in produtos_mapeados if p.get("dt_atualizacao")],
                default=None
            )
            if pagina_maior_data and (maior_data is None or pagina_maior_data > maior_data):
                maior_data = pagina_maior_data

            pagina += 1

        finalizar_log_etl(db_uri, id_log_prod, status="finalizado")
        log_etl("PRODUTOS", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="produto",
            tabela_fisica="stg.produto_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S'
        )

    except Exception as e:
        log_etl("PRODUTOS", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_prod, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("PRODUTOS", "DESLIGADA", "Carga de produtos está desligada (RODAR_PRODUTO = False)")

# %% PRODUTO SALDO
# PRODUTO SALDO
# Coleta de saldos de produtos por depósito. Sempre FULL (não há incremental na API).

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
                        if DEBUG:
                            log_etl("SALDO_PROD_DEP", "WARN", erro=erro_msg)
                        registrar_falha_importacao(
                            db_uri=db_uri,
                            entidade="saldo_produto_deposito",
                            id_referencia=id_prod,
                            erro=erro_msg,
                            id_log=id_log_saldo
                        )
                except Exception as erro:
                    erro_msg = f"Falha ao buscar saldo do produto {id_prod}: {erro}"
                    if DEBUG:
                        log_etl("SALDO_PROD_DEP", "WARN", erro=erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="saldo_produto_deposito",
                        id_referencia=id_prod,
                        erro=erro_msg,
                        id_log=id_log_saldo
                    )

                time.sleep(0.35)

            # Insere o batch da página
            log_etl("SALDO_PROD_DEP", "API", f"Saldos coletados da página {pagina}", quantidade=len(registros_batch))
            upsert_saldo_produto_deposito_bulk(db_uri, registros_batch)
            log_etl("SALDO_PROD_DEP", "DB", f"Inseridos/atualizados no banco (página {pagina})", quantidade=len(registros_batch))

            total_inseridos += len(registros_batch)
            pagina += 1  # Sempre incrementar após processar a página

        finalizar_log_etl(db_uri, id_log_saldo, status="finalizado")
        log_etl("SALDO_PROD_DEP", "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="estoque",
            tabela_fisica="stg.saldo_produto_deposito_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'  # Sempre full
        )

    except Exception as e:
        log_etl("SALDO_PROD_DEP", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_saldo, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("SALDO_PROD_DEP", "DESLIGADA", "Carga de saldos produto x depósito está desligada (RODAR_SALDO_PRODUTO_DEPOSITO = False)")

# %% PRODUTO ESTRUTURA
# PRODUTO ESTRUTURA
# Esse bloco coleta estruturas apenas de produtos compostos (formato = 'E').
# - Usa como janela: (última carga - 3 dias) até ontem
# - Faz upsert em lotes de 20 para não acumular em memória
# - Atualiza controle de carga ao final

if RODAR_PRODUTO_ESTRUTURA:
    log_etl("ESTRUTURA PRODUTO", "INÍCIO", "Iniciando carga de estruturas de produtos do tipo E")
    id_log_prod_estr = iniciar_log_etl(db_uri, tabela="produto_estrutura_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        with psycopg2.connect(db_uri) as conn:
            with conn.cursor() as cur:
                # Calcula X = última carga - 3 dias | Y = ontem
                cur.execute("""
                    SELECT COALESCE(MAX(dt_ultima_carga), '2020-01-01') 
                    FROM conf.controle_carga 
                    WHERE tabela_fisica = 'stg.produto_estrutura_bling'
                """)
                ultima_carga = cur.fetchone()[0]
                x = ultima_carga - timedelta(days=3)
                y = datetime.now().date() - timedelta(days=1)

                # Loga a janela usada
                log_etl("ESTRUTURA PRODUTO", "DEBUG", f"Filtro incremental: dt_atualizacao entre {x} e {y}")

                # Busca IDs de produtos compostos (kits) atualizados no intervalo
                cur.execute("""
                    SELECT id_bling 
                    FROM stg.produto_bling 
                    WHERE formato = 'E'
                      AND dt_atualizacao >= %s 
                      AND dt_atualizacao < %s
                """, (x, y))
                produtos_kit = [row[0] for row in cur.fetchall()]

        total_inseridos = 0
        estruturas_detalhadas = []

        for idx, id_prod in enumerate(produtos_kit, 1):
            if DEBUG:
                log_etl("ESTRUTURA PRODUTO", "DEBUG", f"Produto {idx}/{len(produtos_kit)}: ID {id_prod}")

            try:
                resp_estrutura = api.get_produto_estrutura_por_id(id_prod)
                if resp_estrutura and "data" in resp_estrutura:
                    estrutura_data = resp_estrutura["data"]
                    estruturas_map = map_produto_estrutura(estrutura_data, id_produto=id_prod)

                    # log_etl("ESTRUTURA PRODUTO", "DEBUG", f"{len(estruturas_map)} componentes mapeados para produto {id_prod}")

                    estruturas_detalhadas.extend(estruturas_map)

                    # ✅ Upsert parcial a cada 20 registros acumulados
                    if len(estruturas_detalhadas) >= 20:
                        upsert_produto_estrutura_bling_bulk(db_uri, estruturas_detalhadas)
                        log_etl("ESTRUTURA PRODUTO", "DB", "Inseridos/atualizados no banco (bloco parcial de 20+)", quantidade=len(estruturas_detalhadas))
                        total_inseridos += len(estruturas_detalhadas)
                        estruturas_detalhadas.clear()
                else:
                    erro_msg = f"Estrutura do produto ID {id_prod} não retornou dados."
                    log_etl("ESTRUTURA PRODUTO", "WARN", erro=erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="produto",
                        id_referencia=id_prod,
                        erro=erro_msg,
                        id_log=id_log_prod_estr
                    )

            except Exception as erro:
                erro_msg = f"Erro ao buscar estrutura do produto ID {id_prod}: {erro}"
                log_etl("ESTRUTURA PRODUTO", "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="produto",
                    id_referencia=id_prod,
                    erro=erro_msg,
                    id_log=id_log_prod_estr
                )

            time.sleep(0.35)

        # Inserção final (resto do buffer)
        if estruturas_detalhadas:
            log_etl("ESTRUTURA PRODUTO", "API", "Estruturas coletadas (lote final)", quantidade=len(estruturas_detalhadas))
            upsert_produto_estrutura_bling_bulk(db_uri, estruturas_detalhadas)
            total_inseridos += len(estruturas_detalhadas)

        # Resumo final
        log_etl("ESTRUTURA PRODUTO", "FIM", "Carga finalizada",
                tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        finalizar_log_etl(db_uri, id_log_prod_estr, status="finalizado")

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="produto",
            tabela_fisica="stg.produto_estrutura_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S'
        )

    except Exception as e:
        log_etl("ESTRUTURA PRODUTO", "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_prod_estr, status="erro", mensagem_erro=str(e))
        raise

else:
    log_etl("ESTRUTURA PRODUTO", "DESLIGADA", "Carga de estrutura de produtos está desligada (RODAR_PRODUTO_ESTRUTURA = False)")


# %% CANAL VENDA
# CANAL VENDA
# Coleta de todos os canais de venda disponíveis via API do Bling (FULL).
# Não há suporte a incremental: sempre busca todos os registros.
# Inclui logs de depuração por ID (quando DEBUG=True) e resumo da coleta.
# Realiza upsert em lote diretamente na tabela staging.

if RODAR_CANAIS_VENDA:
    ENT = "CANAIS VENDA"
    log_etl(ENT, "INÍCIO", "Carga de canais de venda iniciada (FULL)")
    id_log = iniciar_log_etl(db_uri, tabela="canais_venda_bling", acao="extracao")
    tempo_inicio = time.time()
    try:
        # Sempre FULL, sem filtro de datas
        canais_venda = api.get_all_paginated("canais-venda", data_path=['data'])

        if DEBUG:
            log_etl(ENT, "DEBUG", f"Total canais recebidos: {len(canais_venda)}")
            for idx, c in enumerate(canais_venda, 1):
                log_etl(ENT, "DEBUG", f"ID {idx}: {c.get('id')}")

        log_etl(ENT, "API", "Dados coletados da API", quantidade=len(canais_venda))

        lista_mapeada = []
        for idx, c in enumerate(canais_venda, 1):
            try:
                mapped = map_canais_venda(c)
                lista_mapeada.append(mapped)
            except Exception as erro:
                erro_msg = f"Falha ao mapear canal de venda idx {idx}: {erro}"
                if DEBUG:
                    log_etl(ENT, "WARN", erro=erro_msg)
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="canais_venda",
                    id_referencia=c.get("id"),
                    erro=erro_msg,
                    id_log=id_log
                )

        if lista_mapeada:
            upsert_canais_venda_bling_bulk(lista_mapeada, db_uri)
            log_etl(ENT, "DB", "Inseridos/atualizados no banco", quantidade=len(lista_mapeada))

        finalizar_log_etl(db_uri, id_log, status="finalizado")
        log_etl(ENT, "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio))

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="canais_venda",
            tabela_fisica="stg.canais_venda_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now(),
            suporte_incremental='N'
        )

    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log, status="erro", mensagem_erro=str(e))
        registrar_falha_importacao(
            db_uri=db_uri,
            entidade="canais_venda",
            id_referencia=None,
            erro=str(e),
            id_log=id_log
        )
        raise
else:
    log_etl("CANAIS VENDA", "DESLIGADA", "Carga de canais de venda está desligada (RODAR_CANAIS_VENDA = False)")



# %% VENDEDOR
# VENDEDOR
# Coleta incremental de vendedores via API do Bling.
# Suporta FULL (data inicial e final fixadas) ou INCREMENTAL (baseado em dt_ultima_carga).
# Implementa paginação (100 IDs por página), coleta detalhada por ID e upsert em lote.
# Inclui logs da janela de datas usada, quantidade de IDs por página, e falhas por vendedor.

if RODAR_VENDEDOR:
    ENT = "VENDEDORES"
    log_etl(ENT, "INÍCIO", "Carga de vendedores iniciada")
    id_log = iniciar_log_etl(db_uri, tabela="vendedor_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        if CARGA_FULL:
            dt_ini = BLING_FULL_INICIO.strftime("%Y-%m-%d") if isinstance(BLING_FULL_INICIO, datetime) else str(BLING_FULL_INICIO)[:10]
            dt_fim = BLING_FULL_FIM.strftime("%Y-%m-%d") if isinstance(BLING_FULL_FIM, datetime) else str(BLING_FULL_FIM)[:10]
            etapa = "carga_full"
        else:
            dt_ini, dt_fim = get_data_periodo_incremental(
                db_uri,
                "stg.vendedor_bling",
                "api_to_stg",
                MARGEM_DIAS_INCREMENTO,
                DATA_FULL_INICIAL
            )
            etapa = "api_to_stg"

        # Janela usada para filtro (sempre logada)
        params_base = {"dataInicial": dt_ini, "dataFinal": dt_fim}
        log_etl(ENT, "INFO", f"Janela de carga definida: De {dt_ini} até {dt_fim}")

        # Tradução para parâmetros aceitos pela API Bling
        params_api = {
            "dataAlteracaoInicial": params_base["dataInicial"],
            "dataAlteracaoFinal": params_base["dataFinal"]
        }

        pagina = 1
        limite = 100
        total_inseridos = 0
        maior_data = None

        while True:
            ids_vendedores = api.get_vendedores_ids_pagina(pagina, limit=limite, params=params_api)
            if not ids_vendedores:
                break

            if DEBUG:
                log_etl(ENT, "DEBUG", f"Página {pagina}: {len(ids_vendedores)} IDs coletados.")

            vendedores_detalhados = []
            for idx, id_vend in enumerate(ids_vendedores, 1):
                if DEBUG:
                    log_etl(ENT, "DEBUG", f"ID {((pagina-1)*limite)+idx}: {id_vend}")
                try:
                    resp = api.get_vendedor_por_id(id_vend)
                    if resp and "data" in resp:
                        vendedores_detalhados.append(resp["data"])
                    else:
                        erro_msg = f"Vendedor ID {id_vend} não retornou detalhes ou veio vazio."
                        log_etl(ENT, "WARN", erro=erro_msg)
                        registrar_falha_importacao(
                            db_uri=db_uri,
                            entidade="vendedor",
                            id_referencia=id_vend,
                            erro=erro_msg,
                            id_log=id_log
                        )
                except Exception as erro:
                    erro_msg = f"Erro ao buscar vendedor ID {id_vend}: {erro}"
                    log_etl(ENT, "WARN", erro=erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="vendedor",
                        id_referencia=id_vend,
                        erro=erro_msg,
                        id_log=id_log
                    )
                time.sleep(0.35)

            log_etl(ENT, "API", f"Detalhes coletados da página {pagina}", quantidade=len(vendedores_detalhados))

            vendedores_mapeados = map_vendedores(vendedores_detalhados)
            upsert_vendedores_bling_bulk(db_uri, vendedores_mapeados)
            log_etl(ENT, "DB", f"Inseridos/atualizados no banco (página {pagina})", quantidade=len(vendedores_mapeados))

            total_inseridos += len(vendedores_mapeados)
            pagina_maior_data = max(
                [p["dt_atualizacao"] for p in vendedores_mapeados if p.get("dt_atualizacao")],
                default=None
            )
            if pagina_maior_data and (maior_data is None or pagina_maior_data > maior_data):
                maior_data = pagina_maior_data

            pagina += 1

        finalizar_log_etl(db_uri, id_log, status="finalizado")
        log_etl(ENT, "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="vendedor",
            tabela_fisica="stg.vendedor_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S'
        )

    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("VENDEDOR", "DESLIGADA", "Carga de vendedores está desligada (RODAR_VENDEDOR = False)")



# %% CONTATO
# CONTATO
# Coleta incremental de contatos via API do Bling.
# Suporta FULL (data inicial e final fixadas) ou INCREMENTAL (baseado em dt_ultima_carga).
# Implementa paginação (100 IDs por página), coleta detalhada por ID e upsert em blocos de 20.
# Inclui logs da janela de datas usada, quantidade de IDs por página e falhas por contato.

if RODAR_CONTATO:
    ENT = "CONTATO"
    log_etl(ENT, "INÍCIO", "Carga de contatos iniciada")
    id_log = iniciar_log_etl(db_uri, tabela="contato_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        etapa = "carga_full" if CARGA_FULL else "api_to_stg"

        if CARGA_FULL:
            dt_ini = BLING_FULL_INICIO.strftime("%Y-%m-%d") if isinstance(BLING_FULL_INICIO, datetime) else str(BLING_FULL_INICIO)[:10]
            dt_fim = BLING_FULL_FIM.strftime("%Y-%m-%d") if isinstance(BLING_FULL_FIM, datetime) else str(BLING_FULL_FIM)[:10]
        else:
            dt_ini, dt_fim = get_data_periodo_incremental(
                db_uri,
                "stg.contato_bling",
                etapa,
                MARGEM_DIAS_INCREMENTO,
                DATA_FULL_INICIAL
            )

        # Usa nomes de campos corretos da API de contatos
        params_base = montar_filtro_contatos(dt_ini, dt_fim, etapa)

        if DEBUG:
            log_etl(ENT, "DEBUG", f"Janela usada: {params_base}")

        pagina = 199
        limite = 100
        total_inseridos = 0
        buffer = []
        BATCH_SIZE = 20
        MAX_PAGES = None  # opcional para testes

        while True:
            ids_contatos = api.get_contatos_ids_pagina(pagina, limit=limite, params=params_base)
            if not ids_contatos:
                break

            if DEBUG:
                log_etl(ENT, "DEBUG", f"Página {pagina}: {len(ids_contatos)} IDs coletados.")

            for idx, id_bling in enumerate(ids_contatos, 1):
                if DEBUG:
                    log_etl(ENT, "DEBUG", f"ID {((pagina-1)*limite)+idx}: {id_bling}")
                try:
                    detalhe = api.get_contato_por_id(id_bling)
                    if detalhe:
                        mapped = map_contato(detalhe)
                        # sobrescreve dt_atualizacao com timestamp atual (segurança)
                        mapped["dt_atualizacao"] = datetime.now()
                        buffer.append(mapped)

                        if len(buffer) >= BATCH_SIZE:
                            total_inseridos += flush_buffer(
                                db_uri=db_uri,
                                buffer=buffer,
                                upsert_fn=upsert_contato_bling_bulk,
                                batch_size=BATCH_SIZE,
                                ent_label=ENT,
                                log_fn=log_etl
                            )
                    else:
                        erro_msg = f"Contato ID {id_bling} não retornou detalhe ou veio vazio."
                        if DEBUG:
                            log_etl(ENT, "WARN", erro=erro_msg)
                        registrar_falha_importacao(
                            db_uri=db_uri,
                            entidade="contato",
                            id_referencia=id_bling,
                            erro=erro_msg,
                            id_log=id_log
                        )
                except Exception as erro:
                    erro_msg = f"Falha ao buscar contato {id_bling}: {erro}"
                    if DEBUG:
                        log_etl(ENT, "WARN", erro=erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="contato",
                        id_referencia=id_bling,
                        erro=erro_msg,
                        id_log=id_log
                    )
                time.sleep(0.35)

            log_etl(ENT, "API", f"Detalhes coletados da página {pagina}", quantidade=len(ids_contatos))

            if MAX_PAGES and pagina >= MAX_PAGES:
                break
            pagina += 1

        total_inseridos += flush_buffer(
            db_uri=db_uri,
            buffer=buffer,
            upsert_fn=upsert_contato_bling_bulk,
            batch_size=BATCH_SIZE,
            ent_label=ENT,
            log_fn=log_etl
        )

        finalizar_log_etl(db_uri, id_log, status="finalizado")
        log_etl(ENT, "FIM", "Carga finalizada", tempo=(time.time() - tempo_inicio), quantidade=total_inseridos)

        atualizar_controle_carga(
            db_uri=db_uri,
            entidade="contato",
            tabela_fisica="stg.contato_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S' if not CARGA_FULL else 'N'
        )

    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("CONTATO", "DESLIGADA", "Carga de contatos está desligada (RODAR_CONTATO = False)")


# %% CATEGORIA RECEITA/DESPESA
# CATEGORIA RECEITA/DESPESA
# Carga completa (FULL) de categorias financeiras via API do Bling.
# Implementa paginação (100 IDs por página), coleta detalhada por página e upsert direto.
# Inclui logs de quantidade de IDs por página, detalhes mapeados e falhas no mapeamento.

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
                    lista_mapeada.append(map_categoria_receita_despesa(it))
                except Exception as erro:
                    erro_msg = f"Falha ao mapear categoria: {erro}"
                    if DEBUG:
                        log_etl(ENT, "WARN", erro=erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="categoria_receita_despesa",
                        id_referencia=it.get("id"),
                        erro=erro_msg,
                        id_log=id_log_cat_fin
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
            db_uri=db_uri,
            entidade="financeiro",
            tabela_fisica="stg.categoria_receita_despesa_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='N'
        )

    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log_cat_fin, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("CAT_REC_DESP", "DESLIGADA", "Carga está desligada (RODAR_CATEGORIAS_RECEITAS_DESPESAS = False)")


# %% PEDIDO DE VENDA
# PEDIDO DE VENDA
# Coleta de pedidos de venda com suporte a carga FULL ou incremental.
# - Usa janela de datas obtida do controle de carga (ou definida via variáveis de FULL).
# - Implementa paginação (100 IDs por página) e busca detalhada por pedido.
# - Faz upsert em lotes de 20 registros usando buffer + flush_buffer().
# - Logs padronizados indicam janela usada, IDs por página, falhas de mapeamento e totais inseridos.

if RODAR_PEDIDOS_VENDAS:
    ENT = "PEDIDOS_VENDAS"
    log_etl(ENT, "INÍCIO", "Carga de pedidos de venda iniciada")
    id_log = iniciar_log_etl(db_uri, tabela="pedido_venda_bling", acao="extracao")
    tempo_inicio = time.time()

    try:
        etapa = "carga_full" if CARGA_FULL else "api_to_stg"

        if CARGA_FULL:
            dt_ini = BLING_FULL_INICIO.strftime("%Y-%m-%d") if isinstance(BLING_FULL_INICIO, datetime) else str(BLING_FULL_INICIO)[:10]
            dt_fim = BLING_FULL_FIM.strftime("%Y-%m-%d") if isinstance(BLING_FULL_FIM, datetime) else str(BLING_FULL_FIM)[:10]
        else:
            dt_ini, dt_fim = get_data_periodo_incremental(
                db_uri,
                "stg.pedido_venda_bling",
                etapa,
                MARGEM_DIAS_INCREMENTO,
                DATA_FULL_INICIAL
            )
        
        # 🔹 Log da janela de carga (mesmo padrão de CONTATOS)
        log_etl(ENT, "INFO", f"Janela de carga definida: De {dt_ini} até {dt_fim}")

        # Gera filtro com nomes de campos corretos para API
        params_base = montar_filtro_pedidos(dt_ini, dt_fim, etapa)

        if DEBUG:
            log_etl(ENT, "DEBUG", f"Janela usada: {params_base}")


        

        pagina = 1
        limite = 100
        total_inseridos = 0
        buffer = []
        BATCH_SIZE = 20
        MAX_PAGES = None  # defina valor para limitar páginas em testes

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
                        erro_msg = f"Pedido ID {id_ped} não retornou detalhe ou veio vazio."
                        if DEBUG:
                            log_etl(ENT, "WARN", erro=erro_msg)
                        registrar_falha_importacao(
                            db_uri=db_uri,
                            entidade="pedido_venda",
                            id_referencia=id_ped,
                            erro=erro_msg,
                            id_log=id_log
                        )
                except Exception as erro:
                    erro_msg = f"Falha ao buscar pedido {id_ped}: {erro}"
                    if DEBUG:
                        log_etl(ENT, "WARN", erro=erro_msg)
                    registrar_falha_importacao(
                        db_uri=db_uri,
                        entidade="pedido_venda",
                        id_referencia=id_ped,
                        erro=erro_msg,
                        id_log=id_log
                    )
                time.sleep(0.35)

            log_etl(ENT, "API", f"Detalhes coletados da página {pagina}", quantidade=len(ids_pedidos))

            if MAX_PAGES and pagina >= MAX_PAGES:
                break
            pagina += 1

        # Flush final do buffer
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
            db_uri=db_uri,
            entidade="pedido_venda",
            tabela_fisica="stg.pedido_venda_bling",
            etapa="api_to_stg",
            dt_ultima_carga=datetime.now().date(),
            suporte_incremental='S' if not CARGA_FULL else 'N'
        )

    except Exception as e:
        log_etl(ENT, "ERRO", erro=str(e))
        finalizar_log_etl(db_uri, id_log, status="erro", mensagem_erro=str(e))
        raise
else:
    log_etl("PEDIDOS_VENDAS", "DESLIGADA", "Carga de pedidos de venda está desligada (RODAR_PEDIDOS_VENDAS = False)")


# %% FINALIZAÇÃO: CARGA COMPLETA (dimensões + fatos + views)
if RODAR_PIPELINE_CARGA_COMPLETA:
    ENT = "PIPELINE_CARGA_COMPLETA"
    log_etl(ENT, "INÍCIO", "Execução do pipeline de carga completa iniciada")
    id_log = iniciar_log_etl(db_uri, tabela="pipeline_completa", acao="pipeline")
    tempo_inicio = time.time()

    try:
        etapa = "pipeline_completa"
        call_procedure(db_uri, "etl.prc_pipeline_carga_completa")

        tempo_fim = time.time()
        log_etl(ENT, "FIM", f"Pipeline completo executado com sucesso em {tempo_fim - tempo_inicio:.2f} segundos", id_log)
    
    except Exception as e:
        log_etl(ENT, "ERRO", f"Erro ao executar pipeline completo: {str(e)}", id_log)
        raise

# %%
