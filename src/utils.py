# region IMPORTS
import psycopg2
from datetime import datetime, timedelta
from src.transformers import map_produtos, map_saldo_produto_deposito
from src.db import upsert_produto_bling_bulk, upsert_saldo_produto_deposito_bulk
from src.date_utils import parse_date_safe
from src.config import (MARGEM_DIAS_INCREMENTO, MARGEM_MINUTOS_DRIFT, DATA_FULL_INICIAL)
from src.log import (log_etl,)

# endregion

# region DEFINE DATA INICIAL E FINAL DO INCREMENTAL
def get_data_periodo_incremental(db_uri, tabela_fisica, etapa, margem_dias, dt_inicial_full):
    dt_ultima_carga = get_ultima_data_carga(db_uri, tabela_fisica, etapa)
    if dt_ultima_carga:
        dt_inicio = dt_ultima_carga - timedelta(days=margem_dias)
    else:
        dt_inicio = dt_inicial_full
    dt_fim = datetime.now()
    return (
        dt_inicio.strftime("%Y-%m-%d"),
        dt_fim.strftime("%Y-%m-%d")
    )

# endregion

# region REPROCESSA FULL
def reprocessar_todas_falhas(api, db_uri):
    import psycopg2
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT entidade, id_referencia FROM conf.importacao_falha
                WHERE processado = FALSE
                ORDER BY dt_falha
            """)
            rows = cur.fetchall()
    if not rows:
        print("[INFO] Nenhuma falha pendente encontrada.")
        return

    print(f"[INFO] {len(rows)} falhas pendentes encontradas.")
    for entidade, id_ref in rows:
        if entidade == "produto":
            from src.utils import reprocessa_produto_por_id
            reprocessa_produto_por_id(id_ref, api, db_uri)
        #elif entidade == "empresa":
        #    from src.utils import reprocessa_empresa_por_id
        #    reprocessa_empresa_por_id(id_ref, api, db_uri)
        # ...adicione mais elif para cada entidade suportada...
        else:
            print(f"[WARN] Entidade '{entidade}' não suportada para reprocessamento.")
# endregion

# region REPROCESSA PRODUTOS POR VÁRIOS IDS
def reprocessa_produtos_por_ids(lista_ids, api, db_uri):
    """
    Reprocessa vários produtos por ID.
    Para cada produto:
      - Busca o detalhe,
      - Faz o upsert no banco,
      - Marca a falha como processada.
    """
    for id_prod in lista_ids:
        try:
            resp = api.get_produto_por_id(id_prod)
            if resp and "data" in resp:
                produto_mapeado = map_produtos([resp["data"]])
                upsert_produto_bling_bulk(db_uri, produto_mapeado)
                log_etl(
                    "PRODUTOS",
                    "INFO",
                    f"Produto {id_prod} reprocessado e inserido/atualizado com sucesso."
                )
                marcar_falha_como_processada(db_uri, "produto", id_prod)
            else:
                log_etl(
                    "PRODUTOS",
                    "WARN",
                    f"Produto ID {id_prod} ainda não retornou detalhes ou veio vazio."
                )
        except Exception as erro:
            log_etl(
                "PRODUTOS",
                "ERRO",
                f"Erro ao reprocessar produto {id_prod}: {erro}"
            )
# endregion

# region MARCA FALHA COMO PROCESSADO EM CASO DE REPROCESSAMENTO
def marcar_falha_como_processada(db_uri, entidade, id_referencia):
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE conf.importacao_falha
                   SET processado = TRUE, dt_falha = NOW()
                 WHERE entidade = %s AND id_referencia = %s AND processado = FALSE
            """, (entidade, str(id_referencia)))
# endregion

# region INSERE FALHAS DE IMPORTACAO NA TABELA conf.importacao_falha
def registrar_falha_importacao(db_uri, entidade, id_referencia, erro, id_log=None):
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO conf.log_detalhes (entidade, id_referencia, erro, id_log)
                VALUES (%s, %s, %s, %s)
            """, (entidade, str(id_referencia), erro, id_log))

# endregion

# region CONTROLE DE CARGA
def atualizar_controle_carga(db_uri, tabela_fisica, etapa, dt_ultima_carga, suporte_incremental='N', entidade=None):
    """
    Insere ou atualiza o controle de carga com base em tabela_fisica + etapa.
    'entidade' é opcional, usada apenas como tag organizacional.
    """
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO conf.controle_carga 
                    (entidade, tabela_fisica, etapa, dt_ultima_carga, suporte_incremental)
                VALUES 
                    (%s, %s, %s, %s, %s)
                ON CONFLICT (tabela_fisica, etapa) DO UPDATE
                   SET dt_ultima_carga = EXCLUDED.dt_ultima_carga,
                       suporte_incremental = EXCLUDED.suporte_incremental,
                       entidade = EXCLUDED.entidade;
            """
            cur.execute(sql, (entidade, tabela_fisica, etapa, dt_ultima_carga, suporte_incremental))



def get_ultima_data_carga(db_uri, tabela_fisica, etapa):
    query = """
        SELECT dt_ultima_carga
          FROM conf.controle_carga
         WHERE tabela_fisica = %s
           AND etapa = %s
         LIMIT 1
    """
    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tabela_fisica, etapa))
            result = cur.fetchone()
            return result[0] if result else None
# endregion

# region UTILITÁRIOS DE DATA/PERÍODOS
def gerar_periodos(dt_inicio, dt_fim, max_dias=365):
    """
    Gera tuplas de períodos (data_inicial, data_final) para dividir uma faixa de datas
    em chunks de no máximo `max_dias` (padrão: 365).
    Útil para APIs que limitam buscas por intervalo de tempo.
    """
    periodos = []
    data_atual = dt_inicio
    while data_atual < dt_fim:
        proxima_data = min(data_atual + timedelta(days=max_dias), dt_fim)
        periodos.append((data_atual, proxima_data))
        data_atual = proxima_data
    return periodos

def obter_data_inicio_carga(db_uri, entidade, tabela_fisica, etapa, carga_full, dias_margem=2, dt_inicial_full=None):
    """
    Retorna a data inicial para busca incremental/full.
    - Se full: retorna dt_inicial_full (padrão 2021-01-01).
    - Se incremental: pega última carga no controle, subtrai margem.
    """
    if carga_full:
        return dt_inicial_full or datetime(2021, 1, 1)
    else:
        dt_inicio = get_ultima_data_carga(db_uri, entidade, etapa)
        if dt_inicio:
            dt_inicio = dt_inicio - timedelta(days=dias_margem)
        else:
            dt_inicio = dt_inicial_full or datetime(2021, 1, 1)
        return dt_inicio

def get_data_periodo_carga(db_uri, entidade, tabela_fisica, etapa, carga_full, data_full_inicial, margem_dias):
    """
    Retorna tupla (dt_inicio, dt_fim) para a carga de dados incremental ou full.
    """
    if carga_full:
        dt_inicio = data_full_inicial
    else:
        dt_inicio = get_ultima_data_carga(db_uri, entidade, etapa)
        if dt_inicio:
            dt_inicio = dt_inicio - timedelta(days=margem_dias)
        else:
            dt_inicio = data_full_inicial
    dt_fim = datetime.now()
    return dt_inicio, dt_fim
# endregion

# region REPROCESSA SALDO DE VÁRIOS PRODUTOS
def reprocessa_saldo_produtos_por_ids(lista_ids, api, db_uri):
    """
    Reprocessa o saldo de vários produtos por ID.
    Para cada produto:
      - Busca o saldo,
      - Faz o upsert no banco,
      - Marca a falha como processada.
    """
    for id_prod in lista_ids:
        try:
            saldo_data = api.get_saldo_produto_por_id(id_prod)
            if saldo_data:
                registros = map_saldo_produto_deposito(saldo_data)
                upsert_saldo_produto_deposito_bulk(db_uri, registros)
                log_etl(
                    "SALDO_PROD_DEP",
                    "INFO",
                    f"Saldo do produto {id_prod} reprocessado e inserido/atualizado com sucesso."
                )
                marcar_falha_como_processada(db_uri, "saldo_produto_deposito", id_prod)
            else:
                log_etl(
                    "SALDO_PROD_DEP",
                    "WARN",
                    f"Produto ID {id_prod} não retornou saldo ou veio vazio."
                )
        except Exception as erro:
            log_etl(
                "SALDO_PROD_DEP",
                "ERRO",
                f"Erro ao reprocessar saldo do produto {id_prod}: {erro}"
            )
# endregion

# region FORMAT DATE TIME
def format_bling_datetime(dt):
    if not dt:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S")
# endregion

# region JANELA INCREMENTAL
def janela_incremental(entidade_nome, carga_full, ultima_execucao, margem_dias, margem_minutos_drift):
    """
    Retorna (dt_inicial, dt_final) já aplicando margens e corte no 'agora - drift'.
    """
    agora = datetime.now()
    dt_final = agora - timedelta(minutes=MARGEM_MINUTOS_DRIFT)
    if carga_full or not ultima_execucao:
        dt_inicial = DATA_FULL_INICIAL
    else:
        dt_inicial = (ultima_execucao - timedelta(days=margem_dias)).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt_inicial, dt_final
# endregion

# region GRAVA O CONTEUDO DO BUFFER E DEPOIS LIMPA
def flush_buffer(db_uri, buffer, upsert_fn, batch_size, ent_label, log_fn):
    """
    Grava o conteúdo do buffer em batch e o esvazia.
    - db_uri: conexão do Postgres
    - buffer: list[dict] com registros já mapeados
    - upsert_fn: função de upsert bulk (ex.: upsert_pedido_venda_bling_bulk)
    - batch_size: tamanho do lote (int)
    - ent_label: rótulo da entidade para logging (ex.: "PEDIDOS_VENDAS")
    - log_fn: função de log (ex.: log_etl)

    Retorna: quantidade gravada (int)
    """
    if not buffer:
        return 0
    upsert_fn(db_uri, buffer, batch_size=batch_size)
    qtd = len(buffer)
    log_fn(ent_label, "DB", "Batch gravado", quantidade=qtd)
    buffer.clear()
    return qtd
# endregion

# region ============= REPROCESSA PEDIDO DE VENDA: LISTA DE IDs (MANUAL) =============
def reprocessa_pedidos_vendas_por_ids(lista_ids, api, db_uri, batch_size=20):
    """
    Reprocessa pedidos de venda por uma lista de IDs.
    - Busca detalhe do pedido na API
    - Mapeia (map_pedido_venda)
    - Faz upsert em batch (upsert_pedido_venda_bling_bulk) usando flush_buffer
    - Marca a falha como processada em conf.importacao_falha
    """
    from src.transformers import map_pedido_venda
    from src.db import upsert_pedido_venda_bling_bulk
    from src.utils import marcar_falha_como_processada, registrar_falha_importacao, flush_buffer
    from src.log import log_etl

    ENT = "PEDIDOS_VENDAS"
    buffer = []
    total_processados = 0

    if not lista_ids:
        log_etl(ENT, "INFO", "Nenhum ID informado para reprocessamento.")
        return

    for pid in lista_ids:
        try:
            det = api.get_pedido_venda_por_id(pid)
            if det:
                registro = map_pedido_venda(det)
                buffer.append(registro)

                if len(buffer) >= batch_size:
                    total_processados += flush_buffer(db_uri, buffer, upsert_pedido_venda_bling_bulk, batch_size, ENT, log_etl)
                    marcar_falha_como_processada(db_uri, "pedido_venda", pid)
            else:
                log_etl(ENT, "WARN", f"ID {pid} não retornou detalhe ou veio vazio.")
                registrar_falha_importacao(
                    db_uri=db_uri,
                    entidade="pedido_venda",
                    id_referencia=pid,
                    erro="Detalhe vazio/None ao reprocessar"
                )
        except Exception as erro:
            log_etl(ENT, "ERRO", f"Erro ao reprocessar pedido {pid}: {erro}")
            registrar_falha_importacao(
                db_uri=db_uri,
                entidade="pedido_venda",
                id_referencia=pid,
                erro=str(erro)
            )

    # Flush final
    total_processados += flush_buffer(db_uri, buffer, upsert_pedido_venda_bling_bulk, batch_size, ENT, log_etl)

    log_etl(ENT, "INFO", f"Reprocessamento manual concluído. Total processados: {total_processados}")
# endregion
