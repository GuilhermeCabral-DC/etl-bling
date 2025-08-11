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
def get_data_periodo_incremental(db_uri, entidade, etapa, margem_dias, dt_inicial_full):
    dt_ultima_carga = get_ultima_data_carga(db_uri, entidade, etapa)
    if dt_ultima_carga:
        dt_inicio = dt_ultima_carga - timedelta(days=margem_dias)
    else:
        dt_inicio = dt_inicial_full  # ou uma data bem antiga (primeiro registro do sistema)
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
def registrar_falha_importacao(db_uri, entidade, id_referencia, erro):
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO conf.importacao_falha (entidade, id_referencia, erro)
                VALUES (%s, %s, %s)
            """, (entidade, str(id_referencia), erro))
# endregion

# region CONTROLE DE CARGA
def atualizar_controle_carga(db_uri, entidade, tabela_fisica, etapa, dt_ultima_carga, suporte_incremental='N'):
    """
    Insere ou atualiza o controle de carga para entidade+tabela+etapa.
    """
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO conf.controle_carga 
                    (entidade, tabela_fisica, etapa, dt_ultima_carga, suporte_incremental)
                VALUES 
                    (%s, %s, %s, %s, %s)
                ON CONFLICT (entidade, tabela_fisica, etapa) DO UPDATE
                   SET dt_ultima_carga = EXCLUDED.dt_ultima_carga,
                       suporte_incremental = EXCLUDED.suporte_incremental;
            """
            cur.execute(sql, (entidade, tabela_fisica, etapa, dt_ultima_carga, suporte_incremental))

def get_ultima_data_carga(db_uri, entidade, etapa):
    """
    Retorna a última data de carga registrada para entidade+etapa.
    """
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dt_ultima_carga FROM conf.controle_carga
                WHERE entidade = %s AND etapa = %s
            """, (entidade, etapa))
            row = cur.fetchone()
            return row[0] if row else None
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
