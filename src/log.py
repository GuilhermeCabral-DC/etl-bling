# region IMPORTS
import psycopg2
from datetime import datetime, timedelta
# endregion

# region LOG DE TROUBLESHOOTING (stdout)
def log_etl(etapa, status=None, mensagem=None, quantidade=None, tempo=None, erro=None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    partes = [f"[{now}]", f"[{etapa}]"]
    if status:
        partes.append(f"[{status}]")
    if mensagem:
        partes.append(mensagem)
    if quantidade is not None:
        partes.append(f"Registros: {quantidade}")
    if tempo is not None:
        partes.append(f"Tempo: {tempo:.1f}s")
    if erro:
        partes.append(f"ERRO: {erro}")
    print(" | ".join(partes))
# endregion

# region LOG INSERIDO NO BANCO
def iniciar_log_etl(db_uri, tabela, acao):
    """
    Insere um registro de início de carga na tabela conf.log.
    Retorna o id_log gerado (para atualizar depois).
    """
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO conf.log (tabela, acao, status, dt_inicio)
                VALUES (%s, %s, %s, %s)
                RETURNING id_log;
            """
            cur.execute(sql, (tabela, acao, 'iniciado', datetime.now()))
            id_log = cur.fetchone()[0]
    return id_log

def finalizar_log_etl(db_uri, id_log, status='finalizado', mensagem_erro=None):
    """
    Atualiza o registro de log iniciado, mudando status para 'finalizado' ou 'erro',
    registrando data/hora do fim e mensagem de erro, se houver.
    """
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                UPDATE conf.log
                   SET status = %s,
                       dt_fim = %s,
                       mensagem_erro = %s
                 WHERE id_log = %s;
            """
            cur.execute(sql, (status, datetime.now(), mensagem_erro, id_log))
# endregion
