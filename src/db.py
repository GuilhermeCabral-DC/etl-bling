# region INICIAR

import json

 #endregion

# region IMPORTS E CONFIGS
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from src.config import DEBUG  
from src.log import (log_etl,)
# endregion

# region EMPRESA (FULL)
def upsert_empresa_bling_bulk(lista_de_dicts, db_uri):
    """
    Upsert em lote na tabela stg.empresa_bling.
    """
    if not lista_de_dicts:
        return
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO stg.empresa_bling
                    (id_bling, nome, cnpj, email, dt_contrato)
                VALUES
                    (%(id_bling)s, %(nome)s, %(cnpj)s, %(email)s, %(dt_contrato)s)
                ON CONFLICT (id_bling) DO UPDATE
                SET
                    nome = EXCLUDED.nome,
                    cnpj = EXCLUDED.cnpj,
                    email = EXCLUDED.email,
                    dt_contrato = EXCLUDED.dt_contrato,
                    dt_atualizacao = CURRENT_TIMESTAMP
            """
            psycopg2.extras.execute_batch(cur, sql, lista_de_dicts, page_size=1000)
# endregion

# region CATEGORIA DE PRODUTO
def upsert_categoria_produto_bling_bulk(lista_de_dicts, db_uri):
    """
    Upsert em lote na tabela stg.categoria_produto_bling.
    """
    if not lista_de_dicts:
        return
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO stg.categoria_produto_bling
                    (id_bling, descricao, id_pai_bling)
                VALUES
                    (%(id_bling)s, %(descricao)s, %(id_pai_bling)s)
                ON CONFLICT (id_bling) DO UPDATE
                SET
                    descricao = EXCLUDED.descricao,
                    id_pai_bling = EXCLUDED.id_pai_bling,
                    dt_atualizacao = CURRENT_TIMESTAMP
            """
            execute_batch(cur, sql, lista_de_dicts, page_size=1000)
# endregion

# region GRUPO DE PRODUTO
def upsert_grupo_produto_bling_bulk(lista_de_dicts, db_uri):
    """
    Upsert em lote na tabela stg.grupo_produto_bling.
    """
    if not lista_de_dicts:
        return
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO stg.grupo_produto_bling
                    (id_bling, descricao, id_pai_bling)
                VALUES
                    (%(id_bling)s, %(descricao)s, %(id_pai_bling)s)
                ON CONFLICT (id_bling) DO UPDATE
                SET
                    descricao = EXCLUDED.descricao,
                    id_pai_bling = EXCLUDED.id_pai_bling,
                    dt_atualizacao = CURRENT_TIMESTAMP
            """
            execute_batch(cur, sql, lista_de_dicts, page_size=500)
# endregion

# region CANAIS DE VENDA
def upsert_canais_venda_bling_bulk(lista_de_dicts, db_uri):
    """
    Upsert em lote na tabela stg.canais_venda_bling.
    """
    if not lista_de_dicts:
        return
    conn = psycopg2.connect(db_uri)
    with conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO stg.canais_venda_bling
                    (id_bling, descricao, tipo, situacao)
                VALUES
                    (%(id_bling)s, %(descricao)s, %(tipo)s, %(situacao)s)
                ON CONFLICT (id_bling) DO UPDATE
                SET
                    descricao = EXCLUDED.descricao,
                    tipo = EXCLUDED.tipo,
                    situacao = EXCLUDED.situacao,
                    dt_atualizacao = CURRENT_TIMESTAMP
            """
            execute_batch(cur, sql, lista_de_dicts, page_size=1000)
# endregion

# region VENDEDOR
def upsert_vendedores_bling(db_uri, lista_vendedores):
    """
    Insere ou atualiza registros de vendedores na tabela stg.vendedor.
    """
    if not lista_vendedores:
        return

    query = """
        INSERT INTO stg.vendedor_bling (
            id_bling,
            vl_desconto_limite,
            id_loja,
            id_contato,
            nome_contato,
            situacao_contato,
            comissoes,
            dt_carga,
            dt_atualizacao
        )
        VALUES %s
        ON CONFLICT (id_bling) DO UPDATE SET
            vl_desconto_limite = EXCLUDED.vl_desconto_limite,
            id_loja = EXCLUDED.id_loja,
            id_contato = EXCLUDED.id_contato,
            nome_contato = EXCLUDED.nome_contato,
            situacao_contato = EXCLUDED.situacao_contato,
            comissoes = EXCLUDED.comissoes,
            dt_carga = EXCLUDED.dt_carga,
            dt_atualizacao = EXCLUDED.dt_atualizacao;
    """

    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, query,
                [(
                    v["id_bling"],
                    v["vl_desconto_limite"],
                    v["id_loja"],
                    v["id_contato"],
                    v["nome_contato"],
                    v["situacao_contato"],
                    psycopg2.extras.Json(v["comissoes"]),
                    v["dt_carga"],
                    v["dt_atualizacao"]
                ) for v in lista_vendedores]
            )
# endregion

# region PRODUTO (FULL OU INCREMENTAL)
def upsert_produto_bling_bulk(db_uri, lista_produtos, batch_size=100):
    """
    Insere ou atualiza registros de produtos na tabela stg.produto_bling, em batches/lotes.
    """
    if not lista_produtos:
        return

    query = """
        INSERT INTO stg.produto_bling (
            id_bling,
            nome,
            codigo,
            tipo,
            situacao,
            formato,
            descricao_curta,
            descricao_complementar,
            observacoes,
            imagem_url,
            dt_validade,
            unidade,
            qt_itens_caixa,
            qt_volumes,
            vl_preco,
            vl_preco_custo,
            vl_peso_liquido,
            vl_peso_bruto,
            largura,
            altura,
            profundidade,
            unidade_medida,
            gtin,
            gtin_embalagem,
            tipo_producao,
            condicao,
            marca,
            id_categoria,
            id_linha_produto,
            id_fornecedor,
            id_grupo_produto,
            ncm,
            cest,
            origem,
            sped_tipo_item,
            dt_carga,
            dt_atualizacao
        )
        VALUES %s
        ON CONFLICT (id_bling) DO UPDATE SET
            nome = EXCLUDED.nome,
            codigo = EXCLUDED.codigo,
            tipo = EXCLUDED.tipo,
            situacao = EXCLUDED.situacao,
            formato = EXCLUDED.formato,
            descricao_curta = EXCLUDED.descricao_curta,
            descricao_complementar = EXCLUDED.descricao_complementar,
            observacoes = EXCLUDED.observacoes,
            imagem_url = EXCLUDED.imagem_url,
            dt_validade = EXCLUDED.dt_validade,
            unidade = EXCLUDED.unidade,
            qt_itens_caixa = EXCLUDED.qt_itens_caixa,
            qt_volumes = EXCLUDED.qt_volumes,
            vl_preco = EXCLUDED.vl_preco,
            vl_preco_custo = EXCLUDED.vl_preco_custo,
            vl_peso_liquido = EXCLUDED.vl_peso_liquido,
            vl_peso_bruto = EXCLUDED.vl_peso_bruto,
            largura = EXCLUDED.largura,
            altura = EXCLUDED.altura,
            profundidade = EXCLUDED.profundidade,
            unidade_medida = EXCLUDED.unidade_medida,
            gtin = EXCLUDED.gtin,
            gtin_embalagem = EXCLUDED.gtin_embalagem,
            tipo_producao = EXCLUDED.tipo_producao,
            condicao = EXCLUDED.condicao,
            marca = EXCLUDED.marca,
            id_categoria = EXCLUDED.id_categoria,
            id_linha_produto = EXCLUDED.id_linha_produto,
            id_fornecedor = EXCLUDED.id_fornecedor,
            id_grupo_produto = EXCLUDED.id_grupo_produto,
            ncm = EXCLUDED.ncm,
            cest = EXCLUDED.cest,
            origem = EXCLUDED.origem,
            sped_tipo_item = EXCLUDED.sped_tipo_item,
            dt_carga = stg.produto_bling.dt_carga,
            dt_atualizacao = EXCLUDED.dt_atualizacao;
    """

    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            agora = datetime.now()
            total = len(lista_produtos)
            for i in range(0, total, batch_size):
                batch = lista_produtos[i:i+batch_size]
                psycopg2.extras.execute_values(
                    cur, query,
                    [(
                        p["id_bling"],
                        p["nome"],
                        p["codigo"],
                        p["tipo"],
                        p["situacao"],
                        p["formato"],
                        p["descricao_curta"],
                        p["descricao_complementar"],
                        p["observacoes"],
                        p["imagem_url"],
                        p["dt_validade"],
                        p["unidade"],
                        p["qt_itens_caixa"],
                        p["qt_volumes"],
                        p["vl_preco"],
                        p["vl_preco_custo"],
                        p["vl_peso_liquido"],
                        p["vl_peso_bruto"],
                        p["largura"],
                        p["altura"],
                        p["profundidade"],
                        p["unidade_medida"],
                        p["gtin"],
                        p["gtin_embalagem"],
                        p["tipo_producao"],
                        p["condicao"],
                        p["marca"],
                        p["id_categoria"],
                        p["id_linha_produto"],
                        p["id_fornecedor"],
                        p["id_grupo_produto"],
                        p["ncm"],
                        p["cest"],
                        p["origem"],
                        p["sped_tipo_item"],
                        agora,                   # dt_carga
                        agora
                    ) for p in batch]
                )
                if DEBUG:
                    log_etl(
                        "PRODUTOS",
                        "DEBUG",
                        f"Batch {i//batch_size + 1}: {len(batch)} produtos inseridos/atualizados."
                    )
# endregion

# region DEPÓSITO (FULL)

def upsert_deposito_bling_bulk(lista_depositos, db_uri):
    """
    Insere ou atualiza depósitos em lote na tabela stg.deposito_bling,
    respeitando o padrão: dt_carga só na primeira vez, dt_atualizacao sempre.
    """
    import psycopg2
    from psycopg2.extras import execute_values
    from datetime import datetime

    if not lista_depositos:
        return

    now = datetime.now()
    conn = psycopg2.connect(db_uri)
    cur = conn.cursor()
    sql = """
    INSERT INTO stg.deposito_bling (
        id_bling, descricao, situacao, padrao, desconsiderar_saldo, dt_carga, dt_atualizacao
    ) VALUES %s
    ON CONFLICT (id_bling) DO UPDATE SET
        descricao = EXCLUDED.descricao,
        situacao = EXCLUDED.situacao,
        padrao = EXCLUDED.padrao,
        desconsiderar_saldo = EXCLUDED.desconsiderar_saldo,
        -- dt_carga NÃO É ATUALIZADO AQUI
        dt_atualizacao = EXCLUDED.dt_atualizacao;
    """
    values = [
        (
            d["id_bling"],
            d["descricao"],
            d["situacao"],
            d["padrao"],
            d["desconsiderar_saldo"],
            now,              # dt_carga: só vai na primeira vez!
            now               # dt_atualizacao: sempre atualizado
        )
        for d in lista_depositos
    ]
    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    conn.close()

# endregion

# region SALDO PRODUTO (FULL)

def upsert_saldo_produto_deposito_bulk(db_uri, lista_registros, batch_size=100):
    """
    Insere ou atualiza registros de saldo produto x deposito, em batch.
    """
    if not lista_registros:
        return

    query = """
        INSERT INTO stg.saldo_produto_deposito_bling (
            id_produto_bling, id_deposito_bling, saldo_fisico, saldo_virtual, dt_carga, dt_atualizacao
        )
        VALUES %s
        ON CONFLICT (id_produto_bling, id_deposito_bling) DO UPDATE SET
            saldo_fisico = EXCLUDED.saldo_fisico,
            saldo_virtual = EXCLUDED.saldo_virtual,
            dt_carga = stg.saldo_produto_deposito_bling.dt_carga,
            dt_atualizacao = EXCLUDED.dt_atualizacao;
    """

    import psycopg2, psycopg2.extras
    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            total = len(lista_registros)
            for i in range(0, total, batch_size):
                batch = lista_registros[i:i+batch_size]
                psycopg2.extras.execute_values(
                    cur, query,
                    [(r["id_produto_bling"], r["id_deposito_bling"], r["saldo_fisico"],
                      r["saldo_virtual"], r["dt_carga"], r["dt_atualizacao"]) for r in batch]
                )
# endregion

# region PEDIDOS: INSERIR/ATUALIZAR (BULK)
def upsert_pedido_venda_bling_bulk(db_uri, registros, batch_size=20):
    if not registros:
        return

    query = """
        INSERT INTO stg.pedido_venda_bling (
            id_bling, numero, numero_loja, data_pedido, data_saida, data_prevista,
            total_produtos, total,
            id_contato, contato_nome, contato_tipo_pessoa, contato_documento,
            id_situacao, situacao_valor, id_loja, numero_pedido_compra,
            outras_despesas, observacoes, observacoes_internas,
            desconto_valor, desconto_unidade, id_categoria_financeira, id_nota_fiscal,
            trib_total_icms, trib_total_ipi,
            itens_json, parcelas_json, transporte_json, taxas_json,
            id_vendedor, intermediador_cnpj, intermediador_usuario,
            dt_carga, dt_atualizacao
        ) VALUES %s
        ON CONFLICT (id_bling) DO UPDATE SET
            numero = EXCLUDED.numero,
            numero_loja = EXCLUDED.numero_loja,
            data_pedido = EXCLUDED.data_pedido,
            data_saida = EXCLUDED.data_saida,
            data_prevista = EXCLUDED.data_prevista,
            total_produtos = EXCLUDED.total_produtos,
            total = EXCLUDED.total,
            id_contato = EXCLUDED.id_contato,
            contato_nome = EXCLUDED.contato_nome,
            contato_tipo_pessoa = EXCLUDED.contato_tipo_pessoa,
            contato_documento = EXCLUDED.contato_documento,
            id_situacao = EXCLUDED.id_situacao,
            situacao_valor = EXCLUDED.situacao_valor,
            id_loja = EXCLUDED.id_loja,
            numero_pedido_compra = EXCLUDED.numero_pedido_compra,
            outras_despesas = EXCLUDED.outras_despesas,
            observacoes = EXCLUDED.observacoes,
            observacoes_internas = EXCLUDED.observacoes_internas,
            desconto_valor = EXCLUDED.desconto_valor,
            desconto_unidade = EXCLUDED.desconto_unidade,
            id_categoria_financeira = EXCLUDED.id_categoria_financeira,
            id_nota_fiscal = EXCLUDED.id_nota_fiscal,
            trib_total_icms = EXCLUDED.trib_total_icms,
            trib_total_ipi = EXCLUDED.trib_total_ipi,
            itens_json = EXCLUDED.itens_json,
            parcelas_json = EXCLUDED.parcelas_json,
            transporte_json = EXCLUDED.transporte_json,
            taxas_json = EXCLUDED.taxas_json,
            id_vendedor = EXCLUDED.id_vendedor,
            intermediador_cnpj = EXCLUDED.intermediador_cnpj,
            intermediador_usuario = EXCLUDED.intermediador_usuario,
            dt_carga = stg.pedido_venda_bling.dt_carga,
            dt_atualizacao = EXCLUDED.dt_atualizacao;
    """

    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            total = len(registros)
            for i in range(0, total, batch_size):
                batch = registros[i:i+batch_size]
                psycopg2.extras.execute_values(
                    cur, query,
                    [(
                        r["id_bling"], r["numero"], r["numero_loja"], r["data_pedido"], r["data_saida"], r["data_prevista"],
                        r["total_produtos"], r["total"],
                        r["id_contato"], r["contato_nome"], r["contato_tipo_pessoa"], r["contato_documento"],
                        r["id_situacao"], r["situacao_valor"], r["id_loja"], r["numero_pedido_compra"],
                        r["outras_despesas"], r["observacoes"], r["observacoes_internas"],
                        r["desconto_valor"], r["desconto_unidade"], r["id_categoria_financeira"], r["id_nota_fiscal"],
                        r["trib_total_icms"], r["trib_total_ipi"],
                        json.dumps(r["itens_json"]), json.dumps(r["parcelas_json"]),
                        json.dumps(r["transporte_json"]), json.dumps(r["taxas_json"]),
                        r["id_vendedor"], r["intermediador_cnpj"], r["intermediador_usuario"],
                        r["dt_carga"], r["dt_atualizacao"]
                    ) for r in batch]
                )
                if DEBUG:
                    log_etl("PEDIDOS_VENDAS", "DEBUG",
                            f"Batch {i//batch_size + 1}: {len(batch)} pedidos inseridos/atualizados.")
# endregion

# region CATEGORIA RECEITA/DESPESA (FULL)
def upsert_categoria_receita_despesa_bling_bulk(lista_de_dicts, db_uri, batch_size=1000):
    """
    Upsert em lote na tabela stg.categoria_receita_despesa_bling.
    """
    if not lista_de_dicts:
        return

    sql = """
        INSERT INTO stg.categoria_receita_despesa_bling (
            id_bling, id_categoria_pai, descricao, tipo, dt_carga, dt_atualizacao
        )
        VALUES %s
        ON CONFLICT (id_bling) DO UPDATE SET
            id_categoria_pai = EXCLUDED.id_categoria_pai,
            descricao        = EXCLUDED.descricao,
            tipo             = EXCLUDED.tipo,
            dt_carga         = stg.categoria_receita_despesa_bling.dt_carga,
            dt_atualizacao   = EXCLUDED.dt_atualizacao;
    """

    with psycopg2.connect(db_uri) as conn:
        with conn.cursor() as cur:
            # execute_values é mais rápido para muitos registros
            psycopg2.extras.execute_values(
                cur, sql,
                [(
                    r["id_bling"],
                    r["id_categoria_pai"],
                    r["descricao"],
                    r["tipo"],
                    r.get("dt_carga"),
                    r.get("dt_atualizacao"),
                ) for r in lista_de_dicts],
                page_size=batch_size
            )
# endregion

