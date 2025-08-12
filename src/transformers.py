# region IMPORTS E UTILS
import json
from datetime import datetime
from src.date_utils import parse_date_safe
# endregion

# region MAPEAR EMPRESA
def map_empresa(data):
    return {
        "id_bling": data.get("id"),
        "nome": data.get("nome"),
        "cnpj": data.get("cnpj"),
        "email": data.get("email"),
        "dt_contrato": data.get("dataContrato")
    }
# endregion

# region MAPEAR CATEGORIA PRODUTO
def map_categoria_produto(data):
    return {
        "id_bling": data.get("id"),
        "descricao": data.get("descricao"),
        "id_pai_bling": data.get("categoriaPai", {}).get("id") if data.get("categoriaPai") else None
    }
# endregion

# region MAPEAR PRODUTOS
def map_produtos(lista_detalhes):
    """
    Transforma uma lista de produtos detalhados da API do Bling
    para o formato compatível com a tabela dim.produto.
    """
    produtos = []
    for item in lista_detalhes:
        if not item or not item.get("id"):   # <- MELHOR, garante que não será inserido NULL no id_bling
            continue

        tributacao = item.get("tributacao", {})
        dimensoes = item.get("dimensoes", {})
        fornecedor = item.get("fornecedor", {})
        midia = item.get("midia", {})
        imagens = midia.get("imagens", {}).get("internas", []) + midia.get("imagens", {}).get("externas", [])
        imagem_url = imagens[0].get("link") if imagens else item.get("imagemURL")

        produtos.append({
            "id_bling": item.get("id"),
            "nome": item.get("nome"),
            "codigo": item.get("codigo"),
            "tipo": item.get("tipo"),
            "situacao": item.get("situacao"),
            "formato": item.get("formato"),
            "descricao_curta": item.get("descricaoCurta"),
            "descricao_complementar": item.get("descricaoComplementar"),
            "observacoes": item.get("observacoes"),
            "imagem_url": imagem_url,

            "dt_validade": parse_date_safe(item.get("dataValidade")),
            "unidade": item.get("unidade"),
            "qt_itens_caixa": item.get("itensPorCaixa"),
            "qt_volumes": item.get("volumes"),

            "vl_preco": item.get("preco"),
            "vl_preco_custo": fornecedor.get("precoCusto"),
            "vl_peso_liquido": item.get("pesoLiquido"),
            "vl_peso_bruto": item.get("pesoBruto"),

            "largura": dimensoes.get("largura"),
            "altura": dimensoes.get("altura"),
            "profundidade": dimensoes.get("profundidade"),
            "unidade_medida": dimensoes.get("unidadeMedida"),

            "gtin": item.get("gtin"),
            "gtin_embalagem": item.get("gtinEmbalagem"),

            "tipo_producao": item.get("tipoProducao"),
            "condicao": item.get("condicao"),
            "marca": item.get("marca"),

            "id_categoria": item.get("categoria", {}).get("id"),
            "id_linha_produto": item.get("linhaProduto", {}).get("id"),
            "id_fornecedor": fornecedor.get("id"),
            "id_grupo_produto": tributacao.get("grupoProduto", {}).get("id"),

            "ncm": tributacao.get("ncm"),
            "cest": tributacao.get("cest"),
            "origem": tributacao.get("origem"),
            "sped_tipo_item": tributacao.get("spedTipoItem"),

            # Campo incremental!
            "dt_atualizacao": parse_date_safe(item.get("dataAlteracaoFinal")),
        })
    return produtos
# endregion

# region MAPEAR GRUPO PRODUTO
def map_grupo_produto(data):
    return {
        "id_bling": data.get("id"),
        "descricao": data.get("nome"),
        "id_pai_bling": data.get("grupoProdutoPai", {}).get("id") if data.get("grupoProdutoPai") else None
    }
# endregion

# region MAPEAR CANAIS VENDA
def map_canais_venda(data):
    return {
        "id_bling": data.get("id"),
        "descricao": data.get("descricao"),
        "tipo": data.get("tipo"),
        "situacao": data.get("situacao")
    }
# endregion

# region MAPEAR VENDEDORES
def map_vendedores(json_vendedor):
    """
    Transforma o JSON de /vendedores/{id} no formato da tabela stg.vendedor_bling.
    """
    if not json_vendedor:
        return None

    return {
        "id_bling": json_vendedor.get("id"),
        "vl_desconto_limite": json_vendedor.get("descontoLimite"),
        "id_loja": json_vendedor.get("loja", {}).get("id"),
        "id_contato": json_vendedor.get("contato", {}).get("id"),
        "nome_contato": json_vendedor.get("contato", {}).get("nome"),
        "situacao_contato": json_vendedor.get("contato", {}).get("situacao"),
        "comissoes": json_vendedor.get("comissoes", []),
        "dt_carga": datetime.now(),
        "dt_atualizacao": parse_date_safe(json_vendedor.get("dataAlteracaoFinal"))
    }
# endregion

# region UTILITÁRIO: DATA SEGURA
def _safe_date(data_str):
    if data_str in [None, "", "0000-00-00", "0000-00-00 00:00:00"]:
        return None
    return data_str  # ou converter para datetime se necessário
# endregion

# region MAPEAR DEPOSITOS

def map_deposito(data):
    return {
        "id_bling": data.get("id"),
        "descricao": data.get("descricao"),
        "situacao": data.get("situacao"),
        "padrao": data.get("padrao"),
        "desconsiderar_saldo": data.get("desconsiderarSaldo"),
        # "dt_carga": preenchido no insert, não no mapeamento!
        "dt_atualizacao": datetime.now()  # ou pode vir do dado, se houver campo na resposta
    }

# endregion

# region MAPEAR SALDO PRODUTO

def map_saldo_produto_deposito(api_data):
    """
    Transforma o saldo de um produto da API em linhas para o banco.
    Entrada: lista (normalmente com 1 elemento), saída: lista de dicts.
    """
    from datetime import datetime
    now = datetime.now()
    registros = []
    for item in api_data:
        id_produto = item.get("produto", {}).get("id")
        for dep in item.get("depositos", []):
            registros.append({
                "id_produto_bling": id_produto,
                "id_deposito_bling": dep.get("id"),
                "saldo_fisico": dep.get("saldoFisico"),
                "saldo_virtual": dep.get("saldoVirtual"),
                "dt_carga": now,
                "dt_atualizacao": now
            })
    return registros

# endregion

# region MAPEAR PEDIDO DE VENDA
def map_pedido_venda(api_obj):
    """
    Recebe o 'data' de /pedidos/vendas/{id} e devolve um dict compatível com stg.pedido_venda_bling.
    """
    from datetime import datetime
    now = datetime.now()

    contato = api_obj.get("contato") or {}
    situacao = api_obj.get("situacao") or {}
    loja = api_obj.get("loja") or {}
    desconto = api_obj.get("desconto") or {}
    categoria = api_obj.get("categoria") or {}
    nf = api_obj.get("notaFiscal") or {}
    trib = api_obj.get("tributacao") or {}
    vendedor = api_obj.get("vendedor") or {}
    intermediador = api_obj.get("intermediador") or {}
    taxas = api_obj.get("taxas") or {}

    return {
        "id_bling":              api_obj.get("id"),
        "numero":                api_obj.get("numero"),
        "numero_loja":           api_obj.get("numeroLoja"),
        "data_pedido":           parse_date_safe(api_obj.get("data")),
        "data_saida":            parse_date_safe(api_obj.get("dataSaida")),
        "data_prevista":         parse_date_safe(api_obj.get("dataPrevista")),
        "total_produtos":        api_obj.get("totalProdutos"),
        "total":                 api_obj.get("total"),

        "id_contato":            contato.get("id"),
        "contato_nome":          contato.get("nome"),
        "contato_tipo_pessoa":   contato.get("tipoPessoa"),
        "contato_documento":     contato.get("numeroDocumento"),

        "id_situacao":           situacao.get("id"),
        "situacao_valor":        situacao.get("valor"),

        "id_loja":               loja.get("id"),
        "numero_pedido_compra":  api_obj.get("numeroPedidoCompra"),

        "outras_despesas":       api_obj.get("outrasDespesas"),
        "observacoes":           api_obj.get("observacoes"),
        "observacoes_internas":  api_obj.get("observacoesInternas"),

        "desconto_valor":        desconto.get("valor"),
        "desconto_unidade":      desconto.get("unidade"),

        "id_categoria_financeira": categoria.get("id"),
        "id_nota_fiscal":        nf.get("id"),

        "trib_total_icms":       trib.get("totalICMS"),
        "trib_total_ipi":        trib.get("totalIPI"),

        "itens_json":            api_obj.get("itens") or [],
        "parcelas_json":         api_obj.get("parcelas") or [],
        "transporte_json":       api_obj.get("transporte") or {},
        "taxas_json":            taxas,

        "id_vendedor":           vendedor.get("id"),
        "intermediador_cnpj":    intermediador.get("cnpj"),
        "intermediador_usuario": intermediador.get("nomeUsuario"),

        "dt_carga":              now,
        "dt_atualizacao":        now,
    }
# endregion

# region MAPEAR CATEGORIA RECEITA/DESPESA
def map_categoria_receita_despesa(item):
    """
    Mapeia um item de /categorias/receitas-despesas (payload 'data'[])
    para o formato da stage stg.categoria_receita_despesa_bling.
    """
    from datetime import datetime
    now = datetime.now()
    return {
        "id_bling":         item.get("id"),
        "id_categoria_pai": item.get("idCategoriaPai"),
        "descricao":        item.get("descricao"),
        "tipo":             item.get("tipo"),
        "dt_carga":         now,
        "dt_atualizacao":   now
    }
# endregion
