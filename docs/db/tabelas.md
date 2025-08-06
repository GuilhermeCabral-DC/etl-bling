# Tabelas do Banco de Dados

Abaixo está o sumário de todas as tabelas dos schemas de negócio, staging, link e controle, seguindo a modelagem dimensional inspirada em Kimball. Para cada tabela, recomenda-se documentar os principais campos, tipos e observações, facilitando auditoria, integração e evolução.

---

## Sumário de Schemas e Tabelas

### conf
- **controle_carga**: Controle incremental das cargas de dados.
- **importacao_falha**: Registro de falhas e exceções durante importações.
- **log**: Logging estruturado de execuções do ETL.

### dim
- **canais_venda**
- **categoria_produto**
- **contato**
- **deposito**
- **empresa**
- **forma_pagamento**
- **fornecedor**
- **grupo_produto**
- **logistica**
- **logistica_servico**
- **notificacao**
- **pessoa_contato**
- **produto**
- **produto_estrutura**
- **produto_estrutura_componente**
- **produto_fornecedor**
- **tipo_contato**
- **tributacao_produto**
- **vendedor**
- **vendedor_comissao**

### fato
- **estoque**
- **nota_fiscal**
- **nota_fiscal_item**
- **nota_fiscal_parcela**
- **pedido_compra**
- **pedido_compra_item**
- **pedido_compra_parcela**
- **pedido_venda**
- **pedido_venda_item**
- **pedido_venda_parcela**

### link
- **contato_pessoa_contato**
- **contato_tipo_contato**

### stg
- **canais_venda_bling**
- **categoria_produto_bling**
- **contato_bling**
- **deposito_bling**
- **empresa_bling**
- **forma_pagamento_bling**
- **fornecedor_bling**
- **grupo_produto_bling**
- **logistica_bling**
- **logistica_servico_bling**
- **natureza_operacao_bling**
- **nota_fiscal_bling**
- **notificacao_bling**
- **pedido_compra_bling**
- **pedido_venda_bling**
- **produto_bling**
- **produto_fornecedor**
- **saldo_estoque_bling**
- **saldo_produto_deposito_bling**
- **vendedor_bling**

---

### dim.produto

| Campo                 | Tipo               | Nulo? | Default   | Observação                             |
|-----------------------|--------------------|-------|-----------|----------------------------------------|
| produto_sk            | integer            | não   | sequencial| Chave primária surrogate key           |
| produto_id            | bigint             | sim   |           | ID original Bling                      |
| nome                  | character varying  | sim   |           | Nome do produto                        |
| preco                 | numeric            | sim   |           | Preço de venda                         |
| dt_carga              | timestamp          | sim   | now()     | Data da carga no DW                    |
| ...                   | ...                | ...   | ...       | ...                                    |

**Descrição:**  
Tabela dimensão de produtos. Utilizada para análises históricas, cruzamento com fatos de vendas, estoque e relacionamento com demais dimensões.

---

> **Observação:**  
> As descrições resumidas serão evoluídas gradualmente conforme o projeto demanda.