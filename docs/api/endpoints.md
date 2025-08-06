# Endpoints da API Bling

Esta seção documenta todos os endpoints da API Bling consumidos pelo pipeline ETL Lojas Stilli.  
Inclui informações sobre URLs, métodos, parâmetros, exemplos de uso e observações específicas para cada caso.

---

## Sumário dos Endpoints Utilizados

| Entidade              | Método | Caminho                                 | Descrição                        |
|-----------------------|:------:|-----------------------------------------|----------------------------------|
| Empresas              | GET    | `/empresas`                             | Consulta dados cadastrais         |
| Produtos              | GET    | `/produtos`                             | Lista produtos                   |
| Estoques              | GET    | `/estoques`                             | Consulta saldos em depósitos     |
| Pedidos de Venda      | GET    | `/pedidos/vendas`                       | Lista pedidos de venda           |
| Pedidos de Compra     | GET    | `/pedidos/compras`                      | Lista pedidos de compra          |
| Vendedores            | GET    | `/vendedores`                           | Consulta vendedores              |
| ...                   | ...    | ...                                     | ...                              |

> **Nota:** Todos os endpoints são acessados via base URL da API Bling, com autenticação OAuth2 obrigatória.

---

## Exemplo Detalhado de Endpoint

### Produtos

- **URL:** `https://api.bling.com.br/v1/produtos`
- **Método:** `GET`
- **Descrição:** Retorna a lista de produtos cadastrados na conta Bling.
- **Parâmetros principais:**
    - `pagina` (opcional, inteiro): Número da página para paginação
    - `limit` (opcional, inteiro): Quantidade de itens por página (máximo permitido: 100)
    - Filtros adicionais conforme documentação oficial

- **Request de exemplo:**
    ```http
    GET /v1/produtos?pagina=1&limit=50 HTTP/1.1
    Host: api.bling.com.br
    Authorization: Bearer <seu_token_de_acesso>
    ```

- **Response de exemplo:**
    ```json
    {
      "data": [
        {
          "id": 12345,
          "nome": "Produto A",
          "codigo": "SKU123",
          "preco": 19.99,
          ...
        },
        ...
      ],
      "page": 1,
      "totalPages": 10
    }
    ```

- **Notas:**
    - Paginação obrigatória para listas grandes.
    - Respeite limites de requisição por minuto (veja seção de limites).
    - Consulte a seção “Paginação” para detalhes de iteração.

---

## Observações Gerais

- Todas as requisições devem conter header `Authorization: Bearer <token>`.
- Parâmetros opcionais podem ser omitidos para retornar todos os registros.
- Consulte a [documentação oficial da API Bling](https://developer.bling.com.br/bling-api#introdu%C3%A7%C3%A3o) para todos os detalhes e endpoints disponíveis.

---

!!! tip
    Lista sempre atualizada conforme novos endpoints forem integrados ao pipeline ETL.

