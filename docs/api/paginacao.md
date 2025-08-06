# Paginação na API Bling

A maioria dos endpoints da API Bling implementa **paginação** para retornar grandes volumes de dados em partes menores (páginas).  
O correto tratamento da paginação é fundamental para garantir a extração completa, eficiente e sem duplicidade de todos os registros.

---

## Como funciona a paginação?

- Os endpoints retornam um conjunto limitado de registros por requisição.
- Parâmetros padrões para paginação:
    - `pagina`: número da página a ser consultada (inicia em 1)
    - `limite`: quantidade máxima de registros por página (definido pelo endpoint; normalmente até 100)
- As respostas normalmente trazem informações de página atual, total de páginas e/ou total de registros.
- Para extrair todos os dados, é necessário iterar sobre todas as páginas disponíveis até não haver mais registros.

---

## Exemplo prático

**Request:**
```http
GET /v1/produtos?pagina=1&limite=100 HTTP/1.1
Host: api.bling.com.br
Authorization: Bearer <seu_token_de_acesso>
```

**Response (exemplo simplificado):**

```python
{
  "data": [ ... ],
  "pagina": 1,
  "totalPaginas": 15,
  "totalRegistros": 1500
}

```

## Estratégia de Iteração
- Comece consultando a página 1.
- Continue requisitando as próximas páginas (pagina=2, pagina=3, ...) até atingir o total de páginas (totalPaginas) informado na resposta.
- Para cada página, processe e armazene os registros retornados.

**Exemplo de pseudocódigo:**

```python
pagina = 1
while True:
    response = requisitar_api(pagina=pagina, limite=100)
    processa_dados(response['data'])
    if pagina >= response['totalPaginas']:
        break
    pagina += 1
```

## Boas Práticas e Cuidados

- **Não assuma limites fixos:** Sempre confira o limite permitido no endpoint.
- **Evite duplicidades:** Se houver inclusão de registros enquanto sua extração roda, utilize critérios incrementais e idempotentes.
- **Respeite limites de requisição:** Paginar muitos dados pode rapidamente atingir o rate limit da API. Veja a seção [Limites e Restrições](limites.md).
- **Registre falhas:** Implemente logs por página processada para reprocessar facilmente em caso de erro.
- **Confira a documentação oficial:** Alguns endpoints podem não suportar paginação ou ter variações nos parâmetros.

---

## Referências

- [Boas Práticas de Paginação - Documentação Oficial Bling](https://developer.bling.com.br/boas-praticas#pagina%C3%A7%C3%A3o)
- [Lista de Endpoints – Bling Developer](https://developer.bling.com.br/referencia)

---

!!! note
    O correto tratamento da paginação é fundamental para garantir a **completude** e **consistência** dos dados extraídos do Bling.
