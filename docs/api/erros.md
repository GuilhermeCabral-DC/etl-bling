# Erros Comuns – API Bling

Esta seção documenta os principais erros retornados pela API Bling e como tratá-los corretamente em seu pipeline ETL.

---

## Introdução

A API Bling utiliza códigos HTTP para indicar sucesso ou falhas nas requisições. Todas as respostas de erro retornam um objeto no JSON com os campos `type`, `message` e `description`, auxiliando no tratamento automático e no registro de logs.  
Erros `4xx` indicam problemas com os dados enviados; erros `5xx` apontam falhas internas do serviço :contentReference[oaicite:3]{index=3}.

---

## Principais Erros e Respostas Esperadas

### `VALIDATION_ERROR`  
- **HTTP Code:** 400  
- **Quando ocorre:** Falha na validação dos campos enviados  
```json
{
  "error": {
    "type": "VALIDATION_ERROR",
    "message": "Não foi possível executar a operação",
    "description": "Ocorreu um erro ao validar os dados recebidos."
  }
}
```

### `MISSING_REQUIRED_FIELD_ERROR`
- **HTTP Code: 400**
- **Quando ocorre: Campo obrigatório ausente**
```json
{
  "error": {
    "type": "UNKNOWN_ERROR",
    "message": "Não foi possível executar a operação",
    "description": "Ocorreu um erro inesperado."
  }
}
```

### `UNKNOWN_ERROR`
- **HTTP Code: 400**
- **Quando ocorre: Erro não mapeado pelo sistema**
```json
{
  "error": {
    "type": "UNKNOWN_ERROR",
    "message": "Não foi possível executar a operação",
    "description": "Ocorreu um erro inesperado."
  }
}
```

### `UNAUTHORIZED`
- **HTTP Code: 401**
- **Quando ocorre: Token de acesso inválido ou expirado**
```json
{
  "error": {
    "type": "UNAUTHORIZED",
    "message": "Não autorizado.",
    "description": "Verifique suas credenciais e tente novamente."
  }
}

```

### `FORBIDDEN`
- **HTTP Code: 403**
- **Quando ocorre: Permissões insuficientes para o recurso solicitado**
```json
{
  "error": {
    "type": "FORBIDDEN",
    "message": "Não permitido.",
    "description": "Você não está autorizado a realizar esta operação."
  }
}

```

### `RESOURCE_NOT_FOUND`
- **HTTP Code: 404**
- **Quando ocorre: Recurso solicitado não encontrado ou ID inválido**
```json
{
  "error": {
    "type": "RESOURCE_NOT_FOUND",
    "message": "Não autorizado.",
    "description": "O recurso requisitado não foi encontrado."
  }
}

```

### `TOO_MANY_REQUESTS`
- **HTTP Code: 429**
- **Quando ocorre: Limite de requisição excedido (per second ou per day)**
```json
{
  "error": {
    "type": "TOO_MANY_REQUESTS",
    "message": "Limite de requisições atingido.",
    "description": "Você atingiu o limite disponível. Aguarde alguns minutos e tente novamente."
  }
}

```

### `SERVER_ERROR`
- **HTTP Code: 500**
- **Quando ocorre: Falha interna no servidor da API**
```json
{
  "error": {
    "type": "SERVER_ERROR",
    "message": "Não foi possível executar a operação",
    "description": "Um erro interno ocorreu."
  }
}

```

## Estratégias de Tratamento

- **Deserializar e logar os campos `type` e `description`** para rastreabilidade.
- **Implementar retries específicos:**
    - `TOO_MANY_REQUESTS` → backoff exponencial e nova tentativa.
    - `UNAUTHORIZED` → requer refresh de token ou reautenticação.
- **Tratamento de erros 4xx:** geralmente reprocessar após ajuste dos dados ou parâmetros.
- **Erros 5xx:** aplicar retry limitado e registrar falha para reanálise posterior.

---

## Referências

- [Erros Comuns – Documentação Oficial Bling](https://developer.bling.com.br/erros-comuns#introdu%C3%A7%C3%A3o)
- [Bling Developer](https://developer.bling.com.br/)

---

!!! note
    Recomenda-se capturar e armazenar os erros em logs estruturados (incluindo `type`, `message`, `description`) para facilitar auditoria, alertas e reprocessamento seletivo.
