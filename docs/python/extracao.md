# Extração

Nesta seção, detalhamos o processo de **extração dos dados** do Bling via API, abordando o fluxo principal, configurações necessárias, exemplos de uso e boas práticas adotadas no projeto.

---

## Visão Geral

A etapa de extração é responsável por:
- **Conectar-se à API do Bling** de forma segura.
- **Solicitar os dados** conforme o endpoint e o período definidos na rotina ETL.
- **Gerenciar paginação, limites e autenticação OAuth2**.
- **Garantir robustez** no tratamento de erros, timeouts e tentativas automáticas (retry).

---

## Fluxo da Extração

1. **Autenticação:**  
   O módulo `auth.py` obtém e renova o token de acesso necessário para autenticar cada requisição à API.

2. **Chamada à API:**  
   O módulo `bling_api.py` executa as requisições, tratando paginação, limites de registros e possíveis erros de comunicação.

3. **Persistência bruta:**  
   Os dados retornados são salvos em memória (ou temporariamente em disco, conforme o volume), antes de serem transformados e enviados ao banco.

---

## Configuração de Parâmetros

- **Endpoints suportados:**  
  Exemplo: `/produtos`, `/categorias`, `/vendedores`, etc.
- **Período de extração:**  
  Controlado via parâmetros de data ou incremental, configurado em `etl.py` e/ou `config.py`.
- **Tamanho do lote (batch size):**  
  Definido conforme o endpoint, evitando rate limit e otimizando performance.

---

## Exemplo de Código

```python
from bling_api import requisitar_endpoint
from auth import get_access_token

token = get_access_token()
dados_produtos = requisitar_endpoint("produtos", token=token, pagina=1, limite=100)
```

---

## Pontos de Atenção

- **Paginação:**  
  A maioria dos endpoints exige iteração sobre múltiplas páginas para obter todos os registros.
- **Rate limit:**  
  Respeite os limites da API. O projeto já implementa tratamento para aguardar e retentar.
- **Tratamento de erros:**  
  Todos os erros (timeout, HTTP 500, 401) são logados e, se possível, retentados automaticamente.

---

## Boas Práticas

- Centralize todas as requisições à API em funções/módulos dedicados.
- Utilize logging detalhado para auditoria e troubleshooting.
- Planeje extrações fora do horário comercial para evitar lentidão ou bloqueios.

---

!!! note
    Para detalhes de cada endpoint e exemplos completos, consulte a seção **API Bling**.