# Limites e Restrições – API Bling

A API Bling possui regras claras de uso para garantir disponibilidade, desempenho e segurança. Esta seção contempla os principais limites e cuidados necessários durante a integração.

---

## ⚠️ Limites gerais de requisição

- **Máximo de 3 requisições por segundo** por conta.
- **Limite diário de 120.000 requisições** por conta.
- Caso esses limites sejam atingidos (código HTTP `429`), a API retorna respostas indicando se o bloqueio é por segundo ou por dia, conforme o contexto :contentReference[oaicite:1]{index=1}.

**Exemplo de resposta ao exceder o limite:**
```json
{
  "error": {
    "type": "TOO_MANY_REQUESTS",
    "message": "Limite de requisições atingido.",
    "description": "O limite de requisições por segundo foi atingido, tente novamente mais tarde.",
    "limit": 3,
    "period": "second"
  }
}
```

## ❌ Bloqueios por IP

A API também bloqueia IPs em cenários extremos:

- **300 erros HTTP em 10 segundos** → bloqueio de 10 minutos.
- **600 requisições em 10 segundos** → bloqueio de 10 minutos.
- **Mais de 20 requisições ao endpoint `/oauth/token` em 60 segundos** → bloqueio por 60 minutos.

Veja mais em: [Limites – Bling Developer](https://developer.bling.com.br/limites#bloqueio-de-ip)

---

## 📊 Filtros de período

Requisições GET com filtros de data cujo **intervalo seja superior a 1 ano** retornam erro HTTP 400.

**Parâmetros típicos de filtros incluem:**

- `dataInicial`, `dataFinal`
- `dataAlteracaoInicial`, `dataAlteracaoFinal`

Veja mais em: [Limites – Bling Developer](https://developer.bling.com.br/limites#filtros)

---

## 🔄 Estratégias para lidar com limites

- **Throttle e backoff exponencial:** Implemente esperas automáticas ao receber erros 429.
- **Distribua as requisições ao longo do tempo** para evitar bursts que excedam os limites por segundo.
- **Monitore erros HTTP 429 e IP blocks** no log do seu ETL.
- **Utilize filtros de data de forma adequada**, respeitando o limite máximo de um ano por consulta.

---

## Tabela de Resumo

| Tipo de Limite             | Valor / Condição                                   |
|----------------------------|---------------------------------------------------|
| Requisições por segundo    | Até 3 requisições                                 |
| Requisições por dia        | Até 120.000 requisições                           |
| Bloqueio por erros         | 300 erros em 10s → bloqueio de 10 minutos         |
| Bloqueio por volume        | 600 requisições em 10s → bloqueio de 10 minutos   |
| Limite em token OAuth      | 20 requisições em 60s → bloqueio de 60 minutos    |
| Filtros de data            | Intervalos maiores que 1 ano → HTTP 400           |

---

## Referências

- [Limites da API – Documentação Oficial Bling](https://developer.bling.com.br/limites)
- [Boas Práticas de Paginação – Bling Developer](https://developer.bling.com.br/boas-praticas#pagina%C3%A7%C3%A3o)

---

!!! note
    É recomendável **monitorar e registrar** erros HTTP 429 e bloqueios por IP para permitir operações seguras, reprocessamento seletivo e conformidade com os limites da API.

