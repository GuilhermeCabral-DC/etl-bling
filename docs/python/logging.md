# Logging

Esta seção explica a abordagem de **logging centralizado e estruturado** adotada no projeto, fundamental para auditoria, troubleshooting e acompanhamento de performance das cargas ETL.

---

## Visão Geral

O logging no projeto é responsável por:
- **Registrar todas as etapas** da execução do pipeline ETL.
- **Capturar falhas, exceções e tentativas de reprocessamento**.
- **Gerar métricas de performance** (tempo de execução, quantidade de registros processados, etc).
- **Facilitar a auditoria e rastreabilidade** de cada rotina de carga.

---

## Como Funciona o Logging

1. **Estrutura Centralizada:**  
   Todas as mensagens e eventos relevantes são tratados pelo módulo `log.py`, que padroniza o formato e a saída dos logs.

2. **Níveis de log:**  
   O sistema suporta diferentes níveis (`INFO`, `WARNING`, `ERROR`, `DEBUG`), permitindo controlar o detalhamento conforme o ambiente (produção, homologação, desenvolvimento).

3. **Registro detalhado de eventos:**  
   Cada execução, falha, reprocessamento ou alerta relevante é registrado com timestamp, nome do processo e contexto da execução.

4. **Persistência dos logs:**  
   Os logs podem ser armazenados em arquivos, banco de dados ou enviados para sistemas externos de monitoramento (opcional).

---

## Exemplo de Código

```python
from log import log_etl

log_etl("INFO", "Iniciando carga de produtos", entidade="produtos")
log_etl("ERROR", "Falha ao inserir registro", detalhe="Chave duplicada", entidade="produtos")
```

---

## O que é Logado

- Início e término de cada carga.
- Quantidade de registros processados por lote.
- Tempo de execução de cada etapa.
- Erros e exceções capturados.
- Tentativas de reprocessamento e eventos de retry.

---

## Pontos de Atenção

- **Evite prints soltos:**  
  Sempre utilize o módulo de logging centralizado para garantir padronização.
- **Sensibilidade de dados:**  
  Não registre informações confidenciais em logs acessíveis por múltiplos usuários.
- **Monitoramento ativo:**  
  Considere integrar o log a sistemas de monitoramento para alertas proativos.

---

## Boas Práticas

- Adote mensagens claras, objetivas e contextuais.
- Use níveis de log de acordo com a criticidade do evento.
- Padronize os campos mínimos: timestamp, entidade, mensagem, nível e contexto.

---

!!! note
    Para mais detalhes ou exemplos avançados, consulte o código do módulo `log.py`.
