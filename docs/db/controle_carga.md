# Controle de Carga

O **controle de carga** é fundamental para garantir confiabilidade, rastreabilidade e governança no processo ETL.  
No projeto Lojas Stilli, o controle é centralizado na tabela `conf.controle_carga`, que monitora cada execução das rotinas de extração, transformação e carga.

---

## Tabela `conf.controle_carga`

Esta tabela registra e parametriza o controle incremental de cada entidade do pipeline, garantindo que nenhuma informação seja processada duplicadamente ou deixada para trás.

| Coluna             | Tipo         | Descrição                                                         |
|--------------------|--------------|-------------------------------------------------------------------|
| `entidade`         | varchar      | Nome da entidade/processo controlado                              |
| `tabela_fisica`    | varchar      | Nome da tabela destino                                            |
| `suporte_incremental` | char      | Indica se suporta carga incremental (`S`/`N`)                     |
| `etapa`            | varchar      | Fase do processo ETL (ex: extração, transformação, carga)         |
| `dt_ultima_carga`  | date         | Data/hora da última execução/carga                                |

- **Finalidade:**  
  - Permitir cargas incrementais
  - Reprocessamento seletivo
  - Auditoria sobre quando e o que foi carregado

---

## Tabela `conf.importacao_falha`

Registra **falhas** ocorridas durante a carga, permitindo reprocessamentos, acompanhamento de problemas e análise de recorrência de erros.

| Coluna          | Tipo         | Descrição                                 |
|-----------------|--------------|-------------------------------------------|
| `id`            | integer      | Identificador da falha                    |
| `entidade`      | varchar      | Entidade afetada                          |
| `id_referencia` | varchar      | Chave do registro com erro                |
| `erro`          | text         | Descrição detalhada da falha              |
| `dt_falha`      | timestamp    | Data/hora do erro                         |
| `processado`    | boolean      | Flag para controle de reprocessamento      |

- **Finalidade:**  
  - Permitir retentativas inteligentes
  - Gerar relatórios de qualidade/processos
  - Facilitar troubleshooting

---

## Tabela `conf.log`

Centraliza o **logging de execuções** das rotinas ETL, detalhando início, fim, status e mensagens de erro de cada carga/processo.

| Coluna         | Tipo         | Descrição                                  |
|----------------|--------------|--------------------------------------------|
| `id_log`       | bigint       | Identificador único do log                 |
| `tabela`       | varchar      | Nome da tabela/processo logado             |
| `acao`         | varchar      | Ação realizada (ex: extração, carga)       |
| `status`       | varchar      | Status da execução (ex: sucesso, erro)     |
| `dt_inicio`    | timestamp    | Data/hora de início                        |
| `dt_fim`       | timestamp    | Data/hora de término                       |
| `mensagem_erro`| text         | Mensagem detalhada do erro (se houver)     |

- **Finalidade:**  
  - Auditoria completa do pipeline ETL
  - Facilitador para troubleshooting e SLA
  - Permite identificar gargalos, recorrências e falhas de execução

---

## Fluxo Resumido do Controle de Carga

1. **Consulta a `conf.controle_carga`** para determinar o ponto de partida da carga incremental de cada entidade.
2. **Execução da carga ETL** (extração → transformação → carga).
3. **Registro de logs em `conf.log`** detalhando status, início/fim e eventuais erros.
4. **Em caso de erro, grava em `conf.importacao_falha`** para permitir análise e reprocessamento posterior.
5. **Atualização de `dt_ultima_carga`** em `conf.controle_carga` após sucesso.

---

!!! tip
    O controle de carga robusto permite automatização segura do pipeline, facilita reprocessamento seletivo e fornece total transparência para auditoria e conformidade.

