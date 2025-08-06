# Arquitetura dos Scripts

Esta seção apresenta a **estrutura modular** dos scripts em Python, detalhando o papel de cada arquivo e como eles se integram para formar um pipeline ETL robusto, escalável e de fácil manutenção.

---

## Organização dos Módulos

O projeto é dividido em módulos independentes, cada um responsável por uma parte do processo, seguindo princípios de separação de responsabilidades e código limpo.

| Módulo             | Função                                                                                   |
|--------------------|-----------------------------------------------------------------------------------------|
| `main.py`          | Orquestração central do pipeline ETL: inicia e monitora as cargas                       |
| `bling_api.py`     | Integração com o Bling: executa requisições à API e retorna os dados brutos             |
| `etl.py`           | Gerencia o controle incremental e a execução das rotinas de carga                       |
| `db.py`            | Opera com PostgreSQL: conexões, inserts, upserts e comandos SQL                         |
| `transformers.py`  | Realiza transformações, validações e padronização dos dados extraídos                   |
| `log.py`           | Logging centralizado e estruturado (execução, falhas, métricas)                         |
| `auth.py`          | Gerenciamento da autenticação OAuth2 com a API Bling                                    |
| `config.py`        | Centraliza variáveis de ambiente e parâmetros globais                                   |
| `utils.py`         | Funções utilitárias diversas (helpers)                                                  |
| `obter_token.py`   | Script manual para obtenção ou renovação de tokens OAuth2                               |
| `date_utils.py`    | Funções para manipulação e padronização de datas                                        |

---

## Fluxo Geral do Pipeline

1. O `main.py` inicia a execução do processo ETL.
2. O módulo `auth.py` gerencia a autenticação e renovação dos tokens de acesso.
3. O `bling_api.py` executa as chamadas aos endpoints do Bling, recebendo os dados brutos.
4. O `etl.py` controla o processamento incremental, definindo quais entidades e períodos devem ser processados.
5. O `transformers.py` normaliza e valida os dados extraídos.
6. O `db.py` realiza a inserção ou atualização dos dados no PostgreSQL, utilizando operações otimizadas para grandes volumes.
7. O `log.py` registra logs estruturados sobre cada etapa, incluindo erros, execuções e métricas de performance.
8. Outros módulos de suporte, como `utils.py`, `config.py` e `date_utils.py`, são utilizados sempre que funções utilitárias ou parâmetros globais são necessários.

---

## Boas Práticas Adotadas

- **Separação de responsabilidades:** cada módulo cuida de uma etapa ou serviço específico.
- **Reaproveitamento de código:** funções utilitárias centralizadas em módulos próprios.
- **Escalabilidade:** suporte para novas entidades/endpoints e fácil expansão do pipeline.
- **Controle e rastreabilidade:** logging centralizado e tabelas de controle no banco garantem rastreamento completo das execuções.

---

!!! note
    Para detalhes do código de cada módulo, consulte a documentação detalhada na seção correspondente do menu lateral.
