# Visão Geral – Python & ETL

Esta seção documenta toda a arquitetura, boas práticas e padrões adotados nos scripts Python responsáveis pelo pipeline ETL (Extract, Transform, Load) que integra os dados do Bling ao banco de dados PostgreSQL das Lojas Stilli.

---

## Objetivo

Padronizar, automatizar e garantir a **qualidade** da extração de dados, transformação e carga incremental, suportando alta volumetria e operações confiáveis para reporting e integrações.

---

## Principais Características

- **Arquitetura modular:**  
  Cada responsabilidade é separada em módulos, facilitando manutenção e escalabilidade.

- **Conexão segura e robusta:**  
  Gerenciamento centralizado de credenciais e tokens para integração com a API Bling e PostgreSQL.

- **Controle incremental:**  
  Apenas dados novos ou alterados são extraídos e carregados, minimizando redundância e otimizando performance.

- **Logging centralizado:**  
  Todos os eventos relevantes do pipeline (sucesso, falha, warnings, performance) são registrados para auditoria e troubleshooting.

- **Tratamento de falhas e reprocessamento:**  
  Estratégias para identificar, registrar e reprocessar falhas, garantindo integridade dos dados.

- **Processamento em lote (batch):**  
  Uso de processamento em blocos para acelerar operações de carga e minimizar impacto em recursos.

---

## Estrutura dos Scripts

O pipeline é organizado em módulos específicos, incluindo:

- **main.py**: Orquestração das rotinas ETL.
- **bling_api.py**: Conexão e requisições à API Bling.
- **etl.py**: Controle e execução das cargas incrementais.
- **db.py**: Operações com PostgreSQL (insert, upsert, etc.).
- **transformers.py**: Transformação e padronização dos dados.
- **utils.py**: Funções utilitárias e helpers.
- **log.py**: Logging centralizado e rastreamento.
- **auth.py**: Autenticação e renovação de tokens.
- **config.py**: Centralização de configurações e parâmetros.
- **obter_token.py**: Script dedicado à obtenção de tokens OAuth2.
- **date_utils.py**: Funções auxiliares para manipulação de datas.

---

## Boas Práticas Adotadas

- **Separação de responsabilidades** entre extração, transformação e carga.
- **Tratamento de exceções** e logs detalhados para análise de falhas.
- **Configuração externa** via arquivos `.env` e parâmetros centralizados.
- **Automação de execução** e facilidade para expansão do pipeline.

---

!!! note
    Consulte os subitens do menu para detalhes de cada etapa, exemplos de código e explicações sobre transformações, controle de carga, logging, batch e tratamento de erros.
