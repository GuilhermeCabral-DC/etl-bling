# Visão Geral do Banco de Dados

O banco de dados utilizado neste projeto é um **PostgreSQL**, projetado para garantir integridade, performance e escalabilidade na centralização dos dados extraídos do Bling.

A modelagem adota as boas práticas do **modelo dimensional de Ralph Kimball**, amplamente reconhecido em projetos de Data Warehouse. Essa abordagem foi escolhida para suportar grandes volumes de informação, facilitar consultas analíticas e garantir flexibilidade para evolução do pipeline ETL.

---

## Objetivos da Modelagem

- **Centralizar dados** provenientes da API Bling em uma estrutura relacional organizada.
- **Otimizar consultas** para relatórios e integrações.
- **Permitir controle incremental** e histórico das cargas, reduzindo redundância.
- **Isolar ambientes por schema** (por exemplo: produção, staging, testes).
- **Facilitar auditoria** e rastreabilidade das operações de carga.

---

## Principais Componentes

- **Schemas dedicados:**  
  Separação lógica entre dados de negócio, controle de carga e schemas auxiliares.

- **Tabelas normalizadas:**  
  Cada entidade (empresa, produto, categoria, depósito, saldo, etc.) possui tabelas específicas, minimizando redundância e garantindo integridade referencial.

- **Procedures e funções:**  
  Automatizam operações recorrentes, como upserts, consistência de dados e tratamentos especiais durante a carga.

- **Controle de Carga:**  
  Tabelas e procedimentos específicos para registrar execuções, falhas e status das cargas, permitindo reprocessamentos inteligentes.

---

## Organização dos Schemas

| Schema     | Descrição                                                  |
|------------|------------------------------------------------------------|
| `conf`     | Controle de carga, configurações do pipeline, parâmetros e logging  |
| `dim`      | Tabelas dimensionais, entidades de negócio (ex: produtos)  |
| `etl`      | Controle das procedures que realizam normalização da stg para dim/fato                   |
| `fato`     | Tabelas fato (dados transacionais e quantitativos)         |
| `link`     | Tabelas de relacionamento entre entidades                  |
| `stg`      | Staging: dados brutos temporários extraídos do Bling       |


> **Observação:**  
> Os nomes dos schemas podem variar conforme a implantação e ambiente.

---

## Convenções

- **Chaves primárias e estrangeiras** implementadas para integridade relacional.
- **Timestamps** para rastreamento de alterações e histórico de carga.
- **Índices** para acelerar consultas frequentes.
- **Naming convention** padronizada para tabelas e campos.

---

## Evolução e Expansão

O banco de dados está em constante evolução para acompanhar novas demandas de negócio, integrações e melhorias de performance.

---

!!! note
    Para visualizar detalhes das tabelas, schemas, procedures e funções, utilize o menu lateral ou acesse as seções específicas desta documentação.

---
