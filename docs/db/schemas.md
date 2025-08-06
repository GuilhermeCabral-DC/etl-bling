# Schemas do Banco de Dados

A modelagem do banco utiliza a separação lógica de dados em schemas, organizando entidades, fatos, controle, transformações e staging conforme o padrão Kimball para Data Warehousing.

---

## Visão Geral

A divisão por schemas garante organização, performance, isolamento de permissões e facilita a governança e evolução do modelo de dados.

---

## Owners dos Schemas

Os schemas do banco estão associados aos seguintes proprietários (owners):

| Schema   | Owner               | Função/Objetivo                                                  |
|----------|---------------------|------------------------------------------------------------------|
| `conf`   | postgres            | Controle de carga, parâmetros globais e logs                     |
| `dim`    | postgres            | Tabelas de dimensão, entidades de negócio                        |
| `etl`    | postgres            | Procedures e lógica de transformação (ETL)                       |
| `fato`   | postgres            | Tabelas fato, dados transacionais e quantitativos                |
| `link`   | postgres            | Relacionamentos N:N entre entidades                              |
| `stg`    | postgres            | Staging de dados brutos extraídos do Bling (landing zone)        |
| `public` | pg_database_owner   | Schema padrão do PostgreSQL (uso genérico, evitar uso no projeto)|
| `pg_toast` | postgres          | Schema interno do PostgreSQL, utilizado para dados grandes       |

> **Nota:**  
> Recomenda-se manter os objetos de negócio e controle fora do schema `public` para garantir maior segurança e organização.

---

## Convenções

- Schemas usam nomes minúsculos, curtos e descritivos.
- Permissões são gerenciadas por schema; schemas sensíveis (como `conf`) podem ter acesso restrito.
- Procedures de ETL são centralizadas em `etl`.
- Evolução incremental: novos schemas podem ser criados conforme novas demandas de negócio.

---

## Fluxo entre Schemas

stg (landing zone) → etl (transformação) → dim/fato/link (modelo) → conf (controle)


---

## Exemplos de Uso

- **conf**: Registro do controle incremental de cargas, falhas e logs.
- **dim**: Cadastro de entidades como produtos, empresas, vendedores.
- **fato**: Movimentação de estoque, vendas, pedidos, notas fiscais.
- **etl**: Procedures de normalização dos dados brutos para o modelo analítico.
- **link**: Relacionamentos N:N, como produto-fornecedor.
- **stg**: Área de staging dos dados extraídos diretamente do Bling.

---

!!! note
    Para gestão de owners, permissões e versionamento dos schemas, consulte o DBA responsável e a política de governança de dados da organização.
