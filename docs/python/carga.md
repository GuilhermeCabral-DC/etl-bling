# Carga

Esta seção descreve o processo de **carga dos dados transformados** para o banco de dados PostgreSQL, destacando estratégias de performance, integridade dos dados e rastreabilidade.

---

## Visão Geral

A etapa de carga é responsável por:
- **Inserir ou atualizar** registros no PostgreSQL de forma eficiente.
- **Garantir a integridade referencial** entre as tabelas.
- **Registrar logs detalhados** sobre cada operação executada.
- **Executar operações em lote** (batch/bulk insert) para alta performance.

---

## Fluxo da Carga

1. **Recepção dos dados transformados:**  
   O módulo `etl.py` recebe os dados validados e padronizados para envio ao banco.

2. **Preparação para inserção/atualização:**  
   Os dados são organizados em lotes (batch) conforme o volume e a entidade.

3. **Execução das operações no banco:**  
   O módulo `db.py` executa `INSERT`, `UPDATE` ou `UPSERT` (conforme a regra de cada entidade), utilizando conexões otimizadas.

4. **Logging:**  
   O módulo `log.py` registra o sucesso, falha ou qualquer evento relevante para auditoria e troubleshooting.

5. **Atualização do controle incremental:**  
   A tabela de controle (`controle_carga`) é atualizada para garantir rastreabilidade e possibilitar reprocessamentos seguros.

---

## Estratégias de Performance

- **Inserts em massa (bulk insert):**  
  Utilização de operações em lote para reduzir o tempo de escrita no banco.
- **Upserts otimizados:**  
  Atualizações inteligentes para evitar duplicidade e garantir consistência.
- **Controle transacional:**  
  Uso de transações para garantir atomicidade e rollback em caso de erro.

---

## Exemplo de Código

```python
from db import inserir_produtos_em_lote

produtos_tratados = [...]  # lista de dicionários já transformados
inserir_produtos_em_lote(produtos_tratados)
```

---

## Pontos de Atenção

- **Chaves únicas e integridade referencial:**  
  Certifique-se de que todas as chaves primárias e estrangeiras estejam corretamente tratadas.
- **Volume de dados:**  
  Ajuste o tamanho do lote para equilibrar performance e consumo de memória.
- **Erros de banco:**  
  Todos os erros de inserção/atualização são logados e tratados para reprocessamento.

---

## Boas Práticas

- Utilize funções específicas para cada entidade/tabela.
- Sempre registre logs detalhados de cada operação de carga.
- Implemente controle incremental robusto para evitar retrabalho e inconsistências.

---

!!! note
    Para mais detalhes sobre a estrutura das tabelas, veja a seção **Banco de Dados**.
