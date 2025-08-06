# Transformação

Esta seção detalha o processo de **transformação dos dados** extraídos da API do Bling, explicando as validações, padronizações e adaptações realizadas antes do envio para o banco de dados.

---

## Visão Geral

A etapa de transformação garante que os dados extraídos estejam:
- **No formato correto** para o destino (PostgreSQL).
- **Validados e limpos** de inconsistências ou valores inesperados.
- **Padronizados** conforme regras de negócio e requisitos de integração.
- **Enriquecidos** quando necessário (cálculos, formatações, agregações).

---

## Fluxo da Transformação

1. **Recepção dos dados brutos:**  
   Os dados extraídos via API são recebidos pelos métodos do módulo `transformers.py`.

2. **Validação de campos obrigatórios:**  
   Checagem de presença e tipos dos dados essenciais.

3. **Padronização:**  
   - Conversão de formatos de data/hora.
   - Padronização de nomes de campos, tipos de dados e chaves estrangeiras.
   - Ajustes em campos de texto (remover espaços, corrigir capitalização, etc.).

4. **Enriquecimento e cálculo:**  
   Adição de informações derivadas, cálculos de totais, indicadores ou status conforme a regra de negócio.

5. **Preparação para o banco:**  
   Transformação dos dados para o formato esperado pelas funções de inserção (dicts, DataFrames, listas de tuplas, etc.).

---

## Exemplo de Código

```python
from transformers import transformar_produto

dados_brutos = {...}  # obtido via API
dados_tratados = transformar_produto(dados_brutos)
```

---

## Principais Transformações Realizadas

- Conversão de datas (ISO 8601 → padrão SQL).
- Conversão e validação de valores numéricos.
- Remoção de campos nulos ou vazios indesejados.
- Adaptação dos nomes dos campos para o padrão do banco.
- Geração de campos auxiliares (ex: SKU formatado, códigos compostos).

---

## Pontos de Atenção

- **Erros de conversão:**  
  Logs detalhados são gerados em casos de falha na transformação, para facilitar troubleshooting.
- **Regra de negócio:**  
  Todas as regras e mapeamentos especiais estão centralizados em `transformers.py` para fácil manutenção.
- **Campos opcionais:**  
  Atenção ao tratamento de campos ausentes ou opcionais para evitar falhas de inserção.

---

## Boas Práticas

- Isolar cada transformação em funções reutilizáveis.
- Documentar claramente cada regra implementada no código.
- Garantir testes automatizados para as principais funções de transformação.

---

!!! note
    Para ver exemplos detalhados e transformações específicas de cada entidade, consulte o código-fonte e as seções de entidades.
