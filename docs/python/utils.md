# Utilitários

Esta seção apresenta os **módulos utilitários** do projeto, que reúnem funções auxiliares reutilizáveis para facilitar o desenvolvimento, manutenção e padronização das rotinas ETL.

---

## Visão Geral

Os utilitários são responsáveis por:
- **Centralizar funções comuns** utilizadas por vários módulos do projeto.
- **Evitar duplicidade de código** e garantir padronização de operações recorrentes.
- **Facilitar manutenção** e futuras evoluções do pipeline ETL.

---

## Principais Utilitários

- `utils.py`:  
  Contém funções genéricas como manipulação de strings, validação de dados, operações de listas/dicionários, tratamento de exceções e geração de identificadores únicos.

- `date_utils.py`:  
  Especializado em funções para manipulação e padronização de datas, conversão de formatos, cálculos de intervalos e validação de períodos.

- Outros scripts utilitários:  
  Podem incluir ferramentas manuais para obtenção de tokens (`obter_token.py`), scripts de teste, exemplos e helpers de integração.

---

## Exemplo de Código

```python
from utils import remover_acentos, gerar_uuid
from date_utils import converter_para_iso, calcular_diferenca_dias

texto = "João da Silva"
texto_limpo = remover_acentos(texto)

data_inicio = "2023-01-01"
data_fim = "2023-01-10"
dias = calcular_diferenca_dias(data_inicio, data_fim)
```

---

## Pontos de Atenção

- **Manutenção centralizada:**  
  Sempre que possível, atualize ou adicione funções utilitárias nos módulos apropriados, evitando cópias em diferentes partes do código.
- **Testes automatizados:**  
  Garanta que as funções utilitárias estejam cobertas por testes para evitar efeitos colaterais em diferentes rotinas.
- **Documentação:**  
  Mantenha os módulos utilitários bem documentados, com exemplos de uso e descrições claras dos parâmetros e retornos.

---

## Boas Práticas

- Prefira funções puras e sem efeitos colaterais para utilitários.
- Reutilize utilitários sempre que possível, reduzindo código duplicado.
- Documente cada função utilitária diretamente no código com docstrings padronizadas.

---

!!! note
    Consulte o código-fonte de `utils.py`, `date_utils.py` e outros scripts para exemplos completos e lista de funções disponíveis.
