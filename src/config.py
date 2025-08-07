# region IMPORTS
from datetime import datetime, timedelta
# endregion

# region ============= CONTROLE DE CARGA =============

CARGA_FULL = False
DATA_FULL_INICIAL = datetime(2020, 1, 1)
MARGEM_DIAS_INCREMENTO = 2

RODAR_EMPRESA = True
RODAR_CATEGORIA = False
RODAR_GRUPO_PRODUTO = False
RODAR_PRODUTO = True
RODAR_CANAIS_VENDA = False
RODAR_VENDEDOR = False
RODAR_DEPOSITOS = False
RODAR_SALDO_PRODUTO_DEPOSITO = True

# endregion

# region ============= CONTROLE DE REPROCESSAMENTO =============

REPROCESSAR_FULL = False #(ainda nao implementado)
REPROCESSAR_PRODUTO_MANUAL = True
REPROCESSAR_EMPRESA_MANUAL = True
REPROCESSAR_SALDO_MANUAL = True

# endregion

# region ============= DEBUGAR =============

DEBUG = True

# endregion