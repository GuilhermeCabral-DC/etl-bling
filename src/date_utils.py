# region IMPORTS
from datetime import datetime
# endregion

# region UTILITÁRIO: DATA
def parse_date_safe(date_str):
    """
    Converte string para datetime.date ou datetime, ou retorna None se inválido.
    Aceita formatos ISO ou apenas data.
    """
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None
# endregion