"""
Temas sazonais do site (Halloween, Natal, Ano Novo).

O Admin escolhe em configuracoes.tema_manual:
  'auto'  -> segue o calendário abaixo (padrão)
  'normal' / 'halloween' / 'natal' / 'ano_novo' -> força esse tema, ignorando datas

O backend devolve o tema já resolvido em 'tema_ativo' (GET /configuracoes), com a
data de Brasília, para o site inteiro concordar mesmo que o relógio de algum
aparelho esteja errado.
"""

from datetime import datetime, timedelta, timezone

TEMAS_VALIDOS = ("normal", "halloween", "natal", "ano_novo")

# (mês, dia) de início e fim, inclusive. Se o fim vem antes do início, o período
# atravessa a virada do ano (Ano Novo: 27/12 a 05/01).
CALENDARIO = {
    "halloween": ((10, 25), (11, 2)),
    "natal": ((12, 1), (12, 26)),
    "ano_novo": ((12, 27), (1, 5)),
}


def hoje_brasilia():
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    except Exception:
        # Sem banco de fusos (ex.: Windows sem tzdata): Brasília é UTC-3 fixo.
        return (datetime.now(timezone.utc) - timedelta(hours=3)).date()


def tema_do_calendario(hoje=None):
    hoje = hoje or hoje_brasilia()
    md = (hoje.month, hoje.day)
    for tema, (inicio, fim) in CALENDARIO.items():
        dentro = inicio <= md <= fim if inicio <= fim else md >= inicio or md <= fim
        if dentro:
            return tema
    return "normal"


def tema_ativo(tema_manual, hoje=None):
    if tema_manual in TEMAS_VALIDOS:
        return tema_manual
    return tema_do_calendario(hoje)
