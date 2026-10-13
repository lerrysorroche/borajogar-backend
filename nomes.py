"""
Nome completo de um jogo = título + subtítulo (como nome e sobrenome).

O título é o nome oficial e estável do jogo ("Cuphead") e é a chave de tudo que
casa por texto: busca no HowLongToBeat, valor arrecadado e estorno de reservas
(todos procuram o título dentro da descrição das transações). O subtítulo é só a
versão/edição ("& The Delicious Last Course", "Complete Edition") e pode mudar
sem perder histórico. O nome completo é o que o cliente enxerga.
"""


def nome_completo(titulo, subtitulo=""):
    sub = (subtitulo or "").strip()
    if not sub:
        return titulo
    # Subtítulo começando por ':' ou ',' cola no título ("Cuphead: Edição X").
    return f"{titulo}{sub}" if sub[0] in ":," else f"{titulo} {sub}"


def sql_nome_completo(alias="j"):
    """Mesma regra de nome_completo(), para usar dentro de um SELECT."""
    sub = f"TRIM({alias}.subtitulo)"
    return (
        f"TRIM({alias}.titulo || CASE WHEN COALESCE({sub}, '') = '' THEN '' "
        f"WHEN LEFT({sub}, 1) IN (':', ',') THEN {sub} "
        f"ELSE ' ' || {sub} END)"
    )


NOME_J = sql_nome_completo("j")
