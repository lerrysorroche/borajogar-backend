"""
Aluguel Platina: desconto para quem aluga de novo um jogo que já alugou.

Regras (definidas com o dono da locadora):
  - 1ª locação do jogo: preço normal (nível 0).
  - 2ª locação: 20% (nível 1). 3ª em diante: 40% (nível 2, o máximo).
  - O desconto só vale se a locação anterior DESSE jogo terminou há no máximo
    JANELA_DIAS (3 meses). Passou disso, a sequência zera e o próximo aluguel
    volta ao preço normal (e conta de novo como "1ª").
  - Platina + Rank somados nunca passam de DESCONTO_TOTAL_MAXIMO (40%).
    A soma é composta: 20% de Platina + 20% de Rank = 36%, não 40%.
  - Não vale para jogos em lançamento (lançados há menos de DIAS_LANCAMENTO, ou
    ainda em pré-venda). A sequência continua contando mesmo assim, então o
    desconto aparece quando o jogo sair do período de lançamento.
  - O cashback da devolução antecipada fica limitado a CASHBACK_MAX_FRACAO do
    valor pago: sem isso, desconto alto + cashback fixo (R$ 2 + R$ 1/dia) fazia
    o cliente ganhar dinheiro devolvendo no primeiro dia.

O nível usado em cada locação fica gravado em locacoes.nivel_platina (e em
fila_espera.nivel_platina, para reservas), e é a partir dele que o próximo
nível é calculado.
"""

from datetime import date, datetime, timedelta

from temas import hoje_brasilia

JANELA_DIAS = 90
DESCONTO_POR_NIVEL = 20
NIVEL_MAXIMO = 2
DESCONTO_TOTAL_MAXIMO = 40.0
DESCONTO_RANK_MAXIMO = 20
DIAS_LANCAMENTO = 90
CASHBACK_MAX_FRACAO = 0.5


def ultimas_locacoes(cursor, utilizador_id, jogo_id=None):
    """
    Última locação ENCERRADA de cada jogo do cliente (ou só do jogo pedido),
    com a indicação se ainda está dentro da janela de 3 meses. A janela é
    calculada no próprio banco para não depender do fuso do servidor.
    Exige cursor do tipo RealDictCursor. Devolve {jogo_id: linha}.
    """
    filtro_jogo = "AND c.jogo_id = %s" if jogo_id is not None else ""
    params = [utilizador_id] + ([jogo_id] if jogo_id is not None else [])
    cursor.execute(
        f"""
        SELECT DISTINCT ON (c.jogo_id)
               c.jogo_id,
               COALESCE(l.nivel_platina, 0) AS nivel_platina,
               (l.data_fim + {JANELA_DIAS} * INTERVAL '1 day') AS valido_ate,
               (l.data_fim + {JANELA_DIAS} * INTERVAL '1 day' >= CURRENT_TIMESTAMP) AS na_janela
        FROM locacoes l
        JOIN contas_psn c ON c.id = l.conta_psn_id
        WHERE l.utilizador_id = %s AND l.status = 'EXPIRADA' {filtro_jogo}
        ORDER BY c.jogo_id, l.data_fim DESC
        """,
        params,
    )
    return {linha["jogo_id"]: linha for linha in cursor.fetchall()}


def nivel_do_proximo(ultima):
    """Nível Platina do próximo aluguel, a partir da última locação do jogo."""
    if not ultima or not ultima["na_janela"]:
        return 0
    return min(int(ultima["nivel_platina"] or 0) + 1, NIVEL_MAXIMO)


def eh_lancamento(data_lancamento):
    if not data_lancamento:
        return False
    if isinstance(data_lancamento, datetime):
        data_lancamento = data_lancamento.date()
    elif isinstance(data_lancamento, str):
        try:
            data_lancamento = date.fromisoformat(data_lancamento[:10])
        except ValueError:
            return False
    return data_lancamento > hoje_brasilia() - timedelta(days=DIAS_LANCAMENTO)


def calcular_desconto(rank, nivel, lancamento):
    """Devolve (desconto_total_%, desconto_platina_%) já com o teto aplicado."""
    platina = 0 if lancamento else nivel * DESCONTO_POR_NIVEL
    rank_pct = max(0, min(int(rank or 0), DESCONTO_RANK_MAXIMO))
    total = 100 * (1 - (1 - platina / 100) * (1 - rank_pct / 100))
    return round(min(total, DESCONTO_TOTAL_MAXIMO), 2), platina


def preco_com_desconto(preco_base, desconto_total):
    return round(float(preco_base or 0) * (1 - desconto_total / 100), 2)


def limitar_cashback(cashback, valor_pago):
    """Cashback nunca passa de metade do que o cliente pagou no aluguel.
    Locações antigas (antes do Platina) não têm valor_pago gravado: ficam como
    estavam."""
    if valor_pago is None:
        return cashback
    return min(float(cashback), round(float(valor_pago) * CASHBACK_MAX_FRACAO, 2))
