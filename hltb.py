import difflib
import os
import re
import threading
import time
import unicodedata
from datetime import datetime

from database import get_db_connection

# A cada quantos dias um jogo é reconsultado pelo agendamento diário.
INTERVALO_DIAS = int(os.getenv("HLTB_INTERVALO_DIAS", "7"))

# Atualização sem ninguém olhando não pode gravar o jogo errado: abaixo disso o
# título é tratado como "não encontrado" e fica para o admin conferir à mão.
# 0.9 separa "Control Resonant" de "Control: Resonant" (iguais depois de
# normalizar) de "Dispatch" vs "Dispatcher" (0.89, jogos diferentes).
SIMILARIDADE_MINIMA_AUTOMATICA = 0.9

# Pausa entre buscas, para não martelar um site que não tem API oficial.
PAUSA_ENTRE_BUSCAS = 1.5
# Se o HowLongToBeat falhar tantas vezes seguidas, a rodada é abortada (site
# fora do ar ou bloqueando o servidor) em vez de insistir em todos os jogos.
MAX_ERROS_SEGUIDOS = 3

_trava = threading.Lock()
estado = {"rodando": False}


# Palavras de "edição" que a locadora usa no título e o HowLongToBeat não tem
# (ele lista o jogo pelo nome simples). Só vocabulário conhecido é removido, para
# nunca cortar parte do nome do jogo ("Spider-Man 2 Deluxe Edition" vira
# "Spider-Man 2", não "Spider-Man").
_PALAVRA_EDICAO = (
    r"(?:digital|standard|deluxe?|ultimate|complete|definitive|gold|premium|special|"
    r"collector'?s|game of the year|goty|anniversary|limited|launch|day[ -]one|"
    r"legendary|enhanced|cross[- ]?gen|ps4|ps5|and|&|\+|the)"
)
_SEPARADOR = r"\s*(?:[:\-–—(]\s*)?"
_SUFIXO_EDICAO = re.compile(
    _SEPARADOR
    + r"(?:"
    + _PALAVRA_EDICAO
    + r"\s+)*(?:edition|bundle|version|pack)\)?\s*$",
    re.IGNORECASE,
)
_SUFIXO_SOLTO = re.compile(
    _SEPARADOR + r"(?:deluxe|ultimate|complete|premium|goty|game of the year)\)?\s*$",
    re.IGNORECASE,
)


_SUFIXO_PLATAFORMA = re.compile(
    r"\s*[(\[]?\s*ps[45](?:\s*(?:&|/|and)\s*ps[45])?\s*[)\]]?\s*$", re.IGNORECASE
)


def remover_edicao(titulo):
    """
    'Dispatch: Deluxe Edition' -> 'Dispatch' (e '... (PS5)' -> sem a plataforma).
    Sem sufixo de edição/plataforma, não muda nada.
    """
    limpo = _SUFIXO_PLATAFORMA.sub("", titulo).strip()
    limpo = _SUFIXO_EDICAO.sub("", limpo).strip()
    limpo = _SUFIXO_SOLTO.sub("", limpo).strip()
    return limpo or titulo


def normalizar(texto):
    """Minúsculas, sem acento e sem pontuação: 'CONTROL Resonant' == 'Control: Resonant'."""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", texto.casefold()).split())


def pontuar(nome_hltb, variantes):
    """Semelhança (0 a 1) entre o nome no HowLongToBeat e a melhor variante do título."""
    alvo = normalizar(nome_hltb)
    return max(
        difflib.SequenceMatcher(None, alvo, normalizar(v)).ratio() for v in variantes
    )


def buscar_melhor(titulo):
    """
    Procura o jogo no HowLongToBeat e devolve (resultado, semelhança), ou
    (None, 0) se o site respondeu mas não tem nada parecido.

    Tenta o título como está e, se ele tiver sufixo de edição (Deluxe Edition,
    Complete Edition...), também sem ele, porque o site só acha o nome simples.
    Compara os nomes sem diferenciar maiúscula/pontuação/acento — a comparação
    da própria biblioteca diferencia, e dava 0.61 para "CONTROL Resonant" vs
    "Control: Resonant". Se o site não responder, levanta RuntimeError (assim
    quem chama não confunde "site fora do ar" com "jogo não existe").
    """
    from howlongtobeatpy import HowLongToBeat

    variantes = [titulo]
    sem_edicao = remover_edicao(titulo)
    if sem_edicao.lower() != titulo.lower():
        variantes.append(sem_edicao)

    melhor, nota_melhor = None, 0.0
    for i, variante in enumerate(variantes):
        if i:
            time.sleep(PAUSA_ENTRE_BUSCAS)
        resultados = HowLongToBeat(input_minimum_similarity=0.0).search(
            variante, similarity_case_sensitive=False
        )
        if resultados is None:
            raise RuntimeError("o HowLongToBeat não respondeu à busca")
        for r in resultados:
            nota = pontuar(r.game_name, variantes)
            if nota > nota_melhor:
                melhor, nota_melhor = r, nota
        if nota_melhor >= SIMILARIDADE_MINIMA_AUTOMATICA:
            break
    return melhor, nota_melhor


def formatar_horas(horas):
    # 0 = o site não tem esse dado (comum em jogo que ainda não saiu).
    return f"{max(1, round(horas))}h" if horas and horas >= 0.5 else ""


def extrair_dados(resultado, similaridade):
    return {
        "nome": resultado.game_name,
        "similaridade": round(similaridade, 2),
        "tempo_jogo": formatar_horas(resultado.main_story),
        "tempo_platina": formatar_horas(resultado.completionist),
        # O HowLongToBeat dá a nota de 0 a 100; o site mostra de 0 a 10.
        "nota": round((resultado.review_score or 0) / 10, 1),
    }


def atualizar_jogos(forcar=False):
    """
    Reconsulta o HowLongToBeat e atualiza tempo da história, tempo 100% e nota
    dos jogos do catálogo. Roda em segundo plano (thread do agendamento ou do
    botão do Painel Admin) e vai gravando o andamento em 'estado'.

    forcar=True ignora o intervalo e refaz todos os jogos; forcar=False pega só
    quem nunca foi consultado ou foi há mais de INTERVALO_DIAS dias.

    Só sobrescreve um campo quando o HowLongToBeat devolveu valor para ele:
    um dado que o site não tem (ou jogo que ainda não saiu) nunca apaga o que já
    está no catálogo. Retorna False se já havia outra rodada em andamento.
    """
    # Importa antes de pegar a trava: se a biblioteca estiver quebrada, o erro
    # sobe sem deixar a trava presa.
    import howlongtobeatpy  # noqa: F401

    if not _trava.acquire(blocking=False):
        return False

    estado.clear()
    estado.update(
        rodando=True,
        forcado=forcar,
        inicio=datetime.now().isoformat(timespec="seconds"),
        fim=None,
        total=0,
        processados=0,
        atualizados=0,
        sem_dados=[],
        nao_encontrados=[],
        erros=0,
        abortado=False,
    )
    conn = cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if forcar:
            cursor.execute("SELECT id, titulo FROM jogos ORDER BY id")
        else:
            cursor.execute(
                "SELECT id, titulo FROM jogos WHERE hltb_atualizado_em IS NULL "
                "OR hltb_atualizado_em < CURRENT_TIMESTAMP - %s * INTERVAL '1 day' "
                "ORDER BY id",
                (INTERVALO_DIAS,),
            )
        jogos = cursor.fetchall()

        estado["total"] = len(jogos)

        erros_seguidos = 0
        for jogo_id, titulo in jogos:
            try:
                melhor, similaridade = buscar_melhor(titulo)
                erros_seguidos = 0
            except Exception as e:
                erros_seguidos += 1
                estado["erros"] += 1
                print(f"HLTB: erro ao buscar {titulo!r}: {e}")
                if erros_seguidos >= MAX_ERROS_SEGUIDOS:
                    estado["abortado"] = True
                    break
                continue
            finally:
                estado["processados"] += 1

            if not melhor or similaridade < SIMILARIDADE_MINIMA_AUTOMATICA:
                estado["nao_encontrados"].append(titulo)
                cursor.execute(
                    "UPDATE jogos SET hltb_atualizado_em = CURRENT_TIMESTAMP WHERE id = %s",
                    (jogo_id,),
                )
                conn.commit()
                time.sleep(PAUSA_ENTRE_BUSCAS)
                continue

            dados = extrair_dados(melhor, similaridade)
            campos = []
            valores = []
            for coluna, chave in (
                ("tempo_jogo", "tempo_jogo"),
                ("tempo_platina", "tempo_platina"),
                ("nota", "nota"),
            ):
                if dados[chave]:
                    campos.append(f"{coluna} = %s")
                    valores.append(dados[chave])
            if campos:
                estado["atualizados"] += 1
            else:
                estado["sem_dados"].append(titulo)

            cursor.execute(
                "UPDATE jogos SET "
                + ", ".join(campos + ["hltb_atualizado_em = CURRENT_TIMESTAMP"])
                + " WHERE id = %s",
                (*valores, jogo_id),
            )
            conn.commit()
            time.sleep(PAUSA_ENTRE_BUSCAS)
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erro na atualização HowLongToBeat: {e}")
        estado["erro_geral"] = str(e)
    finally:
        estado["rodando"] = False
        estado["fim"] = datetime.now().isoformat(timespec="seconds")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        _trava.release()
    return True
