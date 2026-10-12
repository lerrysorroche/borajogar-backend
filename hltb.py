import os
import threading
import time
from datetime import datetime

from database import get_db_connection

# A cada quantos dias um jogo é reconsultado pelo agendamento diário.
INTERVALO_DIAS = int(os.getenv("HLTB_INTERVALO_DIAS", "7"))

# Atualização sem ninguém olhando não pode gravar o jogo errado: abaixo disso o
# título é tratado como "não encontrado" e fica para o admin conferir à mão.
SIMILARIDADE_MINIMA_AUTOMATICA = 0.85

# Pausa entre buscas, para não martelar um site que não tem API oficial.
PAUSA_ENTRE_BUSCAS = 1.5
# Se o HowLongToBeat falhar tantas vezes seguidas, a rodada é abortada (site
# fora do ar ou bloqueando o servidor) em vez de insistir em todos os jogos.
MAX_ERROS_SEGUIDOS = 3

_trava = threading.Lock()
estado = {"rodando": False}


def formatar_horas(horas):
    # 0 = o site não tem esse dado (comum em jogo que ainda não saiu).
    return f"{max(1, round(horas))}h" if horas and horas >= 0.5 else ""


def extrair_dados(resultado):
    return {
        "nome": resultado.game_name,
        "similaridade": round(resultado.similarity, 2),
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
    from howlongtobeatpy import HowLongToBeat

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
                resultados = HowLongToBeat().search(titulo)
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

            melhor = max(resultados, key=lambda r: r.similarity) if resultados else None
            if not melhor or melhor.similarity < SIMILARIDADE_MINIMA_AUTOMATICA:
                estado["nao_encontrados"].append(titulo)
                cursor.execute(
                    "UPDATE jogos SET hltb_atualizado_em = CURRENT_TIMESTAMP WHERE id = %s",
                    (jogo_id,),
                )
                conn.commit()
                time.sleep(PAUSA_ENTRE_BUSCAS)
                continue

            dados = extrair_dados(melhor)
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
