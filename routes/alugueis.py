from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import pyotp

from database import get_db_connection
from auth import verificar_admin
from models import NovaLocacao, NovaReserva, CancelarReserva, DevolucaoRequest

router = APIRouter(tags=["Alugueis e Reservas"])


@router.post("/locacoes", status_code=201)
def realizar_locacao(locacao: NovaLocacao):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT titulo, preco_aluguel, preco_aluguel_14 FROM jogos WHERE id = %s",
            (locacao.jogo_id,),
        )
        jogo_info = cursor.fetchone()
        preco = (
            jogo_info["preco_aluguel_14"]
            if locacao.dias_aluguel == 14
            else jogo_info["preco_aluguel"]
        )

        cursor.execute(
            "SELECT saldo FROM utilizadores WHERE id = %s", (locacao.utilizador_id,)
        )
        saldo = cursor.fetchone()["saldo"]
        if saldo < preco:
            raise HTTPException(status_code=402, detail="Saldo insuficiente.")

        cursor.execute(
            "UPDATE contas_psn SET status = 'ALUGADA' WHERE id = (SELECT id FROM contas_psn WHERE jogo_id = %s AND status ILIKE 'DISPONIVEL' LIMIT 1) RETURNING id, email_login, senha_login;",
            (locacao.jogo_id,),
        )
        conta = cursor.fetchone()
        if not conta:
            raise HTTPException(
                status_code=404, detail="Não há contas disponíveis no momento."
            )

        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s",
            (preco, locacao.utilizador_id),
        )
        cursor.execute(
            "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA') RETURNING id, data_fim;",
            (locacao.utilizador_id, conta["id"], locacao.dias_aluguel),
        )
        recibo = cursor.fetchone()
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)",
            (
                locacao.utilizador_id,
                preco,
                f"Aluguel ({locacao.dias_aluguel}d): {jogo_info['titulo']}",
            ),
        )
        conn.commit()
        return {
            "mensagem": "Aluguel realizado!",
            "pedido_id": recibo["id"],
            "data_devolucao": recibo["data_fim"],
            "psn_email": conta["email_login"],
            "psn_senha": conta["senha_login"],
        }
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/meus-alugueis/{usuario_id}")
def buscar_alugueis_usuario(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT l.id AS locacao_id, j.titulo AS jogo, c.email_login, c.senha_login, l.data_fim, l.status FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id JOIN jogos j ON c.jogo_id = j.id WHERE l.utilizador_id = %s ORDER BY l.data_fim DESC;",
        (usuario_id,),
    )
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return resultados


@router.post("/devolver")
def devolver_jogo(dados: DevolucaoRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT status, conta_psn_id, EXTRACT(EPOCH FROM (data_fim - CURRENT_TIMESTAMP))/3600 AS horas FROM locacoes WHERE id = %s AND utilizador_id = %s",
            (dados.locacao_id, dados.utilizador_id),
        )
        loc = cursor.fetchone()
        if not loc or loc["status"] != "ATIVA":
            raise HTTPException(
                status_code=400, detail="Locação não encontrada ou já expirada."
            )

        cursor.execute(
            "SELECT devolucao_dinamica, valor_por_dia FROM configuracoes LIMIT 1"
        )
        config = cursor.fetchone()
        cashback = 0.0
        if config and config["devolucao_dinamica"] and loc["horas"] > 24:
            dias_restantes = int(loc["horas"] // 24)
            cashback = dias_restantes * config["valor_por_dia"]

        cursor.execute(
            "UPDATE locacoes SET status = 'EXPIRADA', cashback_pendente = %s, data_fim = CURRENT_TIMESTAMP WHERE id = %s",
            (cashback, dados.locacao_id),
        )
        cursor.execute(
            "UPDATE contas_psn SET status = 'MANUTENCAO' WHERE id = %s",
            (loc["conta_psn_id"],),
        )
        conn.commit()
        return {"mensagem": "Devolução solicitada! O jogo foi para análise."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/reservas", status_code=201)
def entrar_fila(reserva: NovaReserva):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT id FROM fila_espera WHERE utilizador_id = %s AND jogo_id = %s AND status = 'AGUARDANDO'",
            (reserva.utilizador_id, reserva.jogo_id),
        )
        if cursor.fetchone():
            raise HTTPException(
                status_code=400, detail="Você já está na fila de espera para este jogo!"
            )

        cursor.execute(
            "SELECT titulo, preco_aluguel, preco_aluguel_14, data_lancamento FROM jogos WHERE id = %s",
            (reserva.jogo_id,),
        )
        jogo_info = cursor.fetchone()
        preco = (
            jogo_info["preco_aluguel_14"]
            if reserva.dias_aluguel == 14
            else jogo_info["preco_aluguel"]
        )

        cursor.execute(
            "SELECT saldo FROM utilizadores WHERE id = %s", (reserva.utilizador_id,)
        )
        if cursor.fetchone()["saldo"] < preco:
            raise HTTPException(status_code=402, detail=f"Saldo insuficiente.")

        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s",
            (preco, reserva.utilizador_id),
        )
        cursor.execute(
            "INSERT INTO fila_espera (utilizador_id, jogo_id, dias_aluguel) VALUES (%s, %s, %s) RETURNING id",
            (reserva.utilizador_id, reserva.jogo_id, reserva.dias_aluguel),
        )
        reserva_id = cursor.fetchone()["id"]
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)",
            (
                reserva.utilizador_id,
                preco,
                f"Reserva na Fila ({reserva.dias_aluguel}d): {jogo_info['titulo']}",
            ),
        )

        hoje_str = datetime.now().strftime("%Y-%m-%d")
        eh_pre_venda = (
            jogo_info["data_lancamento"]
            and str(jogo_info["data_lancamento"]) >= hoje_str
        )

        if eh_pre_venda:
            cursor.execute(
                "SELECT COUNT(*) as qtd FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA'",
                (reserva.utilizador_id,),
            )
            meus_alugueis_qtd = cursor.fetchone()["qtd"]

            if meus_alugueis_qtd > 0:
                cursor.execute(
                    "SELECT data_lancamento, (SELECT MIN(data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = %s AND l.status = 'ATIVA') as prox FROM jogos WHERE id = %s",
                    (reserva.jogo_id, reserva.jogo_id),
                )
                jogo_meta = cursor.fetchone()

                base_date = datetime.now()
                if jogo_meta["data_lancamento"]:
                    dl = datetime.combine(
                        jogo_meta["data_lancamento"], datetime.min.time()
                    )
                    if dl > base_date:
                        base_date = dl
                if jogo_meta["prox"] and jogo_meta["prox"] > base_date:
                    base_date = jogo_meta["prox"]

                cursor.execute(
                    """
                    SELECT f.id, f.utilizador_id,
                    (SELECT COALESCE(SUM(dias_aluguel), 0) FROM fila_espera f2 WHERE f2.jogo_id = f.jogo_id AND f2.status = 'AGUARDANDO' AND f2.data_solicitacao < f.data_solicitacao AND f2.id != %s) as dias_frente_antes
                    FROM fila_espera f 
                    WHERE f.jogo_id = %s AND f.status = 'AGUARDANDO' AND f.utilizador_id != %s
                    AND (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = f.utilizador_id AND status = 'EXPIRADA') < %s
                """,
                    (
                        reserva_id,
                        reserva.jogo_id,
                        reserva.utilizador_id,
                        meus_alugueis_qtd,
                    ),
                )
                bumped = cursor.fetchall()

                for b in bumped:
                    data_antiga = base_date + timedelta(days=b["dias_frente_antes"])
                    data_nova = data_antiga + timedelta(days=reserva.dias_aluguel)
                    msg = f"Devido à prioridade de Rank, a previsão do seu jogo {jogo_info['titulo']} mudou de {data_antiga.strftime('%d/%m/%Y')} para {data_nova.strftime('%d/%m/%Y')} (+{reserva.dias_aluguel} dias)."
                    cursor.execute(
                        "INSERT INTO notificacoes (utilizador_id, reserva_id, jogo, mensagem) VALUES (%s, %s, %s, %s)",
                        (b["utilizador_id"], b["id"], jogo_info["titulo"], msg),
                    )

        conn.commit()
        return {"mensagem": "Reserva confirmada! Valor descontado da sua carteira."}
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/reservas/cancelar")
def cancelar_reserva(dados: CancelarReserva):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT f.jogo_id, j.titulo FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id WHERE f.id = %s AND f.utilizador_id = %s AND f.status = 'AGUARDANDO'",
            (dados.reserva_id, dados.utilizador_id),
        )
        res = cursor.fetchone()
        if not res:
            raise HTTPException(status_code=400, detail="Reserva não encontrada.")

        cursor.execute(
            "SELECT valor FROM transacoes WHERE utilizador_id = %s AND tipo = 'SAIDA' AND descricao LIKE %s ORDER BY id DESC LIMIT 1",
            (dados.utilizador_id, f"Reserva na Fila%{res['titulo']}%"),
        )
        trans = cursor.fetchone()
        reembolso = trans["valor"] if trans else 0.0

        if reembolso > 0:
            cursor.execute(
                "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s",
                (reembolso, dados.utilizador_id),
            )
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)",
                (
                    dados.utilizador_id,
                    reembolso,
                    f"💸 Estorno de Reserva: {res['titulo']}",
                ),
            )

        cursor.execute("DELETE FROM fila_espera WHERE id = %s", (dados.reserva_id,))
        if dados.notificacao_id > 0:
            cursor.execute(
                "UPDATE notificacoes SET lida = TRUE WHERE id = %s",
                (dados.notificacao_id,),
            )
        conn.commit()
        return {"mensagem": "Reserva cancelada e valor estornado para sua carteira!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/minhas-reservas/{usuario_id}")
def buscar_reservas_usuario(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT f.id AS reserva_id, j.id AS jogo_id, j.titulo AS jogo, j.data_lancamento, f.data_solicitacao, f.status,
        (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA') AS proxima_devolucao
        FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id 
        WHERE f.utilizador_id = %s AND f.status = 'AGUARDANDO' ORDER BY f.data_solicitacao ASC;
    """,
        (usuario_id,),
    )
    reservas = cursor.fetchall()

    for r in reservas:
        hoje_str = datetime.now().strftime("%Y-%m-%d")
        eh_pre_venda = r["data_lancamento"] and str(r["data_lancamento"]) >= hoje_str

        if eh_pre_venda:
            cursor.execute(
                """
                SELECT COALESCE(SUM(dias_aluguel), 0) as dias_frente FROM fila_espera 
                WHERE jogo_id = %s AND status = 'AGUARDANDO' AND (
                    (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') > 
                    (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA')
                    OR ((SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') = 
                     (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA') AND data_solicitacao < %s)
                )
            """,
                (r["jogo_id"], usuario_id, usuario_id, r["data_solicitacao"]),
            )
        else:
            cursor.execute(
                "SELECT COALESCE(SUM(dias_aluguel), 0) as dias_frente FROM fila_espera WHERE jogo_id = %s AND status = 'AGUARDANDO' AND data_solicitacao < %s",
                (r["jogo_id"], r["data_solicitacao"]),
            )

        dias_frente = cursor.fetchone()["dias_frente"]
        base_date = datetime.now()
        if r["data_lancamento"]:
            dl = datetime.strptime(str(r["data_lancamento"]), "%Y-%m-%d")
            if dl > base_date:
                base_date = dl
        if r["proxima_devolucao"] and r["proxima_devolucao"] > base_date:
            base_date = r["proxima_devolucao"]

        est_date = base_date + timedelta(days=dias_frente)
        r["data_estimada_str"] = est_date.strftime("%d/%m/%Y")
    cursor.close()
    conn.close()
    return reservas


@router.get("/gerar-2fa/{locacao_id}/{usuario_id}")
def gerar_codigo_2fa(locacao_id: int, usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT c.mfa_secret FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE l.id = %s AND l.utilizador_id = %s AND l.status = 'ATIVA'",
        (locacao_id, usuario_id),
    )
    resultado = cursor.fetchone()
    cursor.close()
    conn.close()
    if not resultado or not resultado["mfa_secret"]:
        raise HTTPException(status_code=404, detail="Conta sem 2FA ou expirada.")
    return {"codigo": pyotp.TOTP(resultado["mfa_secret"]).now()}
