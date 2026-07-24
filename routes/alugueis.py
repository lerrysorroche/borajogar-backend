from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import pyotp

from database import get_db_connection
from auth import verificar_admin
from models import NovaLocacao, NovaReserva, CancelarReserva, DevolucaoRequest

router = APIRouter(tags=["Alugueis e Reservas"])

# ==============================================================================
# FLUXO DE LOCAÇÃO E ESTOQUE
# ==============================================================================


@router.post("/locacoes", status_code=201)
def realizar_locacao(locacao: NovaLocacao):
    """
    [C] O Caixa da Loja.
    Recebe o pedido da vitrine, checa se a vaga específica (Primária ou Secundária)
    está disponível, calcula o desconto do Rank VIP, desconta o saldo e entrega as credenciais.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT titulo, preco_aluguel, preco_aluguel_14, preco_secundaria, preco_secundaria_14 FROM jogos WHERE id = %s",
            (locacao.jogo_id,),
        )
        jogo_info = cursor.fetchone()

        # Roteamento inteligente de preços e status baseado na vaga
        if locacao.tipo_slot == "PRIMARIA":
            preco_base = (
                jogo_info["preco_aluguel_14"]
                if locacao.dias_aluguel == 14
                else jogo_info["preco_aluguel"]
            )
            coluna_status = "status_primaria"
        else:
            preco_base = (
                jogo_info["preco_secundaria_14"]
                if locacao.dias_aluguel == 14
                else jogo_info["preco_secundaria"]
            )
            coluna_status = "status_secundaria"

        # Trava Financeira + APLICAÇÃO DO DESCONTO VIP
        cursor.execute(
            "SELECT saldo, rank FROM usuarios WHERE id = %s", (locacao.utilizador_id,)
        )
        usr_info = cursor.fetchone()
        saldo = usr_info["saldo"]
        rank_atual = usr_info.get("rank", 0)

        # Calcula o desconto (Máximo 20%)
        desconto_percentual = min(rank_atual, 20)
        valor_desconto = preco_base * (desconto_percentual / 100.0)
        preco_final = preco_base - valor_desconto

        if saldo < preco_final:
            raise HTTPException(
                status_code=402,
                detail="Saldo insuficiente na carteira para o valor VIP.",
            )

        # Busca a primeira conta onde O SLOT ESPECÍFICO está livre e já bloqueia ele
        cursor.execute(
            f"UPDATE contas_psn SET {coluna_status} = 'ALUGADA' WHERE id = (SELECT id FROM contas_psn WHERE jogo_id = %s AND {coluna_status} = 'DISPONIVEL' LIMIT 1) RETURNING id, email_login, senha_login;",
            (locacao.jogo_id,),
        )
        conta = cursor.fetchone()

        # Trava de Concorrência (Se dois clientes clicarem ao mesmo tempo no último jogo)
        if not conta:
            raise HTTPException(
                status_code=404,
                detail=f"A vaga {locacao.tipo_slot} deste jogo acabou de ser alugada por outra pessoa. Atualize a página.",
            )

        # Desconta o dinheiro (com o desconto VIP) e registra o recibo e a transação
        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s",
            (preco_final, locacao.utilizador_id),
        )
        cursor.execute(
            "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status, tipo_slot) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA', %s) RETURNING id, data_fim;",
            (
                locacao.utilizador_id,
                conta["id"],
                locacao.dias_aluguel,
                locacao.tipo_slot,
            ),
        )
        recibo = cursor.fetchone()

        # No extrato, você avisa que ele pagou com desconto
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)",
            (
                locacao.utilizador_id,
                preco_final,
                f"Aluguel {locacao.tipo_slot} ({locacao.dias_aluguel}d): {jogo_info['titulo']} - Rank {rank_atual}",
            ),
        )
        conn.commit()
        return {
            "mensagem": f"Vaga {locacao.tipo_slot} alugada com sucesso no valor VIP!",
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


@router.post("/devolver")
def devolver_jogo(dados: DevolucaoRequest):
    """
    [U] A Máquina de Cashback (Atualizada com Zero Trust).
    Encerra a locação, injeta o bônus no limbo (cashback_pendente) com status 'PENDENTE',
    e envia APENAS o slot devolvido para a aba de manutenção do Admin.
    O cliente só ganha o Rank e o Saldo se o Admin aprovar na verificação da Sony.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT status, conta_psn_id, tipo_slot, EXTRACT(EPOCH FROM (data_fim - CURRENT_TIMESTAMP))/3600 AS horas FROM locacoes WHERE id = %s AND utilizador_id = %s",
            (dados.locacao_id, dados.utilizador_id),
        )
        loc = cursor.fetchone()
        if not loc or loc["status"] != "ATIVA":
            raise HTTPException(
                status_code=400, detail="Locação não encontrada ou já expirada."
            )

        cursor.execute("SELECT valor_por_dia FROM configuracoes LIMIT 1")
        config = cursor.fetchone()
        valor_base = float(config["valor_por_dia"]) if config else 2.0

        # Regra Financeira: Primária = Ganha por dia restante | Secundária = Só ganha a taxa fixa (2.00)
        if loc["tipo_slot"] == "SECUNDARIA":
            cashback_recompensa = valor_base
            coluna_status = "status_secundaria"
        else:
            horas = float(loc["horas"])
            dias_restantes = max(0, int(horas // 24))
            cashback_recompensa = valor_base + (dias_restantes * valor_base)
            coluna_status = "status_primaria"

        # A NOVA TRAVA: Adicionamos o status_beneficio = 'PENDENTE'
        cursor.execute(
            "UPDATE locacoes SET status = 'EXPIRADA', cashback_pendente = %s, status_beneficio = 'PENDENTE', data_fim = CURRENT_TIMESTAMP WHERE id = %s",
            (cashback_recompensa, dados.locacao_id),
        )

        # Envia a notificação para o Admin trocar a senha apenas desse slot específico
        cursor.execute(
            f"UPDATE contas_psn SET {coluna_status} = 'MANUTENCAO' WHERE id = %s",
            (loc["conta_psn_id"],),
        )
        conn.commit()

        # MENSAGEM ALTERADA: Educando o cliente sobre a auditoria
        return {
            "mensagem": "Devolução iniciada! A conta está em análise. O seu Rank e Cashback cairão na carteira assim que o sistema confirmar a desativação."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/meus-alugueis/{usuario_id}")
def buscar_alugueis_usuario(usuario_id: int):
    """
    [R] Monta a interface "Meus Acessos" (Cards Verdes).
    Retorna os dados da conta e avisa o React qual é o tipo de slot (Para o React mostrar "Vaga Primária" ou Secundária).
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT l.id AS locacao_id, j.titulo AS jogo, c.email_login, c.senha_login, l.data_fim, l.status, l.tipo_slot FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id JOIN jogos j ON c.jogo_id = j.id WHERE l.utilizador_id = %s ORDER BY l.data_fim DESC;",
        (usuario_id,),
    )
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return resultados


# ==============================================================================
# SEGURANÇA E ZERO TRUST (2FA)
# ==============================================================================


@router.get("/gerar-2fa/{locacao_id}/{usuario_id}")
def gerar_codigo_2fa(locacao_id: int, usuario_id: int):
    """
    [R/U] O Autenticador.
    Verifica se o cliente da vaga SECUNDÁRIA já apertou este botão alguma vez na vida.
    Se sim, joga um Erro 403 e tranca a porta para evitar o "Golpe da Sony".
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT c.mfa_secret, l.tipo_slot, l.codigo_2fa_usado FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE l.id = %s AND l.utilizador_id = %s AND l.status = 'ATIVA'",
            (locacao_id, usuario_id),
        )
        resultado = cursor.fetchone()

        if not resultado or not resultado["mfa_secret"]:
            raise HTTPException(
                status_code=404, detail="Conta sem 2FA ou acesso já expirado."
            )

        # A Trava de Segurança
        if resultado["tipo_slot"] == "SECUNDARIA" and resultado["codigo_2fa_usado"]:
            raise HTTPException(
                status_code=403,
                detail="ALERTA: O código de acesso da vaga Secundária só pode ser gerado 1 ÚNICA VEZ. Caso tenha ocorrido um erro na digitação no seu videogame, acione o suporte.",
            )

        # Queima o cartucho (Salva que ele apertou o botão)
        cursor.execute(
            "UPDATE locacoes SET codigo_2fa_usado = TRUE WHERE id = %s", (locacao_id,)
        )
        conn.commit()

        return {"codigo": pyotp.TOTP(resultado["mfa_secret"]).now()}
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# FLUXO DA FILA DE ESPERA (RESERVAS)
# ==============================================================================


@router.post("/reservas", status_code=201)
def entrar_fila(reserva: NovaReserva):
    """
    [C] Coloca o cliente na fila de espera.
    Isola a lógica completamente: se o cliente clica para entrar na fila da Secundária,
    ele entra em um "túnel" de espera diferente do cliente que quer a Primária.
    AGORA COM DESCONTO VIP E NOVA LÓGICA DE PRIORIDADE!
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Verifica se já está na fila
        cursor.execute(
            "SELECT id FROM fila_espera WHERE utilizador_id = %s AND jogo_id = %s AND tipo_slot = %s AND status = 'AGUARDANDO'",
            (reserva.utilizador_id, reserva.jogo_id, reserva.tipo_slot),
        )
        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail=f"Você já está na fila para a vaga {reserva.tipo_slot} deste jogo.",
            )

        # 2. Puxa os dados do jogo
        cursor.execute(
            "SELECT titulo, preco_aluguel, preco_aluguel_14, preco_secundaria, preco_secundaria_14, data_lancamento FROM jogos WHERE id = %s",
            (reserva.jogo_id,),
        )
        jogo_info = cursor.fetchone()

        if reserva.tipo_slot == "PRIMARIA":
            preco_base = (
                jogo_info["preco_aluguel_14"]
                if reserva.dias_aluguel == 14
                else jogo_info["preco_aluguel"]
            )
        else:
            preco_base = (
                jogo_info["preco_secundaria_14"]
                if reserva.dias_aluguel == 14
                else jogo_info["preco_secundaria"]
            )

        # 3. Puxa Saldo e Rank do Cliente
        # (Nota: estou usando 'utilizadores', assumindo que é o nome da sua tabela de usuários)
        cursor.execute(
            "SELECT saldo, rank FROM utilizadores WHERE id = %s",
            (reserva.utilizador_id,),
        )
        usr_info = cursor.fetchone()
        saldo = usr_info["saldo"]
        rank_atual = usr_info.get("rank", 0)

        # 4. APLICAÇÃO DO DESCONTO VIP MÁXIMO DE 20%
        desconto_percentual = min(rank_atual, 20)
        valor_desconto = preco_base * (desconto_percentual / 100.0)
        preco_final = preco_base - valor_desconto

        if saldo < preco_final:
            raise HTTPException(
                status_code=402,
                detail="Saldo insuficiente na carteira para entrar na fila.",
            )

        # 5. Cobra o valor com desconto e Enfileira
        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s",
            (preco_final, reserva.utilizador_id),
        )
        cursor.execute(
            "INSERT INTO fila_espera (utilizador_id, jogo_id, dias_aluguel, tipo_slot) VALUES (%s, %s, %s, %s) RETURNING id",
            (
                reserva.utilizador_id,
                reserva.jogo_id,
                reserva.dias_aluguel,
                reserva.tipo_slot,
            ),
        )
        reserva_id = cursor.fetchone()["id"]

        # Extrato avisando que o preço teve desconto do rank
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)",
            (
                reserva.utilizador_id,
                preco_final,
                f"Fila {reserva.tipo_slot} ({reserva.dias_aluguel}d): {jogo_info['titulo']} - Rank {rank_atual}",
            ),
        )

        # ==============================================================================
        # 6. Lógica de Rank VIP (Aplica apenas dentro do mesmo tipo de Slot)
        # ==============================================================================
        hoje_str = datetime.now().strftime("%Y-%m-%d")
        eh_pre_venda = (
            jogo_info["data_lancamento"]
            and str(jogo_info["data_lancamento"]) >= hoje_str
        )

        if eh_pre_venda:
            # Puxa a data limite
            cursor.execute(
                "SELECT data_lancamento, (SELECT MIN(data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = %s AND l.status = 'ATIVA' AND l.tipo_slot = %s) as prox FROM jogos WHERE id = %s",
                (reserva.jogo_id, reserva.tipo_slot, reserva.jogo_id),
            )
            jogo_meta = cursor.fetchone()

            base_date = datetime.now()
            if jogo_meta["data_lancamento"]:
                dl = datetime.combine(jogo_meta["data_lancamento"], datetime.min.time())
                if dl > base_date:
                    base_date = dl
            if jogo_meta["prox"] and jogo_meta["prox"] > base_date:
                base_date = jogo_meta["prox"]

            # NOVIDADE: A query que detecta quem foi ultrapassado agora compara o 'rank' direto!
            cursor.execute(
                """
                SELECT f.id, f.utilizador_id,
                (SELECT COALESCE(SUM(dias_aluguel), 0) FROM fila_espera f2 WHERE f2.jogo_id = f.jogo_id AND f2.status = 'AGUARDANDO' AND f2.tipo_slot = f.tipo_slot AND f2.data_solicitacao < f.data_solicitacao AND f2.id != %s) as dias_frente_antes
                FROM fila_espera f 
                WHERE f.jogo_id = %s AND f.status = 'AGUARDANDO' AND f.tipo_slot = %s AND f.utilizador_id != %s
                AND (SELECT rank FROM utilizadores WHERE id = f.utilizador_id) < %s
                """,
                (
                    reserva_id,
                    reserva.jogo_id,
                    reserva.tipo_slot,
                    reserva.utilizador_id,
                    rank_atual,
                ),
            )
            bumped = cursor.fetchall()

            # Avisa os clientes que foram ultrapassados na fila pelo sistema VIP
            for b in bumped:
                data_antiga = base_date + timedelta(days=b["dias_frente_antes"])
                data_nova = data_antiga + timedelta(days=reserva.dias_aluguel)
                msg = f"Devido à prioridade de Rank VIP, a previsão da sua Fila ({reserva.tipo_slot}) do jogo {jogo_info['titulo']} mudou para {data_nova.strftime('%d/%m/%Y')}."
                cursor.execute(
                    "INSERT INTO notificacoes (utilizador_id, reserva_id, jogo, mensagem) VALUES (%s, %s, %s, %s)",
                    (b["utilizador_id"], b["id"], jogo_info["titulo"], msg),
                )

        conn.commit()
        return {
            "mensagem": f"Você entrou na Fila ({reserva.tipo_slot}) com Desconto VIP aplicado!"
        }
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
    """
    [D] O Desistente.
    Cliente sai da fila por conta própria (na aba Meus Acessos ou via Notificação)
    e o dinheiro volta instantaneamente para a carteira dele.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT f.jogo_id, f.tipo_slot, j.titulo FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id WHERE f.id = %s AND f.utilizador_id = %s AND f.status = 'AGUARDANDO'",
            (dados.reserva_id, dados.utilizador_id),
        )
        res = cursor.fetchone()
        if not res:
            raise HTTPException(status_code=400, detail="Reserva não encontrada.")

        cursor.execute(
            "SELECT valor FROM transacoes WHERE utilizador_id = %s AND tipo = 'SAIDA' AND descricao LIKE %s ORDER BY id DESC LIMIT 1",
            (dados.utilizador_id, f"Fila {res['tipo_slot']}%{res['titulo']}%"),
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
                    f"💸 Estorno de Fila: {res['titulo']}",
                ),
            )

        cursor.execute("DELETE FROM fila_espera WHERE id = %s", (dados.reserva_id,))
        if dados.notificacao_id > 0:
            cursor.execute(
                "UPDATE notificacoes SET lida = TRUE WHERE id = %s",
                (dados.notificacao_id,),
            )

        conn.commit()
        return {"mensagem": "Fila cancelada e valor estornado para sua carteira!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/minhas-reservas/{usuario_id}")
def buscar_reservas_usuario(usuario_id: int):
    """
    [R] O Termômetro do Cliente.
    Calcula a estimativa de tempo que o cliente vai demorar para pegar o jogo.
    Agora isolado: se ele reservou Secundária, só soma os dias de quem está na frente dele na fila Secundária.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT f.id AS reserva_id, j.id AS jogo_id, j.titulo AS jogo, j.data_lancamento, f.data_solicitacao, f.status, f.tipo_slot,
        (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA' AND l.tipo_slot = f.tipo_slot) AS proxima_devolucao
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
                WHERE jogo_id = %s AND status = 'AGUARDANDO' AND tipo_slot = %s AND (
                    (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') > 
                    (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA')
                    OR ((SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') = 
                     (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA') AND data_solicitacao < %s)
                )
            """,
                (
                    r["jogo_id"],
                    r["tipo_slot"],
                    usuario_id,
                    usuario_id,
                    r["data_solicitacao"],
                ),
            )
        else:
            cursor.execute(
                "SELECT COALESCE(SUM(dias_aluguel), 0) as dias_frente FROM fila_espera WHERE jogo_id = %s AND tipo_slot = %s AND status = 'AGUARDANDO' AND data_solicitacao < %s",
                (r["jogo_id"], r["tipo_slot"], r["data_solicitacao"]),
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
