from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

from database import get_db_connection
from auth import verificar_admin
from models import (
    EditarClienteRequest,
    ConfigRequest,
    ContaPSNNova,
    AplicarMultaRequest,
    AjusteSaldoRequest,
    ResetSenhaRequest,
)

router = APIRouter(
    prefix="/admin", tags=["Admin"], dependencies=[Depends(verificar_admin)]
)


@router.post("/configuracoes")
def set_config(dados: ConfigRequest, admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM configuracoes LIMIT 1")
        if cursor.fetchone():
            cursor.execute(
                """
                UPDATE configuracoes 
                SET devolucao_dinamica = %s, 
                    valor_por_dia = %s, 
                    anuncio_ativo = %s, 
                    mensagem_anuncio = %s, 
                    banners_url = %s,
                    enquete_titulo = %s,
                    enquete_subtitulo = %s
                """,
                (
                    dados.devolucao_dinamica,
                    dados.valor_por_dia,
                    dados.anuncio_ativo,
                    dados.mensagem_anuncio,
                    dados.banners_url,
                    dados.enquete_titulo,
                    dados.enquete_subtitulo,
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO configuracoes 
                (devolucao_dinamica, valor_por_dia, anuncio_ativo, mensagem_anuncio, banners_url, enquete_titulo, enquete_subtitulo) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dados.devolucao_dinamica,
                    dados.valor_por_dia,
                    dados.anuncio_ativo,
                    dados.mensagem_anuncio,
                    dados.banners_url,
                    dados.enquete_titulo,
                    dados.enquete_subtitulo,
                ),
            )
        conn.commit()
        return {"mensagem": "Configurações salvas!"}

    except Exception as e:
        conn.rollback()  # Desfaz qualquer alteração pela metade se der erro
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # O finally garante que essas duas linhas rodem SEMPRE, mesmo com erro
        cursor.close()
        conn.close()


@router.get("/estatisticas")
def buscar_estatisticas_admin(
    periodo: str = "mes", admin_data=Depends(verificar_admin)
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    if periodo == "mes":
        data_inicio = hoje.replace(day=1, hour=0, minute=0, second=0)
    elif periodo == "30dias":
        data_inicio = hoje - timedelta(days=30)
    elif periodo == "ano":
        data_inicio = hoje.replace(month=1, day=1, hour=0, minute=0, second=0)
    else:
        data_inicio = datetime(2000, 1, 1)

    try:
        cursor.execute(
            "SELECT SUM(valor) as total FROM transacoes WHERE tipo = 'ENTRADA' AND descricao LIKE 'Recarga%%' AND data_transacao >= %s",
            (data_inicio,),
        )
        faturamento = cursor.fetchone()["total"] or 0.0
        cursor.execute(
            "SELECT COUNT(*) as total FROM utilizadores WHERE is_admin = false"
        )
        clientes = cursor.fetchone()["total"] or 0
        cursor.execute("SELECT COUNT(*) as total FROM locacoes WHERE status = 'ATIVA'")
        locacoes_ativas = cursor.fetchone()["total"] or 0
        return {
            "faturamento": float(faturamento),
            "total_clientes": clientes,
            "locacoes_ativas": locacoes_ativas,
        }
    finally:
        cursor.close()
        conn.close()


@router.post("/contas", status_code=201)
def cadastrar_conta_psn(conta: ContaPSNNova, admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO contas_psn (jogo_id, email_login, senha_login, status, mfa_secret) VALUES (%s, %s, %s, 'DISPONIVEL', %s) RETURNING id;",
            (conta.jogo_id, conta.email_login, conta.senha_login, conta.mfa_secret),
        )
        conn.commit()
        return {"mensagem": "Conta adicionada com sucesso!"}
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro ao cadastrar conta.")
    finally:
        cursor.close()
        conn.close()


@router.get("/manutencao")
def listar_contas_manutencao(admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT c.id AS conta_psn_id, j.titulo AS jogo, c.email_login, c.senha_login AS senha_antiga,
               (SELECT utilizador_id FROM locacoes WHERE conta_psn_id = c.id ORDER BY data_fim DESC LIMIT 1) AS ultimo_cliente_id,
               (SELECT u.nome FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id WHERE l.conta_psn_id = c.id ORDER BY l.data_fim DESC LIMIT 1) AS ultimo_cliente_nome,
               (SELECT u.telefone FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id WHERE l.conta_psn_id = c.id ORDER BY l.data_fim DESC LIMIT 1) AS ultimo_cliente_telefone,
               (SELECT cashback_pendente FROM locacoes WHERE conta_psn_id = c.id ORDER BY data_fim DESC LIMIT 1) AS cashback_pendente
        FROM contas_psn c JOIN jogos j ON c.jogo_id = j.id WHERE c.status = 'MANUTENCAO';
    """)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


@router.post("/multar")
def aplicar_multa(dados: AplicarMultaRequest, admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s",
            (dados.valor, dados.utilizador_id),
        )
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, 'MULTA: Conta não desativada no Console')",
            (dados.utilizador_id, dados.valor),
        )
        cursor.execute(
            "UPDATE locacoes SET cashback_pendente = 0 WHERE utilizador_id = %s AND status = 'EXPIRADA'",
            (dados.utilizador_id,),
        )
        conn.commit()
        return {"mensagem": f"A multa de R$ {dados.valor:.2f} foi aplicada!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/ajustar-saldo")
def ajustar_saldo_manual(
    dados: AjusteSaldoRequest, admin_data=Depends(verificar_admin)
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s RETURNING saldo",
            (dados.valor, dados.utilizador_id),
        )
        novo_saldo = cursor.fetchone()
        if not novo_saldo:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        tipo = "ENTRADA" if dados.valor >= 0 else "SAIDA"
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, %s, %s, %s)",
            (dados.utilizador_id, tipo, abs(dados.valor), dados.motivo),
        )
        conn.commit()
        return {
            "mensagem": f"Ajuste realizado! Novo saldo: R$ {novo_saldo['saldo']:.2f}"
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/reset-senha")
def liberar_conta_manutencao(
    dados: ResetSenhaRequest, admin_data=Depends(verificar_admin)
):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "UPDATE contas_psn SET senha_login = %s WHERE id = %s RETURNING jogo_id",
            (dados.nova_senha, dados.conta_psn_id),
        )
        jogo_id = cursor.fetchone()["jogo_id"]

        cursor.execute(
            "SELECT id, utilizador_id, cashback_pendente FROM locacoes WHERE conta_psn_id = %s ORDER BY data_fim DESC LIMIT 1",
            (dados.conta_psn_id,),
        )
        ultima_loc = cursor.fetchone()

        # Injeta o dinheiro na carteira e gera o extrato gamificado
        if ultima_loc and ultima_loc["cashback_pendente"] > 0:
            cash, usr = ultima_loc["cashback_pendente"], ultima_loc["utilizador_id"]
            cursor.execute(
                "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s", (cash, usr)
            )
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, '♻️ Recompensa: Devolução Premium')",
                (usr, cash),
            )
            cursor.execute(
                "UPDATE locacoes SET cashback_pendente = 0 WHERE id = %s",
                (ultima_loc["id"],),
            )

        cursor.execute("SELECT data_lancamento FROM jogos WHERE id = %s", (jogo_id,))
        data_lanc = cursor.fetchone()["data_lancamento"]
        eh_pre_venda = data_lanc and str(data_lanc) >= datetime.now().strftime(
            "%Y-%m-%d"
        )

        query_fila = """SELECT id, utilizador_id, dias_aluguel FROM fila_espera WHERE jogo_id = %s AND status = 'AGUARDANDO' ORDER BY {} data_solicitacao ASC LIMIT 1"""
        ordem = (
            "(SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') DESC,"
            if eh_pre_venda
            else ""
        )
        cursor.execute(query_fila.format(ordem), (jogo_id,))
        proximo = cursor.fetchone()

        if proximo:
            cursor.execute(
                "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA')",
                (
                    proximo["utilizador_id"],
                    dados.conta_psn_id,
                    proximo.get("dias_aluguel", 7),
                ),
            )
            cursor.execute(
                "UPDATE fila_espera SET status = 'CONCLUIDO' WHERE id = %s",
                (proximo["id"],),
            )
            cursor.execute(
                "UPDATE contas_psn SET status = 'ALUGADA' WHERE id = %s",
                (dados.conta_psn_id,),
            )
            msg = "Senha alterada! A conta foi entregue para o próximo da fila e a recompensa foi paga."
        else:
            cursor.execute(
                "UPDATE contas_psn SET status = 'DISPONIVEL' WHERE id = %s",
                (dados.conta_psn_id,),
            )
            msg = "Senha alterada! A conta agora está DISPONÍVEL na vitrine e a recompensa foi paga."

        conn.commit()
        return {"mensagem": msg}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/locacoes")
def listar_todas_locacoes(admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT l.id, u.nome AS cliente, j.titulo AS jogo, c.email_login, l.data_fim, l.status FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id JOIN contas_psn c ON l.conta_psn_id = c.id JOIN jogos j ON c.jogo_id = j.id ORDER BY l.data_fim ASC;"
    )
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


@router.post("/locacoes/{locacao_id}/revogar")
def revogar_locacao_admin(locacao_id: int, admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT conta_psn_id, status FROM locacoes WHERE id = %s", (locacao_id,)
        )
        loc = cursor.fetchone()
        if not loc or loc["status"] != "ATIVA":
            raise HTTPException(status_code=400, detail="Locação inválida ou expirada.")
        cursor.execute(
            "UPDATE locacoes SET status = 'EXPIRADA', data_fim = CURRENT_TIMESTAMP WHERE id = %s",
            (locacao_id,),
        )
        cursor.execute(
            "UPDATE contas_psn SET status = 'MANUTENCAO' WHERE id = %s",
            (loc["conta_psn_id"],),
        )
        conn.commit()
        return {"mensagem": "Locação revogada! Conta enviada para manutenção."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/reservas")
def listar_todas_reservas(admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT f.id, u.nome AS cliente, j.id AS jogo_id, j.titulo AS jogo, j.data_lancamento, f.data_solicitacao, f.status, f.utilizador_id, f.dias_aluguel,
        (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA') AS proxima_devolucao
        FROM fila_espera f JOIN utilizadores u ON f.utilizador_id = u.id JOIN jogos j ON f.jogo_id = j.id WHERE f.status = 'AGUARDANDO' ORDER BY f.data_solicitacao ASC;
    """)
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
                (
                    r["jogo_id"],
                    r["utilizador_id"],
                    r["utilizador_id"],
                    r["data_solicitacao"],
                ),
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

        r["data_inicio"] = (base_date + timedelta(days=dias_frente)).strftime(
            "%d/%m/%Y"
        )
        r["data_fim"] = (
            base_date + timedelta(days=dias_frente + r["dias_aluguel"])
        ).strftime("%d/%m/%Y")

    cursor.close()
    conn.close()
    return reservas


@router.post("/reservas/{reserva_id}/cancelar")
def admin_cancelar_reserva(reserva_id: int, admin_data=Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT f.utilizador_id, j.titulo FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id WHERE f.id = %s AND f.status = 'AGUARDANDO'",
            (reserva_id,),
        )
        res = cursor.fetchone()
        if not res:
            raise HTTPException(status_code=400, detail="Reserva não encontrada.")

        cursor.execute(
            "SELECT valor FROM transacoes WHERE utilizador_id = %s AND tipo = 'SAIDA' AND descricao LIKE %s ORDER BY id DESC LIMIT 1",
            (res["utilizador_id"], f"Reserva na Fila%{res['titulo']}%"),
        )
        trans = cursor.fetchone()
        if trans and trans["valor"] > 0:
            cursor.execute(
                "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s",
                (trans["valor"], res["utilizador_id"]),
            )
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)",
                (
                    res["utilizador_id"],
                    trans["valor"],
                    f"💸 Estorno (Admin): {res['titulo']}",
                ),
            )

        cursor.execute("DELETE FROM fila_espera WHERE id = %s", (reserva_id,))
        conn.commit()
        return {"mensagem": f"Reserva de {res['titulo']} cancelada e estornada."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()
