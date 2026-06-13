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

# Cria um roteador específico para operações do Painel Administrativo.
# A dependência verificar_admin garante que NENHUMA rota abaixo possa ser
# chamada por clientes comuns, protegendo o caixa e as contas da loja.
router = APIRouter(
    prefix="/admin", tags=["Admin"], dependencies=[Depends(verificar_admin)]
)


@router.post("/configuracoes")
def set_config(dados: ConfigRequest, admin_data=Depends(verificar_admin)):
    """
    [C] Salva as configurações globais do sistema.
    Atualiza dados como taxa diária de cashback, mensagem do banner rotativo superior,
    e os títulos dinâmicos que aparecem na área de Enquete da vitrine.
    Se não existir linha na tabela, cria uma nova.
    """
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
        # O finally garante que o banco seja fechado SEMPRE, evitando gargalos no Render.
        cursor.close()
        conn.close()


@router.get("/estatisticas")
def buscar_estatisticas_admin(
    periodo: str = "mes", admin_data=Depends(verificar_admin)
):
    """
    [R] Retorna os dados resumidos dos cards azuis/verdes no topo do painel Admin.
    Calcula Faturamento via Recargas, Total de Clientes e Locações Ativas no momento,
    filtrando com base no dropdown de tempo (mês atual, 30 dias, ano ou todos).
    """
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
    """
    [C] Recebe o e-mail, senha e 2FA Secret de uma conta recém-comprada para o cofre.
    Ao inserir a conta, ela nasce automaticamente com as Vagas Primária e Secundária em 'DISPONIVEL'.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO contas_psn (jogo_id, email_login, senha_login, status_primaria, status_secundaria, mfa_secret) VALUES (%s, %s, %s, 'DISPONIVEL', 'DISPONIVEL', %s) RETURNING id;",
            (conta.jogo_id, conta.email_login, conta.senha_login, conta.mfa_secret),
        )
        conn.commit()
        return {"mensagem": "Conta adicionada com sucesso!"}
    except Exception:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Erro ao cadastrar conta. Verifique se os dados estão corretos.",
        )
    finally:
        cursor.close()
        conn.close()


@router.get("/manutencao")
def listar_contas_manutencao(admin_data=Depends(verificar_admin)):
    """
    [R] Painel de Alerta Vermelho.
    Busca todas as contas (ou slots específicos) que terminaram o tempo de aluguel e
    que VOCÊ precisa trocar a senha no site da PSN antes de liberar para o próximo cliente.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    # Busca contas que tenham PELO MENOS UM dos slots travados em MANUTENCAO.
    cursor.execute("""
        SELECT c.id AS conta_psn_id, j.titulo AS jogo, c.email_login, c.senha_login AS senha_antiga,
               (SELECT utilizador_id FROM locacoes WHERE conta_psn_id = c.id ORDER BY data_fim DESC LIMIT 1) AS ultimo_cliente_id,
               (SELECT u.nome FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id WHERE l.conta_psn_id = c.id ORDER BY l.data_fim DESC LIMIT 1) AS ultimo_cliente_nome,
               (SELECT u.telefone FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id WHERE l.conta_psn_id = c.id ORDER BY l.data_fim DESC LIMIT 1) AS ultimo_cliente_telefone,
               (SELECT cashback_pendente FROM locacoes WHERE conta_psn_id = c.id ORDER BY data_fim DESC LIMIT 1) AS cashback_pendente,
               c.status_primaria, c.status_secundaria
        FROM contas_psn c JOIN jogos j ON c.jogo_id = j.id 
        WHERE c.status_primaria = 'MANUTENCAO' OR c.status_secundaria = 'MANUTENCAO';
    """)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


@router.post("/reset-senha")
def liberar_conta_manutencao(
    dados: ResetSenhaRequest, admin_data=Depends(verificar_admin)
):
    """
    [U] O gatilho mais complexo do Admin.
    Você informa a nova senha que criou no site da PSN. O sistema:
    1. Salva a nova senha.
    2. Paga a recompensa para o cliente antigo (se ele devolver de forma Premium).
    3. Descobre se era a Vaga Primária ou Secundária que estava em manutenção.
    4. Vai na fila de espera correspondente àquela vaga.
    5. Se tiver gente esperando, manda a conta para o próximo. Se não, volta pro estoque.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Atualiza a senha matriz da conta inteira
        cursor.execute(
            "UPDATE contas_psn SET senha_login = %s WHERE id = %s RETURNING jogo_id, status_primaria, status_secundaria",
            (dados.nova_senha, dados.conta_psn_id),
        )
        conta_update = cursor.fetchone()
        jogo_id = conta_update["jogo_id"]

        # Descobre qual "metade" da conta originou esse alerta de manutenção
        slot_em_manutencao = (
            "PRIMARIA"
            if conta_update["status_primaria"] == "MANUTENCAO"
            else "SECUNDARIA"
        )
        coluna_status = f"status_{slot_em_manutencao.lower()}"

        cursor.execute(
            "SELECT id, utilizador_id, cashback_pendente FROM locacoes WHERE conta_psn_id = %s AND tipo_slot = %s ORDER BY data_fim DESC LIMIT 1",
            (dados.conta_psn_id, slot_em_manutencao),
        )
        ultima_loc = cursor.fetchone()

        # Injeta o dinheiro de devolução na carteira e gera o extrato
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

        # Confere se é um lançamento, para saber se a fila respeita RANK VIP ou apenas Data.
        cursor.execute("SELECT data_lancamento FROM jogos WHERE id = %s", (jogo_id,))
        data_lanc = cursor.fetchone()["data_lancamento"]
        eh_pre_venda = data_lanc and str(data_lanc) >= datetime.now().strftime(
            "%Y-%m-%d"
        )

        query_fila = """SELECT id, utilizador_id, dias_aluguel FROM fila_espera WHERE jogo_id = %s AND status = 'AGUARDANDO' AND tipo_slot = %s ORDER BY {} data_solicitacao ASC LIMIT 1"""
        ordem = (
            "(SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') DESC,"
            if eh_pre_venda
            else ""
        )

        # Puxa o próximo da fila específica daquele slot
        cursor.execute(query_fila.format(ordem), (jogo_id, slot_em_manutencao))
        proximo = cursor.fetchone()

        if proximo:
            # Tem gente na fila: Cria a locação para o próximo e muda a conta para ALUGADA
            cursor.execute(
                "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status, tipo_slot) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA', %s)",
                (
                    proximo["utilizador_id"],
                    dados.conta_psn_id,
                    proximo.get("dias_aluguel", 7),
                    slot_em_manutencao,
                ),
            )
            cursor.execute(
                "UPDATE fila_espera SET status = 'CONCLUIDO' WHERE id = %s",
                (proximo["id"],),
            )
            cursor.execute(
                f"UPDATE contas_psn SET {coluna_status} = 'ALUGADA' WHERE id = %s",
                (dados.conta_psn_id,),
            )
            msg = f"Senha alterada! A vaga {slot_em_manutencao} foi entregue para o próximo da fila."
        else:
            # Não tem fila: Apenas devolve o slot para a prateleira
            cursor.execute(
                f"UPDATE contas_psn SET {coluna_status} = 'DISPONIVEL' WHERE id = %s",
                (dados.conta_psn_id,),
            )
            msg = f"Senha alterada! A vaga {slot_em_manutencao} agora está DISPONÍVEL na vitrine."

        conn.commit()
        return {"mensagem": msg}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/reset-2fa/{locacao_id}")
def resetar_trava_2fa_admin(locacao_id: int, admin_data=Depends(verificar_admin)):
    """
    [U] O Botão de Pânico "Zero Trust".
    Permite que o dono da locadora resete manualmente o bloqueio do 2FA para um cliente
    da vaga Secundária que digitou o número errado no console e ficou "preso".
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE locacoes SET codigo_2fa_usado = FALSE WHERE id = %s RETURNING id",
            (locacao_id,),
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Locação não encontrada.")

        conn.commit()
        return {"mensagem": "Botão de Gerar Código liberado para o cliente novamente!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/multar")
def aplicar_multa(dados: AplicarMultaRequest, admin_data=Depends(verificar_admin)):
    """
    [U] Rota Punitiva.
    Se o cliente não desabilitar o Console Principal, você aplica R$ 50 de dívida no painel.
    Retira qualquer direito a Cashback que ele pudesse ter ganhado nesta locação.
    """
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
        return {"mensagem": f"A multa de R$ {dados.valor:.2f} foi aplicada no cliente!"}
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
    """
    [U] Função para corrigir pagamentos no limbo ou injetar dinheiro promocional.
    Adiciona ou remove dinheiro da carteira e exige um motivo para o extrato.
    """
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


@router.get("/locacoes")
def listar_todas_locacoes(admin_data=Depends(verificar_admin)):
    """
    [R] Busca todas as pessoas jogando no momento.
    Usado na aba 'Locações Ativas' do Painel Admin. Inclui o tipo de slot.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT l.id, u.nome AS cliente, j.titulo AS jogo, c.email_login, l.data_fim, l.status, l.tipo_slot "
        "FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id "
        "JOIN contas_psn c ON l.conta_psn_id = c.id JOIN jogos j ON c.jogo_id = j.id ORDER BY l.data_fim ASC;"
    )
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


@router.post("/locacoes/{locacao_id}/revogar")
def revogar_locacao_admin(locacao_id: int, admin_data=Depends(verificar_admin)):
    """
    [U] Cancela uma locação imediatamente, independente da data fim.
    Coloca o slot específico (Primária ou Secundária) direto no alerta de manutenção.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT conta_psn_id, status, tipo_slot FROM locacoes WHERE id = %s",
            (locacao_id,),
        )
        loc = cursor.fetchone()
        if not loc or loc["status"] != "ATIVA":
            raise HTTPException(
                status_code=400, detail="Locação inválida ou já expirada."
            )

        cursor.execute(
            "UPDATE locacoes SET status = 'EXPIRADA', data_fim = CURRENT_TIMESTAMP WHERE id = %s",
            (locacao_id,),
        )
        coluna_status = (
            "status_primaria" if loc["tipo_slot"] == "PRIMARIA" else "status_secundaria"
        )
        cursor.execute(
            f"UPDATE contas_psn SET {coluna_status} = 'MANUTENCAO' WHERE id = %s",
            (loc["conta_psn_id"],),
        )
        conn.commit()
        return {
            "mensagem": "Locação revogada! Slot enviado para a aba de manutenção de senhas."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/reservas")
def listar_todas_reservas(admin_data=Depends(verificar_admin)):
    """
    [R] Calcula a fila de espera e monta as estimativas de dias de forma isolada para
    quem está na fila da vaga Primária e da vaga Secundária.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT f.id, u.nome AS cliente, j.id AS jogo_id, j.titulo AS jogo, j.data_lancamento, f.data_solicitacao, 
               f.status, f.utilizador_id, f.dias_aluguel, f.tipo_slot,
        (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA' AND l.tipo_slot = f.tipo_slot) AS proxima_devolucao
        FROM fila_espera f JOIN utilizadores u ON f.utilizador_id = u.id JOIN jogos j ON f.jogo_id = j.id 
        WHERE f.status = 'AGUARDANDO' ORDER BY f.data_solicitacao ASC;
    """)
    reservas = cursor.fetchall()

    for r in reservas:
        hoje_str = datetime.now().strftime("%Y-%m-%d")
        eh_pre_venda = r["data_lancamento"] and str(r["data_lancamento"]) >= hoje_str

        if eh_pre_venda:
            # A lógica VIP empurra os dias baseando-se apenas na fila do mesmo tipo de slot
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
                    r["utilizador_id"],
                    r["utilizador_id"],
                    r["data_solicitacao"],
                ),
            )
        else:
            # Se não é pré-venda, fila normal por data (mas isolada pelo slot)
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
    """
    [D] Chuta o cliente da fila à força pelo Painel Admin e devolve o saldo dele.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT f.utilizador_id, f.tipo_slot, j.titulo FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id WHERE f.id = %s AND f.status = 'AGUARDANDO'",
            (reserva_id,),
        )
        res = cursor.fetchone()
        if not res:
            raise HTTPException(status_code=400, detail="Reserva não encontrada.")

        cursor.execute(
            "SELECT valor FROM transacoes WHERE utilizador_id = %s AND tipo = 'SAIDA' AND descricao LIKE %s ORDER BY id DESC LIMIT 1",
            (res["utilizador_id"], f"Fila {res['tipo_slot']}%{res['titulo']}%"),
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
        return {
            "mensagem": f"Reserva de {res['titulo']} cancelada e dinheiro estornado."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()
