from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

from database import get_db_connection
from auth import verificar_admin
from notificacoes import enviar_email
from routes.whatsapp import enviar_template_whatsapp, normalizar_telefone
from models import (
    EditarClienteRequest,
    ConfigRequest,
    ContaPSNNova,
    AplicarMultaRequest,
    AjusteSaldoRequest,
    ResetSenhaRequest,
    BroadcastNotificacao,
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
    [R] Retorna os dados dos cards da Visão Geral do Painel Admin, organizados
    em 3 grupos: Financeiro e Atividade no Período (respeitam o filtro de
    tempo do dropdown) e Operação (estado em tempo real, ignora o filtro).
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
        # ---------------- FINANCEIRO (respeita o período) ----------------
        cursor.execute(
            "SELECT SUM(valor) as total FROM transacoes WHERE tipo = 'ENTRADA' AND descricao LIKE 'Recarga%%' AND data_transacao >= %s",
            (data_inicio,),
        )
        faturamento = float(cursor.fetchone()["total"] or 0.0)

        # Aluguéis do período: usado tanto pra "Aluguéis Iniciados" (Atividade)
        # quanto pro Ticket Médio (Financeiro) — uma query só evita repetir
        # o mesmo WHERE duas vezes.
        cursor.execute(
            "SELECT COALESCE(SUM(valor), 0) as total, COUNT(*) as qtd FROM transacoes WHERE descricao LIKE 'Aluguel%%' AND data_transacao >= %s",
            (data_inicio,),
        )
        row = cursor.fetchone()
        faturamento_alugueis = float(row["total"] or 0)
        alugueis_iniciados = row["qtd"] or 0
        ticket_medio = (
            faturamento_alugueis / alugueis_iniciados if alugueis_iniciados > 0 else 0.0
        )

        cursor.execute(
            "SELECT COUNT(*) FILTER (WHERE status = 'CONCLUIDO') as concluidos, COUNT(*) as total FROM pedidos_pix WHERE data_criacao >= %s",
            (data_inicio,),
        )
        row = cursor.fetchone()
        pix_total = row["total"] or 0
        taxa_conversao_pix = (
            float(row["concluidos"] or 0) / pix_total * 100 if pix_total > 0 else 0.0
        )

        # ---------------- ATIVIDADE NO PERÍODO ----------------
        cursor.execute(
            "SELECT COUNT(*) as total FROM locacoes WHERE status = 'EXPIRADA' AND data_fim >= %s",
            (data_inicio,),
        )
        devolucoes_periodo = cursor.fetchone()["total"] or 0

        cursor.execute(
            "SELECT COUNT(DISTINCT utilizador_id) as total FROM transacoes WHERE data_transacao >= %s",
            (data_inicio,),
        )
        clientes_ativos_periodo = cursor.fetchone()["total"] or 0

        cursor.execute(
            "SELECT COUNT(*) as total FROM utilizadores WHERE data_criacao >= %s AND is_admin = false",
            (data_inicio,),
        )
        novos_clientes = cursor.fetchone()["total"] or 0

        # ---------------- OPERAÇÃO (tempo real, ignora o período) ----------------
        cursor.execute(
            "SELECT COUNT(*) as total FROM utilizadores WHERE is_admin = false"
        )
        clientes = cursor.fetchone()["total"] or 0

        cursor.execute("SELECT COUNT(*) as total FROM locacoes WHERE status = 'ATIVA'")
        locacoes_ativas = cursor.fetchone()["total"] or 0

        cursor.execute(
            "SELECT COUNT(*) as total FROM contas_psn WHERE status_primaria = 'MANUTENCAO' OR status_secundaria = 'MANUTENCAO'"
        )
        contas_manutencao = cursor.fetchone()["total"] or 0

        cursor.execute(
            "SELECT COUNT(DISTINCT utilizador_id) as total FROM fila_espera WHERE status = 'AGUARDANDO'"
        )
        clientes_na_fila = cursor.fetchone()["total"] or 0

        cursor.execute(
            "SELECT COUNT(*) FILTER (WHERE status_primaria = 'ALUGADA') + COUNT(*) FILTER (WHERE status_secundaria = 'ALUGADA') as alugadas, COUNT(*) * 2 as total_slots FROM contas_psn"
        )
        row = cursor.fetchone()
        total_slots = row["total_slots"] or 0
        taxa_ocupacao = (
            float(row["alugadas"] or 0) / total_slots * 100 if total_slots > 0 else 0.0
        )

        return {
            # já existentes (mantidos por compatibilidade)
            "faturamento": faturamento,
            "total_clientes": clientes,
            "locacoes_ativas": locacoes_ativas,
            # financeiro
            "ticket_medio": round(ticket_medio, 2),
            "taxa_conversao_pix": round(taxa_conversao_pix, 1),
            # atividade no período
            "alugueis_iniciados": alugueis_iniciados,
            "devolucoes_periodo": devolucoes_periodo,
            "clientes_ativos_periodo": clientes_ativos_periodo,
            "novos_clientes": novos_clientes,
            # operação (tempo real)
            "contas_manutencao": contas_manutencao,
            "clientes_na_fila": clientes_na_fila,
            "taxa_ocupacao": round(taxa_ocupacao, 1),
        }
    finally:
        cursor.close()
        conn.close()


@router.get("/estatisticas/tendencia")
def buscar_tendencia_novos_clientes(
    periodo: str = "mes", admin_data=Depends(verificar_admin)
):
    """
    [R] Série temporal de novos cadastros, para o gráfico da Visão Geral.
    Reaproveita o mesmo corte de data do /admin/estatisticas. Para 'mes' e
    '30dias' agrupa por dia; para 'ano' e 'tudo' agrupa por mês, para não
    gerar um gráfico com 300+ pontos diários ilegíveis.

    LIMITAÇÃO CONHECIDA: como 'data_criacao' foi adicionada via ALTER TABLE
    com valor padrão CURRENT_TIMESTAMP, todo cliente cadastrado ANTES da
    migração aparece com a mesma data (o dia em que a coluna foi criada).
    Isso é preciso a partir da migração em diante, não retroativamente.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()

    if periodo == "mes":
        data_inicio = hoje.replace(day=1, hour=0, minute=0, second=0)
        granularidade = "day"
    elif periodo == "30dias":
        data_inicio = hoje - timedelta(days=30)
        granularidade = "day"
    elif periodo == "ano":
        data_inicio = hoje.replace(month=1, day=1, hour=0, minute=0, second=0)
        granularidade = "month"
    else:
        data_inicio = datetime(2000, 1, 1)
        granularidade = "month"

    try:
        cursor.execute(
            """
            SELECT DATE_TRUNC(%s, data_criacao) as bucket, COUNT(*) as total
            FROM utilizadores
            WHERE data_criacao >= %s AND is_admin = false
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (granularidade, data_inicio),
        )
        linhas = cursor.fetchall()
        formato = "%Y-%m-%d" if granularidade == "day" else "%Y-%m"
        return [
            {"data": row["bucket"].strftime(formato), "novos_clientes": row["total"]}
            for row in linhas
        ]
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
    [U] O gatilho mais complexo do Admin (Atualizado com Benefícios).
    Você informa a nova senha que criou no site da PSN após constatar que a conta
    foi desativada corretamente pelo cliente. O sistema:
    1. Salva a nova senha.
    2. Descobre se era a Vaga Primária ou Secundária.
    3. Se o status_beneficio for 'PENDENTE', libera o Cashback e soma +1 no Rank do cliente honesto.
    4. Vai na fila de espera correspondente e entrega para o próximo (ou devolve pra vitrine).
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
            "SELECT id, utilizador_id, cashback_pendente, status_beneficio FROM locacoes WHERE conta_psn_id = %s AND tipo_slot = %s ORDER BY data_fim DESC LIMIT 1",
            (dados.conta_psn_id, slot_em_manutencao),
        )
        ultima_loc = cursor.fetchone()

        # AUDITORIA APROVADA: Injeta o dinheiro, sobe o rank e marca como PAGO
        if ultima_loc and ultima_loc["status_beneficio"] == "PENDENTE":
            usr = ultima_loc["utilizador_id"]

            # Sobe o Rank
            cursor.execute(
                "UPDATE utilizadores SET rank = rank + 1 WHERE id = %s", (usr,)
            )

            # Injeta o dinheiro de devolução (se houver)
            if ultima_loc["cashback_pendente"] > 0:
                cash = ultima_loc["cashback_pendente"]
                cursor.execute(
                    "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s",
                    (cash, usr),
                )
                cursor.execute(
                    "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, '♻️ Recompensa: Devolução Premium')",
                    (usr, cash),
                )

            # Trava o benefício como PAGO para evitar duplicidade
            cursor.execute(
                "UPDATE locacoes SET status_beneficio = 'PAGO', cashback_pendente = 0 WHERE id = %s",
                (ultima_loc["id"],),
            )

        cursor.execute("SELECT titulo FROM jogos WHERE id = %s", (jogo_id,))
        titulo = cursor.fetchone()["titulo"]

        # Puxa o próximo da fila específica daquele slot, estritamente por ordem de chegada.
        cursor.execute(
            "SELECT fila_espera.id, fila_espera.utilizador_id, fila_espera.dias_aluguel, u.nome, u.email, u.telefone FROM fila_espera JOIN utilizadores u ON u.id = fila_espera.utilizador_id WHERE fila_espera.jogo_id = %s AND fila_espera.status = 'AGUARDANDO' AND fila_espera.tipo_slot = %s ORDER BY data_solicitacao ASC LIMIT 1",
            (jogo_id, slot_em_manutencao),
        )
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
            mensagem_sino = f"🎉 SEU ACESSO FOI LIBERADO! A sua vaga ({slot_em_manutencao}) do jogo {titulo} já está na aba 'Meus Acessos'. Bom jogo!"
            cursor.execute(
                "INSERT INTO notificacoes (utilizador_id, reserva_id, jogo, mensagem) VALUES (%s, %s, %s, %s)",
                (proximo["utilizador_id"], proximo["id"], titulo, mensagem_sino),
            )
            msg = f"Senha alterada e Conta Auditada! A vaga {slot_em_manutencao} foi entregue para o próximo da fila."
        else:
            # Não tem fila: Apenas devolve o slot para a prateleira
            cursor.execute(
                f"UPDATE contas_psn SET {coluna_status} = 'DISPONIVEL' WHERE id = %s",
                (dados.conta_psn_id,),
            )
            msg = f"Senha alterada e Conta Auditada! A vaga {slot_em_manutencao} agora está DISPONÍVEL."

        conn.commit()

        # Este é o caminho REAL de entrega da fila (troca de senha manual, não o
        # cron): devolução e revogação sempre passam a conta por MANUTENCAO antes,
        # então é aqui — não em processar_filas_automaticamente — que o próximo da
        # fila efetivamente recebe o jogo na maioria dos casos. Sem isto, sino,
        # e-mail e WhatsApp nunca disparavam nesse fluxo.
        if proximo:
            if proximo.get("email"):
                enviar_email(
                    proximo["email"],
                    "🎮 Seu jogo já está liberado na Bora Jogar!",
                    f"<p>Fala, {proximo['nome']}! Sua vez chegou: o jogo <strong>{titulo}</strong> já está liberado na sua conta. Acesse a aba 'Meus Acessos' no site pra pegar os dados de acesso.</p>",
                )
            if proximo.get("telefone"):
                enviar_template_whatsapp(
                    normalizar_telefone(proximo["telefone"]),
                    "jogo_pronto",
                    [proximo["nome"], titulo],
                )

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
    que digitou o número errado no console e ficou "preso" — vale pra Primária e
    Secundária, já que as duas travam depois de gerar o código uma vez.
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
    [U] Rota Punitiva / Cobrança no WhatsApp.
    Se o cliente não desabilitar o Console, este botão:
    1. Cancela qualquer benefício PENDENTE daquela devolução.
    2. Aplica a multa (se houver valor).
    3. Corta o Rank atual do cliente pela metade (Punição de confiança).
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Aplica a multa financeira (se você configurou um valor)
        if dados.valor > 0:
            cursor.execute(
                "UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s",
                (dados.valor, dados.utilizador_id),
            )
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, 'MULTA: Conta não desativada no Console')",
                (dados.utilizador_id, dados.valor),
            )

        # 2. Cancela os Benefícios e o Cashback Pendente
        cursor.execute(
            "UPDATE locacoes SET status_beneficio = 'CANCELADO', cashback_pendente = 0 WHERE utilizador_id = %s AND status = 'EXPIRADA' AND status_beneficio = 'PENDENTE'",
            (dados.utilizador_id,),
        )

        # 3. Corta o Rank pela Metade
        cursor.execute(
            "SELECT rank FROM utilizadores WHERE id = %s", (dados.utilizador_id,)
        )
        usuario = cursor.fetchone()

        novo_rank = 0
        if usuario:
            rank_atual = usuario["rank"]
            novo_rank = rank_atual // 2
            cursor.execute(
                "UPDATE utilizadores SET rank = %s WHERE id = %s",
                (novo_rank, dados.utilizador_id),
            )

        conn.commit()
        return {
            "mensagem": f"Cobrança registrada! Os benefícios pendentes foram cancelados e o Rank caiu para {novo_rank}."
        }
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
        # Fila normal por data de chegada, isolada pelo slot (sem exceção para pré-venda).
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


# ==============================================================================
# AVISOS GERAIS (BROADCAST)
# ==============================================================================


@router.post("/notificacoes/broadcast")
def enviar_broadcast(dados: BroadcastNotificacao):
    """
    [C] Painel Admin: envia um aviso in-app (mensagem + botão de ação
    opcional) para todos os clientes cadastrados. Qualquer broadcast
    anterior ainda não lido é marcado como lido antes do novo entrar,
    pra não acumular avisos antigos no sino do cliente.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE notificacoes SET lida = TRUE WHERE tipo = 'BROADCAST' AND lida = FALSE"
        )
        cursor.execute(
            "INSERT INTO notificacoes (utilizador_id, mensagem, url_acao, tipo) "
            "SELECT id, %s, %s, 'BROADCAST' FROM utilizadores",
            (dados.mensagem, dados.url_acao),
        )
        linhas = cursor.rowcount
        conn.commit()
        return {
            "mensagem": f"Aviso enviado para {linhas} clientes.",
            "total": linhas,
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/notificacoes/broadcast/historico")
def historico_broadcast():
    """
    [R] Painel Admin: últimos 5 avisos enviados, pra reaproveitar texto.
    Reconstrói a partir de notificacoes (não existe tabela própria) —
    cada envio insere N linhas (uma por cliente) com o mesmo data_criacao,
    porque o Postgres avalia CURRENT_TIMESTAMP uma única vez por comando;
    agrupar por data_criacao junta essas N linhas de volta num só evento.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT mensagem, url_acao, data_criacao FROM notificacoes "
        "WHERE tipo = 'BROADCAST' "
        "GROUP BY data_criacao, mensagem, url_acao "
        "ORDER BY data_criacao DESC LIMIT 5"
    )
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


# ==============================================================================
# DOSSIÊ FINANCEIRO DO CLIENTE
# ==============================================================================


@router.get("/clientes/{usuario_id}/dossie")
def dossie_cliente(
    usuario_id: int,
    limite_transacoes: int = 10,
    limite_locacoes: int = 10,
    admin_data=Depends(verificar_admin),
):
    """
    [R] Retrato completo de um cliente para o Painel Admin.

    Junta num só lugar o que antes só dava para ver abrindo o banco de dados na
    mão: extrato, histórico de locações, fila e — o mais importante — os pedidos
    de recarga que ficaram presos em PENDENTE.

    O bloco 'resumo' existe para conferência rápida de caixa: se o número de
    recargas não bate com o que entrou na conta bancária, dá para ver aqui sem
    precisar somar o extrato linha a linha. Esse cálculo roda sobre o histórico
    inteiro, sem limite — só a LISTA de transações/locações é paginada, para não
    travar o Painel num cliente antigo com centenas de registros.
    """
    # Limites de sanidade: cliente que faz gerenciamento nunca precisa de mais
    # que isso de uma vez, e evita alguém montar ?limite_transacoes=999999.
    limite_transacoes = max(1, min(limite_transacoes, 500))
    limite_locacoes = max(1, min(limite_locacoes, 500))

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT id, nome, email, telefone, saldo, rank, is_admin,
                   email_verificado, whatsapp_verificado, codigo_indicacao
            FROM utilizadores WHERE id = %s
            """,
            (usuario_id,),
        )
        cliente = cursor.fetchone()
        if not cliente:
            raise HTTPException(status_code=404, detail="Cliente não encontrado.")
        cliente["saldo"] = float(cliente["saldo"])

        cursor.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tipo = 'ENTRADA' THEN valor END), 0) AS total_entradas,
                COALESCE(SUM(CASE WHEN tipo = 'SAIDA'   THEN valor END), 0) AS total_saidas,
                COALESCE(SUM(CASE WHEN descricao LIKE 'Recarga%%' THEN valor END), 0) AS total_recargas,
                COUNT(*) FILTER (WHERE descricao LIKE 'Recarga%%') AS qtd_recargas
            FROM transacoes WHERE utilizador_id = %s
            """,
            (usuario_id,),
        )
        resumo = {k: float(v) for k, v in cursor.fetchone().items()}
        resumo["qtd_recargas"] = int(resumo["qtd_recargas"])

        cursor.execute(
            "SELECT COUNT(*) AS total FROM transacoes WHERE utilizador_id = %s",
            (usuario_id,),
        )
        transacoes_total = cursor.fetchone()["total"]
        cursor.execute(
            "SELECT tipo, valor, descricao, data_transacao FROM transacoes "
            "WHERE utilizador_id = %s ORDER BY data_transacao DESC LIMIT %s",
            (usuario_id, limite_transacoes),
        )
        transacoes = cursor.fetchall()
        for t in transacoes:
            t["valor"] = float(t["valor"])

        cursor.execute(
            """
            SELECT COUNT(*) AS total FROM locacoes WHERE utilizador_id = %s
            """,
            (usuario_id,),
        )
        locacoes_total = cursor.fetchone()["total"]
        cursor.execute(
            """
            SELECT l.id, j.titulo AS jogo, l.tipo_slot, l.data_fim, l.status,
                   l.cashback_pendente, l.status_beneficio
            FROM locacoes l
            JOIN contas_psn c ON c.id = l.conta_psn_id
            JOIN jogos j ON j.id = c.jogo_id
            WHERE l.utilizador_id = %s
            ORDER BY l.data_fim DESC
            LIMIT %s
            """,
            (usuario_id, limite_locacoes),
        )
        locacoes = cursor.fetchall()
        for loc in locacoes:
            loc["cashback_pendente"] = float(loc["cashback_pendente"] or 0)

        # Recargas que nunca fecharam. Um pedido antigo preso aqui costuma ser
        # pagamento que o cliente abandonou, mas também é onde apareceria um
        # pagamento real que não virou saldo.
        # Restrito ao mês atual: o cron 'verificar_pix_perdidos' expira
        # automaticamente qualquer PENDENTE com mais de 48h, então nada deveria
        # sobreviver até aqui de qualquer forma — isto é a rede de segurança
        # para a janela entre um deploy e outro.
        cursor.execute(
            "SELECT id, valor_pago, valor_bonus, cupom, status, data_criacao FROM pedidos_pix "
            "WHERE utilizador_id = %s AND status = 'PENDENTE' "
            "AND data_criacao >= date_trunc('month', CURRENT_TIMESTAMP) "
            "ORDER BY data_criacao DESC",
            (usuario_id,),
        )
        pendentes = cursor.fetchall()
        for pd in pendentes:
            pd["valor_pago"] = float(pd["valor_pago"] or 0)
            pd["valor_bonus"] = float(pd["valor_bonus"] or 0)

        cursor.execute(
            """
            SELECT f.id, j.titulo AS jogo, f.tipo_slot, f.dias_aluguel,
                   f.data_solicitacao, f.status
            FROM fila_espera f
            JOIN jogos j ON j.id = f.jogo_id
            WHERE f.utilizador_id = %s AND f.status = 'AGUARDANDO'
            ORDER BY f.data_solicitacao ASC
            """,
            (usuario_id,),
        )
        reservas = cursor.fetchall()

        return {
            "cliente": cliente,
            "resumo": resumo,
            "transacoes": transacoes,
            "transacoes_total": transacoes_total,
            "locacoes": locacoes,
            "locacoes_total": locacoes_total,
            "pedidos_pendentes": pendentes,
            "reservas": reservas,
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
