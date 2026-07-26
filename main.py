from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2.extras import RealDictCursor
from datetime import datetime

from database import get_db_connection
from routes import usuarios, jogos, pagamentos, alugueis, admin, whatsapp
from routes.pagamentos import processar_sucesso_pagamento, credentials_efi
from efipay import EfiPay

app = FastAPI(title="API Locadora PS5")

# ==============================================================================
# 1. MIDDLEWARES E SEGURANÇA
# ==============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================================
# 2. INJEÇÃO DOS MÓDULOS DE ROTAS (ROUTERS)
# ==============================================================================
app.include_router(usuarios.router)
app.include_router(jogos.router)
app.include_router(pagamentos.router)
app.include_router(alugueis.router)
app.include_router(admin.router)
app.include_router(whatsapp.router)


@app.get("/")
def home():
    """Rota de pulsação para confirmar que a API está acordada e rodando."""
    return {"mensagem": "API Online e Modularizada 🚀"}


@app.get("/configuracoes")
def get_config():
    """
    [R] Rota Pública.
    Retorna os banners e textos que o Frontend usa para desenhar a vitrine.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT * FROM configuracoes LIMIT 1")
        config = cursor.fetchone()
        return config if config else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# MOTOR DE TAREFAS AUTOMÁTICAS (CRON JOBS)
# ==============================================================================


def verificar_alugueis_vencidos():
    """
    Cron Job (A cada 1 minuto):
    Derruba automaticamente locações que passaram da data final e joga aquele
    slot específico ('PRIMARIA' ou 'SECUNDARIA') para manutenção.
    [ATUALIZADO]: Como o sistema precisou derrubar a conta, o cliente perde
    o direito ao Cashback e Rank (status_beneficio = CANCELADO).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id, conta_psn_id, tipo_slot FROM locacoes WHERE status = 'ATIVA' AND data_fim <= CURRENT_TIMESTAMP"
        )
        locacoes_vencidas = cursor.fetchall()
        if locacoes_vencidas:
            for loc in locacoes_vencidas:
                loc_id, conta_id, tipo_slot = loc
                coluna_status = (
                    "status_primaria"
                    if tipo_slot == "PRIMARIA"
                    else "status_secundaria"
                )

                # NOVO: Explicita que o cashback é zero e o benefício está cancelado
                cursor.execute(
                    "UPDATE locacoes SET status = 'EXPIRADA', cashback_pendente = 0, status_beneficio = 'CANCELADO' WHERE id = %s",
                    (loc_id,),
                )
                cursor.execute(
                    f"UPDATE contas_psn SET {coluna_status} = 'MANUTENCAO' WHERE id = %s",
                    (conta_id,),
                )
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro no Cron de Vencidos: {e}")
    finally:
        cursor.close()
        conn.close()


def processar_filas_automaticamente():
    """
    Cron Job (A cada 1 minuto):
    O 'Maestro' da fila de espera.
    [ATUALIZADO]: Agora usa a nova coluna de 'rank' para furar fila no dia do lançamento.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT DISTINCT 
                f.jogo_id, 
                j.titulo, 
                f.tipo_slot,
                CASE WHEN CAST(j.data_lancamento AS VARCHAR) = TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') THEN True ELSE False END as is_dia_lancamento
            FROM fila_espera f
            JOIN jogos j ON f.jogo_id = j.id
            JOIN contas_psn c ON c.jogo_id = j.id
            WHERE f.status = 'AGUARDANDO' 
            AND (
                j.data_lancamento IS NULL 
                OR j.data_lancamento = '' 
                OR CAST(j.data_lancamento AS VARCHAR) <= TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD')
            )
            AND (
                (f.tipo_slot = 'PRIMARIA' AND c.status_primaria = 'DISPONIVEL') OR 
                (f.tipo_slot = 'SECUNDARIA' AND c.status_secundaria = 'DISPONIVEL')
            )
        """)
        jogos_pendentes = cursor.fetchall()

        for jp in jogos_pendentes:
            jogo_id = jp["jogo_id"]
            titulo = jp["titulo"]
            tipo_slot = jp["tipo_slot"]
            is_dia_lancamento = jp["is_dia_lancamento"]

            coluna_status = (
                "status_primaria" if tipo_slot == "PRIMARIA" else "status_secundaria"
            )

            cursor.execute(
                f"SELECT id FROM contas_psn WHERE jogo_id = %s AND {coluna_status} = 'DISPONIVEL'",
                (jogo_id,),
            )
            contas_disponiveis = cursor.fetchall()

            for conta in contas_disponiveis:
                # NOVO: A ordem agora usa o "rank" do utilizador em vez de COUNT de locações
                ordem = (
                    "(SELECT rank FROM utilizadores WHERE id = fila_espera.utilizador_id) DESC, data_solicitacao ASC LIMIT 1"
                    if is_dia_lancamento
                    else "data_solicitacao ASC LIMIT 1"
                )

                query_proximo = f"SELECT id, utilizador_id, dias_aluguel FROM fila_espera WHERE jogo_id = %s AND status = 'AGUARDANDO' AND tipo_slot = %s ORDER BY {ordem}"

                cursor.execute(query_proximo, (jogo_id, tipo_slot))
                proximo = cursor.fetchone()

                if proximo:
                    cursor.execute(
                        "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status, tipo_slot) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA', %s)",
                        (
                            proximo["utilizador_id"],
                            conta["id"],
                            proximo.get("dias_aluguel", 7),
                            tipo_slot,
                        ),
                    )
                    cursor.execute(
                        "UPDATE fila_espera SET status = 'CONCLUIDO' WHERE id = %s",
                        (proximo["id"],),
                    )
                    cursor.execute(
                        f"UPDATE contas_psn SET {coluna_status} = 'ALUGADA' WHERE id = %s",
                        (conta["id"],),
                    )

                    msg = f"🎉 SEU ACESSO FOI LIBERADO! A sua vaga ({tipo_slot}) do jogo {titulo} já está na aba 'Meus Acessos'. Bom jogo!"
                    cursor.execute(
                        "INSERT INTO notificacoes (utilizador_id, reserva_id, jogo, mensagem) VALUES (%s, %s, %s, %s)",
                        (proximo["utilizador_id"], proximo["id"], titulo, msg),
                    )
                    conn.commit()
                else:
                    break
    except Exception as e:
        conn.rollback()
        print(f"Erro Fila Cron: {e}")
    finally:
        cursor.close()
        conn.close()


def verificar_pix_perdidos():
    """
    Cron Job (A cada 1 minuto):
    Rastreador de Pix. Vai na Efí bater na porta e resgatar transações pendentes
    que não dispararam Webhooks.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT id, utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE status = 'PENDENTE'"
        )
        pendentes = cursor.fetchall()

        if pendentes:
            efi = EfiPay(credentials_efi)
            for pedido in pendentes:
                txid = pedido["id"]
                if not txid.startswith("cs_"):
                    try:
                        detalhes = efi.pix_detail_charge(params={"txid": txid})
                        if detalhes.get("status") == "CONCLUIDA":
                            print(f"💰 PIX PERDIDO RECUPERADO VIA CRON! TXID: {txid}")
                            processar_sucesso_pagamento(
                                txid,
                                pedido["utilizador_id"],
                                pedido["valor_pago"],
                                pedido["valor_bonus"],
                                pedido["cupom"],
                            )
                    except Exception:
                        pass
    except Exception as e:
        print(f"Erro no rastreador de Pix Cron: {e}")
    finally:
        cursor.close()
        conn.close()


def limpar_cupons_surpresa_cron():
    """
    Cron Job (Diário às 23:59):
    Apaga todos os cupons promocionais do Instagram que começam com 'SURPRESA',
    garantindo que expirem à meia-noite e não fiquem acumulando no banco.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Usa '%%' porque o psycopg2 entende '%' como formatação de string
        cursor.execute("DELETE FROM cupons WHERE codigo LIKE 'SURPRESA%%'")
        apagados = cursor.rowcount
        conn.commit()

        if apagados > 0:
            print(
                f"🧹 FAXINA NOTURNA: {apagados} cupom(ns) 'SURPRESA' apagado(s) com sucesso."
            )

    except Exception as e:
        conn.rollback()
        print(f"Erro no Cron de Limpeza de Cupons: {e}")
    finally:
        cursor.close()
        conn.close()


@app.post("/admin/forcar-processamento-filas", tags=["Admin"])
def forcar_filas():
    """Botão manual para destravar a fila caso o Cron Job do Render hiberne."""
    processar_filas_automaticamente()
    return {"mensagem": "O motor de filas rodou com sucesso!"}


# ==============================================================================
# INICIALIZAÇÃO DA API E DA CRON
# ==============================================================================
@app.on_event("startup")
def iniciar_servicos():
    """Roda automaticamente quando o Render liga a API pela primeira vez."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS notificacoes (id SERIAL PRIMARY KEY, utilizador_id INT, reserva_id INT, jogo VARCHAR(255), mensagem TEXT, lida BOOLEAN DEFAULT FALSE, data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ AVISO DE STARTUP: Banco de dados falhou na checagem. Detalhe: {e}")

    # Inicia as varreduras de fundo
    scheduler = BackgroundScheduler()
    scheduler.add_job(verificar_alugueis_vencidos, "interval", minutes=1)
    scheduler.add_job(processar_filas_automaticamente, "interval", minutes=1)
    scheduler.add_job(verificar_pix_perdidos, "interval", minutes=1)

    # NOVO: Faxina de cupons rodando todo dia às 23:59
    scheduler.add_job(limpar_cupons_surpresa_cron, "cron", hour=23, minute=59)

    scheduler.start()
