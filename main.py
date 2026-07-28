from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import os

from auth import verificar_admin
from database import get_db_connection
from notificacoes import enviar_email
from routes import usuarios, jogos, pagamentos, alugueis, admin, whatsapp
from routes.pagamentos import processar_sucesso_pagamento, credentials_efi
from routes.whatsapp import enviar_template_whatsapp, normalizar_telefone
from efipay import EfiPay
import stripe

app = FastAPI(title="API Locadora PS5")

# ==============================================================================
# 1. MIDDLEWARES E SEGURANÇA
# ==============================================================================
# Origens autorizadas a chamar a API pelo navegador.
# Antes era allow_origins=["*"] junto com allow_credentials=True — combinação em
# que o Starlette devolve de volta a origem de quem perguntou, ou seja, qualquer
# site na internet podia usar esta API como backend próprio.
# Webhooks (Stripe, Efí, Meta) são servidor-para-servidor e não passam por CORS,
# então esta lista não afeta nenhum deles.
ORIGENS_PERMITIDAS = [
    "https://www.locadoraborajogar.com.br",
    "https://locadoraborajogar.com.br",
    "http://localhost:5173",  # Vite em desenvolvimento
    "http://localhost:3000",
]
_frontend_url = os.getenv("FRONTEND_URL", "").rstrip("/")
if _frontend_url and _frontend_url not in ORIGENS_PERMITIDAS:
    ORIGENS_PERMITIDAS.append(_frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGENS_PERMITIDAS,
    # Previews de branch do Vercel (staging), que mudam de URL a cada deploy.
    allow_origin_regex=r"https://[a-z0-9-]+\.vercel\.app",
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


def enviar_lembretes_devolucao():
    """
    Cron Job (A cada 15 minutos):
    Avisa por e-mail e WhatsApp os clientes cujo aluguel vence nas próximas 12h,
    lembrando de clicar em 'Devolver' pra garantir o Cashback e o Rank antes que
    o cron de vencidos derrube a conta automaticamente.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT l.id, u.nome, u.email, u.telefone, j.titulo
            FROM locacoes l
            JOIN utilizadores u ON u.id = l.utilizador_id
            JOIN contas_psn c ON c.id = l.conta_psn_id
            JOIN jogos j ON j.id = c.jogo_id
            WHERE l.status = 'ATIVA'
              AND l.lembrete_devolucao_enviado = FALSE
              AND l.data_fim BETWEEN CURRENT_TIMESTAMP AND CURRENT_TIMESTAMP + INTERVAL '12 hours'
            """)
        pendentes = cursor.fetchall()

        for loc in pendentes:
            if loc.get("email"):
                enviar_email(
                    loc["email"],
                    "⏰ Seu aluguel na Bora Jogar vence em breve!",
                    f"<p>Fala, {loc['nome']}! Faltam 12h pro fim do seu aluguel de <strong>{loc['titulo']}</strong>. "
                    "Não esqueça de clicar em 'Devolver' na aba 'Meus Acessos' pra garantir seu Cashback e subir de Rank!</p>",
                )
            if loc.get("telefone"):
                # O template aprovado pela Meta ficou só com {{1}} = nome do jogo
                # (a saudação com o nome do cliente caiu na revisão deles).
                enviar_template_whatsapp(
                    normalizar_telefone(loc["telefone"]),
                    "aluguel_devolucao",
                    [loc["titulo"]],
                )

            cursor.execute(
                "UPDATE locacoes SET lembrete_devolucao_enviado = TRUE WHERE id = %s",
                (loc["id"],),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro no Cron de Lembretes de Devolução: {e}")
    finally:
        cursor.close()
        conn.close()


def processar_filas_automaticamente():
    """
    Cron Job (A cada 1 minuto):
    O 'Maestro' da fila de espera. Entrega estritamente por ordem de chegada
    (sem exceção para pré-venda — o antigo furo de fila por rank foi removido).
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT DISTINCT
                f.jogo_id,
                j.titulo,
                f.tipo_slot
            FROM fila_espera f
            JOIN jogos j ON f.jogo_id = j.id
            JOIN contas_psn c ON c.jogo_id = j.id
            WHERE f.status = 'AGUARDANDO'
            AND (
                j.data_lancamento IS NULL
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

            coluna_status = (
                "status_primaria" if tipo_slot == "PRIMARIA" else "status_secundaria"
            )

            cursor.execute(
                f"SELECT id FROM contas_psn WHERE jogo_id = %s AND {coluna_status} = 'DISPONIVEL'",
                (jogo_id,),
            )
            contas_disponiveis = cursor.fetchall()

            for conta in contas_disponiveis:
                query_proximo = "SELECT fila_espera.id, fila_espera.utilizador_id, fila_espera.dias_aluguel, u.nome, u.email, u.telefone FROM fila_espera JOIN utilizadores u ON u.id = fila_espera.utilizador_id WHERE fila_espera.jogo_id = %s AND fila_espera.status = 'AGUARDANDO' AND fila_espera.tipo_slot = %s ORDER BY data_solicitacao ASC LIMIT 1"

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
    Rastreador de pagamentos. Vai na Efí e na Stripe bater na porta e resgatar
    transações pendentes que não dispararam Webhooks.

    É a rede de segurança das rotas de status: como elas agora exigem token,
    cliente deslogado ou com token vencido não confirma o pagamento pelo
    navegador — mas o dinheiro cai aqui em no máximo 1 minuto de qualquer jeito.
    Por isso passou a cobrir também as sessões da Stripe ('cs_...'), que antes
    eram puladas e só tinham o webhook como caminho.

    Antes de checar qualquer coisa na Efí/Stripe, expira em lote quem já passou
    da margem segura (Pix expira em 1h, sessão da Stripe em 24h — 48h cobre os
    dois com folga). Sem isso, um checkout abandonado vira uma chamada de rede
    por minuto para sempre; com isso, o pendente sai de cena sozinho e some
    também da lista de "Recargas Não Confirmadas" no Painel Admin.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "UPDATE pedidos_pix SET status = 'EXPIRADO' "
            "WHERE status = 'PENDENTE' AND data_criacao < CURRENT_TIMESTAMP - INTERVAL '48 hours'"
        )
        conn.commit()

        cursor.execute(
            "SELECT id, utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE status = 'PENDENTE'"
        )
        pendentes = cursor.fetchall()

        if pendentes:
            efi = EfiPay(credentials_efi)
            for pedido in pendentes:
                txid = pedido["id"]
                try:
                    if txid.startswith("cs_"):
                        sessao = stripe.checkout.Session.retrieve(txid)
                        foi_pago = sessao.payment_status == "paid"
                    else:
                        detalhes = efi.pix_detail_charge(params={"txid": txid})
                        foi_pago = detalhes.get("status") == "CONCLUIDA"

                    if foi_pago:
                        print(f"💰 PAGAMENTO PERDIDO RECUPERADO VIA CRON! ID: {txid}")
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
def forcar_filas(admin_data=Depends(verificar_admin)):
    """
    Botão manual para destravar a fila caso o Cron Job do Render hiberne.

    Exige admin: a rota é cara (varre filas, cria locações e dispara e-mail +
    WhatsApp) e estava aberta, então qualquer um podia mandar a API processar
    filas em looping e queimar as cotas de envio.
    """
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
    scheduler.add_job(enviar_lembretes_devolucao, "interval", minutes=15)

    # NOVO: Faxina de cupons rodando todo dia às 23:59
    scheduler.add_job(limpar_cupons_surpresa_cron, "cron", hour=23, minute=59)

    scheduler.start()
