from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2.extras import RealDictCursor
from datetime import datetime

from database import get_db_connection
from routes import usuarios, jogos, pagamentos, alugueis, admin

app = FastAPI(title="API Locadora PS5")

# 1. Middlewares (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Injetando os Módulos de Rotas
app.include_router(usuarios.router)
app.include_router(jogos.router)
app.include_router(pagamentos.router)
app.include_router(alugueis.router)
app.include_router(admin.router)


@app.get("/")
def home():
    return {"mensagem": "API Online e Modularizada 🚀"}


@app.get("/configuracoes")
def get_config():
    """Rota pública para o frontend puxar os Banners e as Regras do Sistema"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT devolucao_dinamica, valor_por_dia, anuncio_ativo, mensagem_anuncio, banners_url FROM configuracoes LIMIT 1"
    )
    config = cursor.fetchone()
    cursor.close()
    conn.close()
    return (
        config
        if config
        else {
            "devolucao_dinamica": False,
            "valor_por_dia": 2.0,
            "anuncio_ativo": False,
            "mensagem_anuncio": "",
            "banners_url": "",
        }
    )


# ==============================================================================
# MOTOR DE TAREFAS AUTOMÁTICAS (CRON JOBS)
# ==============================================================================
def verificar_alugueis_vencidos():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id, conta_psn_id FROM locacoes WHERE status = 'ATIVA' AND data_fim <= CURRENT_TIMESTAMP"
        )
        locacoes_vencidas = cursor.fetchall()
        if locacoes_vencidas:
            for loc in locacoes_vencidas:
                cursor.execute(
                    "UPDATE locacoes SET status = 'EXPIRADA' WHERE id = %s", (loc[0],)
                )
                cursor.execute(
                    "UPDATE contas_psn SET status = 'MANUTENCAO' WHERE id = %s",
                    (loc[1],),
                )
            conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def processar_filas_automaticamente():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT DISTINCT f.jogo_id, j.titulo, j.data_lancamento
            FROM fila_espera f
            JOIN jogos j ON f.jogo_id = j.id
            JOIN contas_psn c ON c.jogo_id = j.id
            WHERE f.status = 'AGUARDANDO' AND c.status = 'DISPONIVEL'
        """)
        jogos_pendentes = cursor.fetchall()
        hoje_str = datetime.now().strftime("%Y-%m-%d")

        for jp in jogos_pendentes:
            jogo_id, titulo, data_lanc_str = (
                jp["jogo_id"],
                jp["titulo"],
                str(jp["data_lancamento"]) if jp["data_lancamento"] else None,
            )
            if data_lanc_str and data_lanc_str > hoje_str:
                continue

            cursor.execute(
                "SELECT id FROM contas_psn WHERE jogo_id = %s AND status = 'DISPONIVEL'",
                (jogo_id,),
            )
            contas_disponiveis = cursor.fetchall()

            for conta in contas_disponiveis:
                eh_pre_venda = data_lanc_str and str(data_lanc_str) >= hoje_str
                query_proximo = "SELECT id, utilizador_id, dias_aluguel FROM fila_espera WHERE jogo_id = %s AND status = 'AGUARDANDO' ORDER BY "
                ordem = (
                    "(SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') DESC, data_solicitacao ASC LIMIT 1"
                    if eh_pre_venda
                    else "data_solicitacao ASC LIMIT 1"
                )

                cursor.execute(query_proximo + ordem, (jogo_id,))
                proximo = cursor.fetchone()

                if proximo:
                    cursor.execute(
                        "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA')",
                        (
                            proximo["utilizador_id"],
                            conta["id"],
                            proximo.get("dias_aluguel", 7),
                        ),
                    )
                    cursor.execute(
                        "UPDATE fila_espera SET status = 'CONCLUIDO' WHERE id = %s",
                        (proximo["id"],),
                    )
                    cursor.execute(
                        "UPDATE contas_psn SET status = 'ALUGADA' WHERE id = %s",
                        (conta["id"],),
                    )
                    msg = f"🎉 SEU ACESSO FOI LIBERADO! O jogo {titulo} já está na aba 'Meus Acessos'. Bom jogo!"
                    cursor.execute(
                        "INSERT INTO notificacoes (utilizador_id, reserva_id, jogo, mensagem) VALUES (%s, %s, %s, %s)",
                        (proximo["utilizador_id"], proximo["id"], titulo, msg),
                    )
                    conn.commit()
                else:
                    break
    except Exception as e:
        conn.rollback()
        print(f"Erro Fila: {e}")
    finally:
        cursor.close()
        conn.close()


@app.post("/admin/forcar-processamento-filas", tags=["Admin"])
def forcar_filas():
    processar_filas_automaticamente()
    return {"mensagem": "O motor de filas rodou com sucesso!"}


@app.on_event("startup")
def iniciar_servicos():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS notificacoes (id SERIAL PRIMARY KEY, utilizador_id INT, reserva_id INT, jogo VARCHAR(255), mensagem TEXT, lida BOOLEAN DEFAULT FALSE, data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.commit()
        try:
            cursor.execute(
                "ALTER TABLE fila_espera ADD COLUMN dias_aluguel INT DEFAULT 7"
            )
            conn.commit()
        except Exception:
            conn.rollback()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ AVISO DE STARTUP: Banco de dados ainda acordando. Detalhe: {e}")

    scheduler = BackgroundScheduler()
    scheduler.add_job(verificar_alugueis_vencidos, "interval", minutes=1)
    scheduler.add_job(processar_filas_automaticamente, "interval", minutes=1)
    scheduler.start()
