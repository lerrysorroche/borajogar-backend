from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import hmac
import os
import random
import string
import base64
import stripe
from efipay import EfiPay

from database import get_db_connection
from auth import verificar_admin, verificar_usuario
from models import NovaRecarga, NovoCupom

router = APIRouter(tags=["Pagamentos"])


# ==============================================================================
# MODELOS (WEBHOOKS)
# ==============================================================================
class WebhookCupomN8n(BaseModel):
    codigo: str
    desconto_percent: float


# ==============================================================================
# CONFIGURAÇÕES DE GATEWAYS (STRIPE & EFÍ)
# ==============================================================================

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

# URLs de retorno do Stripe
URL_SUCESSO_FRONTEND = os.getenv("FRONTEND_URL", "http://localhost:3000") + "/sucesso"
URL_CANCELAMENTO_FRONTEND = (
    os.getenv("FRONTEND_URL", "http://localhost:3000") + "/carteira"
)

# Credenciais Efí (Pix)
EFI_CLIENT_ID = os.getenv("EFI_CLIENT_ID")
EFI_CLIENT_SECRET = os.getenv("EFI_CLIENT_SECRET")
EFI_CHAVE_PIX = os.getenv("EFI_CHAVE_PIX")

# Segredo combinado com a Efí no cadastro do webhook. O Render não deixa exigir
# mTLS de entrada (o TLS é gerenciado por eles, sem configuração de certificado
# de cliente), então a Efí documenta esta alternativa: cadastrar o webhook com o
# header 'x-skip-mtls-checking: true' e a URL no formato
# https://dominio/api/webhooks/efi?hmac=SEGREDO&ignorar=
# O '?ignorar=' existe porque a Efí anexa '/pix' ao final da URL cadastrada.
EFI_WEBHOOK_HMAC = os.getenv("EFI_WEBHOOK_HMAC")

# Segredo combinado com o n8n, enviado por ele no header 'X-N8N-Secret'.
N8N_WEBHOOK_SECRET = os.getenv("N8N_WEBHOOK_SECRET")

# Endereço público desta própria API, usado para montar a URL que a Efí chama.
API_PUBLIC_URL = os.getenv("API_PUBLIC_URL", "https://borajogar-api.onrender.com")

# Tetos de sanidade das rotas de recarga. Não são regra de negócio, são limite
# de estrago: seguram valores absurdos vindos de um cliente adulterado.
MAX_BONUS_CUPOM_PERCENT = 100.0
MAX_VALOR_RECARGA = 1000.0

# Decodificação do certificado mTLS da Efí (Necessário para o Render)
EFI_CERT_BASE64 = os.getenv("EFI_CERT_BASE64")
EFI_CERT_PATH = "/tmp/certificado_cert.pem"

if EFI_CERT_BASE64:
    try:
        with open(EFI_CERT_PATH, "wb") as cert_file:
            cert_file.write(base64.b64decode(EFI_CERT_BASE64))
    except Exception as e:
        print(f"Erro ao decodificar o certificado Efí: {e}")
else:
    print("⚠️ AVISO CRÍTICO: Variável EFI_CERT_BASE64 não encontrada!")

credentials_efi = {
    "client_id": EFI_CLIENT_ID,
    "client_secret": EFI_CLIENT_SECRET,
    "sandbox": False,
    "certificate": EFI_CERT_PATH,
}


# ==============================================================================
# MOTOR FINANCEIRO INTERNO
# ==============================================================================


def calcular_preco_vip(preco_base: float, rank_atual: int) -> float:
    """
    Calcula o preço final com desconto baseado no rank do cliente.
    Cada rank = 1% de desconto. Teto máximo de 20%.
    """
    # Garante que o desconto nunca passe de 20, mesmo se o rank for 50
    desconto_percentual = min(rank_atual, 20)

    # Calcula o valor do desconto e subtrai do preço base
    valor_desconto = preco_base * (desconto_percentual / 100.0)
    preco_final = preco_base - valor_desconto

    return round(preco_final, 2)  # Retorna com 2 casas decimais (ex: 39.90)


def _dono_ou_admin(usuario: dict, utilizador_id) -> bool:
    """
    Diz se o token apresentado pode mexer no pedido de 'utilizador_id'.
    Admin passa direto (usado pelo painel para destravar recarga de cliente).
    """
    if usuario.get("is_admin"):
        return True
    return usuario.get("id") == utilizador_id


def _pagamento_confirmado_na_efi(efi, txid, valor_esperado) -> bool:
    """
    Pergunta para a Efí se o Pix realmente caiu.

    Existe porque o corpo do webhook não prova nada: ele chega pela internet
    aberta e qualquer um pode montar um JSON com um txid válido. A resposta que
    vale é esta consulta de saída, autenticada pelo certificado mTLS da loja
    (EFI_CERT_BASE64) — essa sim o atacante não consegue falsificar.
    """
    try:
        detalhes = efi.pix_detail_charge(params={"txid": txid})
    except Exception as e:
        print(f"Webhook Efí: não deu para confirmar o txid {txid} na Efí: {e}")
        return False

    if detalhes.get("status") != "CONCLUIDA":
        print(
            f"Webhook Efí: txid {txid} avisado como pago, mas a Efí diz "
            f"status={detalhes.get('status')!r}. Ignorado."
        )
        return False

    # Confere o valor efetivamente liquidado quando a Efí devolve a lista de
    # pagamentos. Se ela não vier, o status CONCLUIDA já é a garantia.
    valores = [
        float(p.get("valor", 0)) for p in detalhes.get("pix", []) if p.get("valor")
    ]
    if valores and sum(valores) + 0.01 < float(valor_esperado):
        print(
            f"Webhook Efí: txid {txid} pago a menor "
            f"(recebido={sum(valores)}, esperado={valor_esperado}). Ignorado."
        )
        return False

    return True


def processar_sucesso_pagamento(
    payment_id, user_id, valor_pago, valor_bonus, cupom_nome
):
    """
    Função Interna (Core): A Máquina de Saldo.
    Chamada pelos Webhooks ou pelos validadores automáticos quando um pagamento é confirmado.
    Ela injeta o dinheiro na conta do cliente, aplica bônus de cupons e paga os 10%
    ao afiliado (se for a primeira recarga do usuário).
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # CLAIM ATÔMICO (trava contra Double Spend):
        # Esta única instrução é o que decide quem processa o pagamento. Como o
        # WHERE exige status = 'PENDENTE', apenas o PRIMEIRO chamador consegue
        # virar a linha; qualquer chamada concorrente (webhook + retorno do
        # navegador + polling + cron disparam juntos) casa com 0 linhas e sai
        # sem creditar nada.
        # ATENÇÃO: não troque isto por um SELECT seguido de IF — o intervalo
        # entre ler e trancar é justamente o que causava o crédito em dobro.
        cursor.execute(
            "UPDATE pedidos_pix SET status = 'CONCLUIDO' WHERE id = %s AND status = 'PENDENTE'",
            (payment_id,),
        )
        if cursor.rowcount == 0:
            conn.rollback()
            return

        cursor.execute(
            "SELECT COUNT(*) as qtd FROM transacoes WHERE utilizador_id = %s AND descricao LIKE 'Recarga%%'",
            (user_id,),
        )
        eh_primeira_recarga = cursor.fetchone()["qtd"] == 0
        valor_total = valor_pago + valor_bonus

        # 1. Adiciona o saldo principal (Recarga + Cupom)
        cursor.execute(
            "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s RETURNING nome, indicado_por",
            (valor_total, user_id),
        )
        cliente = cursor.fetchone()

        # Gera extrato da recarga
        cursor.execute(
            "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, 'Recarga de Carteira (Cartão/Pix)')",
            (user_id, valor_pago),
        )

        # Gera extrato do bônus do cupom
        if valor_bonus > 0:
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)",
                (user_id, valor_bonus, f"🎟️ Bônus Cupom ({cupom_nome})"),
            )
            cursor.execute("SELECT id FROM cupons WHERE codigo = %s", (cupom_nome,))
            cupom_db = cursor.fetchone()
            if cupom_db:
                cursor.execute(
                    "INSERT INTO cupons_usados (utilizador_id, cupom_id) VALUES (%s, %s)",
                    (user_id, cupom_db["id"]),
                )

        # 2. Lógica de Afiliados (Indique e Ganhe)
        if eh_primeira_recarga and cliente["indicado_por"]:
            id_amigo = cliente["indicado_por"]
            valor_indicacao = valor_pago * 0.10
            cursor.execute(
                "UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s",
                (valor_indicacao, id_amigo),
            )
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)",
                (
                    id_amigo,
                    valor_indicacao,
                    f"🎁 Bônus de Indicação ({cliente['nome']})",
                ),
            )

        # O claim do topo já marcou o pedido como CONCLUIDO. Commit fecha o
        # claim e os créditos juntos: se algo falhar no meio, o rollback
        # devolve o pedido para PENDENTE e ele pode ser reprocessado.
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro Pagamento DB: {e}")
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# CHECKOUTS E GERAÇÃO DE COBRANÇA
# ==============================================================================


@router.post("/recarga/cartao")
def gerar_checkout_stripe(recarga: NovaRecarga, usuario=Depends(verificar_usuario)):
    """
    [C] Gera um link seguro de pagamento via Cartão de Crédito na Stripe.
    Valida as regras de cupons e amarra as variáveis no `metadata` do Stripe
    para que o Webhook consiga processar o saldo posteriormente.

    O cliente vem do token, nunca do corpo: antes dava para abrir cobrança em
    nome de qualquer id e, com cupom percentual, plantar um bônus na conta alheia.
    """
    utilizador_id = usuario["id"]
    if recarga.valor < 30.0:
        raise HTTPException(status_code=400, detail="Mínimo R$ 30,00.")
    if recarga.valor > MAX_VALOR_RECARGA:
        raise HTTPException(
            status_code=400, detail=f"Máximo R$ {MAX_VALOR_RECARGA:.2f} por recarga."
        )
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        valor_bonus_cupom = 0.0
        cupom_nome = recarga.cupom.upper() if recarga.cupom else ""

        if recarga.cupom:
            cursor.execute(
                "SELECT id, tipo, valor FROM cupons WHERE codigo = %s AND ativo = TRUE",
                (cupom_nome,),
            )
            cupom = cursor.fetchone()
            if not cupom:
                raise HTTPException(status_code=404, detail="Cupom inválido.")
            cursor.execute(
                "SELECT id FROM cupons_usados WHERE utilizador_id = %s AND cupom_id = %s",
                (utilizador_id, cupom["id"]),
            )
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Cupom já utilizado.")
            valor_bonus_cupom = (
                cupom["valor"]
                if cupom["tipo"] == "FIXO"
                else recarga.valor * (cupom["valor"] / 100.0)
            )

        cursor.execute(
            "SELECT email FROM utilizadores WHERE id = %s", (utilizador_id,)
        )
        usr = cursor.fetchone()
        base_frontend = os.getenv("FRONTEND_URL", "http://localhost:3000").rstrip("/")

        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            customer_email=usr["email"],
            line_items=[
                {
                    "price_data": {
                        "currency": "brl",
                        "product_data": {"name": "Recarga Bora Jogar"},
                        "unit_amount": int(recarga.valor * 100),
                    },
                    "quantity": 1,
                }
            ],
            mode="payment",
            success_url=f"{base_frontend}/?aba=dashboard&stripe_session={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{base_frontend}/?aba=dashboard&stripe_cancelado=true",
            metadata={
                "utilizador_id": str(utilizador_id),
                "valor_pago": str(recarga.valor),
                "valor_bonus": str(valor_bonus_cupom),
                "cupom": cupom_nome,
            },
        )

        # Reaproveita a tabela do Pix para mapear Sessões do Stripe como pendentes
        cursor.execute(
            "INSERT INTO pedidos_pix (id, utilizador_id, valor_pago, valor_bonus, cupom, status) VALUES (%s, %s, %s, %s, %s, 'PENDENTE')",
            (
                session.id,
                utilizador_id,
                recarga.valor,
                valor_bonus_cupom,
                cupom_nome,
            ),
        )
        conn.commit()
        return {"checkout_url": session.url, "payment_id": session.id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/recarga/pix")
def gerar_pix_efi(recarga: NovaRecarga, usuario=Depends(verificar_usuario)):
    """
    [C] Gera o QR Code Pix e Linha Digitável usando a Efí Pay.
    Armazena o ID da transação (txid) no banco aguardando o pagamento do cliente.

    Mesmo motivo do /recarga/cartao: o cliente vem do token, não do corpo.
    """
    utilizador_id = usuario["id"]
    if recarga.valor < 30.0:
        raise HTTPException(status_code=400, detail="Mínimo R$ 30,00.")
    if recarga.valor > MAX_VALOR_RECARGA:
        raise HTTPException(
            status_code=400, detail=f"Máximo R$ {MAX_VALOR_RECARGA:.2f} por recarga."
        )
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        valor_bonus_cupom = 0.0
        cupom_nome = recarga.cupom.upper() if recarga.cupom else ""

        if recarga.cupom:
            cursor.execute(
                "SELECT id, tipo, valor FROM cupons WHERE codigo = %s AND ativo = TRUE",
                (cupom_nome,),
            )
            cupom = cursor.fetchone()
            if not cupom:
                raise HTTPException(status_code=404, detail="Cupom inválido.")
            cursor.execute(
                "SELECT id FROM cupons_usados WHERE utilizador_id = %s AND cupom_id = %s",
                (utilizador_id, cupom["id"]),
            )
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Cupom já utilizado.")
            valor_bonus_cupom = (
                cupom["valor"]
                if cupom["tipo"] == "FIXO"
                else recarga.valor * (cupom["valor"] / 100.0)
            )

        if not EFI_CHAVE_PIX:
            raise HTTPException(
                status_code=400, detail="Chave Pix não configurada no servidor."
            )

        efi = EfiPay(credentials_efi)
        txid = "".join(random.choices(string.ascii_letters + string.digits, k=30))
        body = {
            "calendario": {"expiracao": 3600},
            "valor": {"original": f"{recarga.valor:.2f}"},
            "chave": EFI_CHAVE_PIX,
            "infoAdicionais": [{"nome": "Serviço", "valor": "Recarga Bora Jogar"}],
        }

        try:
            resposta_cob = efi.pix_create_charge(params={"txid": txid}, body=body)
        except Exception as err:
            raise HTTPException(
                status_code=400, detail=f"Erro de comunicação com a Efí: {str(err)}"
            )

        loc_id = resposta_cob.get("loc", {}).get("id")
        try:
            resposta_qr = efi.pix_generate_qrcode(params={"id": loc_id})
        except Exception as err:
            raise HTTPException(
                status_code=400, detail=f"Erro ao gerar Imagem QR Code: {str(err)}"
            )

        cursor.execute(
            "INSERT INTO pedidos_pix (id, utilizador_id, valor_pago, valor_bonus, cupom, status) VALUES (%s, %s, %s, %s, %s, 'PENDENTE')",
            (txid, utilizador_id, recarga.valor, valor_bonus_cupom, cupom_nome),
        )
        conn.commit()
        return {
            "payment_id": txid,
            "copia_cola": resposta_qr.get("qrcode"),
            "qr_code": resposta_qr.get("imagemQrcode"),
        }
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# SINCRONIZAÇÃO E WEBHOOKS (RECONCILIAÇÃO FINANCEIRA)
# ==============================================================================


@router.post("/recarga/sincronizar")
def sincronizar_pagamentos_pendentes(usuario=Depends(verificar_usuario)):
    """
    [R/U] Sincronização Passiva (Lazy Sync).
    Puxa do banco todas as transações PENDENTES do cliente. Consulta diretamente
    os gateways (Stripe ou Efí) e aprova as que ficaram 'travadas' no limbo,
    gerando o saldo atrasado imediatamente.

    O id do cliente vem do token, não da URL. Antes era um GET público com o id
    na rota — e como o id é sequencial, dava para varrer 1..N e disparar, sem
    login, uma chamada à Efí e uma conexão de banco por cliente cadastrado.
    É POST porque tem efeito colateral: GET com efeito colateral é candidato a
    prefetch de navegador/CDN e a retry automático de proxy.
    """
    utilizador_id = usuario["id"]
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE utilizador_id = %s AND status = 'PENDENTE'",
            (utilizador_id,),
        )
        pendentes = cursor.fetchall()

        if not pendentes:
            return {"mensagem": "Nenhum pagamento pendente."}

        efi = EfiPay(credentials_efi)

        for ped in pendentes:
            payment_id = ped["id"]

            if payment_id.startswith("cs_"):
                # Sincronização Stripe
                try:
                    session = stripe.checkout.Session.retrieve(payment_id)
                    if session.payment_status == "paid":
                        processar_sucesso_pagamento(
                            payment_id,
                            utilizador_id,
                            ped["valor_pago"],
                            ped["valor_bonus"],
                            ped["cupom"],
                        )
                except Exception as e:
                    print(f"Erro ao sincronizar Stripe: {e}")
                    pass
            else:
                # Sincronização Efí Pay
                try:
                    detalhes = efi.pix_detail_charge(params={"txid": payment_id})
                    if detalhes.get("status") == "CONCLUIDA":
                        processar_sucesso_pagamento(
                            payment_id,
                            utilizador_id,
                            ped["valor_pago"],
                            ped["valor_bonus"],
                            ped["cupom"],
                        )
                except Exception as e:
                    print(f"Erro ao sincronizar Efí: {e}")
                    pass

        return {"mensagem": "Sincronização concluída com sucesso."}
    finally:
        cursor.close()
        conn.close()


@router.get("/recarga/status/{payment_id}")
def checar_status_pagamento_inteligente(
    payment_id: str, usuario=Depends(verificar_usuario)
):
    """
    [R] Active Polling do React. O frontend chama essa rota a cada 5s para verificar o Pix.

    Exige token porque a rota credita saldo ao confirmar o pagamento na Efí.
    Pedido de terceiro responde NAO_ENCONTRADO (mesma resposta de pedido
    inexistente) para a rota não virar um oráculo de 'este txid existe/está pago'.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT status, utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE id = %s",
            (payment_id,),
        )
        pedido = cursor.fetchone()
        if not pedido or not _dono_ou_admin(usuario, pedido["utilizador_id"]):
            return {"status": "NAO_ENCONTRADO"}
        if pedido["status"] == "CONCLUIDO":
            return {"status": "PAGO"}

        efi = EfiPay(credentials_efi)
        try:
            detalhes = efi.pix_detail_charge(params={"txid": payment_id})
            if detalhes.get("status") == "CONCLUIDA":
                processar_sucesso_pagamento(
                    payment_id,
                    pedido["utilizador_id"],
                    pedido["valor_pago"],
                    pedido["valor_bonus"],
                    pedido["cupom"],
                )
                return {"status": "PAGO"}
        except Exception:
            pass
        return {"status": "PENDENTE"}
    finally:
        cursor.close()
        conn.close()


@router.get("/recarga/status-stripe/{session_id}")
def checar_status_stripe_inteligente(
    session_id: str, usuario=Depends(verificar_usuario)
):
    """
    [R] Rota disparada assim que o cliente é devolvido da tela da Stripe para o seu site.

    Mesmo motivo do /recarga/status: credita saldo, então exige token. O id da
    sessão ('cs_...') aparece na barra de endereços no retorno do checkout e
    vaza fácil por Referer e ferramentas de analytics.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT status, utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE id = %s",
            (session_id,),
        )
        pedido = cursor.fetchone()
        if not pedido or not _dono_ou_admin(usuario, pedido["utilizador_id"]):
            return {"status": "NAO_ENCONTRADO"}
        if pedido["status"] == "CONCLUIDO":
            return {"status": "PAGO"}

        try:
            session = stripe.checkout.Session.retrieve(session_id)
            if session.payment_status == "paid":
                processar_sucesso_pagamento(
                    session_id,
                    pedido["utilizador_id"],
                    pedido["valor_pago"],
                    pedido["valor_bonus"],
                    pedido["cupom"],
                )
                return {"status": "PAGO"}
        except Exception:
            pass
        return {"status": "PENDENTE"}
    finally:
        cursor.close()
        conn.close()


@router.post("/api/webhooks/stripe")
async def stripe_webhook(request: Request):
    """
    [C] Ouve os eventos nos bastidores enviados pela infraestrutura da Stripe.
    A validação da assinatura garante que foi a Stripe real enviando o pacote.
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Stripe Signature")

    if event["type"] in [
        "checkout.session.completed",
        "checkout.session.async_payment_succeeded",
    ]:
        session = event["data"]["object"]
        if session.payment_status == "paid":
            processar_sucesso_pagamento(
                session.id,
                int(session.metadata.get("utilizador_id")),
                float(session.metadata.get("valor_pago")),
                float(session.metadata.get("valor_bonus")),
                session.metadata.get("cupom"),
            )
    return {"status": "success"}


@router.post("/api/webhooks/efi")
async def efi_webhook(request: Request):
    """
    [C] Ouve os eventos disparados pela Efí Pay.
    Usado como auxiliar da função 'sincronizar_pagamentos_pendentes' e do cron
    'verificar_pix_perdidos'.

    Duas travas, porque aqui não dá para usar o mTLS que a Efí recomenda (o
    Render termina o TLS e não expõe certificado de cliente para a aplicação):

    1. hmac combinado na URL do cadastro do webhook, comparado em tempo
       constante — barra quem não conhece o segredo.
    2. Confirmação do pagamento direto na Efí antes de creditar qualquer coisa.

    A trava 2 é a que importa de verdade. Sem ela, bastava gerar um Pix em
    /recarga/pix (que devolve o txid na resposta), não pagar, e mandar
    {"pix":[{"txid": "<o próprio txid>"}]} para cá: o saldo caía sem pagamento.
    O claim atômico de processar_sucesso_pagamento não pega esse caso — ele
    impede crédito repetido, não crédito de um pagamento que nunca existiu.
    """
    if not EFI_WEBHOOK_HMAC:
        # Falha fechada de propósito: sem o segredo configurado não há como
        # separar a Efí de um curioso. O cron 'verificar_pix_perdidos' varre os
        # pendentes a cada minuto, então nenhum pagamento se perde por isso.
        print(
            "⚠️ Webhook Efí recusado: EFI_WEBHOOK_HMAC não configurada. "
            "A reconciliação segue pelo cron de Pix perdidos."
        )
        raise HTTPException(status_code=503, detail="Webhook não configurado.")

    if not hmac.compare_digest(request.query_params.get("hmac", ""), EFI_WEBHOOK_HMAC):
        raise HTTPException(status_code=403, detail="Assinatura inválida.")

    try:
        request_data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Corpo inválido.")

    if not isinstance(request_data, dict):
        raise HTTPException(status_code=400, detail="Corpo inválido.")

    pagamentos = request_data.get("pix", [])
    if not pagamentos:
        # Ping de teste do cadastro do webhook e outros eventos sem Pix.
        return {"status": "200 OK"}

    efi = EfiPay(credentials_efi)
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        for pag in pagamentos:
            txid = pag.get("txid")
            if not txid:
                continue

            cursor.execute(
                "SELECT utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE id = %s AND status = 'PENDENTE'",
                (txid,),
            )
            pedido = cursor.fetchone()
            if not pedido:
                continue

            if not _pagamento_confirmado_na_efi(efi, txid, pedido["valor_pago"]):
                continue

            processar_sucesso_pagamento(
                txid,
                pedido["utilizador_id"],
                pedido["valor_pago"],
                pedido["valor_bonus"],
                pedido["cupom"],
            )
        return {"status": "200 OK"}
    except Exception as e:
        print(f"Erro processando Webhook Efí: {e}")
        raise HTTPException(
            status_code=500, detail="Erro interno processando Webhook Efí"
        )
    finally:
        # Sem o finally, qualquer exceção no meio do loop deixava a conexão
        # pendurada até o pool do Postgres estourar.
        cursor.close()
        conn.close()


@router.post("/api/webhooks/n8n/cupom-surpresa")
def criar_cupom_n8n(dados: WebhookCupomN8n, request: Request):
    """
    [C] Rota exclusiva para o n8n criar cupons dinâmicos do Instagram.
    Como o sistema usa "Bônus de Recarga", basta salvar no banco como PERCENTUAL.

    Duas travas, porque esta rota fabrica dinheiro: o cupom vira bônus de saldo
    na recarga. Aberta e sem teto, qualquer um criava um cupom de 10000% e
    recarregava a carteira por centavos.

    1. Segredo combinado no header 'X-N8N-Secret', comparado em tempo constante.
    2. Teto de percentual, para limitar o estrago mesmo se o segredo vazar.
    """
    if not N8N_WEBHOOK_SECRET:
        # Falha fechada: sem segredo configurado não há como distinguir o n8n
        # de um curioso. Cupons continuam podendo ser criados pelo Painel Admin.
        print("⚠️ Webhook n8n recusado: N8N_WEBHOOK_SECRET não configurada.")
        raise HTTPException(status_code=503, detail="Webhook não configurado.")

    if not hmac.compare_digest(
        request.headers.get("x-n8n-secret", ""), N8N_WEBHOOK_SECRET
    ):
        raise HTTPException(status_code=403, detail="Segredo inválido.")

    if not 0 < dados.desconto_percent <= MAX_BONUS_CUPOM_PERCENT:
        raise HTTPException(
            status_code=400,
            detail=f"Percentual deve ser maior que 0 e no máximo {MAX_BONUS_CUPOM_PERCENT}.",
        )

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Usa 'PERCENTUAL' para que a função gerar_checkout_stripe calcule a porcentagem em cima da recarga
        cursor.execute(
            "INSERT INTO cupons (codigo, tipo, valor) VALUES (%s, 'PERCENTUAL', %s)",
            (dados.codigo.upper(), dados.desconto_percent),
        )
        conn.commit()
        return {
            "status": "sucesso",
            "mensagem": f"Cupom {dados.codigo.upper()} ativado na Bora Jogar!",
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=400, detail=f"Erro ao cadastrar cupom: {str(e)}"
        )
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# MANUTENÇÃO DO WEBHOOK DA EFÍ (ADMIN)
# ==============================================================================


def _url_webhook_efi() -> str:
    """
    Monta a URL que a Efí deve chamar.

    O '?ignorar=' no final não é enfeite: a Efí anexa '/pix' ao final da URL
    cadastrada. Sem esse parâmetro, o '/pix' entraria no caminho
    ('/api/webhooks/efi/pix') e cairia em 404, porque essa rota não existe.
    Com ele, o '/pix' vira apenas o valor de um parâmetro ignorado e o caminho
    continua correto.
    """
    return f"{API_PUBLIC_URL}/api/webhooks/efi?hmac={EFI_WEBHOOK_HMAC}&ignorar="


@router.get("/admin/efi/webhook")
def consultar_webhook_efi(admin_data=Depends(verificar_admin)):
    """[R] Mostra o webhook que está cadastrado hoje na Efí, para diagnóstico."""
    if not EFI_CHAVE_PIX:
        raise HTTPException(status_code=400, detail="EFI_CHAVE_PIX não configurada.")
    try:
        efi = EfiPay(credentials_efi)
        atual = efi.pix_detail_webhook(params={"chave": EFI_CHAVE_PIX})
        return {"cadastrado": atual, "url_esperada": _url_webhook_efi()}
    except Exception as e:
        # Webhook nunca cadastrado devolve erro na Efí; isso é informação útil,
        # não falha da rota.
        return {
            "cadastrado": None,
            "detalhe": str(e),
            "url_esperada": _url_webhook_efi(),
        }


@router.put("/admin/efi/webhook")
def configurar_webhook_efi(admin_data=Depends(verificar_admin)):
    """
    [U] (Re)cadastra o webhook do Pix na Efí apontando para esta API.

    Roda aqui dentro em vez de num script local porque o certificado mTLS da
    loja já está decodificado neste servidor (EFI_CERT_BASE64 -> /tmp).

    O header 'x-skip-mtls-checking' é a alternativa que a Efí documenta para
    hospedagens que não deixam a aplicação exigir certificado de cliente na
    entrada — caso do Render, onde o TLS é terminado por eles. A autenticidade
    de quem chama passa a ser garantida pelo 'hmac' na URL, e o crédito só
    acontece após confirmação de saída na própria Efí (ver efi_webhook).
    """
    if not EFI_CHAVE_PIX:
        raise HTTPException(status_code=400, detail="EFI_CHAVE_PIX não configurada.")
    if not EFI_WEBHOOK_HMAC:
        raise HTTPException(
            status_code=400,
            detail="EFI_WEBHOOK_HMAC não configurada. Defina no Render antes de cadastrar.",
        )

    try:
        efi = EfiPay(credentials_efi)
        resposta = efi.pix_config_webhook(
            params={"chave": EFI_CHAVE_PIX},
            body={"webhookUrl": _url_webhook_efi()},
            headers={"x-skip-mtls-checking": "true"},
        )
        return {
            "mensagem": "Webhook cadastrado na Efí com sucesso!",
            "url": _url_webhook_efi(),
            "resposta_efi": resposta,
        }
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Erro ao cadastrar webhook na Efí: {str(e)}"
        )


# ==============================================================================
# GESTÃO DE CUPONS (ADMIN)
# ==============================================================================


@router.get("/admin/cupons")
def listar_cupons(admin_data=Depends(verificar_admin)):
    """[R] Retorna os cupons cadastrados e disponíveis para os clientes."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM cupons ORDER BY id DESC")
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return resultados


@router.post("/admin/cupons")
def criar_cupom(cupom: NovoCupom, admin_data=Depends(verificar_admin)):
    """[C] Criação de desconto de saldo. Pode ser percentual (10%) ou Fixo (R$ 10,00)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO cupons (codigo, tipo, valor) VALUES (%s, %s, %s)",
            (cupom.codigo.upper(), cupom.tipo.upper(), cupom.valor),
        )
        conn.commit()
        return {"mensagem": "Cupom criado com sucesso!"}
    except Exception:
        conn.rollback()
        raise HTTPException(
            status_code=400, detail="Erro: Este código de cupom já existe."
        )
    finally:
        cursor.close()
        conn.close()


@router.delete("/admin/cupons/{cupom_id}")
def remover_cupom(cupom_id: int, admin_data=Depends(verificar_admin)):
    """[D] Invalida um cupom do banco de dados (Hard Delete)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cupons WHERE id = %s", (cupom_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"mensagem": "Cupom deletado."}
