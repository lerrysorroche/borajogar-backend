from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import os
import random
import string
import base64
import stripe
from efipay import EfiPay

from database import get_db_connection
from auth import verificar_admin
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
        cursor.execute("SELECT status FROM pedidos_pix WHERE id = %s", (payment_id,))
        pedido = cursor.fetchone()

        # Só processa se estiver PENDENTE, evitando duplicidade (Double Spend)
        if pedido and pedido["status"] == "PENDENTE":
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

            # 3. Tranca o pedido para evitar que outro webhook processe de novo
            cursor.execute(
                "UPDATE pedidos_pix SET status = 'CONCLUIDO' WHERE id = %s",
                (payment_id,),
            )
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
def gerar_checkout_stripe(recarga: NovaRecarga):
    """
    [C] Gera um link seguro de pagamento via Cartão de Crédito na Stripe.
    Valida as regras de cupons e amarra as variáveis no `metadata` do Stripe
    para que o Webhook consiga processar o saldo posteriormente.
    """
    if recarga.valor < 30.0:
        raise HTTPException(status_code=400, detail="Mínimo R$ 30,00.")
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
                (recarga.utilizador_id, cupom["id"]),
            )
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Cupom já utilizado.")
            valor_bonus_cupom = (
                cupom["valor"]
                if cupom["tipo"] == "FIXO"
                else recarga.valor * (cupom["valor"] / 100.0)
            )

        cursor.execute(
            "SELECT email FROM utilizadores WHERE id = %s", (recarga.utilizador_id,)
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
                "utilizador_id": str(recarga.utilizador_id),
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
                recarga.utilizador_id,
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
def gerar_pix_efi(recarga: NovaRecarga):
    """
    [C] Gera o QR Code Pix e Linha Digitável usando a Efí Pay.
    Armazena o ID da transação (txid) no banco aguardando o pagamento do cliente.
    """
    if recarga.valor < 30.0:
        raise HTTPException(status_code=400, detail="Mínimo R$ 30,00.")
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
                (recarga.utilizador_id, cupom["id"]),
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
            (txid, recarga.utilizador_id, recarga.valor, valor_bonus_cupom, cupom_nome),
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


@router.get("/recarga/sincronizar/{utilizador_id}")
def sincronizar_pagamentos_pendentes(utilizador_id: int):
    """
    [R/U] Sincronização Passiva (Lazy Sync).
    Puxa do banco todas as transações PENDENTES do cliente. Consulta diretamente
    os gateways (Stripe ou Efí) e aprova as que ficaram 'travadas' no limbo,
    gerando o saldo atrasado imediatamente.
    """
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
def checar_status_pagamento_inteligente(payment_id: str):
    """[R] Active Polling do React. O frontend chama essa rota a cada 5s para verificar o Pix."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT status, utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE id = %s",
            (payment_id,),
        )
        pedido = cursor.fetchone()
        if not pedido:
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
def checar_status_stripe_inteligente(session_id: str):
    """[R] Rota disparada assim que o cliente é devolvido da tela da Stripe para o seu site."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT status, utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE id = %s",
            (session_id,),
        )
        pedido = cursor.fetchone()
        if not pedido:
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
    Nota: Frequentemente bloqueado por firewalls devido a exigências de mTLS.
    Usado como auxiliar da função 'sincronizar_pagamentos_pendentes'.
    """
    try:
        request_data = await request.json()
        pagamentos = request_data.get("pix", [])
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        for pag in pagamentos:
            txid = pag.get("txid")
            cursor.execute(
                "SELECT utilizador_id, valor_pago, valor_bonus, cupom FROM pedidos_pix WHERE id = %s AND status = 'PENDENTE'",
                (txid,),
            )
            pedido = cursor.fetchone()
            if pedido:
                processar_sucesso_pagamento(
                    txid,
                    pedido["utilizador_id"],
                    pedido["valor_pago"],
                    pedido["valor_bonus"],
                    pedido["cupom"],
                )
        cursor.close()
        conn.close()
        return {"status": "200 OK"}
    except Exception:
        raise HTTPException(
            status_code=500, detail="Erro interno processando Webhook Efí"
        )


@router.post("/api/webhooks/n8n/cupom-surpresa")
def criar_cupom_n8n(dados: WebhookCupomN8n):
    """
    [C] Rota exclusiva para o n8n criar cupons dinâmicos do Instagram.
    Como o sistema usa "Bônus de Recarga", basta salvar no banco como PERCENTUAL.
    """
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


@router.delete("/api/webhooks/n8n/limpar-cupons-surpresa")
def limpar_cupons_surpresa():
    """
    [D] Rota de limpeza: Apaga todos os cupons que começam com 'SURPRESA',
    garantindo que as campanhas de 24h do Instagram expirem à meia-noite.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # IMPORTANTE: No psycopg2 (banco de dados), usamos '%%' para representar o curinga '%'
        cursor.execute("DELETE FROM cupons WHERE codigo LIKE 'SURPRESA%%'")
        apagados = cursor.rowcount
        conn.commit()

        return {
            "status": "sucesso",
            "mensagem": f"Limpeza concluída. {apagados} cupom(ns) apagado(s).",
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erro na limpeza: {str(e)}")
    finally:
        cursor.close()
        conn.close()
