import hashlib
import hmac
import json
import os
import re
import urllib.request

from fastapi import APIRouter, Depends, HTTPException, Request
from psycopg2.extras import RealDictCursor

from auth import verificar_admin
from database import get_db_connection
from models import AtualizarTelefoneRequest, VerificarWhatsAdminRequest

router = APIRouter(tags=["Verificação WhatsApp"])

META_WHATSAPP_TOKEN = os.getenv("META_WHATSAPP_TOKEN")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID")
META_WEBHOOK_VERIFY_TOKEN = os.getenv("META_WEBHOOK_VERIFY_TOKEN")
META_APP_SECRET = os.getenv("META_APP_SECRET")


def normalizar_telefone(numero: str) -> str:
    """
    Deixa só os dígitos e garante o prefixo do Brasil (55), do mesmo jeito que o
    Admin já faz no frontend ao montar o link do wa.me.
    """
    digitos = re.sub(r"\D", "", numero or "")
    if not digitos.startswith("55"):
        digitos = "55" + digitos
    return digitos


def enviar_mensagem_whatsapp(numero_destino: str, texto: str):
    """
    Dispara uma mensagem de texto via WhatsApp Cloud API. Roda de forma silenciosa
    (fire-and-forget) igual ao disparar_email_confirmacao, pra nunca travar o webhook.
    """
    try:
        if not (META_WHATSAPP_TOKEN and META_PHONE_NUMBER_ID):
            return
        url = f"https://graph.facebook.com/v20.0/{META_PHONE_NUMBER_ID}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "to": numero_destino,
            "type": "text",
            "text": {"body": texto},
        }
        req_http = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {META_WHATSAPP_TOKEN}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req_http) as response:
            pass
    except Exception as e:
        print(f"Aviso: Falha ao enviar mensagem de confirmação no WhatsApp: {e}")


# ==============================================================================
# WEBHOOK OFICIAL DA META (WHATSAPP CLOUD API)
# ==============================================================================


@router.get("/webhook/whatsapp")
def verificar_webhook_whatsapp(request: Request):
    """
    [R] Handshake de verificação exigido pela Meta ao cadastrar a URL do webhook.
    Confere o token secreto combinado no painel da Meta e devolve o 'challenge'.
    """
    params = request.query_params
    modo = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if modo == "subscribe" and token == META_WEBHOOK_VERIFY_TOKEN:
        return int(challenge)
    raise HTTPException(status_code=403, detail="Token de verificação inválido.")


@router.post("/webhook/whatsapp")
async def receber_webhook_whatsapp(request: Request):
    """
    [C] Recebe as mensagens que os clientes mandam pro WhatsApp da loja.
    Se a mensagem contiver o código de verificação (#id) e vier do MESMO número
    cadastrado no perfil do cliente, marca whatsapp_verificado = TRUE.

    A trava do número igual é essencial: sem ela, um golpista poderia mandar a
    mensagem usando o WhatsApp real de outra pessoa e continuar com o número
    falso cadastrado no site.
    """
    corpo_bruto = await request.body()

    # Valida a assinatura da Meta (HMAC-SHA256) para garantir que a chamada é legítima.
    if META_APP_SECRET:
        assinatura = request.headers.get("x-hub-signature-256", "")
        assinatura_esperada = (
            "sha256=" + hmac.new(META_APP_SECRET.encode(), corpo_bruto, hashlib.sha256).hexdigest()
        )
        if not hmac.compare_digest(assinatura, assinatura_esperada):
            raise HTTPException(status_code=403, detail="Assinatura inválida.")

    dados = json.loads(corpo_bruto or b"{}")

    try:
        entrada = dados["entry"][0]["changes"][0]["value"]
        mensagens = entrada.get("messages", [])
    except (KeyError, IndexError):
        mensagens = []

    if not mensagens:
        # Outros eventos da Meta (confirmação de entrega, leitura, etc.) não nos interessam.
        return {"status": "ignorado"}

    msg = mensagens[0]
    numero_remetente = msg.get("from", "")
    texto_mensagem = msg.get("text", {}).get("body", "")

    match = re.search(r"#(\d+)", texto_mensagem)
    if not match:
        return {"status": "sem_codigo"}

    utilizador_id = int(match.group(1))

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT nome, telefone, whatsapp_verificado FROM utilizadores WHERE id = %s",
            (utilizador_id,),
        )
        usuario = cursor.fetchone()

        if not usuario or usuario["whatsapp_verificado"]:
            return {"status": "ignorado"}

        if normalizar_telefone(usuario["telefone"]) != normalizar_telefone(numero_remetente):
            # O código bateu mas veio de um número diferente do cadastrado: não verifica.
            return {"status": "numero_nao_confere"}

        cursor.execute(
            "UPDATE utilizadores SET whatsapp_verificado = TRUE WHERE id = %s",
            (utilizador_id,),
        )
        conn.commit()

        enviar_mensagem_whatsapp(
            numero_remetente,
            f"✅ Prontinho, {usuario['nome']}! Seu WhatsApp foi verificado com sucesso na Bora Jogar. Já pode voltar ao site e alugar seus jogos! 🎮",
        )
        return {"status": "verificado"}
    except Exception as e:
        conn.rollback()
        print(f"Erro ao processar webhook do WhatsApp: {e}")
        return {"status": "erro"}
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# AUTOATENDIMENTO DO CLIENTE (DASHBOARD "MEUS ACESSOS")
# ==============================================================================


@router.put("/usuarios/{usuario_id}/telefone")
def atualizar_telefone(usuario_id: int, dados: AtualizarTelefoneRequest):
    """
    [U] Cliente define/corrige o próprio número antes de verificar.
    Depois que whatsapp_verificado vira TRUE, esta rota passa a recusar a troca
    para sempre (trava orgânica, sem depender de contar "tentativas").
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT whatsapp_verificado FROM utilizadores WHERE id = %s", (usuario_id,)
        )
        usuario = cursor.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        if usuario["whatsapp_verificado"]:
            raise HTTPException(
                status_code=400,
                detail="Seu WhatsApp já foi verificado e o número não pode mais ser alterado.",
            )

        cursor.execute(
            "UPDATE utilizadores SET telefone = %s WHERE id = %s",
            (dados.telefone, usuario_id),
        )
        conn.commit()
        return {"mensagem": "Telefone atualizado! Agora envie a mensagem de verificação no WhatsApp."}
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/usuarios/{usuario_id}/status-whatsapp")
def status_whatsapp(usuario_id: int):
    """[R] Rota leve usada pelo React em polling, igual ao status do PIX, para
    detectar em tempo real quando o webhook confirmar o número."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT telefone, whatsapp_verificado FROM utilizadores WHERE id = %s",
            (usuario_id,),
        )
        res = cursor.fetchone()
        if not res:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        return res
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# REDE DE SEGURANÇA MANUAL (ADMIN)
# ==============================================================================


@router.put("/admin/whatsapp/verificar")
def verificar_whatsapp_manual(
    dados: VerificarWhatsAdminRequest, admin_data=Depends(verificar_admin)
):
    """
    [U] Botão 'Confirmar Whats' do Painel Admin. Usado quando o cliente mandou a
    mensagem mas, por algum motivo, o webhook automático não processou.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE utilizadores SET whatsapp_verificado = TRUE WHERE id = %s",
            (dados.utilizador_id,),
        )
        conn.commit()
        return {"mensagem": "WhatsApp confirmado manualmente com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()
