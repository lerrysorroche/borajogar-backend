import json
import os
import urllib.request


def enviar_email(destino: str, assunto: str, corpo_html: str):
    """
    Envia um e-mail transacional via Brevo. Mesmo padrão usado em
    disparar_email_confirmacao e esqueci_senha (routes/usuarios.py), extraído
    aqui pra ser reaproveitado pelos cron jobs de automação (fila liberada,
    lembrete de devolução). Roda de forma silenciosa (fire-and-forget) pra
    nunca travar quem chamou.
    """
    try:
        remetente = os.getenv("EMAIL_REMETENTE")
        chave_api = os.getenv("BREVO_API_KEY")
        if not (chave_api and remetente):
            return
        url = "https://api.brevo.com/v3/smtp/email"
        headers = {
            "accept": "application/json",
            "api-key": chave_api,
            "content-type": "application/json",
        }
        payload = {
            "sender": {"name": "Equipe Bora Jogar", "email": remetente},
            "to": [{"email": destino}],
            "subject": assunto,
            "htmlContent": corpo_html,
        }
        req_http = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req_http) as response:
            pass
    except Exception as e:
        print(f"Aviso: Falha ao enviar e-mail ({assunto!r}): {e}")
