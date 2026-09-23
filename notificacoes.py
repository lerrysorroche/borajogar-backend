import json
import os
import urllib.request

from routes.whatsapp import enviar_template_whatsapp, normalizar_telefone

FRONTEND_URL = os.getenv("FRONTEND_URL", "https://www.locadoraborajogar.com.br").rstrip(
    "/"
)


def enviar_email(
    destino: str,
    assunto: str,
    titulo: str,
    corpo_html: str,
    cta_label: str = None,
    cta_url: str = None,
):
    """
    Envia um e-mail transacional via Brevo, sempre dentro do mesmo template
    visual da marca (cabeçalho, cores, rodapé). Ponto único de envio de
    e-mail do sistema — toda situação nova chama esta função em vez de montar
    HTML próprio, para nunca mais existir um e-mail com acabamento diferente
    dos outros (ou, no caso da recuperação de senha antiga, nenhum acabamento).
    Roda de forma silenciosa (fire-and-forget) pra nunca travar quem chamou.
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
        cta_html = ""
        if cta_label and cta_url:
            cta_html = f"""
            <div style="text-align:center; margin: 26px 0 6px;">
                <a href="{cta_url}" style="display:inline-block; background-color:#10b981; color:#052e1f; font-weight:bold; text-decoration:none; padding:12px 30px; border-radius:8px; font-size:14px;">{cta_label}</a>
            </div>
            """
        html_completo = f"""
        <div style="font-family: -apple-system, Arial, sans-serif; max-width: 480px; margin: 0 auto; background-color: #09090b; border: 1px solid #27272a; border-radius: 16px; overflow: hidden;">
            <div style="padding: 26px 28px 6px;">
                <p style="margin: 0 0 14px; font-size: 12px; font-weight: bold; letter-spacing: 1.5px; color: #71717a; text-transform: uppercase;">🎮 Bora Jogar</p>
                <h2 style="margin: 0 0 16px; font-size: 21px; color: #3b82f6;">{titulo}</h2>
                <div style="font-size: 14px; line-height: 1.65; color: #d4d4d8;">
                    {corpo_html}
                </div>
                {cta_html}
            </div>
            <div style="margin-top: 10px; padding: 16px 28px; background-color: #18181b; border-top: 1px solid #27272a;">
                <p style="margin: 0; font-size: 11px; color: #52525b;">Mensagem automática — não é necessário responder este e-mail.<br>Equipe Bora Jogar</p>
            </div>
        </div>
        """
        payload = {
            "sender": {"name": "Equipe Bora Jogar", "email": remetente},
            "to": [{"email": destino}],
            "subject": assunto,
            "htmlContent": html_completo,
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


def notificar_jogo_liberado(cursor, destinatario: dict, jogo_titulo: str, tipo_slot: str):
    """
    Ponto único de notificação para "vaga liberada / jogo pronto": grava o
    sino, dispara e-mail e WhatsApp. Antes essa lógica (e o texto de cada
    canal) estava escrita duas vezes quase igual — uma no cron de filas, outra
    no reset de senha do admin, que na prática é quem entrega a vaga com mais
    frequência — e as duas puderam divergir com o tempo. Agora só existe aqui.

    'destinatario' precisa ter utilizador_id, nome, email e telefone (o mesmo
    formato que a query de "próximo da fila" já devolve nos dois chamadores).
    'reserva_id' é opcional: o cron não tem um id de reserva pronto (a fila já
    foi marcada CONCLUIDO antes de chegar aqui), então a coluna fica NULL
    nesse caminho.
    """
    titulo_sino = "🎉 Jogo Liberado!"
    mensagem_sino = (
        f"A sua vaga ({tipo_slot}) do jogo {jogo_titulo} já está na aba "
        "'Meus Acessos'. Bom jogo!"
    )
    cursor.execute(
        "INSERT INTO notificacoes (utilizador_id, jogo, mensagem, titulo, tipo) "
        "VALUES (%s, %s, %s, %s, 'FILA')",
        (destinatario["utilizador_id"], jogo_titulo, mensagem_sino, titulo_sino),
    )

    if destinatario.get("email"):
        enviar_email(
            destinatario["email"],
            "🎮 Seu jogo já está liberado na Bora Jogar!",
            f"Sua vez chegou, {destinatario['nome']}!",
            f"<p>O jogo <strong>{jogo_titulo}</strong> ({tipo_slot}) já está liberado "
            "na sua conta. Acesse a aba 'Meus Acessos' no site pra pegar os dados "
            "de acesso.</p>",
            cta_label="Ver Meus Acessos",
            cta_url=FRONTEND_URL,
        )
    if destinatario.get("telefone"):
        enviar_template_whatsapp(
            normalizar_telefone(destinatario["telefone"]),
            "jogo_pronto",
            [destinatario["nome"], jogo_titulo],
        )
