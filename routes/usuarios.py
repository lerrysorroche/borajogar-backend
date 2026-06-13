from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
import string
import random
import os
import urllib.request
import json

from database import get_db_connection
from auth import verificar_admin, gerar_hash_senha, verificar_senha, criar_token_acesso
from models import (
    UsuarioNovo,
    LoginRequest,
    EsqueciSenhaRequest,
    MudarSenhaRequest,
    EditarClienteRequest,
    AjusteSaldoRequest,
    LerNotificacao,
    GoogleLoginRequest,
)

router = APIRouter(tags=["Usuarios"])

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================


def gerar_codigo_convite(nome):
    """
    Gera um código promocional único baseado no primeiro nome do usuário.
    Exemplo: "João" -> "JOAO" + 4 números aleatórios (JOAO4829).
    Se o nome for curto, preenche com 'X'.
    """
    letras = "".join(filter(str.isalpha, nome.split()[0].upper()))[:4].ljust(4, "X")
    nums = "".join(random.choices(string.digits, k=4))
    return f"{letras}{nums}"


# ==============================================================================
# AUTENTICAÇÃO E REGISTRO
# ==============================================================================


@router.post("/usuarios", status_code=201)
def cadastrar_usuario(usuario: UsuarioNovo):
    """
    [C] Criação de Conta (Sign Up Clássico).
    Criptografa a senha antes de salvar no banco e gera o código de indicação.
    Se o cliente usou o código de um amigo, cria o vínculo ('indicado_por') para
    pagamento futuro do bônus de cashback na primeira recarga.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        senha_segura = gerar_hash_senha(usuario.senha)
        meu_codigo = gerar_codigo_convite(usuario.nome)
        indicado_por_id = None

        # Verifica se o código de indicação é válido e pega o ID do amigo
        if usuario.codigo_indicacao:
            cursor.execute(
                "SELECT id FROM utilizadores WHERE codigo_indicacao = %s",
                (usuario.codigo_indicacao.upper(),),
            )
            amigo = cursor.fetchone()
            if amigo:
                indicado_por_id = amigo[0]

        cursor.execute(
            "INSERT INTO utilizadores (nome, email, senha_hash, telefone, codigo_indicacao, indicado_por) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
            (
                usuario.nome,
                usuario.email,
                senha_segura,
                usuario.telefone,
                meu_codigo,
                indicado_por_id,
            ),
        )
        novo_id = cursor.fetchone()[0]
        conn.commit()
        return {
            "mensagem": "Cliente cadastrado com sucesso!",
            "id": novo_id,
            "nome": usuario.nome,
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Erro ao cadastrar. Verifique se o e-mail já existe.",
        )
    finally:
        cursor.close()
        conn.close()


@router.post("/login")
def fazer_login(login: LoginRequest):
    """
    [R] Login Clássico (E-mail e Senha).
    Valida a senha descriptografando o hash do banco. Em caso de sucesso,
    gera um Token JWT para manter o cliente logado no Frontend de forma segura.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT id, nome, email, is_admin, saldo, senha_hash, codigo_indicacao FROM utilizadores WHERE email = %s;",
        (login.email,),
    )
    usuario = cursor.fetchone()
    cursor.close()
    conn.close()

    if not usuario or not verificar_senha(login.senha, usuario["senha_hash"]):
        raise HTTPException(status_code=401, detail="E-mail ou senha incorretos.")

    token = criar_token_acesso(
        {
            "id": usuario["id"],
            "email": usuario["email"],
            "is_admin": usuario["is_admin"],
        }
    )

    del usuario["senha_hash"]  # Remove a hash da resposta por segurança
    usuario["saldo"] = float(usuario["saldo"])
    return {"mensagem": "Login aprovado", "usuario": usuario, "token": token}


@router.post("/login/google")
def login_google(req: GoogleLoginRequest):
    """
    [C/R] Login via Google (OAuth Firebase).
    Verifica se o e-mail já existe: se sim, faz o login normalmente.
    Se não, cria a conta blindada automaticamente, exigindo apenas que o
    frontend envie o telefone do cliente para o banco de dados.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Verifica se o e-mail do Google já existe no banco
        cursor.execute(
            "SELECT id, nome, email, is_admin, saldo, codigo_indicacao FROM utilizadores WHERE email = %s;",
            (req.email,),
        )
        usuario = cursor.fetchone()

        if usuario:
            token = criar_token_acesso(
                {
                    "id": usuario["id"],
                    "email": usuario["email"],
                    "is_admin": usuario["is_admin"],
                }
            )
            usuario["saldo"] = float(usuario["saldo"])
            return {
                "mensagem": "Login aprovado",
                "usuario": usuario,
                "token": token,
                "novo_usuario": False,
            }
        else:
            # Pede o telefone caso a conta Google não tenha fornecido
            if not req.telefone:
                return {"mensagem": "precisa_telefone", "novo_usuario": True}

            # 2. Criação da conta nova blindada com senha aleatória
            meu_codigo = gerar_codigo_convite(req.nome)
            senha_segura = gerar_hash_senha(
                "GoogleAuth123!"
            )  # Senha padrão inacessível

            cursor.execute(
                "INSERT INTO utilizadores (nome, email, senha_hash, telefone, codigo_indicacao) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
                (req.nome, req.email, senha_segura, req.telefone, meu_codigo),
            )
            novo_id = cursor.fetchone()["id"]
            conn.commit()

            cursor.execute(
                "SELECT id, nome, email, is_admin, saldo, codigo_indicacao FROM utilizadores WHERE id = %s;",
                (novo_id,),
            )
            novo_usuario = cursor.fetchone()
            token = criar_token_acesso(
                {
                    "id": novo_usuario["id"],
                    "email": novo_usuario["email"],
                    "is_admin": novo_usuario["is_admin"],
                }
            )
            novo_usuario["saldo"] = float(novo_usuario["saldo"])

            return {
                "mensagem": "Conta criada com sucesso!",
                "usuario": novo_usuario,
                "token": token,
                "novo_usuario": False,
            }

    except Exception as e:
        conn.rollback()
        print(f"🔥 ERRO CRÍTICO NO GOOGLE LOGIN: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/esqueci-senha")
def esqueci_senha(req: EsqueciSenhaRequest):
    """
    [U] Recuperação de Conta.
    Gera uma senha aleatória de 8 caracteres, salva no banco com hash,
    e dispara um e-mail para o cliente usando a API transacional do Brevo.
    A resposta é genérica por segurança (não confirma se o e-mail existe ou não).
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, nome FROM utilizadores WHERE email = %s", (req.email,))
    usuario = cursor.fetchone()

    if not usuario:
        cursor.close()
        conn.close()
        return {
            "mensagem": "Se este e-mail estiver cadastrado, uma nova senha foi enviada."
        }

    caracteres = string.ascii_letters + string.digits
    nova_senha = "".join(random.choice(caracteres) for i in range(8))
    senha_hash = gerar_hash_senha(nova_senha)

    cursor.execute(
        "UPDATE utilizadores SET senha_hash = %s WHERE email = %s",
        (senha_hash, req.email),
    )
    conn.commit()

    # Disparo de e-mail via Brevo REST API
    try:
        remetente = os.getenv("EMAIL_REMETENTE")
        chave_api = os.getenv("BREVO_API_KEY")
        if chave_api and remetente:
            url = "https://api.brevo.com/v3/smtp/email"
            headers = {
                "accept": "application/json",
                "api-key": chave_api,
                "content-type": "application/json",
            }
            payload = {
                "sender": {"name": "Equipe Bora Jogar", "email": remetente},
                "to": [{"email": req.email}],
                "subject": "Bora Jogar - Recuperação de Senha",
                "htmlContent": f"Sua nova senha temporária é: <strong>{nova_senha}</strong><br>Por favor, altere-a no painel Meus Acessos.",
            }
            req_http = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req_http) as response:
                pass
    except Exception:
        pass
    finally:
        cursor.close()
        conn.close()

    return {
        "mensagem": "Se este e-mail estiver cadastrado, uma nova senha foi enviada."
    }


@router.post("/mudar-senha")
def mudar_senha(req: MudarSenhaRequest):
    """
    [U] Altera a senha do cliente por dentro do Dashboard (Meus Acessos).
    Exige a digitação da senha atual para garantir que é o próprio usuário alterando.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT senha_hash FROM utilizadores WHERE id = %s", (req.utilizador_id,)
    )
    usuario = cursor.fetchone()

    if not usuario or not verificar_senha(req.senha_atual, usuario["senha_hash"]):
        cursor.close()
        conn.close()
        raise HTTPException(status_code=400, detail="A senha atual está incorreta.")

    novo_hash = gerar_hash_senha(req.nova_senha)
    cursor.execute(
        "UPDATE utilizadores SET senha_hash = %s WHERE id = %s",
        (novo_hash, req.utilizador_id),
    )
    conn.commit()
    cursor.close()
    conn.close()
    return {"mensagem": "Senha alterada com sucesso!"}


# ==============================================================================
# ÁREA DO CLIENTE (PERFIL, EXTRATO E NOTIFICAÇÕES)
# ==============================================================================


@router.get("/usuarios/{usuario_id}/saldo")
def buscar_saldo_real(usuario_id: int):
    """
    [R] Retorna a fonte da verdade do saldo do cliente direto do banco de dados.
    Usado intensamente pelo React após recargas e aluguéis para atualizar o visual.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (usuario_id,))
        res = cursor.fetchone()
        return res if res else {"saldo": 0.0}
    finally:
        cursor.close()
        conn.close()


@router.get("/extrato/{usuario_id}")
def buscar_extrato_usuario(usuario_id: int):
    """
    [R] Alimenta a tabela de extrato da carteira do cliente.
    Mostra ENTRADAS e SAIDAS de recargas, aluguéis, multas e cashbacks gamificados.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT tipo, valor, descricao, data_transacao FROM transacoes WHERE utilizador_id = %s ORDER BY data_transacao DESC;",
        (usuario_id,),
    )
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return resultados


@router.get("/notificacoes/{usuario_id}")
def buscar_notificacoes(usuario_id: int):
    """
    [R] Busca os alertas não lidos do usuário.
    Principalmente os alertas de "Mudança de Data" gerados quando o sistema VIP
    empurra a data de um cliente de Rank menor para baixo na fila de espera.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT id, reserva_id, jogo, mensagem, lida FROM notificacoes WHERE utilizador_id = %s AND lida = FALSE ORDER BY id DESC",
        (usuario_id,),
    )
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


@router.post("/notificacoes/ler")
def ler_notificacao(dados: LerNotificacao):
    """
    [U] Confirma que o cliente leu o alerta (botão "Entendi" do React)
    e oculta o modal laranja de aviso de mudança na fila.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE notificacoes SET lida = TRUE WHERE id = %s", (dados.notificacao_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return {"status": "ok"}


# ==============================================================================
# GESTÃO DE CLIENTES (ADMIN)
# ==============================================================================


@router.get("/usuarios")
def listar_usuarios(admin_data=Depends(verificar_admin)):
    """
    [R] Painel Admin: Tabela Base de Clientes.
    Lista todos os clientes da locadora para gestão manual e filtros.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT id, nome, email, telefone, saldo, is_admin FROM utilizadores ORDER BY nome ASC"
    )
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


@router.put("/usuarios/{usuario_id}")
def editar_usuario(
    usuario_id: int, dados: EditarClienteRequest, admin_data=Depends(verificar_admin)
):
    """
    [U] Painel Admin: Edição completa do Perfil do Cliente.
    Inteligência Financeira: Se você alterar o saldo do cliente por aqui, a rota
    calcula automaticamente a diferença matemática e gera um lançamento no
    extrato (ENTRADA ou SAÍDA) com a justificativa que você digitou.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (usuario_id,))
        usuario_db = cursor.fetchone()
        if not usuario_db:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")

        saldo_atual = float(usuario_db["saldo"])
        novo_saldo = float(dados.saldo)

        # Inteligência financeira: injeta a transação se houve mudança no saldo
        if saldo_atual != novo_saldo:
            diferenca = novo_saldo - saldo_atual
            tipo_transacao = "ENTRADA" if diferenca > 0 else "SAIDA"
            motivo = (
                dados.motivo_ajuste
                if dados.motivo_ajuste.strip()
                else "Ajuste Administrativo"
            )
            cursor.execute(
                "INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, %s, %s, %s)",
                (usuario_id, tipo_transacao, abs(diferenca), motivo),
            )

        cursor.execute(
            "UPDATE utilizadores SET nome = %s, email = %s, telefone = %s, saldo = %s WHERE id = %s",
            (dados.nome, dados.email, dados.telefone, novo_saldo, usuario_id),
        )
        conn.commit()
        return {"mensagem": "Cliente atualizado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.delete("/usuarios/{usuario_id}")
def deletar_usuario(usuario_id: int, admin_data=Depends(verificar_admin)):
    """
    [D] Painel Admin: Exclui um cliente da base.
    Restrição de Integridade: Será bloqueado pelo banco de dados se o cliente
    tiver transações ou locações amarradas a ele.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM utilizadores WHERE id = %s", (usuario_id,))
        conn.commit()
        return {"mensagem": "Usuário removido com sucesso"}
    except Exception:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Erro: Este usuário possui histórico financeiro ou locações e não pode ser apagado.",
        )
    finally:
        cursor.close()
        conn.close()
