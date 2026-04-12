from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import pyotp
import jwt 
import os
from passlib.context import CryptContext 
from datetime import datetime, timedelta
import random
import string
import requests
import urllib.request
import json

app = FastAPI(title="API Locadora PS5")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = "chave-super-secreta-bora-jogar"
ALGORITHM = "HS256"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ==============================================================================
# INTEGRAÇÃO ASAAS (PRODUÇÃO - DINHEIRO REAL)
# ==============================================================================
ASAAS_API_KEY = os.getenv("ASAAS_API_KEY")
ASAAS_URL = "https://api.asaas.com/v3"
HEADERS_ASAAS = {
    "access_token": ASAAS_API_KEY,
    "Content-Type": "application/json"
}

def gerar_hash_senha(senha): return pwd_context.hash(senha)
def verificar_senha(senha_pura, senha_criptografada): return pwd_context.verify(senha_pura, senha_criptografada)

def criar_token_acesso(dados: dict):
    dados_para_codificar = dados.copy()
    dados_para_codificar.update({"exp": datetime.utcnow() + timedelta(days=7)})
    return jwt.encode(dados_para_codificar, SECRET_KEY, algorithm=ALGORITHM)

def verificar_admin(authorization: str = Header(None)):
    if not authorization: raise HTTPException(status_code=401, detail="Crachá digital não enviado.")
    try:
        token = authorization.replace("Bearer ", "")
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if not payload.get("is_admin"): raise HTTPException(status_code=403, detail="Acesso Negado.")
        return payload
    except Exception: raise HTTPException(status_code=401, detail="Token inválido.")

def get_db_connection():
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

def gerar_codigo_convite(nome):
    letras = "".join(filter(str.isalpha, nome.split()[0].upper()))[:4].ljust(4, 'X')
    nums = "".join(random.choices(string.digits, k=4))
    return f"{letras}{nums}"

class UsuarioNovo(BaseModel): nome: str; email: str; senha: str; telefone: str; codigo_indicacao: str = ""
class JogoNovo(BaseModel): titulo: str; plataforma: str; preco_aluguel: float; preco_aluguel_14: float = 0.0; descricao: str; url_imagem: str = ""; tempo_jogo: str = ""; nota: float = 0.0; data_lancamento: str = None
class ContaPSNNova(BaseModel): jogo_id: int; email_login: str; senha_login: str; mfa_secret: str = "" 
class NovaLocacao(BaseModel): utilizador_id: int; jogo_id: int; dias_aluguel: int
class LoginRequest(BaseModel): email: str; senha: str
class EsqueciSenhaRequest(BaseModel): email: str
class MudarSenhaRequest(BaseModel): utilizador_id: int; senha_atual: str; nova_senha: str
class NovaReserva(BaseModel): utilizador_id: int; jogo_id: int; dias_aluguel: int = 7
class NovaRecarga(BaseModel): utilizador_id: int; valor: float; cupom: str = ""; cpf: str
class NovoCupom(BaseModel): codigo: str; tipo: str; valor: float
class ResetSenhaRequest(BaseModel): conta_psn_id: int; nova_senha: str
class AplicarMultaRequest(BaseModel): utilizador_id: int; valor: float = 50.0
class AjusteSaldoRequest(BaseModel): utilizador_id: int; valor: float; motivo: str
class ConfigRequest(BaseModel): devolucao_dinamica: bool; valor_por_dia: float; anuncio_ativo: bool; mensagem_anuncio: str; banners_url: str = ""
class DevolucaoRequest(BaseModel): locacao_id: int; utilizador_id: int
class EditarPrecoJogoRequest(BaseModel): preco_aluguel: float; preco_aluguel_14: float = 0.0
class EditarJogoRequest(BaseModel): titulo: str; plataforma: str; preco_aluguel: float; preco_aluguel_14: float = 0.0; descricao: str; url_imagem: str = "";tempo_jogo: str = ""; nota: float = 0.0; data_lancamento: str = None
class NovaOpcaoEnquete(BaseModel): titulo: str; url_imagem: str
class VotoEnquete(BaseModel): utilizador_id: int; opcao_id: int
class EditarClienteRequest(BaseModel): nome: str; email: str; telefone: str; saldo: float; motivo_ajuste: str = "Ajuste Administrativo"
class LerNotificacao(BaseModel): notificacao_id: int
class CancelarReserva(BaseModel): reserva_id: int; utilizador_id: int; notificacao_id: int = 0

@app.get("/")
def home(): return {"mensagem": "API Online"}

@app.get("/configuracoes")
def get_config():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT devolucao_dinamica, valor_por_dia, anuncio_ativo, mensagem_anuncio, banners_url FROM configuracoes LIMIT 1")
    config = cursor.fetchone()
    cursor.close(); conn.close()
    return config if config else {"devolucao_dinamica": False, "valor_por_dia": 2.0, "anuncio_ativo": False, "mensagem_anuncio": "", "banners_url": ""}

@app.post("/admin/configuracoes")
def set_config(dados: ConfigRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM configuracoes LIMIT 1")
    if cursor.fetchone():
        cursor.execute("""
            UPDATE configuracoes 
            SET devolucao_dinamica = %s, valor_por_dia = %s, anuncio_ativo = %s, mensagem_anuncio = %s, banners_url = %s
        """, (dados.devolucao_dinamica, dados.valor_por_dia, dados.anuncio_ativo, dados.mensagem_anuncio, dados.banners_url))
    else:
        cursor.execute("""
            INSERT INTO configuracoes (devolucao_dinamica, valor_por_dia, anuncio_ativo, mensagem_anuncio, banners_url)
            VALUES (%s, %s, %s, %s, %s)
        """, (dados.devolucao_dinamica, dados.valor_por_dia, dados.anuncio_ativo, dados.mensagem_anuncio, dados.banners_url))
        
    conn.commit(); cursor.close(); conn.close()
    return {"mensagem": "Configurações salvas!"}

@app.get("/enquete")
def buscar_enquete(usuario_id: int = 0):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT o.id, o.titulo, o.url_imagem,
               (SELECT COUNT(*) FROM enquete_votos v WHERE v.opcao_id = o.id) as total_votos
        FROM enquete_opcoes o
        ORDER BY o.id ASC
    """)
    opcoes = cursor.fetchall()
    
    voto_usuario = None
    if usuario_id > 0:
        cursor.execute("SELECT opcao_id FROM enquete_votos WHERE utilizador_id = %s", (usuario_id,))
        voto = cursor.fetchone()
        if voto:
            voto_usuario = voto['opcao_id']
            
    cursor.close(); conn.close()
    return {"opcoes": opcoes, "voto_usuario": voto_usuario}

@app.post("/enquete/votar")
def votar_enquete(voto: VotoEnquete):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO enquete_votos (utilizador_id, opcao_id)
            VALUES (%s, %s)
            ON CONFLICT (utilizador_id) 
            DO UPDATE SET opcao_id = EXCLUDED.opcao_id;
        """, (voto.utilizador_id, voto.opcao_id))
        conn.commit()
        return {"mensagem": "Voto registrado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/admin/enquete", status_code=201)
def adicionar_opcao_enquete(opcao: NovaOpcaoEnquete, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO enquete_opcoes (titulo, url_imagem) VALUES (%s, %s)", (opcao.titulo, opcao.url_imagem))
        conn.commit()
        return {"mensagem": "Opção adicionada à enquete!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.delete("/admin/enquete/{opcao_id}")
def remover_opcao_enquete(opcao_id: int, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM enquete_opcoes WHERE id = %s", (opcao_id,))
    conn.commit()
    cursor.close(); conn.close()
    return {"mensagem": "Opção removida da enquete."}

@app.delete("/admin/enquete")
def limpar_enquete_completa(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM enquete_opcoes")
    conn.commit()
    cursor.close(); conn.close()
    return {"mensagem": "Enquete reiniciada."}

@app.post("/jogos", status_code=201)
def cadastrar_jogo(jogo: JogoNovo, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """INSERT INTO jogos (titulo, plataforma, preco_aluguel, preco_aluguel_14, descricao, url_imagem, tempo_jogo, nota, data_lancamento) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;"""
        cursor.execute(query, (jogo.titulo, jogo.plataforma, jogo.preco_aluguel, jogo.preco_aluguel_14, jogo.descricao, jogo.url_imagem, jogo.tempo_jogo, jogo.nota, jogo.data_lancamento))
        conn.commit()
        return {"mensagem": "Jogo adicionado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao cadastrar o jogo: {str(e)}")
    finally:
        cursor.close(); conn.close()

@app.get("/jogos")
def listar_jogos():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 🚀 TENTA A CONSULTA NOVA (Com dias da fila e popularidade)
        query = """SELECT j.id, j.titulo, j.plataforma, j.preco_aluguel, j.preco_aluguel_14, j.descricao, j.url_imagem, j.tempo_jogo, j.nota, CAST(j.data_lancamento AS VARCHAR) as data_lancamento,
                (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status ILIKE 'DISPONIVEL') AS estoque,
                (SELECT COUNT(*) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS tamanho_fila,
                (SELECT COALESCE(SUM(dias_aluguel), 0) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS fila_dias_espera,
                (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id AND l.status = 'ATIVA') AS proxima_devolucao,
                (SELECT COUNT(*) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id) AS popularidade
            FROM jogos j ORDER BY j.titulo ASC;"""
        cursor.execute(query)
        resultados = cursor.fetchall()
    except Exception as e:
        conn.rollback()
        # 🛡️ PARAQUEDAS DE EMERGÊNCIA: Se a tabela não tiver a coluna nova, roda a consulta segura antiga!
        print(f"Erro na query principal (rodando fallback): {e}")
        query_segura = """SELECT j.id, j.titulo, j.plataforma, j.preco_aluguel, j.preco_aluguel_14, j.descricao, j.url_imagem, j.tempo_jogo, j.nota, CAST(j.data_lancamento AS VARCHAR) as data_lancamento,
                (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status ILIKE 'DISPONIVEL') AS estoque,
                (SELECT COUNT(*) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS tamanho_fila,
                0 AS fila_dias_espera,
                (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id AND l.status = 'ATIVA') AS proxima_devolucao,
                (SELECT COUNT(*) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id) AS popularidade
            FROM jogos j ORDER BY j.titulo ASC;"""
        cursor.execute(query_segura)
        resultados = cursor.fetchall()
    finally:
        cursor.close(); conn.close()
    
    return resultados

@app.get("/meus-alugueis/{usuario_id}")
def buscar_alugueis_usuario(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT l.id AS locacao_id, j.titulo AS jogo, c.email_login, c.senha_login, l.data_fim, l.status FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id JOIN jogos j ON c.jogo_id = j.id WHERE l.utilizador_id = %s ORDER BY l.data_fim DESC;", (usuario_id,))
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

@app.get("/minhas-reservas/{usuario_id}")
def buscar_reservas_usuario(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT f.id AS reserva_id, j.id AS jogo_id, j.titulo AS jogo, j.data_lancamento, f.data_solicitacao, f.status,
        (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA') AS proxima_devolucao
        FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id 
        WHERE f.utilizador_id = %s AND f.status = 'AGUARDANDO' ORDER BY f.data_solicitacao ASC;
    """, (usuario_id,))
    reservas = cursor.fetchall()

    # 🚀 CALCULA A DATA EXATA BASEADO EM QUEM ESTÁ NA FRENTE
    for r in reservas:
        cursor.execute("""
            SELECT COALESCE(SUM(dias_aluguel), 0) as dias_frente FROM fila_espera 
            WHERE jogo_id = %s AND status = 'AGUARDANDO' AND (
                (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') > 
                (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA')
                OR ((SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') = 
                 (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA') AND data_solicitacao < %s)
            )
        """, (r['jogo_id'], usuario_id, usuario_id, r['data_solicitacao']))
        dias_frente = cursor.fetchone()['dias_frente']

        base_date = datetime.now()
        if r['data_lancamento']:
            dl = datetime.strptime(str(r['data_lancamento']), "%Y-%m-%d")
            if dl > base_date: base_date = dl
        if r['proxima_devolucao'] and r['proxima_devolucao'] > base_date: base_date = r['proxima_devolucao']
            
        est_date = base_date + timedelta(days=dias_frente)
        r['data_estimada_str'] = est_date.strftime("%d/%m/%Y")

    cursor.close(); conn.close()
    return reservas

@app.get("/notificacoes/{usuario_id}")
def buscar_notificacoes(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, reserva_id, jogo, mensagem, lida FROM notificacoes WHERE utilizador_id = %s AND lida = FALSE ORDER BY id DESC", (usuario_id,))
    res = cursor.fetchall()
    cursor.close(); conn.close()
    return res

@app.post("/notificacoes/ler")
def ler_notificacao(dados: LerNotificacao):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE notificacoes SET lida = TRUE WHERE id = %s", (dados.notificacao_id,))
    conn.commit()
    cursor.close(); conn.close()
    return {"status": "ok"}

@app.get("/extrato/{usuario_id}")
def buscar_extrato_usuario(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT tipo, valor, descricao, data_transacao FROM transacoes WHERE utilizador_id = %s ORDER BY data_transacao DESC;", (usuario_id,))
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

@app.get("/gerar-2fa/{locacao_id}/{usuario_id}")
def gerar_codigo_2fa(locacao_id: int, usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT c.mfa_secret FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE l.id = %s AND l.utilizador_id = %s AND l.status = 'ATIVA'", (locacao_id, usuario_id))
    resultado = cursor.fetchone()
    cursor.close(); conn.close()
    if not resultado or not resultado['mfa_secret']: raise HTTPException(status_code=404, detail="A autenticação não está configurada para esta conta ou a locação expirou.")
    totp = pyotp.TOTP(resultado['mfa_secret'])
    return {"codigo": totp.now()}

@app.post("/usuarios", status_code=201)
def cadastrar_usuario(usuario: UsuarioNovo):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        senha_segura = gerar_hash_senha(usuario.senha)
        meu_codigo = gerar_codigo_convite(usuario.nome)
        indicado_por_id = None
        if usuario.codigo_indicacao:
            cursor.execute("SELECT id FROM utilizadores WHERE codigo_indicacao = %s", (usuario.codigo_indicacao.upper(),))
            amigo = cursor.fetchone()
            if amigo: indicado_por_id = amigo[0]

        cursor.execute("INSERT INTO utilizadores (nome, email, senha_hash, telefone, codigo_indicacao, indicado_por) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;", (usuario.nome, usuario.email, senha_segura, usuario.telefone, meu_codigo, indicado_por_id))
        novo_id = cursor.fetchone()[0] 
        conn.commit() 
        return {"mensagem": "Cliente cadastrado com sucesso!", "id": novo_id, "nome": usuario.nome}
    except Exception as e:
        conn.rollback() 
        raise HTTPException(status_code=400, detail="Erro ao cadastrar. E-mail já existe.")
    finally:
        cursor.close(); conn.close()

@app.put("/usuarios/{usuario_id}")
def editar_usuario(usuario_id: int, dados: EditarClienteRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (usuario_id,))
        usuario_db = cursor.fetchone()
        
        if not usuario_db:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")
            
        saldo_atual = float(usuario_db['saldo'])
        novo_saldo = float(dados.saldo)
        
        if saldo_atual != novo_saldo:
            diferenca = novo_saldo - saldo_atual
            tipo_transacao = "ENTRADA" if diferenca > 0 else "SAIDA"
            motivo = dados.motivo_ajuste if dados.motivo_ajuste.strip() else "Ajuste Administrativo"
            cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, %s, %s, %s)", (usuario_id, tipo_transacao, abs(diferenca), motivo))

        cursor.execute("UPDATE utilizadores SET nome = %s, email = %s, telefone = %s, saldo = %s WHERE id = %s", (dados.nome, dados.email, dados.telefone, novo_saldo, usuario_id))
        conn.commit()
        return {"mensagem": "Cliente atualizado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/login")
def fazer_login(login: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, nome, email, is_admin, saldo, senha_hash, codigo_indicacao FROM utilizadores WHERE email = %s;", (login.email,))
    usuario = cursor.fetchone() 
    cursor.close(); conn.close()
    if not usuario or not verificar_senha(login.senha, usuario['senha_hash']): raise HTTPException(status_code=401, detail="E-mail ou senha incorretos.")
    token = criar_token_acesso({"id": usuario['id'], "email": usuario['email'], "is_admin": usuario['is_admin']})
    del usuario['senha_hash'] 
    usuario['saldo'] = float(usuario['saldo'])
    return {"mensagem": "Login aprovado", "usuario": usuario, "token": token}

@app.post("/esqueci-senha")
def esqueci_senha(req: EsqueciSenhaRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("SELECT id, nome FROM utilizadores WHERE email = %s", (req.email,))
    usuario = cursor.fetchone()
    if not usuario:
        cursor.close(); conn.close()
        return {"mensagem": "Se este e-mail estiver cadastrado, uma nova senha foi enviada."}
    
    caracteres = string.ascii_letters + string.digits
    nova_senha = ''.join(random.choice(caracteres) for i in range(8))
    senha_hash = gerar_hash_senha(nova_senha)
    
    cursor.execute("UPDATE utilizadores SET senha_hash = %s WHERE email = %s", (senha_hash, req.email))
    conn.commit()
    
    try:
        remetente = os.getenv("EMAIL_REMETENTE")
        chave_api = os.getenv("BREVO_API_KEY")
        if chave_api and remetente:
            url = "https://api.brevo.com/v3/smtp/email"
            headers = {"accept": "application/json", "api-key": chave_api, "content-type": "application/json"}
            payload = {
                "sender": {"name": "Equipe Bora Jogar", "email": remetente},
                "to": [{"email": req.email}],
                "subject": "Bora Jogar - Recuperação de Senha",
                "htmlContent": f"Sua nova senha é: {nova_senha}"
            }
            req_http = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), headers=headers, method='POST')
            with urllib.request.urlopen(req_http) as response: pass
    except Exception: pass
    finally:
        cursor.close(); conn.close()
        
    return {"mensagem": "Se este e-mail estiver cadastrado, uma nova senha foi enviada."}

@app.post("/mudar-senha")
def mudar_senha(req: MudarSenhaRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT senha_hash FROM utilizadores WHERE id = %s", (req.utilizador_id,))
    usuario = cursor.fetchone()
    if not usuario or not verificar_senha(req.senha_atual, usuario['senha_hash']):
        cursor.close(); conn.close()
        raise HTTPException(status_code=400, detail="A senha atual está incorreta.")
        
    novo_hash = gerar_hash_senha(req.nova_senha)
    cursor.execute("UPDATE utilizadores SET senha_hash = %s WHERE id = %s", (novo_hash, req.utilizador_id))
    conn.commit()
    cursor.close(); conn.close()
    return {"mensagem": "Senha alterada com sucesso!"}

@app.post("/recarga/gerar-pix")
def gerar_pix_asaas(recarga: NovaRecarga):
    if recarga.valor < 15.0: raise HTTPException(status_code=400, detail="O valor mínimo de recarga é R$ 15,00.")
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        valor_bonus_cupom = 0.0
        cupom_id = None
        
        if recarga.cupom:
            cursor.execute("SELECT id, tipo, valor FROM cupons WHERE codigo = %s AND ativo = TRUE", (recarga.cupom.upper(),))
            cupom = cursor.fetchone()
            if not cupom: raise HTTPException(status_code=404, detail="Cupom inválido ou expirado.")
            
            cursor.execute("SELECT id FROM cupons_usados WHERE utilizador_id = %s AND cupom_id = %s", (recarga.utilizador_id, cupom['id']))
            if cursor.fetchone(): raise HTTPException(status_code=400, detail="Você já utilizou este cupom promocional.")
            
            cupom_id = cupom['id']
            if cupom['tipo'] == 'FIXO': valor_bonus_cupom = cupom['valor']
            elif cupom['tipo'] == 'PORCENTAGEM': valor_bonus_cupom = recarga.valor * (cupom['valor'] / 100.0)

        cursor.execute("SELECT nome, email FROM utilizadores WHERE id = %s", (recarga.utilizador_id,))
        usr = cursor.fetchone()

        payload_cli = {
            "name": usr['nome'], 
            "email": usr['email'],
            "cpfCnpj": recarga.cpf 
        }
        res_cli = requests.post(f"{ASAAS_URL}/customers", json=payload_cli, headers=HEADERS_ASAAS)
        if res_cli.status_code not in [200, 201]: raise Exception(f"Erro Asaas (Cliente): {res_cli.text}")
        cli_id = res_cli.json().get('id')

        vencimento = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        payload_cob = {
            "customer": cli_id, 
            "billingType": "PIX", 
            "value": recarga.valor, 
            "dueDate": vencimento, 
            "description": "Recarga de Carteira - BORA JOGAR"
        }
        res_cob = requests.post(f"{ASAAS_URL}/payments", json=payload_cob, headers=HEADERS_ASAAS)
        if res_cob.status_code not in [200, 201]: raise Exception(f"Erro Asaas (Cobrança): {res_cob.text}")
        pay_id = res_cob.json().get('id')

        res_qr = requests.get(f"{ASAAS_URL}/payments/{pay_id}/pixQrCode", headers=HEADERS_ASAAS)
        if res_qr.status_code not in [200, 201]: raise Exception(f"Erro Asaas (QRCode): {res_qr.text}")
        qr_data = res_qr.json()

        cupom_nome = recarga.cupom.upper() if recarga.cupom else ""
        cursor.execute("INSERT INTO pedidos_pix (id, utilizador_id, valor_pago, valor_bonus, cupom) VALUES (%s, %s, %s, %s, %s)",
                       (pay_id, recarga.utilizador_id, recarga.valor, valor_bonus_cupom, cupom_nome))
        conn.commit()

        return {"payment_id": pay_id, "copia_cola": qr_data.get('payload'), "qr_code": qr_data.get('encodedImage')}

    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.get("/recarga/status/{payment_id}")
def checar_status_pagamento(payment_id: str):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        res = requests.get(f"{ASAAS_URL}/payments/{payment_id}", headers=HEADERS_ASAAS)
        status_asaas = res.json().get('status')

        if status_asaas in ['RECEIVED', 'CONFIRMED']:
            cursor.execute("SELECT * FROM pedidos_pix WHERE id = %s AND status = 'PENDENTE'", (payment_id,))
            pedido = cursor.fetchone()
            
            if pedido:
                user_id = pedido['utilizador_id']
                valor_pago = pedido['valor_pago']
                valor_bonus = pedido['valor_bonus']
                cupom_nome = pedido['cupom']

                cursor.execute("SELECT COUNT(*) as qtd FROM transacoes WHERE utilizador_id = %s AND descricao LIKE 'Recarga%%'", (user_id,))
                eh_primeira_recarga = cursor.fetchone()['qtd'] == 0

                valor_total = valor_pago + valor_bonus
                cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s RETURNING nome, indicado_por", (valor_total, user_id))
                cliente = cursor.fetchone()

                cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, 'Recarga PIX')", (user_id, valor_pago))
                
                if valor_bonus > 0:
                    cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)", (user_id, valor_bonus, f"🎟️ Bônus Cupom ({cupom_nome})"))
                    cursor.execute("SELECT id FROM cupons WHERE codigo = %s", (cupom_nome,))
                    cupom_db = cursor.fetchone()
                    if cupom_db: cursor.execute("INSERT INTO cupons_usados (utilizador_id, cupom_id) VALUES (%s, %s)", (user_id, cupom_db['id']))

                if eh_primeira_recarga and cliente['indicado_por']:
                    id_amigo = cliente['indicado_por']
                    valor_indicacao = valor_pago * 0.10
                    cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s", (valor_indicacao, id_amigo))
                    cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)", (id_amigo, valor_indicacao, f"🎁 Bônus de Indicação ({cliente['nome']})"))

                cursor.execute("UPDATE pedidos_pix SET status = 'CONCLUIDO' WHERE id = %s", (payment_id,))
                conn.commit()
                return {"status": "PAGO"}

        return {"status": "PENDENTE"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/devolver")
def devolver_jogo(dados: DevolucaoRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT status, conta_psn_id, EXTRACT(EPOCH FROM (data_fim - CURRENT_TIMESTAMP))/3600 AS horas FROM locacoes WHERE id = %s AND utilizador_id = %s", (dados.locacao_id, dados.utilizador_id))
        loc = cursor.fetchone()
        if not loc or loc['status'] != 'ATIVA': raise HTTPException(status_code=400, detail="Locação não encontrada ou já expirada.")
        cursor.execute("SELECT devolucao_dinamica, valor_por_dia FROM configuracoes LIMIT 1")
        config = cursor.fetchone()
        cashback = 0.0
        if config and config['devolucao_dinamica'] and loc['horas'] > 24:
            dias_restantes = int(loc['horas'] // 24)
            cashback = dias_restantes * config['valor_por_dia']
        cursor.execute("UPDATE locacoes SET status = 'EXPIRADA', cashback_pendente = %s, data_fim = CURRENT_TIMESTAMP WHERE id = %s", (cashback, dados.locacao_id))
        cursor.execute("UPDATE contas_psn SET status = 'MANUTENCAO' WHERE id = %s", (loc['conta_psn_id'],))
        conn.commit()
        return {"mensagem": "Devolução solicitada! O jogo foi para análise."}
    except Exception as e:
        conn.rollback(); raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/reservas", status_code=201)
def entrar_fila(reserva: NovaReserva):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT id FROM fila_espera WHERE utilizador_id = %s AND jogo_id = %s AND status = 'AGUARDANDO'", (reserva.utilizador_id, reserva.jogo_id))
        if cursor.fetchone(): raise HTTPException(status_code=400, detail="Você já está na fila de espera para este jogo!")
        
        cursor.execute("SELECT titulo, preco_aluguel, preco_aluguel_14 FROM jogos WHERE id = %s", (reserva.jogo_id,))
        jogo_info = cursor.fetchone()
        
        preco = jogo_info['preco_aluguel_14'] if reserva.dias_aluguel == 14 else jogo_info['preco_aluguel']
        titulo = jogo_info['titulo']
        
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (reserva.utilizador_id,))
        saldo = cursor.fetchone()['saldo']
        if saldo < preco: raise HTTPException(status_code=402, detail=f"Saldo insuficiente.")
        cursor.execute("UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s", (preco, reserva.utilizador_id))
        cursor.execute("INSERT INTO fila_espera (utilizador_id, jogo_id, dias_aluguel) VALUES (%s, %s, %s) RETURNING id", (reserva.utilizador_id, reserva.jogo_id, reserva.dias_aluguel))
        reserva_id = cursor.fetchone()['id']
        cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)", (reserva.utilizador_id, preco, f"Reserva na Fila ({reserva.dias_aluguel}d): {titulo}"))
        
        # 🚀 LÓGICA DO RANK FURA-FILA COM CÁLCULO DE DATAS CORRIGIDO
        cursor.execute("SELECT COUNT(*) as qtd FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA'", (reserva.utilizador_id,))
        meus_alugueis_qtd = cursor.fetchone()['qtd']
        
        if meus_alugueis_qtd > 0:
            # 1. Pega a Data Base Real (Hoje vs Lançamento vs Próxima Devolução)
            cursor.execute("SELECT data_lancamento, (SELECT MIN(data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = %s AND l.status = 'ATIVA') as prox FROM jogos WHERE id = %s", (reserva.jogo_id, reserva.jogo_id))
            jogo_meta = cursor.fetchone()
            
            base_date = datetime.now()
            if jogo_meta['data_lancamento']:
                dl = datetime.combine(jogo_meta['data_lancamento'], datetime.min.time())
                if dl > base_date: base_date = dl
            if jogo_meta['prox'] and jogo_meta['prox'] > base_date:
                base_date = jogo_meta['prox']

            # 2. Busca quem foi "empurrado"
            cursor.execute("""
                SELECT f.id, f.utilizador_id,
                (SELECT COALESCE(SUM(dias_aluguel), 0) FROM fila_espera f2 WHERE f2.jogo_id = f.jogo_id AND f2.status = 'AGUARDANDO' AND f2.data_solicitacao < f.data_solicitacao AND f2.id != %s) as dias_frente_antes
                FROM fila_espera f 
                WHERE f.jogo_id = %s AND f.status = 'AGUARDANDO' AND f.utilizador_id != %s
                AND (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = f.utilizador_id AND status = 'EXPIRADA') < %s
            """, (reserva_id, reserva.jogo_id, reserva.utilizador_id, meus_alugueis_qtd))
            bumped = cursor.fetchall()
            
            for b in bumped:
                # 3. A mágica da matemática real:
                # Data Antiga = Base + dias de quem já estava na frente
                # Data Nova = Data Antiga + dias do VIP que furou a fila
                dias_antes = b['dias_frente_antes']
                data_antiga = base_date + timedelta(days=dias_antes)
                data_nova = data_antiga + timedelta(days=reserva.dias_aluguel)
                
                str_antiga = data_antiga.strftime("%d/%m/%Y")
                str_nova = data_nova.strftime("%d/%m/%Y")
                
                msg = f"Devido à prioridade de Rank, a previsão do seu jogo {titulo} mudou de {str_antiga} para {str_nova} (+{reserva.dias_aluguel} dias)."
                cursor.execute("INSERT INTO notificacoes (utilizador_id, reserva_id, jogo, mensagem) VALUES (%s, %s, %s, %s)", (b['utilizador_id'], b['id'], titulo, msg))

        conn.commit()
        return {"mensagem": "Reserva confirmada! Valor descontado da sua carteira."}
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/reservas/cancelar")
def cancelar_reserva(dados: CancelarReserva):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT f.jogo_id, j.titulo FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id WHERE f.id = %s AND f.utilizador_id = %s AND f.status = 'AGUARDANDO'", (dados.reserva_id, dados.utilizador_id))
        res = cursor.fetchone()
        if not res: raise HTTPException(status_code=400, detail="Reserva não encontrada.")

        # 🚀 CORRIGIDO: Busca flexível ignorando espaços exatos
        cursor.execute("SELECT valor FROM transacoes WHERE utilizador_id = %s AND tipo = 'SAIDA' AND descricao LIKE %s ORDER BY id DESC LIMIT 1", (dados.utilizador_id, f"Reserva na Fila%{res['titulo']}%"))
        trans = cursor.fetchone()
        reembolso = trans['valor'] if trans else 0.0

        if reembolso > 0:
            cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s", (reembolso, dados.utilizador_id))
            cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)", (dados.utilizador_id, reembolso, f"💸 Estorno de Reserva: {res['titulo']}"))

        cursor.execute("DELETE FROM fila_espera WHERE id = %s", (dados.reserva_id,))
        
        if dados.notificacao_id > 0:
            cursor.execute("UPDATE notificacoes SET lida = TRUE WHERE id = %s", (dados.notificacao_id,))
        
        conn.commit()
        return {"mensagem": "Reserva cancelada e valor estornado para sua carteira!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/locacoes", status_code=201)
def realizar_locacao(locacao: NovaLocacao):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    try:
        cursor.execute("SELECT titulo, preco_aluguel, preco_aluguel_14 FROM jogos WHERE id = %s", (locacao.jogo_id,))
        jogo_info = cursor.fetchone()
        
        preco = jogo_info['preco_aluguel_14'] if locacao.dias_aluguel == 14 else jogo_info['preco_aluguel']
        titulo = jogo_info['titulo']
        
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (locacao.utilizador_id,))
        saldo = cursor.fetchone()['saldo']
        if saldo < preco: raise HTTPException(status_code=402, detail=f"Saldo insuficiente.")
        query_conta = "UPDATE contas_psn SET status = 'ALUGADA' WHERE id = (SELECT id FROM contas_psn WHERE jogo_id = %s AND status ILIKE 'DISPONIVEL' LIMIT 1) RETURNING id, email_login, senha_login;"
        cursor.execute(query_conta, (locacao.jogo_id,))
        conta = cursor.fetchone()
        if not conta: raise HTTPException(status_code=404, detail="Não há contas disponíveis no momento.")
        cursor.execute("UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s", (preco, locacao.utilizador_id))
        query_recibo = "INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA') RETURNING id, data_fim;"
        cursor.execute(query_recibo, (locacao.utilizador_id, conta['id'], locacao.dias_aluguel))
        recibo = cursor.fetchone()
        cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)", (locacao.utilizador_id, preco, f"Aluguel ({locacao.dias_aluguel}d): {titulo}"))
        conn.commit() 
        return {"mensagem": "Aluguel realizado! Valor descontado.", "pedido_id": recibo['id'], "data_devolucao": recibo['data_fim'], "psn_email": conta['email_login'], "psn_senha": conta['senha_login']}
    except Exception as e:
        conn.rollback() 
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.get("/admin/estatisticas")
def buscar_estatisticas_admin(periodo: str = "mes", admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Define a data de início baseada no filtro
    hoje = datetime.now()
    if periodo == "mes":
        data_inicio = hoje.replace(day=1, hour=0, minute=0, second=0)
    elif periodo == "30dias":
        data_inicio = hoje - timedelta(days=30)
    elif periodo == "ano":
        data_inicio = hoje.replace(month=1, day=1, hour=0, minute=0, second=0)
    else: # "tudo"
        data_inicio = datetime(2000, 1, 1)

    try:
        # 1. Faturamento no período (Apenas recargas de clientes)
        cursor.execute("""
            SELECT SUM(valor) as total FROM transacoes 
            WHERE tipo = 'ENTRADA' AND descricao LIKE 'Recarga%' 
            AND data_transacao >= %s
        """, (data_inicio,))
        faturamento = cursor.fetchone()['total'] or 0.0

        # 2. Novos Clientes no período
        cursor.execute("SELECT COUNT(*) as total FROM utilizadores WHERE is_admin = false AND id IN (SELECT id FROM utilizadores WHERE id > 0)") # Simplificado, mas você pode adicionar data_criacao na tabela users se quiser precisão total de tempo
        clientes = cursor.fetchone()['total'] or 0

        # 3. Locações feitas no período (Movimentação)
        cursor.execute("""
            SELECT COUNT(*) as total FROM transacoes 
            WHERE tipo = 'SAIDA' AND (descricao LIKE 'Aluguel%' OR descricao LIKE 'Reserva%')
            AND data_transacao >= %s
        """, (data_inicio,))
        movimentacao = cursor.fetchone()['total'] or 0

        return {
            "faturamento": float(faturamento),
            "total_clientes": clientes,
            "movimentacao_periodo": movimentacao
        }
    finally:
        cursor.close(); conn.close()

@app.get("/admin/cupons")
def listar_cupons(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM cupons ORDER BY id DESC")
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

@app.post("/admin/cupons")
def criar_cupom(cupom: NovoCupom, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO cupons (codigo, tipo, valor) VALUES (%s, %s, %s)", (cupom.codigo.upper(), cupom.tipo.upper(), cupom.valor))
        conn.commit()
        return {"mensagem": "Cupom criado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro: Este código de cupom já existe.")
    finally:
        cursor.close(); conn.close()

@app.delete("/admin/cupons/{cupom_id}")
def remover_cupom(cupom_id: int, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cupons WHERE id = %s", (cupom_id,))
    conn.commit(); cursor.close(); conn.close()
    return {"mensagem": "Cupom deletado."}

@app.get("/admin/manutencao")
def listar_contas_manutencao(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = """
        SELECT c.id AS conta_psn_id, j.titulo AS jogo, c.email_login, c.senha_login AS senha_antiga,
               (SELECT utilizador_id FROM locacoes WHERE conta_psn_id = c.id ORDER BY data_fim DESC LIMIT 1) AS ultimo_cliente_id,
               (SELECT u.nome FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id WHERE l.conta_psn_id = c.id ORDER BY l.data_fim DESC LIMIT 1) AS ultimo_cliente_nome,
               (SELECT u.telefone FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id WHERE l.conta_psn_id = c.id ORDER BY l.data_fim DESC LIMIT 1) AS ultimo_cliente_telefone,
               (SELECT cashback_pendente FROM locacoes WHERE conta_psn_id = c.id ORDER BY data_fim DESC LIMIT 1) AS cashback_pendente
        FROM contas_psn c
        JOIN jogos j ON c.jogo_id = j.id
        WHERE c.status = 'MANUTENCAO';
    """
    cursor.execute(query)
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

@app.post("/admin/multar")
def aplicar_multa(dados: AplicarMultaRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s", (dados.valor, dados.utilizador_id))
        cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, 'MULTA: Conta não desativada no Console')", (dados.utilizador_id, dados.valor))
        cursor.execute("UPDATE locacoes SET cashback_pendente = 0 WHERE utilizador_id = %s AND status = 'EXPIRADA'", (dados.utilizador_id,))
        conn.commit()
        return {"mensagem": f"A multa de R$ {dados.valor:.2f} foi aplicada!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/admin/ajustar-saldo")
def ajustar_saldo_manual(dados: AjusteSaldoRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s RETURNING saldo", (dados.valor, dados.utilizador_id))
        novo_saldo = cursor.fetchone()
        if not novo_saldo: raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        tipo = "ENTRADA" if dados.valor >= 0 else "SAIDA"
        cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, %s, %s, %s)", (dados.utilizador_id, tipo, abs(dados.valor), dados.motivo))
        conn.commit()
        return {"mensagem": f"Ajuste realizado! Novo saldo: R$ {novo_saldo['saldo']:.2f}"}
    except Exception as e:
        conn.rollback(); raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/admin/reset-senha")
def liberar_conta_manutencao(dados: ResetSenhaRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("UPDATE contas_psn SET senha_login = %s WHERE id = %s RETURNING jogo_id", (dados.nova_senha, dados.conta_psn_id))
        jogo_id = cursor.fetchone()['jogo_id']

        cursor.execute("SELECT id, utilizador_id, cashback_pendente FROM locacoes WHERE conta_psn_id = %s ORDER BY data_fim DESC LIMIT 1", (dados.conta_psn_id,))
        ultima_loc = cursor.fetchone()
        if ultima_loc and ultima_loc['cashback_pendente'] > 0:
            cash = ultima_loc['cashback_pendente']
            usr = ultima_loc['utilizador_id']
            cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s", (cash, usr))
            cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, '♻️ Cashback Devolução Antecipada')", (usr, cash))
            cursor.execute("UPDATE locacoes SET cashback_pendente = 0 WHERE id = %s", (ultima_loc['id'],))

        # 🚀 LÓGICA DE SELEÇÃO BASEADA EM RANK
        cursor.execute("""
            SELECT id, utilizador_id, dias_aluguel FROM fila_espera 
            WHERE jogo_id = %s AND status = 'AGUARDANDO' 
            ORDER BY (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') DESC, data_solicitacao ASC LIMIT 1
        """, (jogo_id,))
        proximo_da_fila = cursor.fetchone()
        
        if proximo_da_fila:
            # 🚀 AGORA ELE RESPEITA SE O CLIENTE PAGOU POR 7 OU 14 DIAS
            dias_comprados = proximo_da_fila.get('dias_aluguel', 7)
            cursor.execute("INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status) VALUES (%s, %s, CURRENT_TIMESTAMP + %s * INTERVAL '1 day', 'ATIVA')", (proximo_da_fila['utilizador_id'], dados.conta_psn_id, dias_comprados))
            cursor.execute("UPDATE fila_espera SET status = 'CONCLUIDO' WHERE id = %s", (proximo_da_fila['id'],))
            cursor.execute("UPDATE contas_psn SET status = 'ALUGADA' WHERE id = %s", (dados.conta_psn_id,))
            mensagem = "Senha alterada! A conta foi entregue para o próximo da fila."
            conn.commit()
        else:
            cursor.execute("UPDATE contas_psn SET status = 'DISPONIVEL' WHERE id = %s", (dados.conta_psn_id,))
            mensagem = "Senha alterada! A conta agora está DISPONÍVEL na vitrine."
            conn.commit()

        return {"mensagem": mensagem}
    except Exception as e:
        conn.rollback(); raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.delete("/jogos/{jogo_id}")
def deletar_jogo(jogo_id: int, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM jogos WHERE id = %s", (jogo_id,))
        conn.commit()
        return {"mensagem": "Jogo removido com sucesso"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Não é possível remover este jogo.")
    finally:
        cursor.close(); conn.close()

@app.post("/admin/locacoes/{locacao_id}/revogar")
def revogar_locacao_admin(locacao_id: int, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT conta_psn_id, status FROM locacoes WHERE id = %s", (locacao_id,))
        loc = cursor.fetchone()
        if not loc or loc['status'] != 'ATIVA': raise HTTPException(status_code=400, detail="Locação não encontrada ou já expirada.")
        cursor.execute("UPDATE locacoes SET status = 'EXPIRADA', data_fim = CURRENT_TIMESTAMP WHERE id = %s", (locacao_id,))
        cursor.execute("UPDATE contas_psn SET status = 'MANUTENCAO' WHERE id = %s", (loc['conta_psn_id'],))
        conn.commit()
        return {"mensagem": "Locação revogada! A conta foi enviada para manutenção."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.get("/admin/locacoes")
def listar_todas_locacoes(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = "SELECT l.id, u.nome AS cliente, j.titulo AS jogo, c.email_login, l.data_fim, l.status FROM locacoes l JOIN utilizadores u ON l.utilizador_id = u.id JOIN contas_psn c ON l.conta_psn_id = c.id JOIN jogos j ON c.jogo_id = j.id ORDER BY l.data_fim ASC;"
    cursor.execute(query)
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

@app.get("/admin/reservas")
def listar_todas_reservas(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # 1. Puxa os dados básicos e a previsão mínima de devolução do jogo
    query = """
        SELECT f.id, u.nome AS cliente, j.id AS jogo_id, j.titulo AS jogo, j.data_lancamento, f.data_solicitacao, f.status, f.utilizador_id, f.dias_aluguel,
        (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA') AS proxima_devolucao
        FROM fila_espera f 
        JOIN utilizadores u ON f.utilizador_id = u.id 
        JOIN jogos j ON f.jogo_id = j.id 
        WHERE f.status = 'AGUARDANDO'
        ORDER BY f.data_solicitacao ASC;
    """
    cursor.execute(query)
    reservas = cursor.fetchall()

    # 2. Aplica a Matemática VIP de Fila para o Admin ver as datas exatas
    for r in reservas:
        cursor.execute("""
            SELECT COALESCE(SUM(dias_aluguel), 0) as dias_frente FROM fila_espera 
            WHERE jogo_id = %s AND status = 'AGUARDANDO' AND (
                (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') > 
                (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA')
                OR ((SELECT COUNT(*) FROM locacoes WHERE utilizador_id = fila_espera.utilizador_id AND status = 'EXPIRADA') = 
                 (SELECT COUNT(*) FROM locacoes WHERE utilizador_id = %s AND status = 'EXPIRADA') AND data_solicitacao < %s)
            )
        """, (r['jogo_id'], r['utilizador_id'], r['utilizador_id'], r['data_solicitacao']))
        dias_frente = cursor.fetchone()['dias_frente']

        base_date = datetime.now()
        if r['data_lancamento']:
            dl = datetime.strptime(str(r['data_lancamento']), "%Y-%m-%d")
            if dl > base_date: base_date = dl
        if r['proxima_devolucao'] and r['proxima_devolucao'] > base_date: base_date = r['proxima_devolucao']
            
        est_start_date = base_date + timedelta(days=dias_frente)
        est_end_date = est_start_date + timedelta(days=r['dias_aluguel']) # Calcula o fim (Soma 7 ou 14)
        
        r['data_inicio'] = est_start_date.strftime("%d/%m/%Y")
        r['data_fim'] = est_end_date.strftime("%d/%m/%Y")

    cursor.close(); conn.close()
    return reservas

@app.post("/admin/reservas/{reserva_id}/cancelar")
def admin_cancelar_reserva(reserva_id: int, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT f.utilizador_id, f.jogo_id, j.titulo 
            FROM fila_espera f 
            JOIN jogos j ON f.jogo_id = j.id 
            WHERE f.id = %s AND f.status = 'AGUARDANDO'
        """, (reserva_id,))
        res = cursor.fetchone()
        if not res: raise HTTPException(status_code=400, detail="Reserva não encontrada ou já processada.")

        usr_id = res['utilizador_id']
        titulo = res['titulo']

        # 🚀 CORRIGIDO: Busca flexível ignorando espaços exatos
        cursor.execute("SELECT valor FROM transacoes WHERE utilizador_id = %s AND tipo = 'SAIDA' AND descricao LIKE %s ORDER BY id DESC LIMIT 1", (usr_id, f"Reserva na Fila%{titulo}%"))
        trans = cursor.fetchone()
        reembolso = trans['valor'] if trans else 0.0

        if reembolso > 0:
            cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s", (reembolso, usr_id))
            cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)", (usr_id, reembolso, f"💸 Estorno (Admin): {titulo}"))

        cursor.execute("DELETE FROM fila_espera WHERE id = %s", (reserva_id,))
        
        conn.commit()
        return {"mensagem": f"Reserva de {titulo} cancelada e valor estornado ao cliente."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.get("/usuarios")
def listar_usuarios(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, nome, email, telefone, saldo, is_admin FROM utilizadores ORDER BY nome ASC")
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

@app.delete("/usuarios/{usuario_id}")
def deletar_usuario(usuario_id: int, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM utilizadores WHERE id = %s", (usuario_id,))
        conn.commit()
        return {"mensagem": "Usuário removido com sucesso"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro: Este usuário possui histórico.")
    finally:
        cursor.close(); conn.close()

@app.post("/contas", status_code=201)
def cadastrar_conta_psn(conta: ContaPSNNova, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = "INSERT INTO contas_psn (jogo_id, email_login, senha_login, status, mfa_secret) VALUES (%s, %s, %s, 'DISPONIVEL', %s) RETURNING id;"
        cursor.execute(query, (conta.jogo_id, conta.email_login, conta.senha_login, conta.mfa_secret))
        conn.commit()
        return {"mensagem": "Conta adicionada com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro ao cadastrar conta.")
    finally:
        cursor.close(); conn.close()

@app.put("/jogos/{jogo_id}")
def editar_jogo_completo(jogo_id: int, dados: EditarJogoRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            UPDATE jogos 
            SET titulo = %s, plataforma = %s, preco_aluguel = %s, preco_aluguel_14 = %s, 
                descricao = %s, url_imagem = %s, tempo_jogo = %s, nota = %s, data_lancamento = %s
            WHERE id = %s
        """
        cursor.execute(query, (dados.titulo, dados.plataforma, dados.preco_aluguel, dados.preco_aluguel_14, dados.descricao, dados.url_imagem, dados.tempo_jogo, dados.nota, dados.data_lancamento, jogo_id))
        if cursor.rowcount == 0: raise HTTPException(status_code=404, detail="Jogo não encontrado.")
        conn.commit()
        return {"mensagem": "Jogo atualizado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro ao atualizar as informações do jogo.")
    finally:
        cursor.close(); conn.close()

def verificar_alugueis_vencidos():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, conta_psn_id FROM locacoes WHERE status = 'ATIVA' AND data_fim <= CURRENT_TIMESTAMP")
        locacoes_vencidas = cursor.fetchall()
        if locacoes_vencidas:
            for loc in locacoes_vencidas:
                cursor.execute("UPDATE locacoes SET status = 'EXPIRADA' WHERE id = %s", (loc[0],))
                cursor.execute("UPDATE contas_psn SET status = 'MANUTENCAO' WHERE id = %s", (loc[1],))
            conn.commit() 
    except Exception as e:
        conn.rollback()
    finally:
        cursor.close(); conn.close()

@app.get("/usuarios/{usuario_id}/saldo")
def buscar_saldo_real(usuario_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (usuario_id,))
        res = cursor.fetchone()
        return res if res else {"saldo": 0.0}
    finally:
        cursor.close(); conn.close()

@app.on_event("startup")
def iniciar_servicos():
    # 🚀 TENTA CONECTAR NO BANCO, MAS SE DER ERRO DE REDE, NÃO DERRUBA O SERVIDOR
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Cria a tabela de Notificações
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notificacoes (
                id SERIAL PRIMARY KEY, utilizador_id INT, reserva_id INT, jogo VARCHAR(255),
                mensagem TEXT, lida BOOLEAN DEFAULT FALSE, data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit() 

        # 2. Tenta adicionar a coluna de dias (Isolado em um Try/Catch)
        try:
            cursor.execute("ALTER TABLE fila_espera ADD COLUMN dias_aluguel INT DEFAULT 7")
            conn.commit() 
        except Exception:
            conn.rollback() # Ignora pacificamente se a coluna já existir

        cursor.close(); conn.close()
    except Exception as e:
        print(f"⚠️ AVISO DE STARTUP: Banco de dados ainda acordando. Detalhe: {e}")

    # Inicia a checagem de devolução de 1 em 1 minuto
    scheduler = BackgroundScheduler()
    scheduler.add_job(verificar_alugueis_vencidos, 'interval', minutes=1)
    scheduler.start()