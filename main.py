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
import requests # NOVO: Para conversar com o Asaas
import socket
import smtplib
from email.mime.text import MIMEText

# --- CLASSE MÁGICA PARA FORÇAR O RENDER A USAR IPV4 NO E-MAIL ---
class SMTP_IPv4(smtplib.SMTP):
    def _get_socket(self, host, port, timeout):
        return socket.create_connection((host, port), timeout, socket.AF_INET)

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
    """Abre a porta com o Banco de Dados PostgreSQL na Nuvem."""
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

def gerar_codigo_convite(nome):
    letras = "".join(filter(str.isalpha, nome.split()[0].upper()))[:4].ljust(4, 'X')
    nums = "".join(random.choices(string.digits, k=4))
    return f"{letras}{nums}"

class UsuarioNovo(BaseModel): nome: str; email: str; senha: str; telefone: str; codigo_indicacao: str = ""
class JogoNovo(BaseModel): titulo: str; plataforma: str; preco_aluguel: float; descricao: str; url_imagem: str = ""; tempo_jogo: str = ""; nota: float = 0.0
class ContaPSNNova(BaseModel): jogo_id: int; email_login: str; senha_login: str; mfa_secret: str = "" 
class NovaLocacao(BaseModel): utilizador_id: int; jogo_id: int; dias_aluguel: int
class LoginRequest(BaseModel): email: str; senha: str
class EsqueciSenhaRequest(BaseModel): email: str
class MudarSenhaRequest(BaseModel): utilizador_id: int; senha_atual: str; nova_senha: str
class NovaReserva(BaseModel): utilizador_id: int; jogo_id: int
class NovaRecarga(BaseModel): utilizador_id: int; valor: float; cupom: str = ""
class NovoCupom(BaseModel): codigo: str; tipo: str; valor: float
class ResetSenhaRequest(BaseModel): conta_psn_id: int; nova_senha: str
class AplicarMultaRequest(BaseModel): utilizador_id: int; valor: float = 50.0
class AjusteSaldoRequest(BaseModel): utilizador_id: int; valor: float; motivo: str
class ConfigRequest(BaseModel): devolucao_dinamica: bool; valor_por_dia: float; anuncio_ativo: bool; mensagem_anuncio: str
class DevolucaoRequest(BaseModel): locacao_id: int; utilizador_id: int
class EditarPrecoJogoRequest(BaseModel): preco_aluguel: float

@app.get("/")
def home(): return {"mensagem": "API Online"}

@app.get("/configuracoes")
def get_config():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT devolucao_dinamica, valor_por_dia, anuncio_ativo, mensagem_anuncio FROM configuracoes LIMIT 1")
    config = cursor.fetchone()
    cursor.close(); conn.close()
    return config if config else {"devolucao_dinamica": False, "valor_por_dia": 2.0, "anuncio_ativo": False, "mensagem_anuncio": ""}

@app.post("/admin/configuracoes")
def set_config(dados: ConfigRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM configuracoes LIMIT 1")
    if cursor.fetchone():
        cursor.execute("""
            UPDATE configuracoes 
            SET devolucao_dinamica = %s, valor_por_dia = %s, anuncio_ativo = %s, mensagem_anuncio = %s
        """, (dados.devolucao_dinamica, dados.valor_por_dia, dados.anuncio_ativo, dados.mensagem_anuncio))
    else:
        cursor.execute("""
            INSERT INTO configuracoes (devolucao_dinamica, valor_por_dia, anuncio_ativo, mensagem_anuncio)
            VALUES (%s, %s, %s, %s)
        """, (dados.devolucao_dinamica, dados.valor_por_dia, dados.anuncio_ativo, dados.mensagem_anuncio))
        
    conn.commit(); cursor.close(); conn.close()
    return {"mensagem": "Configurações salvas!"}

@app.post("/jogos", status_code=201)
def cadastrar_jogo(jogo: JogoNovo, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """INSERT INTO jogos (titulo, plataforma, preco_aluguel, descricao, url_imagem, tempo_jogo, nota) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;"""
        cursor.execute(query, (jogo.titulo, jogo.plataforma, jogo.preco_aluguel, jogo.descricao, jogo.url_imagem, jogo.tempo_jogo, jogo.nota))
        conn.commit()
        return {"mensagem": "Jogo adicionado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro ao cadastrar o jogo.")
    finally:
        cursor.close(); conn.close()

@app.get("/jogos")
def listar_jogos():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    # ATENÇÃO: Adicionamos a linha "popularidade" contando o histórico de locações!
    query = """SELECT j.id, j.titulo, j.plataforma, j.preco_aluguel, j.descricao, j.url_imagem, j.tempo_jogo, j.nota,
            (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status ILIKE 'DISPONIVEL') AS estoque,
            (SELECT COUNT(*) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS tamanho_fila,
            (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id AND l.status = 'ATIVA') AS proxima_devolucao,
            (SELECT COUNT(*) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id) AS popularidade
        FROM jogos j ORDER BY j.titulo ASC;"""
    cursor.execute(query)
    resultados = cursor.fetchall()
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
    cursor.execute("SELECT f.id AS reserva_id, j.titulo AS jogo, f.data_solicitacao, f.status, (SELECT COUNT(*) FROM fila_espera f2 WHERE f2.jogo_id = f.jogo_id AND f2.status = 'AGUARDANDO' AND f2.data_solicitacao < f.data_solicitacao) AS pessoas_na_frente, (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = f.jogo_id AND l.status = 'ATIVA') AS proxima_devolucao FROM fila_espera f JOIN jogos j ON f.jogo_id = j.id WHERE f.utilizador_id = %s AND f.status = 'AGUARDANDO' ORDER BY f.data_solicitacao ASC;", (usuario_id,))
    resultados = cursor.fetchall()
    cursor.close(); conn.close()
    return resultados

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


# ==============================================================================
# NOVAS ROTAS DE SENHA (RECUPERAÇÃO E ALTERAÇÃO)
# ==============================================================================

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
    
    # --- PORTA DOS FUNDOS: MOSTRAR A SENHA NO LOG DO RENDER ---
    print(f"🚨 SENHA DE RESGATE PARA {req.email}: {nova_senha} 🚨")
    
    cursor.execute("UPDATE utilizadores SET senha_hash = %s WHERE email = %s", (senha_hash, req.email))
    conn.commit()
    
    try:
        remetente = os.getenv("EMAIL_REMETENTE")
        senha_app = os.getenv("EMAIL_SENHA_APP")
        
        texto_email = f"Olá, {usuario['nome']}!\n\nSua nova senha temporária para acessar a Bora Jogar é: {nova_senha}\n\nRecomendamos que você altere esta senha no seu Painel do Cliente logo após o login."
        msg = MIMEText(texto_email)
        msg['Subject'] = 'Bora Jogar - Recuperação de Senha'
        msg['From'] = f"Equipe Bora Jogar <{remetente}>"
        msg['To'] = req.email
        
        # Usando a classe que força a estrada antiga (IPv4) com a porta TLS
        with SMTP_IPv4('smtp.gmail.com', 587) as smtp:
            smtp.starttls() # Ativa a criptografia de segurança
            smtp.login(remetente, senha_app)
            smtp.send_message(msg)
            
    except Exception as e:
        print(f"Erro ao enviar email: {e}")
        pass
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


# ==============================================================================
# O NOVO MOTOR FINANCEIRO (Asaas PIX)
# ==============================================================================
@app.post("/recarga/gerar-pix")
def gerar_pix_asaas(recarga: NovaRecarga):
    if recarga.valor < 30.0: raise HTTPException(status_code=400, detail="O valor mínimo de recarga é R$ 30,00.")
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        valor_bonus_cupom = 0.0
        cupom_id = None
        
        # 1. Validações Locais (Cupom)
        if recarga.cupom:
            cursor.execute("SELECT id, tipo, valor FROM cupons WHERE codigo = %s AND ativo = TRUE", (recarga.cupom.upper(),))
            cupom = cursor.fetchone()
            if not cupom: raise HTTPException(status_code=404, detail="Cupom inválido ou expirado.")
            
            cursor.execute("SELECT id FROM cupons_usados WHERE utilizador_id = %s AND cupom_id = %s", (recarga.utilizador_id, cupom['id']))
            if cursor.fetchone(): raise HTTPException(status_code=400, detail="Você já utilizou este cupom promocional.")
            
            cupom_id = cupom['id']
            if cupom['tipo'] == 'FIXO': valor_bonus_cupom = cupom['valor']
            elif cupom['tipo'] == 'PORCENTAGEM': valor_bonus_cupom = recarga.valor * (cupom['valor'] / 100.0)

        # 2. Puxa dados do cliente
        cursor.execute("SELECT nome, email FROM utilizadores WHERE id = %s", (recarga.utilizador_id,))
        usr = cursor.fetchone()

        # 3. Cria o Cliente no Asaas 
        # CORREÇÃO 3: Usando um CPF matemático de teste válido para o Sandbox
        payload_cli = {
            "name": usr['nome'], 
            "email": usr['email'],
            "cpfCnpj": "12345678909" # CPF válido apenas para aprovação do gateway
        }
        res_cli = requests.post(f"{ASAAS_URL}/customers", json=payload_cli, headers=HEADERS_ASAAS)
        if res_cli.status_code not in [200, 201]: 
            raise Exception(f"Erro Asaas (Cliente): {res_cli.text}")
        cli_id = res_cli.json().get('id')

        # 4. Gera a Cobrança PIX
        # CORREÇÃO 2: Vencimento para amanhã (+1 dia) para evitar corte de fuso horário
        vencimento = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        payload_cob = {
            "customer": cli_id, 
            "billingType": "PIX", 
            "value": recarga.valor, 
            "dueDate": vencimento, 
            "description": "Recarga de Carteira - BORA JOGAR"
        }
        res_cob = requests.post(f"{ASAAS_URL}/payments", json=payload_cob, headers=HEADERS_ASAAS)
        if res_cob.status_code not in [200, 201]: 
            raise Exception(f"Erro Asaas (Cobrança): {res_cob.text}")
        pay_id = res_cob.json().get('id')

        # 5. Pede o QR Code dessa cobrança para o Asaas
        res_qr = requests.get(f"{ASAAS_URL}/payments/{pay_id}/pixQrCode", headers=HEADERS_ASAAS)
        if res_qr.status_code not in [200, 201]: 
            raise Exception(f"Erro Asaas (QRCode): {res_qr.text}")
        qr_data = res_qr.json()

        # 6. Salva o pedido PENDENTE no nosso banco local
        cupom_nome = recarga.cupom.upper() if recarga.cupom else ""
        cursor.execute("INSERT INTO pedidos_pix (id, utilizador_id, valor_pago, valor_bonus, cupom) VALUES (%s, %s, %s, %s, %s)",
                       (pay_id, recarga.utilizador_id, recarga.valor, valor_bonus_cupom, cupom_nome))
        conn.commit()

        # Devolve o QRCode pro Frontend exibir
        return {
            "payment_id": pay_id,
            "copia_cola": qr_data.get('payload'),
            "qr_code": qr_data.get('encodedImage')
        }

    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException): raise e
        # Se der erro, agora o Toast vai mostrar o motivo EXATO do Asaas
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.get("/recarga/status/{payment_id}")
def checar_status_pagamento(payment_id: str):
    """Essa rota é chamada pelo React a cada 5 segundos para ver se o cliente já pagou."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Pergunta ao Asaas se o dinheiro já caiu na conta
        res = requests.get(f"{ASAAS_URL}/payments/{payment_id}", headers=HEADERS_ASAAS)
        status_asaas = res.json().get('status')

        # RECEIVED (Asaas recebeu) ou CONFIRMED (dinheiro já liberado)
        if status_asaas in ['RECEIVED', 'CONFIRMED']:
            # Puxa o nosso pedido PENDENTE local
            cursor.execute("SELECT * FROM pedidos_pix WHERE id = %s AND status = 'PENDENTE'", (payment_id,))
            pedido = cursor.fetchone()
            
            if pedido:
                user_id = pedido['utilizador_id']
                valor_pago = pedido['valor_pago']
                valor_bonus = pedido['valor_bonus']
                cupom_nome = pedido['cupom']

                # Verifica se é a PRIMEIRA RECARGA (Para o Bonus de Indicação)
                cursor.execute("SELECT COUNT(*) as qtd FROM transacoes WHERE utilizador_id = %s AND descricao LIKE 'Recarga%%'", (user_id,))
                eh_primeira_recarga = cursor.fetchone()['qtd'] == 0

                # Deposita o saldo na conta da pessoa
                valor_total = valor_pago + valor_bonus
                cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s RETURNING nome, indicado_por", (valor_total, user_id))
                cliente = cursor.fetchone()

                # Escreve os recibos no Extrato
                cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, 'Recarga PIX')", (user_id, valor_pago))
                
                # Regras de Cupom
                if valor_bonus > 0:
                    cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)", (user_id, valor_bonus, f"🎟️ Bônus Cupom ({cupom_nome})"))
                    cursor.execute("SELECT id FROM cupons WHERE codigo = %s", (cupom_nome,))
                    cupom_db = cursor.fetchone()
                    if cupom_db: cursor.execute("INSERT INTO cupons_usados (utilizador_id, cupom_id) VALUES (%s, %s)", (user_id, cupom_db['id']))

                # Regras de Indicação (10% pro amigo)
                if eh_primeira_recarga and cliente['indicado_por']:
                    id_amigo = cliente['indicado_por']
                    valor_indicacao = valor_pago * 0.10
                    cursor.execute("UPDATE utilizadores SET saldo = saldo + %s WHERE id = %s", (valor_indicacao, id_amigo))
                    cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'ENTRADA', %s, %s)", (id_amigo, valor_indicacao, f"🎁 Bônus de Indicação ({cliente['nome']})"))

                # Tranca o pedido como PAGO para não ser creditado de novo
                cursor.execute("UPDATE pedidos_pix SET status = 'CONCLUIDO' WHERE id = %s", (payment_id,))
                conn.commit()
                
                return {"status": "PAGO"}

        # Se não pagou ainda, manda "PENDENTE" pro React continuar aguardando
        return {"status": "PENDENTE"}
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

# ==============================================================================
# ROTAS GERAIS MANTIDAS INTACTAS
# ==============================================================================

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
        cursor.execute("SELECT titulo, preco_aluguel FROM jogos WHERE id = %s", (reserva.jogo_id,))
        jogo_info = cursor.fetchone()
        preco = jogo_info['preco_aluguel']
        titulo = jogo_info['titulo']
        cursor.execute("SELECT saldo FROM utilizadores WHERE id = %s", (reserva.utilizador_id,))
        saldo = cursor.fetchone()['saldo']
        if saldo < preco: raise HTTPException(status_code=402, detail=f"Saldo insuficiente.")
        cursor.execute("UPDATE utilizadores SET saldo = saldo - %s WHERE id = %s", (preco, reserva.utilizador_id))
        cursor.execute("INSERT INTO fila_espera (utilizador_id, jogo_id) VALUES (%s, %s)", (reserva.utilizador_id, reserva.jogo_id))
        cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)", (reserva.utilizador_id, preco, f"Reserva na Fila: {titulo}"))
        conn.commit()
        return {"mensagem": "Reserva confirmada! Valor descontado da sua carteira."}
    except Exception as e:
        conn.rollback()
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.post("/locacoes", status_code=201)
def realizar_locacao(locacao: NovaLocacao):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor) 
    try:
        cursor.execute("SELECT titulo, preco_aluguel FROM jogos WHERE id = %s", (locacao.jogo_id,))
        jogo_info = cursor.fetchone()
        preco = jogo_info['preco_aluguel']
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
        cursor.execute("INSERT INTO transacoes (utilizador_id, tipo, valor, descricao) VALUES (%s, 'SAIDA', %s, %s)", (locacao.utilizador_id, preco, f"Aluguel: {titulo}"))
        conn.commit() 
        return {"mensagem": "Aluguel realizado! Valor descontado.", "pedido_id": recibo['id'], "data_devolucao": recibo['data_fim'], "psn_email": conta['email_login'], "psn_senha": conta['senha_login']}
    except Exception as e:
        conn.rollback() 
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()

@app.get("/admin/estatisticas")
def buscar_estatisticas_admin(admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT SUM(valor) as total FROM transacoes WHERE tipo = 'ENTRADA' AND descricao LIKE 'Recarga%'")
    faturamento = cursor.fetchone()['total'] or 0.0
    cursor.execute("SELECT COUNT(*) as total FROM utilizadores WHERE is_admin = false")
    clientes = cursor.fetchone()['total'] or 0
    cursor.execute("SELECT COUNT(*) as total FROM locacoes WHERE status = 'ATIVA'")
    locacoes_ativas = cursor.fetchone()['total'] or 0
    cursor.close(); conn.close()
    return {"faturamento": float(faturamento), "total_clientes": clientes, "locacoes_ativas": locacoes_ativas}

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

        cursor.execute("SELECT id, utilizador_id FROM fila_espera WHERE jogo_id = %s AND status = 'AGUARDANDO' ORDER BY data_solicitacao ASC LIMIT 1", (jogo_id,))
        proximo_da_fila = cursor.fetchone()
        if proximo_da_fila:
            cursor.execute("INSERT INTO locacoes (utilizador_id, conta_psn_id, data_fim, status) VALUES (%s, %s, CURRENT_TIMESTAMP + 7 * INTERVAL '1 day', 'ATIVA')", (proximo_da_fila['utilizador_id'], dados.conta_psn_id))
            cursor.execute("UPDATE fila_espera SET status = 'CONCLUIDO' WHERE id = %s", (proximo_da_fila['id'],))
            cursor.execute("UPDATE contas_psn SET status = 'ALUGADA' WHERE id = %s", (dados.conta_psn_id,))
            mensagem = "Senha alterada! A conta foi entregue para o próximo da fila."
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
        # Puxa os dados da locação
        cursor.execute("SELECT conta_psn_id, status FROM locacoes WHERE id = %s", (locacao_id,))
        loc = cursor.fetchone()
        
        if not loc or loc['status'] != 'ATIVA':
            raise HTTPException(status_code=400, detail="Locação não encontrada ou já expirada.")
        
        # Encerra a locação no momento exato e joga para Manutenção
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

@app.patch("/jogos/{jogo_id}/preco")
def atualizar_preco_jogo(jogo_id: int, dados: EditarPrecoJogoRequest, admin_data = Depends(verificar_admin)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE jogos SET preco_aluguel = %s WHERE id = %s", (dados.preco_aluguel, jogo_id))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Jogo não encontrado.")
        conn.commit()
        return {"mensagem": "Preço atualizado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Erro ao atualizar preço.")
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

@app.on_event("startup")
def iniciar_relogio():
    scheduler = BackgroundScheduler()
    scheduler.add_job(verificar_alugueis_vencidos, 'interval', minutes=1)

    scheduler.start()