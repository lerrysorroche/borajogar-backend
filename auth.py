import os

import jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# A chave sai do código-fonte: quem tem a chave consegue forjar um token com
# is_admin = True. Sem a variável no ambiente a API não sobe de propósito —
# um fallback silencioso para a chave antiga anularia a troca.
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError(
        "JWT_SECRET_KEY não configurada. Defina a variável de ambiente no Render "
        "(Environment > Add Environment Variable) antes de subir a API."
    )

ALGORITHM = "HS256"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()


def gerar_hash_senha(senha):
    return pwd_context.hash(senha)


def verificar_senha(senha_pura, senha_criptografada):
    return pwd_context.verify(senha_pura, senha_criptografada)


def criar_token_acesso(dados: dict):
    dados_para_codificar = dados.copy()
    dados_para_codificar.update({"exp": datetime.utcnow() + timedelta(days=7)})
    return jwt.encode(dados_para_codificar, SECRET_KEY, algorithm=ALGORITHM)


def _decodificar_token(credentials: HTTPAuthorizationCredentials):
    """Abre o token e devolve o payload, ou 401 se estiver inválido/expirado."""
    try:
        return jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido.")


def verificar_usuario(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Exige um cliente logado (qualquer um) e devolve o payload do token.
    Use o 'id' daqui, nunca o id que veio na URL ou no corpo: o do token é o
    único que o cliente não consegue trocar pelo de outra pessoa.
    """
    payload = _decodificar_token(credentials)
    if not payload.get("id"):
        raise HTTPException(status_code=401, detail="Token inválido.")
    return payload


def verificar_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verifica se o usuário logado é um administrador válido"""
    # O decode fica fora do try do 403: antes, o 'except Exception' engolia o
    # próprio HTTPException(403) e devolvia 401 para token válido de não-admin.
    payload = _decodificar_token(credentials)
    if not payload.get("is_admin"):
        raise HTTPException(status_code=403, detail="Acesso Negado.")
    return payload
