import jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SECRET_KEY = "chave-super-secreta-bora-jogar"
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


def verificar_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verifica se o usuário logado é um administrador válido"""
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if not payload.get("is_admin"):
            raise HTTPException(status_code=403, detail="Acesso Negado.")
        return payload
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido.")
