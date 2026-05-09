from pydantic import BaseModel


class UsuarioNovo(BaseModel):
    nome: str
    email: str
    senha: str
    telefone: str
    codigo_indicacao: str = ""


class JogoNovo(BaseModel):
    titulo: str
    plataforma: str
    preco_aluguel: float
    preco_aluguel_14: float = 0.0
    descricao: str
    url_imagem: str = ""
    tempo_jogo: str = ""
    nota: float = 0.0
    data_lancamento: str = None


class ContaPSNNova(BaseModel):
    jogo_id: int
    email_login: str
    senha_login: str
    mfa_secret: str = ""


class NovaLocacao(BaseModel):
    utilizador_id: int
    jogo_id: int
    dias_aluguel: int


class LoginRequest(BaseModel):
    email: str
    senha: str


class EsqueciSenhaRequest(BaseModel):
    email: str


class MudarSenhaRequest(BaseModel):
    utilizador_id: int
    senha_atual: str
    nova_senha: str


class NovaReserva(BaseModel):
    utilizador_id: int
    jogo_id: int
    dias_aluguel: int = 7


class NovaRecarga(BaseModel):
    utilizador_id: int
    valor: float
    cupom: str = ""
    cpf: str


class NovoCupom(BaseModel):
    codigo: str
    tipo: str
    valor: float


class ResetSenhaRequest(BaseModel):
    conta_psn_id: int
    nova_senha: str


class AplicarMultaRequest(BaseModel):
    utilizador_id: int
    valor: float = 50.0


class AjusteSaldoRequest(BaseModel):
    utilizador_id: int
    valor: float
    motivo: str


class ConfigRequest(BaseModel):
    devolucao_dinamica: bool
    valor_por_dia: float
    anuncio_ativo: bool
    mensagem_anuncio: str
    banners_url: str = ""


class DevolucaoRequest(BaseModel):
    locacao_id: int
    utilizador_id: int


class EditarPrecoJogoRequest(BaseModel):
    preco_aluguel: float
    preco_aluguel_14: float = 0.0


class EditarJogoRequest(BaseModel):
    titulo: str
    plataforma: str
    preco_aluguel: float
    preco_aluguel_14: float = 0.0
    descricao: str
    url_imagem: str = ""
    tempo_jogo: str = ""
    nota: float = 0.0
    data_lancamento: str = None


class NovaOpcaoEnquete(BaseModel):
    titulo: str
    url_imagem: str


class VotoEnquete(BaseModel):
    utilizador_id: int
    opcao_id: int


class EditarClienteRequest(BaseModel):
    nome: str
    email: str
    telefone: str
    saldo: float
    motivo_ajuste: str = "Ajuste Administrativo"


class LerNotificacao(BaseModel):
    notificacao_id: int


class CancelarReserva(BaseModel):
    reserva_id: int
    utilizador_id: int
    notificacao_id: int = 0


class GoogleLoginRequest(BaseModel):
    email: str
    nome: str
    telefone: str = ""
