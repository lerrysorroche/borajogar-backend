from pydantic import BaseModel
from typing import Optional

# ==============================================================================
# MODELOS DE USUÁRIOS E AUTENTICAÇÃO
# ==============================================================================


class UsuarioNovo(BaseModel):
    """
    Recebe os dados do frontend para registrar um novo cliente na locadora.
    O campo codigo_indicacao é opcional e serve para dar o cashback de 10% ao amigo.
    """

    nome: str
    email: str
    senha: str
    telefone: str
    codigo_indicacao: str = ""
    quer_grupo_whatsapp: bool = False


class LoginRequest(BaseModel):
    """Payload padrão para a rota de login via JWT."""

    email: str
    senha: str


class VerificarEmailRequest(BaseModel):
    """Payload para a validação do código de 6 dígitos enviado por e-mail."""

    email: str
    codigo: str


class ReenviarCodigoRequest(BaseModel):
    """Payload para solicitar um novo código de verificação."""

    email: str


class EsqueciSenhaRequest(BaseModel):
    """Recebe o e-mail do cliente para envio do link/código de recuperação."""

    email: str


class MudarSenhaRequest(BaseModel):
    """Usado no painel 'Meus Acessos' pelo próprio cliente para alterar a senha."""

    utilizador_id: int
    senha_atual: str
    nova_senha: str


class GoogleLoginRequest(BaseModel):
    """Recebe os dados do Firebase quando o cliente faz login social (Google)."""

    email: str
    nome: str
    telefone: str = ""
    quer_grupo_whatsapp: bool = False


class AtualizarTelefoneRequest(BaseModel):
    """
    Usado no painel 'Meus Acessos' pelo cliente para definir/corrigir seu telefone.
    O backend rejeita a alteração se o telefone já tiver sido verificado no WhatsApp.
    """

    telefone: str


class VerificarWhatsAdminRequest(BaseModel):
    """Botão manual 'Confirmar Whats' no Painel Admin, usado como rede de segurança
    caso a confirmação automática via webhook da Meta falhe para um cliente específico."""

    utilizador_id: int


# ==============================================================================
# MODELOS DE CATÁLOGO (JOGOS E ENQUETE)
# ==============================================================================


class JogoNovo(BaseModel):
    """
    Estrutura para cadastro de um novo jogo no painel Admin.
    INCLUI OS NOVOS PREÇOS DE VAGA SECUNDÁRIA para a arquitetura de 2 slots.
    """

    titulo: str
    plataforma: str
    preco_aluguel: float
    preco_aluguel_14: float = 0.0
    preco_secundaria: float = 0.0  # Adicionado para suportar locação secundária
    preco_secundaria_14: float = 0.0  # Adicionado para suportar locação secundária
    descricao: str
    url_imagem: str = ""
    tempo_jogo: str = ""
    nota: float = 0.0
    data_lancamento: Optional[str] = None
    recomendacao_cliente: bool = False


class EditarJogoRequest(BaseModel):
    """
    Estrutura para atualização completa de um jogo existente (Painel Admin).
    Mantém o espelho do JogoNovo para garantir a edição dos preços secundários.
    """

    titulo: str
    plataforma: str
    preco_aluguel: float
    preco_aluguel_14: float = 0.0
    preco_secundaria: float = 0.0  # Adicionado para edição
    preco_secundaria_14: float = 0.0  # Adicionado para edição
    descricao: str
    url_imagem: str = ""
    tempo_jogo: str = ""
    nota: float = 0.0
    data_lancamento: Optional[str] = None


class EditarPrecoJogoRequest(BaseModel):
    """Usado para atualização rápida apenas dos valores financeiros de um jogo."""

    preco_aluguel: float
    preco_aluguel_14: float = 0.0
    preco_secundaria: float = 0.0  # Adicionado para edição rápida
    preco_secundaria_14: float = 0.0  # Adicionado para edição rápida


class NovaOpcaoEnquete(BaseModel):
    """Recebe os dados para inserir um novo jogo na fila de votação dos clientes."""

    titulo: str
    url_imagem: str


class VotoEnquete(BaseModel):
    """Registra em qual jogo o cliente votou."""

    utilizador_id: int
    opcao_id: int


# ==============================================================================
# MODELOS DE LOCAÇÃO E ESTOQUE (CONTAS PSN)
# ==============================================================================


class ContaPSNNova(BaseModel):
    """
    Cadastra uma nova conta comprada pela loja para o cofre.
    Cada conta gerada aqui cria automaticamente as vagas PRIMARIA e SECUNDARIA.
    """

    jogo_id: int
    email_login: str
    senha_login: str
    mfa_secret: str = ""


class NovaLocacao(BaseModel):
    """
    Processa o aluguel direto da vitrine.
    A variável tipo_slot define se bloqueamos a Primária ou a Secundária no BD.
    """

    utilizador_id: int
    jogo_id: int
    dias_aluguel: int
    tipo_slot: str = "PRIMARIA"


class NovaReserva(BaseModel):
    """
    Insere o cliente na fila de espera.
    As filas são isoladas pelo tipo_slot (fila da primária e fila da secundária).
    """

    utilizador_id: int
    jogo_id: int
    dias_aluguel: int = 7
    tipo_slot: str = "PRIMARIA"


class CancelarReserva(BaseModel):
    """Permite ao cliente sair da fila e receber o saldo de volta."""

    reserva_id: int
    utilizador_id: int
    notificacao_id: int = 0


class DevolucaoRequest(BaseModel):
    """
    Encerra o aluguel antes do prazo.
    Dispara a lógica gamificada (Cashback diário se Primária, Fixo se Secundária).
    """

    locacao_id: int
    utilizador_id: int


# ==============================================================================
# MODELOS DE GESTÃO FINANCEIRA E ADMIN
# ==============================================================================


class NovaRecarga(BaseModel):
    """Dados enviados pelo cliente para gerar um Pix ou Checkout via Cartão (Stripe/Efí)."""

    utilizador_id: int
    valor: float
    cupom: str = ""
    cpf: str


class NovoCupom(BaseModel):
    """Criação de cupons promocionais (FIXO ou PORCENTAGEM) pelo Admin."""

    codigo: str
    tipo: str
    valor: float


class ResetSenhaRequest(BaseModel):
    """
    Ação do Admin após a manutenção de uma conta.
    Libera a conta para a fila de espera ou estoque após a nova senha ser salva.
    """

    conta_psn_id: int
    nova_senha: str


class AplicarMultaRequest(BaseModel):
    """
    Acionado pelo botão 'Cobrar no WhatsApp'.
    Além da multa financeira, o backend cortará o Rank pela metade
    e cancelará Cashbacks pendentes.
    """

    utilizador_id: int
    valor: float = 0.0  # Mudei o padrão para 0.0 para ser flexível (só punir Rank)


class AjusteSaldoRequest(BaseModel):
    """Usado pelo Admin para injetar ou retirar saldo de um cliente manualmente."""

    utilizador_id: int
    valor: float
    motivo: str


class EditarClienteRequest(BaseModel):
    """Atualização dos dados cadastrais e financeiros de um cliente pelo Admin."""

    nome: str
    email: str
    telefone: str
    saldo: float
    motivo_ajuste: str = "Ajuste Administrativo"


# ==============================================================================
# MODELOS DE SISTEMA E NOTIFICAÇÕES
# ==============================================================================


class ConfigRequest(BaseModel):
    """
    Atualiza as configurações globais da plataforma, como banners e taxa de devolução.
    """

    devolucao_dinamica: bool
    valor_por_dia: float
    anuncio_ativo: bool
    mensagem_anuncio: str
    banners_url: str = ""
    enquete_titulo: str = "Próximas Adições: Você Decide!"
    enquete_subtitulo: str = (
        "Vote no jogo que você mais quer ver no catálogo e ajude a BORA JOGAR! a crescer."
    )


class LerNotificacao(BaseModel):
    """Marca o aviso de mudança de datas na fila de espera como 'Lido'."""

    notificacao_id: int


class BroadcastNotificacao(BaseModel):
    """Painel Admin: dispara uma notificação avulsa (tipo customizável) para
    todos os clientes cadastrados. Usado para avisar a base antiga sobre o
    Grupo do WhatsApp, mas a mensagem é editável pelo admin antes do envio."""

    mensagem: str
    tipo: str = "GRUPO_WHATSAPP"
