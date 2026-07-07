from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
from database import get_db_connection
from auth import verificar_admin
from models import JogoNovo, EditarJogoRequest, VotoEnquete, NovaOpcaoEnquete

router = APIRouter(tags=["Jogos"])

# ==============================================================================
# VITRINE E LISTAGEM DE JOGOS
# ==============================================================================


@router.get("/jogos")
def listar_jogos():
    """
    [R] O Motor da Vitrine Principal.
    Retorna o catálogo completo de jogos, calculando em tempo real:
    - O estoque isolado da Vaga Primária e Secundária.
    - O tamanho e os dias acumulados na Fila de Espera.
    - A ordenação inteligente: Prioriza lançamentos futuros (1), recentes (2) e populares (3).
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = """
            SELECT j.id, j.titulo, j.plataforma, j.descricao, j.url_imagem, j.tempo_jogo, j.nota, CAST(j.data_lancamento AS VARCHAR) as data_lancamento,
                j.preco_aluguel, j.preco_aluguel_14, 
                j.preco_secundaria, j.preco_secundaria_14,
                (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status_primaria = 'DISPONIVEL') AS estoque_primaria,
                (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status_secundaria = 'DISPONIVEL') AS estoque_secundaria,
                (SELECT COUNT(*) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS tamanho_fila,
                (SELECT COALESCE(SUM(dias_aluguel), 0) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS fila_dias_espera,
                (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id AND l.status = 'ATIVA') AS proxima_devolucao,
                (SELECT COUNT(*) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id) AS popularidade,
                (SELECT COALESCE(SUM(
                    CASE 
                        WHEN t.tipo = 'SAIDA' THEN t.valor 
                        WHEN t.tipo = 'ENTRADA' THEN -t.valor 
                        ELSE 0 
                    END
                ), 0) FROM transacoes t WHERE t.utilizador_id != 1 AND t.descricao ILIKE '%' || j.titulo || '%') AS faturamento_total,
                CASE 
                    WHEN j.data_lancamento > CURRENT_DATE THEN 1 
                    WHEN j.data_lancamento >= CURRENT_DATE - INTERVAL '180 days' THEN 2 
                    ELSE 3 
                END as prioridade_vitrine
            FROM jogos j 
            ORDER BY 
                prioridade_vitrine ASC,
                CASE WHEN j.data_lancamento > CURRENT_DATE THEN j.data_lancamento END ASC,
                CASE WHEN j.data_lancamento >= CURRENT_DATE - INTERVAL '180 days' AND j.data_lancamento <= CURRENT_DATE THEN j.data_lancamento END DESC,
                CASE WHEN j.data_lancamento < CURRENT_DATE - INTERVAL '180 days' OR j.data_lancamento IS NULL 
                     THEN (SELECT COUNT(*) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id) 
                END DESC NULLS LAST,
                j.data_lancamento DESC NULLS LAST;
        """
        cursor.execute(query)
        resultados = cursor.fetchall()
    except Exception as e:
        conn.rollback()
        # Fallback de Segurança: Se a query complexa falhar, retorna os dados básicos para não derrubar o site.
        query_segura = """
            SELECT j.id, j.titulo, j.plataforma, j.preco_aluguel, j.preco_aluguel_14, 
                j.preco_secundaria, j.preco_secundaria_14, j.descricao, j.url_imagem, j.tempo_jogo, j.nota, CAST(j.data_lancamento AS VARCHAR) as data_lancamento,
                (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status_primaria = 'DISPONIVEL') AS estoque_primaria,
                (SELECT COUNT(*) FROM contas_psn WHERE jogo_id = j.id AND status_secundaria = 'DISPONIVEL') AS estoque_secundaria,
                (SELECT COUNT(*) FROM fila_espera WHERE jogo_id = j.id AND status = 'AGUARDANDO') AS tamanho_fila,
                0 AS fila_dias_espera,
                (SELECT MIN(l.data_fim) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id AND l.status = 'ATIVA') AS proxima_devolucao,
                (SELECT COUNT(*) FROM locacoes l JOIN contas_psn c ON l.conta_psn_id = c.id WHERE c.jogo_id = j.id) AS popularidade,
                (SELECT COALESCE(SUM(CASE WHEN t.tipo = 'SAIDA' THEN t.valor WHEN t.tipo = 'ENTRADA' THEN -t.valor ELSE 0 END), 0) FROM transacoes t WHERE t.utilizador_id != 1 AND t.descricao ILIKE '%' || j.titulo || '%') AS faturamento_total
            FROM jogos j ORDER BY j.id DESC;
        """
        cursor.execute(query_segura)
        resultados = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
    return resultados


@router.get("/jogos/novidades")
def listar_novidades_locadora():
    """
    [R] Prateleira Rápida.
    Devolve apenas os 5 últimos jogos cadastrados no sistema (maiores IDs),
    evitando que o frontend precise baixar centenas de jogos apenas para montar a aba "Novidades".
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            "SELECT id, titulo, url_imagem, recomendacao_cliente FROM jogos ORDER BY id DESC LIMIT 5"
        )
        return cursor.fetchall()
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Erro ao buscar novidades: {str(e)}"
        )
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# GESTÃO DO CATÁLOGO (ADMIN)
# ==============================================================================


@router.post("/jogos", status_code=201)
def cadastrar_jogo(jogo: JogoNovo, admin_data=Depends(verificar_admin)):
    """
    [C] Adiciona um jogo novo no banco de dados.
    Agora inclui os preços da vaga secundária e a tag de recomendação da comunidade.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            INSERT INTO jogos (
                titulo, plataforma, 
                preco_aluguel, preco_aluguel_14, 
                preco_secundaria, preco_secundaria_14,
                descricao, url_imagem, tempo_jogo, nota, data_lancamento, 
                recomendacao_cliente
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """
        cursor.execute(
            query,
            (
                jogo.titulo,
                jogo.plataforma,
                jogo.preco_aluguel,
                jogo.preco_aluguel_14,
                jogo.preco_secundaria,  # NOVO: Preço Vaga Secundária 7D
                jogo.preco_secundaria_14,  # NOVO: Preço Vaga Secundária 14D
                jogo.descricao,
                jogo.url_imagem,
                jogo.tempo_jogo,
                jogo.nota,
                jogo.data_lancamento,
                jogo.recomendacao_cliente,
            ),
        )
        conn.commit()
        return {"mensagem": "Jogo adicionado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=400, detail=f"Erro ao cadastrar o jogo: {str(e)}"
        )
    finally:
        cursor.close()
        conn.close()


@router.put("/jogos/{jogo_id}")
def editar_jogo_completo(
    jogo_id: int, dados: EditarJogoRequest, admin_data=Depends(verificar_admin)
):
    """
    [U] Atualiza todas as informações de um jogo existente.
    Corrige o problema de edição invisível, garantindo que os novos preços secundários
    possam ser alterados posteriormente pelo Admin.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            UPDATE jogos SET 
                titulo = %s, plataforma = %s, 
                preco_aluguel = %s, preco_aluguel_14 = %s, 
                preco_secundaria = %s, preco_secundaria_14 = %s,
                descricao = %s, url_imagem = %s, tempo_jogo = %s, 
                nota = %s, data_lancamento = %s 
            WHERE id = %s
        """
        cursor.execute(
            query,
            (
                dados.titulo,
                dados.plataforma,
                dados.preco_aluguel,
                dados.preco_aluguel_14,
                dados.preco_secundaria,  # NOVO
                dados.preco_secundaria_14,  # NOVO
                dados.descricao,
                dados.url_imagem,
                dados.tempo_jogo,
                dados.nota,
                dados.data_lancamento,
                jogo_id,
            ),
        )
        if cursor.rowcount == 0:
            raise HTTPException(
                status_code=404, detail="Jogo não encontrado no catálogo."
            )
        conn.commit()
        return {"mensagem": "Jogo atualizado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=400, detail="Erro ao atualizar as informações do jogo."
        )
    finally:
        cursor.close()
        conn.close()


@router.delete("/jogos/{jogo_id}")
def deletar_jogo(jogo_id: int, admin_data=Depends(verificar_admin)):
    """
    [D] Remove um jogo da vitrine.
    Nota: Vai falhar (Proteção de Chave Estrangeira) se houverem contas ou locações
    vinculadas a este jogo, o que é o comportamento esperado para integridade do DB.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM jogos WHERE id = %s", (jogo_id,))
        conn.commit()
        return {"mensagem": "Jogo removido com sucesso"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Não é possível remover este jogo pois existem contas/locações atreladas a ele.",
        )
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# ENQUETE DE LANÇAMENTOS E VOTAÇÃO
# ==============================================================================


@router.get("/enquete")
def buscar_enquete(usuario_id: int = 0):
    """
    [R] Retorna as opções ativas da enquete e o número de votos de cada uma.
    Se um usuario_id for passado (cliente logado), retorna em qual jogo ele votou
    para o frontend marcar a opção visualmente.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT o.id, o.titulo, o.url_imagem, (SELECT COUNT(*) FROM enquete_votos v WHERE v.opcao_id = o.id) as total_votos FROM enquete_opcoes o ORDER BY o.id ASC"
    )
    opcoes = cursor.fetchall()

    voto_usuario = None
    if usuario_id > 0:
        cursor.execute(
            "SELECT opcao_id FROM enquete_votos WHERE utilizador_id = %s", (usuario_id,)
        )
        voto = cursor.fetchone()
        if voto:
            voto_usuario = voto["opcao_id"]

    cursor.close()
    conn.close()
    return {"opcoes": opcoes, "voto_usuario": voto_usuario}


@router.post("/enquete/votar")
def votar_enquete(voto: VotoEnquete):
    """
    [C/U] Computa o voto do cliente.
    O UPSERT (ON CONFLICT DO UPDATE) garante que se o cliente votar de novo,
    ele altera o voto antigo ao invés de computar dois votos para o mesmo cliente.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO enquete_votos (utilizador_id, opcao_id) VALUES (%s, %s) ON CONFLICT (utilizador_id) DO UPDATE SET opcao_id = EXCLUDED.opcao_id;",
            (voto.utilizador_id, voto.opcao_id),
        )
        conn.commit()
        return {"mensagem": "Voto registrado com sucesso!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/admin/enquete", status_code=201)
def adicionar_opcao_enquete(
    opcao: NovaOpcaoEnquete, admin_data=Depends(verificar_admin)
):
    """
    [C] Adiciona um novo candidato à prateleira de votações.
    A URL da imagem agora é preenchida automaticamente pela busca da API RAWG no frontend.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO enquete_opcoes (titulo, url_imagem) VALUES (%s, %s)",
            (opcao.titulo, opcao.url_imagem),
        )
        conn.commit()
        return {"mensagem": "Opção adicionada à enquete!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.delete("/admin/enquete/{opcao_id}")
def remover_opcao_enquete(opcao_id: int, admin_data=Depends(verificar_admin)):
    """[D] Remove uma opção específica da enquete (Painel Admin)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM enquete_opcoes WHERE id = %s", (opcao_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"mensagem": "Opção removida da enquete."}


@router.delete("/admin/enquete")
def limpar_enquete_completa(admin_data=Depends(verificar_admin)):
    """
    [D] O Botão de Reset (Painel Admin).
    Apaga todas as opções e, por cascata (no banco), apaga todos os votos computados,
    preparando o terreno para o mês seguinte.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM enquete_opcoes")
    conn.commit()
    cursor.close()
    conn.close()
    return {"mensagem": "Enquete reiniciada com sucesso."}
