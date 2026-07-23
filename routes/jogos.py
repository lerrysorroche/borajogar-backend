from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
from database import get_db_connection
from auth import verificar_admin
from models import JogoNovo, EditarJogoRequest, VotoEnquete, NovaOpcaoEnquete
import textwrap
import io
import os
import urllib.request
import httpx
from PIL import Image, ImageDraw, ImageFont, ImageEnhance, ImageFilter
from fastapi import Response, Query

router = APIRouter(tags=["Jogos"])


def get_font(size):
    font_path = "Roboto-Black.ttf"
    if not os.path.exists(font_path):
        url = (
            "https://github.com/googlefonts/roboto/raw/main/src/hinted/Roboto-Black.ttf"
        )
        urllib.request.urlretrieve(url, font_path)

    try:
        return ImageFont.truetype(font_path, size)
    except IOError:
        return ImageFont.load_default()


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


@router.get("/flyer")
async def gerar_flyer(
    titulo: str = Query("Lançamento Disponível"),
    bg: str = Query("https://via.placeholder.com/1080x1080"),
):
    try:
        # 1. Baixar a imagem original
        async with httpx.AsyncClient() as client:
            resposta = await client.get(bg)
            fundo = Image.open(io.BytesIO(resposta.content)).convert("RGBA")

        # 2. Cortar a imagem para um Quadrado (1080x1080)
        largura, altura = fundo.size
        tamanho_menor = min(largura, altura)
        esquerda = (largura - tamanho_menor) / 2
        topo = (altura - tamanho_menor) / 2
        direita = (largura + tamanho_menor) / 2
        fundo = fundo.crop((esquerda, topo, direita, altura))
        fundo = fundo.resize((1080, 1080))

        # 3. Máscara Cyberpunk (Gradiente de cima pra baixo, bem escuro no fundo)
        mascara = Image.new("RGBA", (1080, 1080), (0, 0, 0, 0))
        draw_mascara = ImageDraw.Draw(mascara)
        for y in range(1080):
            if y > 100:
                opacidade = int(((y - 100) / 980) * 240)
                # Uma leve tonalidade de roxo/azul escuro no escurecimento
                draw_mascara.line([(0, y), (1080, y)], fill=(8, 4, 16, opacidade))

        imagem_final = Image.alpha_composite(fundo, mascara)
        draw = ImageDraw.Draw(imagem_final)

        # 4. Preparar as fontes (usando a função auxiliar que já criamos)
        fonte_titulo = get_font(84)
        fonte_tag = get_font(36)
        fonte_btn = get_font(44)

        # Centro do Flyer
        cx = 540

        # --- BOTÃO "GARANTA A SUA VEZ" (Cyberpunk Cyan) ---
        texto_btn = "GARANTA A SUA VEZ"
        cy_btn = 980  # Altura fixada perto do rodapé

        # 'anchor="mm"' centraliza perfeitamente o texto, matando o bug do alinhamento!
        bbox_btn = draw.textbbox((cx, cy_btn), texto_btn, font=fonte_btn, anchor="mm")
        pad_x, pad_y = 60, 26
        btn_rect = [
            bbox_btn[0] - pad_x,
            bbox_btn[1] - pad_y,
            bbox_btn[2] + pad_x,
            bbox_btn[3] + pad_y,
        ]

        draw.rounded_rectangle(btn_rect, radius=8, fill="#00f3ff")  # Fundo Ciano Neon
        draw.text(
            (cx, cy_btn), texto_btn, font=fonte_btn, fill="#000000", anchor="mm"
        )  # Texto Preto

        # --- TÍTULO DO JOGO (Com Efeito Glitch) ---
        titulo_upper = titulo.upper()

        # Descobrir a altura exata do título para posicionar logo acima do botão
        bbox_title_temp = draw.textbbox(
            (cx, 0), titulo_upper, font=fonte_titulo, anchor="mm"
        )
        title_h = bbox_title_temp[3] - bbox_title_temp[1]

        cy_title = btn_rect[1] - (title_h / 2) - 50  # 50px de margem acima do botão

        # Efeito Glitch/Aberração Cromática (Sombras deslocadas)
        draw.text(
            (cx - 5, cy_title),
            titulo_upper,
            font=fonte_titulo,
            fill="#00f3ff",
            anchor="mm",
        )  # Sombra Ciano na esquerda
        draw.text(
            (cx + 5, cy_title),
            titulo_upper,
            font=fonte_titulo,
            fill="#ff003c",
            anchor="mm",
        )  # Sombra Rosa na direita
        draw.text(
            (cx, cy_title), titulo_upper, font=fonte_titulo, fill="#ffffff", anchor="mm"
        )  # Texto Principal Branco

        # Pega a caixa real do título gerado
        bbox_title = draw.textbbox(
            (cx, cy_title), titulo_upper, font=fonte_titulo, anchor="mm"
        )

        # --- TAG "BORA JOGAR" (Neon Pink) ---
        texto_tag = "BORA JOGAR"

        # Descobrir a altura exata da tag
        bbox_tag_temp = draw.textbbox((cx, 0), texto_tag, font=fonte_tag, anchor="mm")
        tag_h = bbox_tag_temp[3] - bbox_tag_temp[1]

        cy_tag = bbox_title[1] - (tag_h / 2) - 60  # 60px acima do título

        bbox_tag = draw.textbbox((cx, cy_tag), texto_tag, font=fonte_tag, anchor="mm")
        pad_xt, pad_yt = 30, 14
        tag_rect = [
            bbox_tag[0] - pad_xt,
            bbox_tag[1] - pad_yt,
            bbox_tag[2] + pad_xt,
            bbox_tag[3] + pad_yt,
        ]

        # Fundo escuro com borda Neon Rosa
        draw.rounded_rectangle(
            tag_rect, radius=6, outline="#ff003c", width=4, fill="#0a0a0a"
        )
        draw.text((cx, cy_tag), texto_tag, font=fonte_tag, fill="#ff003c", anchor="mm")

        # 6. Salvar e Retornar
        buffer = io.BytesIO()
        imagem_final.convert("RGB").save(buffer, format="PNG", quality=100)
        buffer.seek(0)

        return Response(content=buffer.getvalue(), media_type="image/png")

    except Exception as e:
        print(f"Erro ao gerar flyer: {e}")
        return Response(content="Erro ao gerar flyer", status_code=500)


@router.get("/flyer_nostalgia")
async def gerar_flyer_nostalgia(
    frase: str = Query("Bons tempos não voltam..."),
    bg: str = Query("https://via.placeholder.com/1080x1080"),
):
    try:
        # 1. Baixar a imagem original (vinda da RAWG)
        async with httpx.AsyncClient() as client:
            resposta = await client.get(bg)
            fundo = Image.open(io.BytesIO(resposta.content)).convert("RGBA")

        # 2. Cortar a imagem para Quadrado (1080x1080)
        largura, altura = fundo.size
        tamanho_menor = min(largura, altura)
        esquerda = (largura - tamanho_menor) / 2
        topo = (altura - tamanho_menor) / 2
        direita = (largura + tamanho_menor) / 2
        fundo = fundo.crop((esquerda, topo, direita, altura))
        fundo = fundo.resize((1080, 1080))

        # 3. Aplicar Filtro Nostálgico (Reduzir cor, escurecer bordas, tom quente)
        # 3a. Reduzir a saturação (deixar mais "antigo")
        enhancer = ImageEnhance.Color(fundo)
        fundo = enhancer.enhance(0.4)

        # 3b. Aumentar o contraste levemente
        enhancer_contrast = ImageEnhance.Contrast(fundo)
        fundo = enhancer_contrast.enhance(1.2)

        # 4. Máscara Escura / Vinheta (Para destacar o texto)
        mascara = Image.new("RGBA", (1080, 1080), (0, 0, 0, 0))
        draw_mascara = ImageDraw.Draw(mascara)

        # Cria um gradiente escuro em cima e embaixo
        for y in range(1080):
            # Escurece o topo
            if y < 300:
                opacidade_topo = int(((300 - y) / 300) * 220)
                draw_mascara.line(
                    [(0, y), (1080, y)], fill=(20, 10, 0, opacidade_topo)
                )  # Tom sépia escuro
            # Escurece a base
            elif y > 780:
                opacidade_base = int(((y - 780) / 300) * 220)
                draw_mascara.line([(0, y), (1080, y)], fill=(20, 10, 0, opacidade_base))

        imagem_final = Image.alpha_composite(fundo, mascara)
        draw = ImageDraw.Draw(imagem_final)

        # 5. Fontes
        fonte_frase = get_font(56)  # Fonte um pouco menor para frases mais longas
        fonte_tag = get_font(32)
        cx = 540

        # 6. Desenhando os elementos

        # --- Tag Superior ---
        texto_tag = "MÁQUINA DO TEMPO"
        bbox_tag = draw.textbbox((cx, 120), texto_tag, font=fonte_tag, anchor="mm")

        # Fundo da Tag (Sépia Quente)
        pad_x, pad_y = 24, 12
        draw.rounded_rectangle(
            [
                bbox_tag[0] - pad_x,
                bbox_tag[1] - pad_y,
                bbox_tag[2] + pad_x,
                bbox_tag[3] + pad_y,
            ],
            radius=8,
            fill="#d97706",
        )
        draw.text((cx, 120), texto_tag, font=fonte_tag, fill="#ffffff", anchor="mm")

        # --- Frase Nostálgica (Gemini) ---
        # Como a frase pode ser longa, precisamos quebrar a linha (wrap)
        import textwrap

        linhas_frase = textwrap.wrap(
            frase, width=28
        )  # Quebra o texto a cada ~28 caracteres

        altura_linha = 70
        inicio_y = 850 - (
            len(linhas_frase) * altura_linha
        )  # Sobe dependendo do tamanho da frase

        for i, linha in enumerate(linhas_frase):
            pos_y = inicio_y + (i * altura_linha)
            # Sombra da frase
            draw.text(
                (cx + 3, pos_y + 3),
                linha,
                font=fonte_frase,
                fill="#000000",
                anchor="mm",
            )
            # Texto principal (Amarelo envelhecido)
            draw.text((cx, pos_y), linha, font=fonte_frase, fill="#fef3c7", anchor="mm")

        # 7. Salvar e Retornar
        buffer = io.BytesIO()
        imagem_final.convert("RGB").save(buffer, format="PNG", quality=100)
        buffer.seek(0)

        return Response(content=buffer.getvalue(), media_type="image/png")

    except Exception as e:
        print(f"Erro ao gerar flyer nostalgia: {e}")
        return Response(content="Erro ao gerar flyer nostalgia", status_code=500)


@router.get("/flyer_beneficio")
async def gerar_flyer_beneficio(
    frase: str = Query("INDIQUE UM AMIGO E GANHE"),
    cta: str = Query("SAIBA MAIS NO SITE"),
):
    try:
        # Fundo temático com luzes Neon/Gamer abstratas (Unsplash)
        bg_url = "https://images.unsplash.com/photo-1542751371-adc38448a05e?q=80&w=1080&h=1080&auto=format&fit=crop"

        async with httpx.AsyncClient() as client:
            resposta = await client.get(bg_url)
            fundo = Image.open(io.BytesIO(resposta.content)).convert("RGBA")

        # Máscara escura forte
        mascara = Image.new(
            "RGBA", (1080, 1080), (0, 0, 0, 200)
        )  # Bem escuro para o texto ler bem
        imagem_final = Image.alpha_composite(fundo, mascara)
        draw = ImageDraw.Draw(imagem_final)

        # Fontes
        fonte_frase = get_font(68)
        fonte_cta = get_font(40)
        cx = 540

        # --- A Frase do Benefício (Gerada pelo Gemini) ---
        frase_upper = frase.upper()
        linhas_frase = textwrap.wrap(frase_upper, width=18)

        altura_linha = 90
        inicio_y = 540 - ((len(linhas_frase) * altura_linha) / 2) - 40

        for i, linha in enumerate(linhas_frase):
            pos_y = inicio_y + (i * altura_linha)
            # Texto principal em Magenta Neon
            draw.text(
                (cx, pos_y),
                linha,
                font=fonte_frase,
                fill="#ff003c",
                anchor="mm",
                stroke_width=2,
                stroke_fill="#ffffff",
            )

        # --- Botão Inferior Dinâmico ---
        cy_cta = 950

        # Fundo do botão (Azul/Ciano para contrastar)
        bbox_cta = draw.textbbox((cx, cy_cta), cta.upper(), font=fonte_cta, anchor="mm")
        pad_x, pad_y = 40, 20
        draw.rounded_rectangle(
            [
                bbox_cta[0] - pad_x,
                bbox_cta[1] - pad_y,
                bbox_cta[2] + pad_x,
                bbox_cta[3] + pad_y,
            ],
            radius=12,
            fill="#00f3ff",
        )
        draw.text(
            (cx, cy_cta), cta.upper(), font=fonte_cta, fill="#000000", anchor="mm"
        )

        # Logo / Tag superior
        texto_tag = "VANTAGEM BORA JOGAR"
        draw.text((cx, 120), texto_tag, font=get_font(28), fill="#e2e8f0", anchor="mm")

        # Salvar e Retornar
        buffer = io.BytesIO()
        imagem_final.convert("RGB").save(buffer, format="PNG", quality=100)
        buffer.seek(0)

        return Response(content=buffer.getvalue(), media_type="image/png")

    except Exception as e:
        print(f"Erro ao gerar flyer beneficio: {e}")
        return Response(content="Erro", status_code=500)
