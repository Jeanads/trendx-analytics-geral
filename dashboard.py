import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import numpy as np

# ========== CONFIGURAÇÃO ==========
DB_PATH = "trendx_bot.db"
CACHE_TTL = 300  # 5 minutos

st.set_page_config(
    page_title="TrendX Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== FUNÇÕES UTILITÁRIAS ==========
def formatar_numero(num):
    """Formata números para exibição (K, M, B)"""
    try:
        if pd.isna(num) or num == 0:
            return "0"
        num = float(num)
        if num >= 1_000_000_000:
            return f"{num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.1f}K"
        else:
            return f"{int(num):,}".replace(",", ".")
    except:
        return "0"

def formatar_numero_completo(num):
    """Formata números para exibição completa com separador de milhares"""
    try:
        if pd.isna(num) or num == 0:
            return "0"
        num = int(num)
        return f"{num:,}".replace(",", ".")
    except:
        return "0"

def formatar_data_br(data):
    """Formata data para padrão brasileiro DD/MM/YYYY"""
    try:
        if pd.isna(data):
            return ""
        if isinstance(data, str):
            return data
        return data.strftime('%d/%m/%Y')
    except:
        return ""

def formatar_data_hora_br(data):
    """Formata data e hora para padrão brasileiro DD/MM/YYYY HH:MM"""
    try:
        if pd.isna(data):
            return ""
        if isinstance(data, str):
            return data
        return data.strftime('%d/%m/%Y às %H:%M')
    except:
        return ""

def conectar_banco():
    """Conecta ao banco de dados"""
    if not os.path.exists(DB_PATH):
        st.error(f"⚠️ Banco não encontrado: {DB_PATH}")
        return None
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@st.cache_data(ttl=CACHE_TTL)
def obter_competicoes():
    """Busca todas as competições"""
    conn = conectar_banco()
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM competitions ORDER BY is_active DESC, id DESC",
            conn
        )
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao buscar competições: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=CACHE_TTL)
def obter_estatisticas_gerais(competition_id):
    """Busca estatísticas gerais da competição"""
    conn = conectar_banco()
    if not conn:
        return {}
    
    try:
        query_videos = """
        SELECT 
            COUNT(*) as total_videos,
            COALESCE(SUM(views), 0) as total_views,
            COALESCE(SUM(likes), 0) as total_likes,
            COALESCE(SUM(comments), 0) as total_comments
        FROM valid_videos
        WHERE competition_id = ?
        """
        
        df_videos = pd.read_sql_query(query_videos, conn, params=(competition_id,))
        
        query_usuarios = """
        SELECT COUNT(DISTINCT user_id) as total_usuarios
        FROM valid_videos
        WHERE competition_id = ?
        """
        
        df_usuarios = pd.read_sql_query(query_usuarios, conn, params=(competition_id,))
        
        # Contar contas REAIS que têm vídeos NESTA competição específica
        query_contas = """
        SELECT 
            LOWER(v.platform) as platform,
            COUNT(DISTINCT v.user_id) as total_contas
        FROM valid_videos v
        WHERE v.competition_id = ?
        AND v.user_id NOT LIKE 'demo_user_%'
        GROUP BY LOWER(v.platform)
        """
        
        df_contas = pd.read_sql_query(query_contas, conn, params=(competition_id,))
        
        conn.close()
        
        contas = {'tiktok': 0, 'youtube': 0, 'instagram': 0}
        for _, row in df_contas.iterrows():
            platform = str(row['platform']).lower()
            if platform in contas:
                contas[platform] = int(row['total_contas'])
        
        return {
            'videos_total': int(df_videos.iloc[0]['total_videos']) if not df_videos.empty else 0,
            'views_total': int(df_videos.iloc[0]['total_views']) if not df_videos.empty else 0,
            'likes_total': int(df_videos.iloc[0]['total_likes']) if not df_videos.empty else 0,
            'comments_total': int(df_videos.iloc[0]['total_comments']) if not df_videos.empty else 0,
            'usuarios_total': int(df_usuarios.iloc[0]['total_usuarios']) if not df_usuarios.empty else 0,
            'contas': contas
        }
    except Exception as e:
        st.error(f"Erro nas estatísticas: {e}")
        if conn:
            conn.close()
        return {}

@st.cache_data(ttl=CACHE_TTL)
def obter_estatisticas_globais():
    """Busca estatísticas agregadas de todas as competições"""
    conn = conectar_banco()
    if not conn:
        return {}
    
    try:
        # Estatísticas básicas dos vídeos
        query_global = """
        SELECT 
            COUNT(DISTINCT competition_id) as total_competicoes,
            COUNT(*) as total_videos,
            COALESCE(SUM(views), 0) as total_views,
            COALESCE(SUM(likes), 0) as total_likes,
            COALESCE(SUM(comments), 0) as total_comments,
            COALESCE(SUM(shares), 0) as total_shares
        FROM valid_videos
        """
        
        df_global = pd.read_sql_query(query_global, conn)
        
        # Contar TODOS os usuários (reais + fake) direto da tabela user_accounts
        query_usuarios = """
        SELECT COUNT(DISTINCT user_id) as total_usuarios
        FROM user_accounts
        """
        
        df_usuarios = pd.read_sql_query(query_usuarios, conn)
        
        # Adicionar contagem de usuários ao resultado global
        if not df_usuarios.empty:
            df_global['total_usuarios'] = df_usuarios.iloc[0]['total_usuarios']
        else:
            df_global['total_usuarios'] = 0
        
        # Dados por competição
        query_por_comp = """
        SELECT 
            c.id,
            c.name,
            c.is_active,
            COUNT(v.id) as videos,
            COALESCE(SUM(v.views), 0) as views,
            COALESCE(SUM(v.likes), 0) as likes,
            COALESCE(SUM(v.comments), 0) as comments,
            COUNT(DISTINCT v.user_id) as usuarios
        FROM competitions c
        LEFT JOIN valid_videos v ON c.id = v.competition_id
        GROUP BY c.id, c.name, c.is_active
        ORDER BY views DESC
        """
        
        df_por_comp = pd.read_sql_query(query_por_comp, conn)
        
        # Dados por plataforma (global)
        query_plataforma = """
        SELECT 
            LOWER(platform) as platform,
            COUNT(*) as videos,
            COALESCE(SUM(views), 0) as views,
            COALESCE(SUM(likes), 0) as likes,
            COALESCE(SUM(comments), 0) as comments
        FROM valid_videos
        GROUP BY LOWER(platform)
        """
        
        df_plataforma = pd.read_sql_query(query_plataforma, conn)
        
        conn.close()
        
        return {
            'global': df_global.iloc[0].to_dict() if not df_global.empty else {},
            'por_competicao': df_por_comp,
            'por_plataforma': df_plataforma
        }
    except Exception as e:
        st.error(f"Erro ao buscar estatísticas globais: {e}")
        if conn:
            conn.close()
        return {}

@st.cache_data(ttl=CACHE_TTL)
def carregar_dados_globais():
    """Carrega todos os vídeos de todas as competições"""
    conn = conectar_banco()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            v.*,
            COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username,
            c.name as competition_name
        FROM valid_videos v
        LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
        LEFT JOIN competitions c ON v.competition_id = c.id
        ORDER BY v.views DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            if 'published_timestamp' in df.columns:
                df['data_publicacao'] = pd.to_datetime(df['published_timestamp'], unit='s', errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados globais: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=CACHE_TTL)
def obter_intervalo_datas(competition_id):
    """Obtém o intervalo de datas da competição"""
    conn = conectar_banco()
    if not conn:
        return None, None
    
    try:
        query = """
        SELECT 
            MIN(published_timestamp) as primeira_data,
            MAX(published_timestamp) as ultima_data
        FROM valid_videos
        WHERE competition_id = ?
        """
        df = pd.read_sql_query(query, conn, params=(competition_id,))
        conn.close()
        
        if not df.empty and df.iloc[0]['primeira_data']:
            primeira = datetime.fromtimestamp(df.iloc[0]['primeira_data'])
            ultima = datetime.fromtimestamp(df.iloc[0]['ultima_data'])
            return primeira, ultima
        
        return None, None
    except:
        if conn:
            conn.close()
        return None, None

@st.cache_data(ttl=CACHE_TTL)
def carregar_ranking(competition_id, limit=100, data_inicio=None, data_fim=None):
    """Carrega ranking dos usuários com filtro opcional por data"""
    conn = conectar_banco()
    if not conn:
        return pd.DataFrame()
    
    try:
        if data_inicio and data_fim:
            # Converter datas para timestamp
            ts_inicio = int(data_inicio.timestamp())
            ts_fim = int(data_fim.timestamp())
            
            query = """
            SELECT 
                COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username,
                COUNT(v.id) as total_videos,
                COALESCE(SUM(v.views), 0) as total_views,
                COALESCE(SUM(v.likes), 0) as total_likes,
                COALESCE(SUM(v.comments), 0) as total_comments,
                COALESCE(SUM(v.shares), 0) as total_shares,
                COALESCE(SUM(CASE WHEN v.platform = 'tiktok' THEN v.views ELSE 0 END), 0) as tiktok_views,
                COALESCE(SUM(CASE WHEN v.platform = 'youtube' THEN v.views ELSE 0 END), 0) as youtube_views,
                COALESCE(SUM(CASE WHEN v.platform = 'instagram' THEN v.views ELSE 0 END), 0) as instagram_views
            FROM valid_videos v
            LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
            WHERE v.competition_id = ?
            AND v.published_timestamp >= ?
            AND v.published_timestamp <= ?
            GROUP BY u.discord_username
            HAVING total_views > 0
            ORDER BY total_views DESC
            LIMIT ?
            """
            
            df = pd.read_sql_query(query, conn, params=(competition_id, ts_inicio, ts_fim, limit))
        else:
            # Usar dados agregados da competição específica
            query = """
            SELECT 
                COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username,
                COUNT(v.id) as total_videos,
                COALESCE(SUM(v.views), 0) as total_views,
                COALESCE(SUM(v.likes), 0) as total_likes,
                COALESCE(SUM(v.comments), 0) as total_comments,
                COALESCE(SUM(v.shares), 0) as total_shares,
                COALESCE(SUM(CASE WHEN v.platform = 'tiktok' THEN v.views ELSE 0 END), 0) as tiktok_views,
                COALESCE(SUM(CASE WHEN v.platform = 'youtube' THEN v.views ELSE 0 END), 0) as youtube_views,
                COALESCE(SUM(CASE WHEN v.platform = 'instagram' THEN v.views ELSE 0 END), 0) as instagram_views
            FROM valid_videos v
            LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
            WHERE v.competition_id = ?
            GROUP BY u.discord_username
            HAVING total_views > 0
            ORDER BY total_views DESC
            LIMIT ?
            """
            
            df = pd.read_sql_query(query, conn, params=(competition_id, limit))
        
        conn.close()
        
        if not df.empty:
            df['total_interactions'] = df['total_likes'] + df['total_comments'] + df['total_shares']
            df['taxa_engajamento'] = ((df['total_interactions'] / df['total_views'].replace(0, 1)) * 100).round(2)
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar ranking: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=CACHE_TTL)
def carregar_videos(competition_id, limit=None):
    """Carrega vídeos da competição - TODOS os vídeos ordenados por views"""
    conn = conectar_banco()
    if not conn:
        return pd.DataFrame()
    
    try:
        if limit:
            query = """
            SELECT 
                v.*,
                COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username
            FROM valid_videos v
            LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
            WHERE v.competition_id = ?
            ORDER BY v.views DESC
            LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(competition_id, limit))
        else:
            query = """
            SELECT 
                v.*,
                COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username
            FROM valid_videos v
            LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
            WHERE v.competition_id = ?
            ORDER BY v.views DESC
            """
            df = pd.read_sql_query(query, conn, params=(competition_id,))
        
        conn.close()
        
        if not df.empty:
            if 'published_timestamp' in df.columns:
                df['data_publicacao'] = pd.to_datetime(df['published_timestamp'], unit='s', errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar vídeos: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=CACHE_TTL)
def carregar_videos_manuais(competition_id=None):
    """Carrega vídeos adicionados manualmente"""
    conn = conectar_banco()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Verificar se a coluna is_admin_added existe
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(valid_videos)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'is_admin_added' not in columns:
            conn.close()
            return pd.DataFrame()
        
        if competition_id:
            query = """
            SELECT 
                v.*,
                COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username,
                u.username as account_username,
                c.name as competition_name
            FROM valid_videos v
            LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
            LEFT JOIN competitions c ON v.competition_id = c.id
            WHERE v.is_admin_added = 1 AND v.competition_id = ?
            ORDER BY v.scraped_at DESC, v.views DESC
            """
            df = pd.read_sql_query(query, conn, params=(competition_id,))
        else:
            query = """
            SELECT 
                v.*,
                COALESCE(u.discord_username, 'Usuário Desconhecido') as discord_username,
                u.username as account_username,
                c.name as competition_name
            FROM valid_videos v
            LEFT JOIN user_accounts u ON v.user_id = u.user_id AND v.platform = u.platform
            LEFT JOIN competitions c ON v.competition_id = c.id
            WHERE v.is_admin_added = 1
            ORDER BY v.scraped_at DESC, v.views DESC
            """
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        
        if not df.empty:
            if 'published_timestamp' in df.columns:
                df['data_publicacao'] = pd.to_datetime(df['published_timestamp'], unit='s', errors='coerce')
            if 'scraped_at' in df.columns:
                df['data_adicao'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar vídeos manuais: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

# ========== CSS PERSONALIZADO ==========
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #e9ecef;
        box-shadow: 0 4px 16px rgba(0,0,0,0.08);
        margin: 0.5rem 0;
    }
    .metric-card h3 {
        color: #333 !important;
        margin: 0;
    }
    .metric-card h1 {
        color: #000 !important;
        margin: 0.5rem 0;
    }
    .metric-card p {
        color: #666 !important;
    }
    .platform-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        margin: 0.25rem;
    }
    .tiktok { background: #000000; color: white; }
    .youtube { background: #FF0000; color: white; }
    .instagram { background: #E4405F; color: white; }
    .date-filter-box {
        background: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #667eea;
        margin-bottom: 1.5rem;
    }
    .competition-selector {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ========== MAIN ==========
def main():
    st.markdown('''
    <div class="main-header">
        <h1>📊 TrendX Analytics </h1>
        <p>Dashboard Completo de Competições - Múltiplas Competições</p>
    </div>
    ''', unsafe_allow_html=True)
    
    if not os.path.exists(DB_PATH):
        st.error(f"⚠️ Banco de dados não encontrado: {DB_PATH}")
        st.info("💡 Certifique-se de que o bot está rodando e gerando o banco de dados.")
        st.stop()
    
    # Carregar todas as competições
    df_comp = obter_competicoes()
    
    if df_comp.empty:
        st.error("❌ Nenhuma competição encontrada!")
        st.info("💡 Crie uma competição usando o bot: `/criar_competicao`")
        st.stop()
    
    # ========== SIDEBAR - SELEÇÃO DE COMPETIÇÃO ==========
    st.sidebar.markdown('<div class="competition-selector">', unsafe_allow_html=True)
    st.sidebar.markdown("## 🏆 Selecionar Competição")
    st.sidebar.markdown('</div>', unsafe_allow_html=True)
    
    # Criar lista com todas as competições
    opcoes_comp = {}
    opcoes_comp_lista = []
    
    # Ordenar: ativas primeiro, depois por ID decrescente
    df_comp_ordenado = df_comp.sort_values(['is_active', 'id'], ascending=[False, False])
    
    for _, row in df_comp_ordenado.iterrows():
        status = "🟢 ATIVA" if row['is_active'] == 1 else "⏸️ Inativa"
        nome_display = f"{status} | {row['name']}"
        opcoes_comp[nome_display] = row['id']
        opcoes_comp_lista.append(nome_display)
    
    # Mostrar quantidade total de competições
    st.sidebar.info(f"📊 Total: **{len(df_comp)}** competições")
    
    # Selectbox para escolher competição
    comp_selecionada = st.sidebar.selectbox(
        "Escolha a competição:",
        opcoes_comp_lista,
        index=0,
        help="Selecione qual competição deseja visualizar"
    )
    
    comp_id = opcoes_comp[comp_selecionada]
    
    # Obter informações da competição selecionada
    comp_info = df_comp[df_comp['id'] == comp_id].iloc[0]
    comp_name = comp_info['name']
    
    # Exibir informações da competição
    st.sidebar.success(f"✅ Competição: **{comp_name}**")
    
    if pd.notna(comp_info.get('hashtags')) and comp_info['hashtags']:
        st.sidebar.info(f"🏷️ Hashtags: {comp_info['hashtags']}")
    
    st.sidebar.divider()
    
    # ========== NAVEGAÇÃO (MOVIDA PARA MAIS PRÓXIMA DO TOPO) ==========
    st.sidebar.markdown("## 🧭 Navegação")
    paginas = ["📊 Dashboard", "🏆 Rankings", "🎬 Vídeos", "📈 Análises", "🌐 Global", "📝 Vídeos Manuais"]
    pag = st.sidebar.radio("Página:", paginas, label_visibility="collapsed")
    
    st.sidebar.divider()
    
    # Botão atualizar
    if st.sidebar.button("🔄 Atualizar Dados"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.divider()
    
    # Mostrar resumo de todas as competições APENAS na página Global
    if pag == "🌐 Global":
        with st.sidebar.expander("📊 Todas as Competições", expanded=True):
            for _, comp in df_comp_ordenado.iterrows():
                status_icon = "🟢" if comp['is_active'] == 1 else "⏸️"
                st.markdown(f"{status_icon} **{comp['name']}** (ID: {comp['id']})")
                
                # Contar vídeos desta competição
                conn = conectar_banco()
                if conn:
                    try:
                        query = "SELECT COUNT(*) as total FROM valid_videos WHERE competition_id = ?"
                        result = pd.read_sql_query(query, conn, params=(comp['id'],))
                        total = result.iloc[0]['total'] if not result.empty else 0
                        st.caption(f"   🔹 {formatar_numero_completo(total)} vídeos")
                        conn.close()
                    except:
                        if conn:
                            conn.close()
        
        st.sidebar.divider()
    
    # Informações do sistema
    st.sidebar.markdown("## ℹ️ Sistema")
    st.sidebar.markdown(f"**Cache:** {CACHE_TTL//60} min")
    if os.path.exists(DB_PATH):
        tam = os.path.getsize(DB_PATH) / (1024 * 1024)
        st.sidebar.metric("💾 Banco", f"{tam:.1f} MB")
    
    st.sidebar.metric("🏆 Competições", len(df_comp))
    
    # Renderizar página selecionada
    if pag == "📊 Dashboard":
        pagina_dashboard(comp_id, comp_name)
    elif pag == "🏆 Rankings":
        pagina_rankings(comp_id, comp_name)
    elif pag == "🎬 Vídeos":
        pagina_videos(comp_id, comp_name)
    elif pag == "📈 Análises":
        pagina_analises(comp_id, comp_name)
    elif pag == "🌐 Global":
        pagina_global()
    elif pag == "📝 Vídeos Manuais":
        pagina_videos_manuais(comp_id, comp_name)
    
    # Rodapé
    st.sidebar.divider()
    st.sidebar.markdown(f"""
    <div style="text-align: center; padding: 1rem; color: #666; font-size: 0.8em;">
        <strong>🚀 TrendX Analytics </strong><br>
        📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}<br>
        🏆 {comp_name}<br>
        🆔 Competição ID: {comp_id}
    </div>
    """, unsafe_allow_html=True)

# ========== PÁGINAS ==========
def pagina_dashboard(comp_id, comp_name):
    """Página principal do dashboard"""
    st.header(f"📊 Dashboard - {comp_name}")
    
    stats = obter_estatisticas_gerais(comp_id)
    
    if not stats:
        st.warning("⚠️ Sem dados disponíveis")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🎥 Vídeos", formatar_numero_completo(stats['videos_total']))
    
    with col2:
        st.metric("👁️ Views", formatar_numero(stats['views_total']))
    
    with col3:
        st.metric("❤️ Likes", formatar_numero(stats['likes_total']))
    
    with col4:
        st.metric("💬 Comentários", formatar_numero(stats['comments_total']))
    
    st.divider()
    
    st.subheader("📈 Progresso da Competição")
    
    conn = conectar_banco()
    if conn:
        try:
            query_datas = """
            SELECT 
                MIN(published_timestamp) as primeira_data,
                MAX(published_timestamp) as ultima_data,
                COUNT(*) as total_videos
            FROM valid_videos
            WHERE competition_id = ?
            """
            df_datas = pd.read_sql_query(query_datas, conn, params=(comp_id,))
            
            if not df_datas.empty and df_datas.iloc[0]['primeira_data']:
                primeira = datetime.fromtimestamp(df_datas.iloc[0]['primeira_data'])
                ultima = datetime.fromtimestamp(df_datas.iloc[0]['ultima_data'])
                total_dias = (ultima - primeira).days + 1
                videos_total = df_datas.iloc[0]['total_videos']
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("📅 Duração", f"{total_dias} dias")
                
                with col2:
                    media_dia = videos_total / max(total_dias, 1)
                    st.metric("📊 Média Diária", f"{media_dia:.1f} vídeos")
                
                with col3:
                    st.metric("🎬 Total Postado", formatar_numero_completo(videos_total))
                
                with col4:
                    st.metric("⚡ Último Registro", formatar_data_hora_br(ultima))
                
                progresso = min(100, (total_dias / 30) * 100)
                st.progress(progresso / 100)
                st.caption(f"⏱️ Competição em andamento há {total_dias} dias")
            
            conn.close()
        except Exception as e:
            if conn:
                conn.close()
    
    st.divider()
    
    st.subheader("📱 Contas por Plataforma")
    
    col1, col2, col3 = st.columns(3)
    
    contas = stats['contas']
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🎵 TikTok</h3>
            <h1 style="font-size: 3em; margin: 0.5rem 0; color: #000 !important;">{contas['tiktok']}</h1>
            <p style="color: #666 !important;">contas</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📸 Instagram</h3>
            <h1 style="font-size: 3em; margin: 0.5rem 0; color: #000 !important;">{contas['instagram']}</h1>
            <p style="color: #666 !important;">contas</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🎬 YouTube</h3>
            <h1 style="font-size: 3em; margin: 0.5rem 0; color: #000 !important;">{contas['youtube']}</h1>
            <p style="color: #666 !important;">contas</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    total_contas = sum(contas.values())
    if total_contas > 0:
        plataformas_ordenadas = ['tiktok', 'instagram', 'youtube']
        valores_ordenados = [contas[p] for p in plataformas_ordenadas]
        nomes_ordenados = ['TikTok', 'Instagram', 'YouTube']
        
        fig = px.pie(
            values=valores_ordenados,
            names=nomes_ordenados,
            title="Distribuição de Contas por Plataforma",
            color=nomes_ordenados,
            color_discrete_map={'TikTok': '#000000', 'Instagram': '#E4405F', 'YouTube': '#FF0000'}
        )
        fig.update_traces(textposition='inside', textinfo='percent+label+value')
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

def pagina_rankings(comp_id, comp_name):
    """Página de rankings com filtro por data"""
    st.header(f"🏆 Rankings - {comp_name}")
    
    # Obter intervalo de datas da competição
    primeira_data, ultima_data = obter_intervalo_datas(comp_id)
    
    # Filtros de Data
    st.markdown('<div class="date-filter-box">', unsafe_allow_html=True)
    st.subheader("📅 Filtrar Rankings por Período")
    
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        tipo_filtro = st.selectbox(
            "Tipo de Filtro:",
            ["Todo o Período", "Última Semana", "Último Mês", "Últimos 3 Meses", "Período Personalizado"]
        )
    
    data_inicio = None
    data_fim = None
    
    if tipo_filtro == "Última Semana":
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=7)
    elif tipo_filtro == "Último Mês":
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=30)
    elif tipo_filtro == "Últimos 3 Meses":
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=90)
    elif tipo_filtro == "Período Personalizado":
        with col2:
            if primeira_data and ultima_data:
                data_inicio = st.date_input(
                    "Data Início:",
                    value=primeira_data.date(),
                    min_value=primeira_data.date(),
                    max_value=ultima_data.date()
                )
                data_inicio = datetime.combine(data_inicio, datetime.min.time())
        
        with col3:
            if primeira_data and ultima_data:
                data_fim = st.date_input(
                    "Data Fim:",
                    value=ultima_data.date(),
                    min_value=primeira_data.date(),
                    max_value=ultima_data.date()
                )
                data_fim = datetime.combine(data_fim, datetime.max.time())
    
    # Mostrar período selecionado
    if data_inicio and data_fim:
        st.info(f"📊 Exibindo rankings do período: **{formatar_data_br(data_inicio)}** até **{formatar_data_br(data_fim)}**")
    else:
        st.info("📊 Exibindo rankings de todo o período da competição")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Carregar ranking com filtro
    df = carregar_ranking(comp_id, limit=100, data_inicio=data_inicio, data_fim=data_fim)
    
    if df.empty:
        st.warning("⚠️ Nenhum dado de ranking disponível")
        return
    
    st.subheader("📊 Visão Geral")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("👁️ Total Views", formatar_numero(df['total_views'].sum()))
    
    with col2:
        st.metric("❤️ Total Likes", formatar_numero(df['total_likes'].sum()))
    
    with col3:
        st.metric("📈 Eng. Médio", f"{df['taxa_engajamento'].mean():.2f}%")
    
    st.divider()
    
    tab1, tab2, tab3, tab4 = st.tabs(["👁️ Mais Views", "❤️ Mais Curtidas", "📈 Maior Engajamento", "🎬 Ranking de Vídeos"])
    
    with tab1:
        st.subheader("🏆 Top 20 - Visualizações")
        top20 = df.nlargest(20, 'total_views')
        
        fig = px.bar(
            top20,
            x='total_views',
            y='discord_username',
            orientation='h',
            title="Top 20 por Visualizações",
            color='total_views',
            color_continuous_scale='Blues'
        )
        fig.update_yaxes(categoryorder='total ascending')
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            top20[['discord_username', 'total_views', 'total_videos', 'taxa_engajamento']].rename(columns={
                'discord_username': 'Usuário',
                'total_views': 'Views',
                'total_videos': 'Vídeos',
                'taxa_engajamento': 'Engajamento (%)'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.subheader("❤️ Top 20 - Curtidas")
        top20 = df.nlargest(20, 'total_likes')
        
        fig = px.bar(
            top20,
            x='total_likes',
            y='discord_username',
            orientation='h',
            title="Top 20 por Curtidas",
            color='total_likes',
            color_continuous_scale='Reds'
        )
        fig.update_yaxes(categoryorder='total ascending')
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            top20[['discord_username', 'total_likes', 'total_views', 'taxa_engajamento']].rename(columns={
                'discord_username': 'Usuário',
                'total_likes': 'Likes',
                'total_views': 'Views',
                'taxa_engajamento': 'Engajamento (%)'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    with tab3:
        st.subheader("📈 Top 20 - Engajamento")
        df_eng = df[df['total_views'] >= 100].copy()
        
        if not df_eng.empty:
            top20 = df_eng.nlargest(20, 'taxa_engajamento')
            
            fig = px.bar(
                top20,
                x='taxa_engajamento',
                y='discord_username',
                orientation='h',
                title="Top 20 por Taxa de Engajamento",
                color='taxa_engajamento',
                color_continuous_scale='Greens'
            )
            fig.update_yaxes(categoryorder='total ascending')
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                top20[['discord_username', 'taxa_engajamento', 'total_views', 'total_interactions']].rename(columns={
                    'discord_username': 'Usuário',
                    'taxa_engajamento': 'Engajamento (%)',
                    'total_views': 'Views',
                    'total_interactions': 'Interações'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("ℹ️ Dados insuficientes para engajamento (mínimo 100 views)")
    
    with tab4:
        st.subheader("🎬 Top 20 - Mais Vídeos Postados")
        
        top20_videos = df.nlargest(20, 'total_videos')
        
        fig = px.bar(
            top20_videos,
            x='total_videos',
            y='discord_username',
            orientation='h',
            title="Top 20 por Quantidade de Vídeos",
            color='total_videos',
            color_continuous_scale='Purples'
        )
        fig.update_yaxes(categoryorder='total ascending')
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            top20_videos[['discord_username', 'total_videos', 'total_views', 'total_likes']].rename(columns={
                'discord_username': 'Usuário',
                'total_videos': 'Vídeos',
                'total_views': 'Views',
                'total_likes': 'Likes'
            }),
            use_container_width=True,
            hide_index=True
        )

def pagina_videos(comp_id, comp_name):
    """Página de vídeos - MOSTRA TODOS OS VÍDEOS COM LINKS"""
    st.header(f"🎬 Todos os Vídeos - {comp_name}")
    
    df = carregar_videos(comp_id, limit=None)
    
    if df.empty:
        st.warning("⚠️ Nenhum vídeo disponível")
        return
    
    st.subheader("📊 Estatísticas Gerais")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("🎬 Total de Vídeos", formatar_numero_completo(len(df)))
    
    with col2:
        videos_com_link = len(df[df['url'].notna() & (df['url'] != '')])
        st.metric("🔗 Com Links", formatar_numero_completo(videos_com_link))
    
    with col3:
        st.metric("👁️ Views Totais", formatar_numero(df['views'].sum()))
    
    with col4:
        st.metric("❤️ Likes Totais", formatar_numero(df['likes'].sum()))
    
    with col5:
        if len(df) > 0:
            avg_views = df['views'].mean()
            st.metric("📊 Média Views", formatar_numero(avg_views))
    
    st.divider()
    
    st.subheader("🔍 Filtros")
    
    col1, col2 = st.columns(2)
    
    with col1:
        plataformas = ['Todas'] + sorted(df['platform'].dropna().unique().tolist())
        plat_filter = st.selectbox("📱 Plataforma:", plataformas)
    
    with col2:
        tipo_filtro = st.selectbox("🎯 Tipo:", ["Todos", "Apenas com Links", "Virais"])
    
    df_filtered = df.copy()
    
    if plat_filter != 'Todas':
        df_filtered = df_filtered[df_filtered['platform'] == plat_filter]
    
    if tipo_filtro == "Apenas com Links":
        df_filtered = df_filtered[df_filtered['url'].notna() & (df_filtered['url'] != '')]
        st.info(f"🔗 Exibindo apenas vídeos com links disponíveis")
    elif tipo_filtro == "Virais":
        if len(df) > 20:
            q75 = df['views'].quantile(0.75)
            q25 = df['views'].quantile(0.25)
            iqr = q75 - q25
            limite_viral = q75 + (1.5 * iqr)
        else:
            limite_viral = df['views'].quantile(0.9)
        df_filtered = df_filtered[df_filtered['views'] >= limite_viral]
        st.info(f"🔥 Exibindo apenas vídeos virais (views ≥ {formatar_numero(limite_viral)})")
    
    st.subheader("📊 Estatísticas dos Vídeos Filtrados")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🎬 Vídeos Filtrados", formatar_numero_completo(len(df_filtered)))
    
    with col2:
        videos_com_link_filtrados = len(df_filtered[df_filtered['url'].notna() & (df_filtered['url'] != '')])
        perc_links = (videos_com_link_filtrados / len(df_filtered) * 100) if len(df_filtered) > 0 else 0
        st.metric("🔗 Com Links", f"{formatar_numero_completo(videos_com_link_filtrados)} ({perc_links:.1f}%)")
    
    with col3:
        st.metric("👁️ Views Totais", formatar_numero(df_filtered['views'].sum()))
    
    st.divider()
    
    st.subheader("💾 Exportar Dados")
    
    df_export = df_filtered[['discord_username', 'platform', 'title', 'url', 'views', 'likes', 'comments']].copy()
    df_export.columns = ['Criador', 'Plataforma', 'Título', 'Link', 'Views', 'Likes', 'Comentários']
    
    csv = df_export.to_csv(index=False, encoding='utf-8-sig')
    
    st.download_button(
        label="💾 Baixar CSV com Todos os Links",
        data=csv,
        file_name=f"videos_{comp_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        help="Baixa um arquivo CSV com todos os vídeos e seus links"
    )
    
    st.divider()
    
    st.subheader(f"📋 Lista Completa de Vídeos ({len(df_filtered)} vídeos)")
    
    videos_por_pagina = st.selectbox("Vídeos por página:", [25, 50, 100, 200], index=1)
    
    total_paginas = (len(df_filtered) - 1) // videos_por_pagina + 1
    pagina_atual = st.number_input("Página:", min_value=1, max_value=total_paginas, value=1)
    
    inicio = (pagina_atual - 1) * videos_por_pagina
    fim = min(inicio + videos_por_pagina, len(df_filtered))
    
    st.info(f"📄 Mostrando vídeos {inicio + 1} a {fim} de {len(df_filtered)}")
    
    df_pagina = df_filtered.iloc[inicio:fim]
    
    for idx, (_, row) in enumerate(df_pagina.iterrows(), start=inicio+1):
        with st.expander(f"#{idx} - 👁️ {formatar_numero_completo(row.get('views', 0))} views - {row.get('title', 'Sem título')[:80]}..."):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"**👤 Criador:** {row.get('discord_username', 'Desconhecido')}")
                st.markdown(f"**📱 Plataforma:** {row.get('platform', 'N/A').upper()}")
                st.markdown(f"**📝 Título:** {row.get('title', 'Sem título')}")
                
                if pd.notna(row.get('data_publicacao')):
                    st.markdown(f"**📅 Publicado:** {formatar_data_hora_br(row['data_publicacao'])}")
                
                if pd.notna(row.get('url')) and row['url'] != '':
                    st.markdown(f"""
                    <a href="{row['url']}" target="_blank">
                        <button style="
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white;
                            padding: 0.5rem 1.5rem;
                            border: none;
                            border-radius: 8px;
                            cursor: pointer;
                            font-weight: bold;
                            font-size: 1em;
                            margin-top: 0.5rem;
                        ">
                            🎬 Assistir Vídeo
                        </button>
                    </a>
                    """, unsafe_allow_html=True)
                else:
                    st.warning("🔗 Link não disponível para este vídeo")
            
            with col2:
                st.metric("👁️ Views", formatar_numero_completo(row.get('views', 0)))
                st.metric("❤️ Likes", formatar_numero_completo(row.get('likes', 0)))
                st.metric("💬 Comentários", formatar_numero_completo(row.get('comments', 0)))
                
                if row.get('views', 0) > 0:
                    taxa = ((row.get('likes', 0) + row.get('comments', 0)) / row['views'] * 100)
                    st.metric("📊 Engajamento", f"{taxa:.2f}%")
    
    if total_paginas > 1:
        st.divider()
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if pagina_atual > 1:
                if st.button("⬅️ Página Anterior"):
                    st.rerun()
        
        with col2:
            st.markdown(f"<div style='text-align: center;'><strong>Página {pagina_atual} de {total_paginas}</strong></div>", unsafe_allow_html=True)
        
        with col3:
            if pagina_atual < total_paginas:
                if st.button("Próxima Página ➡️"):
                    st.rerun()

def pagina_analises(comp_id, comp_name):
    """Página de análises - COMPLETA COM TODOS OS GRÁFICOS"""
    st.header(f"📈 Análises - {comp_name}")
    
    df_videos = carregar_videos(comp_id, limit=None)
    df_ranking = carregar_ranking(comp_id, limit=1000)
    
    if df_videos.empty and df_ranking.empty:
        st.warning("⚠️ Sem dados para análise")
        return
    
    # Análise temporal
    if not df_videos.empty and 'data_publicacao' in df_videos.columns:
        st.subheader("📅 Evolução de Publicações")
        
        df_temp = df_videos.dropna(subset=['data_publicacao']).copy()
        
        if not df_temp.empty:
            df_temp['data'] = df_temp['data_publicacao'].dt.date
            por_dia = df_temp.groupby('data').size().reset_index(name='videos')
            por_dia = por_dia.sort_values('data')
            
            por_dia['data_formatada'] = por_dia['data'].apply(lambda x: x.strftime('%d/%m/%Y'))
            
            fig = px.line(
                por_dia,
                x='data_formatada',
                y='videos',
                title="Vídeos Publicados por Dia",
                markers=True
            )
            fig.update_layout(height=400, xaxis_title="Data", yaxis_title="Quantidade de Vídeos")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            if len(por_dia) > 7:
                primeira_semana = por_dia.head(7)['videos'].mean()
                ultima_semana = por_dia.tail(7)['videos'].mean()
                variacao = ((ultima_semana - primeira_semana) / primeira_semana * 100) if primeira_semana > 0 else 0
                
                if variacao > 10:
                    st.success(f"📈 Tendência crescente! Aumento de {variacao:.1f}% nas publicações (última semana vs primeira semana)")
                elif variacao < -10:
                    st.warning(f"📉 Tendência decrescente. Redução de {abs(variacao):.1f}% nas publicações (última semana vs primeira semana)")
                else:
                    st.info(f"➡️ Publicações estáveis. Variação de {variacao:.1f}% (última semana vs primeira semana)")
    
    st.divider()
    
    # Performance por data
    if not df_videos.empty and 'data_publicacao' in df_videos.columns:
        st.subheader("📊 Performance por Data")
        
        df_perf_data = df_videos.dropna(subset=['data_publicacao']).copy()
        
        if not df_perf_data.empty:
            df_perf_data['data'] = df_perf_data['data_publicacao'].dt.date
            
            perf_por_dia = df_perf_data.groupby('data').agg({
                'views': ['count', 'sum', 'mean'],
                'likes': ['sum', 'mean'],
                'comments': 'sum'
            }).reset_index()
            
            perf_por_dia.columns = ['Data', 'Qtd Vídeos', 'Total Views', 'Média Views', 'Total Likes', 'Média Likes', 'Total Comentários']
            perf_por_dia['Data'] = perf_por_dia['Data'].apply(lambda x: x.strftime('%d/%m/%Y'))
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=perf_por_dia['Data'],
                    y=perf_por_dia['Total Views'],
                    name='Views',
                    marker_color='#667eea'
                ))
                fig.update_layout(
                    title="Views Totais por Data",
                    xaxis_title="Data",
                    yaxis_title="Views",
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            with col2:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=perf_por_dia['Data'],
                    y=perf_por_dia['Total Likes'],
                    name='Likes',
                    marker_color='#f093fb'
                ))
                fig.update_layout(
                    title="Likes Totais por Data",
                    xaxis_title="Data",
                    yaxis_title="Likes",
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            st.markdown("### 🏆 Top 10 Melhores Dias")
            
            top_dias = perf_por_dia.nlargest(10, 'Total Views').copy()
            top_dias['Total Views'] = top_dias['Total Views'].apply(formatar_numero)
            top_dias['Média Views'] = top_dias['Média Views'].apply(formatar_numero)
            top_dias['Total Likes'] = top_dias['Total Likes'].apply(formatar_numero)
            top_dias['Média Likes'] = top_dias['Média Likes'].apply(formatar_numero)
            top_dias['Total Comentários'] = top_dias['Total Comentários'].apply(formatar_numero)
            
            st.dataframe(top_dias[['Data', 'Qtd Vídeos', 'Total Views', 'Média Views', 'Total Likes']], use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Comparação de dias da semana
    if not df_videos.empty and 'data_publicacao' in df_videos.columns:
        st.subheader("📆 Performance por Dia da Semana")
        
        df_dia_semana = df_videos.dropna(subset=['data_publicacao']).copy()
        
        if not df_dia_semana.empty:
            df_dia_semana['dia_semana'] = df_dia_semana['data_publicacao'].dt.day_name()
            
            dias_pt = {
                'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta',
                'Thursday': 'Quinta', 'Friday': 'Sexta', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
            }
            df_dia_semana['dia_semana'] = df_dia_semana['dia_semana'].map(dias_pt)
            
            perf_semana = df_dia_semana.groupby('dia_semana').agg({
                'views': ['count', 'sum', 'mean'],
                'likes': ['sum', 'mean']
            }).reset_index()
            
            perf_semana.columns = ['Dia', 'Qtd Vídeos', 'Total Views', 'Média Views/Vídeo', 'Total Likes', 'Média Likes/Vídeo']
            
            ordem_dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
            perf_semana['Dia'] = pd.Categorical(perf_semana['Dia'], categories=ordem_dias, ordered=True)
            perf_semana = perf_semana.sort_values('Dia')
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    perf_semana,
                    x='Dia',
                    y='Qtd Vídeos',
                    title="Quantidade de Vídeos por Dia da Semana",
                    color='Qtd Vídeos',
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            with col2:
                fig = px.bar(
                    perf_semana,
                    x='Dia',
                    y='Média Views/Vídeo',
                    title="Média de Views por Vídeo (por Dia da Semana)",
                    color='Média Views/Vídeo',
                    color_continuous_scale='Plasma'
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            melhor_dia_qtd = perf_semana.loc[perf_semana['Qtd Vídeos'].idxmax(), 'Dia']
            melhor_dia_views = perf_semana.loc[perf_semana['Média Views/Vídeo'].idxmax(), 'Dia']
            
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"📅 **{melhor_dia_qtd}** é o dia com mais publicações")
            with col2:
                st.success(f"🎯 **{melhor_dia_views}** é o dia com melhor performance média")
    
    st.divider()
    
    # Distribuição por plataforma
    if not df_videos.empty:
        st.subheader("📱 Análise por Plataforma")
        
        col1, col2 = st.columns(2)
        
        with col1:
            dist_videos = df_videos['platform'].value_counts().reset_index()
            dist_videos.columns = ['platform', 'count']
            
            fig = px.pie(
                dist_videos,
                values='count',
                names='platform',
                title="Distribuição de Vídeos",
                color='platform',
                color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        with col2:
            dist_views = df_videos.groupby('platform')['views'].sum().reset_index()
            
            fig = px.pie(
                dist_views,
                values='views',
                names='platform',
                title="Distribuição de Views",
                color='platform',
                color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    st.divider()
    
    # Performance por plataforma
    if not df_videos.empty:
        st.subheader("⚡ Performance por Plataforma")
        
        perf = df_videos.groupby('platform').agg({
            'views': ['sum', 'mean'],
            'likes': ['sum', 'mean'],
            'platform': 'count'
        }).round(0)
        
        perf.columns = ['Total Views', 'Média Views', 'Total Likes', 'Média Likes', 'Total Vídeos']
        perf = perf.reset_index()
        
        for col in ['Total Views', 'Média Views', 'Total Likes', 'Média Likes']:
            perf[col] = perf[col].apply(formatar_numero)
        
        st.dataframe(perf, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Top performers
    if not df_videos.empty:
        st.subheader("🌟 Vídeos Mais Performáticos")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 👁️ Top 10 Views")
            top_views = df_videos.nlargest(10, 'views')[['discord_username', 'platform', 'views', 'title']]
            top_views['views'] = top_views['views'].apply(formatar_numero)
            top_views['title'] = top_views['title'].apply(lambda x: str(x)[:40] + '...' if pd.notna(x) and len(str(x)) > 40 else x)
            st.dataframe(
                top_views.rename(columns={
                    'discord_username': 'Usuário',
                    'platform': 'Plataforma',
                    'views': 'Views',
                    'title': 'Título'
                }),
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            st.markdown("### ❤️ Top 10 Likes")
            top_likes = df_videos.nlargest(10, 'likes')[['discord_username', 'platform', 'likes', 'title']]
            top_likes['likes'] = top_likes['likes'].apply(formatar_numero)
            top_likes['title'] = top_likes['title'].apply(lambda x: str(x)[:40] + '...' if pd.notna(x) and len(str(x)) > 40 else x)
            st.dataframe(
                top_likes.rename(columns={
                    'discord_username': 'Usuário',
                    'platform': 'Plataforma',
                    'likes': 'Likes',
                    'title': 'Título'
                }),
                use_container_width=True,
                hide_index=True
            )

def pagina_global():
    """Página de análises GLOBAIS de todas as competições"""
    st.header("🌐 Análise Global - Todas as Competições")
    
    stats = obter_estatisticas_globais()
    
    if not stats or not stats.get('global'):
        st.warning("⚠️ Sem dados globais disponíveis")
        return
    
    global_data = stats['global']
    df_por_comp = stats.get('por_competicao', pd.DataFrame())
    df_plataforma = stats.get('por_plataforma', pd.DataFrame())
    
    # Métricas Globais
    st.subheader("📊 Visão Geral do Sistema")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("🏆 Competições", formatar_numero_completo(global_data.get('total_competicoes', 0)))
    
    with col2:
        st.metric("🎥 Vídeos", formatar_numero_completo(global_data.get('total_videos', 0)))
    
    with col3:
        st.metric("👁️ Views", formatar_numero(global_data.get('total_views', 0)))
    
    with col4:
        st.metric("❤️ Likes", formatar_numero(global_data.get('total_likes', 0)))
    
    with col5:
        st.metric("👥 Usuários", formatar_numero_completo(global_data.get('total_usuarios', 0)))
    
    st.divider()
    
    # Comparação entre competições
    if not df_por_comp.empty:
        st.subheader("🏆 Performance por Competição")
        
        # Gráfico de barras - Views por competição
        fig = px.bar(
            df_por_comp.sort_values('views', ascending=False),
            x='name',
            y='views',
            title="Total de Views por Competição",
            color='views',
            color_continuous_scale='Viridis',
            text='views'
        )
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(height=400, xaxis_title="Competição", yaxis_title="Views")
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de barras - Vídeos por competição
            fig = px.bar(
                df_por_comp.sort_values('videos', ascending=False),
                x='name',
                y='videos',
                title="Quantidade de Vídeos por Competição",
                color='videos',
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=350, xaxis_title="Competição", yaxis_title="Vídeos")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        with col2:
            # Gráfico de barras - Likes por competição
            fig = px.bar(
                df_por_comp.sort_values('likes', ascending=False),
                x='name',
                y='likes',
                title="Total de Likes por Competição",
                color='likes',
                color_continuous_scale='Reds'
            )
            fig.update_layout(height=350, xaxis_title="Competição", yaxis_title="Likes")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        st.divider()
        
        # Tabela comparativa
        st.subheader("📋 Tabela Comparativa de Competições")
        
        df_comp_table = df_por_comp.copy()
        df_comp_table['status'] = df_comp_table['is_active'].apply(lambda x: '🟢 Ativa' if x == 1 else '⏸️ Inativa')
        df_comp_table['avg_views'] = (df_comp_table['views'] / df_comp_table['videos']).fillna(0).round(0)
        df_comp_table['engagement'] = ((df_comp_table['likes'] + df_comp_table['comments']) / df_comp_table['views'] * 100).fillna(0).round(2)
        
        df_display = df_comp_table[['name', 'status', 'videos', 'views', 'likes', 'usuarios', 'avg_views', 'engagement']].copy()
        df_display.columns = ['Competição', 'Status', 'Vídeos', 'Views', 'Likes', 'Usuários', 'Média Views', 'Engajamento (%)']
        
        # Formatar números
        for col in ['Vídeos', 'Views', 'Likes', 'Usuários', 'Média Views']:
            df_display[col] = df_display[col].apply(formatar_numero)
        
        st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Análise por plataforma (global)
    if not df_plataforma.empty:
        st.subheader("📱 Performance Global por Plataforma")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Pizza - Distribuição de vídeos
            fig = px.pie(
                df_plataforma,
                values='videos',
                names='platform',
                title="Distribuição de Vídeos por Plataforma",
                color='platform',
                color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
            )
            fig.update_traces(textposition='inside', textinfo='percent+label+value')
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        with col2:
            # Pizza - Distribuição de views
            fig = px.pie(
                df_plataforma,
                values='views',
                names='platform',
                title="Distribuição de Views por Plataforma",
                color='platform',
                color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        # Tabela de performance por plataforma
        st.markdown("### 📊 Métricas por Plataforma")
        
        df_plat_table = df_plataforma.copy()
        df_plat_table['avg_views'] = (df_plat_table['views'] / df_plat_table['videos']).fillna(0).round(0)
        df_plat_table['engagement'] = ((df_plat_table['likes'] + df_plat_table['comments']) / df_plat_table['views'] * 100).fillna(0).round(2)
        
        df_plat_display = df_plat_table[['platform', 'videos', 'views', 'likes', 'avg_views', 'engagement']].copy()
        df_plat_display.columns = ['Plataforma', 'Vídeos', 'Views', 'Likes', 'Média Views', 'Engajamento (%)']
        df_plat_display['Plataforma'] = df_plat_display['Plataforma'].str.upper()
        
        for col in ['Vídeos', 'Views', 'Likes', 'Média Views']:
            df_plat_display[col] = df_plat_display[col].apply(formatar_numero)
        
        st.dataframe(df_plat_display, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Análise temporal global
    df_global = carregar_dados_globais()
    
    if not df_global.empty and 'data_publicacao' in df_global.columns:
        st.subheader("📅 Evolução Temporal Global")
        
        df_temp = df_global.dropna(subset=['data_publicacao']).copy()
        
        if not df_temp.empty:
            df_temp['data'] = df_temp['data_publicacao'].dt.date
            
            # Evolução de publicações por dia
            por_dia = df_temp.groupby('data').size().reset_index(name='videos')
            por_dia = por_dia.sort_values('data')
            por_dia['data_formatada'] = por_dia['data'].apply(lambda x: x.strftime('%d/%m/%Y'))
            
            fig = px.line(
                por_dia,
                x='data_formatada',
                y='videos',
                title="Evolução de Publicações (Todas as Competições)",
                markers=True
            )
            fig.update_layout(height=400, xaxis_title="Data", yaxis_title="Vídeos Publicados")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            # Views acumuladas por dia
            views_por_dia = df_temp.groupby('data')['views'].sum().reset_index()
            views_por_dia = views_por_dia.sort_values('data')
            views_por_dia['views_acumuladas'] = views_por_dia['views'].cumsum()
            views_por_dia['data_formatada'] = views_por_dia['data'].apply(lambda x: x.strftime('%d/%m/%Y'))
            
            fig = px.area(
                views_por_dia,
                x='data_formatada',
                y='views_acumuladas',
                title="Views Acumuladas ao Longo do Tempo",
                color_discrete_sequence=['#667eea']
            )
            fig.update_layout(height=400, xaxis_title="Data", yaxis_title="Views Acumuladas")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    st.divider()
    
    # Top performers globais
    if not df_global.empty:
        st.subheader("🌟 Top Performers Globais")
        
        tab1, tab2, tab3 = st.tabs(["🎬 Top Vídeos", "👥 Top Criadores", "🏆 Top Competições"])
        
        with tab1:
            st.markdown("### 🏆 Top 20 Vídeos Mais Vistos (Global)")
            
            top_videos = df_global.nlargest(20, 'views')[['discord_username', 'platform', 'competition_name', 'views', 'likes', 'title']]
            top_videos_display = top_videos.copy()
            top_videos_display['views'] = top_videos_display['views'].apply(formatar_numero)
            top_videos_display['likes'] = top_videos_display['likes'].apply(formatar_numero)
            top_videos_display['title'] = top_videos_display['title'].apply(lambda x: str(x)[:50] + '...' if pd.notna(x) and len(str(x)) > 50 else x)
            top_videos_display['platform'] = top_videos_display['platform'].str.upper()
            
            st.dataframe(
                top_videos_display.rename(columns={
                    'discord_username': 'Criador',
                    'platform': 'Plataforma',
                    'competition_name': 'Competição',
                    'views': 'Views',
                    'likes': 'Likes',
                    'title': 'Título'
                }),
                use_container_width=True,
                hide_index=True
            )
        
        with tab2:
            st.markdown("### 🏆 Top 20 Criadores (Global)")
            
            top_criadores = df_global.groupby('discord_username').agg({
                'views': 'sum',
                'likes': 'sum',
                'id': 'count'
            }).reset_index()
            top_criadores.columns = ['Criador', 'Total Views', 'Total Likes', 'Vídeos']
            top_criadores = top_criadores.nlargest(20, 'Total Views')
            
            top_criadores_display = top_criadores.copy()
            top_criadores_display['Total Views'] = top_criadores_display['Total Views'].apply(formatar_numero)
            top_criadores_display['Total Likes'] = top_criadores_display['Total Likes'].apply(formatar_numero)
            
            st.dataframe(top_criadores_display, use_container_width=True, hide_index=True)
            
            # Gráfico de barras dos top 10 criadores
            top10_criadores = top_criadores.head(10)
            fig = px.bar(
                top10_criadores,
                x='Total Views',
                y='Criador',
                orientation='h',
                title="Top 10 Criadores por Views",
                color='Total Views',
                color_continuous_scale='Viridis'
            )
            fig.update_yaxes(categoryorder='total ascending')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        with tab3:
            if not df_por_comp.empty:
                st.markdown("### 🏆 Ranking de Competições")
                
                df_rank_comp = df_por_comp.sort_values('views', ascending=False).copy()
                df_rank_comp['rank'] = range(1, len(df_rank_comp) + 1)
                df_rank_comp['status'] = df_rank_comp['is_active'].apply(lambda x: '🟢 Ativa' if x == 1 else '⏸️ Inativa')
                
                df_rank_display = df_rank_comp[['rank', 'name', 'status', 'videos', 'views', 'likes', 'usuarios']].copy()
                df_rank_display.columns = ['#', 'Competição', 'Status', 'Vídeos', 'Views', 'Likes', 'Usuários']
                
                for col in ['Vídeos', 'Views', 'Likes', 'Usuários']:
                    df_rank_display[col] = df_rank_display[col].apply(formatar_numero)
                
                st.dataframe(df_rank_display, use_container_width=True, hide_index=True)
                
                # Gráfico comparativo
                fig = go.Figure()
                
                fig.add_trace(go.Bar(
                    name='Vídeos',
                    x=df_rank_comp['name'],
                    y=df_rank_comp['videos'],
                    marker_color='#667eea'
                ))
                
                fig.add_trace(go.Bar(
                    name='Usuários',
                    x=df_rank_comp['name'],
                    y=df_rank_comp['usuarios'],
                    marker_color='#f093fb'
                ))
                
                fig.update_layout(
                    title="Comparativo: Vídeos vs Usuários por Competição",
                    barmode='group',
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

def pagina_videos_manuais(comp_id, comp_name):
    """Página dedicada aos vídeos adicionados manualmente"""
    st.header(f"📝 Vídeos Manuais - {comp_name}")
    
    st.info("ℹ️ Esta página mostra vídeos que foram adicionados manualmente ao sistema porque não foram capturados pela API automática.")
    
    df_manuais = carregar_videos_manuais(comp_id)
    
    if df_manuais.empty:
        st.warning("⚠️ Nenhum vídeo manual encontrado nesta competição")
        st.info("💡 Vídeos manuais são aqueles adicionados diretamente no banco de dados quando a API não conseguiu capturá-los automaticamente.")
        return
    
    # Estatísticas dos vídeos manuais
    st.subheader("📊 Estatísticas de Vídeos Manuais")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📝 Total Manuais", formatar_numero_completo(len(df_manuais)))
    
    with col2:
        st.metric("👁️ Views Totais", formatar_numero(df_manuais['views'].sum()))
    
    with col3:
        st.metric("❤️ Likes Totais", formatar_numero(df_manuais['likes'].sum()))
    
    with col4:
        avg_views = df_manuais['views'].mean()
        st.metric("📊 Média Views", formatar_numero(avg_views))
    
    st.divider()
    
    # Gráfico de distribuição por plataforma
    st.subheader("📱 Distribuição por Plataforma")
    
    col1, col2 = st.columns(2)
    
    with col1:
        dist_plat = df_manuais['platform'].value_counts().reset_index()
        dist_plat.columns = ['platform', 'count']
        
        fig = px.pie(
            dist_plat,
            values='count',
            names='platform',
            title="Vídeos Manuais por Plataforma",
            color='platform',
            color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with col2:
        views_plat = df_manuais.groupby('platform')['views'].sum().reset_index()
        
        fig = px.pie(
            views_plat,
            values='views',
            names='platform',
            title="Views de Vídeos Manuais por Plataforma",
            color='platform',
            color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    st.divider()
    
    # Análise dos motivos
    if 'manual_reason' in df_manuais.columns:
        st.subheader("🔍 Motivos de Adição Manual")
        
        df_motivos = df_manuais[df_manuais['manual_reason'].notna()].copy()
        
        if not df_motivos.empty:
            motivos_count = df_motivos['manual_reason'].value_counts().reset_index()
            motivos_count.columns = ['Motivo', 'Quantidade']
            
            fig = px.bar(
                motivos_count,
                x='Quantidade',
                y='Motivo',
                orientation='h',
                title="Principais Motivos de Adição Manual",
                color='Quantidade',
                color_continuous_scale='Viridis'
            )
            fig.update_yaxes(categoryorder='total ascending')
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            st.dataframe(motivos_count, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Nenhum motivo registrado para os vídeos manuais")
    
    st.divider()
    
    # Lista de vídeos manuais
    st.subheader(f"📋 Lista de Vídeos Manuais ({len(df_manuais)} vídeos)")
    
    # Filtros
    col1, col2 = st.columns(2)
    
    with col1:
        plataformas = ['Todas'] + sorted(df_manuais['platform'].dropna().unique().tolist())
        plat_filter = st.selectbox("📱 Filtrar por Plataforma:", plataformas)
    
    with col2:
        if 'manual_reason' in df_manuais.columns:
            motivos = ['Todos'] + sorted([m for m in df_manuais['manual_reason'].dropna().unique().tolist() if m])
            motivo_filter = st.selectbox("🔍 Filtrar por Motivo:", motivos)
        else:
            motivo_filter = 'Todos'
    
    df_filtered = df_manuais.copy()
    
    if plat_filter != 'Todas':
        df_filtered = df_filtered[df_filtered['platform'] == plat_filter]
    
    if 'manual_reason' in df_filtered.columns and motivo_filter != 'Todos':
        df_filtered = df_filtered[df_filtered['manual_reason'] == motivo_filter]
    
    st.info(f"📊 Exibindo {len(df_filtered)} vídeos manuais")
    
    # Paginação
    videos_por_pagina = st.selectbox("Vídeos por página:", [10, 25, 50], index=1, key="manual_pagination")
    
    total_paginas = (len(df_filtered) - 1) // videos_por_pagina + 1
    pagina_atual = st.number_input("Página:", min_value=1, max_value=total_paginas, value=1, key="manual_page")
    
    inicio = (pagina_atual - 1) * videos_por_pagina
    fim = min(inicio + videos_por_pagina, len(df_filtered))
    
    st.info(f"📄 Mostrando vídeos {inicio + 1} a {fim} de {len(df_filtered)}")
    
    df_pagina = df_filtered.iloc[inicio:fim]
    
    # Mostrar vídeos
    for idx, (_, row) in enumerate(df_pagina.iterrows(), start=inicio+1):
        st.markdown(f"""
        <div class="manual-video-card">
            <h4>#{idx} - 📝 Vídeo Manual</h4>
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander(f"👁️ {formatar_numero(row.get('views', 0))} views - {row.get('title', 'Sem título')[:80]}...", expanded=False):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"**👤 Criador:** {row.get('discord_username', 'Desconhecido')}")
                st.markdown(f"**📱 Plataforma:** {row.get('platform', 'N/A').upper()}")
                st.markdown(f"**📝 Título:** {row.get('title', 'Sem título')}")
                
                if pd.notna(row.get('manual_reason')):
                    st.markdown(f"**🔍 Motivo da Adição Manual:** {row['manual_reason']}")
                
                if pd.notna(row.get('added_by')):
                    st.markdown(f"**👨‍💼 Adicionado por:** {row['added_by']}")
                
                if pd.notna(row.get('data_adicao')):
                    st.markdown(f"**📅 Data de Adição:** {formatar_data_hora_br(row['data_adicao'])}")
                
                if pd.notna(row.get('data_publicacao')):
                    st.markdown(f"**📅 Publicado em:** {formatar_data_hora_br(row['data_publicacao'])}")
                
                if pd.notna(row.get('url')) and row['url'] != '':
                    st.markdown(f"""
                    <a href="{row['url']}" target="_blank">
                        <button style="
                            background: linear-gradient(135deg, #ffc107 0%, #ff9800 100%);
                            color: white;
                            padding: 0.5rem 1.5rem;
                            border: none;
                            border-radius: 8px;
                            cursor: pointer;
                            font-weight: bold;
                            font-size: 1em;
                            margin-top: 0.5rem;
                        ">
                            🎬 Assistir Vídeo Manual
                        </button>
                    </a>
                    """, unsafe_allow_html=True)
                else:
                    st.warning("🔗 Link não disponível para este vídeo")
            
            with col2:
                st.metric("👁️ Views", formatar_numero(row.get('views', 0)))
                st.metric("❤️ Likes", formatar_numero(row.get('likes', 0)))
                st.metric("💬 Comentários", formatar_numero(row.get('comments', 0)))
                
                if row.get('views', 0) > 0:
                    taxa = ((row.get('likes', 0) + row.get('comments', 0)) / row['views'] * 100)
                    st.metric("📊 Engajamento", f"{taxa:.2f}%")
    
def pagina_videos_manuais(comp_id, comp_name):
    """Página dedicada aos vídeos adicionados manualmente"""
    st.header(f"📝 Vídeos Manuais - {comp_name}")
    
    st.info("ℹ️ Esta página mostra vídeos que foram adicionados manualmente ao sistema porque não foram capturados pela API automática.")
    
    df_manuais = carregar_videos_manuais(comp_id)
    
    if df_manuais.empty:
        st.warning("⚠️ Nenhum vídeo manual encontrado nesta competição")
        st.info("💡 Vídeos manuais são aqueles adicionados diretamente no banco de dados quando a API não conseguiu capturá-los automaticamente.")
        return
    
    # Estatísticas dos vídeos manuais
    st.subheader("📊 Estatísticas de Vídeos Manuais")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📝 Total Manuais", formatar_numero_completo(len(df_manuais)))
    
    with col2:
        st.metric("👁️ Views Totais", formatar_numero(df_manuais['views'].sum()))
    
    with col3:
        st.metric("❤️ Likes Totais", formatar_numero(df_manuais['likes'].sum()))
    
    with col4:
        avg_views = df_manuais['views'].mean()
        st.metric("📊 Média Views", formatar_numero(avg_views))
    
    st.divider()
    
    # Gráfico de distribuição por plataforma
    st.subheader("📱 Distribuição por Plataforma")
    
    col1, col2 = st.columns(2)
    
    with col1:
        dist_plat = df_manuais['platform'].value_counts().reset_index()
        dist_plat.columns = ['platform', 'count']
        
        fig = px.pie(
            dist_plat,
            values='count',
            names='platform',
            title="Vídeos Manuais por Plataforma",
            color='platform',
            color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with col2:
        views_plat = df_manuais.groupby('platform')['views'].sum().reset_index()
        
        fig = px.pie(
            views_plat,
            values='views',
            names='platform',
            title="Views de Vídeos Manuais por Plataforma",
            color='platform',
            color_discrete_map={'tiktok': '#000000', 'instagram': '#E4405F', 'youtube': '#FF0000'}
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    st.divider()
    
    # Análise temporal - quando foram adicionados
    if 'data_adicao' in df_manuais.columns or 'scraped_at' in df_manuais.columns:
        st.subheader("📅 Evolução das Adições Manuais")
        
        df_temp = df_manuais.copy()
        
        # Usar data_adicao se existir, senão scraped_at
        if 'data_adicao' in df_temp.columns:
            df_temp = df_temp.dropna(subset=['data_adicao'])
            df_temp['data'] = df_temp['data_adicao'].dt.date
        elif 'scraped_at' in df_temp.columns:
            df_temp['scraped_at_dt'] = pd.to_datetime(df_temp['scraped_at'], errors='coerce')
            df_temp = df_temp.dropna(subset=['scraped_at_dt'])
            df_temp['data'] = df_temp['scraped_at_dt'].dt.date
        
        if not df_temp.empty and 'data' in df_temp.columns:
            # Vídeos adicionados por dia
            por_dia = df_temp.groupby('data').size().reset_index(name='videos')
            por_dia = por_dia.sort_values('data')
            por_dia['data_formatada'] = por_dia['data'].apply(lambda x: x.strftime('%d/%m/%Y'))
            
            fig = px.line(
                por_dia,
                x='data_formatada',
                y='videos',
                title="Vídeos Manuais Adicionados por Dia",
                markers=True
            )
            fig.update_traces(line_color='#667eea')
            fig.update_layout(height=400, xaxis_title="Data", yaxis_title="Quantidade de Vídeos")
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            # Performance dos vídeos adicionados por data
            col1, col2 = st.columns(2)
            
            with col1:
                views_por_dia = df_temp.groupby('data')['views'].sum().reset_index()
                views_por_dia = views_por_dia.sort_values('data')
                views_por_dia['data_formatada'] = views_por_dia['data'].apply(lambda x: x.strftime('%d/%m/%Y'))
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=views_por_dia['data_formatada'],
                    y=views_por_dia['views'],
                    name='Views',
                    marker_color='#667eea'
                ))
                fig.update_layout(
                    title="Views Totais por Data de Adição",
                    xaxis_title="Data",
                    yaxis_title="Views",
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            with col2:
                likes_por_dia = df_temp.groupby('data')['likes'].sum().reset_index()
                likes_por_dia = likes_por_dia.sort_values('data')
                likes_por_dia['data_formatada'] = likes_por_dia['data'].apply(lambda x: x.strftime('%d/%m/%Y'))
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=likes_por_dia['data_formatada'],
                    y=likes_por_dia['likes'],
                    name='Likes',
                    marker_color='#f093fb'
                ))
                fig.update_layout(
                    title="Likes Totais por Data de Adição",
                    xaxis_title="Data",
                    yaxis_title="Likes",
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
            st.divider()
    
    # Lista de vídeos manuais
    st.subheader(f"📋 Lista de Vídeos Manuais ({len(df_manuais)} vídeos)")
    
    # Filtros
    col1, col2 = st.columns(2)
    
    with col1:
        plataformas = ['Todas'] + sorted(df_manuais['platform'].dropna().unique().tolist())
        plat_filter = st.selectbox("📱 Filtrar por Plataforma:", plataformas, key="manual_plat")
    
    with col2:
        usuarios = ['Todos'] + sorted(df_manuais['discord_username'].dropna().unique().tolist())
        user_filter = st.selectbox("👤 Filtrar por Usuário:", usuarios, key="manual_user")
    
    df_filtered = df_manuais.copy()
    
    if plat_filter != 'Todas':
        df_filtered = df_filtered[df_filtered['platform'] == plat_filter]
    
    if user_filter != 'Todos':
        df_filtered = df_filtered[df_filtered['discord_username'] == user_filter]
    
    st.info(f"📊 Exibindo {len(df_filtered)} vídeos manuais")
    
    # Paginação
    videos_por_pagina = st.selectbox("Vídeos por página:", [10, 25, 50], index=1, key="manual_pagination")
    
    total_paginas = (len(df_filtered) - 1) // videos_por_pagina + 1
    pagina_atual = st.number_input("Página:", min_value=1, max_value=max(1, total_paginas), value=1, key="manual_page")
    
    inicio = (pagina_atual - 1) * videos_por_pagina
    fim = min(inicio + videos_por_pagina, len(df_filtered))
    
    st.info(f"📄 Mostrando vídeos {inicio + 1} a {fim} de {len(df_filtered)}")
    
    df_pagina = df_filtered.iloc[inicio:fim]
    
    # Mostrar vídeos
    for idx, (_, row) in enumerate(df_pagina.iterrows(), start=inicio+1):
        titulo_display = row.get('title', 'Sem título')[:80]
        if len(str(row.get('title', ''))) > 80:
            titulo_display += "..."
            
        with st.expander(f"#{idx} - 👁️ {formatar_numero_completo(row.get('views', 0))} views - {titulo_display}", expanded=False):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"**👤 Criador:** {row.get('discord_username', 'Desconhecido')}")
                
                if pd.notna(row.get('account_username')):
                    st.markdown(f"**📱 Conta da Plataforma:** @{row['account_username']}")
                
                st.markdown(f"**📱 Plataforma:** {row.get('platform', 'N/A').upper()}")
                st.markdown(f"**📝 Título:** {row.get('title', 'Sem título')}")
                
                if pd.notna(row.get('added_by_admin')):
                    st.markdown(f"**👨‍💼 Adicionado por:** {row['added_by_admin']}")
                
                if pd.notna(row.get('data_adicao')):
                    st.markdown(f"**📅 Data de Adição:** {formatar_data_hora_br(row['data_adicao'])}")
                
                if pd.notna(row.get('data_publicacao')):
                    st.markdown(f"**📅 Publicado em:** {formatar_data_hora_br(row['data_publicacao'])}")
                
                if pd.notna(row.get('hashtags')) and row['hashtags']:
                    st.markdown(f"**🏷️ Hashtags:** {row['hashtags']}")
                
                if pd.notna(row.get('url')) and row['url'] != '':
                    st.markdown(f"""
                    <a href="{row['url']}" target="_blank">
                        <button style="
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white;
                            padding: 0.5rem 1.5rem;
                            border: none;
                            border-radius: 8px;
                            cursor: pointer;
                            font-weight: bold;
                            font-size: 1em;
                            margin-top: 0.5rem;
                        ">
                            🎬 Assistir Vídeo
                        </button>
                    </a>
                    """, unsafe_allow_html=True)
                else:
                    st.warning("🔗 Link não disponível para este vídeo")
            
            with col2:
                st.metric("👁️ Views", formatar_numero_completo(row.get('views', 0)))
                st.metric("❤️ Likes", formatar_numero_completo(row.get('likes', 0)))
                st.metric("💬 Comentários", formatar_numero_completo(row.get('comments', 0)))
                
                if row.get('shares'):
                    st.metric("🔄 Shares", formatar_numero_completo(row.get('shares', 0)))
                
                if row.get('views', 0) > 0:
                    taxa = ((row.get('likes', 0) + row.get('comments', 0)) / row['views'] * 100)
                    st.metric("📊 Engajamento", f"{taxa:.2f}%")
    
    # Exportar dados
    st.divider()
    
    st.subheader("💾 Exportar Vídeos Manuais")
    
    colunas_exportar = ['discord_username', 'platform', 'title', 'url', 'views', 'likes', 'comments']
    
    # Adicionar colunas opcionais se existirem
    if 'account_username' in df_filtered.columns:
        colunas_exportar.insert(1, 'account_username')
    if 'added_by_admin' in df_filtered.columns:
        colunas_exportar.append('added_by_admin')
    if 'hashtags' in df_filtered.columns:
        colunas_exportar.append('hashtags')
    
    df_export = df_filtered[colunas_exportar].copy()
    
    # Renomear colunas
    rename_dict = {
        'discord_username': 'Criador',
        'account_username': 'Conta Plataforma',
        'platform': 'Plataforma',
        'title': 'Título',
        'url': 'Link',
        'views': 'Views',
        'likes': 'Likes',
        'comments': 'Comentários',
        'added_by_admin': 'Adicionado Por',
        'hashtags': 'Hashtags'
    }
    
    df_export.columns = [rename_dict.get(col, col) for col in df_export.columns]
    
    csv = df_export.to_csv(index=False, encoding='utf-8-sig')
    
    st.download_button(
        label="💾 Baixar CSV com Vídeos Manuais",
        data=csv,
        file_name=f"videos_manuais_{comp_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        help="Baixa um arquivo CSV com todos os vídeos manuais"
    )

if __name__ == "__main__":
    main()