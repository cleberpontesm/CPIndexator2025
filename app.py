# app.py - VERSÃO FINAL CORRIGIDA - CPIndexator com Supabase DB e Autenticação
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from supabase import create_client, Client
from collections import defaultdict
from io import BytesIO
import os

# --- Bloco de importação de bibliotecas de exportação ---
try:
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from reportlab.lib.pagesizes import A3, landscape
    from reportlab.platypus import SimpleDocTemplate, Table as ReportlabTable, TableStyle, Paragraph, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    EXPORT_LIBS_AVAILABLE = True
except ImportError:
    EXPORT_LIBS_AVAILABLE = False

# --- Definições e constantes (do seu código original) ---
FORM_DEFINITIONS = {
    "Nascimento/Batismo": ["Data do Registro", "Data do Evento", "Local do Evento", "Nome do Registrado", "Nome do Pai", "Nome da Mãe", "Padrinhos", "Avô paterno", "Avó paterna", "Avô materno", "Avó materna"],
    "Casamento": ["Data do Registro", "Data do Evento", "Local do Evento", "Nome do Noivo", "Idade do Noivo", "Pai do Noivo", "Mãe do Noivo", "Nome da Noiva", "Idade da Noiva", "Pai da Noiva", "Mãe da Noiva", "Testemunhas"],
    "Óbito": ["Data do Registro", "Data do Óbito", "Local do Óbito", "Nome do Falecido", "Idade no Óbito", "Filiação", "Cônjuge Sobrevivente", "Deixou Filhos?", "Causa Mortis", "Local do Sepultamento"]
}
COMMON_FIELDS = ["Fonte (Livro)", "Fonte (Página/Folha)", "Observações", "Caminho da Imagem"]
EXPORT_COLUMN_ORDER = {
    "Nascimento/Batismo": ["id", "tipo_registro"] + [f.lower().replace(" ", "_") for f in FORM_DEFINITIONS["Nascimento/Batismo"] + COMMON_FIELDS],
    "Casamento": ["id", "tipo_registro"] + [f.lower().replace(" ", "_") for f in FORM_DEFINITIONS["Casamento"] + COMMON_FIELDS],
    "Óbito": ["id", "tipo_registro"] + [f.lower().replace(" ", "_").replace("?", "") for f in FORM_DEFINITIONS["Óbito"] + COMMON_FIELDS]
}

# --- CONFIGURAÇÃO INICIAL E CLIENTES ---
st.set_page_config(layout="wide", page_title="CPIndexator Web")

@st.cache_resource
def init_supabase_auth():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except Exception as e:
        st.error("Erro ao inicializar o cliente Supabase. Verifique seus segredos.")
        st.stop()

@st.cache_resource
def init_db_connection():
    try:
        connection_string = st.secrets["DB_CONNECTION_STRING"]
        return create_engine(connection_string)
    except Exception as e:
        st.error("Erro ao conectar ao banco de dados. Verifique sua Connection String.")
        st.stop()

supabase = init_supabase_auth()
engine = init_db_connection()


# --- FUNÇÕES DE LÓGICA DO BANCO DE DADOS E EXPORTAÇÃO ---

def to_col_name(field_name):
    # CORREÇÃO: Normaliza para remover acentos e caracteres especiais comuns
    clean_name = field_name.lower().replace("ã", "a").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ç", "c").replace("ô", "o").replace("â", "a")
    # Continua com a limpeza de espaços e outros caracteres
    return clean_name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("?", "")

def get_distinct_values(column_name):
    with engine.connect() as conn:
        try:
            query = text(f"SELECT DISTINCT {column_name} FROM registros WHERE {column_name} IS NOT NULL AND {column_name} != '' ORDER BY {column_name}")
            result = conn.execute(query).fetchall()
            return [row[0] for row in result]
        except:
            return []

def fetch_records(search_term="", selected_books=None):
    if not selected_books: return pd.DataFrame()
    with engine.connect() as conn:
        base_query = "SELECT id, tipo_registro, nome_do_registrado, nome_do_noivo, nome_do_falecido, data_do_evento, data_do_óbito, fonte_livro FROM registros"
        params = {'books': tuple(selected_books)}
        conditions = ["fonte_livro IN :books"]
        if search_term:
            params['like_term'] = f"%{search_term}%"
            text_columns = ['nome_do_registrado', 'nome_do_pai', 'nome_da_mae', 'padrinhos', 'avo_paterno', 'avo_paterna', 'avo_materno', 'avo_materna', 'nome_do_noivo', 'pai_do_noivo', 'mae_do_noivo', 'nome_da_noiva', 'pai_da_noiva', 'mae_da_noiva', 'testemunhas', 'nome_do_falecido', 'filiacao', 'conjuge_sobrevivente', 'observacoes']
            conditions.append(f"({ ' OR '.join([f'{col} ILIKE :like_term' for col in text_columns]) })")
        
        final_query = f"{base_query} WHERE {' AND '.join(conditions)} ORDER BY id"
        df = pd.read_sql(text(final_query), conn, params=params)

        df['Nome Principal'] = df['nome_do_registrado'].fillna(df['nome_do_noivo']).fillna(df['nome_do_falecido']).fillna('N/A')
        df['Data'] = df['data_do_evento'].fillna(df['data_do_óbito']).fillna('N/A')
        df_display = df[['id', 'tipo_registro', 'Nome Principal', 'Data', 'fonte_livro']].rename(columns={'id': 'ID', 'tipo_registro': 'Tipo', 'fonte_livro': 'Livro Fonte'})
        return df_display

def fetch_single_record(record_id):
    with engine.connect() as conn:
        query = text("SELECT * FROM registros WHERE id = :id")
        result = conn.execute(query, {'id': record_id}).fetchone()
        return result._asdict() if result else None

def fetch_data_for_export(selected_books):
    if not selected_books: return None
    with engine.connect() as conn:
        query = text(f"SELECT * FROM registros WHERE fonte_livro IN :books ORDER BY tipo_registro, id")
        result = conn.execute(query, {'books': tuple(selected_books)}).fetchall()
        return [row._asdict() for row in result]

def generate_excel_bytes(all_data, grouping_key, column_name_mapper):
    # Esta função está pronta para ser implementada com a lógica de exportação completa
    output_buffer = BytesIO()
    st.warning("Lógica de geração de Excel ainda não implementada no código final.")
    return output_buffer.getvalue()

def generate_pdf_bytes(all_data, grouping_key, column_name_mapper):
    # Esta função está pronta para ser implementada com a lógica de exportação completa
    pdf_buffer = BytesIO()
    st.warning("Lógica de geração de PDF ainda não implementada no código final.")
    return pdf_buffer.getvalue()

# --- INTERFACE DO APLICATIVO ---

def login_form():
    """Exibe o formulário de login."""
    st.title("CPIndexator - Versão WEB")
    st.subheader("Por favor, faça o login para continuar")
    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
        if submitted:
            try:
                response = supabase.auth.sign_in_with_password({"email": email, "password": password})
                st.session_state.user = response.user
                st.rerun()
            except Exception as e:
                st.error("Email ou senha inválidos.")

def main_app():
    """Exibe o aplicativo principal após o login."""
    st.sidebar.title("Bem-vindo(a)!")
    st.sidebar.info(f"Logado como: {st.session_state.user.email}")
    if st.sidebar.button("Sair (Logout)"):
        del st.session_state.user
        st.rerun()

    st.title("CPIndexator - Painel Principal")
    tab_add, tab_manage, tab_export = st.tabs(["➕ Adicionar Registro", "🔍 Consultar e Gerenciar", "📤 Exportar Dados"])

    with tab_add:
        st.header("Adicionar Novo Registro")
        all_books = get_distinct_values("fonte_livro")
        all_locations = get_distinct_values("local_do_evento")
        col1, col2 = st.columns(2)
        with col1:
            book_preset = st.selectbox("Preencher 'Fonte (Livro)' com:", [""] + all_books, key="book_preset_add")
        with col2:
            location_preset = st.selectbox("Preencher 'Local do Evento' com:", [""] + all_locations, key="location_preset_add")
        
        record_type = st.selectbox("Tipo de Registro:", list(FORM_DEFINITIONS.keys()), index=None, placeholder="Selecione...")
        
        if record_type:
            with st.form("new_record_form", clear_on_submit=True):
                fields = FORM_DEFINITIONS.get(record_type, []) + COMMON_FIELDS
                entries = {}
                for field in fields:
                    default_value = ""
                    if field == "Fonte (Livro)" and book_preset:
                        default_value = book_preset
                    elif field == "Local do Evento" and location_preset:
                        default_value = location_preset
                    entries[field] = st.text_input(f"{field}:", value=default_value, key=f"add_{to_col_name(field)}")
                
                submitted = st.form_submit_button(f"Adicionar Registro de {record_type}")
                if submitted:
                    try:
                        with engine.connect() as conn:
                            cols = ["tipo_registro"] + [to_col_name(label) for label in entries.keys()]
                            vals = [record_type] + [value for value in entries.values()]
                            
                            placeholders = ', '.join([f':{c}' for c in cols])
                            query = f"INSERT INTO registros ({', '.join(cols)}) VALUES ({placeholders})"
                            
                            params = dict(zip(cols, vals))
                            
                            conn.execute(text(query), params)
                            conn.commit()
                            st.success("Registro adicionado com sucesso!")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Ocorreu um erro ao salvar: {e}")

    with tab_manage:
        st.header("Consultar Registros")
        st.sidebar.header("Filtros de Consulta")
        all_books_manage = get_distinct_values("fonte_livro")
        selected_books_manage = st.sidebar.multiselect("Filtrar por Livro(s):", all_books_manage, default=all_books_manage, key="manage_books_select")
        search_term = st.sidebar.text_input("Busca Rápida por Termo:")
        
        if not selected_books_manage:
            st.warning("Por favor, selecione ao menos um livro no filtro.")
        else:
            df_records = fetch_records(search_term, selected_books_manage)
            st.dataframe(df_records, use_container_width=True, hide_index=True)
            
            st.header("Gerenciar Registro Selecionado")
            record_id_to_manage = st.number_input("Digite o ID do registro para ver detalhes, editar ou excluir:", min_value=1, step=1, value=None)
            if record_id_to_manage:
                st.info(f"Funcionalidade de gerenciamento para o ID {record_id_to_manage} em desenvolvimento.")

    with tab_export:
        st.header("Exportar Dados")
        st.info("Funcionalidade de exportação em desenvolvimento.")


# --- ROTEADOR PRINCIPAL ---
if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    login_form()
else:
    main_app()
