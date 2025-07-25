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
    clean_name = field_name.lower().replace("ã", "a").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ç", "c").replace("ô", "o").replace("â", "a").replace("õ", "o")
    return clean_name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("?", "")

def get_distinct_values(column_name):
    with engine.connect() as conn:
        try:
            query = text(f"SELECT DISTINCT {column_name} FROM registros WHERE {column_name} IS NOT NULL AND {column_name} != '' ORDER BY {column_name}")
            result = conn.execute(query).fetchall()
            return [row[0] for row in result]
        except:
            return []

def get_table_columns():
    """Retorna as colunas existentes na tabela registros"""
    with engine.connect() as conn:
        query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'registros'
        """)
        result = conn.execute(query).fetchall()
        return [row[0] for row in result]

def fetch_records(search_term="", selected_books=None):
    """
    Busca registros no banco de dados com tratamento robusto de erros
    """
    if not selected_books:
        return pd.DataFrame(columns=['ID', 'Tipo', 'Nome Principal', 'Data', 'Livro Fonte'])

    try:
        with engine.connect() as conn:
            # Query simples e robusta - buscar todos os campos
            if search_term:
                # Com termo de busca - usar query mais simples
                query = """
                SELECT * FROM registros 
                WHERE fonte_livro = ANY(:books)
                AND (
                    CAST(id AS TEXT) ILIKE :search_term OR
                    COALESCE(tipo_registro, '') ILIKE :search_term OR
                    COALESCE(nome_do_registrado, '') ILIKE :search_term OR
                    COALESCE(nome_do_noivo, '') ILIKE :search_term OR
                    COALESCE(nome_do_falecido, '') ILIKE :search_term OR
                    COALESCE(fonte_livro, '') ILIKE :search_term OR
                    COALESCE(fonte_pagina_folha, '') ILIKE :search_term OR
                    COALESCE(observacoes, '') ILIKE :search_term
                )
                ORDER BY id
                """
                params = {
                    'books': selected_books,
                    'search_term': f'%{search_term}%'
                }
            else:
                # Sem termo de busca
                query = """
                SELECT * FROM registros 
                WHERE fonte_livro = ANY(:books)
                ORDER BY id
                """
                params = {'books': selected_books}
            
            # Executar query
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall())
            
            if not df.empty:
                # Adicionar nomes de colunas
                df.columns = result.keys()
                
                # Criar coluna Nome Principal
                df['Nome Principal'] = 'N/A'
                if 'nome_do_registrado' in df.columns:
                    df.loc[df['nome_do_registrado'].notna(), 'Nome Principal'] = df.loc[df['nome_do_registrado'].notna(), 'nome_do_registrado']
                if 'nome_do_noivo' in df.columns:
                    df.loc[(df['Nome Principal'] == 'N/A') & df['nome_do_noivo'].notna(), 'Nome Principal'] = df.loc[(df['Nome Principal'] == 'N/A') & df['nome_do_noivo'].notna(), 'nome_do_noivo']
                if 'nome_do_falecido' in df.columns:
                    df.loc[(df['Nome Principal'] == 'N/A') & df['nome_do_falecido'].notna(), 'Nome Principal'] = df.loc[(df['Nome Principal'] == 'N/A') & df['nome_do_falecido'].notna(), 'nome_do_falecido']
                
                # Criar coluna Data
                df['Data'] = 'N/A'
                if 'data_do_evento' in df.columns:
                    df.loc[df['data_do_evento'].notna(), 'Data'] = df.loc[df['data_do_evento'].notna(), 'data_do_evento']
                if 'data_do_obito' in df.columns:
                    df.loc[(df['Data'] == 'N/A') & df['data_do_obito'].notna(), 'Data'] = df.loc[(df['Data'] == 'N/A') & df['data_do_obito'].notna(), 'data_do_obito']
                
                # Preparar DataFrame final
                columns_to_show = []
                rename_dict = {}
                
                if 'id' in df.columns:
                    columns_to_show.append('id')
                    rename_dict['id'] = 'ID'
                    
                if 'tipo_registro' in df.columns:
                    columns_to_show.append('tipo_registro')
                    rename_dict['tipo_registro'] = 'Tipo'
                    
                columns_to_show.extend(['Nome Principal', 'Data'])
                
                if 'fonte_livro' in df.columns:
                    columns_to_show.append('fonte_livro')
                    rename_dict['fonte_livro'] = 'Livro Fonte'
                
                return df[columns_to_show].rename(columns=rename_dict)
            else:
                return pd.DataFrame(columns=['ID', 'Tipo', 'Nome Principal', 'Data', 'Livro Fonte'])
                
    except Exception as e:
        st.error(f"Erro ao buscar registros: {str(e)}")
        st.info("Verifique se a estrutura do banco de dados está correta.")
        return pd.DataFrame(columns=['ID', 'Tipo', 'Nome Principal', 'Data', 'Livro Fonte'])

def fetch_single_record(record_id):
    with engine.connect() as conn:
        query = text("SELECT * FROM registros WHERE id = :id")
        result = conn.execute(query, {'id': record_id}).fetchone()
        return result._asdict() if result else None

def generate_excel_bytes(records_by_type):
    """Gera arquivo Excel com os registros organizados por tipo"""
    if not EXPORT_LIBS_AVAILABLE:
        st.error("Bibliotecas de exportação não disponíveis.")
        return None
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for record_type, records in records_by_type.items():
            if records:
                df = pd.DataFrame(records)
                # Reordenar colunas conforme definido
                if record_type in EXPORT_COLUMN_ORDER:
                    columns_order = [col for col in EXPORT_COLUMN_ORDER[record_type] if col in df.columns]
                    df = df[columns_order]
                
                # Escrever no Excel
                sheet_name = record_type[:31]  # Excel tem limite de 31 caracteres
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Aplicar formatação
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column = [cell for cell in column]
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
    
    return output.getvalue()

def generate_pdf_bytes(records_by_type):
    """Gera arquivo PDF com os registros organizados por tipo"""
    if not EXPORT_LIBS_AVAILABLE:
        st.error("Bibliotecas de exportação não disponíveis.")
        return None
    
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=landscape(A3))
    story = []
    styles = getSampleStyleSheet()
    
    for record_type, records in records_by_type.items():
        if records:
            # Título da seção
            story.append(Paragraph(f"Registros de {record_type}", styles['Title']))
            
            # Preparar dados para tabela
            df = pd.DataFrame(records)
            if record_type in EXPORT_COLUMN_ORDER:
                columns_order = [col for col in EXPORT_COLUMN_ORDER[record_type] if col in df.columns]
                df = df[columns_order]
            
            # Criar tabela
            data = [df.columns.tolist()] + df.values.tolist()
            table = ReportlabTable(data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(table)
            story.append(PageBreak())
    
    doc.build(story)
    return output.getvalue()

# --- INTERFACE DO APLICATIVO ---

def login_form():
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
                    if field == "Fonte (Livro)" and book_preset: default_value = book_preset
                    elif field == "Local do Evento" and location_preset: default_value = location_preset
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
        
        if not all_books_manage:
            st.warning("Nenhum livro encontrado no banco de dados. Adicione registros primeiro.")
            selected_books_manage = []
        else:
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
                record = fetch_single_record(record_id_to_manage)
                if record:
                    st.success(f"Registro ID {record_id_to_manage} encontrado!")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("📋 Ver Detalhes", use_container_width=True):
                            st.session_state.manage_action = "view"
                    with col2:
                        if st.button("✏️ Editar", use_container_width=True):
                            st.session_state.manage_action = "edit"
                    with col3:
                        if st.button("🗑️ Excluir", use_container_width=True):
                            st.session_state.manage_action = "delete"
                    
                    if hasattr(st.session_state, 'manage_action'):
                        if st.session_state.manage_action == "view":
                            st.subheader("Detalhes do Registro")
                            for key, value in record.items():
                                if value:
                                    st.text(f"{key}: {value}")
                        
                        elif st.session_state.manage_action == "edit":
                            st.info("Funcionalidade de edição em desenvolvimento.")
                        
                        elif st.session_state.manage_action == "delete":
                            st.warning(f"Tem certeza que deseja excluir o registro ID {record_id_to_manage}?")
                            if st.button("Confirmar Exclusão", type="primary"):
                                try:
                                    with engine.connect() as conn:
                                        conn.execute(text("DELETE FROM registros WHERE id = :id"), {'id': record_id_to_manage})
                                        conn.commit()
                                        st.success("Registro excluído com sucesso!")
                                        del st.session_state.manage_action
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Erro ao excluir: {e}")
                else:
                    st.error(f"Registro ID {record_id_to_manage} não encontrado.")

    with tab_export:
        st.header("Exportar Dados")
        
        if EXPORT_LIBS_AVAILABLE:
            all_books_export = get_distinct_values("fonte_livro")
            if not all_books_export:
                st.warning("Nenhum registro encontrado para exportar.")
            else:
                selected_books_export = st.multiselect("Selecione os livros para exportar:", all_books_export, default=all_books_export)
                
                if selected_books_export:
                    export_format = st.radio("Formato de exportação:", ["Excel", "PDF"])
                    
                    if st.button("Gerar Arquivo para Download"):
                        try:
                            # Buscar todos os registros dos livros selecionados
                            with engine.connect() as conn:
                                query = text("SELECT * FROM registros WHERE fonte_livro = ANY(:books) ORDER BY tipo_registro, id")
                                result = conn.execute(query, {'books': selected_books_export})
                                all_records = [dict(row._mapping) for row in result]
                                
                                if all_records:
                                    # Organizar por tipo
                                    records_by_type = defaultdict(list)
                                    for record in all_records:
                                        records_by_type[record['tipo_registro']].append(record)
                                    
                                    if export_format == "Excel":
                                        file_bytes = generate_excel_bytes(dict(records_by_type))
                                        if file_bytes:
                                            st.download_button(
                                                label="📥 Baixar Excel",
                                                data=file_bytes,
                                                file_name="cpindexator_export.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            )
                                    else:  # PDF
                                        file_bytes = generate_pdf_bytes(dict(records_by_type))
                                        if file_bytes:
                                            st.download_button(
                                                label="📥 Baixar PDF",
                                                data=file_bytes,
                                                file_name="cpindexator_export.pdf",
                                                mime="application/pdf"
                                            )
                                else:
                                    st.warning("Nenhum registro encontrado nos livros selecionados.")
                        except Exception as e:
                            st.error(f"Erro ao gerar arquivo: {e}")
        else:
            st.error("Bibliotecas de exportação não instaladas. Instale openpyxl e reportlab.")


# --- ROTEADOR PRINCIPAL ---
if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    login_form()
else:
    main_app()
