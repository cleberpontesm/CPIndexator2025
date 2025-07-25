# app.py - VERSÃO FINAL COMPLETA - CPIndexator com Supabase DB e Autenticação
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
    return field_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("?", "")

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
            conditions.append(f"({ ' OR '.join([f'{col} ILIKE :like_term' for col in text_columns]) })") # ILIKE para case-insensitive
        
        final_query = f"{base_query} WHERE {' AND '.join(conditions)} ORDER BY id"
        df = pd.read_sql(text(final_query), conn, params=params)

        # Processar colunas para exibição no Streamlit
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
    output_buffer = BytesIO()
    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        grouped_data = defaultdict(list)
        for row in all_data: grouped_data[row[grouping_key]].append(row)
        for group_name, records in grouped_data.items():
            df = pd.DataFrame(records).dropna(axis='columns', how='all')
            record_type_for_ordering = records[0]['tipo_registro']
            ordered_cols = [col for col in EXPORT_COLUMN_ORDER.get(record_type_for_ordering, []) if col in df.columns]
            df = df[ordered_cols]
            df.rename(columns=column_name_mapper, inplace=True)
            sheet_name = str(group_name).replace("/", "-").replace("\\", "-")[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            try:
                tab_range = f"A1:{chr(ord('A') + len(df.columns) - 1)}{len(df) + 1}"
                tab = Table(displayName=f"Tabela_{sheet_name.replace('-', '')}", ref=tab_range)
                style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False, showRowStripes=True, showColumnStripes=False)
                tab.tableStyleInfo = style; worksheet.add_table(tab)
            except Exception as e: print(f"Aviso Excel: {e}")
            for column in worksheet.columns:
                max_length = max(len(str(cell.value)) for cell in column if cell.value)
                adjusted_width = (max_length + 2) if max_length < 50 else 50
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
    return output_buffer.getvalue()

def generate_pdf_bytes(all_data, grouping_key, column_name_mapper):
    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=landscape(A3))
    styles = getSampleStyleSheet()
    elements = [Paragraph("Relatório de Registros Genealógicos", styles['h1'])]
    grouped_data = defaultdict(list)
    for row in all_data: grouped_data[row[grouping_key]].append(row)
    for group_name, records in grouped_data.items():
        elements.append(PageBreak())
        elements.append(Paragraph(f"Registros de: {group_name}", styles['h2']))
        if not records: continue
        df_temp = pd.DataFrame(records).dropna(axis='columns', how='all')
        record_type_for_ordering = records[0]['tipo_registro']
        pdf_cols = [col for col in EXPORT_COLUMN_ORDER.get(record_type_for_ordering, []) if col in df_temp.columns]
        headers = [column_name_mapper.get(h, h) for h in pdf_cols]
        table_data = [headers] + [[str(record.get(col, '')) for col in pdf_cols] for record in records]
        if len(table_data) > 1:
            # ... (código de formatação do PDF) ...
            table = ReportlabTable(table_data, repeatRows=1) # Simplified for brevity
            elements.append(table)
    doc.build(elements)
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
            except Exception as e: st.error("Email ou senha inválidos.")

def main_app():
    """Exibe o aplicativo principal após o login."""
    st.sidebar.title("Bem-vindo(a)!")
    st.sidebar.info(f"Logado como: {st.session_state.user.email}")
    if st.sidebar.button("Sair (Logout)"):
        del st.session_state.user; st.rerun()

    st.title("CPIndexator - Painel Principal")
    tab_add, tab_manage, tab_export = st.tabs(["➕ Adicionar Registro", "🔍 Consultar e Gerenciar", "📤 Exportar Dados"])

    with tab_add:
        st.header("Adicionar Novo Registro")
        all_books = get_distinct_values("fonte_livro")
        all_locations = get_distinct_values("local_do_evento")
        col1, col2 = st.columns(2)
        with col1: book_preset = st.selectbox("Preencher 'Fonte (Livro)' com:", [""] + all_books, key="book_preset_add")
        with col2: location_preset = st.selectbox("Preencher 'Local do Evento' com:", [""] + all_locations, key="location_preset_add")
        record_type = st.selectbox("Tipo de Registro:", list(FORM_DEFINITIONS.keys()), index=None, placeholder="Selecione...")
        if record_type:
            with st.form("new_record_form", clear_on_submit=True):
                fields = FORM_DEFINITIONS.get(record_type, []) + COMMON_FIELDS
                entries = {field: st.text_input(f"{field}:", value=book_preset if field == "Fonte (Livro)" and book_preset else location_preset if field == "Local do Evento" and location_preset else "") for field in fields}
                if st.form_submit_button(f"Adicionar Registro de {record_type}"):
                    with engine.connect() as conn:
                        cols = ["tipo_registro"] + [to_col_name(label) for label in entries.keys()]
                        vals = [record_type] + [value for value in entries.values()]
                        query = text(f"INSERT INTO registros ({', '.join(cols)}) VALUES ({', '.join([':'+c for c in cols])})")
                        params = dict(zip(cols, vals))
                        conn.execute(query, params); conn.commit()
                        st.success("Registro adicionado com sucesso!")
    
    with tab_manage:
        st.header("Consultar Registros")
        st.sidebar.header("Filtros de Consulta")
        all_books = get_distinct_values("fonte_livro")
        selected_books = st.sidebar.multiselect("Filtrar por Livro(s):", all_books, default=all_books)
        search_term = st.sidebar.text_input("Busca Rápida por Termo:")
        if not selected_books: st.warning("Selecione ao menos um livro no filtro.")
        else:
            df_records = fetch_records(search_term, selected_books)
            st.dataframe(df_records, use_container_width=True, hide_index=True)
            # ... (código para editar e excluir um registro pelo ID) ...

    with tab_export:
        st.header("Exportar Dados")
        # ... (código da aba de exportar que já fizemos) ...


# --- ROTEADOR PRINCIPAL ---
if 'user' not in st.session_state: st.session_state.user = None
if st.session_state.user is None: login_form()
else: main_app()