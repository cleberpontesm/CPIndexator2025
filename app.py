# app.py - Versão Web com Streamlit do CPIndexator
import streamlit as st
import sqlite3
import os
import pandas as pd
from collections import defaultdict
from datetime import datetime
from io import BytesIO

# --- Bloco de importação de bibliotecas de exportação ---
# Essas bibliotecas precisam estar no seu requirements.txt
try:
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from reportlab.lib.pagesizes import A3, landscape
    from reportlab.platypus import SimpleDocTemplate, Table as ReportlabTable, TableStyle, Paragraph, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    EXPORT_LIBS_AVAILABLE = True
except ImportError:
    EXPORT_LIBS_AVAILABLE = False

# --- Definições e constantes do seu programa original (mantidas) ---
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

# --- LÓGICA DE BANCO DE DADOS (adaptada para funcionar com o Streamlit) ---

# Helper para obter o nome da coluna no padrão do DB
def to_col_name(field_name):
    return field_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("?", "")

def setup_database(conn):
    """Prepara o banco de dados, garantindo que todas as colunas existam."""
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS registros (id INTEGER PRIMARY KEY AUTOINCREMENT, tipo_registro TEXT NOT NULL)")
    cursor.execute("PRAGMA table_info(registros)")
    existing_columns = [row[1] for row in cursor.fetchall()]

    all_fields = set(field for fields in FORM_DEFINITIONS.values() for field in fields)
    all_fields.update(COMMON_FIELDS)

    for field in all_fields:
        col_name = to_col_name(field)
        if col_name not in existing_columns:
            cursor.execute(f"ALTER TABLE registros ADD COLUMN {col_name} TEXT")
    conn.commit()

# --- FUNÇÕES DE LÓGICA (quase inalteradas, apenas recebem a conexão) ---

def get_distinct_values(conn, column_name):
    """Busca valores distintos de uma coluna para preencher seletores."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT {column_name} FROM registros WHERE {column_name} IS NOT NULL AND {column_name} != '' ORDER BY {column_name}")
        return [row[0] for row in cursor.fetchall()]
    except:
        return []

def fetch_records(conn, search_term="", selected_books=None):
    """Busca registros no banco de dados com base nos filtros."""
    if not selected_books:
        return []

    base_query = "SELECT id, tipo_registro, nome_do_registrado, nome_do_noivo, nome_do_falecido, data_do_evento, data_do_óbito, fonte_livro FROM registros"
    params, conditions = [], []

    conditions.append(f"fonte_livro IN ({','.join(['?'] * len(selected_books))})")
    params.extend(selected_books)

    if search_term:
        like_term = f"%{search_term}%"
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(registros)")
        text_columns = [row[1] for row in cursor.fetchall() if row[2] == 'TEXT' and row[1] != 'tipo_registro']
        if text_columns:
            conditions.append(f"({ ' OR '.join([f'{col} LIKE ?' for col in text_columns]) })")
            params.extend([like_term] * len(text_columns))

    final_query = f"{base_query} WHERE {' AND '.join(conditions)} ORDER BY id"
    cursor = conn.cursor()
    cursor.execute(final_query, params)
    
    # Transforma o resultado em um DataFrame do Pandas para fácil exibição
    data = []
    for row in cursor.fetchall():
        nome_principal = row[2] or row[3] or row[4] or "N/A"
        data_principal = row[5] or row[6] or "N/A"
        data.append({
            "ID": row[0],
            "Tipo": row[1],
            "Nome Principal": nome_principal,
            "Data": data_principal,
            "Livro Fonte": row[7]
        })
    return pd.DataFrame(data)

def fetch_single_record(conn, record_id):
    """Busca um único registro completo pelo ID."""
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM registros WHERE id = ?", (record_id,))
    record = cursor.fetchone()
    return dict(record) if record else None

# --- Início da Interface Streamlit ---

st.set_page_config(layout="wide", page_title="CPIndexator Web")
st.title("CPIndexator Web  genealogia_indexador.db  genealogia_indexador.db genealogia_indexador.db")

# A maior mudança: o usuário faz o upload do seu próprio banco de dados.
# Isso garante que os dados de cada usuário sejam privados e separados.
st.sidebar.header("1. Carregue seu Banco de Dados")
uploaded_db = st.sidebar.file_uploader("Selecione o arquivo 'genealogia_indexador.db'", type=["db"])

# O aplicativo só continua se um banco de dados for carregado.
if uploaded_db is not None:
    # Para trabalhar com o DB, o salvamos temporariamente no servidor
    # e guardamos seu caminho no st.session_state para persistir entre as interações.
    if "db_path" not in st.session_state:
        db_path = f"./{uploaded_db.name}"
        with open(db_path, "wb") as f:
            f.write(uploaded_db.getbuffer())
        st.session_state.db_path = db_path
        
        # Prepara o DB na primeira carga
        conn = sqlite3.connect(st.session_state.db_path)
        setup_database(conn)
        conn.close()

    conn = sqlite3.connect(st.session_state.db_path)

    # Interface principal com abas, similar ao seu design original
    tab_add, tab_manage, tab_export = st.tabs(["➕ Adicionar Registro", "🔍 Consultar e Gerenciar", "📤 Exportar Dados"])

    # --- ABA DE ADICIONAR REGISTRO ---
    with tab_add:
        st.header("Adicionar Novo Registro")

        all_books = get_distinct_values(conn, "fonte_livro")
        all_locations = get_distinct_values(conn, "local_do_evento")

        col1, col2 = st.columns(2)
        with col1:
            book_preset = st.selectbox("Preencher 'Fonte (Livro)' com:", [""] + all_books)
        with col2:
            location_preset = st.selectbox("Preencher 'Local do Evento' com:", [""] + all_locations)
        
        # O formulário é salvo no st.session_state para não perder dados
        if 'form_data' not in st.session_state:
            st.session_state.form_data = {}
        
        record_type = st.selectbox("Tipo de Registro:", list(FORM_DEFINITIONS.keys()), index=None, placeholder="Selecione...")

        if record_type:
            with st.form("new_record_form", clear_on_submit=True):
                fields_for_type = FORM_DEFINITIONS.get(record_type, [])
                final_fields = fields_for_type + COMMON_FIELDS
                
                form_entries = {}
                for field in final_fields:
                    # Preenchimento automático com base nos seletores
                    default_value = ""
                    if field == "Fonte (Livro)" and book_preset:
                        default_value = book_preset
                    elif field == "Local do Evento" and location_preset:
                        default_value = location_preset
                    
                    form_entries[field] = st.text_input(f"{field}:", value=default_value, key=f"add_{to_col_name(field)}")

                submitted = st.form_submit_button(f"Adicionar Registro de {record_type}")
                if submitted:
                    try:
                        cursor = conn.cursor()
                        columns = ["tipo_registro"]
                        values = [record_type]
                        for label, value in form_entries.items():
                            columns.append(to_col_name(label))
                            values.append(value)
                        
                        placeholders = ', '.join(['?'] * len(columns))
                        sql = f"INSERT INTO registros ({', '.join(columns)}) VALUES ({placeholders})"
                        cursor.execute(sql, values)
                        conn.commit()
                        st.success("Registro adicionado com sucesso!")
                        st.rerun() # Recarrega a página para atualizar as listas
                    except Exception as e:
                        st.error(f"Ocorreu um erro: {e}")

    # --- ABA DE CONSULTAR E GERENCIAR ---
    with tab_manage:
        st.header("Consultar Registros")
        
        # Filtros na barra lateral para uma interface mais limpa
        st.sidebar.header("2. Filtros de Consulta")
        all_books = get_distinct_values(conn, "fonte_livro")
        selected_books = st.sidebar.multiselect("Filtrar por Livro(s):", all_books, default=all_books)
        search_term = st.sidebar.text_input("Busca Rápida por Termo:")

        if not selected_books:
            st.warning("Por favor, selecione ao menos um livro no filtro da barra lateral.")
        else:
            df_records = fetch_records(conn, search_term, selected_books)
            st.dataframe(df_records, use_container_width=True, hide_index=True)

            st.header("Gerenciar Registro Selecionado")
            record_id_to_manage = st.number_input("Digite o ID do registro para ver detalhes, editar ou excluir:", min_value=1, step=1, value=None)

            if record_id_to_manage:
                record = fetch_single_record(conn, record_id_to_manage)
                if record:
                    # Mapeador de colunas para nomes amigáveis
                    column_mapper = {to_col_name(f): f for fields in FORM_DEFINITIONS.values() for f in fields}
                    column_mapper.update({to_col_name(f): f for f in COMMON_FIELDS})
                    column_mapper['id'] = 'ID'
                    column_mapper['tipo_registro'] = 'Tipo de Registro'
                    
                    with st.expander("Ver Detalhes Completos", expanded=True):
                        details_str = ""
                        for key, value in record.items():
                            if value:
                                friendly_name = column_mapper.get(key, key.replace('_', ' ').title())
                                details_str += f"**{friendly_name}:** {value}\n\n"
                        st.markdown(details_str)

                    # Lógica de Edição
                    with st.expander("Editar Registro"):
                        with st.form(f"edit_form_{record_id_to_manage}"):
                            record_type = record["tipo_registro"]
                            fields_for_type = FORM_DEFINITIONS.get(record_type, [])
                            final_fields = fields_for_type + COMMON_FIELDS
                            
                            edited_entries = {}
                            for field in final_fields:
                                col_name = to_col_name(field)
                                edited_entries[field] = st.text_input(f"{field}:", value=record.get(col_name, ""), key=f"edit_{col_name}_{record_id_to_manage}")
                            
                            if st.form_submit_button("Salvar Alterações"):
                                try:
                                    cursor = conn.cursor()
                                    set_clauses = []
                                    values = []
                                    for label, value in edited_entries.items():
                                        set_clauses.append(f"{to_col_name(label)} = ?")
                                        values.append(value)
                                    values.append(record_id_to_manage)
                                    sql = f"UPDATE registros SET {', '.join(set_clauses)} WHERE id = ?"
                                    cursor.execute(sql, values)
                                    conn.commit()
                                    st.success("Registro atualizado com sucesso!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Erro ao salvar: {e}")

                    # Lógica de Exclusão
                    if st.button("Excluir Registro", key=f"delete_{record_id_to_manage}", type="primary"):
                        try:
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM registros WHERE id = ?", (record_id_to_manage,))
                            conn.commit()
                            st.success(f"Registro ID {record_id_to_manage} excluído com sucesso.")
                            # Limpa a seleção para não mostrar mais os detalhes do registro excluído
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao excluir: {e}")

    # --- ABA DE EXPORTAÇÃO ---
    with tab_export:
        st.header("Exportar Dados")
        
        if not EXPORT_LIBS_AVAILABLE:
            st.error("As bibliotecas de exportação (pandas, openpyxl, reportlab) não estão instaladas. A exportação está desativada.")
        else:
            st.info("A exportação usará os livros selecionados no filtro da barra lateral.")
            if not selected_books:
                st.warning("Selecione um ou mais livros para poder exportar.")
            else:
                grouping_key = st.radio("Agrupar dados por:", ("Tipo de Registro", "Livro Fonte"), horizontal=True)
                grouping_col = "tipo_registro" if grouping_key == "Tipo de Registro" else "fonte_livro"

                # Lógica de exportação para Excel
                if st.button("Gerar Arquivo Excel (.xlsx)"):
                    # ... (Lógica de exportação Excel adaptada)
                    st.info("Função de exportação para Excel ainda em implementação.")

                # Lógica de exportação para PDF
                if st.button("Gerar Arquivo PDF"):
                    # ... (Lógica de exportação PDF adaptada)
                    st.info("Função de exportação para PDF ainda em implementação.")

    # É importante fechar a conexão no final
    conn.close()
else:
    st.info("Aguardando o upload do arquivo de banco de dados (.db) na barra lateral para começar.")