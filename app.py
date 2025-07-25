# app.py - v4 FINAL com funções de exportação
import streamlit as st
import sqlite3
import pandas as pd
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

# --- Definições e constantes (sem alterações) ---
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

# --- Funções de Lógica (quase inalteradas) ---
def to_col_name(field_name):
    return field_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("?", "")

def setup_database(conn):
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

def get_distinct_values(conn, column_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT {column_name} FROM registros WHERE {column_name} IS NOT NULL AND {column_name} != '' ORDER BY {column_name}")
        return [row[0] for row in cursor.fetchall()]
    except: return []

def fetch_records(conn, search_term="", selected_books=None):
    if not selected_books: return pd.DataFrame()
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
    data = []
    for row in cursor.fetchall():
        nome_principal = row[2] or row[3] or row[4] or "N/A"
        data_principal = row[5] or row[6] or "N/A"
        data.append({"ID": row[0], "Tipo": row[1], "Nome Principal": nome_principal, "Data": data_principal, "Livro Fonte": row[7]})
    return pd.DataFrame(data)

def fetch_single_record(conn, record_id):
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM registros WHERE id = ?", (record_id,))
    record = cursor.fetchone()
    return dict(record) if record else None

def fetch_data_for_export(conn, selected_books):
    if not selected_books: return None
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = f"SELECT * FROM registros WHERE fonte_livro IN ({','.join(['?'] * len(selected_books))}) ORDER BY tipo_registro, id"
    cursor.execute(query, selected_books)
    return [dict(row) for row in cursor.fetchall()]

# ####################################################################
# ## INÍCIO DO NOVO BLOCO DE FUNÇÕES DE EXPORTAÇÃO ##
# ####################################################################

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
                tab.tableStyleInfo = style
                worksheet.add_table(tab)
            except Exception as e:
                print(f"Aviso: Não foi possível formatar como Tabela Excel. Motivo: {e}")
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                adjusted_width = (max_length + 2) if max_length < 50 else 50
                worksheet.column_dimensions[column_letter].width = adjusted_width
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
        table_data = [headers]
        for record in records: table_data.append([str(record.get(col, '')) for col in pdf_cols])

        if len(table_data) > 1:
            max_lengths = [max(len(str(item)) for item in col) for col in zip(*table_data)]
            min_col_char_width = 8
            weighted_lengths = [max(l, min_col_char_width) for l in max_lengths]
            total_weight = sum(weighted_lengths)
            available_width = doc.width
            col_widths = [(w / total_weight) * available_width for w in weighted_lengths]

            table = ReportlabTable(table_data, colWidths=col_widths, repeatRows=1)
            style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.darkslategray),('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('FONTSIZE', (0, 1), (-1, -1), 5),('GRID', (0,0), (-1,-1), 0.5, colors.black),
                ('WORDWRAP', (0, 0), (-1, -1), 'CJK')
            ])
            table.setStyle(style)
            elements.append(table)
    doc.build(elements)
    return pdf_buffer.getvalue()

# ####################################################################
# ## FIM DO NOVO BLOCO ##
# ####################################################################


# --- Início da Interface Streamlit ---
st.set_page_config(layout="wide", page_title="CPIndexator Web")
st.title("CPIndexator - Versão 1.1 WEB")

if 'db_bytes' not in st.session_state:
    st.subheader("Bem-vindo ao Indexador Genealógico Online!")
    st.write("Para começar, escolha uma das opções abaixo:")
    col1, col2 = st.columns(2)
    with col1:
        st.info("Opção 1: Já tenho um arquivo")
        uploaded_db = st.file_uploader("Selecione o seu arquivo `.db`:", type=["db"])
        if uploaded_db is not None:
            st.session_state.db_bytes = uploaded_db.getvalue()
            st.session_state.db_name = uploaded_db.name
            st.rerun()
    with col2:
        st.info("Opção 2: Quero começar do zero")
        if st.button("Criar Novo Banco de Dados Vazio"):
            new_db_path = "novo_banco_temporario.db"
            conn = sqlite3.connect(new_db_path)
            setup_database(conn)
            conn.close()
            with open(new_db_path, "rb") as f:
                st.session_state.db_bytes = f.read()
            st.session_state.db_name = "genealogia_novo.db"
            if os.path.exists(new_db_path): os.remove(new_db_path)
            st.rerun()
else:
    db_path = "./database_temp.db"
    with open(db_path, "wb") as f: f.write(st.session_state.db_bytes)
    conn = sqlite3.connect(db_path, check_same_thread=False)

    st.sidebar.header("Salvar/Sair")
    st.sidebar.download_button(
        label="Baixar cópia do Banco de Dados",
        data=st.session_state.db_bytes,
        file_name=st.session_state.db_name,
        mime="application/octet-stream"
    )

    tab_add, tab_manage, tab_export = st.tabs(["➕ Adicionar Registro", "🔍 Consultar e Gerenciar", "📤 Exportar Dados"])
    
    # ... (código das abas Adicionar e Gerenciar, sem alterações) ...
    with tab_add:
        st.header("Adicionar Novo Registro")
        all_books = get_distinct_values(conn, "fonte_livro")
        all_locations = get_distinct_values(conn, "local_do_evento")
        col1, col2 = st.columns(2)
        with col1: book_preset = st.selectbox("Preencher 'Fonte (Livro)' com:", [""] + all_books, key="book_preset_add")
        with col2: location_preset = st.selectbox("Preencher 'Local do Evento' com:", [""] + all_locations, key="location_preset_add")
        record_type = st.selectbox("Tipo de Registro:", list(FORM_DEFINITIONS.keys()), index=None, placeholder="Selecione...")
        if record_type:
            with st.form("new_record_form", clear_on_submit=True):
                fields_for_type = FORM_DEFINITIONS.get(record_type, []); final_fields = fields_for_type + COMMON_FIELDS
                form_entries = {}
                for field in final_fields:
                    default_value = ""
                    if field == "Fonte (Livro)" and book_preset: default_value = book_preset
                    elif field == "Local do Evento" and location_preset: default_value = location_preset
                    form_entries[field] = st.text_input(f"{field}:", value=default_value, key=f"add_{to_col_name(field)}")
                submitted = st.form_submit_button(f"Adicionar Registro de {record_type}")
                if submitted:
                    try:
                        cursor = conn.cursor()
                        columns = ["tipo_registro"]; values = [record_type]
                        for label, value in form_entries.items():
                            columns.append(to_col_name(label)); values.append(value)
                        placeholders = ', '.join(['?'] * len(columns))
                        sql = f"INSERT INTO registros ({', '.join(columns)}) VALUES ({placeholders})"
                        cursor.execute(sql, values); conn.commit()
                        st.success("Registro adicionado com sucesso!")
                        with open(db_path, "rb") as f: st.session_state.db_bytes = f.read()
                    except Exception as e: st.error(f"Ocorreu um erro: {e}")

    with tab_manage:
        st.header("Consultar Registros")
        st.sidebar.header("Filtros de Consulta")
        all_books = get_distinct_values(conn, "fonte_livro")
        selected_books = st.sidebar.multiselect("Filtrar por Livro(s):", all_books, default=all_books)
        search_term = st.sidebar.text_input("Busca Rápida por Termo:")
        if not selected_books: st.warning("Por favor, selecione ao menos um livro no filtro da barra lateral.")
        else:
            df_records = fetch_records(conn, search_term, selected_books)
            st.dataframe(df_records, use_container_width=True, hide_index=True)
            st.header("Gerenciar Registro Selecionado")
            record_id_to_manage = st.number_input("Digite o ID do registro para ver detalhes, editar ou excluir:", min_value=1, step=1, value=None)
            if record_id_to_manage:
                record = fetch_single_record(conn, record_id_to_manage)
                if record:
                    column_mapper = {to_col_name(f): f for fields in FORM_DEFINITIONS.values() for f in fields}
                    column_mapper.update({to_col_name(f): f for f in COMMON_FIELDS})
                    column_mapper['id'] = 'ID'; column_mapper['tipo_registro'] = 'Tipo de Registro'
                    with st.expander("Ver Detalhes Completos", expanded=True):
                        details_str = ""
                        for key, value in record.items():
                            if value:
                                friendly_name = column_mapper.get(key, key.replace('_', ' ').title())
                                details_str += f"**{friendly_name}:** {value}\n\n"
                        st.markdown(details_str)
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
                                    set_clauses = []; values = []
                                    for label, value in edited_entries.items():
                                        set_clauses.append(f"{to_col_name(label)} = ?"); values.append(value)
                                    values.append(record_id_to_manage)
                                    sql = f"UPDATE registros SET {', '.join(set_clauses)} WHERE id = ?"
                                    cursor.execute(sql, values); conn.commit()
                                    st.success("Registro atualizado com sucesso!")
                                    with open(db_path, "rb") as f: st.session_state.db_bytes = f.read()
                                except Exception as e: st.error(f"Erro ao salvar: {e}")
                    if st.button("Excluir Registro", key=f"delete_{record_id_to_manage}", type="primary"):
                        try:
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM registros WHERE id = ?", (record_id_to_manage,)); conn.commit()
                            st.success(f"Registro ID {record_id_to_manage} excluído com sucesso.")
                            with open(db_path, "rb") as f: st.session_state.db_bytes = f.read()
                            st.rerun()
                        except Exception as e: st.error(f"Erro ao excluir: {e}")

    with tab_export:
        st.header("Exportar Dados")
        if not EXPORT_LIBS_AVAILABLE:
            st.error("Bibliotecas de exportação não encontradas. Função desativada.")
        else:
            st.info("A exportação usará os livros selecionados no filtro da barra lateral.")
            # Reutiliza a seleção de livros do filtro da aba de consulta
            all_books_export = get_distinct_values(conn, "fonte_livro")
            selected_books_export = st.multiselect("Selecione os livros para exportar:", all_books_export, default=all_books_export, key="export_books_select")

            if not selected_books_export:
                st.warning("Selecione ao menos um livro para exportar.")
            else:
                grouping_key = st.radio("Agrupar dados por:", ("Tipo de Registro", "Livro Fonte"), horizontal=True, key="export_grouping")
                grouping_col = "tipo_registro" if grouping_key == "Tipo de Registro" else "fonte_livro"

                col_exp1, col_exp2 = st.columns(2)
                with col_exp1:
                    if st.button("Gerar Arquivo Excel (.xlsx)"):
                        with st.spinner("Gerando arquivo Excel..."):
                            all_data = fetch_data_for_export(conn, selected_books_export)
                            if all_data:
                                column_mapper = {to_col_name(f): f for fields in FORM_DEFINITIONS.values() for f in fields}
                                column_mapper.update({to_col_name(f): f for f in COMMON_FIELDS})
                                column_mapper['id'] = 'ID'; column_mapper['tipo_registro'] = 'Tipo de Registro'
                                
                                st.session_state.excel_export = generate_excel_bytes(all_data, grouping_col, column_mapper)
                                st.session_state.pdf_export = None # Limpa o outro para não mostrar os dois botões
                            else:
                                st.warning("Nenhum dado encontrado para exportar.")
                
                with col_exp2:
                    if st.button("Gerar Arquivo PDF"):
                        with st.spinner("Gerando arquivo PDF..."):
                            all_data = fetch_data_for_export(conn, selected_books_export)
                            if all_data:
                                column_mapper = {to_col_name(f): f for fields in FORM_DEFINITIONS.values() for f in fields}
                                column_mapper.update({to_col_name(f): f for f in COMMON_FIELDS})
                                column_mapper['id'] = 'ID'; column_mapper['tipo_registro'] = 'Tipo de Registro'

                                st.session_state.pdf_export = generate_pdf_bytes(all_data, grouping_col, column_mapper)
                                st.session_state.excel_export = None # Limpa o outro
                            else:
                                st.warning("Nenhum dado encontrado para exportar.")

                # Mostra os botões de download se os arquivos foram gerados
                if 'excel_export' in st.session_state and st.session_state.excel_export:
                    st.download_button(
                        label="Clique para Baixar o Arquivo Excel",
                        data=st.session_state.excel_export,
                        file_name="relatorio_genealogico.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                if 'pdf_export' in st.session_state and st.session_state.pdf_export:
                    st.download_button(
                        label="Clique para Baixar o Arquivo PDF",
                        data=st.session_state.pdf_export,
                        file_name="relatorio_genealogico.pdf",
                        mime="application/pdf"
                    )

    conn.close()
    if os.path.exists(db_path): os.remove(db_path)