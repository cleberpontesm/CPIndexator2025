# app.py - VERSÃO FINAL COM DUPLA OPÇÃO DE PDF - CPIndexator com Supabase DB e Autenticação
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
    from reportlab.lib.pagesizes import A4, A3, letter, landscape
    from reportlab.platypus import SimpleDocTemplate, Table as ReportlabTable, TableStyle, Paragraph, PageBreak, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.enums import TA_LEFT, TA_CENTER
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
    "Nascimento/Batismo": ["id", "tipo_registro"] + [f.lower().replace(" ", "_") for f in FORM_DEFINITIONS["Nascimento/Batismo"] + COMMON_FIELDS] + ['ultima_alteracao_por'],
    "Casamento": ["id", "tipo_registro"] + [f.lower().replace(" ", "_") for f in FORM_DEFINITIONS["Casamento"] + COMMON_FIELDS] + ['ultima_alteracao_por'],
    "Óbito": ["id", "tipo_registro"] + [f.lower().replace(" ", "_").replace("?", "") for f in FORM_DEFINITIONS["Óbito"] + COMMON_FIELDS] + ['ultima_alteracao_por']
}

# Mapeamento de nomes de colunas para labels amigáveis
COLUMN_LABELS = {
    'id': 'ID',
    'tipo_registro': 'Tipo de Registro',
    'data_do_registro': 'Data do Registro',
    'data_do_evento': 'Data do Evento',
    'data_do_obito': 'Data do Óbito',
    'local_do_evento': 'Local do Evento',
    'local_do_obito': 'Local do Óbito',
    'nome_do_registrado': 'Nome do Registrado',
    'nome_do_pai': 'Nome do Pai',
    'nome_da_mae': 'Nome da Mãe',
    'padrinhos': 'Padrinhos',
    'avo_paterno': 'Avô Paterno',
    'avo_paterna': 'Avó Paterna',
    'avo_materno': 'Avô Materno',
    'avo_materna': 'Avó Materna',
    'nome_do_noivo': 'Nome do Noivo',
    'idade_do_noivo': 'Idade do Noivo',
    'pai_do_noivo': 'Pai do Noivo',
    'mae_do_noivo': 'Mãe do Noivo',
    'nome_da_noiva': 'Nome da Noiva',
    'idade_da_noiva': 'Idade da Noiva',
    'pai_da_noiva': 'Pai da Noiva',
    'mae_da_noiva': 'Mãe da Noiva',
    'testemunhas': 'Testemunhas',
    'nome_do_falecido': 'Nome do Falecido',
    'idade_no_obito': 'Idade no Óbito',
    'filiacao': 'Filiação',
    'conjuge_sobrevivente': 'Cônjuge Sobrevivente',
    'deixou_filhos': 'Deixou Filhos',
    'causa_mortis': 'Causa Mortis',
    'local_do_sepultamento': 'Local do Sepultamento',
    'fonte_livro': 'Fonte (Livro)',
    'fonte_pagina_folha': 'Fonte (Página/Folha)',
    'observacoes': 'Observações',
    'caminho_da_imagem': 'Caminho da Imagem',
    'ultima_alteracao_por': 'Última Alteração Por'
}

# Colunas essenciais para a visualização em tabela (índice/catálogo)
TABLE_COLUMNS = {
    "Nascimento/Batismo": ['id', 'nome_do_registrado', 'data_do_registro', 'data_do_evento', 'nome_do_pai', 'nome_da_mae', 'avo_paterno', 'avo_paterna', 'avo_materno', 'avo_materna', 'fonte_livro', 'fonte_pagina_folha'],
    "Casamento": ['id', 'nome_do_noivo', 'nome_da_noiva', 'data_do_registro', 'data_do_evento', 'pai_do_noivo', 'mae_do_noivo', 'fonte_livro', 'fonte_pagina_folha'],
    "Óbito": ['id', 'nome_do_falecido', 'data_do_registro', 'data_do_obito', 'idade_no_obito', 'causa_mortis', 'fonte_livro', 'fonte_pagina_folha']
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

def generate_pdf_table(records_by_type):
    """Gera arquivo PDF em formato de tabela (índice/catálogo)"""
    if not EXPORT_LIBS_AVAILABLE:
        st.error("Bibliotecas de exportação não disponíveis.")
        return None
    
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=landscape(A3))
    story = []
    styles = getSampleStyleSheet()
    
    # Estilos personalizados
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=30,
        alignment=TA_CENTER
    )
    
    section_style = ParagraphStyle(
        'SectionTitle',
        parent=styles['Heading2'],
        fontSize=18,
        textColor=colors.HexColor('#2e5090'),
        spaceAfter=20
    )
    
    # Título principal
    story.append(Paragraph("Índice de Registros - CPIndexator", title_style))
    story.append(Spacer(1, 0.5*inch))
    
    for record_type, records in records_by_type.items():
        if records:
            # Título da seção
            story.append(Paragraph(f"Registros de {record_type}", section_style))
            story.append(Paragraph(f"Total: {len(records)} registros", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            # Preparar dados para tabela
            df = pd.DataFrame(records)
            
            # Selecionar apenas as colunas essenciais para visualização em tabela
            if record_type in TABLE_COLUMNS:
                columns_to_show = [col for col in TABLE_COLUMNS[record_type] if col in df.columns]
            else:
                # Fallback para colunas básicas
                columns_to_show = ['id', 'tipo_registro']
                if 'nome_do_registrado' in df.columns: columns_to_show.append('nome_do_registrado')
                if 'nome_do_noivo' in df.columns: columns_to_show.append('nome_do_noivo')
                if 'nome_do_falecido' in df.columns: columns_to_show.append('nome_do_falecido')
                if 'data_do_evento' in df.columns: columns_to_show.append('data_do_evento')
                if 'data_do_obito' in df.columns: columns_to_show.append('data_do_obito')
                if 'fonte_livro' in df.columns: columns_to_show.append('fonte_livro')
                if 'fonte_pagina_folha' in df.columns: columns_to_show.append('fonte_pagina_folha')
            
            df_filtered = df[columns_to_show]
            
            # Criar cabeçalhos com labels amigáveis
            headers = [COLUMN_LABELS.get(col, col.replace('_', ' ').title()) for col in columns_to_show]
            
            # Preparar dados da tabela
            data = [headers]
            for _, row in df_filtered.iterrows():
                row_data = []
                for col in columns_to_show:
                    value = str(row[col]) if pd.notna(row[col]) else ''
                    # Limitar tamanho do texto para caber na tabela
                    if len(value) > 50:
                        value = value[:47] + '...'
                    row_data.append(value)
                data.append(row_data)
            
            # Criar tabela
            table = ReportlabTable(data)
            
            # Estilo da tabela
            table_style = TableStyle([
                # Cabeçalho
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4788')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                
                # Corpo da tabela
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')]),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
                ('TOPPADDING', (0, 1), (-1, -1), 6),
            ])
            
            table.setStyle(table_style)
            story.append(table)
            story.append(PageBreak())
    
    doc.build(story)
    return output.getvalue()

def generate_pdf_detailed(records_by_type):
    """Gera arquivo PDF com TODOS os campos dos registros (relatório detalhado)"""
    if not EXPORT_LIBS_AVAILABLE:
        st.error("Bibliotecas de exportação não disponíveis.")
        return None
    
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
    story = []
    styles = getSampleStyleSheet()
    
    # Criar estilos personalizados
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=30,
        alignment=TA_CENTER
    )
    
    section_style = ParagraphStyle(
        'SectionTitle',
        parent=styles['Heading2'],
        fontSize=18,
        textColor=colors.HexColor('#2e5090'),
        spaceAfter=20,
        spaceBefore=30
    )
    
    record_header_style = ParagraphStyle(
        'RecordHeader',
        parent=styles['Heading3'],
        fontSize=14,
        textColor=colors.HexColor('#333333'),
        spaceAfter=12,
        leftIndent=20
    )
    
    field_style = ParagraphStyle(
        'FieldStyle',
        parent=styles['Normal'],
        fontSize=11,
        leftIndent=40,
        spaceAfter=8
    )
    
    # Adicionar título principal
    story.append(Paragraph("Relatório Detalhado de Registros - CPIndexator", title_style))
    story.append(Spacer(1, 0.5*inch))
    
    # Processar registros por tipo
    for record_type, records in records_by_type.items():
        if records:
            # Título da seção
            story.append(Paragraph(f"Registros de {record_type}", section_style))
            story.append(Paragraph(f"Total de registros: {len(records)}", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            # Processar cada registro individualmente
            for idx, record in enumerate(records, 1):
                # Cabeçalho do registro
                nome_principal = record.get('nome_do_registrado') or record.get('nome_do_noivo') or record.get('nome_do_falecido') or 'Sem nome'
                header_text = f"Registro #{idx} - ID: {record.get('id', 'N/A')} - {nome_principal}"
                story.append(Paragraph(header_text, record_header_style))
                
                # Criar uma tabela de duas colunas para os campos
                data = []
                
                # Determinar quais campos mostrar baseado no tipo de registro
                if record_type == "Nascimento/Batismo":
                    fields_order = ['id', 'tipo_registro'] + [to_col_name(f) for f in FORM_DEFINITIONS["Nascimento/Batismo"] + COMMON_FIELDS] + ['ultima_alteracao_por']
                elif record_type == "Casamento":
                    fields_order = ['id', 'tipo_registro'] + [to_col_name(f) for f in FORM_DEFINITIONS["Casamento"] + COMMON_FIELDS] + ['ultima_alteracao_por']
                elif record_type == "Óbito":
                    fields_order = ['id', 'tipo_registro'] + [to_col_name(f) for f in FORM_DEFINITIONS["Óbito"] + COMMON_FIELDS] + ['ultima_alteracao_por']
                else:
                    fields_order = sorted(record.keys())
                
                # Adicionar campos à tabela
                for field in fields_order:
                    if field in record and record[field]:
                        # Obter o label amigável do campo
                        label = COLUMN_LABELS.get(field, field.replace('_', ' ').title())
                        value = str(record[field])
                        
                        # Quebrar valores muito longos
                        if len(value) > 60:
                            value = Paragraph(value, styles['Normal'])
                        
                        data.append([Paragraph(f"<b>{label}:</b>", field_style), value])
                
                # Se não há dados, adicionar mensagem
                if not data:
                    data.append([Paragraph("Sem dados disponíveis", field_style), ""])
                
                # Criar tabela
                table = ReportlabTable(data, colWidths=[2.5*inch, 4*inch])
                table.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 0), (-1, -1), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
                    ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0f0f0')),
                ]))
                
                story.append(table)
                story.append(Spacer(1, 0.3*inch))
                
                # Adicionar linha divisória entre registros
                if idx < len(records):
                    story.append(Paragraph("<hr/>", styles['Normal']))
                    story.append(Spacer(1, 0.1*inch))
            
            # Quebra de página após cada tipo de registro
            story.append(PageBreak())
    
    # Construir o PDF
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
        # Limpar todo o session_state ao sair para evitar vazamento de dados entre sessões
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    st.title("CPIndexator - Painel Principal")

    # --- LÓGICA DE CONTROLE DE ACESSO PARA AS ABAS (VERSÃO CORRIGIDA) ---
    
    # Pega o e-mail do usuário logado de forma segura
    user_email = ""
    if hasattr(st.session_state, 'user') and st.session_state.user is not None:
        user_email = st.session_state.user.email
    
    # Pega a lista de administradores dos secrets. Retorna uma lista vazia se não for encontrada.
    admin_list = st.secrets.get("ADMIN_USERS", [])

    # Verifica se o usuário é um administrador
    is_admin = user_email in admin_list

    # Define a lista de abas a serem criadas
    tabs_to_create = [
        "➕ Adicionar Registro", 
        "🔍 Consultar e Gerenciar", 
        "📤 Exportar Dados"
    ]
    if is_admin:
        tabs_to_create.append("⚙️ Administração")

    # Cria as abas
    created_tabs = st.tabs(tabs_to_create)

    # Atribui as abas a variáveis para facilitar o acesso
    tab_add = created_tabs[0]
    tab_manage = created_tabs[1]
    tab_export = created_tabs[2]
    if is_admin:
        tab_admin = created_tabs[3]

    # --- FIM DA LÓGICA DE CONTROLE DE ACESSO ---

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
                            # Adiciona o usuário da alteração ao inserir
                            cols = ["tipo_registro"] + [to_col_name(label) for label in entries.keys()] + ["ultima_alteracao_por"]
                            vals = [record_type] + [value for value in entries.values()] + [user_email] # Usa user_email
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
            record_id_to_manage = st.number_input("Digite o ID do registro para ver detalhes, editar ou excluir:", min_value=1, step=1, value=None, key="record_id_input")
            
            if record_id_to_manage:
                if 'record_id' not in st.session_state or st.session_state.record_id != record_id_to_manage:
                    st.session_state.record_id = record_id_to_manage
                    if 'manage_action' in st.session_state:
                        del st.session_state.manage_action

                record = fetch_single_record(record_id_to_manage)
                if record:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("📋 Ver Detalhes", use_container_width=True):
                            st.session_state.manage_action = "view"
                            st.rerun()
                    with col2:
                        if st.button("✏️ Editar", use_container_width=True):
                            st.session_state.manage_action = "edit"
                            st.rerun()
                    with col3:
                        if st.button("🗑️ Excluir", use_container_width=True):
                            st.session_state.manage_action = "delete"
                            st.rerun()
                else:
                    st.error(f"Registro ID {record_id_to_manage} não encontrado.")

            if 'manage_action' in st.session_state and 'record_id' in st.session_state and st.session_state.record_id:
                record_id = st.session_state.record_id
                record = fetch_single_record(record_id)
                if not record:
                    st.error(f"Registro ID {record_id} não encontrado. Pode ter sido excluído.")
                    return

                action = st.session_state.manage_action
                st.subheader(f"Ação: {action.title()} | Registro ID: {record_id}")
                st.markdown("---")

                if action == "view":
                    for key, value in record.items():
                        if value:
                            label = COLUMN_LABELS.get(key, key.replace('_', ' ').title())
                            st.write(f"**{label}:** {value}")
                
                elif action == "edit":
                    record_type = record.get('tipo_registro')
                    if not record_type:
                        st.error("Tipo de registro não definido. Não é possível editar.")
                        return

                    with st.form("edit_record_form"):
                        st.info(f"Editando registro de {record_type}")
                        fields = FORM_DEFINITIONS.get(record_type, []) + COMMON_FIELDS
                        updated_entries = {}
                        
                        for field in fields:
                            col_name = to_col_name(field)
                            current_value = record.get(col_name, "")
                            updated_entries[col_name] = st.text_input(f"{field}:", value=current_value, key=f"edit_{col_name}")

                        submitted = st.form_submit_button("Salvar Alterações")
                        if submitted:
                            try:
                                with engine.connect() as conn:
                                    set_clause = ", ".join([f"{col} = :{col}" for col in updated_entries.keys()])
                                    set_clause += ", ultima_alteracao_por = :user_email"
                                    
                                    query = text(f"UPDATE registros SET {set_clause} WHERE id = :id")
                                    
                                    params = updated_entries
                                    params['id'] = record_id
                                    params['user_email'] = user_email # Usa user_email
                                    
                                    conn.execute(query, params)
                                    conn.commit()
                                    st.success("Registro atualizado com sucesso!")
                                    del st.session_state.manage_action
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Ocorreu um erro ao atualizar: {e}")
                
                elif action == "delete":
                    st.warning(f"Tem certeza que deseja excluir o registro ID {record_id}?")
                    if st.button("Confirmar Exclusão", type="primary"):
                        try:
                            with engine.connect() as conn:
                                conn.execute(text("DELETE FROM registros WHERE id = :id"), {'id': record_id})
                                conn.commit()
                                st.success("Registro excluído com sucesso!")
                                del st.session_state.manage_action
                                del st.session_state.record_id
                                st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao excluir: {e}")

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
                    
                    pdf_style = None
                    if export_format == "PDF":
                        st.subheader("Opções de PDF")
                        pdf_style = st.radio(
                            "Estilo do PDF:",
                            ["Tabela (Índice/Catálogo)", "Relatório Detalhado"],
                            help="**Tabela**: Visão geral compacta com campos principais\n\n**Relatório Detalhado**: Todos os campos de cada registro"
                        )
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if pdf_style == "Tabela (Índice/Catálogo)":
                                st.info("📊 **Formato Tabela**\n\nIdeal para ter uma visão geral dos registros, como um índice ou catálogo. Mostra apenas os campos essenciais em formato compacto.")
                        with col2:
                            if pdf_style == "Relatório Detalhado":
                                st.info("📋 **Formato Detalhado**\n\nExibe todos os campos de cada registro. Ideal para análise completa ou impressão de fichas individuais.")
                    
                    if st.button("Gerar Arquivo para Download", type="primary"):
                        try:
                            with engine.connect() as conn:
                                query = text("SELECT * FROM registros WHERE fonte_livro = ANY(:books) ORDER BY tipo_registro, id")
                                result = conn.execute(query, {'books': selected_books_export})
                                all_records = [dict(row._mapping) for row in result]
                                
                                if all_records:
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
                                        if pdf_style == "Tabela (Índice/Catálogo)":
                                            file_bytes = generate_pdf_table(dict(records_by_type))
                                            filename = "cpindexator_indice.pdf"
                                        else:
                                            file_bytes = generate_pdf_detailed(dict(records_by_type))
                                            filename = "cpindexator_relatorio_detalhado.pdf"
                                        
                                        if file_bytes:
                                            st.download_button(
                                                label="📥 Baixar PDF",
                                                data=file_bytes,
                                                file_name=filename,
                                                mime="application/pdf"
                                            )
                                else:
                                    st.warning("Nenhum registro encontrado nos livros selecionados.")
                        except Exception as e:
                            st.error(f"Erro ao gerar arquivo: {e}")
        else:
            st.error("Bibliotecas de exportação não instaladas. Instale openpyxl e reportlab.")

    # O conteúdo da aba de administração só é processado se o usuário for admin
    if is_admin:
        with tab_admin:
            st.header("⚙️ Administração do Banco de Dados")
            st.markdown("---")

            # Seção de Exportação (Backup)
            st.subheader("Exportar Backup Completo")
            st.info("Esta função exporta **todos** os registros da tabela para um arquivo CSV, que pode ser usado como backup.")
            if st.button("Gerar Arquivo de Backup (CSV)"):
                try:
                    with engine.connect() as conn:
                        df = pd.read_sql_table('registros', conn)
                        csv = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Baixar Backup CSV",
                            data=csv,
                            file_name="cpindexator_backup_completo.csv",
                            mime="text/csv",
                        )
                except Exception as e:
                    st.error(f"Erro ao exportar o banco de dados: {e}")

            st.markdown("---")

            # Seção de Importação (Restaurar)
            st.subheader("Importar de um Backup")
            st.warning("🚨 **Atenção:** A importação irá **APAGAR TODOS OS REGISTROS ATUAIS** antes de carregar os novos dados do arquivo. Use com cuidado!")
            
            uploaded_file = st.file_uploader("Escolha um arquivo CSV de backup", type="csv")
            
            if uploaded_file is not None:
                confirm_import = st.checkbox("Confirmo que entendo que todos os dados atuais serão substituídos.")
                if st.button("Iniciar Importação", disabled=not confirm_import):
                    if confirm_import:
                        try:
                            df_to_import = pd.read_csv(uploaded_file)
                            with engine.connect() as conn:
                                # Transação: apaga tudo e depois insere. Se a inserção falhar, o rollback é automático.
                                with conn.begin(): 
                                    conn.execute(text("DELETE FROM registros"))
                                    df_to_import.to_sql('registros', conn, if_exists='append', index=False)
                                
                                st.success(f"Importação concluída com sucesso! {len(df_to_import)} registros foram importados.")
                                st.info("A página será atualizada para refletir os novos dados.")
                                st.rerun()

                        except Exception as e:
                            st.error(f"Erro durante a importação: {e}")
                            st.info("A operação foi revertida. Seus dados antigos estão seguros.")
                    else:
                        st.error("Você precisa confirmar a ação para continuar.")


# --- ROTEADOR PRINCIPAL ---
if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    login_form()
else:
    main_app()
