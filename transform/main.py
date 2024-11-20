import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do banco
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Criação da conexão com o banco
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# Carrega os links do JSONL
df = pd.read_json("scrapyfada/data/data.jsonl", lines=True)
links_json = df['link'].tolist()  # Lista dos links do JSON

# Consulta para obter todos os links do banco
query_get_links = text("SELECT link_site FROM bd_fada")

# Inicializa as variáveis para armazenar os resultados
links_to_delete = []  # Links do banco que não estão no JSON
links_not_found = []  # Links do JSON que não estão no banco

# Conexão com o banco
with engine.connect() as conn:
    # Obter todos os links do banco
    result = conn.execute(query_get_links)
    links_in_db = [row[0] for row in result]  # Tupla -> índice 0

    # Identificar links no banco que não estão no JSON
    links_to_delete = [link for link in links_in_db if link not in links_json]

    # Identificar links do JSON que não estão no banco
    links_not_found = [link for link in links_json if link not in links_in_db]

    # Deletar links que estão no banco, mas não estão no JSON
    if links_to_delete:
        print(f"Tentando deletar {len(links_to_delete)} links que não estão no JSON...")
        query_delete = text("""
        DELETE FROM bd_fada
        WHERE link_site = ANY(:links)
        """)
        conn.execute(query_delete, {"links": links_to_delete})
        print(f"Links deletados do banco: {links_to_delete}")
    else:
        print("Nenhum link para deletar.")

# Exibe os links que não foram encontrados no banco (precisam de scraping detalhado)
if links_not_found:
    print(f"Links que precisam de scraping detalhado: {len(links_not_found)}")
    print(links_not_found)
else:
    print("Todos os links do JSON foram encontrados no banco.")