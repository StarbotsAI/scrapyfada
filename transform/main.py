import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import json

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do banco
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Define o caminho do arquivo de saída
output_file = "scrapyfada/data/links_not_found.jsonl"

# Remove o arquivo se já existir
if os.path.exists(output_file):
    os.remove(output_file)

try:
    # Estabelece a conexão
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    
    # Cria um cursor
    cur = conn.cursor()
    
    # Carrega os links do JSONL
    df = pd.read_json("scrapyfada/data/data.jsonl", lines=True)
    links_json = df['link'].tolist()
    
    # Consulta para obter todos os links do banco
    cur.execute("SELECT link_site FROM bd_fada")
    links_in_db = [row[0] for row in cur.fetchall()]
    
    # Identificar links no banco que não estão no JSON
    links_to_delete = [link for link in links_in_db if link not in links_json]
    
    # Identificar links do JSON que não estão no banco
    links_not_found = [link for link in links_json if link not in links_in_db]
    
    # Deletar links que estão no banco, mas não estão no JSON
    if links_to_delete:
        print(f"Tentando deletar {len(links_to_delete)} links que não estão no JSON...")
        delete_query = """
        DELETE FROM bd_fada
        WHERE link_site = ANY(%s)
        """
        cur.execute(delete_query, (links_to_delete,))
        conn.commit()
        print(f"Links deletados do banco: {links_to_delete}")
    else:
        print("Nenhum link para deletar.")
    
    # Exibe os links que não foram encontrados no banco
    if links_not_found:
        print(f"Links que precisam de scraping detalhado: {len(links_not_found)}")
        print(links_not_found)
        
        # Salva os links não encontrados em um arquivo JSONL sem escape
        with open(output_file, 'w') as f:
            for link in links_not_found:
                json.dump({'link': link}, f, ensure_ascii=False)
                f.write('\n')
        print(f"Arquivo JSONL criado em: {output_file}")
    else:
        print("Todos os links do JSON foram encontrados no banco.")

except psycopg2.Error as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    if 'conn' in locals():
        conn.rollback()
    
finally:
    # Fecha o cursor e a conexão
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()