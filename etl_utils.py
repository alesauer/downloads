import os  # Adicionando a importação do módulo 'os'
import pymysql
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração de log
log = logging.getLogger("etl_utils")
log.setLevel(logging.DEBUG)

# Função para conectar ao MySQL usando PyMySQL
def mysql_connection():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DB', 'CVCRM')
    )

# Função para conectar ao MySQL usando PyMySQL para o schema de logs
def mysql_connection_log():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', 'teste10'),
        database=os.getenv('LOG_DB', 'log_cvcrm')  # Usando o schema de logs
    )



# Função para upsert usando PyMySQL
def upsert_rows(engine, table_name: str, rows: List[Dict[str, Any]], pk_columns: List[str]) -> int:
    if not rows:
        return 0
    
    # Conectar ao banco MySQL
    connection = mysql_connection()
    
    try:
        with connection.cursor() as cursor:
            for row in rows:
                # Construir a query de upsert
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(row.keys())})
                    VALUES ({', '.join(['%s'] * len(row))})
                    ON DUPLICATE KEY UPDATE
                    {', '.join([f"{k} = VALUES({k})" for k in row.keys()])}
                """
                # Executar a query
                cursor.execute(sql, tuple(row.values()))
                
            # Commit para salvar no banco
            connection.commit()
        
        return len(rows)  # Número de registros afetados (inseridos ou atualizados)
    
    except Exception as e:
        log.error(f"Erro no upsert: {e}")
        connection.rollback()
        return 0
    
    finally:
        connection.close()
