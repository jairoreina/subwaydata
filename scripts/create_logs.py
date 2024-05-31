import os
from sqlalchemy import create_engine, text

db_name = os.environ["SUBWAYDB_N"]
db_user = os.environ["SUBWAYDB_U"]
db_pass = os.environ["SUBWAYDB_P"]
engine = create_engine(f'postgresql://{db_user}:{db_pass}@localhost/{db_name}')

def create_query_logs_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS query_logs (
        id SERIAL PRIMARY KEY,
        user_query TEXT NOT NULL,
        initial_response_json JSONB NOT NULL,
        corrected_response_json JSONB,
        query_result JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        
def create_error_log_table():
    create_error_table_sql = """
    CREATE TABLE IF NOT EXISTS error_logs (
        id SERIAL PRIMARY KEY,
        user_query TEXT NOT NULL,
        error_message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_error_table_sql))
        conn.commit()
        
if __name__ == "__main__":
    create_query_logs_table()
    create_error_log_table()
    print("Query logs table created successfully.")