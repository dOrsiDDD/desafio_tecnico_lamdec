import pandas as pd
from sqlalchemy import create_engine, text

def truncate_dw_tables(dw_conn_str):
    engine = create_engine(dw_conn_str)
    with engine.begin() as conn:
        conn.execute(text("""
            TRUNCATE TABLE dw.fato_cda,
                            dw.dim_data,
                            dw.dim_devedor,
                            dw.dim_tributo,
                            dw.dim_situacao
            RESTART IDENTITY CASCADE;
        """))
    print("Tabelas do DW truncadas com sucesso.")

def load_to_dw(df, table_name, dw_conn_str):
    from sqlalchemy import create_engine
    engine = create_engine(dw_conn_str)
    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, schema = 'dw', if_exists='append', index=False)
            result = pd.read_sql(f"SELECT COUNT(*) AS count FROM dw.{table_name}", conn)['count'].iloc[0]
            print(f"Dados commitados em {table_name} - Total: {result} registros")
    except Exception as e:
        print(f"Erro ao carregar {table_name}: {str(e)}")