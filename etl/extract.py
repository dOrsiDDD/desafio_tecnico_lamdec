import pandas as pd
from sqlalchemy import create_engine

def extract_all(trans_conn_str):
    engine = create_engine(trans_conn_str)
    df_cda = pd.read_sql('SELECT * FROM transacional.cda', engine)
    df_pessoa = pd.read_sql('SELECT * FROM transacional.pessoa', engine)
    df_natureza = pd.read_sql('SELECT * FROM transacional.natureza_divida', engine)
    df_situacao = pd.read_sql('SELECT * FROM transacional.situacao_cda', engine)
    df_cda_score = pd.read_sql('SELECT * FROM transacional.cda_score', engine)
    df_cda_devedor = pd.read_sql('SELECT * FROM transacional.cda_devedor', engine)
    return {
        'cda': df_cda,
        'pessoa': df_pessoa,
        'natureza': df_natureza,
        'situacao': df_situacao,
        'cda_score': df_cda_score,
        'cda_devedor': df_cda_devedor
    }