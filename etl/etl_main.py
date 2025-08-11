from extract import extract_all
from transform import build_dim_tributo, build_dim_devedor, build_dim_situacao, build_dim_data, build_fato_cda, build_rel_cda_devedor
from load import load_to_dw, truncate_dw_tables

# Strings de conexão
trans_conn_str = 'postgresql://postgres:postgres@postgres_transacional:5432/transacional_db'
dw_conn_str = 'postgresql://postgres:postgres@postgres_dw:5432/dw_db'

# Extração
dfs = extract_all(trans_conn_str)

print("Contagem de registros nas fontes:")
print("CDA:", len(dfs['cda']))
print("Pessoas:", len(dfs['cda_devedor']))
print("Situações:", len(dfs['situacao']))

# Transformação
df_dim_tributo = build_dim_tributo(dfs['natureza'])
df_dim_devedor = build_dim_devedor(dfs['pessoa'])
df_dim_situacao = build_dim_situacao(dfs['situacao'])
df_dim_data = build_dim_data(dfs['cda'])
df_fato_cda = build_fato_cda(dfs, df_dim_data, df_dim_devedor)
df_rel_cda_devedor = build_rel_cda_devedor(dfs['cda_devedor'], df_dim_devedor)


# Carga
truncate_dw_tables(dw_conn_str)
load_to_dw(df_dim_tributo, 'dim_tributo', dw_conn_str)
load_to_dw(df_dim_devedor, 'dim_devedor', dw_conn_str)
load_to_dw(df_dim_situacao, 'dim_situacao', dw_conn_str)
load_to_dw(df_dim_data, 'dim_data', dw_conn_str)
load_to_dw(df_fato_cda, 'fato_cda', dw_conn_str)
load_to_dw(df_rel_cda_devedor, 'rel_cda_devedor', dw_conn_str)
