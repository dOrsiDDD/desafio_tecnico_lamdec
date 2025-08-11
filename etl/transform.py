import pandas as pd

def build_dim_tributo(df_natureza):
    print(f"Registros na origem (natureza): {len(df_natureza)}")
    dim_natureza = df_natureza.rename(columns={
        'id_natureza_divida': 'id_natureza_divida',
        'nome_natureza_divida': 'nome',
        'descricao': 'descricao'
    })[['id_natureza_divida', 'nome', 'descricao']]
    print(f"Registros na dimensão (tributo): {len(dim_natureza)}")
    return dim_natureza

def build_dim_devedor(df_pessoa):
    print(f"Registros na origem (pessoa): {len(df_pessoa)}")
    dim_devedor =  df_pessoa.rename(columns={
        'id_pessoa': 'id_pessoa',
        'nome': 'nome',
        'tipo_pessoa': 'tipo_pessoa',
        'cpf': 'cpf',
        'cnpj': 'cnpj'
    })[['id_pessoa', 'nome', 'tipo_pessoa', 'cpf', 'cnpj']].drop_duplicates(subset=['id_pessoa'])
    print(f"Registros na dimensão (devedor): {len(dim_devedor)}")
    return dim_devedor

def build_dim_situacao(df_situacao):
    print(f"Registros na origem (situação): {len(df_situacao)}")
    dim_situacao = df_situacao.rename(columns={
        'cod_situacao_cda': 'cod_situacao_cda',
        'nome_situacao_cda': 'nome_situacao_cda',
        'tipo_situacao': 'tipo_situacao'
    })[['cod_situacao_cda', 'nome_situacao_cda', 'tipo_situacao']].drop_duplicates(subset=['cod_situacao_cda'])
    print(f"Registros na dimensão (situação): {len(dim_situacao)}")
    return dim_situacao

def build_dim_data(df_cda):
    print(f"Registros na origem (cda): {len(df_cda)}")
    # Normaliza as colunas de data e extrai todas as datas únicas (cadastro + situacao)
    df_cda = df_cda.copy()
    df_cda['data_cadastramento'] = pd.to_datetime(df_cda['data_cadastramento'], errors='coerce')
    df_cda['data_situacao'] = pd.to_datetime(df_cda['data_situacao'], errors='coerce')

    # Trabalhar com a data (apenas parte date) para evitar divergência por horas
    datas_cadastro = df_cda['data_cadastramento'].dropna().dt.date
    datas_situacao = df_cda['data_situacao'].dropna().dt.date

    unique_dates = pd.Series(pd.concat([datas_cadastro, datas_situacao]).unique())
    df_dim_data = pd.DataFrame({'data': pd.to_datetime(unique_dates)})
    df_dim_data['ano'] = df_dim_data['data'].dt.year
    df_dim_data['mes'] = df_dim_data['data'].dt.month
    df_dim_data['dia'] = df_dim_data['data'].dt.day
    df_dim_data = df_dim_data.sort_values('data').reset_index(drop=True)
    df_dim_data['id_data'] = df_dim_data.index + 1
    dim_data = df_dim_data[['id_data', 'data', 'ano', 'mes', 'dia']]
    print(f"Registros na dimensão (data): {len(dim_data)}")
    return dim_data

def build_fato_cda(dfs, df_dim_data, df_dim_devedor=None):
    print(f"Registros na origem (cda, cda_score): {len(dfs['cda'])}, {len(dfs['cda_score'])}")
    df = dfs['cda'].copy()
    df['data_cadastramento'] = pd.to_datetime(df['data_cadastramento'], errors='coerce')
    df['data_situacao'] = pd.to_datetime(df['data_situacao'], errors='coerce')

    # Merge com score (1:1 esperado por id_cobranca)
    df = df.merge(dfs['cda_score'], on='id_cobranca', how='left')

    # Preparar mapeamento de data (garantir unicidade)
    dim_map = df_dim_data[['id_data', 'data']].copy()
    dim_map['data_date'] = dim_map['data'].dt.date
    dim_map = dim_map.drop_duplicates(subset=['data_date'])

    # Mapear id_data_cadastro
    cadastro_map = dim_map.rename(columns={'id_data': 'id_data_cadastro'})
    df = df.merge(cadastro_map[['id_data_cadastro', 'data_date']],
                  left_on=df['data_cadastramento'].dt.date,
                  right_on='data_date', how='left').drop(columns=['data_date'])

    # Mapear id_data_situacao
    situacao_map = dim_map.rename(columns={'id_data': 'id_data_situacao'})
    df = df.merge(situacao_map[['id_data_situacao', 'data_date']],
                  left_on=df['data_situacao'].dt.date,
                  right_on='data_date', how='left').drop(columns=['data_date'])

    # Monta fato
    fato_cda = pd.DataFrame({
        'id_cobranca': df['id_cobranca'],
        'id_natureza_divida': df['id_natureza_divida'],
        'id_data_cadastro': df['id_data_cadastro'],
        'id_data_situacao': df['id_data_situacao'],
        'cod_situacao_cda': df['cod_situacao_cda'],
        'valor': df['valor_saldo'],
        'score': df['probabilidade_recuperacao']
    })

    # Verificar duplicados
    dup_count = fato_cda['id_cobranca'].duplicated().sum()
    print(f"Registros no fato (cda): {len(fato_cda)} | duplicados id_cobranca: {dup_count}")

    return fato_cda

def build_rel_cda_devedor(df_cda_devedor, df_dim_devedor):
    df_rel = df_cda_devedor.merge(
        df_dim_devedor[['id_pessoa']],
        on='id_pessoa',
        how='inner' 
    )

    
    df_rel = df_rel[['id_cobranca', 'id_pessoa', 'situacao_devedor']]

    return df_rel