CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw;

DROP TABLE IF EXISTS dim_tributo CASCADE;
CREATE TABLE dim_tributo (
    id_natureza_divida INT PRIMARY KEY,
    nome TEXT,
    descricao TEXT
);

DROP TABLE IF EXISTS dim_devedor CASCADE;
CREATE TABLE dim_devedor (
    id_pessoa INT PRIMARY KEY,
    nome TEXT,
    tipo_pessoa CHAR(2),
    cpf VARCHAR(14),
    cnpj VARCHAR(16)
);

DROP TABLE IF EXISTS dim_data CASCADE;
CREATE TABLE dim_data (
    id_data SERIAL PRIMARY KEY,
    data DATE,
    ano INT,
    mes INT,
    dia INT
);

DROP TABLE IF EXISTS dim_situacao CASCADE;
CREATE TABLE dim_situacao (
    cod_situacao_cda INT PRIMARY KEY,
    nome_situacao_cda TEXT,
    tipo_situacao TEXT
);

DROP TABLE IF EXISTS fato_cda CASCADE;
CREATE TABLE fato_cda (
    id_fato SERIAL PRIMARY KEY,
    id_cobranca INT,
    id_natureza_divida INT REFERENCES dim_tributo(id_natureza_divida),
    id_data_cadastro INT REFERENCES dim_data(id_data),
    id_data_situacao INT REFERENCES dim_data(id_data),
    cod_situacao_cda INT REFERENCES dim_situacao(cod_situacao_cda),
    valor NUMERIC(14,2),
    score FLOAT
);

DROP TABLE IF EXISTS rel_cda_devedor CASCADE;
CREATE TABLE dw.rel_cda_devedor (
    id_cobranca INT NOT NULL,
    id_pessoa INT NOT NULL REFERENCES dim_devedor(id_pessoa),
    situacao_devedor TEXT,
    PRIMARY KEY (id_cobranca, id_pessoa)
);