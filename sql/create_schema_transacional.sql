CREATE SCHEMA IF NOT EXISTS transacional;
SET search_path TO transacional;


DROP TABLE IF EXISTS natureza_divida CASCADE;
CREATE TABLE natureza_divida (
    id_natureza_divida INT PRIMARY KEY,
    nome_natureza_divida TEXT NOT NULL,
    descricao TEXT
);

DROP TABLE IF EXISTS situacao_cda CASCADE;
CREATE TABLE situacao_cda (
    cod_situacao_cda INT PRIMARY KEY,
    nome_situacao_cda TEXT NOT NULL,
    cod_situacao_fiscal INT,
    cod_fase_cobranca INT,
    cod_exigibilidade INT,
    tipo_situacao TEXT
);

DROP TABLE IF EXISTS cda CASCADE;
CREATE TABLE cda (
    id_cobranca SERIAL PRIMARY KEY,
    num_cda VARCHAR(20),
    ano_inscricao INT NOT NULL,
    id_natureza_divida INT REFERENCES natureza_divida(id_natureza_divida),
    cod_situacao_cda INT REFERENCES situacao_cda(cod_situacao_cda),
    data_situacao DATE,
    data_cadastramento DATE,
    cod_fase_cobranca INT,
    valor_saldo NUMERIC(14,2)
);

DROP TABLE IF EXISTS cda_score CASCADE;
CREATE TABLE cda_score (
    id_cobranca INT REFERENCES cda(id_cobranca),
    probabilidade_recuperacao FLOAT,
    status_recuperacao INT
);

DROP TABLE IF EXISTS pessoa CASCADE;
CREATE TABLE pessoa (
    id_pessoa INT PRIMARY KEY,
    nome TEXT NOT NULL,
    tipo_pessoa CHAR(2) CHECK (tipo_pessoa IN ('PF', 'PJ')),
    cpf VARCHAR(14),
    cnpj VARCHAR(16)
);

DROP TABLE IF EXISTS cda_devedor CASCADE;
CREATE TABLE cda_devedor (
    id_cobranca INT REFERENCES cda(id_cobranca),
    id_pessoa INT,
    situacao_devedor TEXT,
    PRIMARY KEY (id_cobranca, id_pessoa)
);