SET search_path TO transacional;

TRUNCATE TABLE natureza_divida CASCADE;
TRUNCATE TABLE situacao_cda CASCADE;
TRUNCATE TABLE cda CASCADE;
TRUNCATE TABLE cda_score CASCADE;
TRUNCATE TABLE pessoa CASCADE;
TRUNCATE TABLE cda_devedor CASCADE;

COPY natureza_divida(id_natureza_divida, nome_natureza_divida, descricao)
FROM '/csv/002.csv'
DELIMITER ',' CSV HEADER;

COPY situacao_cda(cod_situacao_cda, nome_situacao_cda, cod_situacao_fiscal, cod_fase_cobranca, cod_exigibilidade, tipo_situacao)
FROM '/csv/003.csv'
DELIMITER ',' CSV HEADER;

COPY cda(num_cda, ano_inscricao, id_natureza_divida, cod_situacao_cda, data_situacao, data_cadastramento, cod_fase_cobranca, valor_saldo)
FROM '/csv/001.csv'
DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS tmp_cda_score;
CREATE TEMP TABLE tmp_cda_score (
    num_cda VARCHAR(20),
    probabilidade_recuperacao FLOAT,
    status_recuperacao INT
);

COPY tmp_cda_score(num_cda, probabilidade_recuperacao, status_recuperacao)
FROM '/csv/004.csv'
DELIMITER ',' CSV HEADER;

INSERT INTO cda_score(id_cobranca, probabilidade_recuperacao, status_recuperacao)
SELECT c.id_cobranca, t.probabilidade_recuperacao, t.status_recuperacao
FROM tmp_cda_score t
JOIN cda c ON t.num_cda = c.num_cda;

COPY pessoa(id_pessoa, nome, cpf, cnpj)
FROM '/csv/pessoas_unicas.csv'
DELIMITER ',' CSV HEADER;

UPDATE pessoa SET tipo_pessoa = 'PF' WHERE cpf IS NOT NULL;
UPDATE pessoa SET tipo_pessoa = 'PJ' WHERE cnpj IS NOT NULL;

DROP TABLE IF EXISTS tmp_cda_devedor;
CREATE TEMP TABLE tmp_cda_devedor (
    num_cda VARCHAR(20),
    id_pessoa INT,
    situacao_devedor TEXT
);

COPY tmp_cda_devedor(num_cda, id_pessoa, situacao_devedor)
FROM '/csv/005.csv'
DELIMITER ',' CSV HEADER;

INSERT INTO cda_devedor(id_cobranca, id_pessoa, situacao_devedor)
SELECT c.id_cobranca, t.id_pessoa, t.situacao_devedor
FROM tmp_cda_devedor t
JOIN cda c ON t.num_cda = c.num_cda;