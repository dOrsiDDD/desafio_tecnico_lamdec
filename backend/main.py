# main.py
import os
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List, Any, Dict
from sqlalchemy import create_engine, text
from datetime import date

# ----- CONFIGURAÇÃO (env vars) -----
DW_DB_URL = os.getenv("DW_DB_URL", "postgresql+psycopg2://postgres:postgres@postgres_dw:5432/dw_db")
TRANS_DB_URL = os.getenv("TRANS_DB_URL", "postgresql+psycopg2://postgres:postgres@postgres_transacional:5432/transacional_db")

engine_dw = create_engine(DW_DB_URL, pool_pre_ping=True)
engine_trans = create_engine(TRANS_DB_URL, pool_pre_ping=True)

app = FastAPI(title="CDA DW API")


# -------------------------
# Helper: safe fetch mapping
# -------------------------
def rows_to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


# -------------------------
# 1) /cda/search
# -------------------------
@app.get("/cda/search")
def search_cda(
    numCDA: Optional[str] = Query(None),
    minSaldo: Optional[float] = Query(None),
    maxSaldo: Optional[float] = Query(None),
    minAno: Optional[int] = Query(None),
    maxAno: Optional[int] = Query(None),
    natureza: Optional[str] = Query(None),
    agrupamento_situacao: Optional[int] = Query(None),
    sort_by: Optional[str] = Query("ano"),   # "ano" or "valor"
    sort_order: Optional[str] = Query("asc") # "asc" or "desc"
):
    trans_ids = None
    trans_params = {}
    trans_where = " WHERE 1=1 "
    if numCDA is not None:
        trans_where += " AND num_cda ILIKE :numCDA "
        trans_params["numCDA"] = f"%{numCDA}%"
    if minAno is not None:
        trans_where += " AND ano_inscricao >= :minAno "
        trans_params["minAno"] = minAno
    if maxAno is not None:
        trans_where += " AND ano_inscricao <= :maxAno "
        trans_params["maxAno"] = maxAno
    if agrupamento_situacao is not None:
        trans_where += " AND cod_fase_cobranca = :cod_fase "
        trans_params["cod_fase"] = agrupamento_situacao

    if trans_params:
        q = "SELECT id_cobranca, num_cda, ano_inscricao, cod_fase_cobranca FROM transacional.cda " + trans_where
        with engine_trans.connect() as conn:
            rows = conn.execute(text(q), trans_params).mappings().all()
        if not rows:
            return []  
        trans_map = {r["id_cobranca"]: dict(r) for r in rows}
        trans_ids = list(trans_map.keys())
    else:
        trans_map = {}

    dw_q = """
        SELECT f.id_cobranca,
               COALESCE(f.valor, 0)::float AS valor,
               COALESCE(f.score, 0)::float AS score,
               f.id_natureza_divida
        FROM dw.fato_cda f
        JOIN dw.dim_tributo t ON f.id_natureza_divida = t.id_natureza_divida
        WHERE 1=1
    """

    dw_params = {}
    if natureza:
        dw_q += " AND t.nome ILIKE :natureza "
        dw_params["natureza"] = f"%{natureza}%"

    if trans_ids is not None:
        ids_list = ",".join(str(int(i)) for i in trans_ids)
        dw_q += f" AND f.id_cobranca IN ({ids_list}) "

    if minSaldo is not None:
        dw_q += " AND COALESCE(f.valor,0) >= :minSaldo "
        dw_params["minSaldo"] = minSaldo
    if maxSaldo is not None:
        dw_q += " AND COALESCE(f.valor,0) <= :maxSaldo "
        dw_params["maxSaldo"] = maxSaldo

    with engine_dw.connect() as conn:
        rows = conn.execute(text(dw_q), dw_params).mappings().all()
    if not rows:
        return []

    results = []
    current_year = date.today().year
    for r in rows:
        idc = r["id_cobranca"]
        # look up trans info if available; otherwise set None
        trans = trans_map.get(idc)
        if not trans:
            # fallback: try fetch from transacional individually (cheap if few rows)
            try:
                with engine_trans.connect() as conn:
                    tr = conn.execute(text("SELECT id_cobranca, num_cda, ano_inscricao, cod_fase_cobranca FROM transacional.cda WHERE id_cobranca = :idc"), {"idc": idc}).mappings().first()
                trans = dict(tr) if tr else None
            except Exception:
                trans = None

        num_cda = trans["num_cda"] if trans else None
        ano_insc = trans.get("ano_inscricao") if trans else None
        ano_calc = (current_year - int(ano_insc)) if (ano_insc is not None and str(ano_insc).isdigit()) else None
        agrup = trans.get("cod_fase_cobranca") if trans else None

        if (minAno is not None or maxAno is not None) and ano_insc is None:
            continue
        if minAno is not None and ano_insc is not None and ano_insc < minAno:
            continue
        if maxAno is not None and ano_insc is not None and ano_insc > maxAno:
            continue
        if agrupamento_situacao is not None and agrup is not None and agrup != agrupamento_situacao:
            continue

        natureza_name = None
        try:
            with engine_dw.connect() as conn:
                tn = conn.execute(text("SELECT nome FROM dw.dim_tributo WHERE id_natureza_divida = :nid"), {"nid": r["id_natureza_divida"]}).mappings().first()
            if tn:
                natureza_name = tn["nome"]
        except Exception:
            natureza_name = None

        item = {
            "numCDA": num_cda,
            "valor_saldo_atualizado": float(r["valor"]),
            "qtde_anos_idade_cda": int(ano_calc) if ano_calc is not None else None,
            "agrupamento_situacao": int(agrup) if agrup is not None else None,
            "natureza": natureza_name,
            "score": float(r["score"]) if r["score"] is not None else None
        }
        results.append(item)

    # 4) Sort
    reverse = (sort_order and sort_order.lower() == "desc")
    if sort_by == "valor":
        results.sort(key=lambda x: (x["valor_saldo_atualizado"] is None, x["valor_saldo_atualizado"]), reverse=reverse)
    else:
        # default ano
        results.sort(key=lambda x: (x["qtde_anos_idade_cda"] is None, x["qtde_anos_idade_cda"]), reverse=reverse)

    return results


# -------------------------
# 2) /cda/detalhes_devedor
#    (opcional param: numCDA or idCobranca)
# -------------------------
@app.get("/cda/detalhes_devedor")
def detalhes_devedor(numCDA: Optional[str] = Query(None), idCobranca: Optional[int] = Query(None)):
    # if numCDA provided -> find id_cobranca(s) in transacional
    ids = None
    if numCDA:
        q = "SELECT id_cobranca FROM transacional.cda WHERE num_cda ILIKE :num"
        with engine_trans.connect() as conn:
            rr = conn.execute(text(q), {"num": f"%{numCDA}%"}).mappings().all()
        ids = [r["id_cobranca"] for r in rr]
        if not ids:
            return []

    if idCobranca:
        ids = [idCobranca]

    # If ids None -> return all devedores (dim_devedor)
    if ids is None:
        q = "SELECT nome as name, tipo_pessoa, COALESCE(cpf, cnpj, '') as \"CPF / CNPJ\" FROM dw.dim_devedor"
        with engine_dw.connect() as conn:
            rows = conn.execute(text(q)).mappings().all()
        return rows

    # else get devedores associated
    ids_str = ",".join(str(int(i)) for i in ids)
    q = f"""
        SELECT d.nome as name, d.tipo_pessoa, COALESCE(d.cpf, d.cnpj, '') as "CPF / CNPJ"
        FROM dw.rel_cda_devedor r
        JOIN dw.dim_devedor d ON r.id_pessoa = d.id_pessoa
        WHERE r.id_cobranca IN ({ids_str})
        GROUP BY d.nome, d.tipo_pessoa, d.cpf, d.cnpj
    """
    with engine_dw.connect() as conn:
        rows = conn.execute(text(q)).mappings().all()
    return rows


# -------------------------
# 3) /resumo/distribuicao_cdas
# -------------------------
@app.get("/resumo/distribuicao_cdas")
def distribuicao_cdas():
    q = """
    SELECT
      t.nome AS name,
      ROUND(100.0 * SUM(CASE WHEN s.nome_situacao_cda ILIKE '%cobr%' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "Em cobranca",
      ROUND(100.0 * SUM(CASE WHEN s.nome_situacao_cda ILIKE '%cancel%' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "Cancelada",
      ROUND(100.0 * SUM(CASE WHEN s.nome_situacao_cda ILIKE '%paga%' OR s.nome_situacao_cda ILIKE '%quitad%' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "Quitada"
    FROM dw.fato_cda f
    JOIN dw.dim_tributo t ON f.id_natureza_divida = t.id_natureza_divida
    LEFT JOIN dw.dim_situacao s ON f.cod_situacao_cda = s.cod_situacao_cda
    GROUP BY t.nome
    ORDER BY t.nome;
    """
    with engine_dw.connect() as conn:
        rows = conn.execute(text(q)).mappings().all()
    return rows


# -------------------------
# 4) /resumo/inscricoes
# -------------------------
@app.get("/resumo/inscricoes")
def inscricoes():
    q = """
    SELECT (d.ano)::INT AS ano, COUNT(*) AS "Quantidade"
    FROM dw.fato_cda f
    JOIN dw.dim_data d ON f.id_data_cadastro = d.id_data
    GROUP BY d.ano
    ORDER BY d.ano;
    """
    with engine_dw.connect() as conn:
        rows = conn.execute(text(q)).mappings().all()
    return rows


# -------------------------
# 5) /resumo/montante_acumulado
# -------------------------
@app.get("/resumo/montante_acumulado")
def montante_acumulado():
    percentiles = [1,5,10,25,50,75,90,95,99]

    q_rank = """
    WITH base AS (
      SELECT t.nome AS tributo, f.id_cobranca, COALESCE(f.valor,0)::numeric AS valor
      FROM dw.fato_cda f
      JOIN dw.dim_tributo t ON f.id_natureza_divida = t.id_natureza_divida
    ),
    ranked AS (
      SELECT
        tributo,
        id_cobranca,
        valor,
        SUM(valor) OVER (PARTITION BY tributo ORDER BY valor DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumul_val,
        SUM(valor) OVER (PARTITION BY tributo) AS total_tributo,
        100.0 * SUM(valor) OVER (PARTITION BY tributo ORDER BY valor DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / NULLIF(SUM(valor) OVER (PARTITION BY tributo),0) AS cumul_pct
      FROM base
    )
    SELECT * FROM ranked LIMIT 0; -- placeholder
    """
    final_rows = []
    with engine_dw.connect() as conn:
        for p in percentiles:
            q = text(f"""
            WITH base AS (
              SELECT t.nome AS tributo, f.id_cobranca, COALESCE(f.valor,0)::numeric AS valor
              FROM dw.fato_cda f
              JOIN dw.dim_tributo t ON f.id_natureza_divida = t.id_natureza_divida
            ),
            ranked AS (
              SELECT
                tributo,
                id_cobranca,
                valor,
                SUM(valor) OVER (PARTITION BY tributo ORDER BY valor DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumul_val,
                SUM(valor) OVER (PARTITION BY tributo) AS total_tributo,
                100.0 * SUM(valor) OVER (PARTITION BY tributo ORDER BY valor DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / NULLIF(SUM(valor) OVER (PARTITION BY tributo),0) AS cumul_pct
              FROM base
            )
            SELECT
                :p AS "Percentual",
                ROUND(COALESCE( (SELECT MIN(cumul_pct) FROM ranked r WHERE r.tributo ILIKE '%IPTU%' AND r.cumul_pct >= :p), 0),2) AS "IPTU",
                ROUND(COALESCE( (SELECT MIN(cumul_pct) FROM ranked r WHERE r.tributo ILIKE '%ISS%' AND r.cumul_pct >= :p), 0),2) AS "ISS",
                ROUND(COALESCE( (SELECT MIN(cumul_pct) FROM ranked r WHERE r.tributo ILIKE '%taxa%' AND r.cumul_pct >= :p), 0),2) AS "Taxas",
                ROUND(COALESCE( (SELECT MIN(cumul_pct) FROM ranked r WHERE r.tributo ILIKE '%multa%' AND r.cumul_pct >= :p), 0),2) AS "Multas",
                ROUND(COALESCE( (SELECT MIN(cumul_pct) FROM ranked r WHERE r.tributo ILIKE '%ITBI%' AND r.cumul_pct >= :p), 0),2) AS "ITBI"
            """)
            row = conn.execute(q, {"p": p}).mappings().first()
            final_rows.append(dict(row))
    return final_rows


# -------------------------
# 6) /resumo/quantidade_cdas
# -------------------------
@app.get("/resumo/quantidade_cdas")
def quantidade_cdas():
    q = """
      SELECT t.nome AS name, COUNT(*) AS "Quantidade"
      FROM dw.fato_cda f
      JOIN dw.dim_tributo t ON f.id_natureza_divida = t.id_natureza_divida
      GROUP BY t.nome
      ORDER BY t.nome;
    """
    with engine_dw.connect() as conn:
        rows = conn.execute(text(q)).mappings().all()
    return rows


# -------------------------
# 7) /resumo/saldo_cdas
# -------------------------
@app.get("/resumo/saldo_cdas")
def saldo_cdas():
    q = """
      SELECT t.nome AS name, ROUND(SUM(COALESCE(f.valor,0))::numeric,2) AS "Saldo"
      FROM dw.fato_cda f
      JOIN dw.dim_tributo t ON f.id_natureza_divida = t.id_natureza_divida
      GROUP BY t.nome
      ORDER BY t.nome;
    """
    with engine_dw.connect() as conn:
        rows = conn.execute(text(q)).mappings().all()
    return rows
