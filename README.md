Para rodar o projeto:
## 1) Subir containers (Postgres + Python)
No diretório do projeto:

```bash
docker-compose up -d

Execute (no host):

docker exec -it postgres_transacional psql -U postgres -d transacional_db -f /scripts/create_schema_transacional.sql
Em seguida, carregar os CSVs no transacional:

docker exec -it postgres_transacional psql -U postgres -d transacional_db -f /scripts/populate_transacional.sql

Execute:

docker exec -it postgres_dw psql -U postgres -d dw_db -f /scripts/create_schema_dw.sql

# entrar no container (ou pode executar direto os comandos)
docker exec -it python_runner bash
# dentro do container:
pip install -r /app/requirements.txt
python /app/etl_main.py

Iniciar servidor (dentro do container):
uvicorn main:app --host 0.0.0.0 --port 8000

7) Testes rápidos de endpoints (exemplos)
Buscar CDAs:

curl "http://localhost:8000/cda/search?minSaldo=1000&sort_by=valor&sort_order=desc"
Detalhes devedor (todos):

curl "http://localhost:8000/cda/detalhes_devedor"
Resumo distribuição:
curl "http://localhost:8000/resumo/distribuicao_cdas"
Inscrições por ano:

curl "http://localhost:8000/resumo/inscricoes"
Montante acumulado (percentis):

curl "http://localhost:8000/resumo/montante_acumulado"
Quantidade por tributo:

curl "http://localhost:8000/resumo/quantidade_cdas"
Saldo por tributo:

curl "http://localhost:8000/resumo/saldo_cdas"