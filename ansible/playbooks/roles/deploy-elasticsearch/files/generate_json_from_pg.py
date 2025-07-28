import json
import psycopg2

conn = psycopg2.connect(
    dbname="film_sucher",
    user="film_user",
    password="film_user_password",
    host="localhost"
)

cur = conn.cursor()
cur.execute("SELECT id, title, description, country, genre FROM Films")
films = cur.fetchall()

with open("/tmp/films_bulk.ndjson", "w", encoding="utf-8") as f:
    for row in films:
        index_cmd = {"index": {"_id": row[0]}}
        doc = {
            "title": row[1],
            "description": row[2],
            "country": row[3],
            "genre": row[4]
        }
        f.write(json.dumps(index_cmd) + "\n")
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")

cur.close()
conn.close()