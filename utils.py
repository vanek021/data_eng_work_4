import json
import sqlite3

VAR = 29

def write_to_json(path: str, data: str):
    with open(path, 'w', encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False))

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row

    return connection

def get_all_books(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM book")
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items