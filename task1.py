import utils

def parse_data(file_name):
    items = []
    with open(file_name, "r", encoding="utf-8") as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == "=====\n":
                items.append(item)
                item = dict()
            else:
                line = line.strip()
                splitted = line.split("::")

                if splitted[0] in ["pages", "published_year", "views"]:
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] == "rating":
                    item[splitted[0]] = float(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]

    return items

def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO book (title, author, genre, pages, published_year, isbn, rating, views)
        VALUES(
            :title, :author, :genre, :pages, :published_year, :isbn, :rating, :views
        )
    """, data)

    db.commit()

def get_top_by_views(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM book ORDER BY views DESC LIMIT ?", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items

def get_stat_by_pages(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(pages) as sum,
            AVG(pages) as avg,
            MIN(pages) as min,
            MAX(pages) as max
        FROM book
    """)
    
    result = dict(res.fetchone())
    cursor.close()

    return result

def get_freq_by_century(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            CAST(count(*) as REAL) / (SELECT COUNT(*) FROM book) as count,
            (FLOOR(published_year/100)+1) as century
        FROM book
        GROUP BY (FLOOR(published_year/100)+1)
    """)

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result

def filter_by_year(db, min_year, limit=10):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM book
        WHERE published_year > ? ORDER BY views DESC
        LIMIT ?
    """, [min_year, limit])

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result

items = parse_data("./task1/task_1_var_29_item.text")
db = utils.connect_to_db("first.db")
#insert_data(db, items)

filtered_items = filter_by_year(db, 1950, utils.VAR + 10)
utils.write_to_json("./task1/result_filtered.json", filtered_items)

stat = get_stat_by_pages(db)
utils.write_to_json("./task1/result_stat.json", stat)

top_items = get_top_by_views(db, utils.VAR + 10)
utils.write_to_json("./task1/result_top.json", top_items)

freq = get_freq_by_century(db)
utils.write_to_json("./task1/result_freq.json", freq)
