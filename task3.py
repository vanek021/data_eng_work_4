import msgpack
import utils
import json

def load_json_data(path):
    with open(path, "r", encoding="utf-8") as input:
        data = json.load(input)

        for data_item in data:
            data_item.pop("danceability")
            data_item.pop("popularity")
            data_item.pop("explicit")

            data_item["duration_ms"] = int(data_item["duration_ms"])
            data_item["year"] = int(data_item["year"])
            data_item["tempo"] = float(data_item["tempo"])

    return data

def load_msgpack_data(path):
    with open(path, "rb") as input:
        data = msgpack.load(input)
    items = list()
    for data_item in data:
        data_item["duration_ms"] = int(data_item["duration_ms"])
        data_item["year"] = int(data_item["year"])
        data_item["tempo"] = float(data_item["tempo"])

        data_item.pop("mode")
        data_item.pop("speechiness")
        data_item.pop("acousticness")
        data_item.pop("instrumentalness")

        items.append(data_item)

    return items

def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO music (artist, song, duration_ms, year, tempo, genre)
        VALUES(
            :artist, :song, :duration_ms, :year, :tempo, :genre
        )
    """, data)

    db.commit()

def get_top_by_tempo(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM music ORDER BY tempo DESC LIMIT ?", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items

def get_stat_by_duration(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(duration_ms) as sum,
            AVG(duration_ms) as avg,
            MIN(duration_ms) as min,
            MAX(duration_ms) as max
        FROM music
    """)
    
    result = dict(res.fetchone())
    cursor.close()

    return result

def get_freq_by_century(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            CAST(count(*) as REAL) / (SELECT COUNT(*) FROM music) as count,
            (FLOOR(year/100)+1) as century
        FROM music
        GROUP BY (FLOOR(year/100)+1)
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
        FROM music
        WHERE year > ? ORDER BY tempo DESC
        LIMIT ?
    """, [min_year, limit])

    result = list()

    for row in res.fetchall():
        result.append(dict(row))
    cursor.close()

    return result

db = utils.connect_to_db("third.db")

data = load_json_data("./task3/task_3_var_29_part_2.json") + load_msgpack_data("./task3/task_3_var_29_part_1.msgpack")

#insert_data(db, data)

top_by_tempo = get_top_by_tempo(db, limit=utils.VAR+10)
utils.write_to_json("./task3/result_top.json", top_by_tempo)

stat_by_duration = get_stat_by_duration(db)
utils.write_to_json("./task3/result_stat.json", stat_by_duration)

freq = get_freq_by_century(db)
utils.write_to_json("./task3/result_freq.json", freq)

filtered_items = filter_by_year(db, 1950, utils.VAR + 15)
utils.write_to_json("./task3/result_filtered.json", filtered_items)
