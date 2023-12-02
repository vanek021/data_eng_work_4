import pickle
import utils
import random

def load_data(file_name):
    items = []

    with open(file_name, "rb") as input:
        items = pickle.load(input)

        # В датасете что-то странное.
        # У нас есть много повторяющихся заголовков для книг, значит мы не можем однозначно сказать,
        # К какой колонке из первой таблицы относится запись из второй таблицы
        # Связь многие-ко-многим тоже отпадает. Вряд ли многие книги могут иметь одну и ту же информацию по цене
        # Было принято решение выбрать рандомную запись к записи из второй таблицы по дате
        # Связывать будем по ISBN
        books = utils.get_all_books(db)

        for item in items:
            filtered = list(filter(lambda x: x["title"] == item["title"], books))
            item["isbn"] = random.choice(filtered)["isbn"]
        return items

def insert_market_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO market (book_id, price, place, date)
        VALUES(
            (SELECT id FROM book WHERE isbn = :isbn),
            :price, :place, :date
        )""", data)

    db.commit()

def first_query(db, title):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM market
        WHERE book_id = (SELECT id FROM book WHERE title = ?)
        """, [title])
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)
    
    cursor.close()
    return items

def second_query(db, title):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            AVG(price) as avg_price
        FROM market
        WHERE book_id = (SELECT id from book WHERE title = ?)
    """, [title])
    result = dict(res.fetchone())

    cursor.close()
    return result

# Note: количество цен по всем книгам
def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        select b.Title, count(*) as cnt from market m
        left join book b
        on m.book_id = b.Id
        group by b.title
    """)
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)
    
    cursor.close()
    return items

db = utils.connect_to_db("second.db")

data = load_data("./task2/task_2_var_29_subitem.pkl")
#insert_market_data(db, data)

first_result = first_query(db, "451 градус по Фаренгейту")
utils.write_to_json("./task2/first_query_result.json", first_result)

second_result = second_query(db, "451 градус по Фаренгейту")
utils.write_to_json("./task2/second_query_result.json", second_result)

third_result = third_query(db)
utils.write_to_json("./task2/third_query_result.json", third_result)