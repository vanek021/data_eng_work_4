import json
import utils

# Загрузка сырого JSON
def load_json_data(path):
    with open(path, "r", encoding="utf-8") as input:
        data = json.load(input)
        for data_item in data:
            data_item["wineName"] = data_item["shortName"] + " " + data_item["vintage"]
        return data

# Загрузка информации о винном изделии из загруженного JSON
def extract_wines(data):
    items = []
    for data_item in data:
        item = {}
        item["wineName"] = data_item["wineName"]
        item["shortName"] = data_item["shortName"]
        item["vintage"] = data_item["vintage"]
        item["region"] = data_item["region"]
        item["lwin"] = int(data_item["lwin"])
        items.append(item)
    return list({ item['wineName'] : item for item in items}.values())

# Вытаскиваем данные о продукте
def extract_products(data):
    items = []
    for data_item in data:
        item = {}
        if "buyPrice" in data_item.keys() and data_item["buyPrice"] is not None:
            item["buyPrice"] = float(data_item["buyPrice"])
            item["buyQty"] = int(data_item["buyQty"])
        else:
            item["buyPrice"] = None
            item["buyQty"] = None
        if "sellPrice" in data_item.keys() and data_item["sellPrice"] is not None:
            item["sellPrice"] = float(data_item["sellPrice"])
            item["sellQty"] = int(data_item["sellQty"])
        else:
            item["sellPrice"] = None
            item["sellQty"] = None
        item["avgScore"] = 0 if data_item["avgScore"] == "N/A" else float(data_item["avgScore"])
        item["imageUrl"] = data_item["imageUrl"]
        item["urlKey"] = data_item["urlKey"]
        item["ltDate"] = data_item["ltDate"]
        item["packSize"] = data_item["packSize"]
        item["wineName"] = data_item["wineName"]
        item["conditionStatus"] = data_item["conditionStatus"]
        if "marketPrice" in data_item.keys() and data_item["marketPrice"] is not None:
            item["marketPrice"] = float(data_item["marketPrice"].replace("\u00a3", "").replace(",", "."))
        else:
            item["marketPrice"] = None
        items.append(item)
    return items

# Вставка вин в базу
def insert_wines(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO wine (wineName, shortName, vintage, region, lwin)
        VALUES(
            :wineName, :shortName, :vintage, :region, :lwin
        )
    """, data)

    db.commit()

# Вставка курсов валют в базу
def insert_currencies(db, data, path):
    with open(path, "r", encoding="utf-8") as input:
        data = json.load(input)

    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO currency (name, factor, isDefault)
        VALUES(
            :name, :factor, :isDefault
        )
    """, data)

    db.commit()

# Вставка продуктов валют в базу
def insert_products(db, data):

    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO product (buyPrice, buyQty, sellPrice, sellQty, avgScore, imageUrl, urlKey, ltDate, packSize, conditionStatus, marketPrice, wineId, currencyId)
        VALUES(
            :buyPrice, :buyQty, :sellPrice, :sellQty, :avgScore, :imageUrl, :urlKey, :ltDate, :packSize, :conditionStatus, :marketPrice, 
            (SELECT id FROM wine WHERE wineName = :wineName), (SELECT id FROM currency WHERE isDefault = 1)
        )
    """, data)

    db.commit()

# Первый запрос. Группировка по conditionStatus
def group_by_condition(db):
    cursor = db.cursor()
    res = cursor.execute("""
        select conditionStatus, count(*) as cnt from product
        group by conditionStatus
    """)
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)
    
    cursor.close()
    return items


# Второй запрос. Конвертация  в валюту
def convert_to_currency(db, currency_name):
    cursor = db.cursor()
    res = cursor.execute("""
        select * from currency
        where name = ?
    """, [currency_name])

    currency_row = res.fetchone()

    res = cursor.execute("""
        select * from product
    """)

    items = []
    for row in res.fetchall():
        item = dict(row)
        if item["currencyId"] != currency_row["id"]:
            if item["marketPrice"] is not None:
                item["marketPrice"] = item["marketPrice"] * currency_row["factor"]
            if item["sellPrice"] is not None:
                item["sellPrice"] = item["sellPrice"] * currency_row["factor"]
            if item["buyPrice"] is not None:
                item["buyPrice"] = item["buyPrice"] * currency_row["factor"]
        items.append(item)

    cursor.close()

    return items

# Третий запрос. Статистика по рыночным ценам
def get_stat_by_marketprices(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(marketPrice) as sum,
            AVG(marketPrice) as avg,
            MIN(marketPrice) as min,
            MAX(marketPrice) as max
        FROM product
    """)
    
    result = dict(res.fetchone())
    cursor.close()

    return result

# Четвертый запрос. Получение самых высоких оценок с указаным лимитом
def get_top_by_avgscore(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM product ORDER BY avgScore DESC LIMIT ?", [limit])
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items

# Пятый запрос. Присоединение таблицы вин, группировка по их регионам
def get_groupped_by_wine_region(db):
    cursor = db.cursor()
    res = cursor.execute("""
        select region, count(*) from product
        left join wine
        on product.wineId = wine.Id
        group by wine.region
    """)
    items = list()

    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()

    return items

db = utils.connect_to_db("five.db")

# Шестой запрос. Обновить цены в базе в соответствие с новой валютой
# Заметка: конвертация идет относительно дефолтной валюты (EUR)
# И только один раз! Повторное выполнение операции не относительно дефолтной валюты приведет к неправильной конвертации
def update_prices_by_currency(db, currency_name):
    cursor = db.cursor()
    res = cursor.execute("""
        select * from currency
        where name = ?
    """, [currency_name])

    currency_row = res.fetchone()

    res = cursor.execute("""
        update product
        set sellPrice = sellPrice * ?, currencyId = ? WHERE sellPrice is not null
    """, [currency_row["factor"], currency_row["id"]])

    res = cursor.execute("""
        update product
        set buyPrice = buyPrice * ?, currencyId = ? WHERE buyPrice is not null
    """, [currency_row["factor"], currency_row["id"]])

    res = cursor.execute("""
        update product
        set marketPrice = marketPrice * ?, currencyId = ? WHERE marketPrice is not null
    """, [currency_row["factor"], currency_row["id"]])

    db.commit()

db = utils.connect_to_db("five.db")

data = load_json_data("./task5/wine_product_data.json")
wines = extract_wines(data)
products = extract_products(data)

#insert_wines(db, wines)
#insert_currencies(db, data, './task5/currencies_data.json')
#insert_products(db, products)

query_result = group_by_condition(db)
utils.write_to_json("./task5/group_by_condition_query_result.json", query_result)

query_result = convert_to_currency(db, "RUB")
utils.write_to_json("./task5/convert_to_currency_query_result.json", query_result)

query_result = get_stat_by_marketprices(db)
utils.write_to_json("./task5/get_stat_by_marketprices_query_result.json", query_result)

query_result = get_top_by_avgscore(db, 20)
utils.write_to_json("./task5/get_top_by_avgscore_query_result.json", query_result)

query_result = get_groupped_by_wine_region(db)
utils.write_to_json("./task5/get_groupped_by_wine_region_query_result.json", query_result)

update_prices_by_currency(db, "RUB")