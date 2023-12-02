import utils

CREATE_WINE_TABLE = """
CREATE TABLE wine (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    wineName  TEXT,
    shortName TEXT,
    vintage   TEXT,
    region    TEXT,
    lwin      INTEGER
);
"""

CREATE_CURRENCY_TABLE = """
CREATE TABLE currency (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    isDefault TEXT,
    factor    REAL,
    name      TEXT
);
"""

CREATE_PRODUCT_TABLE = """
CREATE TABLE product (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    buyPrice        REAL,
    buyQty          INTEGER,
    sellPrice       REAL,
    sellQty         INTEGER,
    avgScore        REAL,
    imageUrl        TEXT,
    urlKey          TEXT,
    ltDate          TEXT,
    packSize        TEXT,
    conditionStatus TEXT,
    marketPrice     REAL,
    wineId          INTEGER REFERENCES wine (id),
    currencyId      INTEGER REFERENCES currency (id) 
);
"""

def init_db(db):
    cursor = db.cursor()
    res = cursor.execute(CREATE_WINE_TABLE)
    db.commit()

    cursor = db.cursor()
    res = cursor.execute(CREATE_CURRENCY_TABLE)
    db.commit()

    cursor = db.cursor()
    res = cursor.execute(CREATE_PRODUCT_TABLE)
    db.commit()

db = utils.connect_to_db("five.db")

init_db(db)