from clickhouse_driver import Client

# Подключение к ClickHouse
client = Client(host='localhost', port=9000, user='default', password='', database='analytics')

# Заполнение таблицы users фейковыми данными
client.execute('''
INSERT INTO analytics.users
SELECT
    generateUUIDv4() AS id,
    concat('User', toString(number)) AS name,
    concat('user', toString(number), '@example.com') AS email,
    now() - INTERVAL rand() % 365 DAY AS registration_date
FROM system.numbers
LIMIT 1000000;
''')

# Заполнение таблицы products фейковыми данными
client.execute('''
INSERT INTO analytics.products
SELECT
    generateUUIDv4() AS id,
    concat('Product', toString(number)) AS name,
    arrayElement(['Electronics', 'Clothing', 'Food', 'Home', 'Sports'], rand() % 5 + 1) AS category,
    rand() % 1000 + 5 AS price
FROM system.numbers
LIMIT 1000000;
''')

# Заполнение таблицы product_views фейковыми данными
client.execute('''
INSERT INTO analytics.product_views
SELECT
    generateUUIDv4() AS user_id,
    generateUUIDv4() AS product_id,
    now() - INTERVAL rand() % 30 DAY AS timestamp
FROM system.numbers
LIMIT 2000000;  -- 2 просмотра на каждого пользователя
''')

# Заполнение таблицы purchases фейковыми данными
client.execute('''
INSERT INTO analytics.purchases
SELECT
    generateUUIDv4() AS user_id,
    generateUUIDv4() AS product_id,
    rand() % 10 + 1 AS quantity,
    rand() % 1000 + 5 AS total_amount,
    now() - INTERVAL rand() % 30 DAY AS timestamp
FROM system.numbers
LIMIT 2000000;  -- 2 покупки на каждого пользователя
''')

print("Данные успешно вставлены.")
