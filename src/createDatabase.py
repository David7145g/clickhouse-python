from clickhouse_driver import Client

# Подключение через TCP
client = Client(host='localhost', port=9000, user='default', password='', database='default')

# Создание базы данных
client.execute('CREATE DATABASE IF NOT EXISTS analytics')

# Запрос на создание таблицы пользователей
client.execute('''
CREATE TABLE IF NOT EXISTS analytics.users (
    id UUID,
    name String,
    email String,
    registration_date DateTime
) ENGINE = MergeTree()
ORDER BY id;
''')

# Запрос на создание таблицы товаров
client.execute('''
CREATE TABLE IF NOT EXISTS analytics.products (
    id UUID,
    name String,
    category String,
    price Float64
) ENGINE = MergeTree()
ORDER BY id;
''')

# Запрос на создание таблицы просмотров товаров
client.execute('''
CREATE TABLE IF NOT EXISTS analytics.product_views (
    user_id UUID,
    product_id UUID,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id, product_id);
''')

# Запрос на создание таблицы покупок
client.execute('''
CREATE TABLE IF NOT EXISTS analytics.purchases (
    user_id UUID,
    product_id UUID,
    quantity UInt32,
    total_amount Float64,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id, product_id);
''')

print("Таблицы успешно созданы.")