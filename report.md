## Отчет по лабораторной работе №4

### 1. Установка Click House
```sh
brew install clickhouse
```

### 2. Запуск сервера Click House
```sh
brew clickhouse server
```

### 3.	Создаем базу данных analytics
```sql
CREATE DATABASE analytics;
```

### 4.	Создаем таблицы в ClickHouse
```sql
USE analytics;

CREATE TABLE users (
    id UInt32,
    name String,
    email String,
    registration_date Date
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE products (
    id UInt32,
    name String,
    category String,
    price Float32
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE product_views (
    user_id UInt32,
    product_id UInt32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp);

CREATE TABLE purchases (
    user_id UInt32,
    product_id UInt32,
    quantity UInt32,
    amount Float32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp);
```

### 5. Генерация и загрузка тестовых данных
```python
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
```

### 6. Проверим что данные успешно вставились
```python
from clickhouse_driver import Client

# Подключение через TCP
client = Client(host='localhost', port=9000, user='default', password='', database='analytics')

# Пример запроса для получения количества записей в таблицах
def get_table_count(table_name):
    query = f"SELECT COUNT(*) FROM {table_name}"
    result = client.execute(query)
    return result[0][0]  # Возвращаем только количество

# Запросы для проверки количества записей в каждой таблице
users_count = get_table_count('users')
products_count = get_table_count('products')
product_views_count = get_table_count('product_views')
purchases_count = get_table_count('purchases')

# Вывод результатов
print(f"Количество пользователей: {users_count}")
print(f"Количество товаров: {products_count}")
print(f"Количество просмотров товаров: {product_views_count}")
print(f"Количество покупок: {purchases_count}")
```
Вывод
```sh
Количество пользователей: 1000000
Количество товаров: 1000000
Количество просмотров товаров: 2000000
Количество покупок: 2000000
```
### 7. Реализация аналитических запросов

```python
from clickhouse_driver import Client

# Подключение к ClickHouse
client = Client(host='localhost', port=9000, user='default', password='', database='analytics')

# 1. Количество просмотров товаров по категориям за последние 24 часа
query_1 = '''
SELECT 
    p.category AS category,
    COUNT(v.product_id) AS views_count
FROM analytics.product_views v
JOIN analytics.products p ON v.product_id = p.id
WHERE v.timestamp >= now() - INTERVAL 24 HOUR
GROUP BY p.category
ORDER BY views_count DESC;
'''

# 2. Топ-10 самых популярных товаров (по количеству покупок) за последнюю неделю
query_2 = '''
SELECT 
    p.name AS product_name,
    COUNT(pr.product_id) AS purchases_count
FROM analytics.purchases pr
JOIN analytics.products p ON pr.product_id = p.id
WHERE pr.timestamp >= now() - INTERVAL 7 DAY
GROUP BY p.name
ORDER BY purchases_count DESC
LIMIT 10;
'''

# 3. Общая сумма продаж по дням за последний месяц
query_3 = '''
SELECT 
    toStartOfDay(pr.timestamp) AS day,
    SUM(pr.total_amount) AS total_sales
FROM analytics.purchases pr
WHERE pr.timestamp >= now() - INTERVAL 1 MONTH
GROUP BY day
ORDER BY day;
'''

# 4. Средняя конверсия из просмотра в покупку по категориям товаров
query_4 = '''
SELECT 
    p.category AS category,
    COUNT(DISTINCT pr.product_id, pr.user_id) / NULLIF(COUNT(DISTINCT v.product_id, v.user_id), 0) AS conversion_rate
FROM analytics.products p
LEFT JOIN analytics.product_views v ON v.product_id = p.id
LEFT JOIN analytics.purchases pr ON pr.product_id = p.id AND pr.user_id = v.user_id
GROUP BY p.category
ORDER BY conversion_rate DESC;
'''

# Выполнение запросов и вывод результатов
def execute_query(query, description):
    print(f"\n{description}:\n")
    results = client.execute(query)
    for row in results:
        print(row)

# 1. Количество просмотров товаров
execute_query(query_1, "Количество просмотров товаров по категориям за последние 24 часа")

# 2. Топ-10 самых популярных товаров
execute_query(query_2, "Топ-10 самых популярных товаров (по количеству покупок) за последнюю неделю")

# 3. Общая сумма продаж по дням
execute_query(query_3, "Общая сумма продаж по дням за последний месяц")

# 4. Средняя конверсия из просмотра в покупку
execute_query(query_4, "Средняя конверсия из просмотра в покупку по категориям товаров")
```

### Оптимизация производительности
```python
from clickhouse_driver import Client
from dd.module_1.lab_4.query import *

import time
from clickhouse_driver import Client

# Подключение к ClickHouse
client = Client(host='localhost', port=9000, user='default', password='', database='analytics')

# Функция для замера времени выполнения
def measure_query_execution_time(query, description):
    start_time = time.time()
    client.execute(query)
    end_time = time.time()
    print(f"{description} выполнен за {end_time - start_time:.2f} секунд.")

# Оптимизация индексов
print("Добавляем индексы...")
client.execute('ALTER TABLE analytics.product_views ADD INDEX idx_timestamp (timestamp) TYPE minmax GRANULARITY 64;')
client.execute('ALTER TABLE analytics.purchases ADD INDEX idx_timestamp (timestamp) TYPE minmax GRANULARITY 64;')
print("Индексы добавлены.\n")

# Замеры времени выполнения запросов
print("Измеряем время выполнения запросов до оптимизации:")
measure_query_execution_time(query_1, "Запрос 1")
measure_query_execution_time(query_2, "Запрос 2")
measure_query_execution_time(query_3, "Запрос 3")
measure_query_execution_time(query_4, "Запрос 4")

# Оптимизация таблицы (активация индексов)
print("\nОптимизируем таблицы...")
client.execute('OPTIMIZE TABLE analytics.product_views FINAL;')
client.execute('OPTIMIZE TABLE analytics.purchases FINAL;')
print("Оптимизация завершена.\n")

# Замеры времени выполнения запросов после оптимизации
print("Измеряем время выполнения запросов после оптимизации:")
measure_query_execution_time(query_1, "Запрос 1")
measure_query_execution_time(query_2, "Запрос 2")
measure_query_execution_time(query_3, "Запрос 3")
measure_query_execution_time(query_4, "Запрос 4")
```

Результаты
```sh
Добавляем индексы...
Индексы добавлены.

Измеряем время выполнения запросов до оптимизации:
Запрос 1 выполнен за 0.07 секунд.
Запрос 2 выполнен за 0.07 секунд.
Запрос 3 выполнен за 0.01 секунд.
Запрос 4 выполнен за 0.33 секунд.

Оптимизируем таблицы...
Оптимизация завершена.

Измеряем время выполнения запросов после оптимизации:
Запрос 1 выполнен за 0.08 секунд.
Запрос 2 выполнен за 0.05 секунд.
Запрос 3 выполнен за 0.01 секунд.
Запрос 4 выполнен за 0.30 секунд.
```

### Визуализация результатов
```python
import matplotlib.pyplot as plt
from clickhouse_driver import Client
import pandas as pd

# Подключение к ClickHouse
client = Client(host='localhost', port=9000, user='default', password='', database='analytics')


# Запрос для получения просмотров по дням
def fetch_views_by_day():
    query = '''
    SELECT 
        toDate(timestamp) AS day,
        COUNT(*) AS views_count
    FROM analytics.product_views
    WHERE timestamp >= now() - INTERVAL 7 DAY
    GROUP BY day
    ORDER BY day DESC;
    '''
    data = client.execute(query)
    print("Fetched data:", data)  # Логирование данных, полученных из ClickHouse
    return pd.DataFrame(data, columns=["Day", "Views"])


# Отображение графика
def show_chart():
    # Получаем данные
    data = fetch_views_by_day()

    if data.empty:
        print("No data to display.")  # Логирование
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'No data available', transform=ax.transAxes, ha='center', va='center', fontsize=16)
        plt.show()
    else:
        print("Data for chart:", data)  # Логирование данных для графика
        days = data["Day"].astype(str)  # Преобразование даты в строку для отображения на оси X
        views = data["Views"]

        print(f"Days: {days}")
        print(f"Views: {views}")

        # Построение графика
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.bar(days, views, color="lightblue")
        ax.set_title("Product Views by Day (Last 7 Days)")
        ax.set_xlabel("Day")
        ax.set_ylabel("Views")

        # Установка меток оси X
        ax.set_xticks(range(len(days)))  # Устанавливаем позиции меток
        ax.set_xticklabels(days, rotation=45, ha='right')  # Устанавливаем сами метки

        # Показ графика
        plt.show()


# Запуск отображения графика
show_chart()
```
### График
![График](image.jpg "График")