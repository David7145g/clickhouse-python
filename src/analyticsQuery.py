from dd.module_1.lab_4.analQuery import *
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