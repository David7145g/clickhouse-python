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