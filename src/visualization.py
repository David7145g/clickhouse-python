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