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