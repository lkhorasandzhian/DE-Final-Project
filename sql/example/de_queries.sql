-- 1. Общая статистика по дням недели (оборот, выручка, прибыль).
SELECT
    EXTRACT(DOW FROM report_date) AS day_of_week_num,
    TRIM(TO_CHAR(report_date, 'Day')) AS day_of_week,
    SUM(gross_amount) AS total_gross,
    SUM(revenue_amount) AS total_revenue,
    SUM(profit_amount) AS total_profit
FROM dm.dm_orders
GROUP BY
    EXTRACT(DOW FROM report_date),
    TRIM(TO_CHAR(report_date, 'Day'))
ORDER BY
    EXTRACT(DOW FROM report_date);

-- 2. Средний чек и поведение покупателей по месяцам.
SELECT
    report_year,
	report_month,
	ROUND(AVG(avg_check_amount), 2) AS avg_check,
	ROUND(AVG(orders_per_buyer), 2) AS avg_orders_per_buyer,
	ROUND(AVG(revenue_per_buyer), 2) AS avg_revenue_per_buyer
FROM dm.dm_orders
GROUP BY report_year, report_month
ORDER BY report_year, report_month;

-- 3. Какие магазины дают лучший средний чек.
SELECT
    city,
    store_id,
    store_address,
    ROUND(AVG(avg_check_amount), 2) AS avg_check
FROM dm.dm_orders
GROUP BY city, store_id, store_address
ORDER BY avg_check DESC
LIMIT 10;

-- 4. Где чаще всего менялся курьер: доля заказов со сменой курьера по магазинам.
SELECT
    city,
    store_id,
    store_address,
    SUM(orders_created_cnt) AS total_orders,
    SUM(orders_with_driver_change_cnt) AS driver_changed_orders,
    ROUND(
        100.0 * SUM(orders_with_driver_change_cnt) / NULLIF(SUM(orders_created_cnt), 0),
        2
    ) AS driver_change_rate_pct
FROM dm.dm_orders
GROUP BY city, store_id, store_address
HAVING SUM(orders_created_cnt) > 0
ORDER BY driver_change_rate_pct DESC, driver_changed_orders DESC
LIMIT 10;

-- 5. Какие товары приносят больше всего оборота по категориям.
SELECT
    item_category,
    SUM(item_gross_amount) AS total_gross
FROM dm.dm_items
GROUP BY item_category
ORDER BY total_gross DESC;

-- 6. Топ товаров с наибольшим числом отмененных позиций.
SELECT
    item_id,
    item_title,
    item_category,
    SUM(canceled_quantity) AS total_canceled_quantity
FROM dm.dm_items
GROUP BY item_id, item_title, item_category
ORDER BY total_canceled_quantity DESC
LIMIT 10;

-- 7. Товары с самой высокой долей отмен (минимум 100 заказанных единиц).
SELECT
    item_id,
    item_title,
    item_category,
    SUM(ordered_quantity) AS total_ordered,
    SUM(canceled_quantity) AS total_canceled,
    ROUND(100.0 * SUM(canceled_quantity) / NULLIF(SUM(ordered_quantity), 0), 2) AS cancel_rate_pct
FROM dm.dm_items
GROUP BY item_id, item_title, item_category
HAVING SUM(ordered_quantity) >= 100
ORDER BY
    ROUND(100.0 * SUM(canceled_quantity) / NULLIF(SUM(ordered_quantity), 0), 2) DESC,
    SUM(canceled_quantity) DESC
LIMIT 10;

-- 8. Конверсия товара: как часто товар отменяют в заказах.
SELECT
    item_id,
    item_title,
    item_category,
    SUM(orders_with_item_cnt) AS total_orders_with_item,
    SUM(orders_with_item_cancellation_cnt) AS canceled_orders_with_item,
    ROUND(100.0 * SUM(orders_with_item_cancellation_cnt) / NULLIF(SUM(orders_with_item_cnt), 0), 2) AS cancellation_rate_pct
FROM dm.dm_items
GROUP BY item_id, item_title, item_category
HAVING SUM(orders_with_item_cnt) >= 50
ORDER BY ROUND(100.0 * SUM(orders_with_item_cancellation_cnt) / NULLIF(SUM(orders_with_item_cnt), 0), 2) DESC
LIMIT 10;