-- Витрины под расчёт PySpark (src/datamarts/build_marts.py)

CREATE TABLE IF NOT EXISTS dm.dm_orders (
    order_id BIGINT NOT NULL,
    user_id BIGINT,
    store_id BIGINT,
    created_at TIMESTAMP,
    delivery_cost NUMERIC(12, 2),
    canceled_at TIMESTAMP,
    delivered_at TIMESTAMP,
    order_cancellation_reason TEXT,
    turnover DOUBLE PRECISION,
    revenue DOUBLE PRECISION,
    profit DOUBLE PRECISION,
    is_canceled INT,
    is_delivered INT,
    canceled_after_delivery INT,
    canceled_by_service INT,
    driver_changed INT,
    date DATE,
    year INT,
    month INT,
    day INT,
    store_address TEXT
);

CREATE TABLE IF NOT EXISTS dm.dm_orders_aggregated (
    year INT,
    month INT,
    day INT,
    store_id BIGINT,
    store_address TEXT,
    turnover DOUBLE PRECISION,
    revenue DOUBLE PRECISION,
    profit DOUBLE PRECISION,
    total_orders BIGINT,
    delivered_orders BIGINT,
    canceled_orders BIGINT,
    canceled_after_delivery BIGINT,
    canceled_by_service BIGINT,
    customer_count BIGINT,
    avg_check DOUBLE PRECISION,
    orders_per_customer DOUBLE PRECISION,
    revenue_per_customer DOUBLE PRECISION,
    orders_with_driver_change BIGINT
);

CREATE TABLE IF NOT EXISTS dm.dm_items (
    item_id BIGINT NOT NULL,
    item_title TEXT,
    item_category TEXT,
    date DATE,
    store_id BIGINT,
    store_address TEXT,
    turnover DOUBLE PRECISION,
    total_quantity_ordered DOUBLE PRECISION,
    total_quantity_canceled DOUBLE PRECISION,
    orders_with_item BIGINT,
    orders_with_canceled_item INT,
    total_quantity_delivered DOUBLE PRECISION,
    year INT,
    month INT,
    day INT
);

CREATE TABLE IF NOT EXISTS dm.active_drivers (
    driver_id BIGINT NOT NULL,
    order_count BIGINT
);
