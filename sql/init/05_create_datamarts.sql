-- =======================================================
-- |                    ORDERS DATAMART                  |
-- =======================================================

CREATE TABLE IF NOT EXISTS dm.dm_orders (
    report_date DATE NOT NULL,
    report_year INT NOT NULL,
    report_month INT NOT NULL,
    report_day INT NOT NULL,

    city TEXT NOT NULL,

    store_id BIGINT,
    store_address TEXT,

    gross_amount NUMERIC(18,2) NOT NULL DEFAULT 0,                  -- Оборот.
    revenue_amount NUMERIC(18,2) NOT NULL DEFAULT 0,                -- Выручка.
    profit_amount NUMERIC(18,2) NOT NULL DEFAULT 0,                 -- Прибыль.

    orders_created_cnt INT NOT NULL DEFAULT 0,                      -- Кол-во созданных заказов.
    orders_delivered_cnt INT NOT NULL DEFAULT 0,                    -- Кол-во доставленных заказов.
    orders_canceled_cnt INT NOT NULL DEFAULT 0,                     -- Кол-во отмененных заказов.
    orders_canceled_after_delivery_cnt INT NOT NULL DEFAULT 0,      -- Кол-во отмен после доставки.
    orders_canceled_service_fault_cnt INT NOT NULL DEFAULT 0,       -- Кол-во отмен по вине сервиса.

    buyers_cnt INT NOT NULL DEFAULT 0,                              -- Кол-во покупателей.
    avg_check_amount NUMERIC(18,2) NOT NULL DEFAULT 0,              -- Средний чек.
    orders_per_buyer NUMERIC(18,4) NOT NULL DEFAULT 0,              -- Заказов на покупателя.
    revenue_per_buyer NUMERIC(18,2) NOT NULL DEFAULT 0,             -- Выручка на покупателя.

    orders_with_driver_change_cnt INT NOT NULL DEFAULT 0,           -- Заказы со сменой курьера.
    active_drivers_cnt INT NOT NULL DEFAULT 0,                      -- Активные курьеры.

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dm_orders
        PRIMARY KEY (report_date, city, store_id)
);


-- =======================================================
-- |                    ITEMS DATAMART                   |
-- =======================================================

CREATE TABLE IF NOT EXISTS dm.dm_items (
    report_date DATE NOT NULL,
    report_year INT NOT NULL,
    report_month INT NOT NULL,
    report_day INT NOT NULL,

    report_week INT NOT NULL,                                       -- Для недельной популярности.

    city TEXT NOT NULL,

    store_id BIGINT,
    store_address TEXT,

    item_category TEXT,
    item_id BIGINT NOT NULL,
    item_title TEXT NOT NULL,

    item_gross_amount NUMERIC(18,2) NOT NULL DEFAULT 0,             -- Оборот товара.
    ordered_quantity INT NOT NULL DEFAULT 0,                        -- Заказанные единицы.
    canceled_quantity INT NOT NULL DEFAULT 0,                       -- Отмененные единицы.
    orders_with_item_cnt INT NOT NULL DEFAULT 0,                    -- Кол-во заказов с товаром.
    orders_with_item_cancellation_cnt INT NOT NULL DEFAULT 0,       -- Кол-во заказов с отменой товара.

    daily_popularity_rank INT,                                      -- Популярность за день.
    daily_reverse_popularity_rank INT,                              -- Непопулярность за день.

    weekly_popularity_rank INT,                                     -- Популярность за неделю.
    weekly_reverse_popularity_rank INT,                             -- Непопулярность за неделю.

    monthly_popularity_rank INT,                                    -- Популярность за месяц.
    monthly_reverse_popularity_rank INT,                            -- Непопулярность за месяц.

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dm_items
        PRIMARY KEY (report_date, city, store_id, item_id)
);