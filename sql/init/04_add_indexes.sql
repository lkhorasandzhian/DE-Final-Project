-- =======================================================
-- |                       ORDERS                        |
-- =======================================================

-- Для фильтрации по пользователю.
CREATE INDEX IF NOT EXISTS idx_orders_user_id
    ON core.orders(user_id);

-- Для фильтрации по магазину.
CREATE INDEX IF NOT EXISTS idx_orders_store_id
    ON core.orders(store_id);

-- Для временных фильтров (дата создания/доставки/отмены заказа).
CREATE INDEX IF NOT EXISTS idx_orders_created_at
    ON core.orders(created_at);

CREATE INDEX IF NOT EXISTS idx_orders_delivered_at
    ON core.orders(delivered_at);

CREATE INDEX IF NOT EXISTS idx_orders_canceled_at
    ON core.orders(canceled_at);


-- =======================================================
-- |                     ORDER ITEMS                     |
-- =======================================================

-- Частые join'ы заказов и товаров.
CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON core.order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_item_id
    ON core.order_items(item_id);

-- Для замен товаров.
CREATE INDEX IF NOT EXISTS idx_order_items_replaced_id
    ON core.order_items(item_replaced_id);


-- =======================================================
-- |                    ORDER DRIVERS                    |
-- =======================================================

-- Join по заказу.
CREATE INDEX IF NOT EXISTS idx_order_drivers_order_id
    ON core.order_drivers(order_id);

-- Join по курьеру.
CREATE INDEX IF NOT EXISTS idx_order_drivers_driver_id
    ON core.order_drivers(driver_id);


-- =======================================================
-- |    Дополнительно на случай активной фильтрации.     |
-- =======================================================

-- По категории товара.
CREATE INDEX IF NOT EXISTS idx_items_category
    ON core.items(item_category);

-- По городу магазина.
CREATE INDEX IF NOT EXISTS idx_stores_city
    ON core.stores(store_city);

-- По городу доставки.
CREATE INDEX IF NOT EXISTS idx_orders_delivery_city
    ON core.orders(delivery_city);
