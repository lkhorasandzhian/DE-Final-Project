CREATE TABLE IF NOT EXISTS core.users (
    user_id BIGINT,
    user_phone TEXT
);

CREATE TABLE IF NOT EXISTS core.stores (
    store_id BIGINT,
    store_address TEXT,
    store_city TEXT
);

CREATE TABLE IF NOT EXISTS core.drivers (
    driver_id BIGINT,
    driver_phone TEXT
);

CREATE TABLE IF NOT EXISTS core.items (
    item_id BIGINT,
    item_title TEXT NOT NULL,
    item_category TEXT
);

CREATE TABLE IF NOT EXISTS core.orders (
    order_id BIGINT,
    user_id BIGINT NOT NULL,
    store_id BIGINT,
    address_text TEXT,
    delivery_city TEXT,
    created_at TIMESTAMP,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type TEXT,
    order_discount NUMERIC(5,2),
    order_cancellation_reason TEXT,
    delivery_cost NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS core.order_items (
    order_item_sk BIGSERIAL,
    order_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    item_quantity INT NOT NULL,
    item_price NUMERIC(12,2) NOT NULL,
    item_canceled_quantity INT DEFAULT 0,
    item_discount NUMERIC(5,2) DEFAULT 0,
    item_replaced_id BIGINT
);

CREATE TABLE IF NOT EXISTS core.order_drivers (
    order_driver_sk BIGSERIAL,
    order_id BIGINT NOT NULL,
    driver_id BIGINT NOT NULL
);
