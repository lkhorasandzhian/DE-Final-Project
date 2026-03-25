ALTER TABLE core.users
    ADD CONSTRAINT pk_users PRIMARY KEY (user_id);

ALTER TABLE core.stores
    ADD CONSTRAINT pk_stores PRIMARY KEY (store_id);

ALTER TABLE core.drivers
    ADD CONSTRAINT pk_drivers PRIMARY KEY (driver_id);

ALTER TABLE core.items
    ADD CONSTRAINT pk_items PRIMARY KEY (item_id);

ALTER TABLE core.orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

ALTER TABLE core.order_items
    ADD CONSTRAINT pk_order_items PRIMARY KEY (order_item_sk);

ALTER TABLE core.order_drivers
    ADD CONSTRAINT pk_order_drivers PRIMARY KEY (order_driver_sk);


ALTER TABLE core.orders
    ADD CONSTRAINT fk_orders_user
    FOREIGN KEY (user_id) REFERENCES core.users(user_id);

ALTER TABLE core.orders
    ADD CONSTRAINT fk_orders_store
    FOREIGN KEY (store_id) REFERENCES core.stores(store_id);

ALTER TABLE core.order_items
    ADD CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES core.orders(order_id);

ALTER TABLE core.order_items
    ADD CONSTRAINT fk_order_items_item
    FOREIGN KEY (item_id) REFERENCES core.items(item_id);

ALTER TABLE core.order_items
    ADD CONSTRAINT fk_order_items_replaced_item
    FOREIGN KEY (item_replaced_id) REFERENCES core.items(item_id);

ALTER TABLE core.order_drivers
    ADD CONSTRAINT fk_order_drivers_order
    FOREIGN KEY (order_id) REFERENCES core.orders(order_id);

ALTER TABLE core.order_drivers
    ADD CONSTRAINT fk_order_drivers_driver
    FOREIGN KEY (driver_id) REFERENCES core.drivers(driver_id);


ALTER TABLE core.order_drivers
    ADD CONSTRAINT uq_order_drivers_order_driver
    UNIQUE (order_id, driver_id);


ALTER TABLE core.order_items
    ADD CONSTRAINT chk_order_items_quantity_nonnegative
    CHECK (item_quantity >= 0);

ALTER TABLE core.order_items
    ADD CONSTRAINT chk_order_items_canceled_quantity_nonnegative
    CHECK (item_canceled_quantity >= 0);

ALTER TABLE core.order_items
    ADD CONSTRAINT chk_order_items_canceled_lte_quantity
    CHECK (item_canceled_quantity <= item_quantity);

ALTER TABLE core.order_items
    ADD CONSTRAINT chk_order_items_price_nonnegative
    CHECK (item_price >= 0);

ALTER TABLE core.order_items
    ADD CONSTRAINT chk_order_items_discount_range
    CHECK (item_discount >= 0 AND item_discount <= 100);

ALTER TABLE core.orders
    ADD CONSTRAINT chk_orders_discount_range
    CHECK (
        order_discount IS NULL
        OR (order_discount >= 0 AND order_discount <= 100)
    );

ALTER TABLE core.orders
    ADD CONSTRAINT chk_orders_delivery_cost_nonnegative
    CHECK (
        delivery_cost IS NULL
        OR delivery_cost >= 0
    );
