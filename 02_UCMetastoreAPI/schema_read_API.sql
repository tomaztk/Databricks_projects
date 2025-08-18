CREATE CATALOG IF NOT EXISTS demo_api_uc;
CREATE SCHEMA  IF NOT EXISTS demo_api_uc.app2;

CREATE TABLE IF NOT EXISTS demo_api_uc.app2.orders (
  order_id     BIGINT,
  customer_id  BIGINT,
  country      STRING,
  amount       DOUBLE,
  ts           TIMESTAMP
) USING DELTA;

INSERT INTO demo_api_uc.app2.orders VALUES
 (1, 101, 'DE',  19.99,  current_timestamp()),
 (2, 102, 'DE', 150.00,  current_timestamp()),
 (3, 103, 'FR',  75.25,  current_timestamp()),
 (4, 104, 'DE',  49.90,  current_timestamp()),
 (5, 105, 'US', 220.10,  current_timestamp()),
 (6, 106, 'FR',  31.00,  current_timestamp()),
 (7, 107, 'US',  99.95,  current_timestamp()),
 (8, 108, 'DE',  60.00,  current_timestamp()),
 (9, 109, 'US',  12.49,  current_timestamp()),
 (10,110, 'FR', 300.00,  current_timestamp());



SELECT ` order_id`, customer_id, country, amount, `ts `
            FROM demo_api_uc.app2.order
            WHERE country = 'DE' AND amount >= 1
            ORDER BY `ts ` DESC
            LIMIT 100



SELECT *
FROM ai_query(
  "orders_sql_proxy",
  to_json(named_struct("country","DE","min_amount",50.0))
);
