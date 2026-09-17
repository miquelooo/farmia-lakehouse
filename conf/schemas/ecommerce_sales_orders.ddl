order_id STRING,
customer_id STRING,
order_ts STRING,
channel STRING,
items ARRAY<STRUCT<product_id: STRING, qty: INT, unit_price: DOUBLE>>,
total DOUBLE,
currency STRING,
shipping_address STRUCT<city: STRING, zip: STRING, country: STRING>
