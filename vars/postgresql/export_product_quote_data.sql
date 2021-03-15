COPY (SELECT e1_updated_date, st, sku, quote_type, quote_number, NULLIF(discount, 'NaN') AS discount, NULLIF(fixed_price, 'NaN') AS fixed_price, min_order_quantity, effective_date, expiration_date FROM quote_price WHERE updated_date >= NOW() - INTERVAL '%s HOURS') TO STDOUT WITH CSV DELIMITER ',' HEADER