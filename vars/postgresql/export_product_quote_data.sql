SELECT e1_updated_date, st, sku, quote_type, quote_number, discount, fixed_price, min_order_quantity, effective_date, expiration_date FROM quote_price WHERE updated_date >= NOW() - INTERVAL '{0} HOURS'