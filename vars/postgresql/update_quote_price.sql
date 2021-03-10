INSERT INTO quote_price (quote_type, quote_number, sku, st, min_order_quantity, discount, fixed_price, effective_date, expiration_date, e1_updated_date) VALUES {} ON CONFLICT (quote_type, quote_number, sku, st) DO UPDATE SET (quote_type, quote_number, sku, st, min_order_quantity, discount, fixed_price, effective_date, expiration_date, e1_updated_date) = (EXCLUDED.quote_type, EXCLUDED.quote_number, EXCLUDED.sku, EXCLUDED.st, EXCLUDED.min_order_quantity, EXCLUDED.discount, EXCLUDED.fixed_price, EXCLUDED.effective_date, EXCLUDED.expiration_date, EXCLUDED.e1_updated_date)