SELECT 1 FROM quote_price WHERE product_id IN (SELECT product_id FROM product_list WHERE sku = '{0}') AND st_id IN (SELECT st_id FROM ship_to WHERE st = {1}) AND quote_type = '{2}' AND quote_number = '{3}' AND min_order_quantity = {4} AND discount = {5} AND fixed_price = {6} AND effective_date = TIMESTAMP '{7}' AND expiration_date = TIMESTAMP '{8}' AND updated_date = TIMESTAMP '{9}'