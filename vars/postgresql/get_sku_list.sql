SELECT sku FROM product_list WHERE updated_date IS NULL OR updated_date < NOW() - INTERVAL '{0} HOURS' FETCH FIRST {1} ROWS ONLY